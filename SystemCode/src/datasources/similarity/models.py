"""Data carried between the analysis nodes of the ingestion graph.

Everything here is plain in-memory state. Nothing is persisted until the
graph's ``persist_result`` node, which maps the temporary keys used here
(``unit:0``, ``new:S1``, ...) to database ids inside one transaction.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any

from ..ingest.schema import PreparedRecord

LABELS = (
    "same_event_repeated",
    "same_event_update",
    "same_event_conflict",
    "different_event",
    "uncertain",
)
SAME_EVENT_LABELS = frozenset({"same_event_repeated", "same_event_update", "same_event_conflict"})

# Pipeline content_status -> evidence level used by text units and comparison.
_PIPELINE_EVIDENCE = {"full_text": "full", "summary_only": "summary_only", "headline_only": "headline_only"}


SECRET_ENV_NAMES = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "PGPASSWORD", "ALPACA_API_KEY",
                    "ALPACA_SECRET_KEY", "ALPACA_BROKER_CLIENT_SECRET")


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sanitize(message: str) -> str:
    """Mask credential values before an error message is logged or stored."""
    for name in SECRET_ENV_NAMES:
        secret = os.environ.get(name, "")
        if len(secret) >= 6:
            message = message.replace(secret, "***")
    return message


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


@dataclass
class AnalysisSubject:
    """What the analysis needs to know about one revision, stored or not yet stored."""

    record_type: str  # news_article | sec_filing
    provider: str  # alpaca | sec_edgar
    is_sample: bool
    source_record_id: str
    source_namespace: str
    title: str | None
    title_is_generated: bool
    summary_text: str | None
    body_text: str | None
    analysis_text: str | None
    evidence_level: str | None  # full | summary_only | headline_only | None (nothing usable)
    unusable_reason: str | None = None  # e.g. extraction_failed
    publisher: str | None = None
    issuer_name: str | None = None
    cik: str | None = None
    form: str | None = None
    filing_date: str | None = None
    items: list[str] = field(default_factory=list)
    symbols: list[str] = field(default_factory=list)
    published_at: datetime | None = None
    updated_at: datetime | None = None
    retrieved_at: datetime | None = None
    snapshot_hash: str | None = None
    preprocessing_version: str | None = None
    # Known once the revision is stored (reanalysis) or after persist_result.
    document_id: str | None = None
    revision_id: str | None = None
    company_ids: list[str] | None = None

    @classmethod
    def from_record(cls, record: PreparedRecord) -> "AnalysisSubject":
        meta = record.sec_metadata
        evidence = _PIPELINE_EVIDENCE.get(record.content_status)
        unusable = None
        if record.record_type == "sec_filing":
            ok = meta is not None and meta.extraction.status == "ok" and bool((record.body_text or "").strip())
            evidence, unusable = ("full", None) if ok else (None, "extraction_failed")
        return cls(
            record_type=record.record_type,
            provider=record.provider,
            is_sample=record.is_sample,
            source_record_id=record.source_record_id,
            source_namespace=meta.cik if meta is not None else "news",
            title=record.title,
            title_is_generated=record.title_is_generated,
            summary_text=record.summary_text,
            body_text=record.body_text,
            analysis_text=record.analysis_text,
            evidence_level=evidence,
            unusable_reason=unusable,
            publisher=record.publisher,
            issuer_name=record.issuer_name,
            cik=meta.cik if meta is not None else None,
            form=meta.form if meta is not None else None,
            filing_date=meta.filing_date if meta is not None else None,
            items=list(meta.items) if meta is not None else [],
            symbols=list(record.symbols),
            published_at=_utc(record.published_at),
            updated_at=_utc(record.updated_at),
            retrieved_at=_utc(record.retrieved_at),
            snapshot_hash=record.provenance.dedup.content_fingerprint,
            preprocessing_version=record.provenance.pipeline_version,
        )

    @property
    def label(self) -> str:
        return f"{self.provider}:{self.source_namespace}:{self.source_record_id}"

    def anchor(self) -> tuple[datetime | None, str]:
        """Time the retrieval window is anchored to, and where it came from.

        Precedence: the source's own publication time (Alpaca ``created_at``,
        SEC ``acceptanceDateTime``), then the source's update time, then the SEC
        filing date (start of that UTC day; a date-only value is only used for
        the window, never stored as a timestamp), then the retrieval time. The
        worker's current clock is never used.
        """
        if self.published_at is not None:
            return self.published_at, "published_at"
        if self.updated_at is not None:
            return self.updated_at, "source_updated_at"
        if self.filing_date:
            try:
                day = date.fromisoformat(self.filing_date)
                return datetime.combine(day, time(0, 0), tzinfo=timezone.utc), "filing_date"
            except ValueError:
                pass
        if self.retrieved_at is not None:
            return self.retrieved_at, "retrieved_at"
        return None, "missing"


# ----------------------------------------------------------------------------- text units
@dataclass
class UnitDraft:
    key: str  # temporary id, e.g. "unit:0"
    kind: str  # document | chunk
    ordinal: int
    text: str  # exact text embedded (context prefix + passage)
    passage: str  # the traceable source passage
    segment_key: str | None  # None = context unit that supports no event on its own
    location: dict[str, Any]
    token_count: int

    @property
    def input_hash(self) -> str:
        return sha256_text(self.text)


@dataclass
class SegmentDraft:
    """A part of one revision that can describe one event (article, SEC Item)."""

    key: str  # "S1", "S2", ...
    label: str
    kind: str  # article | sec_item | document
    item_codes: list[str]
    unit_keys: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {"key": self.key, "label": self.label, "kind": self.kind,
                "item_codes": self.item_codes, "unit_keys": self.unit_keys}


@dataclass
class UnitPlan:
    units: list[UnitDraft]
    segments: list[SegmentDraft]
    evidence_level: str | None
    skip_reason: str | None = None
    skipped_passages: list[dict[str, Any]] = field(default_factory=list)
    source_field: str | None = None

    def unit(self, key: str) -> UnitDraft:
        return next(u for u in self.units if u.key == key)

    def segment_text(self, segment: SegmentDraft) -> str:
        """Whole segment text without the overlap repeated between chunks."""
        parts: list[str] = []
        for key in segment.unit_keys:
            unit = self.unit(key)
            overlap = int(unit.location.get("overlap_prev_chars") or 0)
            parts.append(unit.passage[overlap:] if overlap else unit.passage)
        return "\n\n".join(parts)


# ----------------------------------------------------------------------------- retrieval
@dataclass
class CandidatePassage:
    unit_id: str
    revision_id: str
    document_id: str
    text: str
    passage: str
    record_type: str
    title: str | None
    publisher: str | None
    source_time: datetime | None
    event_ids: list[str]
    location: dict[str, Any] = field(default_factory=dict)
    semantic_rank: int | None = None
    semantic_similarity: float | None = None
    lexical_rank: int | None = None
    lexical_score: float | None = None
    fused_score: float = 0.0
    incoming_unit_key: str | None = None


@dataclass
class CandidateEvent:
    event_id: str
    title: str
    status: str
    facts: dict[str, Any]
    member_documents: int
    first_source_time: datetime | None
    origin_passages: list[CandidatePassage]
    source: str = "retrieval"  # retrieval | previous_revision
    passages: list[CandidatePassage] = field(default_factory=list)
    best_fused_score: float = 0.0
    best_semantic_similarity: float | None = None
    best_lexical_score: float | None = None
    segment_keys: set[str] = field(default_factory=set)


@dataclass
class RetrievalHit:
    """One (incoming unit, candidate unit) pair with both channels' ranks kept."""

    incoming_unit_key: str
    candidate: CandidatePassage
    fused_rank: int
    shortlisted: bool = False
    not_compared_reason: str | None = None


# ----------------------------------------------------------------------------- decisions
@dataclass
class Judgment:
    """A validated comparison between one incoming segment and one candidate event."""

    segment_key: str
    event_id: str
    label: str
    method: str  # llm | exact_content
    incoming_unit_keys: list[str]
    candidate_unit_ids: list[str]
    candidate_revision_ids: list[str]
    matching_facts: list[str] = field(default_factory=list)
    differences: list[dict[str, Any]] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)
    explanation: str = ""
    insufficient_context: bool = False
    uncertainty: str | None = None
    validation_notes: list[str] = field(default_factory=list)
    model: str | None = None
    prompt_version: str | None = None
    usage: dict[str, Any] = field(default_factory=dict)
    persisted_id: str | None = None  # set when loaded from the database


@dataclass
class NewEventDraft:
    key: str  # "new:S1"
    segment_key: str
    title: str
    facts: dict[str, Any]
    flags: list[str]
    source_time: datetime | None


@dataclass
class MembershipDraft:
    event_ref: str  # an existing event id, or a NewEventDraft key ("new:...")
    segment_key: str
    relation: str  # origin | same_event_* | uncertain
    unit_keys: list[str]
    judgment: Judgment | None = None

    @property
    def state(self) -> str:
        return "pending" if self.relation == "uncertain" else "active"


@dataclass
class SegmentOutcome:
    segment_key: str
    status: str  # linked | new_event | pending_relation | unresolved | kept_existing
    detail: str = ""


@dataclass
class ExistingState:
    """What an earlier attempt already persisted for this revision (reanalysis only)."""

    analysis: dict[str, Any] | None = None
    units: list[dict[str, Any]] = field(default_factory=list)
    judgments: list[Judgment] = field(default_factory=list)
    memberships: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class RecordWork:
    """Per-record state carried through the analysis nodes of the graph."""

    index: int
    subject: AnalysisSubject
    mode: str  # ingest | reanalyze
    record: PreparedRecord | None = None
    raw_payload: dict[str, Any] | None = None
    raw_document: str | None = None
    analysis_enabled: bool = False

    replay: bool = False  # revision and its analysis are already stored: reuse, call no model
    predecessor_revision_id: str | None = None
    existing: ExistingState = field(default_factory=ExistingState)

    plan: UnitPlan | None = None
    unit_ids: dict[str, str] = field(default_factory=dict)  # unit key -> persisted unit id
    vectors: dict[str, list[float]] = field(default_factory=dict)
    stored_vector_keys: set[str] = field(default_factory=set)
    reused_vector_keys: set[str] = field(default_factory=set)
    embedding_error: str | None = None

    company_ids: list[str] = field(default_factory=list)
    anchor_time: datetime | None = None
    anchor_source: str | None = None
    hits: list[RetrievalHit] = field(default_factory=list)
    candidate_events: dict[str, CandidateEvent] = field(default_factory=dict)
    coverage: dict[str, Any] = field(default_factory=dict)
    retrieval_error: str | None = None

    route: str | None = None
    pairs: dict[str, list[str]] = field(default_factory=dict)  # segment key -> candidate event ids to compare
    judgments: list[Judgment] = field(default_factory=list)  # new in this attempt (exact + llm)
    unevaluated: list[tuple[str, str]] = field(default_factory=list)
    error_pairs: list[tuple[str, str]] = field(default_factory=list)  # unevaluated because the model call failed
    comparison_errors: list[dict[str, Any]] = field(default_factory=list)
    segment_events: dict[str, tuple[str, dict[str, Any]]] = field(default_factory=dict)
    llm_calls: int = 0
    usage: dict[str, Any] = field(default_factory=dict)

    new_events: list[NewEventDraft] = field(default_factory=list)
    memberships: list[MembershipDraft] = field(default_factory=list)
    supersede: list[tuple[str, str]] = field(default_factory=list)  # (old event id, replacement event ref)
    segment_outcomes: list[SegmentOutcome] = field(default_factory=list)

    status: str | None = None  # completed | partial | pending | skipped | disabled | failed
    stage_status: dict[str, Any] = field(default_factory=dict)
    limitations: list[str] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)

    def note_error(self, stage: str, error: BaseException | str, *, retryable: bool = True) -> None:
        message = sanitize(str(error))
        self.errors.append({"stage": stage, "error_type": type(error).__name__ if isinstance(error, BaseException)
                            else "error", "message": message[:500], "retryable": retryable})

    @property
    def all_judgments(self) -> list[Judgment]:
        return [*self.existing.judgments, *self.judgments]
