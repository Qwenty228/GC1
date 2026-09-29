"""In-memory state for summarizing one event: evidence, the model's output, the graph task."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

# Generation reasons stored on event_summary / event_summary_job.
INITIAL = "initial"
NEW_INFORMATION = "new_information"
REVIEW_RESOLUTION = "review_resolution"
MANUAL_REFRESH = "manual_refresh"

# Routes out of route_event_summary.
GENERATE = "generate"
CONFLICT = "conflict"
SKIP = "skip"


@dataclass
class SourceInfo:
    """One source document (its current revision) that is an active member of the event."""

    revision_id: str
    document_id: str
    membership_id: str
    relation: str  # origin | same_event_repeated | same_event_update | same_event_conflict
    comparison_id: str | None
    record_type: str  # news_article | sec_filing
    source_name: str | None  # publisher, or the filing issuer
    title: str | None
    url: str | None
    source_time: datetime | None
    content_scope: str | None = None
    form: str | None = None
    accession_number: str | None = None
    item_codes: list[str] = field(default_factory=list)

    @property
    def kind_label(self) -> str:
        if self.record_type == "sec_filing":
            return f"SEC Form {self.form or '8-K'} filed by the company (company disclosure, not verified by the SEC)"
        return "news article (media report)"


@dataclass
class EvidencePassage:
    ref: str  # P1, P2, ... (prompt reference)
    unit_id: str
    revision_id: str
    membership_id: str
    relation: str
    text: str  # the original source passage (exact source spans, no added context prefix)
    input_hash: str
    tier: int
    ordinal: int


@dataclass
class ComparisonNote:
    """What grouping's comparison recorded about one member (machine output; context, not evidence)."""

    comparison_id: str
    revision_id: str
    label: str
    method: str
    matching_facts: list[str]
    differences: list[dict[str, Any]]
    explanation: str
    incoming_unit_ids: list[str]
    candidate_unit_ids: list[str]


@dataclass
class ReviewContext:
    """A resolved conflict review that the summary must respect."""

    review_id: str
    comparison_id: str
    revision_id: str  # the revision whose claim conflicted
    status: str
    resolution: str | None
    reviewer_notes: str | None
    conflicting_claims: list[dict[str, Any]]
    incoming_unit_ids: list[str]
    candidate_unit_ids: list[str]


@dataclass
class PreviousSummary:
    summary_id: str
    version: int
    headline: str
    statements: list[dict[str, Any]]  # {id, kind, text, citations: [{unit_id, quote}]}


@dataclass
class EvidencePack:
    """Bounded, deterministic input for one event summary."""

    event_id: str
    is_sample: bool
    language: str
    event_title: str
    event_status: str
    sources: dict[str, SourceInfo]  # revision id -> source
    passages: list[EvidencePassage]
    comparisons: list[ComparisonNote]
    reviews: list[ReviewContext]
    previous: PreviousSummary | None
    evidence_fingerprint: str
    input_fingerprint: str = ""
    omitted_unit_ids: list[str] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)

    def passage(self, ref: str) -> EvidencePassage | None:
        return next((p for p in self.passages if p.ref == ref), None)

    def refs_for_units(self, unit_ids: list[str]) -> list[str]:
        wanted = set(unit_ids)
        return [p.ref for p in self.passages if p.unit_id in wanted]

    def manifest(self) -> dict[str, Any]:
        """What the model was given, stored with the summary and every attempt."""
        return {
            "evidence_fingerprint": self.evidence_fingerprint,
            "revision_ids": sorted({p.revision_id for p in self.passages}),
            "unit_ids": [p.unit_id for p in self.passages],
            "omitted_unit_ids": list(self.omitted_unit_ids),
            "member_revision_ids": sorted(self.sources),
            "comparison_ids": sorted({c.comparison_id for c in self.comparisons}),
            "review_ids": sorted({r.review_id for r in self.reviews}),
            "previous_summary_id": self.previous.summary_id if self.previous else None,
            "passages": len(self.passages),
            "passage_chars": sum(len(p.text) for p in self.passages),
        }


@dataclass
class Citation:
    unit_id: str
    quote: str
    quote_index: int = 1  # position among this statement's distinct quotes from this unit (from 1)


@dataclass
class Statement:
    id: str  # H1 | O1..O3 | K1..K3 (stable within a version)
    kind: str  # headline | overview | key_point
    text: str
    citations: list[Citation]

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.id, "kind": self.kind, "text": self.text,
                "citations": [{"unit_id": c.unit_id, "quote": c.quote, "quote_index": c.quote_index}
                              for c in self.citations]}


@dataclass
class SummaryDraft:
    """Validated output, ready to publish."""

    statements: list[Statement]
    evidence_gaps: list[str] = field(default_factory=list)

    @property
    def headline(self) -> str:
        return next(s.text for s in self.statements if s.kind == "headline")

    @property
    def overview(self) -> str:
        return " ".join(s.text for s in self.statements if s.kind == "overview")

    @property
    def key_points(self) -> list[dict[str, str]]:
        return [{"id": s.id, "text": s.text} for s in self.statements if s.kind == "key_point"]


@dataclass
class SummaryTask:
    """Summary work for ONE event affected by the record being processed."""

    event_id: str
    language: str
    revision_id: str | None = None  # the record that made this event "affected"
    relation: str | None = None  # this record's relation to the event
    membership_state: str | None = None
    comparison_label: str | None = None
    comparison_method: str | None = None

    route: str | None = None  # generate | conflict | skip
    reason: str = ""
    generation_reason: str | None = None
    job: dict[str, Any] | None = None
    lease_owner: str | None = None

    pack: EvidencePack | None = None
    output: dict[str, Any] | None = None  # raw structured output
    model: str | None = None
    usage: dict[str, Any] = field(default_factory=dict)
    draft: SummaryDraft | None = None
    validation_errors: list[str] = field(default_factory=list)
    error: dict[str, Any] | None = None

    result: str | None = None  # published | unchanged | invalid | error | stale | blocked | deferred |
    #                            awaiting_review | skipped | claimed_elsewhere
    summary_id: str | None = None
    version: int | None = None
    reviews_created: int = 0

    def report(self) -> dict[str, Any]:
        out: dict[str, Any] = {"event_id": self.event_id, "route": self.route, "reason": self.reason,
                               "result": self.result}
        if self.relation:
            out["relation"] = self.relation
        if self.version is not None:
            out["version"] = self.version
        if self.validation_errors:
            out["validation_errors"] = self.validation_errors
        if self.error:
            out["error"] = self.error
        if self.reviews_created:
            out["reviews_created"] = self.reviews_created
        return out
