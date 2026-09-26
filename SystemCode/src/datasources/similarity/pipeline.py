"""Analysis steps run by the ingestion graph's nodes (see ``datasources.ingest.graph``).

Each public function here is the body of one graph node and works on one
``RecordWork``. Failures are recorded on the work item instead of raised, so one
record's embedding outage, retrieval error or LLM refusal never discards the
rest of its batch, and the article itself is still stored with an explicit
``pending`` analysis status.

Resolution rules (``resolve``)
    * repeated information           -> linked to the existing event
    * additional information / later -> linked and marked ``same_event_update``
    * incompatible comparable claims -> linked and flagged ``same_event_conflict``
    * uncertain                      -> a pending relationship, never a merge
    * no suitable event              -> a provisional new event
    A segment gets a new event only after every shortlisted candidate event was
    evaluated: rejecting one candidate does not reject the others, and an
    unevaluated candidate (budget reached, model failure) leaves the segment
    unresolved instead of declaring it new. Links go to events whose defining
    evidence was compared directly, so A~B and B~C never merges A with C.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..db.repository import persist_record
from ..ingest.settings import ConfigurationError
from . import retrieval, store
from .comparison import (
    PROMPT_VERSION,
    ComparisonError,
    ComparisonRequest,
    PromptEvent,
    PromptPassage,
    PromptSegment,
    PromptUnit,
    anthropic_credentials_available,
    build_comparison_backend,
    validate_output,
)
from .embeddings import EmbeddingError, build_embedding_backend, profile_spec, validate_vectors
from .models import (
    SAME_EVENT_LABELS,
    AnalysisSubject,
    CandidateEvent,
    CandidatePassage,
    ExistingState,
    Judgment,
    MembershipDraft,
    NewEventDraft,
    RecordWork,
    SegmentDraft,
    SegmentOutcome,
    sanitize,
)
from .settings import PIPELINE_VERSION, SimilaritySettings
from .textunits import TextUnitBuilder

LOGGER = logging.getLogger(__name__)
RELATION_RANK = {"same_event_conflict": 4, "same_event_update": 3, "same_event_repeated": 2, "origin": 1,
                 "uncertain": 0}


# ----------------------------------------------------------------------------- runtime
@dataclass
class SimilarityRuntime:
    settings: SimilaritySettings
    embedder: Any
    comparator: Any
    spec: Any
    profile_id: str
    builder: TextUnitBuilder
    pipeline_version: str = PIPELINE_VERSION

    @property
    def comparison_model(self) -> str | None:
        return getattr(self.comparator, "model", None)

    def describe(self) -> dict[str, Any]:
        ident = self.spec.identity
        return {"profile_id": self.profile_id, "embedding": f"{ident.model_provider}:{ident.model_name}@"
                f"{ident.model_version[:12]} ({ident.dimensions}d, {ident.distance_metric})",
                "comparison": f"{getattr(self.comparator, 'name', '?')}:{self.comparison_model}",
                "pipeline_version": self.pipeline_version}


def build_runtime(database, settings: SimilaritySettings, *, embedder=None, comparator=None) -> SimilarityRuntime:
    """Validate the similarity configuration and prepare the runtime (startup, not per record)."""
    if database is None:
        raise ConfigurationError("SIMILARITY_ENABLED=true requires PostgreSQL: set DATASOURCES_DB_ENABLED=true "
                                 "and the PG* settings (or set SIMILARITY_ENABLED=false)")
    info = database.check()
    if not info.pgvector_installed:
        raise ConfigurationError(f"SIMILARITY_ENABLED=true requires the pgvector extension in {info.database} "
                                 "(CREATE EXTENSION vector;)")
    with database.connection() as conn:
        missing = store.missing_schema(conn)
    if missing:
        raise ConfigurationError(f"database schema is missing {', '.join(missing)}; run `uv run datasources-db migrate`")
    embedder = embedder or build_embedding_backend(settings)
    identity = embedder.identity()  # loads a local model; never downloads
    if comparator is None:
        if settings.comparison_backend == "anthropic" and not anthropic_credentials_available():
            raise ConfigurationError(
                "SIMILARITY_COMPARISON_BACKEND=anthropic needs API credentials: set ANTHROPIC_API_KEY in .env "
                "(or log in with `ant auth login`), or set SIMILARITY_COMPARISON_BACKEND=mock for an offline run "
                "that never links events")
        comparator = build_comparison_backend(settings)
    spec = profile_spec(embedder, settings)
    with database.transaction() as conn:
        profile_id = store.get_or_create_profile(conn, spec)
    builder = TextUnitBuilder(embedder.count_tokens, max_input_tokens=identity.max_input_tokens,
                              chunk_tokens=settings.chunk_tokens, overlap_tokens=settings.chunk_overlap_tokens)
    return SimilarityRuntime(settings, embedder, comparator, spec, profile_id, builder)


@dataclass
class PipelineContext:
    """Dependencies handed to every graph node (LangGraph runtime context)."""

    database: Any = None
    analyzer: SimilarityRuntime | None = None
    persist: bool = True
    # Bootstrap pass 1: create units and embeddings only; grouping runs once the whole
    # selected set is indexed, so historical records are not "new" merely because
    # their candidates had no embeddings yet.
    index_only: bool = False
    now: Any = None  # callable returning an aware datetime (tests)

    def clock(self) -> datetime:
        return self.now() if self.now else datetime.now(timezone.utc)


@dataclass
class WorkInput:
    """One prepared record waiting for analysis and persistence (ingest mode)."""

    index: int
    record: Any
    raw_payload: dict | None
    raw_document: str | None


@dataclass
class RecordOutcome:
    index: int
    record: Any = None
    revision_id: str | None = None
    document_id: str | None = None
    persisted: bool = False
    persist_outcome: str | None = None  # inserted | updated | skipped
    unresolved_symbols: list[str] = field(default_factory=list)
    analysis_status: str = "disabled"
    analysis: dict[str, Any] = field(default_factory=dict)
    error: dict[str, Any] | None = None


def _runnable(work: RecordWork) -> bool:
    return (work.analysis_enabled and not work.replay and work.status is None and not work.errors)


# ----------------------------------------------------------------------------- node: create_text_units
def start(item, index: int) -> RecordWork:
    if isinstance(item, AnalysisSubject):
        return RecordWork(index=index, subject=item, mode="reanalyze")
    return RecordWork(index=item.index, subject=AnalysisSubject.from_record(item.record), mode="ingest",
                      record=item.record, raw_payload=item.raw_payload, raw_document=item.raw_document)


def create_text_units(work: RecordWork, ctx: PipelineContext) -> None:
    runtime = ctx.analyzer
    if runtime is None or ctx.database is None:
        work.status = "disabled"
        work.stage_status["analysis"] = "disabled"
        return
    work.analysis_enabled = True
    space = (runtime.profile_id, runtime.pipeline_version)
    subject = work.subject
    try:
        with ctx.database.connection() as conn:
            if work.mode == "ingest":
                found = store.find_persisted_revision(conn, subject)
                if found is not None:
                    subject.document_id, subject.revision_id = str(found["document_id"]), str(found["revision_id"])
                    analysis = store.load_analysis(conn, subject.revision_id, *space)
                    if analysis is not None:
                        # Committed before (e.g. the file or ledger write failed afterwards): reuse it and
                        # call no model. Pending work is finished by `datasources-similarity retry`.
                        work.replay = True
                        work.status = analysis["status"]
                        work.stage_status["analysis"] = "reused_persisted"
                        return
                    # Stored earlier without analysis (similarity was off, or a legacy backfill).
                    work.existing = _load_existing(conn, subject, space)
                    work.predecessor_revision_id = _previous_revision(conn, subject)
                else:
                    document = store.find_document(conn, subject)
                    if document is not None:
                        # A new revision of a known document: its earlier revisions are never
                        # independent candidates; the current one is compared deliberately.
                        subject.document_id = str(document["document_id"])
                        if document["current_revision_id"] is not None:
                            work.predecessor_revision_id = str(document["current_revision_id"])
            else:
                work.existing = _load_existing(conn, subject, space)
                work.predecessor_revision_id = _previous_revision(conn, subject)
            work.company_ids = store.resolve_company_ids(conn, subject)
        plan = runtime.builder.build(subject)
        work.plan = plan
        if plan.skip_reason:
            work.status = "skipped"
            work.stage_status["text_units"] = f"skipped:{plan.skip_reason}"
            return
        _attach_stored_units(work)
        work.stage_status["text_units"] = f"ok:{len(plan.units)} unit(s), {len(plan.segments)} segment(s)"
        if plan.skipped_passages:
            reasons = sorted({p["reason"] for p in plan.skipped_passages})
            work.limitations.append(f"not embedded (recorded, not dropped): {', '.join(reasons)}")
            work.stage_status["skipped_passages"] = plan.skipped_passages  # section, reason, source spans
        if plan.evidence_level != "full":
            work.limitations.append(f"limited evidence: {plan.evidence_level}")
    except Exception as error:  # isolate: the record is still stored, analysis stays pending
        LOGGER.exception("text units failed for %s", subject.label)
        work.stage_status["text_units"] = "failed"
        work.note_error("text_units", error)


def _load_existing(conn, subject: AnalysisSubject, space: tuple[str, str]) -> ExistingState:
    analysis = store.load_analysis(conn, subject.revision_id, *space)
    units = store.load_units(conn, subject.revision_id, space[0])
    state = ExistingState(analysis=analysis, units=units,
                          memberships=store.load_memberships(conn, subject.revision_id, *space))
    if analysis is not None:
        keys = {u["unit_id"]: f"unit:{u['ordinal']}" for u in units}
        state.judgments = store.load_judgments(conn, str(analysis["analysis_id"]), keys)
    return state


def _previous_revision(conn, subject: AnalysisSubject) -> str | None:
    row = conn.execute(
        """
        SELECT r.revision_id FROM document_revision r
         WHERE r.document_id = %s AND r.revision_id <> %s
           AND r.retrieved_at <= (SELECT retrieved_at FROM document_revision WHERE revision_id = %s)
         ORDER BY r.retrieved_at DESC, r.revision_id LIMIT 1
        """,
        (subject.document_id, subject.revision_id, subject.revision_id),
    ).fetchone()
    return str(row["revision_id"]) if row else None


def _attach_stored_units(work: RecordWork) -> None:
    if not work.existing.units:
        return
    stored = {(u["unit_kind"], u["ordinal"]): u for u in work.existing.units}
    for unit in work.plan.units:
        row = stored.get((unit.kind, unit.ordinal))
        if row is None:
            continue
        if row["input_hash"] != unit.input_hash:
            raise ValueError(f"stored text unit {unit.kind}/{unit.ordinal} no longer matches the text preparation "
                             "of this profile")
        work.unit_ids[unit.key] = row["unit_id"]
        if row["vector"] is not None and row["embedding_status"] == "completed":
            work.vectors[unit.key] = row["vector"]
            work.stored_vector_keys.add(unit.key)


# ----------------------------------------------------------------------------- node: generate_embeddings
def generate_embeddings(work: RecordWork, ctx: PipelineContext) -> None:
    if not _runnable(work):
        return
    runtime = ctx.analyzer
    plan = work.plan
    missing = [u for u in plan.units if u.key not in work.vectors]
    if not missing:
        work.stage_status["embeddings"] = "reused_stored"
        return
    try:
        with ctx.database.connection() as conn:
            reusable = store.vectors_by_hash(conn, runtime.profile_id, [u.input_hash for u in missing])
        for unit in missing:
            if unit.input_hash in reusable:
                work.vectors[unit.key] = reusable[unit.input_hash]
                work.reused_vector_keys.add(unit.key)
        todo = [u for u in missing if u.key not in work.vectors]
        size = max(1, runtime.settings.embedding_batch_size)
        dimensions = runtime.spec.identity.dimensions
        for start_at in range(0, len(todo), size):
            batch = todo[start_at:start_at + size]
            vectors = validate_vectors(runtime.embedder.embed([u.text for u in batch]), dimensions)
            for unit, vector in zip(batch, vectors):
                work.vectors[unit.key] = vector
        work.stage_status["embeddings"] = (f"ok:{len(todo)} computed, {len(work.reused_vector_keys)} reused, "
                                           f"{len(work.stored_vector_keys)} stored")
        work.usage["embeddings_computed"] = len(todo)
        work.usage["embeddings_reused"] = len(work.reused_vector_keys)
    except EmbeddingError as error:
        work.embedding_error = str(error)
        work.stage_status["embeddings"] = "failed"
        work.note_error("embeddings", error, retryable=error.retryable)
    except Exception as error:
        LOGGER.exception("embedding failed for %s", work.subject.label)
        work.embedding_error = str(error)
        work.stage_status["embeddings"] = "failed"
        work.note_error("embeddings", error)


# ----------------------------------------------------------------------------- node: retrieve_candidates
def retrieve_candidates(work: RecordWork, ctx: PipelineContext) -> None:
    if not _runnable(work):
        return
    if ctx.index_only:
        work.stage_status["retrieval"] = "deferred: index-only pass"
        return
    runtime = ctx.analyzer
    try:
        with ctx.database.connection() as conn:
            retrieval.retrieve(conn, work, profile_id=runtime.profile_id,
                               pipeline_version=runtime.pipeline_version, settings=runtime.settings)
        compared = sum(1 for h in work.hits if h.shortlisted)
        work.stage_status["retrieval"] = (f"ok:{len(work.hits)} hit(s), {len(work.candidate_events)} candidate "
                                          f"event(s), {compared} shortlisted hit(s)")
    except Exception as error:  # never turn a retrieval error into "no matches"
        LOGGER.exception("retrieval failed for %s", work.subject.label)
        work.retrieval_error = str(error)
        work.stage_status["retrieval"] = "failed"
        work.note_error("retrieval", error)


# ----------------------------------------------------------------------------- node: route_candidates
def route(work: RecordWork, ctx: PipelineContext) -> str:
    """Decide which node runs next: ``compare``, ``propose`` or ``resolve``."""
    if not _runnable(work) or ctx.index_only:
        work.route = "resolve"
        return work.route
    settings = ctx.analyzer.settings
    plan = work.plan
    judged = {(j.segment_key, j.event_id) for j in work.existing.judgments}
    linked = _linked_segments(work)
    needs_new = False
    for segment in plan.segments:
        if segment.key in linked:
            continue
        candidates = [e for e in work.candidate_events.values()
                      if segment.key in e.segment_keys and (segment.key, e.event_id) not in judged]
        exact = _exact_match(work, segment, candidates, settings)
        if exact is not None:
            work.judgments.append(exact)
            continue
        if candidates:
            ordered = sorted(candidates, key=lambda e: (e.source != retrieval.PREVIOUS_REVISION,
                                                        -e.best_fused_score, e.event_id))
            work.pairs[segment.key] = [e.event_id for e in ordered]
        elif not _origin_events(work, segment.key):
            needs_new = True
    work.route = "compare" if work.pairs else ("propose" if needs_new else "resolve")
    work.stage_status["route"] = work.route
    return work.route


def _linked_segments(work: RecordWork) -> set[str]:
    return {m["segment_key"] for m in work.existing.memberships
            if m["state"] == "active" and m["relation"] in SAME_EVENT_LABELS and m["event_status"] != "superseded"}


def _origin_events(work: RecordWork, segment_key: str) -> list[dict]:
    return [m for m in work.existing.memberships
            if m["segment_key"] == segment_key and m["relation"] == "origin" and m["event_status"] != "superseded"]


def _exact_match(work: RecordWork, segment: SegmentDraft, candidates: list[CandidateEvent],
                 settings: SimilaritySettings) -> Judgment | None:
    """Identical substantive text in a compatible context: repeated information, no model call.

    Compatible context = the candidate passed retrieval (same partition, shared
    company, inside the time window), both sides have full text (not headline or
    summary only), and the identical segment has at least
    SIMILARITY_EXACT_MATCH_MIN_CHARS characters after boilerplate was removed.
    """
    plan = work.plan
    if plan.evidence_level != "full" or not segment.unit_keys:
        return None
    first = plan.unit(segment.unit_keys[0])
    digest, chars = first.location.get("segment_hash"), first.location.get("segment_chars") or 0
    if not digest or chars < settings.exact_match_min_chars:
        return None
    for event in sorted(candidates, key=lambda e: (-e.best_fused_score, e.event_id)):
        for passage in [*event.passages, *event.origin_passages]:
            loc = passage.location
            if (loc.get("segment_hash") == digest and loc.get("evidence_level") == "full"
                    and (loc.get("segment_chars") or 0) >= settings.exact_match_min_chars):
                return Judgment(
                    segment_key=segment.key, event_id=event.event_id, label="same_event_repeated",
                    method="exact_content", incoming_unit_keys=list(segment.unit_keys),
                    candidate_unit_ids=[passage.unit_id], candidate_revision_ids=[passage.revision_id],
                    matching_facts=["identical substantive text (whitespace-normalized)"],
                    evidence={"incoming": [{"ref": first.key, "quote": first.passage[:160]}],
                              "candidate": [{"ref": passage.unit_id, "quote": passage.passage[:160]}]},
                    explanation=(f"The {chars}-character segment is identical to a passage already supporting "
                                 "this event; accepted without a model call."),
                    prompt_version=None, model=None)
    return None


# ----------------------------------------------------------------------------- node: propose_new_event
def propose_new_event(work: RecordWork, ctx: PipelineContext) -> None:
    """No plausible candidate: propose provisional events without calling the comparison model."""
    for segment in _segments_needing_event(work):
        work.new_events.append(_draft_event(work, segment, reason="no_plausible_candidates"))
    work.stage_status["propose"] = f"{len(work.new_events)} provisional event(s)"


def _segments_needing_event(work: RecordWork) -> list[SegmentDraft]:
    linked = _linked_segments(work) | {j.segment_key for j in work.judgments if j.label in SAME_EVENT_LABELS}
    return [s for s in work.plan.segments
            if s.key not in linked and not _origin_events(work, s.key)
            and not any(k == s.key for k, _ in work.unevaluated)
            and s.key not in work.pairs]


def _draft_event(work: RecordWork, segment: SegmentDraft, *, reason: str) -> NewEventDraft:
    subject = work.subject
    if subject.record_type == "sec_filing":
        issuer = subject.issuer_name or f"CIK {subject.cik}"
        title = f"{issuer} {subject.form or '8-K'} {segment.label} (filed {subject.filing_date or 'unknown'})"
    else:
        title = (subject.title or "").strip() or "Untitled article"
    facts: dict[str, Any] = {}
    described = work.segment_events.get(segment.key)
    if described:
        model_title, model_facts = described
        if any(v not in (None, "", [], "unknown") for v in model_facts.values()):
            facts = {**model_facts, "described_by": "comparison_model"}
        generic = {"", "unknown", "article", segment.label.strip().lower()}
        if model_title and model_title.strip().lower() not in generic:
            title = model_title.strip()
    flags = [reason]
    if work.plan.evidence_level != "full":
        flags.append("limited_evidence")
    if work.coverage and not work.coverage.get("complete", True):
        flags.append("index_coverage_partial")
    if not work.company_ids:
        flags.append("no_company_link")
    return NewEventDraft(key=f"new:{segment.key}", segment_key=segment.key, title=title, facts=facts,
                         flags=flags, source_time=work.anchor_time)


# ----------------------------------------------------------------------------- node: compare_with_llm
def compare_with_llm(work: RecordWork, ctx: PipelineContext) -> None:
    runtime = ctx.analyzer
    settings = runtime.settings
    requests = build_requests(work, settings)
    tokens_used = 0
    stop_reason: str | None = None
    stopped_by_error = False
    for request, pairs in requests:
        if stop_reason is None and work.llm_calls >= settings.llm_max_calls_per_record:
            stop_reason = f"per-record call budget reached ({settings.llm_max_calls_per_record})"
        if stop_reason is None and tokens_used >= settings.llm_token_budget_per_record:
            stop_reason = f"per-record token budget reached ({settings.llm_token_budget_per_record})"
        if stop_reason is not None:
            work.unevaluated.extend(pairs)
            if stopped_by_error:
                work.error_pairs.extend(pairs)
            continue
        try:
            call = runtime.comparator.compare(request)
        except ComparisonError as error:
            work.llm_calls += 1
            work.unevaluated.extend(pairs)
            work.error_pairs.extend(pairs)
            work.comparison_errors.append({"kind": error.kind, "message": str(error), "retryable": error.retryable})
            work.note_error("comparison", error, retryable=error.retryable)
            if not error.retryable or error.kind in ("authentication", "model_not_found"):
                stop_reason, stopped_by_error = f"comparison backend error: {error.kind}", True
            continue
        except Exception as error:
            LOGGER.exception("comparison failed for %s", work.subject.label)
            work.llm_calls += 1
            work.unevaluated.extend(pairs)
            work.error_pairs.extend(pairs)
            work.comparison_errors.append({"kind": type(error).__name__, "message": str(error), "retryable": True})
            work.note_error("comparison", error)
            continue
        work.llm_calls += 1
        tokens_used += int(call.usage.get("input_tokens", 0)) + int(call.usage.get("output_tokens", 0))
        _add_usage(work, call)
        validated = validate_output(request, call.output, model=call.model, usage=call.usage)
        work.judgments.extend(validated.judgments)
        work.unevaluated.extend(validated.missing_pairs)
        work.segment_events.update({k: v for k, v in validated.segment_events.items() if k not in work.segment_events})
        work.limitations.extend(f"comparison: {note}" for note in validated.notes)
        if validated.missing_pairs:
            work.limitations.append(f"comparison: {len(validated.missing_pairs)} requested pair(s) were not judged")
        for judgment in validated.judgments:
            work.limitations.extend(f"comparison {judgment.segment_key}/{judgment.event_id[:8]}: {n}"
                                    for n in judgment.validation_notes)
    if stop_reason:
        work.limitations.append(stop_reason)
    work.stage_status["comparison"] = (f"{work.llm_calls} call(s), {len(work.judgments)} judgment(s), "
                                       f"{len(work.unevaluated)} unevaluated pair(s)")


def _add_usage(work: RecordWork, call) -> None:
    usage = work.usage.setdefault("llm", {"calls": 0, "input_tokens": 0, "output_tokens": 0,
                                          "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0,
                                          "models": [], "request_ids": []})
    usage["calls"] += 1
    for key in ("input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"):
        usage[key] += int(call.usage.get(key, 0) or 0)
    if call.model and call.model not in usage["models"]:
        usage["models"].append(call.model)
    if call.request_id:
        usage["request_ids"].append(call.request_id)


def _source_label(passage: CandidatePassage) -> str:
    kind = "SEC filing (primary document)" if passage.record_type == "sec_filing" else "news article"
    when = passage.source_time.isoformat() if passage.source_time else "unknown time"
    return f"{kind}, {passage.publisher or 'unknown source'}, {when}"


def build_requests(work: RecordWork, settings: SimilaritySettings) -> list[tuple[ComparisonRequest, list]]:
    """Split the (segment, event) pairs into bounded requests.

    At most SIMILARITY_LLM_MAX_EVENTS_PER_CALL events per request, and the
    rendered prompt stays under SIMILARITY_LLM_MAX_INPUT_CHARS. Incoming units
    that do not fit are listed as omitted in the prompt and in the analysis
    limitations, never dropped silently.
    """
    plan = work.plan
    event_order: list[str] = []
    for event_ids in work.pairs.values():
        event_order.extend(e for e in event_ids if e not in event_order)
    budget = settings.llm_max_input_chars
    incoming_budget = int(budget * 0.45)
    requests: list[tuple[ComparisonRequest, list]] = []
    remaining = list(event_order)
    while remaining:
        batch: list[str] = []
        used = 0
        for event_id in list(remaining):
            cost = _event_chars(work.candidate_events[event_id])
            if batch and (len(batch) >= settings.llm_max_events_per_call or used + cost > budget - incoming_budget):
                break
            batch.append(event_id)
            used += cost
            remaining.remove(event_id)
        segments = [s for s in plan.segments if any(e in batch for e in work.pairs.get(s.key, []))]
        requests.append(_request(work, segments, batch, incoming_budget))
    return requests


def _event_chars(event: CandidateEvent) -> int:
    texts = [p.text for p in [*event.origin_passages, *event.passages]]
    return sum(len(t) for t in texts) + len(event.title) + 400


def _request(work: RecordWork, segments: list[SegmentDraft], event_ids: list[str],
             incoming_budget: int) -> tuple[ComparisonRequest, list]:
    plan = work.plan
    subject = work.subject
    hit_units = {h.incoming_unit_key for h in work.hits if any(e in event_ids for e in h.candidate.event_ids)}
    prompt_segments: list[PromptSegment] = []
    unit_number = 0
    per_segment = max(incoming_budget // max(len(segments), 1), 1500)
    for number, segment in enumerate(segments, start=1):
        units = [plan.unit(k) for k in segment.unit_keys]
        ordered = sorted(units, key=lambda u: (u.key not in hit_units, u.ordinal))
        chosen, omitted, size = [], [], 0
        for unit in ordered:
            if chosen and size + len(unit.text) > per_segment:
                omitted.append(unit.key)
                continue
            chosen.append(unit)
            size += len(unit.text)
        chosen.sort(key=lambda u: u.ordinal)
        prompt_units = []
        for unit in chosen:
            unit_number += 1
            prompt_units.append(PromptUnit(f"U{unit_number}", unit.key, unit.text))
        if omitted:
            work.limitations.append(f"segment {segment.key}: {len(omitted)} of {len(units)} unit(s) omitted from "
                                    "the comparison prompt to fit SIMILARITY_LLM_MAX_INPUT_CHARS")
        prompt_segments.append(PromptSegment(f"S{number}", segment.key, segment.label, prompt_units, omitted))
    passage_number = 0
    prompt_events: list[PromptEvent] = []
    for number, event_id in enumerate(event_ids, start=1):
        event = work.candidate_events[event_id]
        passages: list[PromptPassage] = []
        seen: set[str] = set()
        for passage in [*event.origin_passages, *event.passages]:
            if passage.unit_id in seen:
                continue
            seen.add(passage.unit_id)
            passage_number += 1
            passages.append(PromptPassage(f"C{passage_number}", passage.unit_id, passage.revision_id, passage.text,
                                          _source_label(passage)))
        relation = ("earlier revision of this same article (not an independent source)"
                    if event.source == retrieval.PREVIOUS_REVISION else "independent source")
        prompt_events.append(PromptEvent(f"E{number}", event.event_id, event.title or "untitled event",
                                         event.status or "unknown", event.facts, event.member_documents,
                                         event.first_source_time, relation, passages))
    ref_of_event = {e.event_id: e.ref for e in prompt_events}
    pairs: dict[str, list[str]] = {}
    pair_keys: list[tuple[str, str]] = []
    for prompt_segment in prompt_segments:
        wanted = [e for e in work.pairs.get(prompt_segment.segment_key, []) if e in ref_of_event]
        pairs[prompt_segment.ref] = [ref_of_event[e] for e in wanted]
        pair_keys.extend((prompt_segment.segment_key, e) for e in wanted)
    anchor = work.anchor_time.isoformat() if work.anchor_time else "unknown"
    evidence = "full text" if plan.evidence_level == "full" else f"limited: {plan.evidence_level}"
    source = (subject.issuer_name or subject.publisher or subject.provider) if subject.record_type == "sec_filing" \
        else (subject.publisher or subject.provider)
    incoming = {"record_type": subject.record_type, "source": source, "published": anchor, "evidence": evidence,
                "title": None if subject.title_is_generated else subject.title}
    return ComparisonRequest(incoming, prompt_segments, prompt_events, pairs), pair_keys


# ----------------------------------------------------------------------------- node: resolve_event_memberships
def resolve(work: RecordWork, ctx: PipelineContext) -> None:
    if work.replay or work.status == "disabled":
        return
    if work.status == "skipped":
        work.stage_status["resolution"] = "skipped"
        return
    if work.errors and work.plan is None:
        work.status = "pending"
        return
    plan = work.plan
    if work.embedding_error or work.retrieval_error or any(e["stage"] == "text_units" for e in work.errors):
        work.status = "pending"
        work.segment_outcomes = [SegmentOutcome(s.key, "unresolved", "analysis stage failed") for s in plan.segments]
        work.stage_status["resolution"] = "deferred: an earlier stage failed"
        return
    if ctx.index_only:
        work.status = "pending"
        work.segment_outcomes = [SegmentOutcome(s.key, "unresolved", "awaiting grouping") for s in plan.segments]
        work.stage_status["grouping"] = "awaiting: indexed by the bootstrap, grouped in its second pass"
        return

    by_segment: dict[str, list[Judgment]] = {}
    for judgment in work.judgments:
        by_segment.setdefault(judgment.segment_key, []).append(judgment)
    unevaluated = {(s, e) for s, e in work.unevaluated}
    failed_pairs = {(s, e) for s, e in work.error_pairs}
    failed_segments = set()
    drafts = {d.segment_key: d for d in work.new_events}
    memberships: list[MembershipDraft] = []
    for segment in plan.segments:
        judgments = by_segment.get(segment.key, [])
        same = [j for j in judgments if j.label in SAME_EVENT_LABELS]
        for judgment in same:
            memberships.append(MembershipDraft(judgment.event_id, segment.key, judgment.label,
                                               judgment.incoming_unit_keys or list(segment.unit_keys), judgment))
        for judgment in (j for j in judgments if j.label == "uncertain"):
            memberships.append(MembershipDraft(judgment.event_id, segment.key, "uncertain",
                                               judgment.incoming_unit_keys or list(segment.unit_keys), judgment))
        if same or segment.key in _linked_segments(work):
            detail = ", ".join(f"{j.label}->{j.event_id[:8]}" for j in same) or "linked in an earlier attempt"
            work.segment_outcomes.append(SegmentOutcome(segment.key, "linked", detail))
            for origin in _origin_events(work, segment.key):
                if same and int(origin["documents"]) <= 1:
                    work.supersede.append((origin["event_id"], same[0].event_id))
            continue
        pending_pairs = [p for p in unevaluated if p[0] == segment.key]
        if pending_pairs:
            if any(p in failed_pairs for p in pending_pairs):
                failed_segments.add(segment.key)  # a model call failed: retry after the outage
            work.segment_outcomes.append(SegmentOutcome(segment.key, "unresolved",
                                                        f"{len(pending_pairs)} candidate event(s) not evaluated"))
            continue
        if _origin_events(work, segment.key):
            work.segment_outcomes.append(SegmentOutcome(segment.key, "kept_existing", "provisional event kept"))
            continue
        draft = drafts.get(segment.key)
        if draft is None:
            reason = "compared_no_match" if judgments else "no_plausible_candidates"
            draft = _draft_event(work, segment, reason=reason)
            if any(j.label == "uncertain" for j in judgments):
                draft.flags.append("uncertain_relations_pending")
            work.new_events.append(draft)
            drafts[segment.key] = draft
        memberships.append(MembershipDraft(draft.key, segment.key, "origin", list(segment.unit_keys)))
        status = "pending_relation" if any(j.label == "uncertain" for j in judgments) else "new_event"
        work.segment_outcomes.append(SegmentOutcome(segment.key, status, draft.title[:80]))
    work.memberships = _collapse(memberships)

    unresolved = [o for o in work.segment_outcomes if o.status == "unresolved"]
    if failed_segments:
        work.status = "pending"
    elif unresolved:
        work.status = "partial"
    elif work.coverage and not work.coverage.get("complete", True):
        work.status = "partial"
    else:
        work.status = "completed"
    work.stage_status["resolution"] = (f"{len(work.memberships)} membership(s), {len(work.new_events)} new "
                                       f"event(s), {len(unresolved)} unresolved segment(s)")


def _collapse(memberships: list[MembershipDraft]) -> list[MembershipDraft]:
    """One membership per event: overlapping chunks and several segments never duplicate it."""
    merged: dict[str, MembershipDraft] = {}
    for membership in memberships:
        current = merged.get(membership.event_ref)
        if current is None:
            merged[membership.event_ref] = MembershipDraft(membership.event_ref, membership.segment_key,
                                                           membership.relation, list(membership.unit_keys),
                                                           membership.judgment)
            continue
        units = list(dict.fromkeys([*current.unit_keys, *membership.unit_keys]))
        if RELATION_RANK[membership.relation] > RELATION_RANK[current.relation]:
            current.relation, current.judgment, current.segment_key = (membership.relation, membership.judgment,
                                                                       membership.segment_key)
        current.unit_keys = units
    return list(merged.values())


# ----------------------------------------------------------------------------- node: persist_result
def persist(work: RecordWork, ctx: PipelineContext) -> RecordOutcome:
    outcome = RecordOutcome(index=work.index, record=work.record, revision_id=work.subject.revision_id,
                            document_id=work.subject.document_id)
    if ctx.database is None or not ctx.persist:
        outcome.analysis_status = "disabled"
        return outcome
    runtime = ctx.analyzer
    summary: dict[str, Any] = {}
    try:
        with ctx.database.transaction() as conn:
            if work.mode == "ingest":
                stored = persist_record(conn, work.record, raw_payload=work.raw_payload,
                                        raw_document=work.raw_document)
                work.subject.document_id, work.subject.revision_id = stored.document_id, stored.revision_id
                outcome.persist_outcome = stored.outcome
                outcome.unresolved_symbols = list(stored.unresolved_symbols)
            if work.analysis_enabled and not work.replay:
                summary = store.persist_analysis(
                    conn, work, profile_id=runtime.profile_id, pipeline_version=runtime.pipeline_version,
                    comparison_model=runtime.comparison_model, prompt_version=PROMPT_VERSION,
                    max_attempts=runtime.settings.max_analysis_attempts,
                    retry_base_minutes=runtime.settings.retry_base_minutes, now=ctx.clock(),
                    count_attempt=not ctx.index_only, due_now=ctx.index_only)
    except Exception as error:
        message = sanitize(str(error))[:500]
        LOGGER.warning("persist failed for %s: %s", work.subject.label, message)
        outcome.error = {"stage": "persist", "error_type": type(error).__name__, "message": message,
                         "retryable": True}
        outcome.analysis_status = work.status or "pending"
        return outcome
    outcome.persisted = True
    outcome.revision_id, outcome.document_id = work.subject.revision_id, work.subject.document_id
    outcome.analysis_status = ("reused:" + (work.status or "unknown")) if work.replay else (work.status or "disabled")
    outcome.analysis = {
        "route": work.route, "llm_calls": work.llm_calls, "stage_status": work.stage_status,
        "segments": [o.__dict__ for o in work.segment_outcomes], "limitations": list(dict.fromkeys(work.limitations)),
        "errors": work.errors, **{k: v for k, v in summary.items() if k != "event_ids"},
        "event_ids": summary.get("event_ids", []),
    }
    return outcome
