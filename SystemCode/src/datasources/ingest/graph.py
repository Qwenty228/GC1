"""LangGraph ingestion graph: preparation, semantic comparison, event grouping, persistence, summaries.

    START -> prepare_data --(no valid record)--> END
                 |
                 v
    create_text_units -> generate_embeddings -> retrieve_candidates -> route_candidates
        route_candidates --(no plausible candidate)----> propose_new_event ---------+
        route_candidates --(plausible candidates)------> compare_with_llm ----------+
        route_candidates --(nothing to decide: analysis                             |
                            disabled / skipped / failed /                           v
                            reused / exact-content match)-----------> resolve_event_memberships
    resolve_event_memberships -> persist_result -> route_event_summary
        route_event_summary --(generate)--> generate_event_summary -> validate_event_summary
                                                -> persist_event_summary -> route_event_summary
        route_event_summary --(open conflict)--> flag_conflict_review --> route_event_summary
        route_event_summary --(nothing new)----> skip_summary ----------> route_event_summary
        route_event_summary --(every affected event done; more records)--> create_text_units
                            --(every affected event done; last record)--> END

* ``route_event_summary`` runs once per event the committed record is linked
  to (a multi-Item filing can touch several), then moves on. The record and
  its grouping are already committed, so a summary failure never rolls them
  back; unfinished summary work stays in ``event_summary_job``. With
  summarization disabled (no ``summarizer`` in the context) it has no events to
  route and passes straight to the next record.

* ``prepare_data`` validates each ``SourceEnvelope`` (or loads a stored revision
  for retry/backfill) and isolates per-record errors: a malformed record never
  discards the rest of its batch.
* Records are analysed ONE AT A TIME. ``persist_result`` commits a record before
  the next one is analysed, so the next record's retrieval already sees it.
* Retrieval and comparison read previously committed rows only. On the normal
  path the incoming document, revision, text units, embeddings and event
  decisions are all inserted by ``persist_result`` in one transaction. No
  transaction is open while a model runs.
* With similarity disabled (no ``analyzer`` in the context) the analysis nodes
  pass the record through and ``persist_result`` stores it exactly as before
  (or does nothing in file-only mode); the analysis is reported as ``disabled``.

Collection, source-identity checks, raw payload capture, SEC downloads, the
processed file and the ledger all stay outside the graph (``service.py``).
Dependencies (database, embedding model, comparison backend) arrive through
LangGraph's runtime context, so the graph is compiled once and reused.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime
from pydantic import ValidationError

from ..similarity import pipeline
from ..similarity.models import RecordWork
from ..similarity.pipeline import PipelineContext, RecordOutcome, WorkInput
from ..summary import pipeline as summaries
from ..summary.models import CONFLICT, GENERATE, SummaryTask
from .adapters import ADAPTERS, AdapterError
from .schema import PreparedRecord, RecordError, SourceEnvelope

LOGGER = logging.getLogger(__name__)
NODES = ("prepare_data", "create_text_units", "generate_embeddings", "retrieve_candidates", "route_candidates",
         "propose_new_event", "compare_with_llm", "resolve_event_memberships", "persist_result",
         "route_event_summary", "generate_event_summary", "validate_event_summary", "persist_event_summary",
         "flag_conflict_review", "skip_summary")


class PipelineState(TypedDict, total=False):
    envelopes: list[SourceEnvelope | dict[str, Any]]  # ingest mode
    revision_ids: list[str]  # reanalysis mode (retry / backfill of stored revisions)
    items: list[Any]
    errors: list[RecordError]
    cursor: int
    current: RecordWork | None
    outcomes: list[RecordOutcome]
    summary_tasks: list[SummaryTask] | None  # events of the last committed record still to route (None = not read)
    summary_task: SummaryTask | None


def _ctx(runtime: Runtime[PipelineContext] | None) -> PipelineContext:
    context = getattr(runtime, "context", None) if runtime is not None else None
    return context if context is not None else PipelineContext(persist=False)


def _source_id(candidate: Any) -> str | None:
    raw = candidate.raw_record if isinstance(candidate, SourceEnvelope) else (candidate or {}).get("raw_record")
    if isinstance(raw, dict):
        value = raw.get("accessionNumber") or raw.get("id")
        return None if value is None else str(value)
    return None


# ----------------------------------------------------------------------------- nodes
def prepare_data(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    ctx = _ctx(runtime)
    if state.get("revision_ids") is not None:
        return _prepare_stored(state["revision_ids"], ctx)
    items: list[WorkInput] = []
    errors: list[RecordError] = []
    for index, candidate in enumerate(state.get("envelopes", [])):
        provider = candidate.provider if isinstance(candidate, SourceEnvelope) else (
            candidate.get("provider") if isinstance(candidate, dict) else None)
        try:
            envelope = candidate if isinstance(candidate, SourceEnvelope) else SourceEnvelope.model_validate(candidate)
            adapter = ADAPTERS[envelope.provider]
            record = PreparedRecord.model_validate(adapter(envelope).model_dump())
            items.append(WorkInput(index, record, envelope.raw_record, envelope.primary_document_html))
        except AdapterError as error:
            errors.append(RecordError(index=index, provider=provider, source_record_id=error.source_record_id or _source_id(candidate),
                                      stage=error.stage, error_type=type(error).__name__, message=str(error),
                                      retryable=error.retryable))
        except ValidationError as error:
            errors.append(RecordError(index=index, provider=provider, source_record_id=_source_id(candidate),
                                      stage="validate", error_type="ValidationError",
                                      message=str(error), retryable=False))
        except Exception as error:  # defensive: isolate unexpected adapter bugs per record
            LOGGER.exception("unexpected error preparing record %d", index)
            errors.append(RecordError(index=index, provider=provider, source_record_id=_source_id(candidate),
                                      stage="prepare", error_type=type(error).__name__,
                                      message=str(error), retryable=True))
    return {"items": items, "errors": errors, "cursor": 0, "outcomes": [], "current": None}


def _prepare_stored(revision_ids: list[str], ctx: PipelineContext) -> dict:
    from ..similarity import store

    items: list[Any] = []
    errors: list[RecordError] = []
    if ctx.database is None or ctx.analyzer is None:
        raise ValueError("reanalysis needs a database and an enabled similarity runtime")
    for index, revision_id in enumerate(revision_ids):
        try:
            with ctx.database.connection() as conn:
                subject = store.load_subject(conn, revision_id)
        except Exception as error:
            errors.append(RecordError(index=index, provider=None, source_record_id=revision_id, stage="load",
                                      error_type=type(error).__name__, message=str(error), retryable=True))
            continue
        if subject is None:
            errors.append(RecordError(index=index, provider=None, source_record_id=revision_id, stage="load",
                                      error_type="NotFound", message="revision not found", retryable=False))
        else:
            items.append(subject)
    return {"items": items, "errors": errors, "cursor": 0, "outcomes": [], "current": None}


def create_text_units(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    ctx = _ctx(runtime)
    cursor = state["cursor"]
    work = pipeline.start(state["items"][cursor], cursor)
    pipeline.create_text_units(work, ctx)
    return {"current": work}


def generate_embeddings(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    work = state["current"]
    pipeline.generate_embeddings(work, _ctx(runtime))
    return {"current": work}


def retrieve_candidates(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    work = state["current"]
    pipeline.retrieve_candidates(work, _ctx(runtime))
    return {"current": work}


def route_candidates(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    work = state["current"]
    pipeline.route(work, _ctx(runtime))
    return {"current": work}


def propose_new_event(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    work = state["current"]
    pipeline.propose_new_event(work, _ctx(runtime))
    return {"current": work}


def compare_with_llm(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    work = state["current"]
    pipeline.compare_with_llm(work, _ctx(runtime))
    return {"current": work}


def resolve_event_memberships(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    work = state["current"]
    pipeline.resolve(work, _ctx(runtime))
    return {"current": work}


def persist_result(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    work = state["current"]
    outcome = pipeline.persist(work, _ctx(runtime))
    return {"outcomes": [*state.get("outcomes", []), outcome], "cursor": state["cursor"] + 1, "current": None,
            "summary_tasks": None, "summary_task": None}


# ----------------------------------------------------------------------------- summary nodes
def _summary_ctx(runtime: Runtime[PipelineContext] | None):
    return summaries.context_from(_ctx(runtime))


def route_event_summary(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    ctx = _ctx(runtime)
    sctx = summaries.context_from(ctx)
    tasks = state.get("summary_tasks")
    if tasks is None:
        outcome = state["outcomes"][-1] if state.get("outcomes") else None
        analyzer = ctx.analyzer
        tasks = summaries.targets(outcome, sctx, profile_id=getattr(analyzer, "profile_id", None),
                                  pipeline_version=getattr(analyzer, "pipeline_version", None))
    if not tasks:
        return {"summary_tasks": [], "summary_task": None}
    task, rest = tasks[0], list(tasks[1:])
    summaries.route(task, sctx)
    return {"summary_tasks": rest, "summary_task": task}


def _report(state: PipelineState, task: SummaryTask) -> None:
    if state.get("outcomes"):
        state["outcomes"][-1].summaries.append(task.report())


def generate_event_summary(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    task = state["summary_task"]
    summaries.generate(task, _summary_ctx(runtime))
    return {"summary_task": task}


def validate_event_summary(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    task = state["summary_task"]
    summaries.validate(task)
    return {"summary_task": task}


def persist_event_summary(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    task = state["summary_task"]
    summaries.persist(task, _summary_ctx(runtime))
    _report(state, task)
    return {"summary_task": None}


def flag_conflict_review(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    task = state["summary_task"]
    summaries.flag_conflict_review(task, _summary_ctx(runtime))
    _report(state, task)
    return {"summary_task": None}


def skip_summary(state: PipelineState, runtime: Runtime[PipelineContext]) -> dict:
    task = state["summary_task"]
    summaries.skip_summary(task)
    _report(state, task)
    return {"summary_task": None}


# ----------------------------------------------------------------------------- edges
def _after_prepare(state: PipelineState) -> str:
    return "create_text_units" if state.get("items") else END


def _after_route(state: PipelineState) -> str:
    route = state["current"].route
    if route == "compare":
        return "compare_with_llm"
    if route == "propose":
        return "propose_new_event"
    return "resolve_event_memberships"


def _next_record(state: PipelineState) -> str:
    return "create_text_units" if state["cursor"] < len(state.get("items", [])) else END


def _after_summary_route(state: PipelineState) -> str:
    task = state.get("summary_task")
    if task is None:
        return _next_record(state)
    if task.route == GENERATE:
        return "generate_event_summary"
    if task.route == CONFLICT:
        return "flag_conflict_review"
    return "skip_summary"


def build_graph():
    builder = StateGraph(PipelineState, context_schema=PipelineContext)
    builder.add_node("prepare_data", prepare_data)
    builder.add_node("create_text_units", create_text_units)
    builder.add_node("generate_embeddings", generate_embeddings)
    builder.add_node("retrieve_candidates", retrieve_candidates)
    builder.add_node("route_candidates", route_candidates)
    builder.add_node("propose_new_event", propose_new_event)
    builder.add_node("compare_with_llm", compare_with_llm)
    builder.add_node("resolve_event_memberships", resolve_event_memberships)
    builder.add_node("persist_result", persist_result)
    builder.add_node("route_event_summary", route_event_summary)
    builder.add_node("generate_event_summary", generate_event_summary)
    builder.add_node("validate_event_summary", validate_event_summary)
    builder.add_node("persist_event_summary", persist_event_summary)
    builder.add_node("flag_conflict_review", flag_conflict_review)
    builder.add_node("skip_summary", skip_summary)
    builder.add_edge(START, "prepare_data")
    builder.add_conditional_edges("prepare_data", _after_prepare, {"create_text_units": "create_text_units", END: END})
    builder.add_edge("create_text_units", "generate_embeddings")
    builder.add_edge("generate_embeddings", "retrieve_candidates")
    builder.add_edge("retrieve_candidates", "route_candidates")
    builder.add_conditional_edges("route_candidates", _after_route, {
        "compare_with_llm": "compare_with_llm",
        "propose_new_event": "propose_new_event",
        "resolve_event_memberships": "resolve_event_memberships",
    })
    builder.add_edge("propose_new_event", "resolve_event_memberships")
    builder.add_edge("compare_with_llm", "resolve_event_memberships")
    builder.add_edge("resolve_event_memberships", "persist_result")
    builder.add_edge("persist_result", "route_event_summary")
    builder.add_conditional_edges("route_event_summary", _after_summary_route, {
        "generate_event_summary": "generate_event_summary",
        "flag_conflict_review": "flag_conflict_review",
        "skip_summary": "skip_summary",
        "create_text_units": "create_text_units",
        END: END,
    })
    builder.add_edge("generate_event_summary", "validate_event_summary")
    builder.add_edge("validate_event_summary", "persist_event_summary")
    builder.add_edge("persist_event_summary", "route_event_summary")
    builder.add_edge("flag_conflict_review", "route_event_summary")
    builder.add_edge("skip_summary", "route_event_summary")
    return builder.compile()


@lru_cache(maxsize=1)
def get_graph():
    """Compiled graph, built once per process and reused."""
    return build_graph()


# ----------------------------------------------------------------------------- entry points
@dataclass
class PipelineResult:
    outcomes: list[RecordOutcome]
    errors: list[RecordError]

    @property
    def records(self) -> list[PreparedRecord]:
        return [o.record for o in self.outcomes if o.record is not None]


def _invoke(state: dict, context: PipelineContext, size: int) -> PipelineResult:
    # Per record: ~12 grouping steps, plus up to 4 summary steps per affected event
    # (at most summary.pipeline.MAX_EVENTS_PER_RECORD events inline).
    per_record = 14 + 4 * summaries.MAX_EVENTS_PER_RECORD
    result = get_graph().invoke(state, context=context, config={"recursion_limit": 20 + per_record * max(size, 1)})
    return PipelineResult(result.get("outcomes", []), result.get("errors", []))


def run_pipeline(envelopes: list[SourceEnvelope | dict[str, Any]], context: PipelineContext) -> PipelineResult:
    """Prepare, analyse and persist records, sequentially, through the compiled graph."""
    return _invoke({"envelopes": list(envelopes), "outcomes": [], "errors": []}, context, len(envelopes))


def run_reanalysis(revision_ids: list[str], context: PipelineContext) -> PipelineResult:
    """Re-run analysis for stored revisions (retry / backfill). Revisions are never rewritten."""
    return _invoke({"revision_ids": list(revision_ids), "outcomes": [], "errors": []}, context, len(revision_ids))


def run_prepare(envelopes: list[SourceEnvelope | dict[str, Any]]) -> tuple[list[PreparedRecord], list[RecordError]]:
    """Preparation only: no database, no analysis. Used by tools and tests."""
    result = run_pipeline(envelopes, PipelineContext(persist=False))
    return result.records, result.errors
