"""LangGraph ingestion graph: preparation, semantic comparison, event grouping, persistence.

    START -> prepare_data --(no valid record)--> END
                 |
                 v
    create_text_units -> generate_embeddings -> retrieve_candidates -> route_candidates
        route_candidates --(no plausible candidate)----> propose_new_event ---------+
        route_candidates --(plausible candidates)------> compare_with_llm ----------+
        route_candidates --(nothing to decide: analysis                             |
                            disabled / skipped / failed /                           v
                            reused / exact-content match)-----------> resolve_event_memberships
    resolve_event_memberships -> persist_result --(more records in the batch)--> create_text_units
                                                --(done)--> END

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
from .adapters import ADAPTERS, AdapterError
from .schema import PreparedRecord, RecordError, SourceEnvelope

LOGGER = logging.getLogger(__name__)
NODES = ("prepare_data", "create_text_units", "generate_embeddings", "retrieve_candidates", "route_candidates",
         "propose_new_event", "compare_with_llm", "resolve_event_memberships", "persist_result")


class PipelineState(TypedDict, total=False):
    envelopes: list[SourceEnvelope | dict[str, Any]]  # ingest mode
    revision_ids: list[str]  # reanalysis mode (retry / backfill of stored revisions)
    items: list[Any]
    errors: list[RecordError]
    cursor: int
    current: RecordWork | None
    outcomes: list[RecordOutcome]


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
    return {"outcomes": [*state.get("outcomes", []), outcome], "cursor": state["cursor"] + 1, "current": None}


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


def _after_persist(state: PipelineState) -> str:
    return "create_text_units" if state["cursor"] < len(state.get("items", [])) else END


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
    builder.add_conditional_edges("persist_result", _after_persist, {"create_text_units": "create_text_units", END: END})
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
    result = get_graph().invoke(state, context=context, config={"recursion_limit": 20 + 12 * max(size, 1)})
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
