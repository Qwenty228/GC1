"""LangGraph preprocessing graph: START -> prepare_data -> END.

The graph is deterministic: no LLM, no chat-message state. It receives explicit
`SourceEnvelope`s, validates each one, selects the adapter from the envelope's
``provider`` and returns prepared records plus per-record errors. A malformed
record never discards the rest of its batch.

Collection, downloads, duplicate tracking and file writing all happen outside
the graph (see ``service.py``). The compiled graph is built once and reused.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from pydantic import ValidationError

from .adapters import ADAPTERS, AdapterError
from .schema import PreparedRecord, RecordError, SourceEnvelope

LOGGER = logging.getLogger(__name__)


class PrepareState(TypedDict, total=False):
    envelopes: list[SourceEnvelope | dict[str, Any]]
    records: list[PreparedRecord]
    errors: list[RecordError]


def _source_id(candidate: Any) -> str | None:
    raw = candidate.raw_record if isinstance(candidate, SourceEnvelope) else (candidate or {}).get("raw_record")
    if isinstance(raw, dict):
        value = raw.get("accessionNumber") or raw.get("id")
        return None if value is None else str(value)
    return None


def prepare_data(state: PrepareState) -> PrepareState:
    records: list[PreparedRecord] = []
    errors: list[RecordError] = []
    for index, candidate in enumerate(state.get("envelopes", [])):
        provider = candidate.provider if isinstance(candidate, SourceEnvelope) else (
            candidate.get("provider") if isinstance(candidate, dict) else None)
        try:
            envelope = candidate if isinstance(candidate, SourceEnvelope) else SourceEnvelope.model_validate(candidate)
            adapter = ADAPTERS[envelope.provider]
            record = adapter(envelope)
            records.append(PreparedRecord.model_validate(record.model_dump()))
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
    return {"records": records, "errors": errors}


def build_graph():
    builder = StateGraph(PrepareState)
    builder.add_node("prepare_data", prepare_data)
    builder.add_edge(START, "prepare_data")
    builder.add_edge("prepare_data", END)
    return builder.compile()


@lru_cache(maxsize=1)
def get_graph():
    """Compiled graph, built once per process and reused."""
    return build_graph()


def run_prepare(envelopes: list[SourceEnvelope | dict[str, Any]]) -> tuple[list[PreparedRecord], list[RecordError]]:
    result = get_graph().invoke({"envelopes": list(envelopes), "records": [], "errors": []})
    return result.get("records", []), result.get("errors", [])
