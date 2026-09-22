"""Ingestion pipeline: SEC EDGAR 8-K filings + Alpaca news -> LangGraph preprocessing.

Layout (collection and I/O live outside the graph):

    settings.py   environment-driven configuration (live vs demo roots)
    schema.py     Pydantic models: input envelope + versioned common output record
    htmltext.py   deterministic HTML -> readable text (SEC documents, Alpaca content)
    adapters.py   provider adapters + unwrapping of app wrappers / raw payloads
    graph.py      StateGraph: START -> prepare_data -> END (compiled once)
    sec_client.py rate-limited, retrying, host-validated SEC HTTP client
    alpaca_rest.py paginated Alpaca news REST backfill
    store.py      raw / processed / error file layout with atomic writes
    ledger.py     source-identity duplicate tracking + startup reconciliation
    service.py    pollers, WebSocket consumer, bounded queue, single writer
    cli.py        `datasources-ingest` entry point
"""
