"""Ingestion pipeline: SEC EDGAR 8-K filings + Alpaca news -> LangGraph preparation,
semantic comparison, event grouping and PostgreSQL persistence.

Layout (collection, downloads and file I/O live outside the graph):

    settings.py   environment-driven configuration (live vs demo roots)
    schema.py     Pydantic models: input envelope + versioned common output record
    htmltext.py   deterministic HTML -> readable text (SEC documents, Alpaca content)
    adapters.py   provider adapters + unwrapping of app wrappers / raw payloads
    graph.py      StateGraph: prepare_data -> text units -> embeddings -> retrieval ->
                  route -> (propose_new_event | compare_with_llm) -> resolve ->
                  persist_result (compiled once; analysis nodes in datasources.similarity)
    sec_client.py rate-limited, retrying, host-validated SEC HTTP client
    alpaca_rest.py paginated Alpaca news REST backfill
    store.py      raw / processed / error file layout with atomic writes
    ledger.py     source-identity duplicate tracking + startup reconciliation
    service.py    pollers, WebSocket consumer, bounded queue, single writer
    cli.py        `datasources-ingest` entry point
"""
