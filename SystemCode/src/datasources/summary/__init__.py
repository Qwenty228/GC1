"""Event summaries: one cited summary per grouped event, run after grouping is committed.

    settings.py    SUMMARY_* configuration (off unless SUMMARY_ENABLED=true)
    models.py      evidence, output and per-event task state
    evidence.py    bounded, deterministic evidence from active memberships; fingerprints
    generation.py  prompt, structured-output schema, model backends
    validation.py  traceability checks before publication
    store.py       SQL: durable jobs and leases, attempts, atomic publication, reviews
    pipeline.py    the steps behind the graph's summary nodes (and the CLI)
    review.py      conflict review decisions and membership corrections
    read.py        read-only queries for the API
    cli.py         `datasources-summary` commands

Nothing here imports anthropic at import time, and nothing calls a model
unless summarization is enabled or a CLI command is run with
--confirm-paid-calls.
"""
