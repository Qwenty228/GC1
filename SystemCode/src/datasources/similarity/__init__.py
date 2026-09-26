"""Semantic news comparison and event grouping, run inside the ingestion graph.

    settings.py    SIMILARITY_* configuration (off unless SIMILARITY_ENABLED=true)
    models.py      in-memory state carried between graph nodes
    textunits.py   deterministic text units (sections, SEC Items, token-bounded chunks)
    embeddings.py  embedding backends (sentence-transformers, hashing) and profiles
    retrieval.py   company/time-scoped semantic + lexical retrieval, RRF fusion
    comparison.py  LLM comparison prompt, response schema, validation, backends
    pipeline.py    the analysis step behind each graph node
    store.py       SQL for units, embeddings, analyses, events and memberships
    lock.py        database-level single-writer lock
    cli.py         `datasources-similarity` maintenance commands
    evaluation.py  bounded evaluation of the real backends on labelled examples

Nothing here imports torch, sentence-transformers or anthropic at import time,
and no model is ever downloaded implicitly.
"""
