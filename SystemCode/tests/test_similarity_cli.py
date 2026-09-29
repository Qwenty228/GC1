"""`datasources-similarity` commands against PostgreSQL, on sample data only.

The commands read their backends from the environment; these tests select the
deterministic hashing embedder and the mock comparison backend (which labels
every pair 'uncertain' and never links events), so nothing is downloaded and
no API is called. Skipped without a database.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from conftest import cleanup_sample_rows, isolated_test_database

pytest.importorskip("psycopg")

from datasources.ingest.graph import run_pipeline  # noqa: E402
from datasources.similarity import cli, store  # noqa: E402
from datasources.similarity.pipeline import PipelineContext  # noqa: E402
from test_similarity_pipeline import ACQ, ACQ_PARAPHRASE, OTHER, article, envelope  # noqa: E402

AT = datetime(2039, 6, 1, 12, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def db():
    database = isolated_test_database()  # refuses stock_news and anything not verified as a test DB
    with database.connection() as conn:
        if store.missing_schema(conn):
            database.close()
            pytest.skip("migration 002 not applied")
    cleanup_sample_rows(database)
    try:
        yield database
    finally:
        cleanup_sample_rows(database)
        database.close()


@pytest.fixture(autouse=True)
def offline_backends(monkeypatch):
    monkeypatch.setenv("SIMILARITY_EMBEDDING_BACKEND", "hashing")
    monkeypatch.setenv("SIMILARITY_HASHING_DIMENSIONS", "64")
    monkeypatch.setenv("SIMILARITY_COMPARISON_BACKEND", "mock")
    monkeypatch.setenv("SIMILARITY_MIN_SEMANTIC_SIMILARITY", "0.2")


def run(capsys, *argv) -> tuple[int, dict | None]:
    code = cli.main(list(argv))
    out = capsys.readouterr().out
    return code, (json.loads(out) if out.strip().startswith("{") else None)


def test_backfill_retry_status_and_inspect_on_sample_data(db, capsys):
    raws = [article(*ACQ, AT), article(*ACQ_PARAPHRASE, AT + timedelta(hours=1)),
            article(*OTHER, AT + timedelta(hours=2))]
    stored = [run_pipeline([envelope(r)], PipelineContext(database=db)).outcomes[0] for r in raws]

    code, summary = run(capsys, "backfill", "--sample", "--limit", "10")
    assert code == 0
    assert summary["selected"] == 3 and summary["index_pass"]["analysis_pending"] == 3
    assert summary["group_pass"]["analysis_completed"] == 3
    status = summary["status"]
    assert status["current_revisions_by_analysis_status"] == {"completed": 3}
    # The mock backend never links: every compared pair stays a pending 'uncertain' relation.
    assert status["memberships"].get("same_event_repeated:active") is None
    assert status["memberships"]["uncertain:pending"] >= 1 and status["events_by_status"]["provisional"] == 3

    code, again = run(capsys, "backfill", "--sample", "--limit", "10")  # idempotent
    assert code == 0 and again["selected"] == 0

    code, retried = run(capsys, "retry", "--sample", "--all")
    assert code == 0 and retried["selected"] == 0  # nothing pending

    code, report = run(capsys, "status", "--sample")
    assert code == 0 and report["partition"] == "sample"
    assert report["current_revisions_by_analysis_status"] == {"completed": 3}

    code, detail = run(capsys, "inspect", "--revision-id", stored[1].revision_id)
    assert code == 0 and detail["analyses"][0]["status"] == "completed"
    assert detail["text_units"] and detail["memberships"] and detail["comparisons"]
    assert all(c["label"] == "uncertain" for c in detail["comparisons"])


def test_retry_refuses_to_run_next_to_another_writer(db, capsys):
    from datasources.similarity.lock import GraphWriterLock

    with GraphWriterLock(db, is_sample=True, purpose="pytest holder"):
        code = cli.main(["retry", "--sample"])
    assert code == 2
    assert "already writing" in capsys.readouterr().err


def test_evaluate_requires_explicit_consent_for_paid_calls(capsys, monkeypatch):
    monkeypatch.setenv("SIMILARITY_COMPARISON_BACKEND", "anthropic")
    assert cli.main(["evaluate", "--limit", "2"]) == 2
    assert "--confirm-paid-calls" in capsys.readouterr().err
