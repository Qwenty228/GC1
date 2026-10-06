"""The page's read API on an ISOLATED test database: /api/articles, /api/status counts and the events
`symbol` filter. Sample rows only; the module refuses to start unless
``conftest.verify_isolated_database`` confirms a test database (never stock_news).

Records enter through the real compiled graph with the deterministic hashing embedder and a scripted
comparator and summarizer, and a guard makes any model client or outbound socket fail.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from conftest import TEST_PUBLISHER, cleanup_sample_rows, isolated_test_database

psycopg = pytest.importorskip("psycopg")

from datasources.db.read import EXCERPT_CHARS  # noqa: E402
from datasources.ingest.graph import run_pipeline  # noqa: E402
from datasources.similarity import store as similarity_store  # noqa: E402
from datasources.similarity.lock import GraphWriterLock, WriterLockError  # noqa: E402
from datasources.similarity.pipeline import PipelineContext  # noqa: E402
from datasources.summary import store as summary_store  # noqa: E402
from datasources.summary.generation import ScriptedSummaryBackend, extractive_output  # noqa: E402
from datasources.summary.pipeline import build_summary_runtime  # noqa: E402
from datasources.summary.settings import SummarySettings  # noqa: E402
from test_conflict_notice import no_model_calls, offline_reads  # noqa: E402,F401  (autouse guard)
from test_similarity_pipeline import anchor, article, envelope, new_id, sec_envelope  # noqa: E402
from test_similarity_pipeline import runtime as grouping_runtime  # noqa: E402


@pytest.fixture(scope="module")
def db():
    database = isolated_test_database()
    with database.connection() as conn:
        missing = similarity_store.missing_schema(conn) + summary_store.missing_schema(conn)
    if missing:
        database.close()
        pytest.skip(f"migrations 002/004/005 not applied ({missing}); run `uv run datasources-db migrate`")
    try:
        lock = GraphWriterLock(database, is_sample=True, purpose="pytest web api").acquire()
    except WriterLockError as error:
        database.close()
        pytest.skip(str(error))
    cleanup_sample_rows(database)
    try:
        yield database
    finally:
        cleanup_sample_rows(database)
        lock.release()
        database.close()


@pytest.fixture(scope="module")
def client(db):
    from fastapi.testclient import TestClient

    from datasources import server

    server.app.dependency_overrides[server.get_database] = lambda: db
    server.app.dependency_overrides[server.optional_database] = lambda: (db, None)
    try:
        yield TestClient(server.app)
    finally:
        server.app.dependency_overrides.pop(server.get_database, None)
        server.app.dependency_overrides.pop(server.optional_database, None)


def ingest(db, grt, env, srt=None):
    result = run_pipeline([env], PipelineContext(database=db, analyzer=grt, summarizer=srt))
    assert not result.errors, result.errors
    (outcome,) = result.outcomes
    assert outcome.error is None, outcome.error
    return outcome


@pytest.fixture(scope="module")
def stored(db):
    """One news article (long excerpt, unsafe link, two revisions) and one SEC filing, grouped and summarized."""
    at = anchor()
    grt = grouping_runtime(db)
    srt = build_summary_runtime(db, SummarySettings(enabled=True), backend=ScriptedSummaryBackend(extractive_output))
    article_id = new_id()
    long_excerpt = "NVIDIA expands its Example Lab. " * 40  # far longer than the excerpt limit
    first = {**article("NVIDIA expands the Example Lab", "NVIDIA expanded the Example Lab in Example City.", at,
                       article_id=article_id, summary=long_excerpt), "url": "javascript:alert(1)"}
    ingest(db, grt, envelope(first), srt)
    revised = {**first, "headline": "NVIDIA expands the Example Lab (updated)",
               "updated_at": (at + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
               "content": "<p>NVIDIA expanded the Example Lab in Example City and hired 40 staff.</p>"}
    news = ingest(db, grt, envelope(revised), srt)
    filing = ingest(db, grt, sec_envelope(at + timedelta(hours=2), f"0001045810-34-{new_id() % 1_000_000:06d}"))
    return {"news": news, "filing": filing, "grt": grt, "srt": srt}


def by_document(items):
    return {a["document_id"]: a for a in items}


def test_articles_are_stored_records_with_their_own_source_fields(client, stored):
    news, filing = stored["news"], stored["filing"]
    default = by_document(client.get("/api/articles", params={"limit": 200}).json()["articles"])
    assert news.document_id not in default and filing.document_id not in default  # sample rows hidden by default

    page = client.get("/api/articles", params={"include_sample": "true", "limit": 200}).json()
    items = by_document(page["articles"])
    assert page["total"] >= 2 and len(items) == len(page["articles"])  # one card per document, no duplicates
    a = items[news.document_id]
    assert a["record_type"] == "news_article" and a["provider"] == "alpaca" and a["publisher"] == TEST_PUBLISHER
    assert a["title"] == "NVIDIA expands the Example Lab (updated)" and a["revision_id"] == news.revision_id
    assert a["revisions"] == 2  # the earlier revision is kept, counted, and not shown as a second article
    assert a["url"] is None  # javascript: never becomes a link
    assert a["excerpt_truncated"] is True and len(a["excerpt"]) <= EXCERPT_CHARS
    assert long_prefix(a["excerpt"]) and a["is_sample"] is True
    assert a["grouping_status"] == "completed" and a["events"] and a["events"][0]["event_id"]
    assert a["published_at"] and a["retrieved_at"] and a["source_time"]

    f = items[filing.document_id]
    assert f["record_type"] == "sec_filing" and f["provider"] == "sec_edgar" and f["excerpt"] is None
    assert f["form"] == "8-K" and f["item_codes"] == ["5.02", "8.01"] and f["issuer"]
    assert f["url"].startswith("https://www.sec.gov/") and f["filing_index_url"].startswith("https://www.sec.gov/")


def long_prefix(text: str) -> bool:
    return text.startswith("NVIDIA expands its Example Lab.")


def test_article_filters_by_source_and_company(client, stored):
    params = {"include_sample": "true", "limit": 200}
    sec = by_document(client.get("/api/articles", params={**params, "source": "sec"}).json()["articles"])
    news = by_document(client.get("/api/articles", params={**params, "source": "news"}).json()["articles"])
    assert stored["filing"].document_id in sec and stored["news"].document_id not in sec
    assert stored["news"].document_id in news and stored["filing"].document_id not in news
    nvda = by_document(client.get("/api/articles", params={**params, "symbol": "nvda"}).json()["articles"])
    assert {stored["news"].document_id, stored["filing"].document_id} <= set(nvda)
    other = client.get("/api/articles", params={**params, "symbol": "ZZZZ"}).json()
    assert other["articles"] == [] and other["total"] == 0


def test_status_counts_and_the_events_company_filter(client, stored):
    body = client.get("/api/status", params={"include_sample": "true"}).json()
    counts = body["counts"]
    assert body["database"]["available"] is True
    assert counts["news"] >= 1 and counts["sec"] >= 1 and counts["articles"] == counts["news"] + counts["sec"]
    assert counts["events"] >= 1 and counts["grouping"].get("completed", 0) >= 2
    event_id = next(a for a in client.get("/api/articles", params={"include_sample": "true", "limit": 200})
                    .json()["articles"] if a["document_id"] == stored["news"].document_id)["events"][0]["event_id"]
    listed = {e["event_id"] for e in client.get("/api/events", params={"include_sample": "true", "limit": 100,
                                                                        "symbol": "NVDA"}).json()["events"]}
    assert event_id in listed
    assert client.get("/api/events", params={"include_sample": "true", "symbol": "ZZZZ"}).json()["events"] == []


def test_event_cards_carry_their_tickers_and_latest_source(client, stored):
    """The reader card shows the ticker without a company filter and, before a summary exists, a link to the
    most recent source; unsafe links are dropped by the API itself."""
    articles = client.get("/api/articles", params={"include_sample": "true", "limit": 200}).json()["articles"]
    news = next(a for a in articles if a["document_id"] == stored["news"].document_id)
    events = {e["event_id"]: e for e in client.get("/api/events", params={"include_sample": "true",
                                                                          "limit": 100}).json()["events"]}
    event = events[news["events"][0]["event_id"]]
    assert event["symbols"] == ["NVDA"]
    latest = event["latest_source"]
    assert latest["title"] == "NVIDIA expands the Example Lab (updated)" and latest["name"] == TEST_PUBLISHER
    assert latest["record_type"] == "news_article" and latest["published_at"]
    assert latest["url"] is None  # javascript: never becomes a link


def test_the_launcher_only_counts_a_graph_writer_in_its_own_database(db):
    """Advisory locks are per database, but pg_locks lists them all: a writer in a test database must not
    block launching against another database."""
    import dataclasses

    from datasources.db import Database
    from datasources.launcher import _graph_writer_holder

    other = Database(dataclasses.replace(db.settings, dbname="postgres"))  # the maintenance database, read only
    lock = GraphWriterLock(db, is_sample=False, purpose="pytest launcher check").acquire()
    try:
        assert _graph_writer_holder(db) is not None
        assert _graph_writer_holder(other) is None
    finally:
        lock.release()
        other.close()
    assert _graph_writer_holder(db) is None


def test_polling_the_read_api_never_runs_grouping_or_summaries(client, stored):
    grt, srt = stored["grt"], stored["srt"]
    comparisons, summaries = len(grt.comparator.calls), len(srt.backend.calls)
    with offline_reads(srt):  # no outbound socket and no scripted model call while reading
        for _ in range(5):
            events = client.get("/api/events", params={"include_sample": "true"}).json()["events"]
            for event in events[:3]:
                assert client.get(f"/api/events/{event['event_id']}", params={"include_sample": "true"}).status_code == 200
            assert client.get("/api/articles", params={"include_sample": "true"}).status_code == 200
            assert client.get("/api/status", params={"include_sample": "true"}).status_code == 200
    assert len(grt.comparator.calls) == comparisons and len(srt.backend.calls) == summaries
