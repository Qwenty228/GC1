"""The reader feed's filters and card fields on ``GET /api/events``, on an ISOLATED test database.

Filters: search (``q``), the latest-report time window (``since``/``until``), ``has_filing`` and ``conflict``
("sources disagree"); every filter runs in SQL before paging. Card fields: ``filings`` (form and the 8-K items of
the event's own part of a filing, with official captions) and ``latest_update`` (the newest substantive update's
time, source and stored aspects). The "sources disagree" SQL is checked against the list's own conflict status for
every review outcome, because the two are written separately.

Records enter through the real compiled graph with the hashing embedder and scripted comparison and summary
models (the fictional Veltrani Photonics acquisition and the example 8-K), and a guard makes any model client or
outbound socket fail. Sample rows only; the module refuses to start unless ``conftest.verify_isolated_database``
confirms a test database (never stock_news).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import pytest

from conftest import cleanup_sample_rows, isolated_test_database

psycopg = pytest.importorskip("psycopg")

from datasources.ingest.graph import run_pipeline  # noqa: E402
from datasources.sec_items import SEC_8K_ITEMS  # noqa: E402
from datasources.similarity import store as similarity_store  # noqa: E402
from datasources.similarity.lock import GraphWriterLock, WriterLockError  # noqa: E402
from datasources.similarity.pipeline import PipelineContext  # noqa: E402
from datasources.summary import store as summary_store  # noqa: E402
from datasources.summary.conflicts import ACTIVE  # noqa: E402
from datasources.summary.read import search_terms  # noqa: E402
from test_conflict_notice import ARTICLES, build, decide, no_model_calls, offline_reads  # noqa: E402,F401
from test_similarity_pipeline import anchor, q, scripted, sec_envelope  # noqa: E402
from test_similarity_pipeline import runtime as grouping_runtime  # noqa: E402


@pytest.fixture(scope="module")
def db():
    database = isolated_test_database()  # refuses stock_news and anything not verified as a test DB
    with database.connection() as conn:
        missing = similarity_store.missing_schema(conn) + summary_store.missing_schema(conn)
    if missing:
        database.close()
        pytest.skip(f"migrations 002/004/005 not applied ({missing}); run `uv run datasources-db migrate`")
    try:
        lock = GraphWriterLock(database, is_sample=True, purpose="pytest feed filters").acquire()
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
    try:
        yield TestClient(server.app)
    finally:
        server.app.dependency_overrides.pop(server.get_database, None)


def events(client, **params) -> list[dict]:
    response = client.get("/api/events", params={"include_sample": "true", "limit": 100, **params})
    assert response.status_code == 200, response.text
    return response.json()["events"]


def window(at: datetime, **params) -> dict:
    """Each scenario has its own 60-day time window: bound the list to it so other scenarios never interfere."""
    return {"since": (at - timedelta(hours=1)).isoformat(), "until": (at + timedelta(days=3)).isoformat(), **params}


def ids(client, at: datetime, **params) -> set[str]:
    return {e["event_id"] for e in events(client, **window(at, **params))}


# ----------------------------------------------------------------------------- search terms (no database)
def test_search_text_becomes_literal_word_start_patterns():
    assert search_terms(None) is None and search_terms("   ") is None
    assert search_terms("Nvidia nvidia  AI") == ["\\mNvidia", "\\mAI"]  # case-insensitive duplicates dropped
    assert search_terms("$5,899 (AI) a_b%") == ["\\$5,899", "\\(AI\\)", "\\ma_b%"]  # never a live pattern
    assert len(search_terms(" ".join(f"w{i}" for i in range(20)))) == 8
    with pytest.raises(ValueError):
        search_terms("x" * 201)


# ----------------------------------------------------------------------------- card fields
def test_latest_update_search_and_time_window(db, client):
    s = build(db)  # a founds the story; b repeats it, c is a substantive update, d a conflicting price
    with offline_reads(s.srt):
        (item,) = events(client, **window(s.at))
        update = item["latest_update"]
        assert update["aspects"] == ["regulatory approval", "transaction status"]  # as the timeline shows them
        assert datetime.fromisoformat(update["time"]) == s.at + timedelta(minutes=ARTICLES["c"]["offset_minutes"])
        assert update["name"] == ARTICLES["c"]["source"] and update["record_type"] == "news_article"
        assert item["filings"] == []

        # Search: every term, case-insensitive, from the start of a word, in the headline, summary, a source's
        # title or a linked company's name or ticker.
        assert ids(client, s.at, q="veltrani") == {s.event_id}
        assert ids(client, s.at, q="VELTRANI photonics") == {s.event_id}
        assert ids(client, s.at, q="NVDA") == {s.event_id}  # a linked company's ticker
        assert ids(client, s.at, q="Fixture Finance Journal's") == set()  # a word that appears nowhere
        assert ids(client, s.at, q="eltrani") == set()  # not the start of a word
        assert ids(client, s.at, q="veltrani zzqx") == set()  # every term must match
        assert ids(client, s.at, q="(veltrani [ * \\") == set()  # special characters are literal, never an error
        assert ids(client, s.at, q="Photonics") == ids(client, s.at, q="photonics")
        applied = client.get("/api/events", params=window(s.at, q="  Veltrani   photonics ", conflict="true")).json()
        assert applied["filters"]["q"] == "Veltrani photonics" and applied["filters"]["conflict"] is True
        assert applied["filters"]["has_filing"] is False and applied["filters"]["since"]  # echoed: what was applied

        # The time window bounds the latest report (until is exclusive).
        last = datetime.fromisoformat(item["last_source_time"])
        assert ids(client, s.at, since=last.isoformat()) == {s.event_id}
        assert ids(client, s.at, since=(last + timedelta(seconds=1)).isoformat()) == set()
        assert {e["event_id"] for e in events(client, since=(s.at - timedelta(hours=1)).isoformat(),
                                              until=last.isoformat())} == set()

    for params in ({"since": "yesterday"}, {"until": "2026-13-01"}, {"q": "x" * 201},
                   {"since": "2034-01-02T00:00:00Z", "until": "2034-01-01T00:00:00Z"}):
        response = client.get("/api/events", params=params)
        assert response.status_code == 422 and "error" in response.json(), params


def test_filings_show_the_items_of_their_own_part_and_the_filing_filter_keeps_them(db, client):
    t0 = anchor()

    def label(segment, event):
        texts = " ".join(u.text for u in segment.units)
        same = "repurchase" in texts and "repurchase" in " ".join(p.text for p in event.passages)
        return "same_event_repeated" if same else "different_event"

    grt = grouping_runtime(db, scripted(label), chunk_tokens=60, chunk_overlap_tokens=12)
    (filing,) = run_pipeline([sec_envelope(t0, f"9999999999-34-{uuid.uuid4().int % 1_000_000:06d}")],
                             PipelineContext(database=db, analyzer=grt)).outcomes
    origin = {r["id"] for r in q(db, "SELECT event_id::text AS id FROM news_event WHERE origin_revision_id = %s",
                                 filing.revision_id)}
    assert len(origin) == 2  # the example 8-K covers two events: an officer departure (5.02) and a buyback (8.01)
    listed = {e["event_id"]: e for e in events(client, **window(t0))}
    items = {}
    for event_id in origin:
        (one,) = listed[event_id]["filings"]
        assert one["form"] == "8-K"
        assert all(i["caption"] == SEC_8K_ITEMS[i["code"]] for i in one["items"])
        items[event_id] = [i["code"] for i in one["items"]]
    assert sorted(items.values()) == [["5.02"], ["8.01"]]  # each event only its own part of the filing
    assert ids(client, t0, has_filing="true") == origin

    news = build(db, ("a", "b"))  # news-only stories are left out by the filing filter
    assert news.event_id in ids(client, news.at) and ids(client, news.at, has_filing="true") == set()


# ----------------------------------------------------------------------------- "sources disagree"
def _scenario(db, outcome: str):
    if outcome == "none":
        return build(db, ("a", "b"))
    s = build(db)
    if outcome == "correction_pending":  # a different_events decision whose correction never ran
        with db.transaction() as conn:
            conn.execute("UPDATE event_conflict_review SET status = 'resolved', resolution = 'different_events', "
                         "reviewer_id = 'pytest-reviewer', reviewer_notes = 'pytest private note', resolved_at = now() "
                         "WHERE review_id = %s", (s.reviews["d"],))
    elif outcome != "open":
        decide(db, s.reviews["d"], outcome)
    return s


@pytest.mark.parametrize("outcome, expected", [
    ("none", "none"), ("open", "under_review"), ("insufficient_evidence", "under_review"),
    ("confirmed_conflict", "disputed"), ("not_a_conflict", "resolved"), ("different_events", "resolved"),
    ("correction_pending", "correction_pending"),
])
def test_the_disagree_filter_matches_the_listed_conflict_status(db, client, outcome, expected):
    s = _scenario(db, outcome)
    with offline_reads(s.srt):
        listed = {e["event_id"]: e for e in events(client, **window(s.at))}
        status = listed[s.event_id]["conflict"]["status"]
        assert status == expected
        disagree = ids(client, s.at, conflict="true")
    assert (s.event_id in disagree) is (status in ACTIVE)
    # Any other event in the window (a report moved to its own story) follows the same rule.
    assert disagree == {i for i, e in listed.items() if e["conflict"]["status"] in ACTIVE}
