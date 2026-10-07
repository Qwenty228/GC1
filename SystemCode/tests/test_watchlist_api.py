"""The watchlist filter and the company list on the read API, on a temporary database.

The module builds one temporary database from the repository's migration files (``conftest.temporary_database``,
dropped afterwards; the configured test database and stock_news are not touched), registers the 11 coverage
companies, and groups sample articles through the real compiled graph with the deterministic hashing embedder and
a scripted comparator. A guard makes any model client or outbound socket fail.

The stories, newest first: six NVIDIA-only stories, one story shared by NVIDIA, Microsoft and Apple (two articles),
then two Microsoft-only stories. A watchlist of Microsoft must therefore find stories that are not on the first page
of the unfiltered feed.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from conftest import TEST_PUBLISHER, temporary_database

psycopg = pytest.importorskip("psycopg")

from datasources.db.companies import coverage_companies, register_companies  # noqa: E402
from datasources.ingest.graph import run_pipeline  # noqa: E402
from datasources.similarity.lock import GraphWriterLock  # noqa: E402
from datasources.similarity.pipeline import PipelineContext  # noqa: E402
from test_conflict_notice import no_model_calls, offline_reads  # noqa: E402,F401  (autouse guard)
from test_similarity_pipeline import anchor, article, envelope, new_id, scripted  # noqa: E402
from test_similarity_pipeline import runtime as grouping_runtime  # noqa: E402

NVDA, MSFT, AAPL = "0001045810", "0000789019", "0000320193"
SHARED = "Shared Example Alliance"


def same_story(segment, event):
    """Scripted comparator: only the two articles about the shared alliance describe the same story."""
    incoming = " ".join(u.text for u in segment.units)
    known = " ".join(p.text for p in event.passages)
    if SHARED in incoming and SHARED in known:
        return {"label": "same_event_repeated", "incoming_unit_refs": [segment.units[0].ref],
                "incoming_evidence": [{"ref": segment.units[0].ref, "quote": segment.units[0].text[:40]}],
                "candidate_evidence": [{"ref": event.passages[0].ref, "quote": event.passages[0].text[:40]}]}
    return "different_event"


@pytest.fixture(scope="module")
def db():
    with temporary_database("watchlist") as database:
        with database.transaction() as conn:
            report = register_companies(conn, coverage_companies())
        assert not report.conflicts
        lock = GraphWriterLock(database, is_sample=True, purpose="pytest watchlist").acquire()
        try:
            yield database
        finally:
            lock.release()


@pytest.fixture(scope="module")
def client(db):
    from fastapi.testclient import TestClient

    from datasources import server

    server.app.dependency_overrides[server.get_database] = lambda: db
    try:
        yield TestClient(server.app)
    finally:
        server.app.dependency_overrides.pop(server.get_database, None)


def ingest(db, rt, raw):
    result = run_pipeline([envelope(raw)], PipelineContext(database=db, analyzer=rt))
    assert not result.errors, result.errors
    (outcome,) = result.outcomes
    assert outcome.error is None and outcome.analysis_status == "completed", outcome
    return outcome


def event_of(db, revision_id: str) -> str:
    with db.connection() as conn:
        return str(conn.execute("SELECT event_id FROM event_membership WHERE revision_id = %s AND state = 'active'",
                                (revision_id,)).fetchone()["event_id"])


@pytest.fixture(scope="module")
def stories(db):
    t0 = anchor()
    rt = grouping_runtime(db, scripted(same_story))

    def story(headline, body, at, symbols):
        return ingest(db, rt, {**article(headline, body, at, article_id=new_id()), "symbols": symbols})

    m2 = story("Microsoft opens an example data centre in Example Town",
               "Microsoft said it opened a data centre in Example Town.", t0 - timedelta(hours=2), ["MSFT"])
    m1 = story("Microsoft updates its example office suite",
               "Microsoft released an update to its office suite.", t0 - timedelta(hours=1), ["MSFT"])
    s1 = story(f"NVIDIA and Microsoft announce the {SHARED}",
               f"NVIDIA and Microsoft announced the {SHARED} for example chips.", t0 - timedelta(minutes=30),
               ["NVDA", "MSFT"])
    s2 = story(f"Apple joins the {SHARED}",
               f"Apple said it will join the {SHARED} with Microsoft and NVIDIA.", t0 - timedelta(minutes=20),
               ["MSFT", "AAPL", "TSLA"])
    nvda = [story(f"NVIDIA example product news {i}", f"NVIDIA shipped example product number {i} to customers.",
                  t0 + timedelta(hours=i), ["NVDA"]) for i in range(1, 7)]
    shared = event_of(db, s1.revision_id)
    assert event_of(db, s2.revision_id) == shared  # one story, two articles
    return {"rt": rt, "shared": shared, "msft": [event_of(db, m1.revision_id), event_of(db, m2.revision_id)],
            "nvda": [event_of(db, o.revision_id) for o in reversed(nvda)]}  # newest first


def listed(client, **params) -> list[dict]:
    response = client.get("/api/events", params={"include_sample": "true", "limit": 100, **params})
    assert response.status_code == 200, response.text
    return response.json()["events"]


def ids(events) -> list[str]:
    return [e["event_id"] for e in events]


def test_the_watchlist_is_filtered_before_the_page_limit(client, stories):
    newest_two = ids(listed(client, limit=2))
    assert newest_two == stories["nvda"][:2]  # the unfiltered first page has no Microsoft story at all
    page1 = ids(listed(client, company=MSFT, limit=2))
    page2 = ids(listed(client, company=MSFT, limit=2, offset=2))
    assert page1 == [stories["shared"], stories["msft"][0]] and page2 == [stories["msft"][1]]
    assert ids(listed(client, company=MSFT, limit=2, offset=3)) == []


def test_a_story_about_several_followed_companies_is_listed_once(client, stories):
    combined = listed(client, company=[NVDA, MSFT, AAPL])
    assert ids(combined) == stories["nvda"] + [stories["shared"]] + stories["msft"]  # newest first, no repeats
    assert len(ids(combined)) == len(set(ids(combined)))
    assert ids(listed(client, company=f"{NVDA},{MSFT},{AAPL}")) == ids(combined)  # comma form
    assert ids(listed(client, company=["1045810"])) == ids(listed(client, company=NVDA))  # unpadded CIK
    shared = next(e for e in combined if e["event_id"] == stories["shared"])
    assert [c["cik"] for c in shared["companies"]] == [AAPL, MSFT, NVDA]  # by name: Apple, Microsoft, NVIDIA
    assert shared["symbols"] == ["AAPL", "MSFT", "NVDA"] and shared["documents"] == 2
    assert ids(listed(client, company=AAPL)) == [stories["shared"]]


def test_the_single_company_filter_is_unchanged(client, stories):
    assert ids(listed(client, symbol="msft")) == ids(listed(client, company=MSFT))
    assert set(ids(listed(client, symbol="NVDA"))) == set(stories["nvda"]) | {stories["shared"]}
    assert ids(listed(client, symbol="MSFT", company=AAPL)) == [stories["shared"]]  # both filters apply


@pytest.mark.parametrize("value, status", [("abc", 422), ("", 422), ("12345678901", 422), ("0000000001", 200)])
def test_watchlist_values_are_validated(client, stories, value, status):
    response = client.get("/api/events", params={"include_sample": "true", "company": value})
    assert response.status_code == status, response.text
    if status == 200:
        assert response.json()["events"] == []  # a CIK nobody stored matches nothing
    else:
        assert "company" in response.json()["error"]


def test_too_many_watchlist_companies_are_refused(client, stories):
    many = [str(n) for n in range(1, 52)]
    assert client.get("/api/events", params={"company": many}).status_code == 422


def test_sample_stories_stay_out_of_the_watchlist_unless_requested(client, stories):
    default = client.get("/api/events", params={"company": [NVDA, MSFT], "limit": 100}).json()["events"]
    assert not set(ids(default)) & (set(stories["nvda"]) | {stories["shared"]} | set(stories["msft"]))
    assert all(not e["is_sample"] for e in default)


def test_coverage_lists_followable_companies_and_what_is_collected(client, stories, monkeypatch):
    monkeypatch.setenv("INGEST_SYMBOLS", "NVDA,MSFT")
    body = client.get("/api/coverage").json()
    companies = {c["symbol"]: c for c in body["companies"]}
    assert sorted(companies) == sorted(c.symbol for c in coverage_companies())
    assert companies["GOOGL"]["cik"] == "0001652044" and companies["GOOGL"]["name"] == "Alphabet Inc."
    assert {s for s, c in companies.items() if c["collecting"]} == {"NVDA", "MSFT"}
    assert body["collecting_symbols"] == ["NVDA", "MSFT"]
    assert list(companies) == sorted(companies)  # ordered by ticker


def test_reading_with_a_watchlist_changes_nothing_and_calls_no_model(client, stories, db):
    tables = ("news_event", "event_membership", "event_comparison", "revision_company", "company", "company_symbol")

    def counts():
        with db.connection() as conn:
            return {t: conn.execute(f"SELECT count(*) AS n FROM {t}").fetchone()["n"] for t in tables}

    before, calls = counts(), len(stories["rt"].comparator.calls)
    with offline_reads():
        for watchlist in ([NVDA], [MSFT, AAPL], [NVDA, MSFT, AAPL], ["0000000001"]):
            assert client.get("/api/events", params={"include_sample": "true", "company": watchlist}).status_code == 200
        assert client.get("/api/coverage").status_code == 200
        assert client.get(f"/api/events/{stories['shared']}", params={"include_sample": "true"}).status_code == 200
    assert counts() == before and len(stories["rt"].comparator.calls) == calls
    assert TEST_PUBLISHER  # the stories are the conftest's synthetic publisher's, all sample rows
