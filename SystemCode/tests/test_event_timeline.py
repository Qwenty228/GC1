"""The event timeline (``GET /api/events/{id}/timeline``) on an ISOLATED test database.

Like the conflict-notice tests, records enter through the real compiled graph with the hashing embedder and
scripted comparison and summary models (the fictional Veltrani Photonics acquisition, plus a few small
scenarios), and a guard makes any model client or outbound socket fail. Sample rows only; the module refuses to
start unless ``conftest.verify_isolated_database`` confirms a test database (never stock_news).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from conftest import cleanup_sample_rows, isolated_test_database

psycopg = pytest.importorskip("psycopg")

from datasources.ingest.graph import run_pipeline  # noqa: E402
from datasources.similarity import store as similarity_store  # noqa: E402
from datasources.similarity.lock import GraphWriterLock, WriterLockError  # noqa: E402
from datasources.similarity.pipeline import PipelineContext  # noqa: E402
from datasources.summary import store as summary_store  # noqa: E402
from datasources.summary.timeline import report_time  # noqa: E402
from test_conflict_notice import ARTICLES, JUDGMENTS, build, decide, no_model_calls, offline_reads  # noqa: E402,F401
from test_similarity_pipeline import anchor, article, envelope, memberships, q, scripted, sec_envelope  # noqa: E402
from test_similarity_pipeline import runtime as grouping_runtime  # noqa: E402

REPORTS = ("first_report", "additional_coverage", "substantive_update", "conflicting_report")


@pytest.fixture(scope="module")
def db():
    database = isolated_test_database()  # refuses stock_news and anything not verified as a test DB
    with database.connection() as conn:
        missing = similarity_store.missing_schema(conn) + summary_store.missing_schema(conn)
    if missing:
        database.close()
        pytest.skip(f"migrations 002/004/005 not applied ({missing}); run `uv run datasources-db migrate`")
    try:
        lock = GraphWriterLock(database, is_sample=True, purpose="pytest timeline").acquire()
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


def timeline(client, event_id: str) -> dict:
    response = client.get(f"/api/events/{event_id}/timeline", params={"include_sample": "true"})
    assert response.status_code == 200, response.text
    for private in ("pytest-reviewer", "pytest private note", "replayed from the recorded fixture judgment"):
        assert private not in response.text  # reviewer identities, notes and model explanations stay internal
    body = response.json()
    entry_ids = [e["id"] for e in body["entries"]]
    assert len(entry_ids) == len(set(entry_ids))  # never a duplicate entry
    return body


def reports(body) -> list[dict]:
    return [e for e in body["entries"] if e["kind"] in REPORTS]


def at(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def sorted_by_time(body) -> bool:
    keys = []
    for e in body["entries"]:
        when = at(e["time"]) or (datetime.fromisoformat(f"{e['date']}T00:00:00+00:00") if e["date"] else None)
        keys.append((when is None, when or datetime.min.replace(tzinfo=timezone.utc)))
    return keys == sorted(keys)


def ingest(db, grt, raw, srt=None, retrieved=None):
    env = envelope(raw, retrieved=retrieved) if raw.get("updated_at") else {
        "provider": "alpaca", "transport": "rest", "api_endpoint": "https://data.alpaca.markets/v1beta1/news",
        "retrieved_at": retrieved, "requested_symbols": ["NVDA"], "is_sample": True, "raw_record": raw}
    result = run_pipeline([env], PipelineContext(database=db, analyzer=grt, summarizer=srt))
    assert not result.errors, result.errors
    (outcome,) = result.outcomes
    assert outcome.error is None, outcome.error
    return outcome


def origin_event(db, revision_id: str) -> str:
    return next(m["event_id"] for m in memberships(db, revision_id) if m["relation"] == "origin")


# ----------------------------------------------------------------------------- the Veltrani story
def test_reports_are_labelled_from_their_stored_relation_and_placed_by_their_own_time(db, client):
    s = build(db)  # a founds the story; b repeats it, c is an update, d a conflicting price
    with offline_reads(s.srt):
        body = timeline(client, s.event_id)
    assert sorted_by_time(body)
    by_revision = {e["source"]["revision_id"]: e for e in reports(body)}
    expected = {"a": "first_report", "b": "additional_coverage", "c": "substantive_update", "d": "conflicting_report"}
    for key, kind in expected.items():
        e = by_revision[s.revisions[key]]
        assert e["kind"] == kind and e["time_basis"] == "published" and not e["revised_report"], key
        published = s.at + timedelta(minutes=ARTICLES[key]["offset_minutes"])
        assert at(e["time"]) == published  # the source's publication time, not when it was collected
        assert at(e["source"]["retrieved_at"]) == published + timedelta(minutes=5)
        assert e["source"]["title"] == ARTICLES[key]["headline"] and e["source"]["url"] == ARTICLES[key]["url"]
    assert [e["kind"] for e in reports(body)] == list(expected.values())  # publication order
    assert by_revision[s.revisions["a"]]["earlier_report_exists"] is False

    update = by_revision[s.revisions["c"]]
    assert update["aspects"] == ["regulatory approval", "transaction status"]
    assert update["quotes"] == JUDGMENTS["c"]["incoming"]  # verbatim, exactly as stored
    conflict = by_revision[s.revisions["d"]]
    assert conflict["aspects"] == ["purchase price"] and conflict["quotes"] == JUDGMENTS["d"]["incoming"]
    assert conflict["review"]["display_status"] == "under_review" and conflict["review"]["review_status"] == "open"

    # Conflict detection is its own entry, at the comparison's time (not the report's publication time).
    (detected,) = [e for e in body["entries"] if e["kind"] == "conflict_detected"]
    compared = q(db, "SELECT created_at FROM event_comparison WHERE revision_id = %s AND label = "
                     "'same_event_conflict'", s.revisions["d"])[0]["created_at"]
    assert at(detected["time"]) == compared and detected["time_basis"] == "detected"
    assert detected["report"]["revision_id"] == s.revisions["d"] and detected["report"]["in_event"] is True
    assert detected["review"]["topic"] == ["purchase price"]

    # Summary versions at their own publication times.
    summaries = [e for e in body["entries"] if e["kind"].startswith("summary_")]
    stored = q(db, "SELECT version, published_at, status FROM event_summary WHERE event_id = %s ORDER BY version",
               s.event_id)
    assert [(e["kind"], e["version"]) for e in summaries] == [("summary_published", 1), ("summary_revised", 2)]
    assert [at(e["time"]) for e in summaries] == [r["published_at"] for r in stored]
    assert [e["current"] for e in summaries] == [False, True]
    assert all(e["time_basis"] == "summary_published" for e in summaries)
    assert summaries[1]["sources"] == 3 and summaries[1]["generation_reason"] == "new_information"
    assert body["counts"] == {"reports": 4, "report_versions": 4, "undated": 0}
    assert body["conflict_status"] == "under_review" and body["under_review"] is True


def test_review_decisions_are_listed_without_reviewer_identity_or_notes(db, client):
    s = build(db, ("a", "b", "c", "d", "e"))
    decide(db, s.reviews["d"], "insufficient_evidence")
    decide(db, s.reviews["d"], "confirmed_conflict")
    result = decide(db, s.reviews["e"], "different_events")
    with offline_reads(s.srt):
        body = timeline(client, s.event_id)  # also checks that no reviewer name or note is returned
    decisions = [e for e in body["entries"] if e["kind"] == "review_decision"]
    assert [(e["decision"], e["final"]) for e in decisions] == [
        ("insufficient_evidence", False), ("confirmed_conflict", True), ("different_events", True)]
    stored = q(db, "SELECT d.created_at FROM event_review_decision d WHERE d.review_id = ANY(%s::uuid[]) "
                   "ORDER BY d.created_at", [s.reviews["d"], s.reviews["e"]])
    assert [at(e["time"]) for e in decisions] == [r["created_at"] for r in stored]
    (correction,) = result["membership_corrections"]
    assert decisions[2]["moved_to_event_id"] == correction["new_event_id"]
    # The report moved out after the review is no longer a report of this story, but its conflict history stays.
    assert s.revisions["e"] not in {e["source"]["revision_id"] for e in reports(body)}
    assert body["excluded"]["detached_reports"] == 1
    detected = {e["report"]["revision_id"]: e for e in body["entries"] if e["kind"] == "conflict_detected"}
    assert detected[s.revisions["e"]]["report"]["in_event"] is False
    assert detected[s.revisions["d"]]["review"]["display_status"] == "disputed"


# ----------------------------------------------------------------------------- order, revisions, missing times
def test_a_report_collected_later_is_placed_by_its_publication_time(db, client):
    t0 = anchor()
    grt = grouping_runtime(db, scripted(lambda s, e: "different_event"))
    late = ingest(db, grt, article("Example Lab opens a second Example City site",
                                   "NVIDIA's Example Lab opened a second site in Example City.", t0 + timedelta(hours=3)))
    grt.comparator = scripted(lambda s, e: "same_event_repeated")
    early = ingest(db, grt, article("NVIDIA's Example Lab adds a site in Example City",
                                    "NVIDIA said its Example Lab added a site in Example City.", t0 + timedelta(hours=1)),
                   retrieved=t0 + timedelta(hours=4))  # published first, collected last
    body = timeline(client, origin_event(db, late.revision_id))
    first, second = reports(body)
    assert (first["source"]["revision_id"], first["kind"]) == (early.revision_id, "additional_coverage")
    assert (second["source"]["revision_id"], second["kind"]) == (late.revision_id, "first_report")
    assert second["earlier_report_exists"] is True  # the story started from it, but it was not the earliest report


def test_a_revised_report_keeps_its_own_relation_and_is_placed_at_the_source_update_time(db, client):
    t0 = anchor()
    article_id = 8_100_000_000 + uuid.uuid4().int % 90_000_000
    body_text = "NVIDIA agreed to buy Example Optics. The deal is expected to close next year."

    def label(segment, event):
        text = " ".join(u.text for u in segment.units)
        if "(final)" in text:
            return {"label": "same_event_update", "differences": [
                {"aspect": "closing date", "incoming": "closed on Monday", "candidate": "next year",
                 "kind": "temporal_development"}]}
        return "same_event_repeated"

    grt = grouping_runtime(db, scripted(label))
    raw = article("NVIDIA to buy Example Optics", body_text, t0, article_id=article_id)
    first = ingest(db, grt, raw)
    second = ingest(db, grt, {**raw, "headline": "NVIDIA to buy Example Optics (updated)",
                              "updated_at": (t0 + timedelta(hours=2)).isoformat().replace("+00:00", "Z")})
    third = ingest(db, grt, {**raw, "headline": "NVIDIA to buy Example Optics (final)",
                             "content": "<p>NVIDIA completed its purchase of Example Optics, which closed on Monday.</p>",
                             "updated_at": (t0 + timedelta(hours=5)).isoformat().replace("+00:00", "Z")})
    body = timeline(client, origin_event(db, first.revision_id))
    entries = reports(body)
    assert [e["source"]["revision_id"] for e in entries] == [first.revision_id, second.revision_id, third.revision_id]
    assert [(e["kind"], e["revised_report"], e["time_basis"]) for e in entries] == [
        ("first_report", False, "published"),
        ("additional_coverage", True, "source_updated"),  # a new timestamp alone is never an update
        ("substantive_update", True, "source_updated"),
    ]
    assert [at(e["time"]) for e in entries] == [t0, t0 + timedelta(hours=2), t0 + timedelta(hours=5)]
    assert entries[2]["aspects"] == ["closing date"]
    assert [e["source"]["revision_number"] for e in entries] == [1, 2, 3]
    assert [e["source"]["is_current_revision"] for e in entries] == [False, False, True]
    assert all(e["source"]["revisions_total"] == 3 and e["source"]["current_revision_in_event"] for e in entries)
    assert body["counts"] == {"reports": 1, "report_versions": 3, "undated": 0}


def test_missing_times_are_reported_as_missing_and_never_replaced_by_the_retrieval_time(db, client):
    t0 = anchor()
    grt = grouping_runtime(db, scripted(lambda s, e: "different_event"))
    undated = {**article("Undated Example Lab memo", "NVIDIA's Example Lab circulated a memo about its plans.", t0),
               "created_at": None, "updated_at": None}
    collected = t0 + timedelta(hours=1)
    news = ingest(db, grt, undated, retrieved=collected)
    body = timeline(client, origin_event(db, news.revision_id))
    (entry,) = reports(body)
    assert entry["time"] is None and entry["date"] is None and entry["time_basis"] == "missing"
    assert at(entry["source"]["retrieved_at"]) == collected  # reported separately, as the collection time
    assert body["counts"]["undated"] == 1

    # A filing without an acceptance time is placed by its filing date, given as a date only.
    filing = sec_envelope(t0 + timedelta(days=1), f"9999999999-34-{uuid.uuid4().int % 1_000_000:06d}")
    filing["raw_record"] = {**filing["raw_record"], "acceptanceDateTime": ""}
    result = run_pipeline([filing], PipelineContext(database=db, analyzer=grt))
    (outcome,) = result.outcomes
    body = timeline(client, origin_event(db, outcome.revision_id))
    (entry,) = reports(body)
    assert entry["time"] is None and entry["time_basis"] == "sec_filing_date"
    assert entry["date"] == (t0 + timedelta(days=1)).date().isoformat()
    assert body["counts"]["undated"] == 0


def test_report_time_prefers_the_sources_own_times():
    when = datetime(2034, 1, 1, tzinfo=timezone.utc)
    base = {"record_type": "news_article", "published_at": None, "source_updated_at": None, "accepted_at": None,
            "filing_date": None, "retrieved_at": when}
    assert report_time({**base, "published_at": when}, later_revision=False) == (when, "published", None)
    assert report_time({**base, "source_updated_at": when}, later_revision=False) == (when, "source_updated", None)
    assert report_time({**base, "published_at": when}, later_revision=True) == (None, "missing", None)
    assert report_time(base, later_revision=False) == (None, "missing", None)  # retrieved_at is never used
    sec_row = {**base, "record_type": "sec_filing", "filing_date": when.date()}
    assert report_time(sec_row, later_revision=False) == (None, "sec_filing_date", "2034-01-01")
    assert report_time({**sec_row, "accepted_at": when}, later_revision=False) == (when, "sec_accepted", None)


# ----------------------------------------------------------------------------- event-specific evidence
def test_a_filing_covering_two_events_shows_only_its_own_section_in_each(db, client):
    t0 = anchor()

    def label(segment, event):
        texts = " ".join(u.text for u in segment.units)
        same = "repurchase" in texts and "repurchase" in " ".join(p.text for p in event.passages)
        return "same_event_repeated" if same else "different_event"

    grt = grouping_runtime(db, scripted(label), chunk_tokens=60, chunk_overlap_tokens=12)
    filing_env = sec_envelope(t0, f"9999999999-34-{uuid.uuid4().int % 1_000_000:06d}")
    (filing,) = run_pipeline([filing_env], PipelineContext(database=db, analyzer=grt)).outcomes
    events = {r["title"]: r["id"] for r in q(db, "SELECT e.title, e.event_id::text AS id FROM news_event e "
                                                 "WHERE e.origin_revision_id = %s", filing.revision_id)}
    buyback = next(i for t, i in events.items() if "8.01" in t)
    departure = next(i for t, i in events.items() if "5.02" in t)
    from test_similarity_pipeline import BUYBACK_SENTENCES

    news = ingest(db, grt, article("NVIDIA adds $60 billion to share repurchase program",
                                   "\n\n".join(" ".join(BUYBACK_SENTENCES) for _ in range(3)), t0 + timedelta(hours=3)))
    assert q(db, "SELECT count(*) AS n FROM text_unit WHERE revision_id = %s", news.revision_id)[0]["n"] > 1

    first = timeline(client, buyback)
    run_pipeline([filing_env], PipelineContext(database=db, analyzer=grt))  # repeated delivery of the filing
    again = timeline(client, buyback)
    assert again["entries"] == first["entries"]  # replays and chunks add nothing
    sources = {e["source"]["revision_id"]: e for e in reports(first)}
    assert set(sources) == {filing.revision_id, news.revision_id}  # one entry per report, however many chunks
    assert any("8.01" in s for s in sources[filing.revision_id]["sections"])
    assert not any("5.02" in s for s in sources[filing.revision_id]["sections"])
    assert sources[filing.revision_id]["time_basis"] == "sec_accepted" and at(sources[filing.revision_id]["time"]) == t0

    other = timeline(client, departure)
    (only,) = reports(other)  # the buyback article is not evidence of the other event
    assert only["source"]["revision_id"] == filing.revision_id
    assert any("5.02" in s for s in only["sections"]) and not any("8.01" in s for s in only["sections"])


def test_sample_timelines_stay_hidden_unless_requested_and_unknown_events_are_404(db, client):
    t0 = anchor()
    grt = grouping_runtime(db, scripted(lambda s, e: "different_event"))
    news = ingest(db, grt, article("Example Lab hires staff", "NVIDIA's Example Lab hired 40 staff.", t0))
    event_id = origin_event(db, news.revision_id)
    with offline_reads():
        assert client.get(f"/api/events/{event_id}/timeline").status_code == 404
        assert client.get(f"/api/events/{event_id}/timeline", params={"include_sample": "true"}).status_code == 200
        assert client.get(f"/api/events/{uuid.uuid4()}/timeline", params={"include_sample": "true"}).status_code == 404
        assert client.get("/api/events/not-a-uuid/timeline").status_code == 422
    assert client.post(f"/api/events/{event_id}/timeline").status_code == 405  # read-only


def test_reading_timelines_makes_no_model_call(db, client):
    s = build(db, ("a", "c"))
    comparisons, summaries = len(s.grt.comparator.calls), len(s.srt.backend.calls)
    with offline_reads(s.srt):
        for _ in range(3):
            timeline(client, s.event_id)
            assert client.get(f"/api/events/{s.event_id}", params={"include_sample": "true"}).status_code == 200
    assert len(s.grt.comparator.calls) == comparisons and len(s.srt.backend.calls) == summaries


def test_malformed_stored_differences_are_skipped_not_repaired():
    from datasources.summary.timeline import _aspects

    stored = ["not a dict", None, {"kind": "conflict"}, {"kind": "conflict", "aspect": " "},
              {"kind": "conflict", "aspect": "purchase price"}, {"kind": "new_information", "aspect": "closing date"},
              {"kind": "conflict", "aspect": "purchase price"}]
    assert _aspects(stored, ("conflict",)) == ["purchase price"]
    assert _aspects({"not": "a list"}, ("conflict",)) == []
