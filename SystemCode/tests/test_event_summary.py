"""Event summaries through the real compiled graph, PostgreSQL and the read API.

WORKFLOW tests: the grouping model and the summary model are scripted, so these
prove routing, persistence, versioning, review, recovery and isolation. They do
not measure summary quality (that needs a real, paid run read by a person; see
the README). Everything is sample data (``is_sample = true``) in the hashing
embedder's analysis space, deleted before and after the module. Skipped
without a database or without migration 004.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from conftest import cleanup_sample_rows, isolated_test_database

psycopg = pytest.importorskip("psycopg")

from datasources.ingest.graph import get_graph, run_pipeline  # noqa: E402
from datasources.similarity import store as similarity_store  # noqa: E402
from datasources.similarity.lock import GraphWriterLock, WriterLockError  # noqa: E402
from datasources.similarity.pipeline import PipelineContext  # noqa: E402
from datasources.summary import cli as summary_cli  # noqa: E402
from datasources.summary import catchup, review, store  # noqa: E402
from datasources.summary.generation import ScriptedSummaryBackend, SummaryError, extractive_output  # noqa: E402
from datasources.summary.pipeline import (  # noqa: E402
    SummaryBudget,
    SummaryContext,
    build_summary_runtime,
    process_event,
)
from datasources.summary.settings import SummarySettings  # noqa: E402
from test_similarity_pipeline import (  # noqa: E402
    ACQ,
    ACQ_PARAPHRASE,
    OTHER,
    anchor,
    article,
    envelope,
    memberships,
    q,
    scripted,
    sec_envelope,
)
from test_similarity_pipeline import runtime as grouping_runtime  # noqa: E402

UPDATE = ("NVIDIA completes Example Robotics acquisition",
          "NVIDIA said on Thursday it completed its $2.1 billion acquisition of Example Robotics after regulators in "
          "two countries approved the deal.\n\nExample Robotics will operate as a unit of NVIDIA's robotics group.")
CONFLICT_TEXT = ("NVIDIA to pay $3.4 billion for Example Robotics, report says",
                 "NVIDIA agreed to acquire Example Robotics for $3.4 billion, according to people familiar with the "
                 "matter.\n\nThe acquisition of Example Robotics is expected to close in the first quarter.")


@pytest.fixture(scope="module")
def db():
    database = isolated_test_database()  # refuses stock_news and anything not verified as a test DB
    with database.connection() as conn:
        missing = similarity_store.missing_schema(conn) + store.missing_schema(conn)
    if missing:
        database.close()
        pytest.skip(f"migrations 002/004/005 not applied ({missing}); run `uv run datasources-db migrate`")
    try:
        lock = GraphWriterLock(database, is_sample=True, purpose="pytest summaries").acquire()
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


# ----------------------------------------------------------------------------- helpers
def extractive(request):
    return extractive_output(request)


def summarizer(db, script=extractive, **overrides):
    settings = SummarySettings(**{"enabled": True, "retry_base_minutes": 10.0, **overrides})
    return build_summary_runtime(db, settings, backend=ScriptedSummaryBackend(script))


def ctx(db, grt, srt=None, now=None) -> PipelineContext:
    return PipelineContext(database=db, analyzer=grt, summarizer=srt, now=now)


def ingest(db, grt, srt, raw, now=None):
    result = run_pipeline([envelope(raw)], ctx(db, grt, srt, now))
    assert not result.errors, result.errors
    (outcome,) = result.outcomes
    assert outcome.error is None, outcome.error
    return outcome


def trace(db, grt, srt, raw) -> list[str]:
    state = {"envelopes": [envelope(raw)], "outcomes": [], "errors": []}
    return [name for update in get_graph().stream(state, context=ctx(db, grt, srt), stream_mode="updates")
            for name in update]


def event_of(db, revision_id: str, relation: str | None = None) -> str:
    rows = [m for m in memberships(db, revision_id) if relation is None or m["relation"] == relation]
    return rows[0]["event_id"]


def versions(db, event_id: str) -> list[dict]:
    return q(db, "SELECT summary_id::text AS id, version, status, headline, generation_reason, input_manifest "
                 "FROM event_summary WHERE event_id = %s ORDER BY version", event_id)


def job(db, event_id: str) -> dict | None:
    rows = q(db, "SELECT * FROM event_summary_job WHERE event_id = %s", event_id)
    return rows[0] if rows else None


def attempts(db, event_id: str) -> list[dict]:
    return q(db, "SELECT outcome, attempt_number, error_kind, validation_errors, output FROM event_summary_attempt "
                 "WHERE event_id = %s ORDER BY created_at", event_id)


def sctx(db, srt, *, now=None, limit=5) -> SummaryContext:
    return SummaryContext(db, srt, SummaryBudget(limit), now)


def conflict_label(segment, event):
    return {"label": "same_event_conflict",
            "differences": [{"aspect": "price", "incoming": "$3.4 billion", "candidate": "$2.1 billion",
                             "kind": "conflict"}]}


def with_conflict(db) -> SimpleNamespace:
    """An event with a published v1 and an incoming conflicting article (review open)."""
    at = anchor()
    grt, srt = grouping_runtime(db, scripted(lambda s, e: "different_event")), summarizer(db)
    first = ingest(db, grt, srt, article(*ACQ, at))
    event_id = event_of(db, first.revision_id)
    grt.comparator = scripted(conflict_label)
    calls = len(srt.backend.calls)
    conflict_raw = article(*CONFLICT_TEXT, at + timedelta(hours=3))
    conflict = ingest(db, grt, srt, conflict_raw)
    assert len(srt.backend.calls) == calls  # no summary call while the conflict is open
    (review_row,) = q(db, "SELECT v.review_id::text AS id, v.status FROM event_conflict_review v JOIN "
                          "event_comparison c ON c.comparison_id = v.comparison_id WHERE c.candidate_event_id = %s",
                      event_id)
    return SimpleNamespace(at=at, grt=grt, srt=srt, first=first, conflict=conflict, conflict_raw=conflict_raw,
                           event_id=event_id, review_id=review_row["id"])


# ----------------------------------------------------------------------------- first summary / updates / repeats
def test_first_summary_for_a_single_source_event_is_published_with_citations(db):
    grt, srt = grouping_runtime(db), summarizer(db)
    raw = article(*ACQ, anchor())
    nodes = trace(db, grt, srt, raw)
    assert nodes[-6:] == ["persist_result", "route_event_summary", "generate_event_summary",
                          "validate_event_summary", "persist_event_summary", "route_event_summary"]
    revision = q(db, "SELECT current_revision_id::text AS r FROM document WHERE source_record_id = %s",
                 str(raw["id"]))[0]["r"]
    event_id = event_of(db, revision)
    (v1,) = versions(db, event_id)
    assert v1["version"] == 1 and v1["status"] == "published" and v1["generation_reason"] == "initial"
    assert v1["input_manifest"]["member_revision_ids"] == [revision]
    cited = q(db, "SELECT t.revision_id::text AS r, c.statement_key, c.evidence_quote FROM event_summary_citation c "
                  "JOIN text_unit t ON t.unit_id = c.unit_id WHERE c.summary_id = %s", v1["id"])
    assert cited and {c["r"] for c in cited} == {revision} and {c["statement_key"] for c in cited} >= {"H1", "O1"}
    assert job(db, event_id)["status"] == "completed" and [a["outcome"] for a in attempts(db, event_id)] == \
        ["published"]
    assert len(srt.backend.calls) == 1


def test_update_publishes_a_new_version_and_preserves_the_old_one(db):
    at = anchor()
    grt, srt = grouping_runtime(db, scripted(lambda s, e: "different_event")), summarizer(db)
    first = ingest(db, grt, srt, article(*ACQ, at))
    event_id = event_of(db, first.revision_id)
    (v1,) = versions(db, event_id)
    v1_citations = q(db, "SELECT unit_id, statement_key, evidence_quote FROM event_summary_citation "
                         "WHERE summary_id = %s ORDER BY statement_key", v1["id"])

    grt.comparator = scripted(lambda s, e: "same_event_update")
    second = ingest(db, grt, srt, article(*UPDATE, at + timedelta(days=2)))
    assert second.summaries == [{"event_id": event_id, "route": "generate", "relation": "same_event_update",
                                 "reason": "the record adds new information to the event", "result": "published",
                                 "version": 2}]
    old, new = versions(db, event_id)
    assert (old["status"], new["status"], new["generation_reason"]) == ("superseded", "published", "new_information")
    assert old["headline"] == v1["headline"]  # old content preserved
    assert q(db, "SELECT unit_id, statement_key, evidence_quote FROM event_summary_citation WHERE summary_id = %s "
                 "ORDER BY statement_key", old["id"]) == v1_citations
    request = srt.backend.calls[-1]
    assert request.pack.previous.version == 1  # previous summary given as context...
    assert {p.revision_id for p in request.pack.passages} == {first.revision_id, second.revision_id}  # ...evidence too
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        with db.transaction() as conn:
            conn.execute("UPDATE event_summary SET headline = 'rewritten' WHERE summary_id = %s", (old["id"],))


def test_repeated_and_exact_copy_articles_make_no_summary_call_but_are_listed_as_sources(db):
    from fastapi.testclient import TestClient

    from datasources import server

    at = anchor()
    grt, srt = grouping_runtime(db, scripted(lambda s, e: "different_event")), summarizer(db)
    body = ("NVIDIA said on Monday it agreed to acquire Example Robotics, a maker of warehouse robots, for $2.1 "
            "billion in cash. The companies expect the acquisition to close in the first quarter of next year, "
            "subject to regulatory approvals and customary closing conditions.\n\nExample Robotics employs about "
            "900 people and will continue to operate from its headquarters, NVIDIA said in a statement on Monday.")
    first = ingest(db, grt, srt, article("NVIDIA agrees to buy Example Robotics", body, at))
    event_id = event_of(db, first.revision_id)
    calls = len(srt.backend.calls)

    grt.comparator = scripted(lambda s, e: "same_event_repeated")
    repeat = ingest(db, grt, srt, article(*ACQ_PARAPHRASE, at + timedelta(hours=1)))
    copy = ingest(db, grt, srt, article("NVIDIA agrees to buy Example Robotics", body, at + timedelta(hours=2),
                                        source="pytest-other-wire"))
    assert q(db, "SELECT method FROM event_comparison WHERE revision_id = %s", copy.revision_id)[0]["method"] == \
        "exact_content"
    assert len(srt.backend.calls) == calls  # neither repeat called the summary model
    assert [s["result"] for s in repeat.summaries + copy.summaries] == ["skipped", "skipped"]
    assert [v["version"] for v in versions(db, event_id)] == [1]

    server.app.dependency_overrides[server.get_database] = lambda: db
    try:
        detail = TestClient(server.app).get(f"/api/events/{event_id}", params={"include_sample": "true"}).json()
    finally:
        server.app.dependency_overrides.clear()
    by_revision = {s["revision_id"]: s for s in detail["sources"]}
    assert set(by_revision) == {first.revision_id, repeat.revision_id, copy.revision_id}
    for later in (repeat.revision_id, copy.revision_id):  # listed, but never presented as v1's evidence
        assert by_revision[later]["added_after_current_summary"] is True
        assert by_revision[later]["cited_in_current_summary"] is False
        assert by_revision[later]["in_current_summary_input"] is False
    assert by_revision[first.revision_id]["cited_in_current_summary"] is True


# ----------------------------------------------------------------------------- several quotes from one unit
def test_every_distinct_quote_from_one_unit_survives_saving_retrieval_and_replay(db, capsys):
    import json

    from fastapi.testclient import TestClient

    from datasources import server

    keys = q(db, "SELECT pg_get_constraintdef(oid) AS d FROM pg_constraint "
                 "WHERE conrelid = 'public.event_summary_citation'::regclass AND contype = 'u'")
    assert [k["d"] for k in keys] == ["UNIQUE (summary_id, statement_key, unit_id, quote_index)"]

    price = "agreed to acquire Example Robotics for $2.1 billion in cash"
    closing = "expect the deal to close in the first quarter of next year"
    body = f"NVIDIA said on Monday it {price}. The companies {closing}, subject to regulatory approval."

    def two_quotes(request):
        (passage,) = request.pack.passages  # one short article: one text unit
        cites = [{"passage_ref": passage.ref, "quote": price}, {"passage_ref": passage.ref, "quote": closing},
                 {"passage_ref": passage.ref, "quote": "Agreed to  acquire Example Robotics for $2.1 BILLION in cash"}]
        text = "NVIDIA agreed to buy Example Robotics for $2.1 billion; closing is expected next year."
        return {"headline": {"id": "H1", "text": text, "citations": cites},
                "overview": [{"id": "O1", "text": text, "citations": cites[:1]}], "key_points": [],
                "evidence_gaps": []}

    grt, srt = grouping_runtime(db), summarizer(db, two_quotes)
    raw = article("NVIDIA agrees to buy Example Robotics", body, anchor())
    first = ingest(db, grt, srt, raw)
    event_id = event_of(db, first.revision_id)
    (v1,) = versions(db, event_id)

    def citations(summary_id):
        return q(db, "SELECT citation_id::text AS id, statement_key, unit_id::text AS unit_id, quote_index, "
                     "evidence_quote FROM event_summary_citation WHERE summary_id = %s "
                     "ORDER BY statement_key, unit_id, quote_index", summary_id)

    rows = citations(v1["id"])
    headline = [r for r in rows if r["statement_key"] == "H1"]
    # Both different quotes are kept, separately and in order; the normalized duplicate is stored once.
    assert [(r["quote_index"], r["evidence_quote"]) for r in headline] == [(1, price), (2, closing)]
    assert len({r["unit_id"] for r in headline}) == 1
    assert [(r["quote_index"], r["evidence_quote"]) for r in rows if r["statement_key"] == "O1"] == [(1, price)]
    stored = q(db, "SELECT statements FROM event_summary WHERE summary_id = %s", v1["id"])[0]["statements"]
    assert [(c["quote_index"], c["quote"]) for c in stored[0]["citations"]] == [(1, price), (2, closing)]

    server.app.dependency_overrides[server.get_database] = lambda: db
    try:
        detail = TestClient(server.app).get(f"/api/events/{event_id}", params={"include_sample": "true"}).json()
    finally:
        server.app.dependency_overrides.clear()
    api_h1 = next(s for s in detail["summary"]["statements"] if s["id"] == "H1")
    assert [(c["quote_index"], c["quote"]) for c in api_h1["citations"]] == [(1, price), (2, closing)]
    assert {c["source"]["revision_id"] for c in api_h1["citations"]} == {first.revision_id}

    assert summary_cli.main(["show", "--event-id", event_id]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert [(c["quote_index"], c["evidence_quote"]) for c in shown["citations"] if c["statement_key"] == "H1"] == \
        [(1, price), (2, closing)]

    # Replay: the same payload again, then the event again, changes nothing and calls nothing.
    calls = len(srt.backend.calls)
    replay = ingest(db, grt, srt, raw)
    assert replay.persist_outcome == "skipped" and len(srt.backend.calls) == calls
    assert process_event(event_id, sctx(db, srt)).result == "skipped"
    assert citations(v1["id"]) == rows and len(srt.backend.calls) == calls

    # Regenerating the same output numbers the quotes the same way; v1's rows are untouched.
    assert process_event(event_id, sctx(db, srt), refresh=True).result == "published"
    old, new = versions(db, event_id)
    assert citations(old["id"]) == rows
    assert [(r["statement_key"], r["quote_index"], r["evidence_quote"]) for r in citations(new["id"])] == \
        [(r["statement_key"], r["quote_index"], r["evidence_quote"]) for r in rows]


# ----------------------------------------------------------------------------- multi-event filings
def test_each_event_of_a_multi_item_filing_is_summarized_from_its_own_evidence(db):
    at = anchor()
    grt, srt = grouping_runtime(db, scripted(lambda s, e: "different_event")), summarizer(db)
    accession = f"9999999999-35-{uuid.uuid4().int % 1_000_000:06d}"
    result = run_pipeline([sec_envelope(at, accession)], ctx(db, grt, srt))
    (filing,) = result.outcomes
    events = {m["event_id"] for m in memberships(db, filing.revision_id)}
    assert len(events) == 2 and len(srt.backend.calls) == 2
    assert [s["result"] for s in filing.summaries] == ["published", "published"]
    for request in srt.backend.calls:
        own_units = {r["unit_id"] for r in q(db, "SELECT mu.unit_id::text FROM event_membership m JOIN "
                                                 "event_membership_unit mu ON mu.membership_id = m.membership_id "
                                                 "WHERE m.event_id = %s", request.pack.event_id)}
        assert {p.unit_id for p in request.pack.passages} <= own_units  # never the other Item's text
    texts = {request.pack.event_id: " ".join(p.text for p in request.pack.passages) for request in srt.backend.calls}
    assert sum("repurchase" in t for t in texts.values()) == 1 and sum("retire" in t for t in texts.values()) == 1

    # Evidence from the other event is refused at publication even if validation were bypassed.
    first_id, second_id = sorted(events)
    task = process_event(first_id, sctx(db, srt), trigger="job", refresh=False)
    assert task.result == "skipped"  # already summarized, nothing queued
    other_unit = q(db, "SELECT mu.unit_id::text AS u FROM event_membership m JOIN event_membership_unit mu "
                       "ON mu.membership_id = m.membership_id WHERE m.event_id = %s LIMIT 1", second_id)[0]["u"]
    from datasources.summary import evidence
    from datasources.summary.models import Citation, Statement, SummaryDraft, SummaryTask

    with db.connection() as conn:
        pack = evidence.build_pack(conn, first_id, srt.settings)
    forged = SummaryTask(event_id=first_id, language="en", pack=pack, generation_reason="manual_refresh",
                         draft=SummaryDraft([Statement("H1", "headline", "forged", [Citation(other_unit, "x" * 9)]),
                                             Statement("O1", "overview", "forged", [Citation(other_unit, "x" * 9)])]))
    with db.transaction() as conn:
        outcome = store.publish(conn, forged, provider="scripted", now=datetime.now(timezone.utc))
    assert outcome.result == "stale" and "no longer an active member" in outcome.detail


# ----------------------------------------------------------------------------- conflicts and review
def test_conflict_blocks_automatic_publication_and_keeps_the_current_summary(db):
    c = with_conflict(db)
    grt, srt, event_id = c.grt, c.srt, c.event_id
    assert c.conflict.summaries[0]["result"] == "awaiting_review" and c.conflict.summaries[0]["reviews_created"] == 1
    assert [v["status"] for v in versions(db, event_id)] == ["published"]  # v1 stays up
    assert job(db, event_id)["status"] == "awaiting_review"
    flag = q(db, "SELECT under_review, open_review_count FROM current_event_summary WHERE event_id = %s", event_id)[0]
    assert flag == {"under_review": True, "open_review_count": 1}

    calls = len(srt.backend.calls)
    replay = ingest(db, grt, srt, c.conflict_raw)
    grt.comparator = scripted(lambda s, e: "same_event_update")
    update = ingest(db, grt, srt, article(*UPDATE, c.at + timedelta(days=1)))
    assert len(srt.backend.calls) == calls  # neither a replay nor an update publishes past an open conflict
    assert replay.analysis_status.startswith("reused") and update.summaries[0]["result"] == "awaiting_review"
    assert q(db, "SELECT count(*) AS n FROM event_conflict_review v JOIN event_comparison c ON "
                 "c.comparison_id = v.comparison_id WHERE c.candidate_event_id = %s", event_id)[0]["n"] == 1


def test_conflict_on_an_event_without_a_summary_waits_for_review_without_a_factual_summary(db):
    at = anchor()
    grt = grouping_runtime(db, scripted(lambda s, e: "different_event"))
    first = ingest(db, grt, None, article(*ACQ, at))  # summaries off for the first article
    event_id = event_of(db, first.revision_id)
    grt.comparator = scripted(conflict_label)
    srt = summarizer(db)
    ingest(db, grt, srt, article(*CONFLICT_TEXT, at + timedelta(hours=1)))
    assert srt.backend.calls == [] and versions(db, event_id) == []
    assert job(db, event_id)["status"] == "awaiting_review"


def test_not_a_conflict_decision_is_audited_and_queues_regeneration(db):
    c = with_conflict(db)
    srt, conflict, event_id, review_id = c.srt, c.conflict, c.event_id, c.review_id
    result = review.decide(db, review_id, decision="not_a_conflict", reviewer="analyst-1",
                           explanation="$3.4 billion includes assumed debt; compatible with $2.1 billion cash",
                           language="en")
    assert result["resulting_status"] == "resolved" and result["queued_jobs"] == [
        {"event_id": event_id, "queued": True, "reason": "review_resolution"}]
    assert job(db, event_id)["status"] == "pending"
    task = process_event(event_id, sctx(db, srt))
    assert task.result == "published" and task.version == 2
    v2 = versions(db, event_id)[-1]
    assert v2["generation_reason"] == "review_resolution"
    assert conflict.revision_id in v2["input_manifest"]["member_revision_ids"]
    audit = q(db, "SELECT decision, reviewer_id, explanation, resulting_status FROM event_review_decision "
                  "WHERE review_id = %s", review_id)
    assert audit == [{"decision": "not_a_conflict", "reviewer_id": "analyst-1",
                      "explanation": "$3.4 billion includes assumed debt; compatible with $2.1 billion cash",
                      "resulting_status": "resolved"}]
    with pytest.raises(review.ReviewError, match="already resolved"):
        review.decide(db, review_id, decision="confirmed_conflict", reviewer="analyst-2", explanation="x",
                      language="en")


def test_insufficient_evidence_keeps_the_review_blocking(db):
    c = with_conflict(db)
    srt, event_id, review_id = c.srt, c.event_id, c.review_id
    result = review.decide(db, review_id, decision="insufficient_evidence", reviewer="analyst-1",
                           explanation="need the merger agreement exhibit", language="en")
    assert result["resulting_status"] == "in_review" and result["queued_jobs"] == []
    task = process_event(event_id, sctx(db, srt), refresh=True)
    assert task.route == "conflict" and task.result == "awaiting_review"
    assert len(versions(db, event_id)) == 1


def test_confirmed_conflict_summary_must_attribute_both_claims(db):
    c = with_conflict(db)
    srt, first, conflict, event_id, review_id = c.srt, c.first, c.conflict, c.event_id, c.review_id
    review.decide(db, review_id, decision="confirmed_conflict", reviewer="analyst-1",
                  explanation="the two reports give different prices", language="en")
    one_sided = [0]

    def script(request):
        own = [p for p in request.pack.passages if p.revision_id == first.revision_id]
        if one_sided[0] == 0:
            one_sided[0] += 1
            quote = " ".join(own[0].text.split()[:10])
            item = {"id": "H1", "text": quote, "citations": [{"passage_ref": own[0].ref, "quote": quote}]}
            return {"headline": item, "overview": [{**item, "id": "O1"}], "key_points": [], "evidence_gaps": []}
        other = [p for p in request.pack.passages if p.revision_id == conflict.revision_id]
        statements = []
        for sid, passage in (("O1", own[0]), ("O2", other[0])):
            quote = " ".join(passage.text.split()[:10])
            statements.append({"id": sid, "text": quote, "citations": [{"passage_ref": passage.ref, "quote": quote}]})
        return {"headline": {**statements[0], "id": "H1"}, "overview": statements, "key_points": [],
                "evidence_gaps": []}

    srt.backend.script = script
    first_try = process_event(event_id, sctx(db, srt))
    assert first_try.result == "invalid" and any("both sides" in e for e in first_try.validation_errors)
    assert job(db, event_id)["status"] == "pending" and len(versions(db, event_id)) == 1
    assert attempts(db, event_id)[-1]["output"] is not None  # the rejected output is kept, not published
    retried = process_event(event_id, sctx(db, srt), ignore_backoff=True)
    assert retried.result == "published" and "reviewed_conflicts" in srt.backend.calls[-1].render()


def test_different_events_decision_corrects_membership_before_regeneration(db):
    c = with_conflict(db)
    srt, conflict, event_id, review_id = c.srt, c.conflict, c.event_id, c.review_id
    calls = len(srt.backend.calls)
    result = review.decide(db, review_id, decision="different_events", reviewer="analyst-1",
                           explanation="the report concerns a different, later bid", language="en")
    (correction,) = result["membership_corrections"]
    assert correction["action"] == "detached" and correction["revision_id"] == conflict.revision_id
    new_event = correction["new_event_id"]
    states = {m["event_id"]: (m["relation"], m["state"]) for m in memberships(db, conflict.revision_id)}
    assert states == {event_id: ("same_event_conflict", "detached"), new_event: ("origin", "active")}
    assert {j["event_id"] for j in result["queued_jobs"]} == {event_id, new_event}
    assert q(db, "SELECT membership_corrections FROM event_review_decision WHERE review_id = %s",
             review_id)[0]["membership_corrections"][0]["new_event_id"] == new_event

    original = process_event(event_id, sctx(db, srt))
    assert original.result == "unchanged"  # its summarized evidence is exactly what v1 used: no call
    split = process_event(new_event, sctx(db, srt))
    assert split.result == "published" and len(srt.backend.calls) == calls + 1
    assert {p.revision_id for p in srt.backend.calls[-1].pack.passages} == {conflict.revision_id}


# ----------------------------------------------------------------------------- unresolved grouping evidence
def test_uncertain_relations_are_never_summarized_into_the_existing_event(db):
    at = anchor()
    grt, srt = grouping_runtime(db, scripted(lambda s, e: "different_event")), summarizer(db)
    first = ingest(db, grt, srt, article(*ACQ, at))
    event_id = event_of(db, first.revision_id)
    grt.comparator = scripted(lambda s, e: "uncertain")
    vague = ingest(db, grt, srt, article("NVIDIA deal talk continues", "Talks continue, people said, about a robotics "
                                         "deal that could be worth billions of dollars.", at + timedelta(hours=1)))
    routes = {s["event_id"]: s for s in vague.summaries}
    assert routes[event_id]["route"] == "skip" and "uncertain" in routes[event_id]["reason"]
    own = event_of(db, vague.revision_id, "origin")
    assert routes[own]["result"] == "published"  # the article's own provisional event is summarized alone
    refreshed = process_event(event_id, sctx(db, srt), refresh=True)
    assert refreshed.result == "published"
    assert vague.revision_id not in {p.revision_id for p in srt.backend.calls[-1].pack.passages}


# ----------------------------------------------------------------------------- failures, replay, recovery
def test_summary_failure_keeps_the_article_and_grouping_and_schedules_a_retry(db):
    at = anchor()

    def outage(request):
        raise SummaryError("simulated timeout", kind="timeout", retryable=True)

    grt, srt = grouping_runtime(db), summarizer(db, outage)
    outcome = ingest(db, grt, srt, article(*OTHER, at))
    assert outcome.persisted and outcome.analysis_status == "completed" and outcome.summaries[0]["result"] == "error"
    event_id = event_of(db, outcome.revision_id)
    assert q(db, "SELECT count(*) AS n FROM document WHERE document_id = %s", outcome.document_id)[0]["n"] == 1
    row = job(db, event_id)
    assert row["status"] == "pending" and row["attempts"] == 1 and row["next_retry_at"] > datetime.now(timezone.utc)
    assert "simulated timeout" in row["last_error"] and attempts(db, event_id)[0]["error_kind"] == "timeout"
    assert versions(db, event_id) == []

    def fabricated(request):
        output = extractive_output(request)
        output["headline"]["citations"][0]["quote"] = "NVIDIA announced record profits of $99 billion"
        return output

    srt.backend.script = fabricated
    task = process_event(event_id, sctx(db, srt), ignore_backoff=True)
    assert task.result == "invalid" and any("quote not found" in e for e in task.validation_errors)
    assert [a["outcome"] for a in attempts(db, event_id)] == ["error", "invalid"] and versions(db, event_id) == []


def test_replay_and_recovery_never_repeat_summary_calls(db):
    at = anchor()
    grt, srt = grouping_runtime(db, scripted(lambda s, e: "different_event")), summarizer(db)
    raw = article(*ACQ, at)
    first = ingest(db, grt, srt, raw)
    calls = len(srt.backend.calls)
    replay = ingest(db, grt, srt, raw)  # same payload again (e.g. a crash before the ledger write)
    assert replay.persist_outcome == "skipped" and len(srt.backend.calls) == calls

    # Crash after the article's commit, before any summary work: found and finished by backfill.
    lost = ingest(db, grt, None, article(*OTHER, at + timedelta(hours=1)))
    lost_event = event_of(db, lost.revision_id)
    with db.connection() as conn:
        assert lost_event in store.select_missing(conn, is_sample=True, language="en", limit=500)
    assert process_event(lost_event, sctx(db, srt)).result == "published"
    again = process_event(lost_event, sctx(db, srt))
    assert again.result == "skipped" and len(srt.backend.calls) == calls + 1

    # A failed update is retried when the next record for the event is only a repeat.
    event_id = event_of(db, first.revision_id)
    grt.comparator = scripted(lambda s, e: "same_event_update")
    srt.backend.script = lambda request: (_ for _ in ()).throw(SummaryError("down", kind="connection",
                                                                            retryable=True))
    failed = ingest(db, grt, srt, article(*UPDATE, at + timedelta(days=1)))
    assert failed.summaries[0]["result"] == "error" and job(db, event_id)["status"] == "pending"
    srt.backend.script = extractive
    grt.comparator = scripted(lambda s, e: "same_event_repeated")
    later = datetime.now(timezone.utc) + timedelta(hours=2)  # past the retry backoff
    repeat = ingest(db, grt, srt, article(*ACQ_PARAPHRASE, at + timedelta(days=1, hours=1)), now=lambda: later)
    assert repeat.summaries[0]["result"] == "published" and repeat.summaries[0]["relation"] == "same_event_repeated"
    assert versions(db, event_id)[-1]["version"] == 2


def test_stale_drafts_and_concurrent_publishers_never_publish_two_versions(db):
    at = anchor()
    grt, srt = grouping_runtime(db, scripted(lambda s, e: "different_event")), summarizer(db)
    first = ingest(db, grt, srt, article(*ACQ, at))
    event_id = event_of(db, first.revision_id)

    # The event gains a member while the model is writing: the draft is stale and requeued.
    def while_generating(request):
        other = grouping_runtime(db, scripted(lambda s, e: "same_event_update"))
        ingest(db, other, None, article(*UPDATE, at + timedelta(days=1)))
        return extractive_output(request)

    srt.backend.script = while_generating
    stale = process_event(event_id, sctx(db, srt), refresh=True)
    assert stale.result == "stale" and "evidence changed" in stale.reason
    assert [v["status"] for v in versions(db, event_id)] == ["published"]
    assert job(db, event_id)["status"] == "pending" and attempts(db, event_id)[-1]["outcome"] == "stale"
    srt.backend.script = extractive
    assert process_event(event_id, sctx(db, srt)).result == "published"

    # Another process published meanwhile: the version check refuses the second draft.
    def rival(request):
        with db.transaction() as conn:
            current = conn.execute("SELECT * FROM event_summary WHERE event_id = %s AND status = 'published'",
                                   (event_id,)).fetchone()
            conn.execute("UPDATE event_summary SET status = 'superseded' WHERE summary_id = %s",
                         (current["summary_id"],))
            conn.execute("INSERT INTO event_summary (event_id, version, headline, summary_text, status, "
                         "generation_reason, model_provider, model_name, prompt_version, input_fingerprint, "
                         "published_at) VALUES (%s, %s, 'rival', 'rival', 'published', 'manual_refresh', 'x', 'x', "
                         "'x', 'x', now())", (event_id, current["version"] + 1))
        return extractive_output(request)

    srt.backend.script = rival
    assert process_event(event_id, sctx(db, srt), refresh=True).result == "stale"
    assert q(db, "SELECT count(*) AS n FROM event_summary WHERE event_id = %s AND status = 'published'",
             event_id)[0]["n"] == 1

    # A live lease held by another process: no second model call for the same event.
    with db.transaction() as conn:
        store.request_job(conn, event_id, "en", "manual_refresh", now=datetime.now(timezone.utc),
                          reset_attempts=True)
        store.claim_job(conn, event_id, "en", owner="other-process", lease_minutes=10,
                        now=datetime.now(timezone.utc))
    srt.backend.script = extractive
    calls = len(srt.backend.calls)
    busy = process_event(event_id, sctx(db, srt), refresh=True)
    assert busy.result == "skipped" and "another process" in busy.reason and len(srt.backend.calls) == calls


def test_per_run_request_limit_defers_durably(db):
    at = anchor()
    grt, srt = grouping_runtime(db), summarizer(db, max_requests_per_run=1)
    accession = f"9999999999-36-{uuid.uuid4().int % 1_000_000:06d}"
    (filing,) = run_pipeline([sec_envelope(at, accession)], ctx(db, grt, srt)).outcomes
    assert sorted(s["result"] for s in filing.summaries) == ["deferred", "published"]
    deferred = next(s["event_id"] for s in filing.summaries if s["result"] == "deferred")
    assert job(db, deferred)["status"] == "pending" and job(db, deferred)["attempts"] == 0


# ----------------------------------------------------------------------------- startup catch-up
def distinct_article(n: int, at: datetime) -> dict:
    tag = uuid.uuid4().hex[:8]
    return article(f"Example Holdings program {n} update {tag}",
                   f"Example Holdings said on Monday that program {n} reached milestone {tag} after a review by its "
                   f"board.\n\nThe company expects the next step of program {n} within weeks.", at)


def test_startup_catch_up_summarizes_recent_events_without_a_summary_and_nothing_else(db):
    at = anchor()
    grt = grouping_runtime(db, scripted(lambda s, e: "different_event"))
    srt = summarizer(db)

    def stored(n: int, when: datetime) -> str:  # stored and grouped; the summary step never ran
        return event_of(db, ingest(db, grt, None, distinct_article(n, when)).revision_id)

    old = stored(1, at - timedelta(days=10))  # outside the 3-day window
    missing = stored(2, at - timedelta(days=1))
    interrupted = stored(3, at - timedelta(hours=2))  # a process died mid-summary; its lease has expired
    waiting = stored(4, at - timedelta(hours=3))  # retry backoff not over yet
    gave_up = stored(5, at - timedelta(hours=4))  # attempts used up
    covered = event_of(db, ingest(db, grt, srt, distinct_article(6, at - timedelta(hours=5))).revision_id)
    ingest(db, None, None, distinct_article(7, at - timedelta(hours=6)))  # stored, never grouped
    with db.transaction() as conn:
        store.request_job(conn, interrupted, "en", "initial", now=at - timedelta(hours=1), reset_attempts=True)
        store.claim_job(conn, interrupted, "en", owner="dead-process", lease_minutes=10, now=at - timedelta(hours=1))
        store.request_job(conn, waiting, "en", "initial", now=at + timedelta(hours=1), reset_attempts=False)
        store.request_job(conn, gave_up, "en", "initial", now=at, reset_attempts=True)
        conn.execute("UPDATE event_summary_job SET status = 'failed', attempts = 4 WHERE event_id = %s", (gave_up,))
    calls = len(srt.backend.calls)

    out = catchup.run_catchup(db, srt, clock=lambda: at, is_sample=True)
    assert [e["event_id"] for e in out["events"]] == [interrupted, waiting, missing]  # newest first
    assert out["results"] == {"published": 2, "deferred": 1} and out["model_requests"] == 2
    assert len(srt.backend.calls) == calls + 2 and out["left_queued"] == 0 and out["ungrouped_articles"] == 1
    assert job(db, interrupted)["status"] == "completed" and job(db, missing)["status"] == "completed"
    assert "backoff" in out["events"][1]["reason"] and job(db, waiting)["status"] == "pending"
    assert job(db, old) is None and versions(db, old) == []
    assert job(db, gave_up)["status"] == "failed" and versions(db, gave_up) == []
    assert [v["version"] for v in versions(db, covered)] == [1]

    again = catchup.run_catchup(db, srt, clock=lambda: at, is_sample=True)  # the next start
    assert again["results"] == {"deferred": 1} and len(srt.backend.calls) == calls + 2


def test_startup_catch_up_stops_at_its_request_cap_or_a_stop_request_and_keeps_the_rest_queued(db):
    at = anchor()
    grt = grouping_runtime(db, scripted(lambda s, e: "different_event"))
    srt = summarizer(db, startup_catchup_max_requests=1)
    events = [event_of(db, ingest(db, grt, None, distinct_article(n, at - timedelta(hours=n))).revision_id)
              for n in (1, 2, 3)]

    stopped = catchup.run_catchup(db, srt, clock=lambda: at, stop=lambda: True, is_sample=True)
    assert stopped["events"] == [] and stopped["left_queued"] == 3 and srt.backend.calls == []
    assert [job(db, e)["status"] for e in events] == ["pending"] * 3  # queued durably for the next start

    capped = catchup.run_catchup(db, srt, clock=lambda: at, is_sample=True)
    assert capped["results"] == {"published": 1} and capped["left_queued"] == 2 and len(srt.backend.calls) == 1
    assert versions(db, events[0]) and not versions(db, events[1]) and not versions(db, events[2])


# ----------------------------------------------------------------------------- live activity for the page
def steps_of(item: dict) -> list[tuple[str, str, str | None]]:
    return [(s["key"], s["state"], s["result"]) for s in item["steps"]]


def test_each_record_reports_its_steps_from_search_to_summary(db, tmp_path):
    from datasources.activity import ActivityTracker, load_activity

    at = anchor()
    track = ActivityTracker(tmp_path, "worker", heartbeat=0)
    grt, srt = grouping_runtime(db, scripted(lambda s, e: "same_event_update")), summarizer(db)

    def run(raw) -> None:  # what the worker's run_graph does around each record
        result = run_pipeline([envelope(raw)], PipelineContext(database=db, analyzer=grt, summarizer=srt,
                                                               activity=track))
        assert not result.errors and result.outcomes[0].error is None
        track.finish_item()

    run(article(*ACQ, at))
    run(article(*UPDATE, at + timedelta(days=1)))
    newest, oldest = load_activity(tmp_path, "worker")["recent"]
    assert oldest["kind"] == "article" and oldest["title"] == ACQ[0] and oldest["revision_id"]
    assert steps_of(oldest) == [("similar", "done", "no related story found"),
                                ("save", "done", "saved; started a new story"),
                                ("summary", "done", "published as version 1")]
    assert steps_of(newest) == [("similar", "done", "1 possibly related story"),
                                ("compare", "done", "same story, new details"),
                                ("save", "done", "saved; added to an existing story"),
                                ("summary", "done", "published as version 2")]
    compare = newest["steps"][1]
    assert compare["label"] == "Comparing with 1 story" and compare["model"]
    assert newest["steps"][3]["model"] == srt.model and oldest["steps"][1]["model"] is None
    track.close("stopped")


def test_the_catch_up_reports_each_event_and_a_rejected_summary(db, tmp_path):
    from datasources.activity import ActivityTracker, load_activity

    at = anchor()
    grt = grouping_runtime(db, scripted(lambda s, e: "different_event"))
    event_id = event_of(db, ingest(db, grt, None, distinct_article(1, at - timedelta(hours=1))).revision_id)

    def fabricated(request):
        output = extractive_output(request)
        output["headline"]["citations"][0]["quote"] = "Example Holdings announced record profits of $99 billion"
        return output

    srt = summarizer(db, fabricated)
    track = ActivityTracker(tmp_path, "summary", heartbeat=0)
    catchup.run_catchup(db, srt, clock=lambda: at, is_sample=True, activity=track)
    track.close("finished", note="invalid 1")
    view = load_activity(tmp_path, "summary")
    (item,) = view["recent"]
    assert view["state"] == "finished" and view["note"] == "invalid 1"
    assert view["progress"]["done"] == 1 and view["progress"]["total"] == 1
    assert item["kind"] == "event" and item["event_id"] == event_id and item["state"] == "failed"
    ((key, state, result),) = steps_of(item)
    assert key == "summary" and state == "failed" and result.startswith("rejected by the citation checks (H1: quote")


# ----------------------------------------------------------------------------- isolation and read API
def test_citations_cannot_cross_the_sample_live_partition(db):
    from datasources.db.repository import persist_record
    from datasources.ingest.graph import run_prepare

    at = anchor()
    grt, srt = grouping_runtime(db), summarizer(db)
    sample = ingest(db, grt, srt, article(*OTHER, at))
    summary_id = versions(db, event_of(db, sample.revision_id))[0]["id"]
    (live_record,), _ = run_prepare([{**envelope(article(*ACQ, at)), "is_sample": False}])
    with db.connection() as conn:
        conn.execute("BEGIN")
        try:
            stored = persist_record(conn, live_record, raw_payload={"id": "live"})
            unit_id = conn.execute(
                "INSERT INTO text_unit (revision_id, profile_id, unit_kind, ordinal, input_text, input_hash) "
                "VALUES (%s, %s, 'document', 0, 'live text', %s) RETURNING unit_id",
                (stored.revision_id, grt.profile_id, "a" * 64)).fetchone()["unit_id"]
            with pytest.raises(psycopg.errors.RaiseException, match="partition"):
                conn.execute("INSERT INTO event_summary_citation (summary_id, unit_id, statement_key, evidence_quote) "
                             "VALUES (%s, %s, 'H1', 'live text')", (summary_id, unit_id))
        finally:
            conn.execute("ROLLBACK")


def test_read_endpoints_show_published_pending_and_review_states(db):
    from fastapi.testclient import TestClient

    from datasources import server

    c = with_conflict(db)
    grt, first, conflict, reviewed_event = c.grt, c.first, c.conflict, c.event_id
    pending_out = ingest(db, grt, None, article("NVIDIA opens a lab in Example Town",
                                                 "NVIDIA opened a research lab in Example Town.", anchor()))
    pending_event = event_of(db, pending_out.revision_id)
    with db.transaction() as conn:
        store.request_job(conn, pending_event, "en", "initial", now=datetime.now(timezone.utc), reset_attempts=True)

    server.app.dependency_overrides[server.get_database] = lambda: db
    client = TestClient(server.app)
    try:
        assert client.get(f"/api/events/{reviewed_event}").status_code == 404  # sample data hidden by default
        assert all(e["is_sample"] is False for e in client.get("/api/events").json()["events"])
        detail = client.get(f"/api/events/{reviewed_event}", params={"include_sample": "true"}).json()
        pending = client.get(f"/api/events/{pending_event}", params={"include_sample": "true"}).json()
        listed = client.get("/api/events", params={"include_sample": "true", "limit": 100,
                                                   "state": "pending"}).json()["events"]
        assert client.get("/api/events/not-a-uuid").status_code == 422
        assert client.get("/api/events", params={"state": "bogus"}).status_code == 422
        assert client.post(f"/api/events/{reviewed_event}").status_code in (404, 405)  # read-only
    finally:
        server.app.dependency_overrides.clear()

    assert detail["summary_state"] == "published" and detail["under_review"] is True and detail["open_reviews"] == 1
    summary = detail["summary"]
    assert summary["version"] == 1 and summary["published_at"] and summary["headline"]
    cited = [c for s in summary["statements"] for c in s["citations"]]
    assert cited and all(c["source"]["url"] and c["quote"] for c in cited)
    assert {s["revision_id"] for s in detail["sources"]} == {first.revision_id, conflict.revision_id}
    assert detail["reviews"][0]["status"] == "open" and detail["versions"][0]["version"] == 1
    assert pending["summary_state"] == "pending" and pending["summary"] is None
    assert pending_event in {e["event_id"] for e in listed}
    assert listed and all(e["summary_state"] == "pending" for e in listed)  # filtered in SQL, before the page


def test_summary_cli_status_pending_review_and_paid_call_consent(db, capsys, monkeypatch):
    import json

    c = with_conflict(db)
    srt, event_id, review_id = c.srt, c.event_id, c.review_id

    def run(*argv):
        code = summary_cli.main(list(argv))
        out = capsys.readouterr()
        return code, (json.loads(out.out) if out.out.strip().startswith("{") else None), out.err

    code, listed, _ = run("review", "list", "--sample")
    assert code == 0 and review_id in {r["review_id"] for r in _stringify(listed["reviews"])}
    code, shown, _ = run("review", "show", "--review-id", review_id)
    assert code == 0 and shown["conflicting_claims"][0]["incoming"] == "$3.4 billion"
    assert shown["incoming_source"] and shown["event_passages_compared"]
    code, status, _ = run("status", "--sample")
    assert code == 0 and status["jobs_by_status"].get("awaiting_review", 0) >= 1
    code, pending, _ = run("pending", "--sample")
    assert code == 0 and event_id in pending["awaiting_review"]

    monkeypatch.setattr(summary_cli, "_runtime", lambda database: build_summary_runtime(
        database, SummarySettings(enabled=True), backend=type("Paid", (), {"name": "anthropic", "model": "m"})()))
    code, _, err = run("run-event", "--event-id", event_id)
    assert code == 2 and "--confirm-paid-calls" in err  # no paid call without explicit consent

    code, decided, _ = run("review", "decide", "--review-id", review_id, "--reviewer", "cli-analyst",
                           "--decision", "not_a_conflict", "--explanation", "compatible figures")
    assert code == 0 and decided["resulting_status"] == "resolved"
    monkeypatch.setattr(summary_cli, "_runtime", lambda database: srt)
    code, retried, _ = run("retry", "--sample", "--limit", "5")
    assert code == 0 and retried["results"].get("published", 0) >= 1
    code, shown_event, _ = run("show", "--event-id", event_id)
    assert code == 0 and [s["version"] for s in shown_event["summaries"]] == [1, 2]


def _stringify(rows):
    return [{k: str(v) for k, v in row.items()} for row in rows]
