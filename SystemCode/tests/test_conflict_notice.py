"""Conflict notices on the event endpoints and the event page.

The first tests are offline (the pure functions that turn stored review and
comparison records into a notice). The rest run on an ISOLATED test database:
the module refuses to start unless ``conftest.verify_isolated_database`` confirms
the server-reported database is a test database (never stock_news, whose
retained sample records these tests never read or write).

Fixture: the fictional NVIDIA / Veltrani Photonics acquisition
(``fixtures/synthetic_conflict_notice_veltrani.json``). Its recorded comparison
judgments (labels, differences, verbatim evidence quotes) and summary
statements drive scripted backends through the real compiled graph, so nothing
here calls a model; a guard makes any model client or outbound socket fail.
"""

from __future__ import annotations

import json
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from conftest import FIXTURES, cleanup_sample_rows, isolated_test_database

psycopg = pytest.importorskip("psycopg")

from datasources.ingest.graph import run_pipeline  # noqa: E402
from datasources.similarity import store as similarity_store  # noqa: E402
from datasources.similarity.comparison import _fold  # noqa: E402
from datasources.similarity.lock import GraphWriterLock, WriterLockError  # noqa: E402
from datasources.similarity.pipeline import PipelineContext  # noqa: E402
from datasources.summary import conflicts, review, store  # noqa: E402
from datasources.summary.generation import ScriptedSummaryBackend  # noqa: E402
from datasources.summary.pipeline import SummaryBudget, SummaryContext, build_summary_runtime, process_event  # noqa: E402
from datasources.summary.settings import SummarySettings  # noqa: E402
from test_similarity_pipeline import anchor, article, envelope, memberships, q, scripted  # noqa: E402
from test_similarity_pipeline import runtime as grouping_runtime  # noqa: E402

FIXTURE = json.loads((FIXTURES / "synthetic_conflict_notice_veltrani.json").read_text(encoding="utf-8"))
ARTICLES, JUDGMENTS, SUMMARIES = FIXTURE["articles"], FIXTURE["judgments"], FIXTURE["summaries"]
T0 = datetime(2026, 9, 29, 12, tzinfo=timezone.utc)


class ModelCallAttempted(AssertionError):
    pass


def _refuse(*_args, **_kwargs):
    raise ModelCallAttempted("a model client, model call or outbound socket was attempted")


@pytest.fixture(autouse=True)
def no_model_calls(monkeypatch):
    """Every test: constructing an Anthropic client or calling a real backend fails the test."""
    import anthropic

    from datasources.similarity import comparison
    from datasources.summary import generation

    monkeypatch.setattr(anthropic, "Anthropic", _refuse)
    monkeypatch.setattr(anthropic, "AsyncAnthropic", _refuse)
    monkeypatch.setattr(generation.AnthropicSummaryBackend, "summarize", _refuse)
    monkeypatch.setattr(comparison.AnthropicComparisonBackend, "compare", _refuse)


LOOPBACK = ("127.0.0.1", "::1", "localhost")


@contextmanager
def offline_reads(*summarizers):
    """API reads and page rendering: no Python socket may leave this machine, and no scripted model call runs.

    Loopback stays open (asyncio's self-pipe on Windows and the test server use it); a model API is never on
    loopback. PostgreSQL is reached through libpq (C), which Python socket patches do not affect.
    """
    before = [len(s.backend.calls) for s in summarizers]
    original_connect, original_create = socket.socket.connect, socket.create_connection

    def connect(sock, address):
        if not (isinstance(address, tuple) and address[0] in LOOPBACK):
            _refuse()
        return original_connect(sock, address)

    def create_connection(address, *args, **kwargs):
        if address[0] not in LOOPBACK:
            _refuse()
        return original_create(address, *args, **kwargs)

    socket.socket.connect, socket.create_connection = connect, create_connection
    try:
        yield
    finally:
        socket.socket.connect, socket.create_connection = original_connect, original_create
    assert [len(s.backend.calls) for s in summarizers] == before


def test_the_no_model_guard_refuses_clients_and_outbound_connections():
    import anthropic

    from datasources.summary.generation import AnthropicSummaryBackend

    with pytest.raises(ModelCallAttempted):
        anthropic.Anthropic()
    with pytest.raises(ModelCallAttempted):
        AnthropicSummaryBackend.summarize(None, None)
    with offline_reads(), pytest.raises(ModelCallAttempted):
        socket.create_connection(("api.anthropic.com", 443), timeout=1)
    with offline_reads(), pytest.raises(ModelCallAttempted), socket.socket() as sock:
        sock.connect(("203.0.113.1", 443))  # TEST-NET-3: never reached, refused before sending


# ----------------------------------------------------------------------------- offline: pure notice logic
def review_record(**overrides):
    base = {"review_id": "11111111-2222-3333-4444-555555555555", "status": "open", "resolution": None,
            "review_opened_at": T0, "resolved_at": None, "detected_at": T0, "revision_id": "rev-d",
            "differences": [{"kind": "conflict", "aspect": "purchase price", "incoming": "USD 300 million",
                             "candidate": "USD 250 million"}],
            "evidence": {"incoming": [{"ref": "U1", "quote": "cash consideration of USD 300 million"}],
                         "candidate": [{"ref": "C1", "quote": "for USD 250 million in cash"},
                                       {"ref": "C2", "quote": "valued at USD 250 million"}]},
            "incoming_unit_ids": ["unit-d"], "candidate_unit_ids": ["unit-a", "unit-b"],
            "candidate_revision_ids": ["rev-a", "rev-b"], "membership_state": "active", "corrections": None,
            "last_decision": None}
    return {**base, **overrides}


UNITS = {"unit-a": {"revision_id": "rev-a", "text": "NVIDIA will buy Veltrani for USD 250 million in cash."},
         "unit-b": {"revision_id": "rev-b", "text": "The deal is valued at USD 250 million."},
         "unit-d": {"revision_id": "rev-d", "text": "It specified cash consideration of USD 300 million."}}
SOURCES = {r: {"revision_id": r, "name": n, "title": f"{n} story", "published_at": T0 + timedelta(hours=h),
               "record_type": "news_article", "url": f"https://example.test/{r}"}
           for r, n, h in (("rev-a", "Example Newswire", -20), ("rev-b", "Sample Markets Daily", -10),
                           ("rev-d", "Fixture Finance Journal", -1))}


def dispute(**overrides):
    return conflicts.build_dispute(review_record(**overrides), sources=SOURCES, units=UNITS,
                                   summary_published_at=T0 - timedelta(hours=2), summary_revisions={"rev-a", "rev-b"})


@pytest.mark.parametrize("url, ok", [
    ("https://example.com/a?b=1#c", True), ("http://example.org/x", True), ("HTTPS://EXAMPLE.COM/", True),
    ("javascript:alert(1)", False), ("JaVaScRiPt:alert(1)", False), ("data:text/html,<b>x</b>", False),
    ("/relative/path", False), ("//example.com/no-scheme", False), ("https://", False), ("ftp://example.com", False),
    ('https://example.com/"onmouseover=alert(1)', False), ("https://exa mple.com", False), ("", False), (None, False),
    ("https://example.com/\u0000", False), ("vbscript:msgbox(1)", False),
])
def test_only_absolute_http_links_survive(url, ok):
    assert (conflicts.safe_url(url) == url.strip()) if ok else conflicts.safe_url(url) is None


def test_a_precise_claim_pair_gives_each_source_its_claim_and_quote():
    d = dispute()
    assert d["precision"] == "claim_pair" and d["topic"] == ["purchase price"] and d["notice"] is None
    by_name = {s["name"]: s for s in d["sources"]}
    assert [s["name"] for s in d["sources"]] == ["Example Newswire", "Sample Markets Daily",
                                                 "Fixture Finance Journal"]  # by publication time
    assert by_name["Fixture Finance Journal"]["claims"] == [{"aspect": "purchase price", "value": "USD 300 million"}]
    assert by_name["Fixture Finance Journal"]["quotes"] == ["cash consideration of USD 300 million"]
    assert by_name["Example Newswire"]["claims"] == [{"aspect": "purchase price", "value": "USD 250 million"}]
    # Each candidate quote is attributed only to the source whose text contains it.
    assert by_name["Example Newswire"]["quotes"] == ["for USD 250 million in cash"]
    assert by_name["Sample Markets Daily"]["quotes"] == ["valued at USD 250 million"]
    assert {s["side"] for s in d["sources"]} == {"incoming", "candidate"} and d["unattributed_quotes"] == []
    assert d["summary_predates_detection"] is True and d["summary_includes_report"] is False


def test_a_candidate_source_without_a_located_quote_gets_no_claim():
    d = dispute(evidence={"incoming": [{"ref": "U1", "quote": "USD 300 million"}],
                          "candidate": [{"ref": "C1", "quote": "for USD 250 million in cash"}]})
    assert d["precision"] == "claim_pair"
    assert [s["name"] for s in d["sources"]] == ["Example Newswire", "Fixture Finance Journal"]
    assert d["display_status"] == "under_review" and d["active"] is True and d["outcome"] is None


@pytest.mark.parametrize("case, overrides", [
    ("no differences", {"differences": []}),
    ("difference is not a conflict", {"differences": [{"kind": "new_information", "aspect": "price",
                                                       "incoming": "USD 300 million",
                                                       "candidate": "USD 250 million"}]}),
    ("claim not stated", {"differences": [{"kind": "conflict", "aspect": "purchase price",
                                           "incoming": "USD 300 million", "candidate": "unknown"}]}),
    ("difference missing fields", {"differences": [{"kind": "conflict", "aspect": "purchase price"}, "junk", None]}),
    ("differences not a list", {"differences": {"kind": "conflict"}}),
    ("no incoming quote", {"evidence": {"incoming": [], "candidate": [{"ref": "C1", "quote": "USD 250 million"}]}}),
    ("candidate quote not locatable", {"evidence": {"incoming": [{"ref": "U1", "quote": "USD 300 million"}],
                                                    "candidate": [{"ref": "C1", "quote": "a sentence nobody wrote"}]}}),
    ("evidence empty", {"evidence": {}}),
    ("evidence malformed", {"evidence": {"incoming": "USD 300 million", "candidate": [{"quote": None}, 7]}}),
])
def test_missing_or_incomplete_evidence_falls_back_to_quotes_with_a_generic_notice(case, overrides):
    d = dispute(**overrides)
    assert d["precision"] == "quotes_only" and d["topic"] == [] and d["notice"] == conflicts.GENERIC_NOTICE
    assert all(s["claims"] == [] for s in d["sources"])  # no claim is attributed without a precise pair
    # Every compared source is still listed so readers can check the originals.
    assert {s["revision_id"] for s in d["sources"]} == {"rev-d", "rev-a", "rev-b"}
    stored = conflicts._quotes(overrides.get("evidence", review_record()["evidence"]), "candidate")
    shown = [qt for s in d["sources"] if s["side"] == "candidate" for qt in s["quotes"]] + d["unattributed_quotes"]
    assert sorted(set(shown)) == sorted(stored)  # every stored quote is returned, none invented
    if case == "candidate quote not locatable":
        assert d["unattributed_quotes"] == ["a sentence nobody wrote"]


def test_display_status_follows_the_review_outcome_not_the_closed_flag():
    ds = conflicts.display_status
    assert ds({"status": "open"}, None) == "under_review"
    assert ds({"status": "in_review"}, None) == "under_review"  # insufficient_evidence
    assert ds({"status": "resolved", "resolution": "confirmed_conflict"}, None) == "disputed"
    assert ds({"status": "resolved", "resolution": "not_a_conflict"}, None) == "cleared"
    moved = {"status": "resolved", "resolution": "different_events", "revision_id": "rev-d"}
    assert ds({**moved, "membership_state": "detached"}, {"rev-a"}) == "separated"
    assert ds({**moved, "membership_state": "active"}, {"rev-a"}) == "correction_pending"
    assert ds({**moved, "membership_state": "detached"}, {"rev-a", "rev-d"}) == "correction_pending"
    assert ds({"status": "resolved", "resolution": "something new"}, None) == "under_review"
    # Several disputes: the most urgent wins, so closing one never hides another.
    assert conflicts.event_status(["cleared", "under_review"]) == "under_review"
    assert conflicts.event_status(["disputed", "cleared"]) == "disputed"
    assert conflicts.event_status(["separated", "correction_pending", "disputed"]) == "correction_pending"
    assert conflicts.event_status(["cleared", "separated"]) == "resolved"
    assert conflicts.event_status([]) == "none"
    assert conflicts.compact(["cleared", "under_review", "disputed"]) == {"status": "under_review", "count": 2,
                                                                         "reviews": 3}


# ----------------------------------------------------------------------------- isolation guard
class _FakeDatabase:
    """Configured for one database while the server reports another (e.g. an overriding connection setting)."""

    def __init__(self, configured: str, actual: str, live: bool = False):
        self.settings = SimpleNamespace(dbname=configured)
        self.actual, self.live = actual, live

    @contextmanager
    def connection(self):
        answers = iter([{"name": self.actual, "has_documents": True}, {"live": self.live}])
        yield SimpleNamespace(execute=lambda *_a, **_k: SimpleNamespace(fetchone=lambda: next(answers)))

    def transaction(self):
        raise AssertionError("destructive setup must not start")


@pytest.mark.parametrize("configured, actual, live, reason", [
    ("stock_news_conflict_test", "stock_news", False, "protected"),  # PGDATABASE says test, server says otherwise
    ("stock_news", "stock_news", False, "protected"),
    ("scratch", "scratch", False, "not named as a test database"),
    ("stock_news_copy_test", "stock_news_copy_test", True, "live (non-sample) documents"),
])
def test_destructive_setup_refuses_anything_but_a_verified_test_database(configured, actual, live, reason):
    from conftest import UnsafeTestDatabase, verify_isolated_database

    with pytest.raises(UnsafeTestDatabase, match=re.escape(reason)):
        verify_isolated_database(_FakeDatabase(configured, actual, live))
    with pytest.raises(UnsafeTestDatabase):
        cleanup_sample_rows(_FakeDatabase(configured, actual, live))  # the cleanup itself refuses too
    assert verify_isolated_database(_FakeDatabase("x_test", "x_test")) == "x_test"


# ----------------------------------------------------------------------------- database fixture
@pytest.fixture(scope="module")
def db():
    database = isolated_test_database()  # refuses stock_news and anything not verified as a test DB
    with database.connection() as conn:
        missing = similarity_store.missing_schema(conn) + store.missing_schema(conn)
    if missing:
        database.close()
        pytest.skip(f"migrations 002/004/005 not applied ({missing}); run `uv run datasources-db migrate`")
    try:
        lock = GraphWriterLock(database, is_sample=True, purpose="pytest conflict notices").acquire()
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


# ----------------------------------------------------------------------------- scenario builders
def _locate(texts: dict[str, str], quote: str) -> str | None:
    folded = _fold(quote)
    return next((ref for ref, text in texts.items() if folded in _fold(text)), None)


def replay(judgment: dict):
    """Scripted comparator replaying a recorded judgment; each quote cites the supplied text that contains it."""

    def decide(segment, event):
        units = {u.ref: u.text for u in segment.units}
        passages = {p.ref: p.text for p in event.passages}
        incoming = [{"ref": ref, "quote": qt} for qt in judgment["incoming"] if (ref := _locate(units, qt))]
        candidate = [{"ref": ref, "quote": qt} for qt in judgment["candidate"] if (ref := _locate(passages, qt))]
        return {"label": judgment["label"], "matching_facts": judgment.get("matching_facts", []),
                "differences": judgment["differences"], "incoming_evidence": incoming,
                "candidate_evidence": candidate,
                "incoming_unit_refs": sorted({e["ref"] for e in incoming}) or [segment.units[0].ref],
                "candidate_passage_refs": sorted({e["ref"] for e in candidate}),
                "explanation": "replayed from the recorded fixture judgment"}

    return scripted(decide)


def fixture_statements(request, name: str) -> dict:
    """The recorded statements, each quote cited from the evidence-pack passage that contains it."""

    def cite(quote: str) -> dict:
        ref = _locate({p.ref: p.text for p in request.pack.passages}, quote)
        assert ref, f"fixture quote not in the evidence pack: {quote!r}"
        return {"passage_ref": ref, "quote": quote}

    items = [{"id": s["id"], "text": s["text"], "citations": [cite(qt) for qt in s["quotes"]]}
             for s in SUMMARIES[name]]
    return {"headline": items[0], "overview": [i for i in items if i["id"][0] == "O"],
            "key_points": [i for i in items if i["id"][0] == "K"], "evidence_gaps": []}


def fixture_summary(request) -> dict:
    if any(r.resolution == "confirmed_conflict" for r in request.pack.reviews):
        return fixture_statements(request, "v3_confirmed")
    return fixture_statements(request, "v2" if request.pack.previous else "v1")


def summarizer(db):
    settings = SummarySettings(enabled=True, retry_base_minutes=10.0)
    return build_summary_runtime(db, settings, backend=ScriptedSummaryBackend(fixture_summary))


def raw_article(key: str, at: datetime, **overrides) -> dict:
    a = ARTICLES[key]
    raw = article(a["headline"], "\n\n".join(a["paragraphs"]), at + timedelta(minutes=a["offset_minutes"]),
                  source=a["source"])
    return {**raw, "url": a["url"], **overrides}


def ingest(db, grt, srt, raw: dict):
    result = run_pipeline([envelope(raw)], PipelineContext(database=db, analyzer=grt, summarizer=srt))
    assert not result.errors, result.errors
    (outcome,) = result.outcomes
    assert outcome.error is None, outcome.error
    return outcome


def build(db, keys=("a", "b", "c", "d"), *, first_summary: bool = True, judgments: dict | None = None,
          raws: dict | None = None) -> SimpleNamespace:
    """One copy of the scenario in its own time window: article ``a`` founds the event, the rest are compared
    to it with their recorded judgments. Returns the event, revisions and reviews by article key."""
    at = anchor()
    grt, srt = grouping_runtime(db), summarizer(db)
    revisions = {}
    for key in keys:
        if key != "a":
            grt.comparator = replay({**JUDGMENTS[key], **(judgments or {}).get(key, {})})
        raw = (raws or {}).get(key) or raw_article(key, at)
        revisions[key] = ingest(db, grt, srt if first_summary or key != "a" else None, raw).revision_id
    event_id = next(m["event_id"] for m in memberships(db, revisions["a"]) if m["relation"] == "origin")
    rows = q(db, "SELECT v.review_id::text AS id, c.revision_id::text AS revision FROM event_conflict_review v "
                 "JOIN event_comparison c ON c.comparison_id = v.comparison_id WHERE c.candidate_event_id = %s",
             event_id)
    by_revision = {r["revision"]: r["id"] for r in rows}
    reviews = {k: by_revision[r] for k, r in revisions.items() if r in by_revision}
    return SimpleNamespace(at=at, grt=grt, srt=srt, event_id=event_id, revisions=revisions, reviews=reviews)


def decide(db, review_id: str, decision: str):
    return review.decide(db, review_id, decision=decision, reviewer="pytest-reviewer",
                         explanation=f"pytest private note for {decision}", language="en")


def detail(client, event_id: str) -> dict:
    response = client.get(f"/api/events/{event_id}", params={"include_sample": "true"})
    assert response.status_code == 200, response.text
    assert "pytest-reviewer" not in response.text and "pytest private note" not in response.text
    assert "replayed from the recorded fixture judgment" not in response.text  # model explanations stay internal
    return response.json()


def listed(client, event_id: str) -> dict:
    events = client.get("/api/events", params={"include_sample": "true", "limit": 100}).json()["events"]
    return next(e for e in events if e["event_id"] == event_id)


def published(db, event_id: str) -> dict:
    return q(db, "SELECT version, headline, summary_text, statements, published_at FROM event_summary "
                 "WHERE event_id = %s AND status = 'published'", event_id)[0]


# ----------------------------------------------------------------------------- API: the states
def test_a_normal_event_shows_no_conflict(db, client):
    s = build(db, ("a", "b"))
    with offline_reads(s.srt):
        d, item = detail(client, s.event_id), listed(client, s.event_id)
    assert item["conflict"] == {"status": "none", "count": 0, "reviews": 0}
    assert d["conflict"]["status"] == "none" and d["conflict"]["disputes"] == []
    assert d["conflict"]["pending_update"] is None and d["conflict"]["summary_not_updated_for_conflict"] is False
    assert d["summary"]["version"] == 1


def test_open_conflict_with_a_published_summary_shows_both_claims_beside_the_unchanged_summary(db, client):
    s = build(db)
    before = published(db, s.event_id)
    with offline_reads(s.srt):
        d, item = detail(client, s.event_id), listed(client, s.event_id)
    assert item["conflict"] == {"status": "under_review", "count": 1, "reviews": 1} and item["under_review"]
    c = d["conflict"]
    assert c["status"] == "under_review" and c["count"] == 1 and c["pending_update"] is None
    (dispute_,) = c["disputes"]
    assert dispute_["review_id"] == s.reviews["d"] and dispute_["review_status"] == "open"
    assert dispute_["precision"] == "claim_pair" and dispute_["topic"] == ["purchase price"]
    claims = {src["name"]: (src["claims"], src["quotes"], src["url"], src["title"]) for src in dispute_["sources"]}
    assert claims["Fixture Finance Journal"][0] == [{"aspect": "purchase price", "value": "USD 300 million"}]
    assert claims["Example Newswire"][0] == [{"aspect": "purchase price", "value": "USD 250 million"}]
    assert claims["Fixture Finance Journal"][2] == ARTICLES["d"]["url"]
    assert claims["Fixture Finance Journal"][3] == ARTICLES["d"]["headline"]
    assert all(src["published_at"] and src["quotes"] for src in dispute_["sources"])
    for src in dispute_["sources"]:  # every quote is verbatim from that source's own article
        article_text = " ".join(next(a for a in ARTICLES.values() if a["url"] == src["url"])["paragraphs"])
        assert all(_fold(qt) in _fold(article_text) for qt in src["quotes"])
    # Timestamps support "not yet updated": the summary was published before the conflict was detected.
    assert c["summary_published_at"] == d["summary"]["published_at"]
    assert dispute_["summary_predates_detection"] is True and c["summary_not_updated_for_conflict"] is True
    assert dispute_["summary_includes_report"] is False
    # The published summary is returned exactly as stored: same version, text and statements.
    assert d["summary"]["version"] == before["version"] == 2
    assert d["summary"]["overview"] == before["summary_text"]
    assert [st["text"] for st in d["summary"]["statements"]] == [st["text"] for st in before["statements"]]
    assert published(db, s.event_id) == before  # reading changed nothing


def test_open_conflict_without_a_summary_still_lists_claims_and_links(db, client):
    s = build(db, ("a", "d"), first_summary=False)
    with offline_reads(s.srt):
        d, item = detail(client, s.event_id), listed(client, s.event_id)
    assert d["summary"] is None and d["summary_state"] == "awaiting_review" == item["summary_state"]
    assert item["conflict"]["status"] == "under_review"
    c = d["conflict"]
    assert c["summary_published_at"] is None and c["summary_not_updated_for_conflict"] is False
    (dispute_,) = c["disputes"]
    assert dispute_["precision"] == "claim_pair" and dispute_["summary_predates_detection"] is None
    assert {src["name"] for src in dispute_["sources"]} == {"Example Newswire", "Fixture Finance Journal"}
    assert all(src["url"].startswith("https://") for src in dispute_["sources"])


def test_several_disputes_closing_one_never_hides_the_other(db, client):
    s = build(db, ("a", "b", "c", "d", "e"))
    assert set(s.reviews) == {"d", "e"}
    result = decide(db, s.reviews["d"], "not_a_conflict")
    assert result["queued_jobs"][0]["queued"] is False  # the other open review still blocks regeneration
    with offline_reads(s.srt):
        d, item = detail(client, s.event_id), listed(client, s.event_id)
    assert item["conflict"] == {"status": "under_review", "count": 1, "reviews": 2}
    active, history = d["conflict"]["disputes"]
    assert (active["review_id"], active["display_status"], active["topic"]) == (
        s.reviews["e"], "under_review", ["expected closing date"])
    assert (history["review_id"], history["display_status"], history["active"]) == (s.reviews["d"], "cleared", False)
    assert history["outcome"]["decision"] == "not_a_conflict" and history["outcome"]["final"] is True
    assert d["conflict"]["pending_update"]["status"] == "blocked_by_review"


def test_confirmed_disagreement_stays_visible_next_to_another_open_review_and_after_it(db, client):
    s = build(db, ("a", "b", "c", "d", "e"))
    decide(db, s.reviews["d"], "confirmed_conflict")
    with offline_reads(s.srt):
        both = detail(client, s.event_id)
    assert both["conflict"]["status"] == "under_review" and both["conflict"]["count"] == 2
    assert {x["display_status"] for x in both["conflict"]["disputes"]} == {"under_review", "disputed"}
    decide(db, s.reviews["e"], "not_a_conflict")
    with offline_reads(s.srt):
        after, item = detail(client, s.event_id), listed(client, s.event_id)
    assert item["conflict"] == {"status": "disputed", "count": 1, "reviews": 2}
    assert [x["display_status"] for x in after["conflict"]["disputes"]] == ["disputed", "cleared"]
    assert after["conflict"]["pending_update"]["status"] == "queued"


@pytest.mark.parametrize("decision, review_status, shown, event_status, count", [
    ("insufficient_evidence", "in_review", "under_review", "under_review", 1),
    ("confirmed_conflict", "resolved", "disputed", "disputed", 1),
    ("not_a_conflict", "resolved", "cleared", "resolved", 0),
    ("different_events", "resolved", "separated", "resolved", 0),
])
def test_each_review_outcome(db, client, decision, review_status, shown, event_status, count):
    s = build(db)
    result = decide(db, s.reviews["d"], decision)
    with offline_reads(s.srt):
        d, item = detail(client, s.event_id), listed(client, s.event_id)
    (x,) = d["conflict"]["disputes"]  # the review stays in the history whatever the outcome
    assert (x["review_status"], x["display_status"], x["active"]) == (review_status, shown, count == 1)
    assert item["conflict"] == {"status": event_status, "count": count, "reviews": 1}
    assert x["outcome"]["decision"] == decision
    assert x["outcome"]["final"] is (decision != "insufficient_evidence")
    assert x["precision"] == "claim_pair"  # history keeps the claims and quotes
    if decision == "insufficient_evidence":
        assert d["conflict"]["pending_update"] is None and d["under_review"] is True
    else:
        # The decision queued a regeneration that has not run: the summary does not include the findings.
        pending = d["conflict"]["pending_update"]
        assert pending["status"] == "queued" and pending["summary_includes_review"] is False
        assert d["summary"]["version"] == 2
        assert datetime.fromisoformat(d["summary"]["published_at"]) < datetime.fromisoformat(pending["decided_at"])
    if decision == "different_events":
        (correction,) = result["membership_corrections"]
        assert x["outcome"]["correction_complete"] is True
        assert x["outcome"]["moved_to_event_id"] == correction["new_event_id"]
        assert s.revisions["d"] not in {src["revision_id"] for src in d["sources"]}


def test_different_events_is_not_shown_resolved_until_the_membership_correction_is_complete(db, client):
    s = build(db)
    # Simulate a decision recorded without its correction (e.g. an interrupted or legacy workflow):
    # the review is resolved as different_events but the report is still an active member.
    with db.transaction() as conn:
        conn.execute("UPDATE event_conflict_review SET status = 'resolved', resolution = 'different_events', "
                     "reviewer_id = 'pytest-reviewer', reviewer_notes = 'pytest private note', resolved_at = now() "
                     "WHERE review_id = %s", (s.reviews["d"],))
    with offline_reads(s.srt):
        d, item = detail(client, s.event_id), listed(client, s.event_id)
    (x,) = d["conflict"]["disputes"]
    assert x["display_status"] == "correction_pending" and x["active"] is True
    assert x["outcome"]["correction_complete"] is False
    assert item["conflict"] == {"status": "correction_pending", "count": 1, "reviews": 1}


def test_pending_regeneration_after_review_then_the_new_summary(db, client):
    s = build(db)
    decide(db, s.reviews["d"], "confirmed_conflict")
    with offline_reads(s.srt):
        queued = detail(client, s.event_id)
    assert queued["conflict"]["pending_update"]["status"] == "queued"
    assert queued["summary"]["version"] == 2 and queued["conflict"]["summary_not_updated_for_conflict"] is True

    task = process_event(s.event_id, SummaryContext(db, s.srt, SummaryBudget(5), None))
    assert task.result == "published" and task.version == 3
    with offline_reads(s.srt):
        after = detail(client, s.event_id)
    c = after["conflict"]
    assert c["pending_update"] is None and after["summary"]["version"] == 3
    assert c["status"] == "disputed" and c["disputes"][0]["active"] is True  # still visible after the review
    # Published after the detection: the notice must not claim the summary predates the conflict.
    assert c["disputes"][0]["summary_predates_detection"] is False and c["summary_not_updated_for_conflict"] is False
    assert "USD 300 million" in after["summary"]["headline"] and "USD 250 million" in after["summary"]["headline"]


@pytest.mark.parametrize("case, override", [
    ("no differences stored", {"differences": []}),
    ("claim not stated", {"differences": [{"kind": "conflict", "aspect": "purchase price",
                                           "incoming": "USD 300 million", "candidate": "not stated"}]}),
])
def test_incomplete_stored_claims_show_quotes_with_a_generic_notice(db, client, case, override):
    s = build(db, ("a", "d"), judgments={"d": override})
    with offline_reads(s.srt):
        d = detail(client, s.event_id)
    (x,) = d["conflict"]["disputes"]
    assert x["precision"] == "quotes_only" and x["notice"] == conflicts.GENERIC_NOTICE
    assert all(src["claims"] == [] for src in x["sources"])
    assert any(src["quotes"] for src in x["sources"] if src["name"] == "Fixture Finance Journal")
    assert d["conflict"]["status"] == "under_review"


def test_sample_events_stay_hidden_unless_requested(db, client):
    s = build(db, ("a", "d"))
    with offline_reads(s.srt):
        default_list = client.get("/api/events", params={"limit": 100}).json()["events"]
        hidden = client.get(f"/api/events/{s.event_id}")
        shown = client.get(f"/api/events/{s.event_id}", params={"include_sample": "true"})
    assert s.event_id not in {e["event_id"] for e in default_list} and all(not e["is_sample"] for e in default_list)
    assert hidden.status_code == 404 and shown.status_code == 200 and shown.json()["is_sample"] is True
    assert client.post(f"/api/events/{s.event_id}").status_code in (404, 405)  # still no write endpoint


# ----------------------------------------------------------------------------- page rendering in a real browser
def _browser() -> str | None:
    candidates = [shutil.which("msedge"), shutil.which("chrome"), shutil.which("chromium"),
                  shutil.which("google-chrome"),
                  r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                  r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
                  r"C:\Program Files\Google\Chrome\Application\chrome.exe"]
    return next((c for c in candidates if c and Path(c).exists()), None)


@contextmanager
def live_server(db):
    import uvicorn

    from datasources import server

    server.app.dependency_overrides[server.get_database] = lambda: db
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    srv = uvicorn.Server(uvicorn.Config(server.app, host="127.0.0.1", port=port, log_level="warning",
                                        lifespan="off"))
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 20
    while not srv.started:
        if time.monotonic() > deadline or not thread.is_alive():
            raise RuntimeError("test server did not start")
        time.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        srv.should_exit = True
        thread.join(10)


def rendered_dom(browser: str, url: str, attempts: int = 3) -> str:
    """The page's DOM after its scripts ran. Background browser traffic is switched off: a pending
    request pauses the virtual clock, which occasionally stalled a first launch. A launch that still
    stalls is retried in a fresh profile (a browser hang, not a page result)."""
    flags = ["--headless=new", "--disable-gpu", "--no-first-run", "--no-default-browser-check",
             "--disable-extensions", "--disable-background-networking", "--disable-component-update",
             "--disable-sync", "--disable-default-apps", "--no-pings", "--disable-domain-reliability",
             "--virtual-time-budget=20000", "--dump-dom"]
    for attempt in range(1, attempts + 1):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as profile:  # never the user's profile
            try:
                result = subprocess.run([browser, *flags, f"--user-data-dir={profile}", url], capture_output=True,
                                        text=True, encoding="utf-8", timeout=45)
            except subprocess.TimeoutExpired:
                if attempt == attempts:
                    raise
                continue
        assert result.returncode == 0, result.stderr[-2000:]
        return result.stdout
    raise AssertionError("unreachable")


HOSTILE = "<img src=x onerror=\"document.title='pwned'\">"


def test_page_renders_the_notice_and_escapes_hostile_source_text(db):
    browser = _browser()
    if browser is None:
        pytest.skip("no Edge/Chrome available for a headless rendering check")
    s = build(db)
    at = anchor()
    hostile_body = ("The signed agreement specified cash consideration of USD 300 million "
                    "&lt;img src=x onerror=\"document.title='pwned'\"&gt; for Veltrani Photonics.")
    grt, srt = grouping_runtime(db), summarizer(db)
    first = ingest(db, grt, srt, raw_article("a", at))
    grt.comparator = replay({"label": "same_event_conflict", "differences": JUDGMENTS["d"]["differences"],
                             "incoming": [f"USD 300 million {HOSTILE} for Veltrani Photonics"],
                             "candidate": [JUDGMENTS["d"]["candidate"][0]]})
    bad = ingest(db, grt, srt, article(f"{HOSTILE} Veltrani price report", hostile_body, at + timedelta(hours=2),
                                       source="<script>document.title='pwned'</script> Wire")
                 | {"url": "javascript:document.title='pwned'"})
    hostile_event = next(m["event_id"] for m in memberships(db, first.revision_id) if m["relation"] == "origin")
    assert q(db, "SELECT count(*) AS n FROM event_conflict_review v JOIN event_comparison c ON "
                 "c.comparison_id = v.comparison_id WHERE c.revision_id = %s", bad.revision_id)[0]["n"] == 1
    stored = q(db, "SELECT r.title, p.name FROM document_revision r LEFT JOIN publisher p "
                   "ON p.publisher_id = r.publisher_id WHERE r.revision_id = %s", bad.revision_id)[0]

    with live_server(db) as base, offline_reads(s.srt, srt):
        page = rendered_dom(browser, f"{base}/#event={s.event_id}&sample=1")
        hostile = rendered_dom(browser, f"{base}/#event={hostile_event}&sample=1")
        cards = rendered_dom(browser, f"{base}/#events&sample=1")

    assert 'data-events-ready="detail"' in page and 'data-events-ready="list"' in cards
    for text in ("Conflicting reports—under review", "Sources disagree about the purchase price.",
                 "Fixture Finance Journal reports USD 300 million.", "Example Newswire reports USD 250 million.",
                 "This summary has not yet been updated to reflect the disagreement.", "Conflict detected",
                 "The summary below was published", ARTICLES["d"]["url"]):
        assert text in page, text
    assert published(db, s.event_id)["headline"] in page  # the published summary is shown unchanged
    assert "Conflicting reports—under review" in cards and 'aria-hidden="true">⚠' in cards

    # Hostile article text, publisher and link are displayed as inert text (the serialized DOM escapes
    # text-node markup, so injected elements would appear as real tags here).
    assert "Conflicting reports—under review" in hostile and f"USD 300 million {HOSTILE}" not in hostile
    assert "<img" not in hostile and "&lt;img src=x onerror=" in hostile  # the quote, shown as text
    for value in (stored["title"], stored["name"]):
        if value and "<" in value:
            assert value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") in hostile
    assert "<script>document.title" not in hostile
    assert 'href="javascript' not in hostile.lower() and "(no valid link)" in hostile
    assert "<title>Node 1 — Data Sources Explorer</title>" in hostile and "pwned</title>" not in hostile
