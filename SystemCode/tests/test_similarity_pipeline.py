"""Semantic comparison and event grouping through the real compiled graph and PostgreSQL.

Skipped when no `stock_news` database is reachable or migration 002 is not
applied. Everything written here is sample data (``is_sample = true``), in its
own analysis space (the deterministic hashing embedder), and is deleted before
and after the module. Each test uses its own far-apart time window, so tests
never see each other's records. The comparison model is scripted: these tests
prove orchestration, not model accuracy (see ``datasources-similarity evaluate``).
"""

from __future__ import annotations

import itertools
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from conftest import FIXTURES, TEST_PUBLISHER, cleanup_sample_rows, isolated_test_database

psycopg = pytest.importorskip("psycopg")

from datasources.ingest.graph import get_graph, run_pipeline, run_reanalysis  # noqa: E402
from datasources.ingest.service import IngestionService  # noqa: E402
from datasources.similarity import store  # noqa: E402
from datasources.similarity.comparison import ComparisonError, ScriptedComparisonBackend, build_output  # noqa: E402
from datasources.similarity.embeddings import EmbeddingError, HashingEmbeddingBackend  # noqa: E402
from datasources.similarity.lock import GraphWriterLock, WriterLockError  # noqa: E402
from datasources.similarity.pipeline import PipelineContext, build_runtime  # noqa: E402
from datasources.similarity.settings import SimilaritySettings  # noqa: E402

NVDA_CIK = "0001045810"
EXAMPLES = json.loads((FIXTURES / "synthetic_similarity_labelled_examples.json").read_text(encoding="utf-8"))
_windows = itertools.count()


@pytest.fixture(scope="module")
def db():
    database = isolated_test_database()  # refuses stock_news and anything not verified as a test DB
    with database.connection() as conn:
        missing = store.missing_schema(conn)
    if missing:
        database.close()
        pytest.skip(f"migration 002 not applied ({missing}); run `uv run datasources-db migrate`")
    try:
        lock = GraphWriterLock(database, is_sample=True, purpose="pytest").acquire()
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


def anchor() -> datetime:
    """A fresh 60-day-apart window per test (far beyond the 7-day search window)."""
    return datetime(2034, 1, 1, 12, tzinfo=timezone.utc) + timedelta(days=60 * next(_windows))


def settings(**overrides) -> SimilaritySettings:
    base = dict(enabled=True, embedding_backend="hashing", hashing_dimensions=64, comparison_backend="mock",
                min_semantic_similarity=0.2, lexical_plausible_rank=3)
    return SimilaritySettings(**{**base, **overrides})


def runtime(db, comparator=None, embedder=None, **overrides):
    comparator = comparator or ScriptedComparisonBackend(lambda req: build_output(req, lambda s, e: "different_event"))
    return build_runtime(db, settings(**overrides), embedder=embedder or HashingEmbeddingBackend(64),
                         comparator=comparator)


def scripted(label_for):
    """Scripted model: ``label_for(segment, event)`` returns a label (or a dict of overrides)."""
    return ScriptedComparisonBackend(lambda req: build_output(req, label_for))


def new_id() -> int:
    return 8_000_000_000 + uuid.uuid4().int % 900_000_000


def article(headline: str, body: str | None, at: datetime, *, article_id: int | None = None,
            updated: datetime | None = None, source: str = TEST_PUBLISHER, summary: str = "") -> dict:
    content = "".join(f"<p>{p}</p>" for p in body.split("\n\n")) if body else ""
    return {"id": article_id or new_id(), "headline": headline, "summary": summary, "content": content,
            "author": "Pytest Reporter", "created_at": at.isoformat().replace("+00:00", "Z"),
            "updated_at": (updated or at).isoformat().replace("+00:00", "Z"),
            "url": f"https://news.example.test/{article_id}", "symbols": ["NVDA"], "source": source}


def envelope(raw: dict, *, retrieved: datetime | None = None) -> dict:
    created = datetime.fromisoformat(raw["updated_at"].replace("Z", "+00:00"))
    return {"provider": "alpaca", "transport": "rest", "api_endpoint": "https://data.alpaca.markets/v1beta1/news",
            "retrieved_at": retrieved or created + timedelta(minutes=5), "requested_symbols": ["NVDA"],
            "is_sample": True, "raw_record": raw}


def ingest(db, rt, raw: dict):
    result = run_pipeline([envelope(raw)], PipelineContext(database=db, analyzer=rt))
    assert not result.errors, result.errors
    (outcome,) = result.outcomes
    assert outcome.error is None, outcome.error
    return outcome


def trace(db, rt, raw: dict) -> list[str]:
    """Node names in execution order, from LangGraph's own stream."""
    state = {"envelopes": [envelope(raw)], "outcomes": [], "errors": []}
    return [name for update in get_graph().stream(state, context=PipelineContext(database=db, analyzer=rt),
                                                  stream_mode="updates") for name in update]


def q(db, sql: str, *params):
    with db.connection() as conn:
        return conn.execute(sql, params).fetchall()


def memberships(db, revision_id: str) -> list[dict]:
    return q(db, "SELECT m.event_id::text AS event_id, m.relation, m.state, e.status AS event_status, "
                 "(SELECT count(*) FROM event_membership_unit mu WHERE mu.membership_id = m.membership_id) AS units "
                 "FROM event_membership m JOIN news_event e ON e.event_id = m.event_id "
                 "WHERE m.revision_id = %s ORDER BY m.created_at", revision_id)


def analysis(db, revision_id: str) -> dict:
    (row,) = q(db, "SELECT * FROM revision_analysis WHERE revision_id = %s", revision_id)
    return row


ACQ = ("NVIDIA agrees to acquire Example Robotics for $2.1 billion",
       "NVIDIA said on Monday it agreed to acquire Example Robotics for $2.1 billion in cash.\n\n"
       "The acquisition of Example Robotics is expected to close in the first quarter, pending regulatory approval.")
ACQ_PARAPHRASE = ("Example Robotics to be bought by NVIDIA in $2.1 billion deal",
                  "Example Robotics agreed to be acquired by NVIDIA for $2.1 billion in cash, the companies said.\n\n"
                  "The NVIDIA acquisition is expected to close in the first quarter after regulatory approval.")
OTHER = ("NVIDIA opens new research lab in Example City",
         "NVIDIA opened a research lab in Example City focused on robotics simulation software.")


# ----------------------------------------------------------------------------- graph routing
def test_first_article_takes_the_no_candidate_path_without_calling_the_model(db):
    model = scripted(lambda s, e: "same_event_repeated")
    rt = runtime(db, model)
    nodes = trace(db, rt, article(*ACQ, anchor()))
    assert nodes == ["prepare_data", "create_text_units", "generate_embeddings", "retrieve_candidates",
                     "route_candidates", "propose_new_event", "resolve_event_memberships", "persist_result",
                     "route_event_summary"]  # summaries are off here: no event to route
    assert model.calls == []  # no plausible candidate, no comparison call


def test_second_article_finds_the_first_and_is_compared_before_it_is_stored(db):
    at = anchor()
    first_raw, second_raw = article(*ACQ, at), article(*ACQ_PARAPHRASE, at + timedelta(hours=2))
    seen_in_db_during_comparison = []

    def label(segment, event):
        rows = q(db, "SELECT count(*) AS n FROM document WHERE source_record_id = %s AND is_sample",
                 str(second_raw["id"]))
        seen_in_db_during_comparison.append(rows[0]["n"])
        return "same_event_repeated"

    model = scripted(label)
    rt = runtime(db, model)
    first = ingest(db, rt, first_raw)
    assert first.analysis_status == "completed" and first.persist_outcome == "inserted"
    (origin,) = memberships(db, first.revision_id)
    assert origin["relation"] == "origin" and origin["event_status"] == "provisional"

    nodes = trace(db, rt, second_raw)
    assert "compare_with_llm" in nodes and "propose_new_event" not in nodes
    assert len(model.calls) == 1
    assert seen_in_db_during_comparison == [0]  # compared first, stored afterwards
    (request,) = model.calls
    assert [e.event_id for e in request.events] == [origin["event_id"]]

    second_revision = q(db, "SELECT d.current_revision_id::text AS r FROM document d WHERE d.source_record_id = %s",
                        str(second_raw["id"]))[0]["r"]
    (link,) = memberships(db, second_revision)
    assert link["event_id"] == origin["event_id"] and link["relation"] == "same_event_repeated"
    assert link["event_status"] == "corroborated"  # two independent documents
    comparison = q(db, "SELECT label, method, model, prompt_version FROM event_comparison WHERE revision_id = %s",
                   second_revision)[0]
    assert comparison == {"label": "same_event_repeated", "method": "llm", "model": "scripted-test",
                          "prompt_version": "compare-1"}
    scores = q(db, "SELECT semantic_rank, lexical_rank, fused_score, shortlisted FROM retrieval_candidate rc "
                   "JOIN revision_analysis a ON a.analysis_id = rc.analysis_id WHERE a.revision_id = %s",
               second_revision)
    assert scores and all(r["fused_score"] > 0 for r in scores) and any(r["shortlisted"] for r in scores)


def test_one_batch_is_processed_sequentially_and_isolates_a_malformed_record(db):
    """Within one graph invocation, record 2 already sees record 1 committed by persist_result."""
    at = anchor()
    model = scripted(lambda s, e: "same_event_repeated")
    rt = runtime(db, model)
    first, second = article(*ACQ, at), article(*ACQ_PARAPHRASE, at + timedelta(hours=1))
    malformed = {**envelope(article(*OTHER, at)), "raw_record": {"T": "n", "headline": "no id"}}
    result = run_pipeline([envelope(first), malformed, envelope(second)], PipelineContext(database=db, analyzer=rt))
    assert [e.index for e in result.errors] == [1]  # the bad record is reported, the others proceed
    one, two = result.outcomes
    assert one.analysis_status == two.analysis_status == "completed"
    assert len(model.calls) == 1  # only the second record had a candidate: the first one
    assert memberships(db, two.revision_id)[0]["event_id"] == memberships(db, one.revision_id)[0]["event_id"]


# ----------------------------------------------------------------------------- failures and recovery
def test_comparison_failure_stores_the_article_with_pending_analysis_then_retry_completes(db):
    at = anchor()
    rt = runtime(db, scripted(lambda s, e: "same_event_repeated"))
    first = ingest(db, rt, article(*ACQ, at))

    def outage(request):
        raise ComparisonError("simulated outage", kind="timeout", retryable=True)

    rt.comparator = ScriptedComparisonBackend(outage)
    second = ingest(db, rt, article(*ACQ_PARAPHRASE, at + timedelta(hours=1)))
    assert second.persisted and second.analysis_status == "pending"  # article kept, nothing fabricated
    row = analysis(db, second.revision_id)
    assert row["attempts"] == 1 and "simulated outage" in row["last_error"] and row["next_retry_at"] is not None
    assert memberships(db, second.revision_id) == []
    assert q(db, "SELECT count(*) AS n FROM text_unit t JOIN embedding e USING (unit_id) WHERE t.revision_id = %s",
             second.revision_id)[0]["n"] >= 1  # valid intermediate results kept

    working = scripted(lambda s, e: "same_event_update")
    rt.comparator = working
    ctx = PipelineContext(database=db, analyzer=rt)
    assert run_reanalysis([second.revision_id], ctx).outcomes[0].analysis_status == "completed"
    (link,) = memberships(db, second.revision_id)
    assert link["relation"] == "same_event_update"
    assert link["event_id"] == memberships(db, first.revision_id)[0]["event_id"]
    calls = len(working.calls)
    run_reanalysis([second.revision_id], ctx)  # a second retry is a no-op
    assert len(memberships(db, second.revision_id)) == 1 and len(working.calls) == calls


def test_embedding_failure_keeps_units_pending_and_retry_embeds_them(db):
    class Broken(HashingEmbeddingBackend):
        def embed(self, texts):
            raise EmbeddingError("simulated device failure")

    rt = runtime(db, embedder=Broken(64))
    outcome = ingest(db, rt, article(*OTHER, anchor()))
    assert outcome.persisted and outcome.analysis_status == "pending"
    units = q(db, "SELECT embedding_status, retry_count, last_error FROM text_unit WHERE revision_id = %s",
              outcome.revision_id)
    assert units and all(u["embedding_status"] in ("pending", "failed") and u["last_error"] for u in units)

    rt.embedder = HashingEmbeddingBackend(64)
    retried = run_reanalysis([outcome.revision_id], PipelineContext(database=db, analyzer=rt)).outcomes[0]
    assert retried.analysis_status == "completed"
    assert {u["embedding_status"] for u in q(db, "SELECT embedding_status FROM text_unit WHERE revision_id = %s",
                                             outcome.revision_id)} == {"completed"}
    assert memberships(db, outcome.revision_id)[0]["relation"] == "origin"


def test_same_source_replay_is_idempotent_and_calls_no_model(db):
    at = anchor()
    model = scripted(lambda s, e: "same_event_repeated")
    rt = runtime(db, model)
    ingest(db, rt, article(*ACQ, at))
    raw = article(*ACQ_PARAPHRASE, at + timedelta(hours=1))
    ingest(db, rt, raw)
    counted = ("document", "document_revision", "text_unit", "embedding", "news_event", "event_membership",
               "event_comparison", "revision_analysis")
    before = {t: q(db, f"SELECT count(*) AS n FROM {t}")[0]["n"] for t in counted}
    calls = len(model.calls)
    replay = ingest(db, rt, raw)
    assert replay.persist_outcome == "skipped" and replay.analysis_status == "reused:completed"
    assert {t: q(db, f"SELECT count(*) AS n FROM {t}")[0]["n"] for t in counted} == before
    assert len(model.calls) == calls


def test_database_failure_rolls_back_the_whole_record(db, monkeypatch):
    original = store.persist_analysis

    def fail_after_writing(*args, **kwargs):
        original(*args, **kwargs)
        raise psycopg.errors.CheckViolation("simulated constraint failure after the analysis rows")

    monkeypatch.setattr(store, "persist_analysis", fail_after_writing)
    raw = article(*OTHER, anchor())
    result = run_pipeline([envelope(raw)], PipelineContext(database=db, analyzer=runtime(db)))
    (outcome,) = result.outcomes
    assert outcome.error and outcome.error["stage"] == "persist" and not outcome.persisted
    left = q(db, "SELECT (SELECT count(*) FROM document WHERE source_record_id = %s) AS docs, "
                 "(SELECT count(*) FROM news_event e JOIN document_revision r ON r.revision_id = e.origin_revision_id "
                 " JOIN document d ON d.document_id = r.document_id WHERE d.source_record_id = %s) AS events",
             str(raw["id"]), str(raw["id"]))[0]
    assert left == {"docs": 0, "events": 0}


def test_commit_then_file_write_crash_replays_without_duplicates(db, settings_fixture, monkeypatch):
    model = scripted(lambda s, e: "same_event_repeated")
    rt = runtime(db, model)
    service = IngestionService(settings_fixture.with_overrides(demo=True), database=db, analyzer=rt)
    service.startup()
    at = anchor()
    raw = article(*ACQ, at)
    items = service.alpaca_items([raw], transport="rest", endpoint="rest", retrieved_at=at, raw_refs=[])
    real_write = service.store.write_processed
    monkeypatch.setattr(service.store, "write_processed", lambda record: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        service.process_item(items[0])
    assert service.ledger.alpaca_entry(f"alpaca:{raw['id']}") is None  # ingestion not marked complete
    stored = q(db, "SELECT count(*) AS n FROM document WHERE source_record_id = %s", str(raw["id"]))[0]["n"]
    assert stored == 1  # but PostgreSQL committed
    events_before = q(db, "SELECT count(*) AS n FROM news_event WHERE is_sample")[0]["n"]

    monkeypatch.setattr(service.store, "write_processed", real_write)
    again = service.alpaca_items([raw], transport="rest", endpoint="rest", retrieved_at=at, raw_refs=[])
    assert [service.process_item(i) for i in again] == ["saved"]
    assert service.ledger.alpaca_entry(f"alpaca:{raw['id']}") is not None
    assert q(db, "SELECT count(*) AS n FROM news_event WHERE is_sample")[0]["n"] == events_before
    assert service.stats["analysis_reused_completed"] == 1 and model.calls == []


# ----------------------------------------------------------------------------- identity rules
def test_identical_text_from_different_sources_stays_two_documents_linked_without_a_model_call(db):
    body = ("NVIDIA said on Monday it agreed to acquire Example Robotics, a maker of warehouse robots, for "
            "$2.1 billion in cash. The companies expect the acquisition to close in the first quarter of next year, "
            "subject to regulatory approvals and customary closing conditions.\n\n"
            "Example Robotics employs about 900 people and will continue to operate from its headquarters. "
            "NVIDIA said the purchase would expand its robotics simulation business and that it does not expect "
            "the deal to change its financial outlook for the current fiscal year.")
    at = anchor()
    model = scripted(lambda s, e: "different_event")
    rt = runtime(db, model)
    first = ingest(db, rt, article("NVIDIA agrees to buy Example Robotics", body, at))
    second = ingest(db, rt, article("NVIDIA agrees to buy Example Robotics", body, at + timedelta(minutes=30),
                                    source="pytest-other-wire"))
    assert first.document_id != second.document_id  # source identity never merges documents
    (link,) = memberships(db, second.revision_id)
    assert link["relation"] == "same_event_repeated"
    method = q(db, "SELECT method FROM event_comparison WHERE revision_id = %s", second.revision_id)[0]["method"]
    assert method == "exact_content" and model.calls == []


def test_changed_revision_preserves_history_and_counts_the_document_once(db):
    at = anchor()
    relations = []

    def label(segment, event):
        relations.append(event.relation)
        return "same_event_update"

    rt = runtime(db, scripted(label))
    article_id = new_id()
    first = ingest(db, rt, article(*ACQ, at, article_id=article_id))
    revised_body = ACQ[1] + "\n\nOn Wednesday NVIDIA said regulators in two countries had approved the deal."
    second = ingest(db, rt, article(ACQ[0], revised_body, at, article_id=article_id,
                                    updated=at + timedelta(days=2)))
    assert second.document_id == first.document_id and second.revision_id != first.revision_id
    assert second.persist_outcome == "updated"
    assert relations == ["earlier revision of this same article (not an independent source)"]
    old, new = memberships(db, first.revision_id), memberships(db, second.revision_id)
    assert old[0]["relation"] == "origin" and new[0]["relation"] == "same_event_update"
    assert old[0]["event_id"] == new[0]["event_id"]
    assert analysis(db, first.revision_id)["status"] == "completed"  # old analysis preserved
    current = q(db, "SELECT count(DISTINCT document_id) AS docs, count(*) AS rows FROM current_event_membership "
                    "WHERE event_id = %s", new[0]["event_id"])[0]
    assert current == {"docs": 1, "rows": 1}  # the superseded revision is not a second source
    status = q(db, "SELECT status FROM news_event WHERE event_id = %s", new[0]["event_id"])[0]["status"]
    assert status == "provisional"  # still one document


# ----------------------------------------------------------------------------- isolation
def test_embedding_profiles_never_mix(db):
    at = anchor()
    first_space = runtime(db)
    ingest(db, first_space, article(*ACQ, at))
    # Same 64 dimensions, different text preparation: a different profile, so nothing is found.
    model = scripted(lambda s, e: "same_event_repeated")
    other_space = runtime(db, model, chunk_tokens=200)
    assert other_space.profile_id != first_space.profile_id
    outcome = ingest(db, other_space, article(*ACQ_PARAPHRASE, at + timedelta(hours=1)))
    assert model.calls == [] and memberships(db, outcome.revision_id)[0]["relation"] == "origin"
    # The first article is not indexed in this space, so "no match" is explicitly not proof of uniqueness.
    assert outcome.analysis_status == "partial"
    assert analysis(db, outcome.revision_id)["index_coverage"]["unindexed_revisions"] == 1
    # A different dimension count must not even reach pgvector's distance operator.
    third = runtime(db, embedder=HashingEmbeddingBackend(32), hashing_dimensions=32)
    last = ingest(db, third, article(*ACQ_PARAPHRASE, at + timedelta(hours=2)))  # ingest() asserts no error
    assert last.analysis_status == "partial" and analysis(db, last.revision_id)["index_coverage"]["pool_revisions"] == 2


def test_sample_and_live_rows_are_never_candidates_for_each_other(db):
    """Live rows exist only inside a transaction that is rolled back: nothing is committed."""
    from datasources.db.repository import persist_record
    from datasources.ingest.graph import run_prepare
    from datasources.similarity import retrieval
    from datasources.similarity.models import AnalysisSubject, RecordWork

    rt = runtime(db)
    at = anchor()
    raw = article(*ACQ, at)
    (live_record,), _ = run_prepare([{**envelope(raw), "is_sample": False}])
    conn_ctx = db.connection()
    conn = conn_ctx.__enter__()
    try:
        conn.execute("BEGIN")
        stored = persist_record(conn, live_record, raw_payload=raw)
        live = RecordWork(index=0, subject=AnalysisSubject.from_record(live_record), mode="ingest",
                          analysis_enabled=True)
        live.subject.document_id, live.subject.revision_id = stored.document_id, stored.revision_id
        live.plan = rt.builder.build(live.subject)
        live.vectors = {u.key: v for u, v in zip(live.plan.units, rt.embedder.embed([u.text for u in live.plan.units]))}
        live.status = "completed"
        from datasources.similarity.models import NewEventDraft, MembershipDraft

        live.new_events = [NewEventDraft("new:S1", "S1", "live test event", {}, [], at)]
        live.memberships = [MembershipDraft("new:S1", "S1", "origin", [u.key for u in live.plan.units])]
        store.persist_analysis(conn, live, profile_id=rt.profile_id, pipeline_version=rt.pipeline_version,
                               comparison_model=None, prompt_version="compare-1", max_attempts=5,
                               retry_base_minutes=10)

        (sample_record,), _ = run_prepare([envelope(article(*ACQ_PARAPHRASE, at))])
        sample = RecordWork(index=0, subject=AnalysisSubject.from_record(sample_record), mode="ingest",
                            analysis_enabled=True, company_ids=store.resolve_company_ids(conn, AnalysisSubject.from_record(sample_record)))
        sample.plan = rt.builder.build(sample.subject)
        sample.vectors = {u.key: v for u, v in zip(sample.plan.units, rt.embedder.embed([u.text for u in sample.plan.units]))}
        retrieval.retrieve(conn, sample, profile_id=rt.profile_id, pipeline_version=rt.pipeline_version,
                           settings=rt.settings)
        assert sample.hits == [] and sample.candidate_events == {}  # the live row is invisible to sample data
        assert sample.coverage["pool_revisions"] == 0

        live_event = q_conn(conn, "SELECT event_id FROM news_event WHERE origin_revision_id = %s",
                            stored.revision_id)[0]["event_id"]
        with pytest.raises(psycopg.errors.RaiseException, match="partition"):
            conn.execute("INSERT INTO event_membership (event_id, revision_id, document_id, analysis_id, segment_key, "
                         "relation, state) SELECT %s, a.revision_id, a.document_id, a.analysis_id, 'S1', 'origin', "
                         "'active' FROM revision_analysis a JOIN document d ON d.document_id = a.document_id "
                         "WHERE d.is_sample LIMIT 1", (live_event,))
    finally:
        conn.execute("ROLLBACK")
        conn_ctx.__exit__(None, None, None)
    assert q(db, "SELECT count(*) AS n FROM document WHERE source_record_id = %s AND NOT is_sample",
             str(raw["id"]))[0]["n"] == 0


def q_conn(conn, sql, *params):
    return conn.execute(sql, params).fetchall()


# ----------------------------------------------------------------------------- multi-event and chunking
FILING_HTML = """<html><body>
<p>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</p><p>FORM 8-K</p>
<p>Item 5.02. Departure of Directors or Certain Officers.</p>
<p>On {d1}, Jane Example notified NVIDIA of her decision to retire as Senior Vice President of Example Operations,
effective at the end of the quarter. She will remain an advisor during the transition.</p>
<p>Item 8.01. Other Events.</p>
{buyback}
<p>SIGNATURE</p><p>Pursuant to the requirements of the Securities Exchange Act of 1934, the registrant has duly caused
this report to be signed on its behalf.</p>
</body></html>"""

BUYBACK_SENTENCES = [
    "NVIDIA announced that its board of directors approved an additional $60 billion share repurchase authorization.",
    "The share repurchase program has no expiration date and may be suspended at any time.",
    "NVIDIA expects to fund the share repurchases with cash on hand and future cash flows.",
    "Repurchases may be made through open market purchases or privately negotiated transactions.",
]


def sec_envelope(at: datetime, accession: str) -> dict:
    buyback = "".join(f"<p>{s} " * 3 + "</p>" for s in BUYBACK_SENTENCES)
    html = FILING_HTML.format(d1=at.strftime("%B %d, %Y"), buyback=buyback)
    row = {"accessionNumber": accession, "form": "8-K", "filingDate": at.date().isoformat(),
           "reportDate": at.date().isoformat(), "acceptanceDateTime": at.isoformat().replace("+00:00", "Z"),
           "items": "5.02,8.01", "primaryDocument": "example-8k.htm", "primaryDocDescription": "8-K"}
    return {"provider": "sec_edgar", "transport": "rest", "api_endpoint": "https://data.sec.gov/submissions/CIK.json",
            "retrieved_at": at + timedelta(minutes=10), "requested_symbols": ["NVDA"], "is_sample": True,
            "raw_record": row, "sec_context": {"cik": NVDA_CIK, "company_name": "NVIDIA CORP", "tickers": ["NVDA"]},
            "primary_document_html": html}


def test_multi_event_filing_and_overlapping_chunks(db):
    at = anchor()
    accession = f"9999999999-34-{uuid.uuid4().int % 1_000_000:06d}"

    def label(segment, event):
        texts = " ".join(u.text for u in segment.units)
        buyback = "repurchase" in texts and "repurchase" in " ".join(p.text for p in event.passages)
        if not buyback:
            return "different_event"
        return {"label": "same_event_repeated",
                "incoming_unit_refs": [u.ref for u in segment.units],  # every overlapping chunk supports it
                "incoming_evidence": [{"ref": segment.units[0].ref, "quote": segment.units[0].text[:40]}],
                "candidate_evidence": [{"ref": event.passages[0].ref, "quote": event.passages[0].text[:40]}]}

    rt = runtime(db, scripted(label), chunk_tokens=60, chunk_overlap_tokens=12)
    result = run_pipeline([sec_envelope(at, accession)], PipelineContext(database=db, analyzer=rt))
    (filing,) = result.outcomes
    assert filing.error is None and filing.analysis_status == "completed"
    links = memberships(db, filing.revision_id)
    assert len(links) == 2 and {m["relation"] for m in links} == {"origin"}  # one event per Item
    titles = {r["title"] for r in q(db, "SELECT e.title FROM news_event e WHERE e.origin_revision_id = %s",
                                    filing.revision_id)}
    assert any("5.02" in t for t in titles) and any("8.01" in t for t in titles)
    buyback_event = q(db, "SELECT e.event_id::text AS id FROM news_event e WHERE e.origin_revision_id = %s "
                          "AND e.title LIKE '%%8.01%%'", filing.revision_id)[0]["id"]

    body = "\n\n".join(" ".join(BUYBACK_SENTENCES) for _ in range(3))
    news = ingest(db, rt, article("NVIDIA adds $60 billion to share repurchase program", body,
                                  at + timedelta(hours=3)))
    (link,) = memberships(db, news.revision_id)  # overlapping chunks collapse into one membership
    assert link["event_id"] == buyback_event and link["relation"] == "same_event_repeated"
    chunks = q(db, "SELECT count(*) AS n FROM text_unit WHERE revision_id = %s", news.revision_id)[0]["n"]
    assert chunks > 1 and link["units"] == chunks


def test_no_transitive_merging(db):
    at = anchor()
    rt = runtime(db, scripted(lambda s, e: "different_event"))
    a = ingest(db, rt, article(*ACQ, at))
    event_a = memberships(db, a.revision_id)[0]["event_id"]
    rt.comparator = scripted(lambda s, e: "same_event_repeated")
    b = ingest(db, rt, article(*ACQ_PARAPHRASE, at + timedelta(hours=1)))
    assert memberships(db, b.revision_id)[0]["event_id"] == event_a
    # C resembles B (a member of A's event), but C is judged against the event's evidence: not the same.
    rt.comparator = scripted(lambda s, e: "different_event")
    c = ingest(db, rt, article("Example Robotics files patent for warehouse robot gripper",
                               "Example Robotics, which NVIDIA agreed to acquire for $2.1 billion in cash, filed a "
                               "patent for a new robot gripper.", at + timedelta(hours=2)))
    (own,) = memberships(db, c.revision_id)
    assert own["relation"] == "origin" and own["event_id"] != event_a


def test_call_budget_exhaustion_marks_partial_and_never_declares_new(db):
    at = anchor()
    rt = runtime(db, scripted(lambda s, e: "different_event"))
    ingest(db, rt, article(*ACQ, at))
    ingest(db, rt, article("NVIDIA acquisition of Example Robotics draws regulator questions",
                           "Regulators asked NVIDIA about its $2.1 billion acquisition of Example Robotics.",
                           at + timedelta(hours=1)))
    limited = runtime(db, scripted(lambda s, e: "different_event"), llm_max_calls_per_record=1,
                      llm_max_events_per_call=1)
    outcome = ingest(db, limited, article(*ACQ_PARAPHRASE, at + timedelta(hours=2)))
    assert outcome.analysis_status == "partial" and len(limited.comparator.calls) == 1
    assert memberships(db, outcome.revision_id) == []  # an unevaluated candidate blocks "new event"
    assert any("budget" in note for note in analysis(db, outcome.revision_id)["limitations"])
    retry = runtime(db, scripted(lambda s, e: "different_event"))
    done = run_reanalysis([outcome.revision_id], PipelineContext(database=db, analyzer=retry)).outcomes[0]
    assert done.analysis_status == "completed" and len(retry.comparator.calls) == 1  # only the unjudged pair
    assert memberships(db, outcome.revision_id)[0]["relation"] == "origin"


@pytest.mark.parametrize("example", EXAMPLES["examples"], ids=lambda e: e["id"])
def test_labelled_examples_resolve_as_labelled(db, example):
    at = anchor()
    rt = runtime(db, scripted(lambda s, e: "different_event"))
    candidate = example["candidate"]
    first = ingest(db, rt, article(candidate["title"], candidate["text"], at))
    event_id = memberships(db, first.revision_id)[0]["event_id"]
    expected = example["expected_label"]
    rt.comparator = scripted(lambda s, e: {
        "label": expected,
        "incoming_evidence": [{"ref": s.units[0].ref, "quote": s.units[0].text[:30]}],
        "candidate_evidence": [{"ref": e.passages[0].ref, "quote": e.passages[0].text[:30]}]})
    incoming = example["incoming"]
    second = ingest(db, rt, article(incoming["title"], incoming["text"] or None, at + timedelta(hours=1)))
    links = {(m["event_id"] == event_id, m["relation"], m["state"]) for m in memberships(db, second.revision_id)}
    if expected == "different_event":
        assert links == {(False, "origin", "active")}
    elif expected == "uncertain":
        assert links == {(True, "uncertain", "pending"), (False, "origin", "active")}
    else:
        assert links == {(True, expected, "active")}
    if not incoming["text"]:
        assert analysis(db, second.revision_id)["evidence_level"] == "headline_only"


# ----------------------------------------------------------------------------- bootstrap
def test_bootstrap_indexes_everything_first_then_groups_without_self_matches(db):
    at = anchor()
    raws = [article(*ACQ, at), article(*ACQ_PARAPHRASE, at + timedelta(hours=1)),
            article(*OTHER, at + timedelta(hours=2))]
    legacy = [run_pipeline([envelope(r)], PipelineContext(database=db)).outcomes[0] for r in raws]
    assert all(o.persisted and o.analysis_status == "disabled" for o in legacy)
    revisions = [o.revision_id for o in legacy]

    model = scripted(lambda s, e: "same_event_repeated" if "Robotics" in s.units[0].text
                     and "Robotics" in e.passages[0].text else "different_event")
    rt = runtime(db, model)
    with db.connection() as conn:
        selected = store.select_unanalyzed(conn, profile_id=rt.profile_id, pipeline_version=rt.pipeline_version,
                                           is_sample=True, limit=100)
    assert revisions == [r for r in selected if r in revisions]  # oldest first

    indexed = run_reanalysis(revisions, PipelineContext(database=db, analyzer=rt, index_only=True))
    assert [o.analysis_status for o in indexed.outcomes] == ["pending"] * 3 and model.calls == []
    assert all(analysis(db, r)["attempts"] == 0 for r in revisions)  # indexing is not a failed attempt
    assert q(db, "SELECT count(*) AS n FROM news_event e WHERE e.origin_revision_id = ANY(%s::uuid[])",
             revisions)[0]["n"] == 0

    grouped = run_reanalysis(revisions, PipelineContext(database=db, analyzer=rt))
    assert [o.analysis_status for o in grouped.outcomes] == ["completed"] * 3
    first, second, third = (memberships(db, r) for r in revisions)
    assert first[0]["relation"] == "origin"
    assert second[0]["event_id"] == first[0]["event_id"] and second[0]["relation"] == "same_event_repeated"
    assert third[0]["relation"] == "origin" and third[0]["event_id"] != first[0]["event_id"]
    assert all(analysis(db, r)["index_coverage"]["complete"] for r in revisions)
    self_hits = q(db, "SELECT count(*) AS n FROM retrieval_candidate rc JOIN revision_analysis a "
                      "ON a.analysis_id = rc.analysis_id WHERE rc.candidate_revision_id = a.revision_id")[0]["n"]
    self_compared = q(db, "SELECT count(*) AS n FROM event_comparison c JOIN event_membership m "
                          "ON m.event_id = c.candidate_event_id AND m.revision_id = c.revision_id "
                          "WHERE m.relation = 'origin'")[0]["n"]
    assert self_hits == 0 and self_compared == 0
    # Idempotent: nothing left to select, re-running changes nothing.
    with db.connection() as conn:
        assert not set(revisions) & set(store.select_unanalyzed(conn, profile_id=rt.profile_id,
                                                                pipeline_version=rt.pipeline_version,
                                                                is_sample=True, limit=100))


def test_unindexed_history_is_reported_as_partial_coverage(db):
    at = anchor()
    run_pipeline([envelope(article(*OTHER, at))], PipelineContext(database=db))  # stored, never indexed
    rt = runtime(db)
    outcome = ingest(db, rt, article(*ACQ, at + timedelta(hours=1)))
    row = analysis(db, outcome.revision_id)
    assert outcome.analysis_status == "partial" and row["index_coverage"]["complete"] is False
    assert row["index_coverage"]["unindexed_revisions"] == 1
    flags = q(db, "SELECT flags FROM news_event WHERE origin_revision_id = %s", outcome.revision_id)[0]["flags"]
    assert "index_coverage_partial" in flags


# ----------------------------------------------------------------------------- concurrency
def test_second_graph_writer_is_refused(db):
    with pytest.raises(WriterLockError, match="already writing"):
        GraphWriterLock(db, is_sample=True, purpose="second writer").acquire()


@pytest.fixture
def settings_fixture(settings):
    return settings
