"""The run-wide model-attempt cap and `datasources-ingest bounded-run`, with scripted providers.

The real Anthropic SDK and the real comparison / summary adapters run against a fake network: every HTTP
request reaches `FakeModelAPI`, which answers from a script (an error status, a timeout, an interruption, or a
valid structured answer built for that request) and records it. Nothing leaves the machine, and no credential is
real. The database tests write sample rows to an isolated test database and delete them, like the other
database tests.
"""

from __future__ import annotations

import itertools
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import anthropic
import httpx2
import pytest

from conftest import cleanup_sample_rows, isolated_test_database

psycopg = pytest.importorskip("psycopg")

from datasources.ingest import bounded  # noqa: E402
from datasources.ingest.service import ALPACA_CURSOR, IngestionService  # noqa: E402
from datasources.ingest.settings import IngestSettings  # noqa: E402
from datasources.llm_budget import (  # noqa: E402
    AttemptBudgetExhausted,
    AttemptLedger,
    counting_anthropic,
    counting_http_client,
)
from datasources.similarity.comparison import (  # noqa: E402
    ComparisonError,
    ComparisonRequest,
    build_output,
    call_json_model,
)
from datasources.similarity.settings import SimilaritySettings  # noqa: E402
from datasources.summary.generation import SummaryRequest, extractive_output  # noqa: E402
from datasources.summary.settings import SummarySettings  # noqa: E402

REQUESTS: dict[str, object] = {}  # rendered prompt -> the request object it was rendered from
PROXY_VARIABLES = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy", "NO_PROXY",
                   "no_proxy")


class FakeModelAPI:
    """The network behind the SDK. Script steps, consumed per request: an HTTP status (int), "timeout",
    "interrupt" (Ctrl+C while waiting), a callable(request) returning a step, or "ok" (the default)."""

    def __init__(self, script=(), decide=lambda segment, event: "same_event_update"):
        self.script = list(script)
        self.decide = decide
        self.requests: list[dict] = []
        self._lock = threading.Lock()

    def transport(self, proxy_url=None) -> httpx2.BaseTransport:
        return httpx2.MockTransport(lambda request: self.handle(request, proxy_url))

    def handle(self, request: httpx2.Request, proxy_url):
        body = json.loads(request.content)
        with self._lock:
            self.requests.append({"model": body.get("model"), "proxy": proxy_url,
                                  "retry": request.headers.get("x-stainless-retry-count")})
            step = self.script.pop(0) if self.script else "ok"
        if callable(step):
            step = step(request)
        if step == "timeout":
            raise httpx2.ReadTimeout("scripted timeout", request=request)
        if step == "interrupt":
            raise KeyboardInterrupt
        if isinstance(step, int):
            return httpx2.Response(step, headers={"retry-after-ms": "1"},
                                   json={"type": "error", "error": {"type": "overloaded_error", "message": "scripted"}})
        return httpx2.Response(200, json=self.answer(body))

    def answer(self, body: dict) -> dict:
        request = REQUESTS.get(body["messages"][0]["content"])
        if isinstance(request, ComparisonRequest):
            text = build_output(request, self.decide).model_dump_json()
        elif isinstance(request, SummaryRequest):
            text = json.dumps(extractive_output(request))
        else:
            text = "{}"
        return {"id": "msg_scripted", "type": "message", "role": "assistant", "model": f"{body['model']}-served",
                "content": [{"type": "text", "text": text}], "stop_reason": "end_turn", "stop_sequence": None,
                "usage": {"input_tokens": 100, "output_tokens": 20, "cache_creation_input_tokens": 0,
                          "cache_read_input_tokens": 0}}

    def by_model(self) -> list[str]:
        return [r["model"] for r in self.requests]


@pytest.fixture(autouse=True)
def offline_sdk(monkeypatch):
    """No proxy, a fake key and an unreachable API host: only FakeModelAPI can answer."""
    for name in PROXY_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-not-a-key")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", "https://api.anthropic.invalid")
    render_comparison, render_summary = ComparisonRequest.render, SummaryRequest.render

    def comparison(self):
        text = render_comparison(self)
        REQUESTS[text] = self
        return text

    def summary(self):
        text = render_summary(self)
        REQUESTS[text] = self
        return text

    monkeypatch.setattr(ComparisonRequest, "render", comparison)
    monkeypatch.setattr(SummaryRequest, "render", summary)


def client(api: FakeModelAPI, ledger: AttemptLedger, *, retries: int = 2) -> anthropic.Anthropic:
    return counting_anthropic(ledger, "comparison", timeout=5, max_retries=retries, transport_factory=api.transport)


def one_call(c: anthropic.Anthropic):
    return c.messages.create(model="claude-scripted", max_tokens=10, messages=[{"role": "user", "content": "hi"}])


# ----------------------------------------------------------------------------- the cap, at the HTTP boundary
def test_sdk_retries_are_counted_as_attempts(tmp_path):
    api = FakeModelAPI([529, 529])  # then ok
    ledger = AttemptLedger(5, tmp_path / "attempts.json")
    one_call(client(api, ledger))
    assert len(api.requests) == 3 and ledger.used == 3
    assert [a["sdk_retry"] for a in ledger.attempts] == [0, 1, 2]
    assert [a["outcome"] for a in ledger.attempts] == ["http_529", "http_529", "http_200"]
    summary = ledger.summary()
    assert summary["logical_calls_sent"] == {"comparison": 1} and summary["sdk_retries"] == 2
    assert summary["tokens_reported"]["input_tokens"] == 100 and summary["served_models"] == ["claude-scripted-served"]
    on_disk = json.loads((tmp_path / "attempts.json").read_text(encoding="utf-8"))
    assert on_disk["used"] == 3 and "sk-ant" not in json.dumps(on_disk)  # no key, header or content is stored


def test_the_cap_refuses_an_sdk_retry_before_it_is_sent():
    api = FakeModelAPI([529, 529])
    ledger = AttemptLedger(2)
    with pytest.raises(AttemptBudgetExhausted):  # passed through unwrapped: the SDK does not retry it
        one_call(client(api, ledger))
    assert len(api.requests) == 2 and ledger.used == 2
    assert len(ledger.blocked) == 1 and ledger.blocked[0]["sdk_retry"] == 2


def test_an_exhausted_budget_sends_nothing_and_the_adapters_see_a_typed_error():
    api = FakeModelAPI()
    ledger = AttemptLedger(1)
    c = client(api, ledger)
    one_call(c)
    params = {"model": "claude-scripted", "max_tokens": 10, "messages": [{"role": "user", "content": "x"}]}
    with pytest.raises(ComparisonError) as info:
        call_json_model(c, params, fallbacks=False, model="claude-scripted", purpose="comparison",
                        max_tokens_setting="SIMILARITY_LLM_MAX_OUTPUT_TOKENS", error=ComparisonError)
    assert info.value.kind == "budget_exhausted" and len(api.requests) == 1  # no second request left the machine


def test_a_timed_out_attempt_counts_and_reports_unknown_usage():
    api = FakeModelAPI(["timeout"])  # then ok
    ledger = AttemptLedger(3)
    one_call(client(api, ledger))
    assert [a["outcome"] for a in ledger.attempts] == ["timeout", "http_200"]
    summary = ledger.summary()
    assert summary["attempts_without_usage_that_may_be_billed"] == 1
    assert summary["tokens_reported"]["attempts_with_usage"] == 1


def test_each_attempt_is_on_disk_before_its_request_is_sent(tmp_path):
    path = tmp_path / "attempts.json"
    seen = []

    def check(_request):
        seen.append(json.loads(path.read_text(encoding="utf-8")))
        return "ok"

    api = FakeModelAPI([check])
    one_call(client(api, AttemptLedger(3, path)))
    assert seen[0]["used"] == 1 and seen[0]["attempts"][0]["outcome"] == "sending"
    assert json.loads(path.read_text(encoding="utf-8"))["attempts"][0]["outcome"] == "http_200"


def test_a_proxy_from_the_environment_does_not_bypass_the_count(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid:3128")
    api = FakeModelAPI()
    ledger = AttemptLedger(2)
    c = anthropic.Anthropic(max_retries=0, http_client=counting_http_client(ledger, "comparison",
                                                                            transport_factory=api.transport))
    one_call(c)
    assert ledger.used == 1 and api.requests[0]["proxy"] == "http://proxy.invalid:3128"


def test_records_are_classified_by_what_is_left_to_do():
    from datasources.summary.pipeline import LEASED_ELSEWHERE

    def outcome(status, *summaries):
        return SimpleNamespace(error=None, analysis_status=status, summaries=list(summaries), revision_id="r",
                               document_id="d", persist_outcome="inserted", analysis={"llm_calls": 1})

    assert bounded.classify(outcome("completed", {"result": "published"}), None)["status"] == "completed"
    repeat = bounded.classify(outcome("reused:completed", {"result": "skipped", "reason": "exact copy"}), None)
    assert repeat["status"] == "completed"
    assert bounded.classify(outcome("partial"), None)["left_to_do"] == ["grouping partial"]
    assert bounded.classify(outcome("completed", {"result": "deferred"}), None)["left_to_do"] == ["summary deferred"]
    held = bounded.classify(outcome("completed", {"result": "skipped", "reason": LEASED_ELSEWHERE}), None)
    assert held["status"] == "pending" and held["left_to_do"] == ["summary held by another lease"]
    review = bounded.classify(outcome("completed", {"result": "awaiting_review"}), None)
    assert review["status"] == "completed" and "reviewer" in review["note"]
    assert bounded.classify(outcome("failed"), None)["status"] == "failed"
    assert bounded.classify(None, None, "failed")["status"] == "failed"


# ----------------------------------------------------------------------------- bounded runs (database)
@pytest.fixture(scope="module")
def db():
    from datasources.similarity import store as similarity_store
    from datasources.similarity.lock import GraphWriterLock, WriterLockError
    from datasources.summary import store as summary_store

    database = isolated_test_database()  # refuses stock_news and anything not verified as a test DB
    with database.connection() as conn:
        missing = similarity_store.missing_schema(conn) + summary_store.missing_schema(conn)
    if missing:
        database.close()
        pytest.skip(f"migrations not applied ({missing}); run `uv run datasources-db migrate`")
    try:
        lock = GraphWriterLock(database, is_sample=True, purpose="pytest bounded runs").acquire()
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


SIM = SimilaritySettings(enabled=True, embedding_backend="hashing", hashing_dimensions=64, comparison_backend="anthropic",
                         comparison_model="claude-scripted", comparison_fallbacks="none", llm_max_retries=1,
                         llm_timeout_seconds=5, min_semantic_similarity=0.2, lexical_plausible_rank=3,
                         retry_base_minutes=0.0001)
SUMM = SummarySettings(enabled=True, model="claude-scripted-summary", fallbacks="none", llm_max_retries=1,
                       llm_timeout_seconds=5, lease_minutes=0.001, retry_base_minutes=0.0001)
_windows = itertools.count()


def window() -> datetime:
    return datetime(2041, 1, 1, 12, tzinfo=timezone.utc) + timedelta(days=60 * next(_windows))


def articles(at: datetime, n: int = 3) -> list[dict]:
    from test_event_summary import UPDATE
    from test_similarity_pipeline import ACQ, ACQ_PARAPHRASE, OTHER, article

    texts = [ACQ, ACQ_PARAPHRASE, UPDATE, OTHER, ("NVIDIA shares close higher", "NVIDIA shares closed higher on "
                                                  "Friday after a broad rally in chip stocks.")]
    return [article(*texts[i], at + timedelta(hours=2 * i)) for i in range(n)]


def pages_of(*pages: list[dict]):
    fetched: list[int] = []

    def iter_pages(_auth, _symbols, *, start, end, max_pages, page_size, session, timeout):
        for number, news in enumerate(pages[:max_pages]):
            fetched.append(number)
            token = f"page-{number + 2}" if number + 1 < len(pages) else None
            yield {"news": news, "next_page_token": token}, f"https://data.alpaca.markets/v1beta1/news?page={number + 1}"

    iter_pages.fetched = fetched
    return iter_pages


def new_run(ledger: AttemptLedger, max_records: int, source: str = "collect") -> bounded.BoundedRun:
    lines: list[str] = []
    run = bounded.BoundedRun(bounded.new_run_id(), source, max_records, ledger, say=lines.append)
    run.lines = lines
    return run


def collect(db, root, api, *, max_records, attempts, pages, summaries=True):
    ledger = AttemptLedger(attempts)
    analyzer, summarizer = bounded.counted_runtimes(db, ledger, summaries=summaries, similarity_settings=SIM,
                                                    summary_settings=SUMM, transport_factory=api.transport)
    settings = IngestSettings(symbols=["NVDA"], output_root=root, demo=True, alpaca_backfill_max_pages=5)
    service = IngestionService(settings, database=db, analyzer=analyzer, summarizer=summarizer, model_budget=ledger)
    run = new_run(ledger, max_records)
    bounded.run_collect(run, service, sec=False, alpaca=True, iter_pages=pages)
    return run, service


def counts(db, ids: list[str]) -> dict:
    with db.connection() as conn:
        return conn.execute(
            """
            SELECT count(DISTINCT d.document_id) AS documents, count(DISTINCT r.revision_id) AS revisions,
                   (SELECT count(*) FROM revision_analysis a JOIN document d2 USING (document_id)
                     WHERE d2.source_record_id = ANY(%(ids)s) AND d2.is_sample) AS analyses,
                   (SELECT count(*) FROM event_membership m JOIN document d3 USING (document_id)
                     WHERE d3.source_record_id = ANY(%(ids)s) AND d3.is_sample) AS memberships,
                   (SELECT count(*) FROM event_comparison c JOIN document_revision r4 ON r4.revision_id = c.revision_id
                      JOIN document d4 ON d4.document_id = r4.document_id
                     WHERE d4.source_record_id = ANY(%(ids)s) AND d4.is_sample) AS comparisons
              FROM document d JOIN document_revision r USING (document_id)
             WHERE d.source_record_id = ANY(%(ids)s) AND d.is_sample
            """, {"ids": ids}).fetchone()


def summaries_of(db, event_id: str) -> list[dict]:
    with db.connection() as conn:
        return conn.execute("SELECT version, status FROM event_summary WHERE event_id = %s ORDER BY version",
                            (event_id,)).fetchall()


def test_both_model_stages_share_one_cap_and_the_run_stops_cleanly(db, tmp_path):
    news = articles(window(), 3)
    api = FakeModelAPI()
    run, service = collect(db, tmp_path / "out", api, max_records=10, attempts=3, pages=pages_of(news))
    # A: its first summary (1). B: comparison (2) and a new summary version (3). The cap is reached, so C never
    # starts: no request is attempted and refused, the pre-check stops before any is sent.
    assert api.by_model() == ["claude-scripted-summary", "claude-scripted", "claude-scripted-summary"]
    assert run.ledger.used == 3 and run.ledger.blocked == []
    assert run.stop_reason == "attempt_budget"
    assert run.counts() == {"started": 2, "completed": 2, "pending": 0, "failed": 0, "skipped_as_duplicates": 0,
                            "not_started": 1}
    assert run.not_started == [f"alpaca:{news[2]['id']}"]
    assert run.ledger.summary()["logical_calls_sent"] == {"summary": 2, "comparison": 1}
    assert service.ledger.alpaca_entry(f"alpaca:{news[2]['id']}") is None  # not started = not marked
    assert service.ledger.get_cursor(ALPACA_CURSOR) is None and run.cursor["advanced"] is False


def test_a_run_that_runs_out_mid_record_defers_the_summary_without_a_failed_attempt(db, tmp_path):
    news = articles(window(), 3)
    api = FakeModelAPI()
    run, _ = collect(db, tmp_path / "out", api, max_records=10, attempts=2, pages=pages_of(news))
    assert api.by_model() == ["claude-scripted-summary", "claude-scripted"]
    assert run.ledger.blocked == []  # the summary was deferred before a request, not refused at the transport
    record_b = run.records[1]
    assert record_b["status"] == "pending" and record_b["left_to_do"] == ["summary deferred"]
    (event_id,) = record_b["events"]
    with db.connection() as conn:
        job = conn.execute("SELECT status, attempts FROM event_summary_job WHERE event_id = %s", (event_id,)).fetchone()
        attempt_rows = conn.execute("SELECT count(*) AS n FROM event_summary_attempt WHERE event_id = %s",
                                    (event_id,)).fetchone()["n"]
    assert job["status"] == "pending" and attempt_rows == 1  # only v1's attempt; the deferral recorded nothing

    # Resumed under a new hard cap with the existing summary command.
    from datasources.summary import cli as summary_cli

    ledger = AttemptLedger(1)
    backend = bounded.counted_runtimes(db, ledger, summaries=True, similarity_settings=SIM, summary_settings=SUMM,
                                       transport_factory=api.transport)[1]
    from datasources.summary.pipeline import SummaryBudget, SummaryContext

    report = summary_cli._run([event_id], SummaryContext(db, backend, SummaryBudget(5, attempts=ledger)),
                              ignore_backoff=True)
    assert report["results"] == {"published": 1} and report["http_attempts"]["used"] == 1
    assert [s["version"] for s in summaries_of(db, event_id)] == [1, 2]


def test_a_timed_out_comparison_leaves_the_record_stored_and_resumable(db, tmp_path):
    news = articles(window(), 2)
    api = FakeModelAPI([lambda r: "ok", "timeout", "timeout"])  # A's summary; both attempts of B's comparison
    run, service = collect(db, tmp_path / "out", api, max_records=10, attempts=10, pages=pages_of(news))
    record_b = run.records[1]
    assert record_b["status"] == "pending" and record_b["left_to_do"] == ["grouping pending"]
    assert [a["outcome"] for a in run.ledger.attempts] == ["http_200", "timeout", "timeout"]
    assert service.ledger.alpaca_entry(f"alpaca:{news[1]['id']}") is not None  # stored = ingested, accurately
    assert run.cursor["advanced"] is True  # every article of the window was stored

    ids = [str(a["id"]) for a in news]
    before = counts(db, ids)
    ledger = AttemptLedger(5)
    analyzer, summarizer = bounded.counted_runtimes(db, ledger, summaries=True, similarity_settings=SIM,
                                                    summary_settings=SUMM, transport_factory=api.transport)
    resume = new_run(ledger, 5, "stored")
    bounded.run_stored(resume, db, analyzer, summarizer, is_sample=True)
    assert [r["record"] for r in resume.records] == [f"revision:{record_b['revision_id']}"]
    assert resume.records[0]["status"] == "completed" and ledger.used == 2  # comparison + summary, nothing else
    after = counts(db, ids)
    assert (after["documents"], after["revisions"], after["analyses"]) == (
        before["documents"], before["revisions"], before["analyses"])  # retried in place, nothing duplicated


def test_an_interruption_during_a_model_call_leaves_nothing_half_recorded(db, tmp_path):
    news = articles(window(), 2)
    root = tmp_path / "out"
    api = FakeModelAPI(["interrupt"])  # Ctrl+C while waiting for A's summary
    with pytest.raises(KeyboardInterrupt):
        collect(db, root, api, max_records=10, attempts=10, pages=pages_of(news))
    settings = IngestSettings(symbols=["NVDA"], output_root=root, demo=True)
    check = IngestionService(settings)
    check.startup()
    assert check.ledger.alpaca_entry(f"alpaca:{news[0]['id']}") is None  # not marked: the next run redoes it
    assert check.ledger.get_cursor(ALPACA_CURSOR) is None
    with db.connection() as conn:
        (job,) = conn.execute("SELECT j.status, j.lease_expires_at FROM event_summary_job j JOIN event_membership m "
                              "USING (event_id) JOIN document d USING (document_id) WHERE d.source_record_id = %s",
                              (str(news[0]["id"]),)).fetchall()
    assert job["status"] == "running"  # the stopped run's lease: it expires after SUMMARY_LEASE_MINUTES
    time.sleep(SUMM.lease_minutes * 60 + 0.1)

    api2 = FakeModelAPI()
    run, service = collect(db, root, api2, max_records=10, attempts=10, pages=pages_of(news))
    assert run.counts()["completed"] == 2 and run.cursor["advanced"] is True
    # A reused its stored grouping (no comparison) and got its summary once the dead run's lease expired.
    assert api2.by_model() == ["claude-scripted-summary", "claude-scripted", "claude-scripted-summary"]
    assert run.records[0]["grouping"].startswith("reused:")
    assert counts(db, [str(a["id"]) for a in news])["documents"] == 2


def test_rerunning_duplicates_nothing_and_repeats_no_model_call(db, tmp_path):
    news = articles(window(), 2)
    ids = [str(a["id"]) for a in news]
    api = FakeModelAPI()
    first, _ = collect(db, tmp_path / "out", api, max_records=10, attempts=10, pages=pages_of(news))
    assert first.counts()["completed"] == 2 and first.ledger.used == 3
    stored = counts(db, ids)

    again, _ = collect(db, tmp_path / "out", api, max_records=10, attempts=10, pages=pages_of(news))
    assert again.counts()["skipped_as_duplicates"] == 2 and again.ledger.used == 0  # the file ledger knows them

    lost, _ = collect(db, tmp_path / "fresh-ledger", api, max_records=10, attempts=10, pages=pages_of(news))
    assert lost.ledger.used == 0  # stored analysis reused; unchanged summary evidence makes no call
    assert [r["grouping"] for r in lost.records] == ["reused:completed", "reused:completed"]
    assert counts(db, ids) == stored


def test_the_record_limit_never_moves_the_cursor_past_unprocessed_articles(db, tmp_path):
    at = window()
    from test_similarity_pipeline import article

    news = [article(f"Unrelated sample headline number {i}", f"Sample body text number {i} about topic {i}.",
                    at + timedelta(days=3 * i)) for i in range(5)]
    pages = [news[:3], news[3:]]
    root = tmp_path / "out"
    api = FakeModelAPI(decide=lambda s, e: "different_event")
    first, service = collect(db, root, api, max_records=2, attempts=50, pages=pages_of(*pages), summaries=False)
    assert first.counts()["started"] == 2 and first.not_started == [f"alpaca:{news[2]['id']}"]
    assert first.cursor["pages_fetched"] == 1 and first.cursor["advanced"] is False  # page 2 was never fetched
    assert service.ledger.get_cursor(ALPACA_CURSOR) is None

    second, service = collect(db, root, api, max_records=2, attempts=50, pages=pages_of(*pages), summaries=False)
    assert second.counts()["skipped_as_duplicates"] == 2
    assert [r["record"] for r in second.records] == [f"alpaca:{news[2]['id']}", f"alpaca:{news[3]['id']}"]
    assert second.cursor["advanced"] is False and service.ledger.get_cursor(ALPACA_CURSOR) is None

    third, service = collect(db, root, api, max_records=2, attempts=50, pages=pages_of(*pages), summaries=False)
    assert [r["record"] for r in third.records] == [f"alpaca:{news[4]['id']}"]
    assert third.stop_reason == "completed" and third.cursor["advanced"] is True
    assert service.ledger.get_cursor(ALPACA_CURSOR) is not None
    assert counts(db, [str(a["id"]) for a in news])["documents"] == 5


def test_a_stored_run_honours_the_record_limit_and_reports_what_is_left(db, tmp_path):
    from datasources.ingest.graph import run_pipeline
    from datasources.similarity.pipeline import PipelineContext
    from test_similarity_pipeline import envelope

    from datasources.ingest.graph import run_reanalysis

    news = articles(window(), 3)
    revisions = []
    for raw in news:  # stored without grouping, as records stored while SIMILARITY_ENABLED was false
        result = run_pipeline([envelope(raw)], PipelineContext(database=db))
        assert not result.errors and result.outcomes[0].persisted
        revisions.append(result.outcomes[0].revision_id)
    api = FakeModelAPI()
    ledger = AttemptLedger(20)
    analyzer, summarizer = bounded.counted_runtimes(db, ledger, summaries=True, similarity_settings=SIM,
                                                    summary_settings=SUMM, transport_factory=api.transport)
    # Indexed but not grouped (local embeddings only), the state of stock_news's live records.
    run_reanalysis(revisions, PipelineContext(db, analyzer, index_only=True))
    assert ledger.used == 0
    run = new_run(ledger, 2, "stored")
    bounded.run_stored(run, db, analyzer, summarizer, is_sample=True)
    assert run.counts()["started"] == 2 and run.stop_reason == "completed"
    assert any("still have unfinished grouping" in note for note in run.notes)
    assert run.records[0]["status"] == "completed" and ledger.used == 3  # A: summary; B: comparison + summary


def test_bounded_run_refuses_paid_calls_without_consent(monkeypatch, capsys, tmp_path):
    from datasources.ingest import cli

    # main() configures process-wide logging onto this test's captured stream; keep that out of later tests.
    monkeypatch.setattr(cli, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setenv("SIMILARITY_COMPARISON_BACKEND", "anthropic")
    monkeypatch.setenv("INGEST_OUTPUT_DIR", str(tmp_path / "out"))
    code = cli.main(["bounded-run", "--source", "stored", "--max-records", "1", "--max-model-attempts", "2"])
    assert code == 2 and "--confirm-paid-calls" in capsys.readouterr().err
    assert not (tmp_path / "out" / "bounded-runs").exists()  # nothing started, nothing written
    assert cli.main(["bounded-run", "--source", "stored", "--max-records", "0", "--max-model-attempts", "2",
                     "--confirm-paid-calls"]) == 2
