"""Collection services, duplicate tracking, recovery and demo/live separation (offline, mocked network)."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone

import pytest

from conftest import (
    FIXTURES,
    SYNTHETIC_8K,
    FakeResponse,
    FakeSession,
    html_response,
    json_response,
    make_sec_client,
    synthetic_submissions,
)
from datasources import alpaca as alpaca_source
from datasources import config, sec
from datasources.alpaca import AlpacaAuth
from datasources.ingest import cli
from datasources.ingest.service import ALPACA_CURSOR, IngestionService, ProcessLock
from datasources.ingest.settings import ConfigurationError, load_settings

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK0009999999.json"
ARCHIVE = "https://www.sec.gov/Archives/edgar/data/9999999"
WS_BATCH = json.loads((FIXTURES / "synthetic_alpaca_ws_batch.json").read_text(encoding="utf-8"))
ARTICLE = {k: v for k, v in WS_BATCH[2].items() if k != "T"}


@pytest.fixture(autouse=True)
def synthetic_ticker(monkeypatch, tmp_path):
    """Map the synthetic ticker SYNX to CIK 9999999 and isolate the shared SEC cache."""
    mapping = {**config.load_ticker_map(), "SYNX": {"cik": "0009999999", "name": "EXAMPLE SYNTHETIC CORP"}}
    monkeypatch.setattr(config, "load_ticker_map", lambda: mapping)
    monkeypatch.setattr(sec, "CACHE_DIR", tmp_path / "cache")


def filing(n: int, doc: str = "synthetic-8k.htm", form: str = "8-K", date: str = "2026-09-01") -> dict:
    return {"accessionNumber": f"0009999999-26-{n:06d}", "form": form, "filingDate": date, "primaryDocument": doc}


def doc_url(n: int, doc: str = "synthetic-8k.htm") -> str:
    return f"{ARCHIVE}/{9999999_26_000000 + n:018d}/{doc}"


def sec_service(settings, session, **kwargs) -> IngestionService:
    service = IngestionService(settings.with_overrides(symbols=["SYNX"]), sec_client=make_sec_client(session, **kwargs))
    service.startup()
    return service


def poll_and_process(service: IngestionService) -> list[str]:
    return [service.process_item(item) for item in service.sec_poll_once("SYNX")]


def processed_files(root, provider: str) -> list[str]:
    return sorted(p.relative_to(root).as_posix() for p in (root / "processed" / provider).glob("*/*.json"))


# ---------------------------------------------------------------- SEC
def test_sec_poll_downloads_document_saves_raw_and_processed(settings):
    session = FakeSession({SUBMISSIONS_URL: json_response(synthetic_submissions([filing(1)])),
                           doc_url(1): html_response(SYNTHETIC_8K.read_bytes())})
    service = sec_service(settings, session)
    assert poll_and_process(service) == ["saved"]
    root = service.settings.root
    assert processed_files(root, "sec") == ["processed/sec/0009999999/0009999999-26-000001.json"]
    record = json.loads((root / "processed/sec/0009999999/0009999999-26-000001.json").read_text(encoding="utf-8"))
    assert record["primary_document_url"] == doc_url(1)
    assert record["provenance"]["api_endpoint"] == SUBMISSIONS_URL
    raw_doc = root / "raw/sec/documents/0009999999/0009999999-26-000001/synthetic-8k.htm"
    assert raw_doc.read_bytes() == SYNTHETIC_8K.read_bytes()
    assert record["raw_file_refs"][1] == raw_doc.relative_to(root).as_posix()
    assert (root / record["raw_file_refs"][0]).exists()  # raw submissions snapshot


def test_same_accession_is_skipped_after_success(settings):
    session = FakeSession({SUBMISSIONS_URL: json_response(synthetic_submissions([filing(1)])),
                           doc_url(1): html_response(SYNTHETIC_8K.read_bytes())})
    service = sec_service(settings, session)
    poll_and_process(service)
    assert poll_and_process(service) == []  # nothing queued, no second download
    assert session.urls().count(doc_url(1)) == 1
    assert service.stats["sec_skip_already_completed"] == 1


def test_different_accessions_with_identical_text_are_both_kept(settings):
    session = FakeSession({SUBMISSIONS_URL: json_response(synthetic_submissions([filing(1), filing(2), filing(3, form="8-K/A")])),
                           doc_url(1): html_response(SYNTHETIC_8K.read_bytes()),
                           doc_url(2): html_response(SYNTHETIC_8K.read_bytes()),
                           doc_url(3): html_response(SYNTHETIC_8K.read_bytes())})
    service = sec_service(settings, session)
    assert poll_and_process(service) == ["saved", "saved", "saved"]
    files = processed_files(service.settings.root, "sec")
    assert len(files) == 3
    bodies = {json.loads((service.settings.root / f).read_text(encoding="utf-8"))["body_text"] for f in files}
    assert len(bodies) == 1  # identical text, still three separate filings


def test_download_failure_stays_retryable_then_succeeds(settings):
    session = FakeSession({SUBMISSIONS_URL: json_response(synthetic_submissions([filing(1)])),
                           doc_url(1): [html_response("busy", 503)]})
    service = sec_service(settings, session, max_retries=1)
    assert poll_and_process(service) == ["failed"]
    identity = "sec_edgar:0009999999:0009999999-26-000001"
    entry = service.ledger.sec_entry(identity)
    assert entry["status"] == "failed" and entry["retryable"] is True
    assert processed_files(service.settings.root, "sec") == []
    assert list((service.settings.root / "errors/sec_edgar").rglob("*.json"))
    # Within the backoff window the filing is not retried...
    assert poll_and_process(service) == []
    # ...after it, the next poll retries and succeeds.
    entry["next_retry_at"] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    session.routes[doc_url(1)] = [html_response(SYNTHETIC_8K.read_bytes())]
    assert poll_and_process(service) == ["saved"]
    assert service.ledger.sec_entry(identity)["status"] == "completed"


def test_403_and_error_page_and_empty_extraction_are_not_completed(settings):
    empty = "<html><body><script>x</script><div style='display:none'>hidden</div></body></html>"
    throttled = "<html><body>Request Rate Threshold Exceeded</body></html>"
    session = FakeSession({SUBMISSIONS_URL: json_response(synthetic_submissions([filing(1), filing(2), filing(3)])),
                           doc_url(1): html_response("Forbidden", 403),
                           doc_url(2): html_response(throttled),
                           doc_url(3): html_response(empty)})
    service = sec_service(settings, session)
    assert poll_and_process(service) == ["failed", "failed", "failed"]
    kinds = {k: service.ledger.sec_entry(f"sec_edgar:0009999999:0009999999-26-00000{k}")["error_kind"] for k in (1, 2, 3)}
    assert kinds == {1: "access_denied", 2: "error_page", 3: "AdapterError"}
    assert session.urls().count(doc_url(1)) == 1  # 403 not retried
    assert processed_files(service.settings.root, "sec") == []
    assert service.ledger.counts()["sec_completed"] == 0


def test_poll_bypasses_12h_cache_and_refreshes_it(settings):
    cik = "0009999999"
    stale = synthetic_submissions([])
    sec.save_submissions_cache(cik, stale)
    assert sec.load_submissions(cik)[1] is True  # UI would be served from cache
    fresh = synthetic_submissions([filing(1)])
    session = FakeSession({SUBMISSIONS_URL: json_response(fresh), doc_url(1): html_response(SYNTHETIC_8K.read_bytes())})
    service = sec_service(settings, session)
    items = service.sec_poll_once("SYNX")
    assert session.urls()[0] == SUBMISSIONS_URL
    assert len(items) == 1
    cached, from_cache = sec.load_submissions(cik)
    assert from_cache is True and cached == fresh


def test_first_run_lookback_scope_and_recovery_floor(settings):
    old, new = filing(1, date="2020-01-01"), filing(2, date="2026-09-01")
    session = FakeSession({SUBMISSIONS_URL: json_response(synthetic_submissions([new, old]))})
    service = sec_service(settings.with_overrides(sec_initial_lookback_days=30), session)
    items = service.sec_poll_once("SYNX")
    assert [i.payload["row"]["accessionNumber"] for i in items] == [new["accessionNumber"]]
    floor = service.ledger.get_value("sec_scope_floor:0009999999")
    assert floor == (datetime.now(timezone.utc).date() - timedelta(days=30)).isoformat()


def test_recovery_fetches_historical_file_when_range_requires_it(settings):
    main = synthetic_submissions([filing(2, date="2026-09-01")])
    main["filings"]["files"] = [{"name": "CIK0009999999-submissions-001.json", "filingCount": 1,
                                 "filingFrom": "2019-01-01", "filingTo": "2026-01-01"}]
    historical = synthetic_submissions([filing(1, date="2026-01-01")])["filings"]["recent"]
    session = FakeSession({SUBMISSIONS_URL: json_response(main),
                           "https://data.sec.gov/submissions/CIK0009999999-submissions-001.json": json_response(historical)})
    service = sec_service(settings.with_overrides(sec_initial_lookback_days=365, sec_recovery_max_days=365), session)
    items = service.sec_poll_once("SYNX")
    assert {i.payload["row"]["accessionNumber"] for i in items} == {"0009999999-26-000001", "0009999999-26-000002"}
    service.sec_poll_once("SYNX")
    assert session.urls().count("https://data.sec.gov/submissions/CIK0009999999-submissions-001.json") == 1


# ---------------------------------------------------------------- Alpaca
def alpaca_service(settings) -> IngestionService:
    service = IngestionService(settings, alpaca_auth=AlpacaAuth(mode="trading", api_key="k" * 8, secret_key="s" * 8))
    service.startup()
    return service


def deliver(service, articles, transport="websocket") -> list[str]:
    items = service.alpaca_items(articles, transport=transport, endpoint="wss://stream.data.alpaca.markets/v1beta1/news",
                                 retrieved_at=datetime.now(timezone.utc), raw_refs=[])
    return [service.process_item(item) for item in items]


def test_repeated_rest_and_ws_delivery_is_processed_once(settings):
    service = alpaca_service(settings)
    assert deliver(service, [ARTICLE], "rest") == ["saved"]
    assert deliver(service, [ARTICLE], "websocket") == []
    assert deliver(service, [dict(ARTICLE)], "rest") == []
    assert len(processed_files(service.settings.root, "alpaca")) == 1
    assert service.stats["alpaca_skip_unchanged_delivery"] == 2


def test_new_revision_saved_separately_and_stale_arrival_ignored(settings):
    service = alpaca_service(settings)
    deliver(service, [ARTICLE])
    revised = {**ARTICLE, "updated_at": "2026-09-21T16:00:00Z", "headline": "Revised synthetic headline"}
    assert deliver(service, [revised]) == ["saved"]
    stale = {**ARTICLE, "updated_at": "2026-09-21T15:00:00Z", "headline": "Stale edit"}
    assert deliver(service, [stale]) == []
    assert service.stats["alpaca_skip_stale_revision"] == 1
    files = processed_files(service.settings.root, "alpaca")
    assert files == ["processed/alpaca/91000001/20260921T140500Z_" + files[0].split("_")[1],
                     "processed/alpaca/91000001/20260921T160000Z_" + files[1].split("_")[1]]
    entry = service.ledger.alpaca_entry("alpaca:91000001")
    assert entry["updated_at"] == "2026-09-21T16:00:00+00:00"


def test_richer_payload_fills_missing_content(settings):
    service = alpaca_service(settings)
    thin = {**ARTICLE, "content": ""}
    assert deliver(service, [thin]) == ["saved"]
    assert deliver(service, [ARTICLE]) == ["saved"]  # same updated_at, now with content
    assert deliver(service, [thin]) == []  # thin payload again: unchanged
    assert service.ledger.alpaca_entry("alpaca:91000001")["content_level"] == 2


def test_different_article_ids_with_identical_text_are_both_kept(settings):
    service = alpaca_service(settings)
    assert deliver(service, [{**ARTICLE, "id": 1}, {**ARTICLE, "id": 2}]) == ["saved", "saved"]
    assert len(processed_files(service.settings.root, "alpaca")) == 2


def test_malformed_article_is_recorded_without_blocking_others(settings):
    service = alpaca_service(settings)
    assert deliver(service, [{"headline": "no id"}, ARTICLE]) == ["failed", "saved"]
    assert list((service.settings.root / "errors/alpaca").rglob("*.json"))


# ---------------------------------------------------------------- restart / interrupted writes
def test_state_survives_restart(settings):
    first = alpaca_service(settings)
    deliver(first, [ARTICLE])
    second = alpaca_service(settings)
    assert deliver(second, [ARTICLE]) == []


def test_crash_between_output_and_ledger_update_is_reconciled(settings):
    service = alpaca_service(settings)
    deliver(service, [ARTICLE])
    ledger_file = service.settings.root / "state/ledger.json"
    ledger_file.unlink()  # simulate crash before the ledger write
    (service.settings.root / "processed/alpaca/91000001/.leftover.json.abc.tmp").write_text("{partial")
    restarted = alpaca_service(settings)
    assert restarted.ledger.alpaca_entry("alpaca:91000001") is not None
    assert deliver(restarted, [ARTICLE]) == []
    assert not list(service.settings.root.glob("**/*.tmp"))
    assert len(processed_files(service.settings.root, "alpaca")) == 1


def test_sec_output_without_ledger_entry_is_not_reprocessed(settings):
    session = FakeSession({SUBMISSIONS_URL: json_response(synthetic_submissions([filing(1)])),
                           doc_url(1): html_response(SYNTHETIC_8K.read_bytes())})
    service = sec_service(settings, session)
    poll_and_process(service)
    (service.settings.root / "state/ledger.json").write_text(json.dumps({"version": 1, "sec": {}, "alpaca": {}, "cursors": {}}))
    restarted = sec_service(settings, session)
    assert poll_and_process(restarted) == []
    assert session.urls().count(doc_url(1)) == 1


def test_ledger_entry_without_output_file_is_retried(settings):
    session = FakeSession({SUBMISSIONS_URL: json_response(synthetic_submissions([filing(1)])),
                           doc_url(1): html_response(SYNTHETIC_8K.read_bytes())})
    service = sec_service(settings, session)
    poll_and_process(service)
    (service.settings.root / "processed/sec/0009999999/0009999999-26-000001.json").unlink()
    restarted = sec_service(settings, session)
    assert poll_and_process(restarted) == ["saved"]


def test_corrupt_ledger_is_set_aside_and_rebuilt(settings):
    service = alpaca_service(settings)
    deliver(service, [ARTICLE])
    (service.settings.root / "state/ledger.json").write_text("{not json")
    restarted = alpaca_service(settings)
    assert restarted.ledger.alpaca_entry("alpaca:91000001") is not None
    assert list((service.settings.root / "state").glob("ledger.corrupt-*.json"))


def test_one_worker_per_output_directory(settings):
    settings.root.mkdir(parents=True)
    with ProcessLock(settings.root):
        with pytest.raises(ConfigurationError, match="another ingestion worker"):
            with ProcessLock(settings.root):
                pass


# ---------------------------------------------------------------- REST backfill + WebSocket coordination
class AlpacaRestSession:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "headers": headers})
        return FakeResponse(200, self.pages.pop(0), {"Content-Type": "application/json"})


def test_rest_backfill_paginates_with_content_and_explicit_window(settings):
    pages = [json.loads((FIXTURES / name).read_text()) for name in
             ("synthetic_alpaca_rest_page1.json", "synthetic_alpaca_rest_page2.json")]
    session = AlpacaRestSession(pages)
    service = alpaca_service(settings)
    service.alpaca_session = session

    async def scenario():
        writer = asyncio.create_task(service.writer())
        await service.alpaca_backfill("test")
        writer.cancel()

    asyncio.run(scenario())
    first, second = session.calls
    assert first["params"]["include_content"] == "true"
    assert first["params"]["limit"] == 50
    assert "start" in first["params"] and "end" in first["params"]
    assert "page_token" not in first["params"]
    assert second["params"]["page_token"] == "c3ludGhldGljLXBhZ2UtMg=="
    assert first["headers"] == {"APCA-API-KEY-ID": "k" * 8, "APCA-API-SECRET-KEY": "s" * 8}
    files = processed_files(service.settings.root, "alpaca")
    assert len(files) == 2
    no_content = json.loads((service.settings.root / [f for f in files if "92000002" in f][0]).read_text(encoding="utf-8"))
    assert no_content["content_status"] == "summary_only" and no_content["publisher"] == "reuters"
    assert service.ledger.get_cursor(ALPACA_CURSOR) is not None
    assert list((service.settings.root / "raw/alpaca/rest").rglob("*.json"))


def test_backfill_window_uses_cursor_overlap_and_is_bounded(settings):
    service = alpaca_service(settings.with_overrides(alpaca_reconnect_overlap_minutes=15, alpaca_recovery_max_hours=72,
                                                     alpaca_initial_lookback_hours=24))
    now = datetime(2026, 9, 22, 12, tzinfo=timezone.utc)
    assert service.backfill_window(now)[0] == now - timedelta(hours=24)
    service.ledger.set_cursor(ALPACA_CURSOR, now - timedelta(hours=1))
    assert service.backfill_window(now)[0] == now - timedelta(hours=1, minutes=15)
    service.ledger.set_cursor(ALPACA_CURSOR, now - timedelta(days=30))
    assert service.backfill_window(now)[0] == now - timedelta(hours=72)


def test_concurrent_rest_and_ws_delivery_processed_once(settings):
    service = alpaca_service(settings)

    async def scenario():
        writer = asyncio.create_task(service.writer())
        rest_items = service.alpaca_items([ARTICLE], transport="rest", endpoint="rest", retrieved_at=datetime.now(timezone.utc), raw_refs=[])
        await asyncio.gather(service.on_ws_articles([dict(ARTICLE)]), *(service.enqueue(i) for i in rest_items))
        await service.queue.join()
        writer.cancel()

    asyncio.run(scenario())
    assert len(processed_files(service.settings.root, "alpaca")) == 1
    assert service.stats["alpaca_saved"] == 1


def test_existing_ws_client_feeds_ingestion_callbacks(settings, monkeypatch):
    from alpaca_news import client

    frames = [json.dumps(WS_BATCH[:2]), json.dumps(WS_BATCH[2:])]

    class FakeSocket:
        def __init__(self):
            self.sent = []

        async def send(self, message):
            self.sent.append(json.loads(message))

        def __aiter__(self):
            async def gen():
                for frame in frames:
                    yield frame
            return gen()

    class FakeConnect:
        def __init__(self, url, additional_headers):
            self.headers = additional_headers
            self.socket = FakeSocket()

        async def __aenter__(self):
            return self.socket

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(client.websockets, "connect", FakeConnect)
    service = alpaca_service(settings)
    subscribed = []

    async def on_subscribed():
        subscribed.append(True)

    async def scenario():
        writer = asyncio.create_task(service.writer())
        await client.stream_once(client.NewsSettings("k" * 8, "s" * 8, ["NVDA"]), set(),
                                 on_articles=service.on_ws_articles, on_subscribed=on_subscribed, on_frame=service.on_ws_frame)
        await service.queue.join()
        writer.cancel()

    asyncio.run(scenario())
    assert subscribed == [True]
    assert service.stats["alpaca_saved"] == 2
    log = list((service.settings.root / "raw/alpaca/websocket").glob("*.jsonl"))
    assert len(log) == 1 and len(log[0].read_text(encoding="utf-8").splitlines()) == 2
    record = json.loads(next((service.settings.root / "processed/alpaca/91000001").glob("*.json")).read_text(encoding="utf-8"))
    assert record["provenance"]["transport"] == "websocket"
    assert record["raw_file_refs"][0] == log[0].relative_to(service.settings.root).as_posix()


def test_non_transient_stream_error_raises(monkeypatch):
    from alpaca_news import client

    class FakeConnect:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            class S:
                async def send(self, m):
                    pass

                def __aiter__(self):
                    async def gen():
                        yield json.dumps([{"T": "error", "code": 406, "msg": "connection limit exceeded"}])
                    return gen()
            return S()

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(client.websockets, "connect", FakeConnect)

    async def noop(_):
        pass

    with pytest.raises(client.StreamRejectedError):
        asyncio.run(client.stream_once(client.NewsSettings("k", "s", ["NVDA"]), set(), on_articles=noop))


# ---------------------------------------------------------------- demo / live separation
def test_demo_and_live_use_separate_roots_and_markers(tmp_path, monkeypatch):
    monkeypatch.setenv("INGEST_OUTPUT_DIR", str(tmp_path / "out"))
    live, demo = load_settings(demo=False), load_settings(demo=True)
    assert live.root == tmp_path / "out" and demo.root == tmp_path / "out" / "demo"
    service = cli.run_demo(demo)
    assert service.stats["sec_saved"] == 3 and service.stats["alpaca_saved"] == 1
    records = [json.loads(p.read_text(encoding="utf-8")) for p in (demo.root / "processed").rglob("*.json")]
    assert records and all(r["is_sample"] for r in records)
    assert not (live.root / "processed").exists()
    assert not (live.root / "state" / "ledger.json").exists()
    with pytest.raises(ConfigurationError):
        cli.build_live_service(demo, enable_sec=False, enable_alpaca=False)


def test_live_run_without_alpaca_credentials_is_a_configuration_error(settings, monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    monkeypatch.delenv("ALPACA_AUTH_MODE", raising=False)
    with pytest.raises(alpaca_source.AlpacaConfigError, match="ALPACA_API_KEY"):
        cli.build_live_service(settings, enable_sec=False, enable_alpaca=True)


def test_live_sec_requires_identifying_user_agent(settings):
    with pytest.raises(ConfigurationError, match="SEC_USER_AGENT"):
        cli.build_live_service(settings.with_overrides(sec_user_agent=""), enable_sec=True, enable_alpaca=False)


def test_process_file_ingests_ws_archive_jsonl(settings, tmp_path):
    archive = tmp_path / "alpaca_news_stream.jsonl"
    archive.write_text("\n".join(json.dumps(b) for b in (WS_BATCH, WS_BATCH)) + "\n", encoding="utf-8")
    service = cli.run_process_file(settings, archive, "alpaca", None, None)
    assert service.stats["alpaca_saved"] == 2
    assert service.stats["alpaca_skip_unchanged_delivery"] == 2
    record = json.loads(next((settings.root / "processed/alpaca/91000001").glob("*.json")).read_text(encoding="utf-8"))
    assert record["provenance"]["transport"] == "file" and record["is_sample"] is False


def test_broker_auth_uses_cached_bearer_token():
    class TokenSession:
        def __init__(self):
            self.posts = 0

        def post(self, url, data=None, timeout=None):
            self.posts += 1
            assert data["grant_type"] == "client_credentials"
            return FakeResponse(200, {"access_token": "tok123", "expires_in": 900})

    session = TokenSession()
    auth = AlpacaAuth(mode="broker", client_id="cid", client_secret="csecret")
    assert auth.headers(session) == {"Authorization": "Bearer tok123"}
    assert auth.headers(session) == {"Authorization": "Bearer tok123"}
    assert session.posts == 1
    assert "csecret" not in repr(auth)


def test_shutdown_finishes_current_record_and_leaves_queue_retryable(settings):
    service = alpaca_service(settings)
    items = service.alpaca_items([{**ARTICLE, "id": 1}, {**ARTICLE, "id": 2}], transport="rest", endpoint="rest",
                                 retrieved_at=datetime.now(timezone.utc), raw_refs=[])
    original = service.process_item

    def slow(item):
        time.sleep(0.3)
        return original(item)

    service.process_item = slow

    async def scenario():
        for item in items:
            await service.enqueue(item)
        writer = asyncio.create_task(service.writer())
        await asyncio.sleep(0.05)  # first record is mid-processing
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)

    asyncio.run(scenario())
    assert service.stats["alpaca_saved"] == 1  # current record completed, not torn
    assert service.ledger.alpaca_entry("alpaca:2") is None  # queued item left unmarked -> recovered later
    assert len(processed_files(service.settings.root, "alpaca")) == 1
