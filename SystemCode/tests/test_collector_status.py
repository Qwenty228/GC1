"""Collector status: the worker's status file, how it is classified, and the read-only /api/status.

Offline: collectors are mocked (no WebSocket, no Alpaca or SEC request), and the
database is disabled or replaced, so nothing here needs PostgreSQL or the network.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from datasources.alpaca import AlpacaAuth
from datasources.ingest import cli as ingest_cli
from datasources.ingest.service import IngestionService
from datasources.ingest.status import (
    STALE_AFTER_SECONDS,
    collector_status,
    load_status_file,
    status_path,
)

NOW = datetime(2026, 9, 30, 12, tzinfo=timezone.utc)


def raw_status(state="running", age_seconds=5.0, **extra):
    updated = (NOW - timedelta(seconds=age_seconds)).isoformat()
    return {"version": 1, "state": state, "pid": 4242, "started_at": updated, "updated_at": updated,
            "symbols": ["NVDA"], "alpaca": {"enabled": True, "stream_connected": True},
            "sec": {"enabled": True, "last_poll_ok_at": {"NVDA": updated}}, "processing": {"summaries": False}, **extra}


@pytest.mark.parametrize("raw, expected", [
    (None, "not_running"),
    (raw_status(), "running"),
    (raw_status(age_seconds=STALE_AFTER_SECONDS + 1), "stale"),  # killed or hung: never shown as healthy
    (raw_status(state="stopped", age_seconds=3600), "stopped"),
    (raw_status(state="something-new"), "stale"),
    (raw_status(updated_at="not a time"), "stale"),
    (raw_status(state="starting", stage="loading the embedding model"), "starting"),
    (raw_status(state="starting", age_seconds=STALE_AFTER_SECONDS + 1), "stale"),  # a previous run's startup
    (["not", "a", "dict"], "not_running"),
])
def test_collector_state_comes_only_from_the_heartbeat(raw, expected):
    status = collector_status(raw if isinstance(raw, dict) or raw is None else None, now=NOW)
    assert status["state"] == expected
    assert "pid" not in status  # process ids stay local


def test_startup_stages_are_reported_then_handed_over_or_marked_stopped(tmp_path, caplog):
    from datasources.ingest.status import StartupReporter

    caplog.set_level("INFO")
    reporter = StartupReporter(tmp_path, ["NVDA"], interval=0.05)
    reporter.stage("connecting to the database")
    reporter.stage("loading the embedding model")
    starting = collector_status(load_status_file(tmp_path))
    assert starting["state"] == "starting" and starting["stage"] == "loading the embedding model"
    assert starting["stage_started_at"] and starting["alpaca"]["stream_connected"] is None  # nothing claimed yet
    reporter.abort()  # interrupted before collectors started
    stopped = collector_status(load_status_file(tmp_path))
    assert stopped["state"] == "stopped" and stopped["stage"] == "loading the embedding model"
    assert "startup: connecting to the database done in" in caplog.text
    assert "startup stopped after" in caplog.text

    finished = StartupReporter(tmp_path / "b", ["NVDA"], interval=0.05)
    finished.stage("reconciling the ingestion ledger")
    finished.finish("collectors starting")
    finished.abort()  # after finish: leaves the file to the running worker's heartbeat
    assert load_status_file(tmp_path / "b")["state"] == "starting"
    assert "startup complete in" in caplog.text


def test_unreadable_or_foreign_status_files_count_as_missing(tmp_path):
    path = status_path(tmp_path)
    path.parent.mkdir(parents=True)
    assert load_status_file(tmp_path) is None
    path.write_text("{half written", encoding="utf-8")
    assert load_status_file(tmp_path) is None
    path.write_text(json.dumps({"version": 99, "state": "running"}), encoding="utf-8")
    assert load_status_file(tmp_path) is None


def test_status_file_reports_flags_and_times_but_no_credentials(settings):
    auth = AlpacaAuth(mode="trading", api_key="key-SHOULD-NOT-APPEAR", secret_key="secret-SHOULD-NOT-APPEAR")
    service = IngestionService(settings, alpaca_auth=auth)
    service.startup()
    service.write_status("running")
    text = status_path(settings.root).read_text(encoding="utf-8")
    assert "SHOULD-NOT-APPEAR" not in text
    raw = load_status_file(settings.root)
    assert raw["state"] == "running" and raw["alpaca"]["enabled"] is True and raw["sec"]["enabled"] is False
    assert raw["processing"] == {"database": False, "similarity": False, "summaries": False}
    assert collector_status(raw)["state"] == "running"


def test_a_websocket_disconnect_stops_the_heartbeat_from_advancing_the_cursor(settings):
    service = IngestionService(settings)
    service.startup()
    service._ensure_async_state()

    async def scenario():
        service.alpaca_backfill = lambda reason: asyncio.sleep(0)  # no REST request in this test
        await service.on_ws_subscribed()
        assert service.stream_subscribed is True
        service.on_ws_disconnected()

    asyncio.run(scenario())
    assert service.stream_subscribed is False and service.health["disconnected_at"] is not None
    assert service.status_snapshot("running")["alpaca"]["stream_connected"] is False


def test_run_forever_reports_each_disconnect(monkeypatch):
    from alpaca_news import client

    attempts = []

    async def failing_stream(*_args, **_kwargs):
        attempts.append(1)
        if len(attempts) == 3:
            raise asyncio.CancelledError
        raise OSError("connection reset")

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(client, "stream_once", failing_stream)
    monkeypatch.setattr(client.asyncio, "sleep", no_sleep)
    disconnects = []
    settings = client.NewsSettings(api_key="k", secret_key="s", symbols=["NVDA"])
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(client.run_forever(settings, on_articles=lambda _a: None,
                                       on_disconnected=lambda: disconnects.append(1)))
    assert len(disconnects) == 2  # two dropped connections; cancellation is shutdown, not a disconnect


def test_worker_lifecycle_with_mocked_collectors_writes_running_then_stopped(settings, monkeypatch):
    """run_service with a fake WebSocket, no REST pages and a fake SEC client: the status file follows the
    collectors (subscribed, disconnected, SEC polled) and ends as `stopped` after a clean shutdown."""
    from alpaca_news import client

    from conftest import synthetic_submissions
    from datasources.ingest import service as service_module
    from datasources.ingest.sec_client import FetchResult

    seen: dict[str, dict] = {}

    async def fake_run_forever(_settings, *, on_articles, on_subscribed, on_frame, on_disconnected, **_):
        await on_subscribed()
        await asyncio.sleep(0.05)
        seen["subscribed"] = load_status_file(settings.root) or {}
        on_disconnected()
        await asyncio.sleep(3600)

    class FakeSec:
        def fetch_submissions(self, cik10, _max_bytes):
            body = json.dumps(synthetic_submissions([], cik=cik10)).encode()
            return json.loads(body), FetchResult(url="https://data.sec.gov/fake", status=200,
                                                  content_type="application/json", body=body,
                                                  retrieved_at=datetime.now(timezone.utc))

    monkeypatch.setattr(client, "run_forever", fake_run_forever)
    monkeypatch.setattr(service_module, "iter_news_pages", lambda *a, **k: iter(()))
    monkeypatch.setattr(service_module.sec, "save_submissions_cache", lambda *a, **k: None)  # never the shared cache
    service = IngestionService(settings, sec_client=FakeSec(),
                               alpaca_auth=AlpacaAuth(mode="trading", api_key="k" * 8, secret_key="s" * 8))
    monkeypatch.setattr(service, "status_heartbeat", lambda: _heartbeat(service))

    async def main():
        task = asyncio.create_task(ingest_cli.run_service(service, enable_sec=True, enable_alpaca=True, once=False))
        for _ in range(200):
            raw = load_status_file(settings.root) or {}
            if raw.get("sec", {}).get("last_poll_ok_at") and not raw.get("alpaca", {}).get("stream_connected", True):
                break
            await asyncio.sleep(0.02)
        seen["later"] = load_status_file(settings.root) or {}
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())
    assert seen["subscribed"]["state"] == "running"
    later = seen["later"]
    assert later["alpaca"]["stream_connected"] is False and later["alpaca"]["disconnected_at"]
    assert later["sec"]["last_poll_ok_at"].get("NVDA")
    final = load_status_file(settings.root)
    assert final["state"] == "stopped" and collector_status(final)["state"] == "stopped"


async def _heartbeat(service):
    while True:
        service.write_status("running")
        await asyncio.sleep(0.02)


# ----------------------------------------------------------------------------- /api/status and friends, no database
@pytest.fixture
def client_without_database(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    from datasources import server

    monkeypatch.setenv("DATASOURCES_DB_ENABLED", "false")
    monkeypatch.setenv("INGEST_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("INGEST_SYMBOLS", "NVDA")
    monkeypatch.setattr(server, "_database", None)
    with TestClient(server.app) as client:
        yield client, tmp_path


def test_status_endpoint_works_without_a_database_and_reads_the_status_file(client_without_database):
    client, root = client_without_database
    body = client.get("/api/status").json()
    assert body["collector"]["state"] == "not_running" and body["symbols"] == ["NVDA"]
    assert body["database"]["available"] is False and "DATASOURCES_DB_ENABLED=false" in body["database"]["error"]
    fresh = raw_status(age_seconds=0)
    fresh["updated_at"] = datetime.now(timezone.utc).isoformat()
    status_path(root).parent.mkdir(parents=True, exist_ok=True)
    status_path(root).write_text(json.dumps(fresh), encoding="utf-8")
    response = client.get("/api/status")
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["collector"]["state"] == "running"
    assert client.get("/api/articles").status_code == 503  # a clear error, not an empty list
    assert client.get("/").headers["cache-control"] == "no-cache"


def test_filters_are_validated_before_touching_the_database(monkeypatch):
    from fastapi.testclient import TestClient

    from datasources import server

    class NoDatabase:
        def connection(self):
            raise AssertionError("validation must fail first")

    server.app.dependency_overrides[server.get_database] = lambda: NoDatabase()
    try:
        client = TestClient(server.app)
        assert client.get("/api/articles", params={"source": "rss"}).status_code == 422
        assert client.get("/api/articles", params={"symbol": "<script>"}).status_code == 422
        assert client.get("/api/events", params={"symbol": "NV DA"}).status_code == 422
        assert client.get("/api/articles", params={"limit": 500}).status_code == 422
        assert client.post("/api/articles").status_code in (404, 405)  # read-only
    finally:
        server.app.dependency_overrides.pop(server.get_database, None)


def test_the_page_references_its_assets_by_content_version(client_without_database):
    """Old servers sent no Cache-Control, so browsers may reuse an old app.js without asking; a stale script
    mixed with a newer page threw at startup and left nothing clickable. Versioned URLs rule that out."""
    import hashlib
    import re

    from datasources import config

    client, _ = client_without_database
    pages = {"/": {"styles.css", "render.js", "app.js"}, "/index.html": {"styles.css", "render.js", "app.js"},
             "/tools.html": {"styles.css", "render.js", "tools.js", "explorer.js"}}
    for path, assets in pages.items():
        response = client.get(path)
        assert response.status_code == 200 and response.headers["cache-control"] == "no-cache"
        refs = dict(re.findall(r'(?:src|href)="([\w.-]+\.(?:js|css))\?v=([0-9a-f]{12})"', response.text))
        assert set(refs) == assets
        for name, version in refs.items():
            assert hashlib.sha256((config.WEB_DIR / name).read_bytes()).hexdigest()[:12] == version
        assert 'src="render.js"' not in response.text
    asset = client.get(f"/render.js?v={refs['render.js']}")
    assert asset.status_code == 200 and asset.headers["cache-control"] == "no-cache"


def test_a_starting_worker_is_reported_with_its_stage(client_without_database):
    from datasources.ingest.status import StartupReporter

    client, root = client_without_database
    reporter = StartupReporter(root, ["NVDA"], interval=60)
    reporter.stage("loading the embedding model")
    collector = client.get("/api/status").json()["collector"]
    assert collector["state"] == "starting" and collector["stage"] == "loading the embedding model"
    reporter.abort()
    assert client.get("/api/status").json()["collector"]["state"] == "stopped"
