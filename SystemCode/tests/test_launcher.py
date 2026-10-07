"""`datasources-dev`: supervision, startup checks and clean shutdown.

Every collector here is mocked: fake services are small Python scripts, and the
end-to-end test runs the real web server and the real ingestion worker with a fake
WebSocket, no Alpaca REST pages and a fake SEC client. No network request, database
write or model call is made.
"""

from __future__ import annotations

import io
import json
import os
import socket
import subprocess
import sys
import textwrap
import threading
import time
import urllib.request
from pathlib import Path

import pytest
from filelock import FileLock

from datasources import launcher
from datasources.launcher import Launcher, ServiceSpec

FAKE_SERVICE = textwrap.dedent("""
    import os, signal, sys, time
    name, mode, marker = sys.argv[1], sys.argv[2], sys.argv[3]
    signal.signal(signal.SIGINT, lambda *a: print(f"{name}: DIRECT-CTRL-C", flush=True))
    print(f"{name}: started, token {os.environ.get('ALPACA_SECRET_KEY', '')}", flush=True)
    if mode == "crash-at-start":
        sys.exit(4)
    open(marker, "w").close()
    if mode == "crash-later":
        time.sleep(0.3)
        sys.exit(3)
    sys.stdin.buffer.read()  # until the launcher closes our stdin
    if mode == "ignore-stop":
        print(f"{name}: ignoring the stop request", flush=True)
        time.sleep(60)
    print(f"{name}: STDIN-EOF, stopping", flush=True)
""")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def fake(tmp_path):
    script = tmp_path / "fake_service.py"
    script.write_text(FAKE_SERVICE, encoding="utf-8")

    def spec(name: str, mode: str = "normal", stop_timeout: float = 10.0) -> ServiceSpec:
        marker = tmp_path / f"{name}.ready"
        return ServiceSpec(name, [sys.executable, str(script), name, mode, str(marker)],
                           ready=lambda _service: marker.exists(), ready_timeout=15, stop_timeout=stop_timeout)

    return spec


def stop_when_ready(launched: Launcher) -> None:
    if all(service.ready for service in launched.services):
        raise KeyboardInterrupt  # what Ctrl+C does in the launcher's main thread


def test_services_start_in_order_log_with_prefixes_and_stop_in_reverse(fake, monkeypatch):
    monkeypatch.setenv("ALPACA_SECRET_KEY", "secret-value-1234")
    out = io.StringIO()
    launched = Launcher([fake("web"), fake("ingest")], out=out)
    assert launched.run(monitor=stop_when_ready) == 0
    log = out.getvalue()
    assert "[web]     web: started" in log and "[ingest]  ingest: started" in log
    assert "secret-value-1234" not in log and "token ***" in log  # credentials are masked in relayed logs
    assert log.index("ingest: STDIN-EOF") < log.index("web: STDIN-EOF")  # collectors stop before the server
    assert "DIRECT-CTRL-C" not in log
    assert [s.proc.returncode for s in launched.services] == [0, 0]


def test_an_unexpected_exit_is_reported_and_stops_everything_else(fake):
    out = io.StringIO()
    launched = Launcher([fake("web"), fake("ingest", "crash-later")], out=out)
    assert launched.run() == 1
    log = out.getvalue()
    assert "ERROR: ingest exited unexpectedly with code 3" in log
    assert "web: STDIN-EOF" in log and launched.services[0].proc.returncode == 0


def test_a_startup_failure_stops_the_launch(fake):
    out = io.StringIO()
    launched = Launcher([fake("web", "crash-at-start"), fake("ingest")], out=out)
    assert launched.run() == 1
    assert "ERROR: web exited during startup with code 4" in out.getvalue()
    assert launched.services[1].proc is None  # never started


def test_a_service_that_ignores_the_stop_request_is_terminated(fake):
    out = io.StringIO()
    launched = Launcher([fake("ingest", "ignore-stop", stop_timeout=1.0)], out=out)
    assert launched.run(monitor=stop_when_ready) == 0
    assert "ingest did not stop within 1 s; terminating it" in out.getvalue()
    assert launched.services[0].proc.returncode != 0


# ----------------------------------------------------------------------------- startup checks
@pytest.fixture
def offline_env(monkeypatch, tmp_path):
    """Configuration with the database off and no paid steps: preflight needs no server and no network."""
    values = {"DATASOURCES_DB_ENABLED": "false", "SIMILARITY_ENABLED": "false", "SUMMARY_ENABLED": "false",
              "INGEST_OUTPUT_DIR": str(tmp_path / "output"), "INGEST_SYMBOLS": "NVDA", "INGEST_DEMO_MODE": "false",
              "ALPACA_AUTH_MODE": "trading", "ALPACA_API_KEY": "test-key-id", "ALPACA_SECRET_KEY": "test-secret-key",
              "SEC_USER_AGENT": "TestSuite tests@example.test", "INGEST_LIVE_ONLY": "false"}
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    return values


def test_preflight_reports_an_occupied_port_and_a_running_worker(offline_env, tmp_path):
    from datasources.ingest.service import ProcessLock

    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        port = busy.getsockname()[1]
        root = Path(offline_env["INGEST_OUTPUT_DIR"])
        with ProcessLock(root):
            report = launcher.preflight(port=port, collectors=True)
    problems = " | ".join(report.problems)
    assert f"port {port} on 127.0.0.1 is already in use" in problems and f"--port {port + 1}" in problems
    assert "an ingestion worker is already running" in problems

    clean = launcher.preflight(port=free_port(), collectors=True)
    assert clean.problems == [] and clean.paid_calls is False
    lines = dict(clean.lines)
    assert "none with these settings" in lines["Paid model calls"]
    assert "test-secret-key" not in json.dumps(clean.lines) + json.dumps(clean.warnings)


def test_paid_model_calls_are_announced_from_the_settings(monkeypatch):
    from datasources.similarity.settings import SimilaritySettings
    from datasources.summary.settings import SummarySettings

    monkeypatch.setattr("datasources.similarity.comparison.anthropic_credentials_available", lambda: True)
    report = launcher.Preflight()
    launcher._describe_processing(report, SimilaritySettings(enabled=True, comparison_model="model-a"),
                                  SummarySettings(enabled=True, model="model-b"))
    lines = dict(report.lines)
    assert report.paid_calls and report.problems == []
    assert "POSSIBLE" in lines["Paid model calls"] and "model-a" in lines["Paid model calls"]
    assert "model-b" in lines["Paid model calls"] and "not retried automatically" in lines["Not scheduled"]

    quiet = launcher.Preflight()
    launcher._describe_processing(quiet, SimilaritySettings(enabled=True, comparison_backend="mock"),
                                  SummarySettings(enabled=False))
    assert quiet.paid_calls is False and "OFF (SUMMARY_ENABLED=false)" in dict(quiet.lines)["Event summaries"]

    blocked = launcher.Preflight()
    launcher._describe_processing(blocked, SimilaritySettings(enabled=False), SummarySettings(enabled=True))
    assert any("needs SIMILARITY_ENABLED=true" in p for p in blocked.problems)


def test_a_second_launcher_is_refused_with_the_first_ones_address(offline_env, capsys):
    state = Path(offline_env["INGEST_OUTPUT_DIR"]) / "state"
    state.mkdir(parents=True)
    (state / "launcher.json").write_text(json.dumps({"pid": 1234, "url": "http://127.0.0.1:8000"}), encoding="utf-8")
    with FileLock(str(state / "launcher.lock")):
        assert launcher.main(["--check"]) == 2
    err = capsys.readouterr().err
    assert "already running" in err and "pid 1234, http://127.0.0.1:8000" in err


def test_check_mode_starts_nothing(offline_env, capsys):
    assert launcher.main(["--check", "--port", str(free_port())]) == 0
    assert "Checks passed (--check: nothing was started)" in capsys.readouterr().out
    state = Path(offline_env["INGEST_OUTPUT_DIR"]) / "state"
    assert not (state / "launcher.json").exists() and not (state / "worker_status.json").exists()


# ----------------------------------------------------------------------------- real children
def child_env(values: dict) -> dict:
    env = dict(os.environ)
    env.update(values)
    return env


def test_the_web_child_shuts_down_gracefully_when_its_stdin_closes(offline_env):
    port = free_port()
    proc = subprocess.Popen([sys.executable, "-m", "datasources.launcher", "--service", "web", "--port", str(port)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            env=child_env(offline_env))
    try:
        for _ in range(150):
            try:
                body = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status", timeout=1).read())
                break
            except OSError:
                time.sleep(0.1)
        else:
            pytest.fail("web child did not start")
        assert body["collector"]["state"] == "not_running"
        proc.stdin.close()
        assert proc.wait(timeout=20) == 0
        log = proc.stdout.read().decode("utf-8", "replace")
        assert "Application shutdown complete" in log  # uvicorn's own graceful path, not a kill
    finally:
        if proc.poll() is None:
            proc.kill()


MOCKED_INGEST = textwrap.dedent("""
    # The real `datasources-ingest run` worker with mocked collector connections.
    import asyncio, json, sys
    from datetime import datetime, timezone

    import alpaca_news.client as client
    from datasources import sec
    from datasources.ingest import cli, service
    from datasources.ingest.sec_client import FetchResult

    async def fake_run_forever(settings, *, on_articles=None, on_subscribed=None, on_frame=None,
                               on_disconnected=None, **_):
        print("fake WebSocket: subscribed", flush=True)
        await on_subscribed()
        await asyncio.Event().wait()

    class FakeSecClient:
        def __init__(self, *args, **kwargs):
            pass

        def fetch_submissions(self, cik10, _max_bytes):  # no filings: nothing is stored
            recent = {key: [] for key in ("accessionNumber", "filingDate", "reportDate", "acceptanceDateTime",
                                          "form", "items", "primaryDocument", "primaryDocDescription")}
            data = {"cik": cik10, "name": "EXAMPLE", "tickers": ["NVDA"], "filings": {"recent": recent, "files": []}}
            body = json.dumps(data).encode()
            return data, FetchResult(url="https://data.sec.gov/fake", status=200, content_type="application/json",
                                     body=body, retrieved_at=datetime.now(timezone.utc))

    client.run_forever = fake_run_forever
    service.iter_news_pages = lambda *a, **k: iter(())
    cli.SecClient = FakeSecClient
    sec.save_submissions_cache = lambda *a, **k: None

    from datasources.launcher import run_child
    sys.exit(run_child("ingest", port=0, ingest_args=[]))
""")


def test_launcher_runs_the_web_app_and_the_worker_and_stops_both_cleanly(offline_env, tmp_path):
    from datasources.ingest.status import collector_status, load_status_file

    script = tmp_path / "mocked_ingest.py"
    script.write_text(MOCKED_INGEST, encoding="utf-8")
    port, root = free_port(), Path(offline_env["INGEST_OUTPUT_DIR"])
    web, _ = launcher.build_services(port=port, collectors=True, root=root, ingest_args=[])
    ingest = ServiceSpec("ingest", [sys.executable, str(script)], launcher._ingest_ready(root), 60, 30)
    seen = {}

    def check_then_stop(launched: Launcher) -> None:
        if not all(s.ready for s in launched.services):
            return
        status = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status", timeout=5).read())
        seen["collector"] = status["collector"]
        raise KeyboardInterrupt

    out = io.StringIO()
    launched = Launcher([web, ingest], out=out, env=child_env(offline_env))
    assert launched.run(monitor=check_then_stop) == 0, out.getvalue()
    log = out.getvalue()
    collector = seen["collector"]
    assert collector["state"] == "running" and collector["alpaca"]["stream_connected"] is True
    assert collector["sec"]["enabled"] is True and collector["processing"]["summaries"] is False
    assert "[ingest]" in log and "starting ingestion" in log and "fake WebSocket: subscribed" in log
    assert "startup [" in log and "connecting to the database" in log and "startup complete in" in log
    assert "[web]" in log and "Application shutdown complete" in log
    assert [s.proc.returncode for s in launched.services] == [0, 0]
    assert log.index("stopping ingest") < log.index("stopping web")
    assert collector_status(load_status_file(root))["state"] == "stopped"


@pytest.mark.skipif(os.name != "nt", reason="Windows console control events")
def test_console_ctrl_c_reaches_only_the_launcher_which_stops_the_services(tmp_path):
    """In a real (hidden) console: Ctrl+C goes to every process attached to it, but the services run in their own
    process groups, so only the launcher sees it; they stop because it closes their stdin."""
    script = tmp_path / "fake_service.py"
    script.write_text(FAKE_SERVICE, encoding="utf-8")
    driver = tmp_path / "driver.py"
    driver.write_text(textwrap.dedent(f"""
        import ctypes, sys, threading, time
        from pathlib import Path
        from datasources.launcher import Launcher, ServiceSpec
        # A terminal the user starts has Ctrl+C enabled; a test runner started from a process group with it
        # disabled passes that flag down (CREATE_NEW_CONSOLE keeps it), so enable it like a real console.
        ctypes.windll.kernel32.SetConsoleCtrlHandler(None, False)
        tmp = Path({str(tmp_path)!r})
        def spec(name):
            marker = tmp / f"{{name}}.ready"
            return ServiceSpec(name, [sys.executable, {str(script)!r}, name, "normal", str(marker)],
                               ready=lambda s: marker.exists(), ready_timeout=15, stop_timeout=10)
        def ctrl_c_when_ready():
            while not all((tmp / f"{{n}}.ready").exists() for n in ("web", "ingest")):
                time.sleep(0.1)
            time.sleep(0.5)
            ctypes.windll.kernel32.GenerateConsoleCtrlEvent(0, 0)  # CTRL_C_EVENT to the whole console
        threading.Thread(target=ctrl_c_when_ready, daemon=True).start()
        code = Launcher([spec("web"), spec("ingest")]).run()
        print(f"launcher exit code {{code}}", flush=True)
    """), encoding="utf-8")
    info = subprocess.STARTUPINFO()
    info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    info.wShowWindow = 0  # SW_HIDE
    log_path = tmp_path / "driver.log"
    with open(log_path, "wb") as log_file:
        proc = subprocess.Popen([sys.executable, str(driver)], stdin=subprocess.DEVNULL, stdout=log_file,
                                stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NEW_CONSOLE, startupinfo=info)
        try:
            proc.wait(timeout=60)
        finally:
            if proc.poll() is None:
                proc.kill()
    log = log_path.read_text(encoding="utf-8", errors="replace")
    assert "Ctrl+C received: stopping all services" in log, log
    assert "web: STDIN-EOF" in log and "ingest: STDIN-EOF" in log, log
    assert "DIRECT-CTRL-C" not in log, log
    assert "launcher exit code 0" in log, log


# ----------------------------------------------------------------------------- worker startup
NATIVE_IMPORT_CHILD = textwrap.dedent("""
    import sys, threading, time
    from datasources.launcher import _stop_when_stdin_closes
    threading.Thread(target=_stop_when_stdin_closes, daemon=True).start()
    time.sleep(0.5)  # the watcher is already waiting on stdin when the extension module loads
    try:
        import numpy  # its DLL initialisation queries the standard handles under the loader lock
        print("imported", flush=True)
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("stopped by stdin EOF", flush=True)
""")


def _lines(proc: subprocess.Popen) -> list[str]:
    lines: list[str] = []

    def pump():
        for raw in iter(proc.stdout.readline, b""):
            lines.append(raw.decode("utf-8", "replace").strip())

    threading.Thread(target=pump, daemon=True).start()
    return lines


def _wait_for(predicate, timeout: float) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.05)
    return False


@pytest.mark.skipif(os.name != "nt", reason="Windows pipe / loader-lock interaction")
def test_the_stdin_watcher_does_not_block_native_imports(tmp_path):
    """Regression: the watcher's blocking stdin read deadlocked `import numpy` (sentence-transformers) until stdin
    closed, so under `datasources-dev` the ingestion worker never finished starting."""
    script = tmp_path / "child.py"
    script.write_text(NATIVE_IMPORT_CHILD, encoding="utf-8")
    proc = subprocess.Popen([sys.executable, str(script)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    try:
        lines = _lines(proc)
        assert _wait_for(lambda: "imported" in lines, 30), f"import blocked while stdin was open: {lines}"
        proc.stdin.close()  # what the launcher does to stop a child
        assert proc.wait(timeout=10) == 0
        assert _wait_for(lambda: "stopped by stdin EOF" in lines, 5), lines
    finally:
        if proc.poll() is None:
            proc.kill()


STARTING_WORKER = textwrap.dedent("""
    # Reports startup stages like the real worker; "hang" never finishes, "fail" exits, "slow-ok" becomes ready.
    import logging, sys, threading, time
    from datetime import datetime, timezone
    from pathlib import Path
    from datasources.ingest.status import STATUS_VERSION, StartupReporter, status_path
    from datasources.ingest.store import atomic_write_json
    from datasources.launcher import _stop_when_stdin_closes

    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(message)s")
    root, mode = Path(sys.argv[1]), sys.argv[2]
    threading.Thread(target=_stop_when_stdin_closes, daemon=True).start()
    reporter = StartupReporter(root, ["NVDA"], interval=0.5)
    try:
        reporter.stage("checking collector settings")
        reporter.stage("loading the embedding model")
        if mode == "fail":
            sys.exit(2)
        if mode == "slow-ok":
            time.sleep(1.5)
            reporter.finish("collectors starting")
            now = datetime.now(timezone.utc).isoformat()
            atomic_write_json(status_path(root), {"version": STATUS_VERSION, "state": "running",
                                                  "started_at": reporter.started_at.isoformat(), "updated_at": now})
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("worker: interrupted", flush=True)
    finally:
        reporter.abort()
""")


@pytest.fixture
def starting_worker(tmp_path):
    script = tmp_path / "starting_worker.py"
    script.write_text(STARTING_WORKER, encoding="utf-8")
    root = tmp_path / "output"

    def spec(mode: str, ready_timeout: float = 20) -> ServiceSpec:
        return ServiceSpec("ingest", [sys.executable, str(script), str(root), mode], launcher._ingest_ready(root),
                           ready_timeout, 10, "no stage yet", progress=launcher._ingest_progress(root))

    return spec, root


def test_a_worker_that_never_becomes_ready_is_stopped_with_its_last_stage(fake, starting_worker):
    from dataclasses import replace

    from datasources.ingest.status import collector_status, load_status_file

    spec, root = starting_worker
    web = replace(fake("web"), ready_message="Web app ready: the page shows stored data now")
    out = io.StringIO()
    launched = Launcher([web, spec("hang", ready_timeout=4)], out=out)
    began = time.monotonic()
    assert launched.run() == 1
    assert time.monotonic() - began < 30  # bounded: timeout + graceful stop
    log = out.getvalue()
    assert log.index("Web app ready") < log.index("started ingest")  # the page is usable while the worker starts
    assert "startup [" in log and "loading the embedding model" in log  # the worker's own flushed stage lines
    assert "ERROR: ingest was not ready within 4 s; its last reported stage: loading the embedding model" in log
    assert "worker: interrupted" in log and "web: STDIN-EOF" in log  # controlled shutdown, not a kill
    assert [s.proc.returncode for s in launched.services] == [0, 0]
    collector = collector_status(load_status_file(root))
    assert collector["state"] == "stopped" and collector["stage"] == "loading the embedding model"


def test_a_worker_that_fails_during_startup_stops_the_launch(fake, starting_worker):
    from datasources.ingest.status import collector_status, load_status_file

    spec, root = starting_worker
    out = io.StringIO()
    launched = Launcher([fake("web"), spec("fail")], out=out)
    assert launched.run() == 1
    log = out.getvalue()
    assert "ERROR: ingest exited during startup with code 2" in log and "web: STDIN-EOF" in log
    assert collector_status(load_status_file(root))["state"] == "stopped"


def test_a_slow_worker_start_is_waited_for_and_then_ready(fake, starting_worker):
    spec, root = starting_worker
    out = io.StringIO()
    launched = Launcher([fake("web"), spec("slow-ok")], out=out)
    assert launched.run(monitor=stop_when_ready) == 0
    log = out.getvalue()
    assert "ingest is ready" in log and "startup complete in" in log and "ERROR" not in log
