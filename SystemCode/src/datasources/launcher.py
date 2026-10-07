"""`datasources-dev`: start the local application (web app + API + collectors) with one command.

    uv run datasources-dev                  # web app, API and the ingestion worker
    uv run datasources-dev --check          # configuration, database and port checks only; starts nothing
    uv run datasources-dev --no-collectors  # web app and API only (no collectors)
    uv run datasources-dev --port 8001      # another port for the web app

Services, each in its own child process, with every log line prefixed by the service name:

    web     `datasources.server` (FastAPI + the static frontend) on 127.0.0.1, without reload.
            It only reads stored data and starts no collectors, so browser tabs and page
            refreshes never start ingestion, grouping or summaries.
    summary one-shot startup catch-up (`summary.catchup`), with or without collectors, when
            SUMMARY_ENABLED=true and SUMMARY_STARTUP_CATCHUP_DAYS > 0: recent events without a
            current summary are summarized (bounded paid requests), then it exits.
    ingest  the existing worker (`datasources-ingest run`): Alpaca WebSocket with REST
            recovery at startup and after every reconnect, SEC EDGAR polling, and the
            inline grouping / summary steps that SIMILARITY_ENABLED / SUMMARY_ENABLED enable.

One launcher per output directory (a file lock), one ingestion worker per output directory
and one graph writer per database partition (the worker's own locks) keep a second
launch, a second worker or a stray `datasources-ingest run` from collecting twice.
Nothing is installed, migrated or backfilled here: problems are reported with the command
that fixes them. Ctrl+C stops the worker first (it finishes the record it is writing) and
then the web server; a second Ctrl+C stops them immediately.

Stopping works without console signals: each child watches its stdin, and the launcher
closes that pipe to ask it to stop. The child then raises SIGINT in itself, which runs the
same graceful shutdown as pressing Ctrl+C in its own window. If the launcher dies, the pipe
closes too, so no collector is left running on its own.
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, TextIO

from filelock import FileLock, Timeout

from . import config

HOST = "127.0.0.1"
DEFAULT_PORT = 8000
WEB_READY_SECONDS = 45.0
# Preparation only (database, local embedding model, ledger): collection and backlog recovery start after it.
# Normally well under a minute; past this the worker is stopped and its last reported stage is shown.
INGEST_READY_SECONDS = 180.0
# The worker may be finishing a record (and its model calls); the catch-up a summary request.
STOP_SECONDS = {"web": 15.0, "ingest": 90.0, "summary": 90.0}
SECRET_VARIABLES = ("ALPACA_API_KEY", "ALPACA_SECRET_KEY", "ALPACA_BROKER_CLIENT_ID", "ALPACA_BROKER_CLIENT_SECRET",
                    "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "PGPASSWORD")


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ============================================================================ child side
STDIN_POLL_SECONDS = 0.25
_PIPE_CLOSED = {109, 232, 233}  # ERROR_BROKEN_PIPE, ERROR_NO_DATA, ERROR_PIPE_NOT_CONNECTED


def _wait_for_stdin_eof_windows() -> bool:
    """Poll the stdin pipe until the launcher closes it. False when stdin is not a pipe.

    Never a blocking read: on Windows a thread blocked in ReadFile on the stdin pipe makes
    other calls on that handle wait for it, and DLL initialisation queries the standard
    handles under the loader lock. `import numpy` (via sentence-transformers) then hung
    until stdin closed, so the worker never finished starting.
    """
    import _winapi
    import msvcrt

    fd = sys.stdin.fileno()
    handle = msvcrt.get_osfhandle(fd)
    while True:
        try:
            available, _left = _winapi.PeekNamedPipe(handle, 0)
        except OSError as error:
            if getattr(error, "winerror", None) in _PIPE_CLOSED:
                return True
            return False
        if available:
            os.read(fd, available)  # drain; never blocks (the bytes are already there)
        else:
            time.sleep(STDIN_POLL_SECONDS)


def _stop_when_stdin_closes() -> None:
    """Wait until the launcher closes our stdin (or dies), then stop gracefully via our own SIGINT handler."""
    try:
        if os.name != "nt" or not _wait_for_stdin_eof_windows():
            while sys.stdin.buffer.read(4096):
                pass
    except (OSError, ValueError):
        pass
    signal.raise_signal(signal.SIGINT)


def run_child(service: str, *, port: int, ingest_args: list[str]) -> int:
    threading.Thread(target=_stop_when_stdin_closes, name="stop-on-stdin-eof", daemon=True).start()
    try:
        if service == "web":
            from .server import main as serve

            serve(host=HOST, port=port)
            return 0
        if service == "summary":
            from .summary.catchup import main as catchup

            return catchup()
        from .ingest.cli import main as ingest

        return ingest(["run", *ingest_args])
    except KeyboardInterrupt:  # stopped during startup, before a graceful handler was installed
        return 130


# ============================================================================ preflight
@dataclass
class Preflight:
    problems: list[str] = field(default_factory=list)  # any problem stops the launch
    warnings: list[str] = field(default_factory=list)
    lines: list[tuple[str, str]] = field(default_factory=list)  # (label, text) for the startup summary
    paid_calls: bool = False
    output_root: Path | None = None
    database_ok: bool = False  # reachable, with the grouping and summary tables
    catchup: bool = False  # start the one-shot summary catch-up


def port_in_use(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        if os.name == "nt":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        try:
            sock.bind((host, port))
        except OSError:
            return True
    return False


def _graph_writer_holder(database) -> str | None:
    """Who holds the live graph-writer lock in THIS database, if anyone (read-only: pg_locks, never acquired here).
    pg_locks lists every database's advisory locks, and a test database's writer must not count."""
    from .similarity.lock import LOCK_NAMESPACE, PARTITIONS

    with database.connection() as conn:
        row = conn.execute(
            "SELECT a.application_name, a.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid "
            "WHERE l.locktype = 'advisory' AND l.classid = %s::oid AND l.objid = %s::oid AND l.granted "
            "AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())",
            (LOCK_NAMESPACE, PARTITIONS[False]),
        ).fetchone()
    return f"{row['application_name'] or 'unknown application'} (PostgreSQL backend pid {row['pid']})" if row else None


def preflight(*, port: int, collectors: bool, sec: bool = True, alpaca: bool = True) -> Preflight:
    """Every check that can fail before a process starts. Reads configuration and the database; writes nothing,
    calls no model, and never prints a credential (only whether one is present)."""
    from .ingest.service import ProcessLock
    from .ingest.settings import ConfigurationError, load_settings, require_sec_user_agent

    report = Preflight()
    add = report.lines.append
    url = f"http://{HOST}:{port}"
    add(("Web app and API", f"{url}  (FastAPI serves the page; read-only, no model calls)"))
    if port_in_use(HOST, port):
        report.problems.append(f"port {port} on {HOST} is already in use (another datasources-server or "
                               f"datasources-dev?). Stop it, or pick a free port: uv run datasources-dev --port "
                               f"{port + 1}")
    try:
        settings = load_settings()
    except ConfigurationError as error:
        report.problems.append(f"configuration: {error}")
        return report
    report.output_root = settings.root

    similarity = summary = None
    if collectors:
        if settings.demo:
            report.problems.append("INGEST_DEMO_MODE=true: live collectors cannot run in demo mode (set it to "
                                   "false, or use `uv run datasources-ingest demo` for sample data)")
        parts = []
        if alpaca:
            from .alpaca import AlpacaConfigError, auth_from_env

            try:
                recovery = "" if settings.live_only else " + REST recovery"
                parts.append(f"Alpaca news WebSocket{recovery} ({auth_from_env().mode} credentials present)")
            except AlpacaConfigError as error:
                report.problems.append(str(error))
        if sec:
            try:
                require_sec_user_agent(settings)
                for symbol in settings.symbols:
                    config.cik_for(symbol)
                parts.append(f"SEC EDGAR {'/'.join(settings.sec_forms)} polling every "
                             f"{settings.sec_poll_interval_seconds:g} s")
            except (ConfigurationError, KeyError) as error:
                report.problems.append(error.args[0] if isinstance(error, KeyError) else str(error))
        add(("Collectors", f"{'; '.join(parts) or 'none'} — symbols {', '.join(settings.symbols)}"))
        add(("Recovery", recovery_summary(settings)))
        try:
            with ProcessLock(settings.root):
                pass
        except ConfigurationError:
            report.problems.append(f"an ingestion worker is already running for {settings.root} (it holds "
                                   f"{settings.root / 'state' / 'ingest.lock'}); stop it first, e.g. Ctrl+C in its "
                                   "window")
        try:
            from .similarity.settings import load_similarity_settings
            from .summary.settings import load_summary_settings

            similarity, summary = load_similarity_settings(), load_summary_settings()
        except ConfigurationError as error:
            report.problems.append(f"configuration: {error}")
    else:
        add(("Collectors", "not started (--no-collectors): the page shows stored data only"))
        try:
            from .summary.settings import load_summary_settings

            summary = load_summary_settings()
        except ConfigurationError as error:  # only the catch-up needs these; the page still works
            report.warnings.append(f"summary catch-up off: configuration: {error}")

    _check_database(report, collectors=collectors, similarity=similarity, summary=summary,
                    symbols=settings.symbols if collectors else ())
    if collectors and similarity is not None and summary is not None:
        _describe_processing(report, similarity, summary, live_only=settings.live_only)
    if summary is not None:
        _describe_catchup(report, summary)
    return report


def recovery_summary(settings) -> str:
    """The startup summary's Recovery line. Live-only mode also lists the saved recovery state it leaves untouched,
    read from the ledger file (read-only; nothing is created)."""
    if not settings.live_only:
        return (f"at startup: Alpaca news since the last sync (at most {settings.alpaca_recovery_max_hours} h, "
                f"{settings.alpaca_backfill_max_pages} pages) and SEC filings from the last "
                f"{settings.sec_initial_lookback_days} days not yet stored")
    saved: dict = {}
    path = settings.root / "state" / "ledger.json"
    try:
        saved = json.loads(path.read_text(encoding="utf-8")).get("cursors") or {}
    except FileNotFoundError:
        pass
    except (OSError, ValueError):
        saved = {"unreadable": True}
    floors = sorted(k.split(":", 1)[1] for k in saved if k.startswith("sec_scope_floor:"))
    cursor = saved.get("alpaca_synced_through")
    kept = ("unreadable ledger, left as it is" if saved.get("unreadable") else
            f"Alpaca cursor {cursor or 'none'}, SEC scope floor(s) for {len(floors)} compan"
            f"{'y' if len(floors) == 1 else 'ies'}")
    return ("OFF (INGEST_LIVE_ONLY=true): each startup takes a fresh UTC cutoff and only news and SEC filings "
            "published after it are collected; no SEC catch-up, no Alpaca REST backfill at startup or after a "
            "reconnect, and after a reconnect only news published from then on. Saved recovery state is kept "
            f"untouched for later ({kept})")


def _check_database(report: Preflight, *, collectors: bool, similarity, summary, symbols=()) -> None:
    from .db import Database, DatabaseError, db_enabled, load_db_settings
    from .db.companies import unregistered_symbols
    from .db.migrate import BASELINE, applied_migrations, migration_files
    from .ingest.settings import ConfigurationError

    try:
        enabled = db_enabled()
    except ConfigurationError as error:
        report.problems.append(f"configuration: {error}")
        return
    if not enabled:
        report.lines.append(("Database", "DATASOURCES_DB_ENABLED=false: records go to files only; the Events and "
                                         "All articles tabs need PostgreSQL"))
        report.warnings.append("the database is disabled, so the page has no stored events or articles to show")
        if collectors and (similarity is not None and similarity.enabled or summary is not None and summary.enabled):
            report.problems.append("SIMILARITY_ENABLED / SUMMARY_ENABLED need PostgreSQL (DATASOURCES_DB_ENABLED=true)")
        return
    try:
        database = Database(load_db_settings())
    except ConfigurationError as error:
        report.problems.append(f"database configuration: {error}")
        return
    try:
        info = database.check()
        report.lines.append(("Database", info.describe()))
        with database.connection() as conn:
            from .similarity.store import missing_schema as grouping_missing
            from .summary.store import missing_schema as summary_missing

            missing = []
            unlinked: list[str] = []
            if not conn.execute("SELECT to_regclass('public.document') IS NOT NULL AS ok").fetchone()["ok"]:
                missing.append("the base stock_news tables (document, document_revision, ...)")
            else:
                unlinked = unregistered_symbols(conn, symbols)
                if similarity is None or similarity.enabled or summary is None or summary.enabled:
                    missing += grouping_missing(conn)
                    if summary is None or summary.enabled:
                        missing += summary_missing(conn)
        if unlinked:
            report.warnings.append(
                f"INGEST_SYMBOLS includes {', '.join(unlinked)}, which {'has' if len(unlinked) == 1 else 'have'} no "
                "company_symbol row in this database: those articles would be stored without a company link (not "
                "shown under that company or in watchlists). `uv run datasources-db register-companies --dry-run` "
                "shows what registration would add")
        report.database_ok = not missing
        if missing:
            # Without collectors only the page is affected (its Events tab needs the event tables).
            (report.problems if collectors else report.warnings).append(
                f"database {info.database} is missing {', '.join(missing)}; `uv run datasources-db migrate --dry-run` "
                "shows what the migrations would do, `uv run datasources-db migrate` applies them (this launcher "
                "never changes the schema)")
        unrecorded = sorted(p.name for p in migration_files() if p.name not in applied_migrations(database))
        if unrecorded and not missing:
            adopt = ("; the existing base schema is recorded with `migrate --adopt-baseline` (checked first, never "
                     "executed)" if BASELINE in unrecorded else "")
            report.warnings.append(f"migration file(s) not recorded in schema_migration: {', '.join(unrecorded)}. "
                                   "The required tables and columns are present, so the launch continues. "
                                   "`uv run datasources-db migrate --dry-run` shows what migrate would do "
                                   f"(read-only){adopt}")
        if collectors and (holder := _graph_writer_holder(database)):
            report.problems.append(f"another process is writing grouping results for the live data ({holder}): "
                                   "an ingestion worker, or a `datasources-similarity backfill`/`retry`. Stop it "
                                   "first; only one graph writer may run")
    except DatabaseError as error:
        report.problems.append(f"database: {error}")
    finally:
        database.close()


def _describe_processing(report: Preflight, similarity, summary, live_only: bool = False) -> None:
    from .similarity.comparison import anthropic_credentials_available

    add = report.lines.append
    paid = []
    if similarity.enabled:
        comparison = f"{similarity.comparison_backend} comparison ({similarity.comparison_model})"
        if similarity.comparison_backend == "anthropic":
            paid.append(f"up to {similarity.llm_max_calls_per_record} comparison request(s) to "
                        f"{similarity.comparison_model} (each retried up to {similarity.llm_max_retries} time(s))")
            if not anthropic_credentials_available():
                report.problems.append("SIMILARITY_ENABLED=true with the anthropic backend needs ANTHROPIC_API_KEY")
        add(("Event grouping", f"ON (SIMILARITY_ENABLED=true): {similarity.embedding_backend} embeddings "
                               f"({similarity.embedding_model}) + {comparison}, inline in the worker"))
    else:
        add(("Event grouping", "OFF (SIMILARITY_ENABLED=false): records are stored without grouping"))
    if summary.enabled:
        if not similarity.enabled:
            report.problems.append("SUMMARY_ENABLED=true needs SIMILARITY_ENABLED=true (summaries are per event)")
        if summary.backend == "anthropic":
            paid.append(f"up to {summary.max_requests_per_run} summary request(s) to {summary.model} (each retried up "
                        f"to {summary.llm_max_retries} time(s))")
            if not anthropic_credentials_available():
                report.problems.append("SUMMARY_ENABLED=true needs ANTHROPIC_API_KEY")
        add(("Event summaries", f"ON (SUMMARY_ENABLED=true): {summary.backend} ({summary.model}), generated "
                                "inline after grouping; published versions and citations are kept"))
    else:
        add(("Event summaries", "OFF (SUMMARY_ENABLED=false): no summaries are generated; stored ones stay visible"))
    report.paid_calls = bool(paid)
    if paid:
        which = ("published after the startup cutoff (live-only: nothing is recovered)" if live_only
                 else "(including ones recovered at startup)")
        add(("Paid model calls", f"POSSIBLE. Each newly ingested article or filing {which} can send "
                                 f"{' and '.join(paid)} to the Anthropic API. Page views and API polling never call "
                                 "a model"))
    else:
        add(("Paid model calls", "none with these settings (no enabled step uses a paid backend)"))
    summaries = ("failed summaries, and queued ones older than the startup catch-up window, need "
                 if summary.enabled and summary.startup_catchup_days > 0 else "queued/failed summaries need ")
    add(("Not scheduled", "deferred work is not retried automatically while running: pending groupings need "
                          f"`uv run datasources-similarity retry` (with the worker stopped) and {summaries}"
                          "`uv run datasources-summary retry --confirm-paid-calls`"))


def _describe_catchup(report: Preflight, summary) -> None:
    """The one-shot startup catch-up, with or without collectors. Without its prerequisites it stays off and the
    rest of the launch goes on (with collectors, those prerequisites are already problems above)."""
    from .similarity.comparison import anthropic_credentials_available

    label = "Summary catch-up"
    if not summary.enabled:
        report.lines.append((label, "OFF (SUMMARY_ENABLED=false)"))
        return
    if summary.startup_catchup_days <= 0:
        report.lines.append((label, "OFF (SUMMARY_STARTUP_CATCHUP_DAYS=0)"))
        return
    if not report.database_ok:
        report.lines.append((label, "OFF: it needs PostgreSQL with the summary tables (see above)"))
        return
    if summary.backend == "anthropic" and not anthropic_credentials_available():
        report.lines.append((label, "OFF: SUMMARY_BACKEND=anthropic needs ANTHROPIC_API_KEY"))
        report.warnings.append("summary catch-up off: ANTHROPIC_API_KEY is not set")
        return
    report.catchup = True
    paid = summary.backend == "anthropic"
    report.paid_calls = report.paid_calls or paid
    report.lines.append((label, (
        f"ON, once per start: events with an article from the last {summary.startup_catchup_days:g} day(s) and no "
        "current summary (or queued / interrupted summary work) are summarized, newest first, with at most "
        f"{summary.startup_catchup_max_requests} {'PAID ' if paid else ''}request(s) to {summary.model} (each "
        f"retried up to {summary.llm_max_retries} time(s)); then it exits. Log: logs/summary_catchup.log")))


# ============================================================================ supervision
@dataclass
class ServiceSpec:
    name: str
    argv: list[str]
    ready: Callable[["Service"], bool]
    ready_timeout: float
    stop_timeout: float
    waiting_hint: str = ""
    ready_message: str = ""  # said once the service is ready (e.g. the web address while the worker still starts)
    progress: Callable[["Service"], str | None] | None = None  # what a starting service reports it is doing
    oneshot: bool = False  # a task that exits by itself when done: not waited for, and its exit stops nothing


class Service:
    def __init__(self, spec: ServiceSpec):
        self.spec = spec
        self.proc: subprocess.Popen | None = None
        self.started_at: datetime | None = None
        self.ready = False
        self.failure = ""  # why startup failed, when it did
        self.recent: collections.deque[str] = collections.deque(maxlen=15)
        self.pump: threading.Thread | None = None
        self.finished = False  # a one-shot task's exit was reported

    @property
    def name(self) -> str:
        return self.spec.name

    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None


class Launcher:
    def __init__(self, specs: list[ServiceSpec], *, out: TextIO | None = None, env: dict[str, str] | None = None,
                 cwd: Path | None = None, ready_banner: str = ""):
        self.services = [Service(spec) for spec in specs]
        self.out = out or sys.stdout
        self.env = env
        self.cwd = cwd
        self.ready_banner = ready_banner
        self._lock = threading.Lock()
        secrets = [os.getenv(name, "") for name in SECRET_VARIABLES]
        self._secrets = [s for s in secrets if s and len(s) >= 6]
        self.stopping = False

    # ------------------------------------------------------------------ output
    def say(self, text: str, source: str = "dev") -> None:
        for secret in self._secrets:
            text = text.replace(secret, "***")
        with self._lock:
            self.out.write(f"[{source}]{' ' * max(1, 8 - len(source))}{text}\n")
            self.out.flush()

    def _pump(self, service: Service) -> None:
        assert service.proc is not None and service.proc.stdout is not None
        for raw in iter(service.proc.stdout.readline, b""):
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            service.recent.append(line)
            self.say(line, service.name)

    # ------------------------------------------------------------------ lifecycle
    def start(self, service: Service) -> None:
        env = dict(self.env if self.env is not None else os.environ)
        env.update({"PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"})
        kwargs: dict = {}
        if os.name == "nt":
            # Own process group: a console Ctrl+C reaches only this launcher, which then stops each
            # service in order instead of every process receiving it at once.
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True
        service.started_at = _now()
        service.proc = subprocess.Popen(service.spec.argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, env=env, cwd=self.cwd, **kwargs)
        service.pump = threading.Thread(target=self._pump, args=(service,), name=f"log:{service.name}", daemon=True)
        service.pump.start()
        service.ready = service.spec.oneshot  # nothing to wait for: it reports its own progress
        self.say(f"started {service.name}")

    def report_finished(self, service: Service) -> None:
        """Say once that a one-shot task exited (after relaying its last lines). Its exit code stops nothing."""
        if service.finished or service.running():
            return
        service.finished = True
        if service.pump is not None:
            service.pump.join(timeout=2)
        code = service.proc.returncode if service.proc else None
        self.say(f"{service.name} finished" + ("" if code == 0 else f" with exit code {code}; see its lines above. "
                                                "The other services keep running"))

    def check_ready(self, service: Service) -> bool:
        if not service.ready:
            try:
                service.ready = bool(service.spec.ready(service))
            except Exception:  # not ready yet (connection refused, file mid-write, ...)
                return False
            if service.ready:
                self.say(f"{service.name} is ready")
        return service.ready

    def _progress(self, service: Service) -> str | None:
        try:
            return service.spec.progress(service) if service.spec.progress else None
        except Exception:  # status file missing or mid-write: nothing to report yet
            return None

    def wait_ready(self, service: Service) -> bool:
        """True once the service reports ready. False when it exits during startup or is not ready within its
        bounded timeout; `service.failure` then says which (and the last stage it reported)."""
        started = time.monotonic()
        deadline = started + service.spec.ready_timeout
        last_progress, next_note = None, started + 10
        while True:
            if not service.running():
                service.failure = "exited during startup"
                return False
            if self.check_ready(service):
                return True
            now = time.monotonic()
            last_progress = self._progress(service) or last_progress
            if now >= next_note and (last_progress or service.spec.waiting_hint):
                self.say(f"waiting for {service.name} ({now - started:.0f} s of at most "
                         f"{service.spec.ready_timeout:.0f} s): {last_progress or service.spec.waiting_hint}")
                next_note = now + 30
            if now > deadline:
                stage = f"; its last reported stage: {last_progress}" if last_progress else \
                    "; it never reported a startup stage (it may be stuck before its first log line)"
                service.failure = f"was not ready within {service.spec.ready_timeout:.0f} s{stage}"
                return False
            time.sleep(0.25)

    def request_stop(self, service: Service) -> None:
        if service.proc is None or service.proc.stdin is None:
            return
        try:
            service.proc.stdin.close()  # the child's stdin watcher raises SIGINT in the child
        except OSError:
            pass

    def stop_all(self) -> None:
        """Stop in reverse start order (collectors before the web server), each within its timeout. One-shot
        tasks are asked first, so they finish their current step while the others stop."""
        self.stopping = True
        deadlines: dict[str, float] = {}
        for service in self.services:
            if service.spec.oneshot and service.running():
                self.say(f"stopping {service.name} (letting its current step finish)")
                self.request_stop(service)
                deadlines[service.name] = time.monotonic() + service.spec.stop_timeout
        for service in reversed(self.services):
            if not service.running():
                continue
            if service.name not in deadlines:
                self.say(f"stopping {service.name}" + (" (finishing the record being written)"
                                                       if service.name == "ingest" else ""))
                self.request_stop(service)
            timeout = (max(deadlines[service.name] - time.monotonic(), 0.1) if service.name in deadlines
                       else service.spec.stop_timeout)
            try:
                service.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.say(f"{service.name} did not stop within {service.spec.stop_timeout:.0f} s; terminating it "
                         "(queued work stays unmarked and is retried on the next start)")
                service.proc.kill()
                service.proc.wait()
            except KeyboardInterrupt:
                self.say("second Ctrl+C: terminating the remaining services now")
                for other in self.services:
                    if other.running():
                        other.proc.kill()
                        other.proc.wait()
                break
        for service in self.services:
            if service.finished:  # a one-shot task that ended earlier was reported then
                continue
            if service.pump is not None:
                service.pump.join(timeout=2)
            if service.proc is not None and service.proc.returncode is not None:
                self.say(f"{service.name} stopped (exit code {service.proc.returncode})")

    def _failed(self, service: Service, during: str) -> int:
        if service.running():  # not ready in time: stop it the normal way first (bounded, then killed)
            self.say(f"ERROR: {service.name} {service.failure}. Stopping everything; see its log lines above.")
            self.stop_all()
            self.say(f"{service.name} was stopped after a startup timeout; nothing is left running. "
                     "`uv run datasources-dev --no-collectors` shows the stored data without collectors.")
            return 1
        code = service.proc.returncode if service.proc else None
        if service.pump is not None:
            service.pump.join(timeout=2)
        self.say(f"ERROR: {service.name} exited {during} with code {code}. Its last lines are above; nothing "
                 "else is left running.")
        self.stop_all()
        return 1

    def run(self, *, monitor: Callable[["Launcher"], None] | None = None) -> int:
        try:
            for service in self.services:
                self.start(service)
                if service.spec.oneshot:
                    continue
                if not self.wait_ready(service):
                    return self._failed(service, "during startup")
                if service.spec.ready_message:
                    self.say(service.spec.ready_message)
            if self.ready_banner:
                self.say(self.ready_banner)
            while True:
                for service in self.services:
                    if service.spec.oneshot:
                        self.report_finished(service)
                        continue
                    if not service.running():
                        return self._failed(service, "unexpectedly")
                    self.check_ready(service)  # announces a service that was slow to start
                if monitor is not None:
                    monitor(self)
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.say("Ctrl+C received: stopping all services")
            self.stop_all()
            return 0


# ============================================================================ the real services
def _web_ready(port: int) -> Callable[[Service], bool]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def ready(_service: Service) -> bool:
        with opener.open(f"http://{HOST}:{port}/api/status", timeout=2) as response:
            body = json.loads(response.read())
        return response.status == 200 and "collector" in body  # our server, not something else on the port

    return ready


def _this_run(raw: dict | None, service: Service) -> bool:
    """The status file was written by this worker, not left by a previous run."""
    if not raw or service.started_at is None or not raw.get("started_at"):
        return False
    return datetime.fromisoformat(raw["started_at"]) >= service.started_at - timedelta(seconds=2)


def _ingest_ready(root: Path) -> Callable[[Service], bool]:
    from .ingest.status import load_status_file

    def ready(service: Service) -> bool:
        raw = load_status_file(root)
        return _this_run(raw, service) and raw.get("state") == "running"

    return ready


def _ingest_progress(root: Path) -> Callable[[Service], str | None]:
    from .ingest.status import load_status_file

    def progress(service: Service) -> str | None:
        raw = load_status_file(root)
        if not _this_run(raw, service) or raw.get("state") != "starting" or not raw.get("stage"):
            return None
        since = datetime.fromisoformat(raw["stage_started_at"])
        return f"{raw['stage']} (for {(_now() - since).total_seconds():.0f} s)"

    return progress


class _HeartbeatWatch:
    """Warn (once per episode) when the running worker stops writing its status heartbeat."""

    def __init__(self, root: Path):
        self.root = root
        self.warned = False
        self.next_check = 0.0

    def __call__(self, launcher: Launcher) -> None:
        from .ingest.status import collector_status, load_status_file

        if time.monotonic() < self.next_check:
            return
        self.next_check = time.monotonic() + 15
        ingest = next((s for s in launcher.services if s.name == "ingest"), None)
        if ingest is None or not ingest.ready:
            return
        state = collector_status(load_status_file(self.root))["state"]
        if state == "stale" and not self.warned:
            launcher.say("WARNING: the ingestion worker is running but has not written a heartbeat for over a "
                         "minute; the page shows its collector status as unknown")
            self.warned = True
        elif state == "running" and self.warned:
            launcher.say("the ingestion worker's heartbeat is back")
            self.warned = False


def build_services(*, port: int, collectors: bool, root: Path, ingest_args: list[str],
                   catchup: bool = False) -> list[ServiceSpec]:
    child = [sys.executable, "-m", "datasources.launcher"]
    url = f"http://{HOST}:{port}"
    specs = [ServiceSpec("web", [*child, "--service", "web", "--port", str(port)], _web_ready(port),
                         WEB_READY_SECONDS, STOP_SECONDS["web"], "starting the web server",
                         ready_message=f"Web app ready: {url} shows the stored data now; the ingestion worker is "
                                       "still starting (the page shows its progress)" if collectors else "")]
    if catchup:  # right after the page is up; the worker (if any) starts meanwhile, leases keep them apart
        specs.append(ServiceSpec("summary", [*child, "--service", "summary"], lambda _service: True, 0,
                                 STOP_SECONDS["summary"], oneshot=True))
    if collectors:
        specs.append(ServiceSpec("ingest", [*child, "--service", "ingest", *ingest_args], _ingest_ready(root),
                                 INGEST_READY_SECONDS, STOP_SECONDS["ingest"],
                                 "the worker has not reported a startup stage yet",
                                 progress=_ingest_progress(root)))
    return specs


def print_summary(report: Preflight, out: TextIO) -> None:
    width = max(len(label) for label, _ in report.lines) if report.lines else 0
    out.write("\nStock News: local application\n")
    for label, text in report.lines:
        out.write(f"  {label:<{width}}  {text}\n")
    for warning in report.warnings:
        out.write(f"  ! {warning}\n")
    for problem in report.problems:
        out.write(f"  X {problem}\n")
    out.write("\n")
    out.flush()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="datasources-dev", description=__doc__.split("\n\n")[0])
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"web app port on {HOST} (default {DEFAULT_PORT})")
    parser.add_argument("--no-collectors", action="store_true", help="start only the web app and API")
    parser.add_argument("--no-sec", action="store_true", help="collect Alpaca news only")
    parser.add_argument("--no-alpaca", action="store_true", help="poll SEC EDGAR only")
    parser.add_argument("--check", action="store_true", help="run the startup checks, print the summary, start nothing")
    parser.add_argument("--service", choices=["web", "ingest", "summary"], help=argparse.SUPPRESS)  # child mode
    args = parser.parse_args(argv)
    ingest_args = [flag for flag, on in (("--no-sec", args.no_sec), ("--no-alpaca", args.no_alpaca)) if on]
    if args.service:
        return run_child(args.service, port=args.port, ingest_args=ingest_args)

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    if os.name == "nt":
        signal.signal(signal.SIGBREAK, signal.default_int_handler)  # Ctrl+Break stops like Ctrl+C
    collectors = not args.no_collectors
    if collectors and args.no_sec and args.no_alpaca:
        parser.error("--no-sec and --no-alpaca together leave nothing to collect; use --no-collectors")

    from .ingest.settings import ConfigurationError, load_settings

    try:
        root = load_settings().root
    except ConfigurationError as error:
        print(f"Not started: configuration error: {error}", file=sys.stderr)
        return 2
    # One launcher per output directory, checked first so a repeated launch gets the clearest message.
    state_dir = root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(state_dir / "launcher.lock"))
    try:
        lock.acquire(timeout=0)
    except Timeout:
        print(f"Not started: `datasources-dev` is already running for {root} ({_describe_other_launcher(state_dir)})."
              " Use that one, or stop it with Ctrl+C in its window.", file=sys.stderr)
        return 2
    info_path = state_dir / "launcher.json"
    try:
        report = preflight(port=args.port, collectors=collectors, sec=not args.no_sec, alpaca=not args.no_alpaca)
        print_summary(report, sys.stdout)
        if report.problems:
            print(f"Not started: fix the {len(report.problems)} problem(s) marked X above.", file=sys.stderr)
            return 2
        if args.check:
            print("Checks passed (--check: nothing was started).")
            return 0
        info_path.write_text(json.dumps({"pid": os.getpid(), "url": f"http://{HOST}:{args.port}",
                                         "started_at": _now().isoformat()}), encoding="utf-8")
        specs = build_services(port=args.port, collectors=collectors, root=root, ingest_args=ingest_args,
                               catchup=report.catchup)
        launcher = Launcher(specs, cwd=config.ROOT,
                            ready_banner=f"Ready: open http://{HOST}:{args.port} in a browser. Ctrl+C stops everything.")
        return launcher.run(monitor=_HeartbeatWatch(root) if collectors else None)
    finally:
        info_path.unlink(missing_ok=True)  # ours: nothing else can write it while we hold the lock
        lock.release()


def _describe_other_launcher(state_dir: Path) -> str:
    try:
        other = json.loads((state_dir / "launcher.json").read_text(encoding="utf-8"))
        return f"pid {other.get('pid')}, {other.get('url')}"
    except (OSError, ValueError):
        return "details unavailable"


if __name__ == "__main__":
    raise SystemExit(main())
