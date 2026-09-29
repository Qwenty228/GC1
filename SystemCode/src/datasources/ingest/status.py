"""The ingestion worker's status file: the only source of collector state for the web page and the launcher.

The live worker (`datasources-ingest run`) rewrites ``<output root>/state/worker_status.json``
every HEARTBEAT_SECONDS while it runs and marks it ``stopped`` on a clean shutdown.
Readers never infer collector health from anything else (an API request succeeding
says nothing about the collectors):

    not_running  no status file: no worker has run with this output directory
    starting     the worker is preparing (database, embedding model, ledger) before its
                 collectors start; `stage` says which step, refreshed every HEARTBEAT_SECONDS
    stopped      the worker shut down cleanly (or stopped during startup: `stage` is set)
    stale        the file says running/starting, but no heartbeat for STALE_AFTER_SECONDS
                 (the process was killed, hung, or the machine slept)
    running      a heartbeat within STALE_AFTER_SECONDS

The file holds no credentials or error messages, only states, times and flags.
It is replaced atomically, so a reader never sees a partial write.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

STATUS_FILE = "worker_status.json"
STATUS_VERSION = 1
HEARTBEAT_SECONDS = 15.0
STALE_AFTER_SECONDS = 60.0


def status_path(root: Path) -> Path:
    return root / "state" / STATUS_FILE


def load_status_file(root: Path) -> dict[str, Any] | None:
    """The raw status file, or None when it is missing or unreadable (e.g. mid-replace)."""
    try:
        data = json.loads(status_path(root).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) and data.get("version") == STATUS_VERSION else None


def _parse(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        return None
    return parsed if parsed is None or parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def collector_status(raw: dict[str, Any] | None, now: datetime | None = None) -> dict[str, Any]:
    """The public collector block: state plus what the worker last reported (no process ids)."""
    now = now or datetime.now(timezone.utc)
    if raw is None:
        return {"state": "not_running", "updated_at": None}
    updated = _parse(raw.get("updated_at"))
    state = raw.get("state")
    if state in ("running", "starting"):
        if updated is None or (now - updated).total_seconds() > STALE_AFTER_SECONDS:
            state = "stale"
    elif state != "stopped":
        state = "stale"  # an unknown state is never reported as healthy
    alpaca, sec = raw.get("alpaca") or {}, raw.get("sec") or {}
    stage = raw.get("stage") if isinstance(raw.get("stage"), str) else None
    return {
        "state": state,
        "stage": stage if raw.get("state") in ("starting", "stopped") else None,
        "stage_started_at": raw.get("stage_started_at") if stage else None,
        "updated_at": raw.get("updated_at"),
        "started_at": raw.get("started_at"),
        "symbols": raw.get("symbols") or [],
        "alpaca": {key: alpaca.get(key) for key in (
            "enabled", "stream_connected", "subscribed_at", "disconnected_at", "last_article_at",
            "backfill_ok_at", "backfill_error_at", "synced_through")},
        "sec": {key: sec.get(key) for key in (
            "enabled", "poll_interval_seconds", "last_poll_ok_at", "last_poll_error_at")},
        "processing": raw.get("processing") or {},
        "queue_depth": raw.get("queue_depth"),
    }


class StartupReporter:
    """The worker's preparation stages, before any collector starts: a flushed log line per stage with the
    elapsed time, and a ``starting`` status file (refreshed by a small thread every HEARTBEAT_SECONDS, so a long
    stage such as loading the embedding model is not mistaken for a dead worker). The launcher and the page read
    the stage from the file; `finish()` hands the file over to the running worker's own heartbeat."""

    def __init__(self, root: Path, symbols: list[str] | tuple[str, ...], interval: float = HEARTBEAT_SECONDS):
        self.root = Path(root)
        self.symbols = list(symbols)
        self.interval = interval
        self.started_at = datetime.now(timezone.utc)
        self._t0 = time.monotonic()
        self._stage: str | None = None
        self._stage_at = self.started_at
        self._stage_t0 = self._t0
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._thread: threading.Thread | None = None

    def elapsed(self) -> float:
        return time.monotonic() - self._t0

    def stage(self, description: str) -> None:
        now = time.monotonic()
        if self._stage is not None:
            LOGGER.info("startup: %s done in %.1f s", self._stage, now - self._stage_t0)
        with self._lock:
            self._stage, self._stage_at, self._stage_t0 = description, datetime.now(timezone.utc), now
        LOGGER.info("startup [%.1f s]: %s", now - self._t0, description)
        for handler in logging.getLogger().handlers:
            handler.flush()
        self._write("starting")
        if self._thread is None:
            self._thread = threading.Thread(target=self._heartbeat, name="startup-status", daemon=True)
            self._thread.start()

    def finish(self, message: str) -> None:
        """Preparation is over; the caller's own status heartbeat takes over the file."""
        self._stop()
        if self._stage is not None:
            LOGGER.info("startup: %s done in %.1f s", self._stage, time.monotonic() - self._stage_t0)
        LOGGER.info("startup complete in %.1f s: %s", self.elapsed(), message)
        self._stage = None

    def abort(self) -> None:
        """Stopped (interrupted or failed) before collectors started: say so instead of leaving 'starting'."""
        self._stop()
        if self._stage is not None:
            LOGGER.info("startup stopped after %.1f s during: %s", self.elapsed(), self._stage)
            self._write("stopped")
            self._stage = None

    def _stop(self) -> None:
        self._done.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _heartbeat(self) -> None:
        while not self._done.wait(self.interval):
            self._write("starting")

    def _write(self, state: str) -> None:
        from .store import atomic_write_json

        with self._lock:
            snapshot = {"version": STATUS_VERSION, "state": state, "pid": os.getpid(),
                        "started_at": self.started_at.isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat(), "heartbeat_seconds": self.interval,
                        "symbols": self.symbols, "stage": self._stage, "stage_started_at": self._stage_at.isoformat()}
            try:
                atomic_write_json(status_path(self.root), snapshot)
            except OSError as error:  # the status file must never stop the worker
                LOGGER.warning("could not write the worker status file: %s", error)
