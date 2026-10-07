"""Live processing activity for the page: what the ingestion worker and the startup summary catch-up are doing.

Each process that analyzes records keeps its own small file under ``<output root>/state/``:

    activity_worker.json    the ingestion worker (`datasources-ingest run`): one item per article or filing
    activity_summary.json   the startup summary catch-up: one item per event it looks at

An item (done | skipped: nothing needed doing | failed | interrupted) lists its steps in order, for example

    Looking for related stories -> Comparing with 2 stories (AI) -> Saving and grouping -> Writing the summary (AI)

each with a state (running | done | skipped | failed | interrupted), its start and end time and a short
plain-language result. The file keeps the item in progress and the last RECENT_ITEMS finished ones. It is
replaced atomically on every change and rewritten every HEARTBEAT_SECONDS while the process runs, so a reader
can tell a long model call from a process that died (STALE_AFTER_SECONDS without a write). It holds titles, ids,
model names and outcomes only: never article text, prompts, model output or credentials. Writing it never
stops the work it describes.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

LOGGER = logging.getLogger(__name__)

FILES = {"worker": "activity_worker.json", "summary": "activity_summary.json"}
VERSION = 1
HEARTBEAT_SECONDS = 10.0
STALE_AFTER_SECONDS = 45.0
RECENT_ITEMS = 25
MAX_TEXT = 300


def activity_path(root: Path, process: str) -> Path:
    return Path(root) / "state" / FILES[process]


def _clip(text: Any) -> str | None:
    if text is None:
        return None
    text = " ".join(str(text).split())
    return text if len(text) <= MAX_TEXT else text[:MAX_TEXT - 1] + "…"


class ActivityTracker:
    """Thread-safe writer of one process's activity file. Every method is a no-op without a current item."""

    def __init__(self, root: Path, process: str, *, heartbeat: float = HEARTBEAT_SECONDS,
                 clock: Callable[[], datetime] | None = None):
        self.path = activity_path(root, process)
        self.process = process
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._state = "running"
        self._started_at = self._now()
        self._current: dict[str, Any] | None = None
        self._recent: list[dict[str, Any]] = []
        self._progress: dict[str, Any] | None = None
        self._note: str | None = None
        self._done = threading.Event()
        self._write()
        self._thread: threading.Thread | None = None
        if heartbeat > 0:
            self._thread = threading.Thread(target=self._beat, args=(heartbeat,), name=f"activity-{process}",
                                            daemon=True)
            self._thread.start()

    def _now(self) -> str:
        return self._clock().isoformat()

    # ------------------------------------------------------------------ items and steps
    def start_item(self, kind: str, title: str | None, *, symbols=(), event_id: str | None = None,
                   revision_id: str | None = None) -> None:
        """Begin the next article, filing or event (an unfinished previous one is closed first)."""
        with self._lock:
            if self._current is not None:
                self._close_item(None, None)
            self._current = {"id": uuid.uuid4().hex[:12], "kind": kind, "title": _clip(title) or "(untitled)",
                             "symbols": [str(s) for s in symbols][:8], "event_id": event_id,
                             "revision_id": revision_id, "state": "running", "result": None,
                             "started_at": self._now(), "finished_at": None, "steps": []}
            self._write()

    def step(self, key: str, label: str, *, model: str | None = None) -> None:
        """Start a step of the current item; a step still running is closed as done first."""
        with self._lock:
            if self._current is None:
                return
            self._close_step("done", None)
            self._current["steps"].append({"key": key, "label": _clip(label), "model": model, "state": "running",
                                           "result": None, "started_at": self._now(), "finished_at": None})
            self._write()

    def end_step(self, state: str = "done", result: str | None = None, *, key: str | None = None) -> None:
        """Close the running step (only when it is ``key``, if given)."""
        with self._lock:
            steps = self._current["steps"] if self._current else []
            if not steps or steps[-1]["state"] != "running" or (key is not None and steps[-1]["key"] != key):
                return
            self._close_step(state, result)
            self._write()

    def running(self, key: str) -> bool:
        """Whether the current item's last step is ``key`` and still running."""
        with self._lock:
            steps = self._current["steps"] if self._current else []
            return bool(steps) and steps[-1]["key"] == key and steps[-1]["state"] == "running"

    def note(self, key: str, label: str, state: str = "skipped", result: str | None = None) -> None:
        """A step decided at once, with nothing to wait for (e.g. "Summary not needed")."""
        with self._lock:
            if self._current is None:
                return
            self._close_step("done", None)
            now = self._now()
            self._current["steps"].append({"key": key, "label": _clip(label), "model": None, "state": state,
                                           "result": _clip(result), "started_at": now, "finished_at": now})
            self._write()

    def update(self, **fields: Any) -> None:
        """Attach what becomes known later (e.g. the stored revision id) to the current item."""
        with self._lock:
            if self._current is None:
                return
            for name in ("event_id", "revision_id"):
                if fields.get(name):
                    self._current[name] = str(fields[name])
            self._write()

    def finish_item(self, state: str | None = None, result: str | None = None) -> None:
        with self._lock:
            if self._current is not None:
                self._close_item(state, result)
                self._write()

    def progress(self, done: int, total: int, label: str) -> None:
        with self._lock:
            self._progress = {"done": done, "total": total, "label": _clip(label)}
            self._write()

    def close(self, state: str = "stopped", note: str | None = None) -> None:
        """The process is done (``finished``) or stopping (``stopped``): an unfinished item is interrupted."""
        self._done.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        with self._lock:
            if self._current is not None:
                self._close_item("interrupted", None)
            self._state, self._note = state, _clip(note)
            self._write()

    # ------------------------------------------------------------------ internals (lock held)
    def _close_step(self, state: str, result: str | None) -> None:
        steps = self._current["steps"] if self._current else []
        if steps and steps[-1]["state"] == "running":
            steps[-1].update(state=state, result=_clip(result) if result else steps[-1]["result"],
                             finished_at=self._now())

    def _close_item(self, state: str | None, result: str | None) -> None:
        item = self._current
        self._close_step(state if state in ("interrupted", "failed") else "done", None)
        steps = item["steps"]
        if state is None:
            state = ("failed" if any(s["state"] == "failed" for s in steps)
                     else "skipped" if steps and all(s["state"] == "skipped" for s in steps) else "done")
        last = next((s["result"] for s in reversed(steps) if s["result"]), None)
        item.update(state=state, result=_clip(result) or last, finished_at=self._now())
        self._recent = [item, *self._recent][:RECENT_ITEMS]
        self._current = None

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps({
                "version": VERSION, "process": self.process, "pid": os.getpid(), "state": self._state,
                "started_at": self._started_at, "updated_at": self._now(), "heartbeat_seconds": HEARTBEAT_SECONDS,
                "current": self._current, "recent": self._recent, "progress": self._progress, "note": self._note,
            }))

    def _write(self) -> None:
        from .ingest.store import atomic_write_json

        with self._lock:  # one writer at a time, so an older snapshot never replaces a newer one
            try:
                atomic_write_json(self.path, self.snapshot())
            except OSError as error:
                LOGGER.warning("could not write the activity file %s: %s", self.path.name, error)

    def _beat(self, interval: float) -> None:
        while not self._done.wait(interval):
            self._write()


def _parse(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        return None
    return parsed if parsed is None or parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def load_activity(root: Path, process: str, now: datetime | None = None) -> dict[str, Any]:
    """The public view of one process's activity: ``not_running`` (no file), ``running``, ``stale`` (no write for
    STALE_AFTER_SECONDS: the process died or hung), ``stopped`` or ``finished``."""
    now = now or datetime.now(timezone.utc)
    raw = None
    for attempt in range(3):  # Windows: the writer may be replacing the file this very moment
        try:
            raw = json.loads(activity_path(root, process).read_text(encoding="utf-8"))
            break
        except FileNotFoundError:
            break
        except (OSError, ValueError):
            time.sleep(0.02 * (attempt + 1))
    if not isinstance(raw, dict) or raw.get("version") != VERSION:
        return {"process": process, "state": "not_running", "started_at": None, "updated_at": None,
                "current": None, "recent": [], "progress": None, "note": None}
    state = raw.get("state") if raw.get("state") in ("running", "stopped", "finished") else "stale"
    updated = _parse(raw.get("updated_at"))
    if state == "running" and (updated is None or (now - updated).total_seconds() > STALE_AFTER_SECONDS):
        state = "stale"
    recent = raw.get("recent") if isinstance(raw.get("recent"), list) else []
    return {"process": process, "state": state, "started_at": raw.get("started_at"), "updated_at": raw.get("updated_at"),
            "current": raw.get("current") if isinstance(raw.get("current"), dict) else None,
            "recent": recent[:RECENT_ITEMS], "progress": raw.get("progress"), "note": raw.get("note")}
