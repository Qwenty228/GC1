"""`--verbose` for evaluation runs: timestamped stage logs, written the moment each stage happens.

    enable()          one stdout handler on the evaluation loggers and llm_openai, flushed after every line
    heartbeat(label)  while a block runs, a "still running" line when nothing was logged for a while,
                      naming the last thing that happened (e.g. the Gemini request being waited on)
    LoggedEmbedder    the embedder, unchanged, with every embedding call logged (count, time)

Without `enable()` these loggers stay below their level: a normal run prints exactly what it printed before
and runs no extra query. With it, every line goes through ONE handler on loggers that do not propagate,
so each message is printed once, and the handler's lock keeps lines from the heartbeat thread and the
pipeline whole. Only names, counts, labels, statuses and timings are logged: never a key value, a header,
a prompt or article text.
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator, TextIO

LOGGERS = ("datasources.evaluation", "datasources.llm_openai")
LOGGER = logging.getLogger("datasources.evaluation.run")
HEARTBEAT = logging.getLogger("datasources.evaluation.heartbeat")
FORMAT = "%(asctime)s.%(msecs)03d %(threadName)-10.10s %(message)s"

_lock = threading.Lock()
_handler: logging.Handler | None = None
_last: "_LastLine | None" = None


class _LastLine(logging.Filter):
    """Remembers when the last (non-heartbeat) line was written and what it said, for the heartbeat."""

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self.at, self.message = time.monotonic(), "start"

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != HEARTBEAT.name:
            with self._lock:
                self.at, self.message = time.monotonic(), record.getMessage()[:110]
        return True

    def snapshot(self) -> tuple[float, str]:
        with self._lock:
            return self.at, self.message


def enabled() -> bool:
    return _handler is not None


def enable(stream: TextIO | None = None) -> None:
    """Turn the stage logs on (idempotent: a second handler would print every line twice)."""
    global _handler, _last
    with _lock:
        if _handler is not None:
            return
        handler = logging.StreamHandler(stream or sys.stdout)  # flushes after every record
        handler.setFormatter(logging.Formatter(FORMAT, "%Y-%m-%d %H:%M:%S"))
        last = _LastLine()
        handler.addFilter(last)
        for name in LOGGERS:
            logger = logging.getLogger(name)
            logger.setLevel(logging.INFO)
            logger.addHandler(handler)
            logger.propagate = False
        _handler, _last = handler, last


def disable() -> None:
    """Undo `enable()` (tests)."""
    global _handler, _last
    with _lock:
        if _handler is None:
            return
        for name in LOGGERS:
            logger = logging.getLogger(name)
            logger.removeHandler(_handler)
            logger.setLevel(logging.NOTSET)
            logger.propagate = True
        _handler, _last = None, None


@contextmanager
def heartbeat(label: str, every: float = 15.0) -> Iterator[None]:
    """While the block runs, log '<label> still running' each time nothing else was logged for `every` s."""
    last = _last
    if last is None:
        yield
        return
    stop, started = threading.Event(), time.monotonic()

    def beat() -> None:
        while not stop.wait(every):
            at, message = last.snapshot()
            idle = time.monotonic() - at
            if idle >= every * 0.95:
                HEARTBEAT.info("%s still running (%.0fs); quiet for %.0fs since: %s", label,
                               time.monotonic() - started, idle, message)

    thread = threading.Thread(target=beat, name="heartbeat", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=2)


class LoggedEmbedder:
    """The embedder, unchanged (same model, same vectors), with each `embed` call logged."""

    def __init__(self, inner: Any):
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def embed(self, texts: list[str]) -> list[list[float]]:
        started = time.monotonic()
        vectors = self._inner.embed(texts)
        LOGGER.info("    embeddings: %d text unit(s) encoded in %.2fs", len(texts), time.monotonic() - started)
        return vectors
