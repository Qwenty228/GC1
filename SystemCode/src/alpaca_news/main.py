"""Command-line entry point for the Alpaca real-time news stream."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from filelock import FileLock, Timeout

from .client import MissingCredentialsError, load_settings, run_forever

# Shared with `datasources-ingest`, which owns the same WebSocket when running.
INGEST_LOCK = Path(__file__).resolve().parents[2] / "output" / "state" / "ingest.lock"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        settings = load_settings()
    except MissingCredentialsError as error:
        logging.error("%s", error)
        return 2

    INGEST_LOCK.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(INGEST_LOCK))
    try:
        lock.acquire(timeout=0)
    except Timeout:
        logging.error("datasources-ingest is running and already owns the Alpaca news stream; not opening a second one")
        return 2
    try:
        asyncio.run(run_forever(settings))
    except KeyboardInterrupt:
        logging.info("Alpaca news stream stopped")
    finally:
        lock.release()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())