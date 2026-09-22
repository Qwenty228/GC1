"""Command-line entry point for the Alpaca real-time news stream."""

from __future__ import annotations

import asyncio
import logging

from .client import MissingCredentialsError, load_settings, run_forever


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        settings = load_settings()
    except MissingCredentialsError as error:
        logging.error("%s", error)
        return 2

    try:
        asyncio.run(run_forever(settings))
    except KeyboardInterrupt:
        logging.info("Alpaca news stream stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())