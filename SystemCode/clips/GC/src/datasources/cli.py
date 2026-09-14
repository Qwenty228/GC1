"""Command-line entry point for the Data Sources backend.

This exercises the backend core WITHOUT the web layer, proving the module is
usable on its own. Examples:

    uv run datasources edgar NVDA --limit 10
    uv run datasources alpaca AAPL --limit 5
    uv run datasources companies
"""

from __future__ import annotations

import argparse
import json
import sys

from . import sec
from . import alpaca
from . import config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="datasources",
        description="Node 1 data sources: Alpaca news + SEC EDGAR 8-K filings.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("companies", help="Print the ticker -> CIK mapping.")

    p_edgar = sub.add_parser("edgar", help="Fetch most-recent 8-K filings.")
    p_edgar.add_argument("ticker")
    p_edgar.add_argument("--limit", type=int, default=10)
    p_edgar.add_argument(
        "--refresh",
        action="store_true",
        help="Bypass the cache and force a fresh EDGAR request.",
    )

    p_alpaca = sub.add_parser("alpaca", help="Fetch most-recent Alpaca news.")
    p_alpaca.add_argument("ticker")
    p_alpaca.add_argument("--limit", type=int, default=10)

    args = parser.parse_args(argv)

    if args.command == "companies":
        result: object = config.load_ticker_map()
    elif args.command == "edgar":
        result = sec.recent_8k(args.ticker, limit=args.limit, refresh=args.refresh)
    elif args.command == "alpaca":
        result = alpaca.recent_news(args.ticker, limit=args.limit)
    else:  # pragma: no cover - argparse enforces choices
        parser.error(f"unknown command {args.command!r}")
        return 2

    json.dump(result, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
