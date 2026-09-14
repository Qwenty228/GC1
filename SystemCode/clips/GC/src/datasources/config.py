"""Shared configuration: paths, credentials, and the ticker->CIK mapping.

This module has no third-party dependency beyond python-dotenv and is safe to
import from the pure backend core (sec.py / alpaca.py) as well as the web layer.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv

# Project root = two levels up from src/datasources/config.py
ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
WEB_DIR = ROOT / "web"
TICKER_MAP_FILE = DATA_DIR / "ticker_cik_map.json"
SAMPLE_ALPACA_FILE = DATA_DIR / "alpaca_news_sample.json"

# Load .env from the project root if present (does not override real env vars).
load_dotenv(ROOT / ".env")

# SEC requires a descriptive User-Agent per its fair-access policy.
SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT", "StudentResearchProject nathan.kittichai@gmail.com"
)
ALPACA_NEWS_URL = "https://data.alpaca.markets/v1beta1/news"


def alpaca_creds() -> tuple[str | None, str | None]:
    """Return (api_key, secret_key) from the environment; either may be None."""
    return os.getenv("ALPACA_API_KEY"), os.getenv("ALPACA_SECRET_KEY")


def load_ticker_map() -> dict:
    """Return the ticker -> {cik, name} mapping loaded from disk."""
    return json.loads(TICKER_MAP_FILE.read_text())


def cik_for(ticker: str) -> str:
    """Return the zero-padded 10-digit CIK for a ticker symbol.

    Raises KeyError if the ticker is not in the mapping.
    """
    mapping = load_ticker_map()
    key = ticker.upper()
    if key not in mapping:
        raise KeyError(
            f"Unknown ticker {ticker!r}. Known tickers: {sorted(mapping)}"
        )
    return str(mapping[key]["cik"]).zfill(10)
