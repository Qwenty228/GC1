"""Alpaca financial-news source (REST historical pull).

Uses the Alpaca News REST endpoint (https://data.alpaca.markets/v1beta1/news).
Requires ALPACA_API_KEY / ALPACA_SECRET_KEY. When credentials are absent, this
falls back to a bundled sample response so the data-exploration frontend still
works. Each record is tagged with `_source` ('alpaca_live' or 'sample') so the
provenance is always visible.
"""

from __future__ import annotations

import json

import requests

from . import config
from .datadict import ALPACA_FIELDS, build_data_dict


def _load_sample(symbols: list[str], limit: int) -> list[dict]:
    data = json.loads(config.SAMPLE_ALPACA_FILE.read_text())
    wanted = {s.upper() for s in symbols}
    items = [
        item
        for item in data.get("news", [])
        if wanted & {s.upper() for s in item.get("symbols", [])}
    ]
    for item in items:
        item["_source"] = "sample"
    return items[:limit]


def fetch_news(symbols: list[str], limit: int = 10) -> tuple[list[dict], bool]:
    """Fetch the most-recent news for `symbols`.

    Returns (records, is_live). Falls back to the bundled sample (is_live=False)
    when credentials are missing.
    """
    key, secret = config.alpaca_creds()
    if not (key and secret):
        return _load_sample(symbols, limit), False

    resp = requests.get(
        config.ALPACA_NEWS_URL,
        params={"symbols": ",".join(symbols), "limit": limit, "sort": "desc"},
        headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret},
        timeout=30,
    )
    resp.raise_for_status()
    items = resp.json().get("news", [])
    for item in items:
        item["_source"] = "alpaca_live"
    return items[:limit], True


def recent_news(ticker: str, limit: int = 10) -> dict:
    """Return the most-recent news for a ticker plus a data dictionary.

    Result: {source, ticker, live, count, data_dict, records}.
    """
    records, is_live = fetch_news([ticker], limit=limit)
    return {
        "source": "alpaca_news",
        "ticker": ticker.upper(),
        "live": is_live,
        "count": len(records),
        "data_dict": build_data_dict(records, ALPACA_FIELDS),
        "records": records,
    }
