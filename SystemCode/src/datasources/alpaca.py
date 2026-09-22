"""Alpaca financial-news source (REST historical pull).

Uses the Alpaca News REST endpoint (https://data.alpaca.markets/v1beta1/news).

Authentication (``ALPACA_AUTH_MODE``):
    trading (default)  ALPACA_API_KEY / ALPACA_SECRET_KEY sent as the
                       APCA-API-KEY-ID / APCA-API-SECRET-KEY headers (the scheme
                       this project already used).
    broker             ALPACA_BROKER_CLIENT_ID / ALPACA_BROKER_CLIENT_SECRET are
                       exchanged at the documented OAuth2 client-credentials
                       endpoint for a short-lived token, cached and sent as
                       ``Authorization: Bearer``.

Missing credentials are a configuration error. Sample data is only served in
explicit demo mode (``DATASOURCES_DEMO_MODE=true`` or ``demo=True``); records are
then tagged ``_source='sample'`` and the response says ``live: false, demo: true``.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests

from . import config
from .datadict import ALPACA_FIELDS, build_data_dict

BROKER_TOKEN_URL = "https://authx.alpaca.markets/v1/oauth2/token"
MAX_PAGE_SIZE = 50  # documented maximum `limit` for /v1beta1/news
# Tokens are valid for 15 minutes; refresh a little early and never per call.
_TOKEN_REFRESH_MARGIN_SECONDS = 120


class AlpacaConfigError(ValueError):
    """Live Alpaca access was requested but credentials are not configured."""


def demo_mode_enabled() -> bool:
    return os.getenv("DATASOURCES_DEMO_MODE", "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class AlpacaAuth:
    """Produces request headers for the configured authentication mode."""

    mode: str
    api_key: str | None = None
    secret_key: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    token_url: str = BROKER_TOKEN_URL
    timeout: float = 30.0
    _token: str | None = field(default=None, repr=False)
    _token_expiry: float = field(default=0.0, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __repr__(self) -> str:  # never expose secrets in logs
        return f"AlpacaAuth(mode={self.mode!r})"

    def _broker_token(self, session: requests.Session | None = None) -> str:
        with self._lock:
            if self._token and time.monotonic() < self._token_expiry - _TOKEN_REFRESH_MARGIN_SECONDS:
                return self._token
            poster = session.post if session is not None else requests.post
            resp = poster(
                self.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=self.timeout,
            )
            if resp.status_code != 200:
                raise AlpacaConfigError(f"Alpaca broker token request failed with HTTP {resp.status_code}")
            payload = resp.json()
            self._token = payload["access_token"]
            self._token_expiry = time.monotonic() + float(payload.get("expires_in", 900))
            return self._token

    def headers(self, session: requests.Session | None = None) -> dict[str, str]:
        if self.mode == "broker":
            return {"Authorization": f"Bearer {self._broker_token(session)}"}
        return {"APCA-API-KEY-ID": self.api_key or "", "APCA-API-SECRET-KEY": self.secret_key or ""}


def auth_from_env() -> AlpacaAuth:
    """Resolve credentials for the configured mode or raise `AlpacaConfigError`."""
    mode = (os.getenv("ALPACA_AUTH_MODE") or "trading").split("#")[0].strip().lower() or "trading"
    if mode == "trading":
        key, secret = config.alpaca_creds()
        key, secret = (key or "").strip(), (secret or "").strip()
        if not (key and secret):
            raise AlpacaConfigError(
                "Alpaca credentials missing: set ALPACA_API_KEY and ALPACA_SECRET_KEY "
                "(or enable demo mode explicitly with DATASOURCES_DEMO_MODE=true)."
            )
        return AlpacaAuth(mode="trading", api_key=key, secret_key=secret)
    if mode == "broker":
        client_id = (os.getenv("ALPACA_BROKER_CLIENT_ID") or "").strip()
        client_secret = (os.getenv("ALPACA_BROKER_CLIENT_SECRET") or "").strip()
        if not (client_id and client_secret):
            raise AlpacaConfigError(
                "ALPACA_AUTH_MODE=broker requires ALPACA_BROKER_CLIENT_ID and ALPACA_BROKER_CLIENT_SECRET."
            )
        token_url = (os.getenv("ALPACA_BROKER_TOKEN_URL") or BROKER_TOKEN_URL).strip()
        return AlpacaAuth(mode="broker", client_id=client_id, client_secret=client_secret, token_url=token_url)
    raise AlpacaConfigError(f"ALPACA_AUTH_MODE must be 'trading' or 'broker', got {mode!r}")


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


def _rfc3339(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_news_page(
    auth: AlpacaAuth,
    symbols: list[str],
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = MAX_PAGE_SIZE,
    sort: str = "desc",
    include_content: bool = False,
    page_token: str | None = None,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> tuple[dict, str]:
    """Fetch one page of news. Returns (response JSON, request URL without secrets)."""
    params: dict[str, object] = {
        "symbols": ",".join(symbols),
        "limit": max(1, min(limit, MAX_PAGE_SIZE)),
        "sort": sort,
    }
    if include_content:
        params["include_content"] = "true"
    if start is not None:
        params["start"] = _rfc3339(start)
    if end is not None:
        params["end"] = _rfc3339(end)
    if page_token:
        params["page_token"] = page_token
    getter = session.get if session is not None else requests.get
    resp = getter(config.ALPACA_NEWS_URL, params=params, headers=auth.headers(session), timeout=timeout)
    resp.raise_for_status()
    request_url = requests.Request("GET", config.ALPACA_NEWS_URL, params=params).prepare().url or config.ALPACA_NEWS_URL
    return resp.json(), request_url


def iter_news_pages(
    auth: AlpacaAuth,
    symbols: list[str],
    *,
    start: datetime,
    end: datetime,
    max_pages: int = 40,
    page_size: int = MAX_PAGE_SIZE,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> Iterator[tuple[dict, str]]:
    """Yield every page for an explicit [start, end] window, following next_page_token."""
    token: str | None = None
    for _ in range(max_pages):
        payload, url = fetch_news_page(
            auth, symbols, start=start, end=end, limit=page_size, sort="asc",
            include_content=True, page_token=token, session=session, timeout=timeout,
        )
        yield payload, url
        token = payload.get("next_page_token")
        if not token:
            return


def fetch_news(symbols: list[str], limit: int = 10, demo: bool | None = None) -> tuple[list[dict], bool]:
    """Fetch the most-recent news for `symbols`.

    Returns (records, is_live). Demo mode returns the bundled synthetic sample;
    otherwise missing credentials raise `AlpacaConfigError`.
    """
    if demo if demo is not None else demo_mode_enabled():
        return _load_sample(symbols, limit), False

    auth = auth_from_env()
    payload, _ = fetch_news_page(auth, symbols, limit=limit, sort="desc")
    items = payload.get("news", [])
    for item in items:
        item["_source"] = "alpaca_live"
    return items[:limit], True


def recent_news(ticker: str, limit: int = 10, demo: bool | None = None) -> dict:
    """Return the most-recent news for a ticker plus a data dictionary.

    Result: {source, ticker, live, demo, count, data_dict, records}.
    """
    records, is_live = fetch_news([ticker], limit=limit, demo=demo)
    return {
        "source": "alpaca_news",
        "ticker": ticker.upper(),
        "live": is_live,
        "demo": not is_live,
        "count": len(records),
        "data_dict": build_data_dict(records, ALPACA_FIELDS),
        "records": records,
    }
