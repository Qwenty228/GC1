"""Opt-in live tests against real SEC EDGAR and Alpaca endpoints.

Skipped unless RUN_LIVE_TESTS=1. Alpaca tests additionally need credentials in
the environment or .env. They write only to pytest's temporary directory.

    $env:RUN_LIVE_TESTS = "1"; uv run pytest -m live -v
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from datasources import alpaca, sec
from datasources.ingest.graph import run_prepare
from datasources.ingest.sec_client import SecClient
from datasources.ingest.settings import load_settings, require_sec_user_agent

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.getenv("RUN_LIVE_TESTS") != "1", reason="live tests are opt-in (RUN_LIVE_TESTS=1)"),
]


def test_live_sec_submissions_and_primary_document():
    settings = load_settings()
    client = SecClient(require_sec_user_agent(settings), requests_per_second=1.0)
    data, result = client.fetch_submissions("0001045810", settings.sec_max_submissions_bytes)
    rows = sec.filing_rows(sec.filing_columns(data), forms={"8-K"})
    assert rows, "expected NVIDIA to have recent 8-K filings"
    row = rows[0]
    url = sec.primary_document_url("0001045810", row["accessionNumber"], row["primaryDocument"])
    document = client.fetch_document(url, settings.sec_max_document_bytes)
    records, errors = run_prepare([{
        "provider": "sec_edgar", "transport": "rest", "api_endpoint": result.url,
        "retrieved_at": document.retrieved_at, "requested_symbols": ["NVDA"], "is_sample": False,
        "raw_record": row, "sec_context": {"cik": data["cik"], "company_name": data["name"], "tickers": data["tickers"]},
        "primary_document_html": document.text(), "primary_document_url": url,
    }])
    assert not errors, errors
    assert records[0].body_text and records[0].original_url == url


def test_live_alpaca_rest_backfill_page():
    try:
        auth = alpaca.auth_from_env()
    except alpaca.AlpacaConfigError as error:
        pytest.skip(str(error))
    end = datetime.now(timezone.utc)
    pages = list(alpaca.iter_news_pages(auth, ["NVDA"], start=end - timedelta(days=3), end=end, max_pages=2))
    assert pages
    for payload, url in pages:
        assert "news" in payload
        assert "include_content=true" in url
