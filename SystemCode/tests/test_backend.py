"""Offline tests for the backend core (no network required)."""

from __future__ import annotations

import json
from pathlib import Path

from datasources import alpaca, config, sec
from datasources.datadict import EDGAR_FIELDS, build_data_dict
from datasources.sec import parse_8k

ROOT = Path(__file__).resolve().parents[1]
SUBMISSIONS = ROOT / "data" / "submissions_CIK0001045810.json"


def test_ticker_map_has_three_companies():
    mapping = config.load_ticker_map()
    assert set(mapping) == {"AAPL", "NVDA", "MSFT"}
    assert config.cik_for("nvda") == "0001045810"


def test_parse_8k_filters_and_derives_urls():
    submissions = json.loads(SUBMISSIONS.read_text())
    records = parse_8k(submissions, "0001045810", ticker="NVDA", limit=10)

    assert 0 < len(records) <= 10
    assert all(r["form"] == "8-K" for r in records)
    first = records[0]
    assert first["ticker"] == "NVDA"
    assert first["cik"] == "0001045810"
    assert first["filing_index_url"].startswith(
        "https://www.sec.gov/Archives/edgar/data/1045810/"
    )
    assert first["filing_index_url"].endswith("-index.htm")


def test_build_data_dict_reports_fields_types_descriptions():
    submissions = json.loads(SUBMISSIONS.read_text())
    records = parse_8k(submissions, "0001045810", ticker="NVDA", limit=5)
    rows = build_data_dict(records, EDGAR_FIELDS)

    by_field = {r["field"]: r for r in rows}
    assert by_field["form"]["example"] == "8-K"
    assert by_field["accessionNumber"]["description"].startswith("Unique filing")
    assert by_field["filing_index_url"]["type"] == "str"


def test_load_submissions_uses_cache_and_blocks_request(tmp_path, monkeypatch):
    # Point the cache at a temp dir and pre-seed it with a submissions file.
    monkeypatch.setattr(sec, "CACHE_DIR", tmp_path)
    cik = "0001045810"
    payload = json.loads(SUBMISSIONS.read_text())
    (tmp_path / f"submissions_CIK{cik}.json").write_text(json.dumps(payload))

    # Any network attempt must fail — a cache hit should never call this.
    def _boom(*a, **k):
        raise AssertionError("network request should have been blocked by cache")

    monkeypatch.setattr(sec, "_download_submissions", _boom)

    data, from_cache = sec.load_submissions(cik)
    assert from_cache is True
    assert "filings" in data

    # force=True must bypass the cache (and therefore hit the patched network).
    import pytest

    with pytest.raises(AssertionError):
        sec.load_submissions(cik, force=True)


def test_alpaca_falls_back_to_sample_without_keys(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)

    result = alpaca.recent_news("NVDA", limit=10)
    assert result["live"] is False
    assert result["count"] >= 1
    assert all("NVDA" in r["symbols"] for r in result["records"])
    assert all(r["_source"] == "sample" for r in result["records"])
    # data dict should describe the headline field with a known description
    by_field = {r["field"]: r for r in result["data_dict"]}
    assert "headline" in by_field
