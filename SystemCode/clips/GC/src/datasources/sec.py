"""SEC EDGAR filings source (no API key required).

Uses the official Submissions API:
    https://data.sec.gov/submissions/CIK##########.json

The submissions endpoint returns ALL of a company's filings in a single request,
so the meaningful place to avoid a duplicate request is that call. `load_submissions`
caches the response per CIK under data/cache/ and skips the network entirely when
a fresh cached copy already exists (a "duplication block before request to EDGAR").

`parse_8k` is a pure function (no network) so it can be unit-tested against a
saved submissions JSON. `recent_8k` loads (cache-or-fetch) then parses.
"""

from __future__ import annotations

import json
import time

import requests

from . import config
from .datadict import EDGAR_FIELDS, build_data_dict

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
CACHE_DIR = config.DATA_DIR / "cache"
# Cache is considered fresh for this long. SEC updates throughout the day, so a
# few hours keeps repeated exploration fast without going permanently stale.
DEFAULT_MAX_AGE_HOURS = 12


def _cache_path(cik10: str):
    return CACHE_DIR / f"submissions_CIK{cik10}.json"


def _download_submissions(cik10: str) -> dict:
    url = SUBMISSIONS_URL.format(cik=cik10)
    resp = requests.get(
        url,
        headers={
            "User-Agent": config.SEC_USER_AGENT,
            "Accept-Encoding": "identity",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def load_submissions(
    cik: str, force: bool = False, max_age_hours: float | None = DEFAULT_MAX_AGE_HOURS
) -> tuple[dict, bool]:
    """Return (submissions, from_cache).

    If a cached copy for this CIK exists and is younger than `max_age_hours`
    (and `force` is False), it is read from disk and NO request is made to EDGAR.
    Otherwise the endpoint is fetched and the result is written to the cache.
    Pass `force=True` or `max_age_hours=None`-with-no-file to always refresh.
    """
    cik10 = cik.zfill(10)
    path = _cache_path(cik10)

    if not force and path.exists():
        age_hours = (time.time() - path.stat().st_mtime) / 3600
        if max_age_hours is None or age_hours < max_age_hours:
            return json.loads(path.read_text()), True  # cache hit -> request blocked

    data = _download_submissions(cik10)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))
    return data, False


def fetch_submissions(cik: str, force: bool = False) -> dict:
    """Backwards-compatible helper: return submissions dict (cache-aware)."""
    data, _ = load_submissions(cik, force=force)
    return data


def parse_8k(
    submissions: dict, cik: str, ticker: str | None = None, limit: int = 10
) -> list[dict]:
    """Extract up to `limit` most-recent 8-K filings from a submissions dict.

    EDGAR stores filings.recent as parallel arrays already sorted newest-first.
    Each returned record keeps every native field plus derived URLs so the data
    dictionary reflects exactly what EDGAR provides.
    """
    cik10 = cik.zfill(10)
    cik_int = int(cik10)
    recent = submissions["filings"]["recent"]
    keys = list(recent.keys())
    total = len(recent.get("accessionNumber", []))

    records: list[dict] = []
    for i in range(total):
        if recent["form"][i] != "8-K":
            continue

        rec = {key: recent[key][i] for key in keys}
        accession = rec["accessionNumber"]
        acc_nodash = accession.replace("-", "")
        primary = rec.get("primaryDocument") or ""

        rec["cik"] = cik10
        rec["ticker"] = ticker
        rec["filing_index_url"] = (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{cik_int}/{acc_nodash}/{accession}-index.htm"
        )
        rec["primary_doc_url"] = (
            f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{primary}"
            if primary
            else None
        )

        records.append(rec)
        if len(records) >= limit:
            break

    return records


def recent_8k(ticker: str, limit: int = 10, refresh: bool = False) -> dict:
    """Return the most-recent 8-K filings for a ticker plus a data dictionary.

    Uses the per-CIK submissions cache (see `load_submissions`); pass
    `refresh=True` to bypass it and force a fresh EDGAR request.

    Result: {source, ticker, cik, from_cache, count, data_dict, records}.
    """
    cik = config.cik_for(ticker)
    submissions, from_cache = load_submissions(cik, force=refresh)
    records = parse_8k(submissions, cik, ticker=ticker.upper(), limit=limit)
    return {
        "source": "sec_edgar",
        "ticker": ticker.upper(),
        "cik": cik,
        "from_cache": from_cache,
        "count": len(records),
        "data_dict": build_data_dict(records, EDGAR_FIELDS),
        "records": records,
    }
