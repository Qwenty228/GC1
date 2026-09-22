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
import logging
import os
import re
import tempfile
import time
from pathlib import Path

import requests

from . import config
from .datadict import EDGAR_FIELDS, build_data_dict

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
CACHE_DIR = config.DATA_DIR / "cache"
# Cache is considered fresh for this long. SEC updates throughout the day, so a
# few hours keeps repeated exploration fast without going permanently stale.
DEFAULT_MAX_AGE_HOURS = 12
ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"
LOGGER = logging.getLogger(__name__)

# filings.recent columns that must be present and aligned with accessionNumber.
REQUIRED_FILING_FIELDS = ("accessionNumber", "form", "filingDate", "primaryDocument")
_ACCESSION = re.compile(r"^\d{10}-\d{2}-\d{6}$")
# EDGAR primary documents are plain filenames, occasionally one sub-directory deep.
_PRIMARY_DOCUMENT = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]*(/[A-Za-z0-9_][A-Za-z0-9._-]*)?$")


class SubmissionsFormatError(ValueError):
    """Raised when submissions JSON violates the aligned-array contract."""


def _cache_path(cik10: str):
    return CACHE_DIR / f"submissions_CIK{cik10}.json"


def save_submissions_cache(cik: str, data: dict) -> Path:
    """Atomically replace the shared per-CIK cache used by `load_submissions`."""
    path = _cache_path(cik.zfill(10))
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return path


def archive_directory_url(cik: str, accession: str) -> str:
    """Archive folder: CIK without leading zeros, accession without hyphens."""
    return f"{ARCHIVES_BASE}/{int(cik)}/{accession.replace('-', '')}"


def filing_index_url(cik: str, accession: str) -> str:
    """Human-readable filing index; the filename keeps the hyphenated accession."""
    return f"{archive_directory_url(cik, accession)}/{accession}-index.htm"


def primary_document_url(cik: str, accession: str, primary_document: str) -> str:
    """Primary document URL. Raises ValueError for an unsafe/invalid filename."""
    if not valid_primary_document(primary_document):
        raise ValueError(f"invalid primary document filename {primary_document!r}")
    return f"{archive_directory_url(cik, accession)}/{primary_document}"


def valid_accession(accession: str | None) -> bool:
    return bool(accession) and bool(_ACCESSION.match(accession))


def valid_primary_document(name: str | None) -> bool:
    return bool(name) and ".." not in name and bool(_PRIMARY_DOCUMENT.match(name))


def filing_columns(submissions_or_file: dict) -> dict:
    """Return the columnar filings arrays.

    Main submissions JSON nests them under ``filings.recent``; the historical
    ``CIK##########-submissions-###.json`` files listed in ``filings.files``
    carry the same arrays at the top level.
    """
    filings = submissions_or_file.get("filings")
    if isinstance(filings, dict) and isinstance(filings.get("recent"), dict):
        return filings["recent"]
    if isinstance(submissions_or_file.get("accessionNumber"), list):
        return submissions_or_file
    raise SubmissionsFormatError("no filings.recent arrays found")


def filing_rows(
    columns: dict, forms: set[str] | None = None, warnings: list[str] | None = None
) -> list[dict]:
    """Turn aligned filings arrays into row dicts, newest-first as EDGAR orders them.

    Required columns must exist and share accessionNumber's length; otherwise the
    whole payload is rejected rather than silently truncated (no `zip`). An
    optional column that is missing or misaligned becomes null for every row and
    a warning is recorded.
    """
    for name in REQUIRED_FILING_FIELDS:
        if not isinstance(columns.get(name), list):
            raise SubmissionsFormatError(f"required filings column {name!r} is missing")
    total = len(columns["accessionNumber"])
    for name in REQUIRED_FILING_FIELDS:
        if len(columns[name]) != total:
            raise SubmissionsFormatError(
                f"filings column {name!r} has {len(columns[name])} entries, "
                f"accessionNumber has {total}"
            )
    usable: list[str] = []
    for name, values in columns.items():
        if name in REQUIRED_FILING_FIELDS:
            usable.append(name)
        elif not isinstance(values, list) or len(values) != total:
            message = (
                f"optional filings column {name!r} is misaligned "
                f"({len(values) if isinstance(values, list) else type(values).__name__} vs {total}); set to null"
            )
            LOGGER.warning(message)
            if warnings is not None:
                warnings.append(message)
        else:
            usable.append(name)
    all_names = list(columns.keys())
    rows: list[dict] = []
    for i in range(total):
        form = columns["form"][i]
        if forms is not None and form not in forms:
            continue
        rows.append({name: (columns[name][i] if name in usable else None) for name in all_names})
    return rows


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
    save_submissions_cache(cik10, data)
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
    records: list[dict] = []
    for rec in filing_rows(filing_columns(submissions), forms={"8-K"}):
        accession = rec["accessionNumber"]
        primary = rec.get("primaryDocument") or ""

        rec["cik"] = cik10
        rec["ticker"] = ticker
        rec["filing_index_url"] = filing_index_url(cik10, accession)
        rec["primary_doc_url"] = (
            primary_document_url(cik10, accession, primary)
            if valid_primary_document(primary)
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
