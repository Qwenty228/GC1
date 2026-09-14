"""Data-dictionary generation.

The whole point of this module is understanding the shape of the data so it can
later be turned into structured records. `build_data_dict` walks a list of record
dicts and, for every top-level field, reports its type, an example value, and a
human-readable description (where known).
"""

from __future__ import annotations

from typing import Any

# --- Known field descriptions -------------------------------------------------
# Documented fields for Alpaca's real-time/historical news items.
# See https://docs.alpaca.markets/reference/news-3
ALPACA_FIELDS: dict[str, str] = {
    "id": "Unique numeric ID of the news article.",
    "headline": "Article headline / title.",
    "author": "Author of the article.",
    "created_at": "Timestamp the article was created (ISO-8601, UTC).",
    "updated_at": "Timestamp the article was last updated (ISO-8601, UTC).",
    "summary": "Short text summary of the article.",
    "content": "Full article content (HTML; may be empty or truncated).",
    "url": "URL to the original article.",
    "images": "List of image objects, each with 'size' (large/small/thumb) and 'url'.",
    "symbols": "List of stock ticker symbols the article references.",
    "source": "News provider/source (e.g. 'benzinga').",
    "_source": "Injected by this module: 'alpaca_live' or 'sample' (data provenance).",
}

# Documented fields from EDGAR submissions filings.recent parallel arrays.
# See https://www.sec.gov/search-filings/edgar-application-programming-interfaces
EDGAR_FIELDS: dict[str, str] = {
    "accessionNumber": "Unique filing accession number (e.g. 0001045810-26-000073).",
    "filingDate": "Date the filing was submitted to SEC (YYYY-MM-DD).",
    "reportDate": "Date of the period or event being reported (YYYY-MM-DD).",
    "acceptanceDateTime": "Timestamp SEC accepted the filing (ISO-8601).",
    "act": "Securities act the filing is made under (e.g. '34').",
    "form": "Filing form type (filtered to '8-K' here).",
    "fileNumber": "SEC file number assigned to the filer.",
    "filmNumber": "Film / document control number.",
    "items": "Comma-separated 8-K item numbers disclosed (e.g. '2.02,9.01').",
    "core_type": "Internal EDGAR document core type.",
    "size": "Size of the filing submission in bytes.",
    "isXBRL": "1 if the filing contains XBRL structured data, else 0.",
    "isInlineXBRL": "1 if the filing uses Inline XBRL, else 0.",
    "primaryDocument": "Filename of the primary document within the filing.",
    "primaryDocDescription": "Description of the primary document.",
    # Derived fields added by sec.py:
    "cik": "Zero-padded 10-digit Central Index Key of the company (derived).",
    "ticker": "Ticker symbol requested (derived).",
    "filing_index_url": "URL to the human-readable filing index page (derived).",
    "primary_doc_url": "Direct URL to the primary filing document (derived).",
}

_MAX_EXAMPLE_LEN = 160


def _type_name(value: Any) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, list):
        elem = value[0] if value else None
        return f"array<{_type_name(elem)}>"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _example(value: Any) -> Any:
    """Return a compact, display-friendly example value."""
    if isinstance(value, str) and len(value) > _MAX_EXAMPLE_LEN:
        return value[:_MAX_EXAMPLE_LEN] + "…"
    if isinstance(value, (list, dict)):
        # Show a shallow preview rather than a huge nested blob.
        if isinstance(value, list):
            return value[:2]
        return {k: value[k] for k in list(value)[:3]}
    return value


def build_data_dict(
    records: list[dict], descriptions: dict[str, str] | None = None
) -> list[dict]:
    """Describe every top-level field seen across `records`.

    Returns a list of rows: {field, type, example, description}. Field order
    follows first appearance. The example is taken from the first record where
    the field is non-null, so empty leading values do not hide the real type.
    """
    descriptions = descriptions or {}
    order: list[str] = []
    example_for: dict[str, Any] = {}

    for rec in records:
        for key, val in rec.items():
            if key not in example_for:
                example_for[key] = None
                order.append(key)
            if example_for[key] is None and val is not None:
                example_for[key] = val

    return [
        {
            "field": key,
            "type": _type_name(example_for[key]),
            "example": _example(example_for[key]),
            "description": descriptions.get(key, ""),
        }
        for key in order
    ]
