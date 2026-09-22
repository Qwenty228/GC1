"""Provider adapters (envelope -> PreparedRecord) and payload unwrapping.

Adapters are pure and deterministic; the graph selects one from the envelope's
explicit ``provider``. Unwrapping turns the shapes that exist in this repository
into envelopes:

    SEC     raw submissions JSON (``filings.recent`` aligned arrays), historical
            submissions files, and the ``/api/edgar`` wrapper
            ``{"source": "sec_edgar", "records": [...]}``
    Alpaca  REST ``{"news": [...], "next_page_token": ...}``, the ``/api/alpaca``
            wrapper ``{"source": "alpaca_news", "records": [...]}``, WebSocket
            batches ``[{"T": "n", ...}, {"T": "success", ...}]``

Data-dictionary files (lists of ``{"field", "type", "example"}``) describe an API
and are ignored as document content.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from .. import sec
from .htmltext import clean_inline, html_to_text, looks_like_error_page, normalize_whitespace, sec_archive_links
from .schema import (
    DedupInfo,
    ExhibitLink,
    ExtractionInfo,
    PreparedRecord,
    Provenance,
    SecContext,
    SecMetadata,
    SourceEnvelope,
)

# Official 8-K item captions (Form 8-K General Instructions), used for labelled
# metadata only. Unknown item numbers are kept without a caption.
SEC_8K_ITEMS = {
    "1.01": "Entry into a Material Definitive Agreement",
    "1.02": "Termination of a Material Definitive Agreement",
    "1.03": "Bankruptcy or Receivership",
    "1.04": "Mine Safety - Reporting of Shutdowns and Patterns of Violations",
    "1.05": "Material Cybersecurity Incidents",
    "2.01": "Completion of Acquisition or Disposition of Assets",
    "2.02": "Results of Operations and Financial Condition",
    "2.03": "Creation of a Direct Financial Obligation or an Obligation under an Off-Balance Sheet Arrangement of a Registrant",
    "2.04": "Triggering Events That Accelerate or Increase a Direct Financial Obligation or an Obligation under an Off-Balance Sheet Arrangement",
    "2.05": "Costs Associated with Exit or Disposal Activities",
    "2.06": "Material Impairments",
    "3.01": "Notice of Delisting or Failure to Satisfy a Continued Listing Rule or Standard; Transfer of Listing",
    "3.02": "Unregistered Sales of Equity Securities",
    "3.03": "Material Modification to Rights of Security Holders",
    "4.01": "Changes in Registrant's Certifying Accountant",
    "4.02": "Non-Reliance on Previously Issued Financial Statements or a Related Audit Report or Completed Interim Review",
    "5.01": "Changes in Control of Registrant",
    "5.02": "Departure of Directors or Certain Officers; Election of Directors; Appointment of Certain Officers; Compensatory Arrangements of Certain Officers",
    "5.03": "Amendments to Articles of Incorporation or Bylaws; Change in Fiscal Year",
    "5.04": "Temporary Suspension of Trading Under Registrant's Employee Benefit Plans",
    "5.05": "Amendment to Registrant's Code of Ethics, or Waiver of a Provision of the Code of Ethics",
    "5.06": "Change in Shell Company Status",
    "5.07": "Submission of Matters to a Vote of Security Holders",
    "5.08": "Shareholder Director Nominations",
    "6.01": "ABS Informational and Computational Material",
    "6.02": "Change of Servicer or Trustee",
    "6.03": "Change in Credit Enhancement or Other External Support",
    "6.04": "Failure to Make a Required Distribution",
    "6.05": "Securities Act Updating Disclosure",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events",
    "9.01": "Financial Statements and Exhibits",
}

MIN_EXTRACTED_CHARS = 200  # a real 8-K cover page alone is far longer
_SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9.\-/:^=]*$|^\*$")


class AdapterError(ValueError):
    """A single record could not be prepared. Carries a retryability hint."""

    def __init__(self, message: str, *, stage: str, retryable: bool, source_record_id: str | None = None):
        super().__init__(message)
        self.stage = stage
        self.retryable = retryable
        self.source_record_id = source_record_id


# --------------------------------------------------------------------------- helpers
def parse_timestamp(value: Any, field_name: str) -> tuple[datetime | None, str | None]:
    """Parse RFC 3339 / ISO 8601 with an explicit offset into UTC. Never guesses."""
    if value is None or value == "":
        return None, None
    if not isinstance(value, str):
        return None, f"{field_name}: non-string timestamp {value!r} ignored"
    text = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return None, f"{field_name}: date-only value {text!r} not converted to a timestamp"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        return None, f"{field_name}: invalid timestamp {text!r}"
    if parsed.tzinfo is None:
        return None, f"{field_name}: timestamp {text!r} has no timezone; not guessed"
    return parsed.astimezone(timezone.utc), None


def normalize_symbols(values: Any) -> tuple[list[str], list[str]]:
    """Upper-case, trim and de-duplicate; keep ticker punctuation (BRK.B, BTC/USD)."""
    warnings: list[str] = []
    if values is None:
        return [], warnings
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        return [], [f"symbols: unexpected type {type(values).__name__}"]
    out: list[str] = []
    for raw in values:
        if not isinstance(raw, str):
            warnings.append(f"symbols: non-string entry {raw!r} dropped")
            continue
        symbol = raw.strip().upper()
        if not symbol:
            continue
        if not _SYMBOL.match(symbol):
            warnings.append(f"symbols: invalid symbol {raw!r} dropped")
            continue
        if symbol not in out:
            out.append(symbol)
    return out, warnings


def _sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


ALPACA_FINGERPRINT_FIELDS = (
    "headline", "summary", "content", "author", "created_at", "updated_at", "url", "symbols", "source",
)


def alpaca_fingerprint(article: dict) -> str:
    """Content fingerprint of one article payload, used ONLY within the same article id."""
    return _sha256({name: article.get(name) for name in ALPACA_FINGERPRINT_FIELDS})


def alpaca_content_level(article: dict) -> int:
    """2 = full content, 1 = summary only, 0 = headline only / nothing."""
    if clean_inline(article.get("content")):
        return 2
    if clean_inline(article.get("summary")):
        return 1
    return 0


# --------------------------------------------------------------------------- SEC
class SecFilingRow(BaseModel):
    """One filing row taken from aligned ``filings.recent`` arrays."""

    model_config = ConfigDict(extra="allow")

    accessionNumber: str
    form: str
    filingDate: str | None = None
    reportDate: str | None = None
    acceptanceDateTime: str | None = None
    items: str | None = None
    primaryDocument: str
    primaryDocDescription: str | None = None
    fileNumber: str | None = None
    filmNumber: str | None = None
    act: str | None = None
    size: int | None = None
    isXBRL: int | None = None
    isInlineXBRL: int | None = None

    @field_validator("accessionNumber")
    @classmethod
    def _accession(cls, value: str) -> str:
        if not sec.valid_accession(value):
            raise ValueError(f"invalid accession number {value!r}")
        return value

    @field_validator("primaryDocument")
    @classmethod
    def _primary(cls, value: str) -> str:
        if not sec.valid_primary_document(value):
            raise ValueError(f"invalid primary document filename {value!r}")
        return value


def _blank_to_none(value: Any) -> Any:
    return None if value == "" else value


def prepare_sec(env: SourceEnvelope) -> PreparedRecord:
    raw = env.raw_record
    source_id = raw.get("accessionNumber") if isinstance(raw.get("accessionNumber"), str) else None
    try:
        row = SecFilingRow.model_validate({k: _blank_to_none(v) for k, v in raw.items()})
    except ValueError as error:
        raise AdapterError(f"invalid SEC filing row: {error}", stage="validate", retryable=False,
                           source_record_id=source_id) from error
    if env.sec_context is None:
        raise AdapterError("SEC envelope requires sec_context (CIK / company)", stage="validate",
                           retryable=False, source_record_id=row.accessionNumber)
    ctx = env.sec_context
    warnings: list[str] = []

    index_url = sec.filing_index_url(ctx.cik, row.accessionNumber)
    primary_url = sec.primary_document_url(ctx.cik, row.accessionNumber, row.primaryDocument)
    if env.primary_document_url and env.primary_document_url != primary_url:
        raise AdapterError(
            f"primary document URL {env.primary_document_url!r} does not match expected {primary_url!r}",
            stage="validate", retryable=False, source_record_id=row.accessionNumber,
        )
    if not env.primary_document_html:
        raise AdapterError("primary document HTML was not downloaded", stage="download",
                           retryable=True, source_record_id=row.accessionNumber)

    extracted = html_to_text(env.primary_document_html, base_url=primary_url, sec_document=True)
    visible = len(re.sub(r"\s", "", extracted.text))
    if looks_like_error_page(extracted.text):
        raise AdapterError("downloaded document is an SEC error page", stage="extract",
                           retryable=True, source_record_id=row.accessionNumber)
    if visible < MIN_EXTRACTED_CHARS:
        raise AdapterError(f"extraction produced only {visible} visible characters", stage="extract",
                           retryable=True, source_record_id=row.accessionNumber)
    warnings.extend(extracted.warnings)

    published_at, warning = parse_timestamp(row.acceptanceDateTime, "acceptanceDateTime")
    if warning:
        warnings.append(warning)
    if published_at is None:
        warnings.append("published_at unavailable: no valid acceptanceDateTime (filing/report dates kept separately)")

    symbols, symbol_warnings = normalize_symbols(ctx.tickers)
    warnings.extend(symbol_warnings)

    items = [item.strip() for item in (row.items or "").split(",") if item.strip()]
    descriptions = {item: SEC_8K_ITEMS[item] for item in items if item in SEC_8K_ITEMS}
    exhibits = sec_archive_links(extracted.links, primary_url)
    if exhibits:
        warnings.append(
            f"{len(exhibits)} exhibit link(s) preserved but not extracted (content_scope=primary_document_only)"
        )
    issuer = ctx.company_name
    form = row.form
    is_amendment = form.endswith("/A")

    items_label = ", ".join(f"{i} {descriptions[i]}" if i in descriptions else i for i in items) or "not listed"
    title = f"{issuer or 'CIK ' + ctx.cik} Form {form} filed {row.filingDate or 'unknown date'}"
    if items:
        title += f" (Items {', '.join(items)})"

    header_lines = [
        "[SEC EDGAR filing metadata]",
        f"Issuer: {issuer or 'unknown'} (CIK {ctx.cik})",
        f"Form: {form}{' (amendment)' if is_amendment else ''}",
        f"Accession number: {row.accessionNumber}",
        f"Filing date: {row.filingDate or 'not provided'}",
        f"Report date: {row.reportDate or 'not provided'}",
        f"Accepted: {published_at.isoformat() if published_at else 'not provided'}",
        f"Items: {items_label}",
        f"Primary document: {row.primaryDocument}",
        f"Source URL: {primary_url}",
        "Content scope: primary document only; exhibits are linked, not extracted",
        "",
        "[Primary document text]",
    ]
    analysis_text = "\n".join(header_lines) + "\n" + extracted.text

    extraction = ExtractionInfo(
        status="ok",
        parser="beautifulsoup4+lxml",
        char_count=len(extracted.text),
        table_count=extracted.table_count,
        inline_xbrl_fact_count=extracted.inline_xbrl_fact_count,
        section_headings=extracted.section_headings,
        warnings=extracted.warnings,
    )
    identity = f"sec_edgar:{ctx.cik}:{row.accessionNumber}"
    metadata = SecMetadata(
        cik=ctx.cik,
        accession_number=row.accessionNumber,
        form=form,
        is_amendment=is_amendment,
        filing_date=row.filingDate,
        report_date=row.reportDate,
        acceptance_datetime_raw=row.acceptanceDateTime,
        items=items,
        item_descriptions=descriptions,
        primary_document=row.primaryDocument,
        primary_document_description=row.primaryDocDescription,
        file_number=row.fileNumber,
        film_number=row.filmNumber,
        act=row.act,
        size=row.size,
        is_xbrl=None if row.isXBRL is None else bool(row.isXBRL),
        is_inline_xbrl=None if row.isInlineXBRL is None else bool(row.isInlineXBRL),
        exhibit_links=[ExhibitLink(url=url, text=text or None) for url, text in exhibits],
        exhibits_extracted=False,
        extraction=extraction,
    )
    return PreparedRecord(
        record_id=identity,
        record_type="sec_filing",
        provider="sec_edgar",
        source_record_id=row.accessionNumber,
        publisher=None,
        issuer_name=issuer,
        author=None,
        symbols=symbols,
        title=title,
        title_is_generated=True,
        summary_text=None,
        body_text=extracted.text,
        analysis_text=analysis_text,
        content_status="full_text",
        content_scope="primary_document_only",
        published_at=published_at,
        updated_at=None,
        retrieved_at=env.retrieved_at.astimezone(timezone.utc),
        original_url=primary_url,
        filing_index_url=index_url,
        primary_document_url=primary_url,
        exhibit_urls=[url for url, _ in exhibits],
        is_sample=env.is_sample,
        provenance=Provenance(
            provider=env.provider,
            transport=env.transport,
            api_endpoint=env.api_endpoint,
            retrieved_at=env.retrieved_at.astimezone(timezone.utc),
            requested_symbols=env.requested_symbols,
            adapter="sec_edgar.submissions_8k",
            source_record_keys=sorted(raw.keys()),
            dedup=DedupInfo(identity=identity, content_fingerprint=_sha256(extracted.text), content_level=2),
        ),
        raw_file_refs=env.raw_file_refs,
        warnings=warnings,
        sec_metadata=metadata,
    )


# --------------------------------------------------------------------------- Alpaca
class AlpacaArticle(BaseModel):
    """Documented Alpaca news fields (REST and WebSocket ``T="n"``)."""

    model_config = ConfigDict(extra="allow")

    id: int
    headline: str | None = None
    summary: str | None = None
    content: str | None = None
    author: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    url: str | None = None
    symbols: list[Any] | None = None
    source: str | None = None


def alpaca_revision_key(updated_at: datetime | None, fingerprint: str) -> str:
    stamp = updated_at.strftime("%Y%m%dT%H%M%SZ") if updated_at else "no-updated-at"
    return f"{stamp}_{fingerprint[:12]}"


def prepare_alpaca(env: SourceEnvelope) -> PreparedRecord:
    raw = env.raw_record
    source_id = str(raw.get("id")) if raw.get("id") is not None else None
    message_type = raw.get("T")
    if message_type is not None and message_type != "n":
        raise AdapterError(f"not a news message (T={message_type!r})", stage="validate",
                           retryable=False, source_record_id=source_id)
    try:
        article = AlpacaArticle.model_validate(raw)
    except ValueError as error:
        raise AdapterError(f"invalid Alpaca article: {error}", stage="validate", retryable=False,
                           source_record_id=source_id) from error
    warnings: list[str] = []
    published_at, warning = parse_timestamp(article.created_at, "created_at")
    if warning:
        warnings.append(warning)
    updated_at, warning = parse_timestamp(article.updated_at, "updated_at")
    if warning:
        warnings.append(warning)
    symbols, symbol_warnings = normalize_symbols(article.symbols)
    warnings.extend(symbol_warnings)

    headline = clean_inline(article.headline)
    summary = clean_inline(article.summary)
    body = None
    if article.content and article.content.strip():
        body = html_to_text(article.content).text or None
    if body is None:
        warnings.append("content not provided by Alpaca for this delivery")
    if headline is None:
        warnings.append("headline missing")

    if body:
        status = "full_text"
    elif summary:
        status = "summary_only"
    elif headline:
        status = "headline_only"
    else:
        status = "empty"

    parts: list[str] = []
    if headline:
        parts.append(headline)
    body_norm = " ".join((body or "").split())
    if summary and summary not in body_norm and summary != headline:
        parts.append(summary)
    if body:
        text = body
        if headline and body_norm.startswith(headline):
            text = normalize_whitespace(body[len(headline):] if body.startswith(headline) else body)
        if text:
            parts.append(text)
    analysis_text = "\n\n".join(parts)

    fingerprint = alpaca_fingerprint(raw)
    identity = f"alpaca:{article.id}"
    return PreparedRecord(
        record_id=f"{identity}:{alpaca_revision_key(updated_at, fingerprint)}",
        record_type="news_article",
        provider="alpaca",
        source_record_id=str(article.id),
        publisher=clean_inline(article.source),
        issuer_name=None,
        author=clean_inline(article.author),
        symbols=symbols,
        title=headline,
        title_is_generated=False,
        summary_text=summary,
        body_text=body,
        analysis_text=analysis_text,
        content_status=status,
        content_scope="provider_article_payload",
        published_at=published_at,
        updated_at=updated_at,
        retrieved_at=env.retrieved_at.astimezone(timezone.utc),
        original_url=article.url or None,
        filing_index_url=None,
        primary_document_url=None,
        exhibit_urls=[],
        is_sample=env.is_sample,
        provenance=Provenance(
            provider=env.provider,
            transport=env.transport,
            api_endpoint=env.api_endpoint,
            retrieved_at=env.retrieved_at.astimezone(timezone.utc),
            requested_symbols=env.requested_symbols,
            adapter=f"alpaca.news_{env.transport}",
            source_record_keys=sorted(raw.keys()),
            dedup=DedupInfo(
                identity=identity,
                revision_updated_at=updated_at,
                content_fingerprint=fingerprint,
                content_level=alpaca_content_level(raw),
            ),
        ),
        raw_file_refs=env.raw_file_refs,
        warnings=warnings,
        sec_metadata=None,
    )


ADAPTERS: dict[str, Callable[[SourceEnvelope], PreparedRecord]] = {
    "sec_edgar": prepare_sec,
    "alpaca": prepare_alpaca,
}


# --------------------------------------------------------------------------- unwrapping
def is_data_dictionary(payload: Any) -> bool:
    return (
        isinstance(payload, list)
        and bool(payload)
        and all(isinstance(row, dict) and "field" in row and "type" in row for row in payload)
    )


def sec_context_from_submissions(submissions: dict, cik: str | None = None) -> SecContext:
    return SecContext(
        cik=str(submissions.get("cik") or cik or ""),
        company_name=submissions.get("name"),
        tickers=list(submissions.get("tickers") or []),
    )


HtmlLookup = Callable[[str, str], tuple[str, str] | None]


def unwrap_payload(
    payload: Any,
    *,
    provider: str,
    sec_forms: set[str] | None = None,
    cik: str | None = None,
) -> tuple[list[tuple[dict, SecContext | None]], dict[str, Any]]:
    """Return ([(raw_record, sec_context)], info) for a payload of a known shape.

    ``info`` reports the detected ``shape`` and, for Alpaca wrappers, ``live``.
    """
    if is_data_dictionary(payload):
        return [], {"shape": "data_dictionary", "ignored": True}
    if provider == "sec_edgar":
        if isinstance(payload, dict) and ("filings" in payload or "accessionNumber" in payload):
            warnings: list[str] = []
            rows = sec.filing_rows(sec.filing_columns(payload), forms=sec_forms, warnings=warnings)
            if "filings" in payload:
                ctx = sec_context_from_submissions(payload, cik)
            elif cik:
                ctx = SecContext(cik=cik)
            else:
                raise ValueError("historical submissions file needs an explicit CIK")
            return [(row, ctx) for row in rows], {"shape": "sec_submissions", "warnings": warnings}
        if isinstance(payload, dict) and payload.get("source") == "sec_edgar" and isinstance(payload.get("records"), list):
            out = []
            for rec in payload["records"]:
                ctx = SecContext(cik=str(rec.get("cik") or payload.get("cik")),
                                 tickers=[rec["ticker"]] if rec.get("ticker") else [])
                row = {k: v for k, v in rec.items() if k not in {"cik", "ticker", "filing_index_url", "primary_doc_url"}}
                if sec_forms is None or row.get("form") in sec_forms:
                    out.append((row, ctx))
            return out, {"shape": "api_edgar_wrapper"}
        raise ValueError("payload is not a recognised SEC shape (submissions JSON or /api/edgar wrapper)")
    if provider == "alpaca":
        if isinstance(payload, dict) and isinstance(payload.get("news"), list):
            return [(a, None) for a in payload["news"]], {"shape": "alpaca_rest"}
        if isinstance(payload, dict) and payload.get("source") == "alpaca_news" and isinstance(payload.get("records"), list):
            return [(a, None) for a in payload["records"]], {"shape": "api_alpaca_wrapper", "live": payload.get("live")}
        if isinstance(payload, list):
            if any(isinstance(m, dict) and "T" in m for m in payload):
                news = [m for m in payload if isinstance(m, dict) and m.get("T") == "n"]
                return [(m, None) for m in news], {"shape": "alpaca_websocket_batch",
                                                   "operational": len(payload) - len(news)}
            return [(m, None) for m in payload if isinstance(m, dict)], {"shape": "alpaca_article_list"}
        raise ValueError("payload is not a recognised Alpaca shape")
    raise ValueError(f"unknown provider {provider!r}")
