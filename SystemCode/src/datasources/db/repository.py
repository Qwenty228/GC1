"""Persistence layer: one `PreparedRecord` -> the existing `stock_news` tables.

Both Alpaca and SEC ingestion call `persist_record`, so the mapping, the
idempotency rules and the transaction boundary live in exactly one place.

Table mapping
-------------
    provider                  looked up by `code` ('sec_edgar' | 'alpaca'), never created
    document                  one row per SOURCE RECORD; identity is
                              (provider_id, source_namespace, source_record_id, is_sample)
                              SEC     source_namespace = 10-digit CIK, source_record_id = accession
                              Alpaca  source_namespace = 'news',       source_record_id = article id
    document_revision         one row per distinct payload of that record; identity is
                              (document_id, source_snapshot_hash, preprocessing_version)
    document_revision_source  original payload + original HTML (added by migration 001)
    revision_company          revision <-> company links, resolved via company/company_symbol
    sec_filing_details        SEC-only filing metadata (CIK, accession, items, URLs, exhibits)
    publisher                 original publisher of a news article, distinct from the API provider

Idempotency is enforced by the database, not by this code: replaying an
unchanged payload conflicts on the two unique constraints above and inserts
nothing. `source_snapshot_hash` is the ingestion pipeline's existing
`provenance.dedup.content_fingerprint` (a full SHA-256), so a payload that did
not change produces the same hash and therefore no new revision.

Not written here: `text_unit`, `embedding`, `embedding_profile` and the event
tables. When semantic analysis is enabled, `datasources.similarity.store`
writes them in the same transaction, right after this function, from the
ingestion graph's `persist_result` node.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from ..ingest.schema import PreparedRecord

LOGGER = logging.getLogger(__name__)

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ALPACA_NAMESPACE = "news"

# Source classification already defined by the schema's CHECK constraints. SEC
# must be 'official_filing' (the validate_sec_details trigger enforces it) and
# an Alpaca article is a third-party media report. Publisher credibility tiers
# are NOT set here: `publisher.credibility_tier` keeps its 'unreviewed' default
# until a human reviews it, and an existing tier is never overwritten.
SOURCE_CATEGORY = {"sec_filing": "official_filing", "news_article": "media_report"}


class PersistenceError(RuntimeError):
    """A record could not be mapped onto the schema (the transaction is rolled back)."""


@dataclass
class PersistResult:
    """What one `persist_record` call did, for the caller's counters."""

    document_id: str
    revision_id: str
    document_inserted: bool = False
    revision_inserted: bool = False
    companies_linked: int = 0
    unresolved_symbols: list[str] = field(default_factory=list)
    sec_details_inserted: bool = False
    raw_source_inserted: bool = False

    @property
    def outcome(self) -> str:
        """'inserted' = new revision stored, 'updated' = new revision for a known
        document, 'skipped' = this exact payload was already stored."""
        if not self.revision_inserted:
            return "skipped"
        return "inserted" if self.document_inserted else "updated"


# --------------------------------------------------------------------- mapping helpers
def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def publisher_key(name: str) -> str:
    """Stable key for one publisher, so 'Benzinga' and 'benzinga' are one row."""
    return re.sub(r"\s+", " ", name).strip().lower()


def map_content(record: PreparedRecord) -> tuple[str, str]:
    """Map the pipeline's content flags onto the schema's CHECK-constrained values.

    The pipeline uses full_text/summary_only/headline_only/empty; the schema uses
    available/partial/missing/extraction_failed plus a scope. Metadata-only and
    failed extractions must never look like fully extracted documents.
    """
    if record.record_type == "sec_filing":
        extraction_ok = record.sec_metadata is not None and record.sec_metadata.extraction.status == "ok"
        if not extraction_ok or not (record.body_text or "").strip():
            return "extraction_failed", "primary_document_only"
        return "available", "primary_document_only"

    if record.content_status == "full_text" and (record.body_text or "").strip():
        return "available", "full_article"
    if record.content_status == "summary_only" or (record.summary_text or "").strip():
        return "partial", "title_summary_only"
    if record.content_status == "headline_only" or (record.title or "").strip():
        # A headline with no article text is not extracted content.
        return "missing", "title_summary_only"
    return "missing", "metadata_only"


def _namespace(record: PreparedRecord) -> str:
    if record.record_type == "sec_filing":
        if record.sec_metadata is None:
            raise PersistenceError(f"SEC record {record.record_id} has no sec_metadata")
        return record.sec_metadata.cik
    return ALPACA_NAMESPACE


def _snapshot_hash(record: PreparedRecord) -> str:
    digest = record.provenance.dedup.content_fingerprint
    if not digest or not SHA256_RE.match(digest):
        raise PersistenceError(
            f"record {record.record_id} has no usable content fingerprint "
            f"({digest!r}); source_snapshot_hash must be a SHA-256 hex digest"
        )
    return digest


def _as_utc(value: datetime | None) -> datetime | None:
    """All stored timestamps are timezone-aware UTC."""
    if value is None:
        return None
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _as_date(value: str | None) -> date | None:
    try:
        return date.fromisoformat(value) if value else None
    except ValueError:
        return None


def _provenance_payload(record: PreparedRecord) -> dict[str, Any]:
    """Provenance as stored: the pipeline's provenance plus the record's own ids."""
    payload = record.provenance.model_dump(mode="json")
    payload["record_id"] = record.record_id
    payload["raw_file_refs"] = list(record.raw_file_refs)
    payload["pipeline_content_status"] = record.content_status
    payload["pipeline_content_scope"] = record.content_scope
    if record.title_is_generated:
        payload["title_is_generated"] = True
    return payload


# --------------------------------------------------------------------- reference data
def provider_id(conn: Connection, code: str) -> int:
    cur = conn.execute("SELECT provider_id FROM provider WHERE code = %s", (code,))
    row = cur.fetchone()
    if row is None:
        raise PersistenceError(
            f"provider {code!r} is not registered in the provider table; "
            "insert it before ingesting (the schema seeds 'sec_edgar' and 'alpaca')"
        )
    return int(row["provider_id"])


def upsert_publisher(conn: Connection, name: str) -> str:
    """Get or create the original publisher, preserving any existing credibility review."""
    key = publisher_key(name)
    cur = conn.execute(
        """
        INSERT INTO publisher (canonical_key, name)
        VALUES (%s, %s)
        ON CONFLICT (canonical_key) DO NOTHING
        RETURNING publisher_id
        """,
        (key, name.strip()),
    )
    row = cur.fetchone()
    if row is not None:
        return str(row["publisher_id"])
    cur = conn.execute("SELECT publisher_id FROM publisher WHERE canonical_key = %s", (key,))
    existing = cur.fetchone()
    assert existing is not None
    return str(existing["publisher_id"])


def company_by_cik(conn: Connection, cik: str, name: str | None) -> str:
    """Get or create the filing issuer, identified by its 10-digit CIK."""
    cur = conn.execute("SELECT company_id FROM company WHERE cik = %s", (cik,))
    row = cur.fetchone()
    if row is not None:
        return str(row["company_id"])
    label = (name or "").strip() or f"CIK {cik}"
    cur = conn.execute(
        """
        INSERT INTO company (name, cik) VALUES (%s, %s)
        ON CONFLICT (cik) DO NOTHING
        RETURNING company_id
        """,
        (label, cik),
    )
    inserted = cur.fetchone()
    if inserted is not None:
        return str(inserted["company_id"])
    cur = conn.execute("SELECT company_id FROM company WHERE cik = %s", (cik,))
    existing = cur.fetchone()
    assert existing is not None
    return str(existing["company_id"])


def companies_for_symbols(conn: Connection, symbols: list[str]) -> tuple[list[str], list[str]]:
    """Resolve tickers through `company_symbol`. Unknown tickers are reported, not invented.

    A ticker we have no company row for cannot be linked without inventing a CIK
    and a market, so it is returned as unresolved and kept in provenance instead.
    """
    if not symbols:
        return [], []
    cur = conn.execute(
        "SELECT DISTINCT symbol, company_id FROM company_symbol WHERE symbol = ANY(%s)",
        (list(symbols),),
    )
    found = {row["symbol"]: str(row["company_id"]) for row in cur.fetchall()}
    company_ids = list(dict.fromkeys(found[s] for s in symbols if s in found))
    unresolved = [s for s in symbols if s not in found]
    return company_ids, unresolved


# --------------------------------------------------------------------- main entry point
def persist_record(
    conn: Connection,
    record: PreparedRecord,
    *,
    raw_payload: dict[str, Any] | None = None,
    raw_document: str | None = None,
) -> PersistResult:
    """Store one prepared record and everything related to it.

    Must be called inside a transaction (`Database.transaction()`): the document,
    its revision, the company links, the SEC details and the raw source are all
    written together, so a failure on any one of them leaves no partial article.

    Replaying an unchanged payload inserts nothing new and returns
    `outcome == 'skipped'`.
    """
    namespace = _namespace(record)
    snapshot_hash = _snapshot_hash(record)
    content_status, content_scope = map_content(record)
    category = SOURCE_CATEGORY[record.record_type]
    pid = provider_id(conn, record.provider)

    # 1. Document: the source record's stable identity.
    cur = conn.execute(
        """
        INSERT INTO document (provider_id, source_namespace, source_record_id, record_type, is_sample)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (provider_id, source_namespace, source_record_id, is_sample)
        DO UPDATE SET last_seen_at = now()
        RETURNING document_id, (xmax = 0) AS inserted
        """,
        (pid, namespace, record.source_record_id, record.record_type, record.is_sample),
    )
    row = cur.fetchone()
    assert row is not None
    document_id = str(row["document_id"])
    result = PersistResult(
        document_id=document_id,
        revision_id="",
        document_inserted=bool(row["inserted"]),
    )

    publisher_id = upsert_publisher(conn, record.publisher) if record.publisher else None

    # 2. Revision: one row per distinct payload. An unchanged replay conflicts here.
    cur = conn.execute(
        """
        INSERT INTO document_revision (
            document_id, publisher_id, source_category, title, title_generated, author,
            summary_text, body_text, analysis_text, original_url, published_at,
            source_updated_at, retrieved_at, schema_version, preprocessing_version,
            source_snapshot_hash, content_status, content_scope, provenance, processing_warnings
        ) VALUES (
            %(document_id)s, %(publisher_id)s, %(source_category)s, %(title)s, %(title_generated)s,
            %(author)s, %(summary_text)s, %(body_text)s, %(analysis_text)s, %(original_url)s,
            %(published_at)s, %(source_updated_at)s, %(retrieved_at)s, %(schema_version)s,
            %(preprocessing_version)s, %(source_snapshot_hash)s, %(content_status)s,
            %(content_scope)s, %(provenance)s, %(processing_warnings)s
        )
        ON CONFLICT (document_id, source_snapshot_hash, preprocessing_version) DO NOTHING
        RETURNING revision_id
        """,
        {
            "document_id": document_id,
            "publisher_id": publisher_id,
            "source_category": category,
            "title": record.title,
            "title_generated": record.title_is_generated,
            "author": record.author,
            "summary_text": record.summary_text,
            "body_text": record.body_text,
            "analysis_text": record.analysis_text,
            "original_url": record.original_url,
            "published_at": _as_utc(record.published_at),
            "source_updated_at": _as_utc(record.updated_at),
            "retrieved_at": _as_utc(record.retrieved_at),
            "schema_version": record.schema_version,
            "preprocessing_version": record.provenance.pipeline_version,
            "source_snapshot_hash": snapshot_hash,
            "content_status": content_status,
            "content_scope": content_scope,
            "provenance": Jsonb(_provenance_payload(record)),
            "processing_warnings": Jsonb(list(record.warnings)),
        },
    )
    revision_row = cur.fetchone()

    if revision_row is None:
        # Same payload, same preprocessing version: already stored. Nothing to add.
        cur = conn.execute(
            """
            SELECT revision_id FROM document_revision
            WHERE document_id = %s AND source_snapshot_hash = %s AND preprocessing_version = %s
            """,
            (document_id, snapshot_hash, record.provenance.pipeline_version),
        )
        existing = cur.fetchone()
        assert existing is not None, "conflict target row disappeared inside the transaction"
        result.revision_id = str(existing["revision_id"])
        return result

    revision_id = str(revision_row["revision_id"])
    result.revision_id = revision_id
    result.revision_inserted = True

    # 3. Company links.
    company_ids, unresolved = companies_for_symbols(conn, record.symbols)
    issuer_id: str | None = None
    if record.record_type == "sec_filing":
        assert record.sec_metadata is not None
        issuer_id = company_by_cik(conn, record.sec_metadata.cik, record.issuer_name)
        if issuer_id not in company_ids:
            company_ids.append(issuer_id)
    result.unresolved_symbols = unresolved
    for company_id in company_ids:
        conn.execute(
            """
            INSERT INTO revision_company (revision_id, company_id) VALUES (%s, %s)
            ON CONFLICT (revision_id, company_id) DO NOTHING
            """,
            (revision_id, company_id),
        )
    result.companies_linked = len(company_ids)

    # 4. SEC-specific metadata.
    if record.record_type == "sec_filing":
        assert issuer_id is not None
        _insert_sec_details(conn, record, revision_id=revision_id, issuer_company_id=issuer_id)
        result.sec_details_inserted = True

    # 5. Original payload and original document HTML.
    payload = raw_payload if raw_payload is not None else {}
    if payload:
        conn.execute(
            """
            INSERT INTO document_revision_source (
                revision_id, raw_payload, raw_payload_sha256,
                raw_document, raw_document_sha256, raw_document_url, raw_file_refs
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (revision_id) DO NOTHING
            """,
            (
                revision_id,
                Jsonb(payload),
                _sha256_text(_canonical_json(payload)),
                raw_document,
                _sha256_text(raw_document) if raw_document is not None else None,
                record.primary_document_url,
                Jsonb(list(record.raw_file_refs)),
            ),
        )
        result.raw_source_inserted = True

    # 6. This payload is the newest one we have, so it becomes the current revision.
    conn.execute(
        "UPDATE document SET current_revision_id = %s, last_seen_at = now() WHERE document_id = %s",
        (revision_id, document_id),
    )
    return result


def _insert_sec_details(conn: Connection, record: PreparedRecord, *, revision_id: str,
                       issuer_company_id: str) -> None:
    meta = record.sec_metadata
    assert meta is not None
    accepted_at = _as_utc(record.published_at)
    if accepted_at is None and meta.acceptance_datetime_raw:
        try:
            accepted_at = _as_utc(datetime.fromisoformat(meta.acceptance_datetime_raw.replace("Z", "+00:00")))
        except ValueError:
            accepted_at = None
    filing_date = _as_date(meta.filing_date)
    if filing_date is None:
        raise PersistenceError(
            f"filing {meta.accession_number} has no valid filingDate; sec_filing_details.filing_date is NOT NULL"
        )
    if not record.filing_index_url or not record.primary_document_url:
        raise PersistenceError(f"filing {meta.accession_number} is missing its index or primary-document URL")
    exhibits = [
        {"url": link.url, "text": link.text, "extracted": meta.exhibits_extracted}
        for link in meta.exhibit_links
    ]
    conn.execute(
        """
        INSERT INTO sec_filing_details (
            revision_id, issuer_company_id, cik, accession_number, form, filing_date,
            report_date, accepted_at, item_codes, filing_index_url, primary_document_url, exhibits
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (revision_id) DO NOTHING
        """,
        (
            revision_id,
            issuer_company_id,
            meta.cik,
            meta.accession_number,
            meta.form,
            filing_date,
            _as_date(meta.report_date),
            accepted_at,
            list(meta.items),
            record.filing_index_url,
            record.primary_document_url,
            Jsonb(exhibits),
        ),
    )
