"""PostgreSQL persistence tests.

Mapping tests run offline. The rest need a reachable `stock_news` database and
are skipped when there is none (no `.env`, no server, wrong credentials).

Every record these tests write uses ``is_sample=True``, which is part of the
`document` identity, so synthetic rows can never collide with or be mistaken for
real ingested news — and `current_live_document` hides them. The `db` fixture
deletes exactly those rows afterwards and nothing else.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import date, datetime, timezone

import pytest

from datasources.ingest.schema import (
    DedupInfo,
    ExhibitLink,
    ExtractionInfo,
    PreparedRecord,
    Provenance,
    SecMetadata,
)

psycopg = pytest.importorskip("psycopg")

from conftest import TEST_PUBLISHER, cleanup_sample_rows
from datasources.db import Database, PersistResult, map_content, persist_record
from datasources.db.repository import ALPACA_NAMESPACE, publisher_key
from datasources.db.settings import load_db_settings

NVDA_CIK = "0001045810"


# --------------------------------------------------------------------------- fixtures
@pytest.fixture(scope="module")
def db():
    """A live database, or skip. Removes only the sample rows the tests created."""
    try:
        database = Database(load_db_settings())
        database.check()
    except Exception as error:  # ConfigurationError, DatabaseError, OperationalError
        pytest.skip(f"no stock_news database available: {error}")
    try:
        yield database
    finally:
        _cleanup(database)
        database.close()


def _cleanup(database: Database) -> None:
    cleanup_sample_rows(database)


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def alpaca_record(article_id: str, *, body: str | None = "Synthetic body text about NVIDIA.",
                  summary: str | None = "Synthetic summary.", title: str | None = "Synthetic NVDA headline",
                  updated_at: datetime | None = None, fingerprint: str | None = None,
                  symbols: list[str] | None = None, status: str = "full_text") -> PreparedRecord:
    """A synthetic news article shaped exactly like the Alpaca adapter's output."""
    stamp = updated_at or datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
    digest = fingerprint or _digest(f"{article_id}|{body}|{summary}|{title}")
    return PreparedRecord(
        record_id=f"alpaca:{article_id}:rev",
        record_type="news_article",
        provider="alpaca",
        source_record_id=article_id,
        publisher=TEST_PUBLISHER,
        issuer_name=None,
        author="Synthetic Reporter",
        symbols=symbols if symbols is not None else ["NVDA"],
        title=title,
        title_is_generated=False,
        summary_text=summary,
        body_text=body,
        analysis_text="\n\n".join(p for p in (title, summary, body) if p) or "empty",
        content_status=status,
        content_scope="provider_article_payload",
        published_at=datetime(2026, 9, 20, 11, 0, tzinfo=timezone.utc),
        updated_at=stamp,
        retrieved_at=datetime(2026, 9, 20, 12, 5, tzinfo=timezone.utc),
        original_url=f"https://news.example.test/{article_id}",
        filing_index_url=None,
        primary_document_url=None,
        exhibit_urls=[],
        is_sample=True,
        provenance=Provenance(
            provider="alpaca", transport="rest", api_endpoint="https://data.alpaca.markets/v1beta1/news",
            retrieved_at=datetime(2026, 9, 20, 12, 5, tzinfo=timezone.utc), requested_symbols=["NVDA"],
            adapter="alpaca.news_rest",
            dedup=DedupInfo(identity=f"alpaca:{article_id}", revision_updated_at=stamp,
                            content_fingerprint=digest, content_level=2),
        ),
        raw_file_refs=["raw/alpaca/articles/x/y.json"],
        warnings=[],
        sec_metadata=None,
    )


def sec_record(accession: str, *, form: str = "8-K", cik: str = NVDA_CIK,
               body: str = "Synthetic extracted 8-K text.", extraction_status: str = "ok",
               fingerprint: str | None = None) -> PreparedRecord:
    """A synthetic filing shaped exactly like the SEC adapter's output."""
    base = f"https://www.sec.gov/Archives/edgar/data/1045810/{accession.replace('-', '')}"
    return PreparedRecord(
        record_id=f"sec_edgar:{cik}:{accession}",
        record_type="sec_filing",
        provider="sec_edgar",
        source_record_id=accession,
        publisher=None,
        issuer_name="NVIDIA Corporation",
        author=None,
        symbols=["NVDA"],
        title=f"NVIDIA Corporation Form {form} filed 2026-09-10",
        title_is_generated=True,
        summary_text=None,
        body_text=body,
        analysis_text=f"[SEC EDGAR filing metadata]\n{body}",
        content_status="full_text",
        content_scope="primary_document_only",
        published_at=datetime(2026, 9, 10, 20, 0, tzinfo=timezone.utc),
        updated_at=None,
        retrieved_at=datetime(2026, 9, 10, 21, 0, tzinfo=timezone.utc),
        original_url=f"{base}/nvda-20260910.htm",
        filing_index_url=f"{base}/{accession}-index.htm",
        primary_document_url=f"{base}/nvda-20260910.htm",
        exhibit_urls=[f"{base}/ex991.htm"],
        is_sample=True,
        provenance=Provenance(
            provider="sec_edgar", transport="rest",
            api_endpoint=f"https://data.sec.gov/submissions/CIK{cik}.json",
            retrieved_at=datetime(2026, 9, 10, 21, 0, tzinfo=timezone.utc), requested_symbols=["NVDA"],
            adapter="sec_edgar.submissions_8k",
            dedup=DedupInfo(identity=f"sec_edgar:{cik}:{accession}",
                            content_fingerprint=fingerprint or _digest(f"{accession}|{body}"),
                            content_level=2),
        ),
        raw_file_refs=[f"raw/sec/documents/{cik}/{accession}/nvda-20260910.htm"],
        warnings=[],
        sec_metadata=SecMetadata(
            cik=cik, accession_number=accession, form=form, is_amendment=form.endswith("/A"),
            filing_date="2026-09-10", report_date="2026-09-10",
            acceptance_datetime_raw="2026-09-10T20:00:00.000Z", items=["8.01"],
            item_descriptions={"8.01": "Other Events"}, primary_document="nvda-20260910.htm",
            primary_document_description="8-K",
            exhibit_links=[ExhibitLink(url=f"{base}/ex991.htm", text="Press release")],
            exhibits_extracted=False,
            extraction=ExtractionInfo(status=extraction_status, parser="beautifulsoup4+lxml",
                                      char_count=len(body), table_count=1),
        ),
    )


def _unique_accession() -> str:
    """A syntactically valid accession the schema's CHECK accepts."""
    return f"9999999999-26-{uuid.uuid4().int % 1_000_000:06d}"


def _unique_article_id() -> str:
    return f"9{uuid.uuid4().int % 100_000_000:08d}"


# --------------------------------------------------------------------------- offline mapping
def test_map_content_full_article():
    assert map_content(alpaca_record("1")) == ("available", "full_article")


def test_map_content_summary_only_is_partial():
    record = alpaca_record("1", body=None, status="summary_only")
    assert map_content(record) == ("partial", "title_summary_only")


def test_map_content_headline_only_is_not_extracted():
    """A metadata-only article must never look like a fully extracted document."""
    record = alpaca_record("1", body=None, summary=None, status="headline_only")
    status, scope = map_content(record)
    assert status == "missing"
    assert scope == "title_summary_only"


def test_map_content_empty_article_is_metadata_only():
    record = alpaca_record("1", body=None, summary=None, title=None, status="empty")
    assert map_content(record) == ("missing", "metadata_only")


def test_map_content_marks_failed_sec_extraction():
    record = sec_record("0001045810-26-000099", body="", extraction_status="failed")
    assert map_content(record) == ("extraction_failed", "primary_document_only")


def test_publisher_key_is_case_and_space_insensitive():
    assert publisher_key("  Benzinga  ") == publisher_key("benzinga") == "benzinga"


# --------------------------------------------------------------------------- persistence
def test_alpaca_article_round_trip(db):
    article_id = _unique_article_id()
    record = alpaca_record(article_id)
    with db.transaction() as conn:
        result = persist_record(conn, record, raw_payload={"id": article_id, "headline": "Synthetic NVDA headline"})
    assert result.outcome == "inserted"
    assert result.revision_inserted and result.document_inserted
    assert result.companies_linked == 1  # NVDA resolves through company_symbol

    with db.connection() as conn:
        row = conn.execute(
            """
            SELECT d.source_namespace, d.record_type, d.current_revision_id, p.code AS provider,
                   pub.name AS publisher, pub.credibility_tier, r.title, r.original_url,
                   r.body_text, r.content_status, r.content_scope, r.published_at, r.source_updated_at,
                   s.raw_payload->>'id' AS raw_id
              FROM document d
              JOIN provider p ON p.provider_id = d.provider_id
              JOIN document_revision r ON r.revision_id = d.current_revision_id
              LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
              LEFT JOIN document_revision_source s ON s.revision_id = r.revision_id
             WHERE d.source_record_id = %s AND d.is_sample = true
            """,
            (article_id,),
        ).fetchone()
    assert row is not None
    assert row["provider"] == "alpaca"                     # API provider
    assert row["publisher"] == TEST_PUBLISHER              # original publisher, kept distinct
    assert row["credibility_tier"] == "unreviewed"         # never invented
    assert row["source_namespace"] == ALPACA_NAMESPACE
    assert row["original_url"].endswith(article_id)
    assert row["content_status"] == "available"
    assert row["raw_id"] == article_id                     # original payload preserved
    assert row["published_at"].utcoffset() is not None     # timezone-aware
    assert str(row["current_revision_id"]) == result.revision_id


def test_replaying_the_same_article_creates_nothing(db):
    article_id = _unique_article_id()
    record = alpaca_record(article_id)
    payload = {"id": article_id}
    with db.transaction() as conn:
        first = persist_record(conn, record, raw_payload=payload)
    before = _counts(db, article_id)
    for _ in range(3):
        with db.transaction() as conn:
            again = persist_record(conn, record, raw_payload=payload)
        assert again.outcome == "skipped"
        assert again.revision_id == first.revision_id
        assert not again.revision_inserted
    assert _counts(db, article_id) == before
    assert before == {"documents": 1, "revisions": 1, "links": 1}


def test_changed_article_adds_a_revision_not_a_document(db):
    """A genuinely updated payload follows the schema's revision model."""
    article_id = _unique_article_id()
    first = alpaca_record(article_id, body="First version of the story.")
    with db.transaction() as conn:
        r1 = persist_record(conn, first, raw_payload={"id": article_id, "v": 1})
    second = alpaca_record(
        article_id, body="Second version with more detail.",
        updated_at=datetime(2026, 9, 20, 13, 0, tzinfo=timezone.utc),
    )
    with db.transaction() as conn:
        r2 = persist_record(conn, second, raw_payload={"id": article_id, "v": 2})
    assert r2.outcome == "updated"
    assert r2.document_id == r1.document_id      # same source record
    assert r2.revision_id != r1.revision_id      # new revision
    counts = _counts(db, article_id)
    assert counts == {"documents": 1, "revisions": 2, "links": 2}
    with db.connection() as conn:
        row = conn.execute(
            "SELECT current_revision_id FROM document WHERE document_id = %s", (r1.document_id,)
        ).fetchone()
    assert str(row["current_revision_id"]) == r2.revision_id  # newest becomes current


def test_sec_filing_round_trip_with_details(db):
    accession = _unique_accession()
    record = sec_record(accession)
    with db.transaction() as conn:
        result = persist_record(conn, record, raw_payload={"accessionNumber": accession},
                                raw_document="<html>original 8-K</html>")
    assert result.outcome == "inserted"
    assert result.sec_details_inserted

    with db.connection() as conn:
        row = conn.execute(
            """
            SELECT d.source_namespace, r.content_scope, s.accession_number, s.form, s.cik,
                   s.filing_date, s.accepted_at, s.item_codes, s.filing_index_url,
                   s.primary_document_url, jsonb_array_length(s.exhibits) AS exhibits,
                   c.name AS issuer, src.raw_document
              FROM document d
              JOIN document_revision r ON r.revision_id = d.current_revision_id
              JOIN sec_filing_details s ON s.revision_id = r.revision_id
              JOIN company c ON c.company_id = s.issuer_company_id
              LEFT JOIN document_revision_source src ON src.revision_id = r.revision_id
             WHERE d.source_record_id = %s AND d.is_sample = true
            """,
            (accession,),
        ).fetchone()
    assert row is not None
    assert row["source_namespace"] == NVDA_CIK      # SEC namespace is the CIK
    assert row["accession_number"] == accession
    assert row["form"] == "8-K"
    assert row["issuer"] == "NVIDIA Corporation"
    assert row["filing_date"] == date(2026, 9, 10)
    assert row["accepted_at"].utcoffset() is not None
    assert list(row["item_codes"]) == ["8.01"]
    assert row["exhibits"] == 1                    # linked, not extracted
    assert row["content_scope"] == "primary_document_only"
    assert row["raw_document"] == "<html>original 8-K</html>"


def test_amendment_is_a_separate_filing(db):
    """8-K/A has its own accession, so it is its own document - never merged."""
    original, amendment = _unique_accession(), _unique_accession()
    with db.transaction() as conn:
        first = persist_record(conn, sec_record(original, form="8-K"), raw_payload={"a": 1})
    with db.transaction() as conn:
        second = persist_record(conn, sec_record(amendment, form="8-K/A"), raw_payload={"a": 2})
    assert first.document_id != second.document_id
    with db.connection() as conn:
        rows = conn.execute(
            "SELECT form FROM sec_filing_details WHERE accession_number = ANY(%s) ORDER BY form",
            ([original, amendment],),
        ).fetchall()
    assert [r["form"] for r in rows] == ["8-K", "8-K/A"]


def test_identical_text_under_different_ids_stays_separate(db):
    """Same story, two article ids: two documents. No semantic de-duplication."""
    first_id, second_id = _unique_article_id(), _unique_article_id()
    shared = "Exactly the same body text in both articles."
    with db.transaction() as conn:
        a = persist_record(conn, alpaca_record(first_id, body=shared), raw_payload={"id": first_id})
    with db.transaction() as conn:
        b = persist_record(conn, alpaca_record(second_id, body=shared), raw_payload={"id": second_id})
    assert a.document_id != b.document_id
    assert a.outcome == b.outcome == "inserted"


def test_unknown_symbols_are_reported_not_invented(db):
    """A ticker with no company row is recorded as unresolved, never guessed."""
    article_id = _unique_article_id()
    record = alpaca_record(article_id, symbols=["NVDA", "ZZZZ-NOT-A-TICKER"])
    with db.transaction() as conn:
        result = persist_record(conn, record, raw_payload={"id": article_id})
    assert result.companies_linked == 1
    assert result.unresolved_symbols == ["ZZZZ-NOT-A-TICKER"]
    with db.connection() as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM company WHERE name = 'ZZZZ-NOT-A-TICKER' OR cik = 'ZZZZ-NOT-A-TICKER'"
        ).fetchone()
    assert row["n"] == 0


def test_no_embeddings_are_created(db):
    """This stage stores news only: no text units, no vectors, no fake embeddings."""
    article_id = _unique_article_id()
    with db.transaction() as conn:
        result = persist_record(conn, alpaca_record(article_id), raw_payload={"id": article_id})
    with db.connection() as conn:
        row = conn.execute(
            """
            SELECT (SELECT count(*) FROM text_unit WHERE revision_id = %(rev)s) AS units,
                   (SELECT count(*) FROM embedding e JOIN text_unit t USING (unit_id)
                     WHERE t.revision_id = %(rev)s) AS embeddings
            """,
            {"rev": result.revision_id},
        ).fetchone()
    assert row["units"] == 0
    assert row["embeddings"] == 0


# --------------------------------------------------------------------------- rollback
def test_failed_related_insert_leaves_no_partial_record(db):
    """A failure on a related insert must roll back the whole record.

    The failure here is enforced by the database, not simulated: `form` is
    '10-Q', which `sec_filing_details_form_check` rejects. The document and the
    revision are inserted first, so if the transaction boundary were wrong we
    would be left with an article row that has no filing details.
    """
    accession = _unique_accession()
    bad = sec_record(accession, form="10-Q")

    with pytest.raises(psycopg.errors.CheckViolation):
        with db.transaction() as conn:
            persist_record(conn, bad, raw_payload={"accessionNumber": accession})

    with db.connection() as conn:
        row = conn.execute(
            """
            SELECT (SELECT count(*) FROM document WHERE source_record_id = %(acc)s) AS documents,
                   (SELECT count(*) FROM sec_filing_details WHERE accession_number = %(acc)s) AS details,
                   (SELECT count(*) FROM document_revision r JOIN document d USING (document_id)
                     WHERE d.source_record_id = %(acc)s) AS revisions
            """,
            {"acc": accession},
        ).fetchone()
    assert row == {"documents": 0, "revisions": 0, "details": 0}, "partial record survived a failed insert"

    # The connection is still usable afterwards, and a good record still stores.
    good = sec_record(_unique_accession())
    with db.transaction() as conn:
        assert persist_record(conn, good, raw_payload={"ok": True}).outcome == "inserted"


def test_result_outcome_labels():
    assert PersistResult("d", "r", document_inserted=True, revision_inserted=True).outcome == "inserted"
    assert PersistResult("d", "r", document_inserted=False, revision_inserted=True).outcome == "updated"
    assert PersistResult("d", "r", revision_inserted=False).outcome == "skipped"


# --------------------------------------------------------------------------- helpers
def _counts(db: Database, source_record_id: str) -> dict[str, int]:
    with db.connection() as conn:
        row = conn.execute(
            """
            SELECT (SELECT count(*) FROM document WHERE source_record_id = %(id)s AND is_sample = true) AS documents,
                   (SELECT count(*) FROM document_revision r JOIN document d USING (document_id)
                     WHERE d.source_record_id = %(id)s AND d.is_sample = true) AS revisions,
                   (SELECT count(*) FROM revision_company rc
                     JOIN document_revision r USING (revision_id) JOIN document d USING (document_id)
                     WHERE d.source_record_id = %(id)s AND d.is_sample = true) AS links
            """,
            {"id": source_record_id},
        ).fetchone()
    assert row is not None
    return {k: int(v) for k, v in row.items()}
