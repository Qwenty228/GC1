"""Pydantic models for the ingestion pipeline.

`SourceEnvelope` is the explicit input to the graph: it says which provider and
transport produced a raw record, when and from where it was retrieved, and
whether it is sample data. `PreparedRecord` is the versioned common output.

Terminology kept deliberately distinct:
    provider         API we obtained data from ("sec_edgar" | "alpaca")
    publisher        original news publisher (Alpaca article ``source``, e.g. "benzinga")
    issuer_name      company that submitted an SEC filing
    api_endpoint     URL the JSON came from (in ``provenance``)
    original_url     citable document/article URL
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = "1.0"
PIPELINE_VERSION = "ingest-1"

Provider = Literal["sec_edgar", "alpaca"]
Transport = Literal["rest", "websocket", "file"]
RecordType = Literal["sec_filing", "news_article"]
ContentStatus = Literal["full_text", "summary_only", "headline_only", "empty"]
ContentScope = Literal["primary_document_only", "provider_article_payload"]


class SecContext(BaseModel):
    """Company-level fields that accompany one filing row from submissions JSON."""

    model_config = ConfigDict(extra="ignore")

    cik: str
    company_name: str | None = None
    tickers: list[str] = Field(default_factory=list)

    @field_validator("cik")
    @classmethod
    def _pad_cik(cls, value: str) -> str:
        digits = str(value).strip()
        if not digits.isdigit() or len(digits) > 10:
            raise ValueError(f"CIK must be up to 10 digits, got {value!r}")
        return digits.zfill(10)


class SourceEnvelope(BaseModel):
    """One raw source record plus the metadata needed to prepare it."""

    model_config = ConfigDict(extra="forbid")

    provider: Provider
    transport: Transport
    api_endpoint: str
    retrieved_at: datetime
    requested_symbols: list[str] = Field(default_factory=list)
    is_sample: bool
    raw_record: dict[str, Any]
    raw_file_refs: list[str] = Field(default_factory=list)
    # SEC only
    sec_context: SecContext | None = None
    primary_document_html: str | None = None
    primary_document_url: str | None = None

    @field_validator("retrieved_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("retrieved_at must be timezone-aware")
        return value


class ExhibitLink(BaseModel):
    url: str
    text: str | None = None


class ExtractionInfo(BaseModel):
    status: Literal["ok", "failed"]
    parser: str
    char_count: int
    table_count: int = 0
    inline_xbrl_fact_count: int = 0
    section_headings: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class SecMetadata(BaseModel):
    cik: str
    accession_number: str
    form: str
    is_amendment: bool
    filing_date: str | None
    report_date: str | None
    acceptance_datetime_raw: str | None
    items: list[str]
    item_descriptions: dict[str, str]
    primary_document: str
    primary_document_description: str | None
    file_number: str | None = None
    film_number: str | None = None
    act: str | None = None
    size: int | None = None
    is_xbrl: bool | None = None
    is_inline_xbrl: bool | None = None
    exhibit_links: list[ExhibitLink] = Field(default_factory=list)
    exhibits_extracted: bool = False
    extraction: ExtractionInfo


class DedupInfo(BaseModel):
    """Source-identity tracking fields (never used across different identities)."""

    identity: str
    revision_updated_at: datetime | None = None
    content_fingerprint: str | None = None
    content_level: int = 0


class Provenance(BaseModel):
    provider: Provider
    transport: Transport
    api_endpoint: str
    retrieved_at: datetime
    requested_symbols: list[str]
    adapter: str
    pipeline_version: str = PIPELINE_VERSION
    source_record_keys: list[str] = Field(default_factory=list)
    dedup: DedupInfo


class PreparedRecord(BaseModel):
    """Versioned common output record written to ``processed/``."""

    schema_version: str = SCHEMA_VERSION
    record_id: str
    record_type: RecordType
    provider: Provider
    source_record_id: str
    publisher: str | None
    issuer_name: str | None
    author: str | None
    symbols: list[str]
    title: str | None
    title_is_generated: bool
    summary_text: str | None
    body_text: str | None
    analysis_text: str
    content_status: ContentStatus
    content_scope: ContentScope
    published_at: datetime | None
    updated_at: datetime | None
    retrieved_at: datetime
    original_url: str | None
    filing_index_url: str | None
    primary_document_url: str | None
    exhibit_urls: list[str]
    is_sample: bool
    provenance: Provenance
    raw_file_refs: list[str]
    warnings: list[str]
    sec_metadata: SecMetadata | None


class RecordError(BaseModel):
    """Per-record failure; the rest of the batch still proceeds."""

    index: int
    provider: str | None
    source_record_id: str | None
    stage: str
    error_type: str
    message: str
    retryable: bool = True
