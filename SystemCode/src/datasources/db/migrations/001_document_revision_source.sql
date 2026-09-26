-- Additive migration: preserve the original source payload for every revision.
--
-- Why: the existing schema keeps curated and cleaned fields on `document_revision`
-- (title / summary_text / body_text / analysis_text) but has no column for the
-- ORIGINAL API payload or the ORIGINAL primary-document HTML. Ingestion is
-- required to preserve both, so this adds one 1:1 side table instead of widening
-- `document_revision`, which is the hot, indexed table.
--
-- Nothing existing is dropped, altered or backfilled: two new tables only. The
-- new table reuses the schema's existing `reject_snapshot_update()` trigger so
-- stored raw source is immutable, exactly like `document_revision` itself.

CREATE TABLE IF NOT EXISTS schema_migration (
    filename    text PRIMARY KEY,
    applied_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS document_revision_source (
    revision_id          uuid PRIMARY KEY REFERENCES document_revision(revision_id),
    -- Exactly what the provider returned for this record (Alpaca article object,
    -- or the SEC `filings.recent` row), before any cleaning.
    raw_payload          jsonb NOT NULL,
    raw_payload_sha256   text  NOT NULL,
    -- Original primary-document HTML for SEC filings; NULL for news articles.
    raw_document         text,
    raw_document_sha256  text,
    raw_document_url     text,
    -- Paths of the raw files the worker also wrote under output/raw/.
    raw_file_refs        jsonb NOT NULL DEFAULT '[]'::jsonb,
    stored_at            timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT document_revision_source_payload_check
        CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT document_revision_source_payload_hash_check
        CHECK (raw_payload_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT document_revision_source_document_hash_check
        CHECK (raw_document_sha256 IS NULL OR raw_document_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT document_revision_source_document_pair_check
        CHECK ((raw_document IS NULL) = (raw_document_sha256 IS NULL)),
    CONSTRAINT document_revision_source_refs_check
        CHECK (jsonb_typeof(raw_file_refs) = 'array')
);

COMMENT ON TABLE document_revision_source IS
    'Immutable original provider payload and primary-document HTML for one document_revision.';

-- Same immutability rule the rest of the snapshot tables use.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'revision_source_immutable'
          AND tgrelid = 'public.document_revision_source'::regclass
    ) THEN
        CREATE TRIGGER revision_source_immutable
            BEFORE UPDATE ON document_revision_source
            FOR EACH ROW EXECUTE FUNCTION reject_snapshot_update();
    END IF;
END
$$;
