-- Baseline: the stock_news base schema (the project ERD) that migrations 001-005 build on.
--
-- Why this file exists
--   The base tables were created outside this repository, so a new database
--   could not be built from the migration files alone. This file reproduces the
--   base objects exactly as they exist in stock_news: the same tables, columns,
--   defaults, constraint names and definitions, indexes, trigger functions,
--   triggers and view, taken from a read-only schema dump. It contains nothing
--   that migrations 001-005 create, and none of the objects left behind by the
--   reverted `003_event_match_routing.sql` branch (datasources/db/schema.py
--   lists those as legacy objects).
--
-- How it is used
--   `datasources-db migrate` runs this file only on a database that has none of
--   these objects yet. It never runs on a database that already has them
--   (stock_news and its copies): there, `datasources-db migrate --adopt-baseline`
--   compares the existing structure with the one this file creates and records
--   the file as 'adopted' only when every object matches.
--
-- Prerequisites: PostgreSQL 13 or newer (gen_random_uuid() is built in) and the
-- pgvector extension files installed on the server. Creating the extension needs
-- a role allowed to run CREATE EXTENSION vector (or create it beforehand).
--
-- Reference data: the two API providers the ingestion code looks up by code, and
-- NVIDIA (CIK 0001045810, NASDAQ: NVDA), the default INGEST_SYMBOLS company, as in
-- stock_news. Other symbols need their own company and company_symbol rows.

CREATE TABLE IF NOT EXISTS schema_migration (
    filename    text PRIMARY KEY,
    applied_at  timestamptz NOT NULL DEFAULT now()
);

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;

-- --------------------------------------------------------------------- trigger functions
CREATE FUNCTION reject_snapshot_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION '% rows are immutable; insert a new version', TG_TABLE_NAME;
END;
$$;

CREATE FUNCTION protect_text_unit_input() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF ROW(NEW.unit_id, NEW.revision_id, NEW.profile_id, NEW.unit_kind,
           NEW.ordinal, NEW.input_text, NEW.input_hash, NEW.location)
       IS DISTINCT FROM
       ROW(OLD.unit_id, OLD.revision_id, OLD.profile_id, OLD.unit_kind,
           OLD.ordinal, OLD.input_text, OLD.input_hash, OLD.location) THEN
        RAISE EXCEPTION 'Text unit input is immutable; create a new profile/revision';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION validate_embedding_dimensions() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE expected_dimensions integer;
BEGIN
    SELECT p.dimensions INTO expected_dimensions
    FROM public.text_unit t
    JOIN public.embedding_profile p ON p.profile_id = t.profile_id
    WHERE t.unit_id = NEW.unit_id;
    IF expected_dimensions IS NULL OR
       public.vector_dims(NEW.embedding) <> expected_dimensions THEN
        RAISE EXCEPTION 'Embedding dimension mismatch or missing text unit/profile';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION validate_sec_details() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.document_revision r
        JOIN public.document d ON d.document_id = r.document_id
        JOIN public.provider p ON p.provider_id = d.provider_id
        JOIN public.company c ON c.company_id = NEW.issuer_company_id
        WHERE r.revision_id = NEW.revision_id
          AND d.record_type = 'sec_filing' AND p.code = 'sec_edgar'
          AND r.source_category = 'official_filing'
          AND d.source_namespace = NEW.cik
          AND d.source_record_id = NEW.accession_number
          AND c.cik = NEW.cik
    ) THEN
        RAISE EXCEPTION 'SEC details must match filing identity, issuer CIK and category';
    END IF;
    RETURN NEW;
END;
$$;

-- --------------------------------------------------------------------- sources and companies
CREATE TABLE provider (
    provider_id smallint GENERATED ALWAYS AS IDENTITY NOT NULL,
    code text NOT NULL,
    name text NOT NULL,
    CONSTRAINT provider_pkey PRIMARY KEY (provider_id),
    CONSTRAINT provider_code_key UNIQUE (code),
    CONSTRAINT provider_code_check CHECK ((btrim(code) <> ''::text))
);

CREATE TABLE company (
    company_id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    cik text,
    CONSTRAINT company_pkey PRIMARY KEY (company_id),
    CONSTRAINT company_cik_key UNIQUE (cik),
    CONSTRAINT company_cik_check CHECK ((cik ~ '^[0-9]{10}$'::text)),
    CONSTRAINT company_name_check CHECK ((btrim(name) <> ''::text))
);

CREATE TABLE company_symbol (
    symbol_id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    symbol text NOT NULL,
    market text NOT NULL,
    CONSTRAINT company_symbol_pkey PRIMARY KEY (symbol_id),
    CONSTRAINT company_symbol_market_symbol_key UNIQUE (market, symbol),
    CONSTRAINT company_symbol_company_id_fkey FOREIGN KEY (company_id) REFERENCES company(company_id),
    CONSTRAINT company_symbol_market_check CHECK ((btrim(market) <> ''::text)),
    CONSTRAINT company_symbol_symbol_check CHECK (((symbol = upper(symbol)) AND (btrim(symbol) <> ''::text)))
);

CREATE INDEX company_symbol_company_idx ON company_symbol USING btree (company_id);

CREATE TABLE publisher (
    publisher_id uuid DEFAULT gen_random_uuid() NOT NULL,
    canonical_key text NOT NULL,
    name text NOT NULL,
    credibility_tier text DEFAULT 'unreviewed'::text NOT NULL,
    credibility_reason text,
    credibility_reviewed_at timestamp with time zone,
    CONSTRAINT publisher_pkey PRIMARY KEY (publisher_id),
    CONSTRAINT publisher_canonical_key_key UNIQUE (canonical_key),
    CONSTRAINT publisher_credibility_tier_check
        CHECK ((credibility_tier = ANY (ARRAY['high'::text, 'medium'::text, 'low'::text, 'unreviewed'::text])))
);

-- --------------------------------------------------------------------- documents and revisions
CREATE TABLE document (
    document_id uuid DEFAULT gen_random_uuid() NOT NULL,
    provider_id smallint NOT NULL,
    source_namespace text NOT NULL,
    source_record_id text NOT NULL,
    record_type text NOT NULL,
    is_sample boolean DEFAULT false NOT NULL,
    current_revision_id uuid,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT document_pkey PRIMARY KEY (document_id),
    CONSTRAINT document_provider_id_source_namespace_source_record_id_is_s_key
        UNIQUE (provider_id, source_namespace, source_record_id, is_sample),
    CONSTRAINT document_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES provider(provider_id),
    CONSTRAINT document_check CHECK ((last_seen_at >= first_seen_at)),
    CONSTRAINT document_record_type_check
        CHECK ((record_type = ANY (ARRAY['news_article'::text, 'sec_filing'::text]))),
    CONSTRAINT document_source_namespace_check CHECK ((btrim(source_namespace) <> ''::text)),
    CONSTRAINT document_source_record_id_check CHECK ((btrim(source_record_id) <> ''::text))
);

CREATE TABLE document_revision (
    revision_id uuid DEFAULT gen_random_uuid() NOT NULL,
    document_id uuid NOT NULL,
    publisher_id uuid,
    source_category text NOT NULL,
    title text,
    title_generated boolean DEFAULT false NOT NULL,
    author text,
    summary_text text,
    body_text text,
    analysis_text text,
    original_url text,
    published_at timestamp with time zone,
    source_updated_at timestamp with time zone,
    retrieved_at timestamp with time zone NOT NULL,
    schema_version text NOT NULL,
    preprocessing_version text NOT NULL,
    source_snapshot_hash text NOT NULL,
    content_status text NOT NULL,
    content_scope text NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    processing_warnings jsonb DEFAULT '[]'::jsonb NOT NULL,
    CONSTRAINT document_revision_pkey PRIMARY KEY (revision_id),
    CONSTRAINT document_revision_document_id_revision_id_key UNIQUE (document_id, revision_id),
    CONSTRAINT document_revision_document_id_source_snapshot_hash_preproce_key
        UNIQUE (document_id, source_snapshot_hash, preprocessing_version),
    CONSTRAINT document_revision_document_id_fkey FOREIGN KEY (document_id) REFERENCES document(document_id),
    CONSTRAINT document_revision_publisher_id_fkey FOREIGN KEY (publisher_id) REFERENCES publisher(publisher_id),
    CONSTRAINT document_revision_content_scope_check
        CHECK ((content_scope = ANY (ARRAY['full_article'::text, 'primary_document_only'::text,
                                           'title_summary_only'::text, 'metadata_only'::text]))),
    CONSTRAINT document_revision_content_status_check
        CHECK ((content_status = ANY (ARRAY['available'::text, 'partial'::text, 'missing'::text,
                                            'extraction_failed'::text]))),
    CONSTRAINT document_revision_processing_warnings_check CHECK ((jsonb_typeof(processing_warnings) = 'array'::text)),
    CONSTRAINT document_revision_provenance_check CHECK ((jsonb_typeof(provenance) = 'object'::text)),
    CONSTRAINT document_revision_source_category_check
        CHECK ((source_category = ANY (ARRAY['official_filing'::text, 'media_report'::text,
                                             'company_announcement'::text, 'other'::text]))),
    CONSTRAINT document_revision_source_snapshot_hash_check CHECK ((source_snapshot_hash ~ '^[0-9a-f]{64}$'::text))
);

CREATE INDEX revision_category_idx ON document_revision USING btree (source_category);
CREATE INDEX revision_published_idx ON document_revision USING btree (published_at);
CREATE INDEX revision_publisher_idx ON document_revision USING btree (publisher_id);

-- document <-> document_revision reference each other; the current revision is checked at commit.
ALTER TABLE document
    ADD CONSTRAINT document_current_revision_fk FOREIGN KEY (document_id, current_revision_id)
        REFERENCES document_revision(document_id, revision_id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE revision_company (
    revision_id uuid NOT NULL,
    company_id uuid NOT NULL,
    CONSTRAINT revision_company_pkey PRIMARY KEY (revision_id, company_id),
    CONSTRAINT revision_company_company_id_fkey FOREIGN KEY (company_id) REFERENCES company(company_id),
    CONSTRAINT revision_company_revision_id_fkey FOREIGN KEY (revision_id) REFERENCES document_revision(revision_id)
);

CREATE INDEX revision_company_company_idx ON revision_company USING btree (company_id, revision_id);

CREATE TABLE sec_filing_details (
    revision_id uuid NOT NULL,
    issuer_company_id uuid NOT NULL,
    cik text NOT NULL,
    accession_number text NOT NULL,
    form text NOT NULL,
    filing_date date NOT NULL,
    report_date date,
    accepted_at timestamp with time zone,
    item_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    filing_index_url text NOT NULL,
    primary_document_url text NOT NULL,
    exhibits jsonb DEFAULT '[]'::jsonb NOT NULL,
    CONSTRAINT sec_filing_details_pkey PRIMARY KEY (revision_id),
    CONSTRAINT sec_filing_details_issuer_company_id_fkey FOREIGN KEY (issuer_company_id) REFERENCES company(company_id),
    CONSTRAINT sec_filing_details_revision_id_fkey FOREIGN KEY (revision_id) REFERENCES document_revision(revision_id),
    CONSTRAINT sec_filing_details_accession_number_check
        CHECK ((accession_number ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'::text)),
    CONSTRAINT sec_filing_details_cik_check CHECK ((cik ~ '^[0-9]{10}$'::text)),
    CONSTRAINT sec_filing_details_exhibits_check CHECK ((jsonb_typeof(exhibits) = 'array'::text)),
    CONSTRAINT sec_filing_details_form_check CHECK ((form = ANY (ARRAY['8-K'::text, '8-K/A'::text])))
);

CREATE INDEX sec_accepted_idx ON sec_filing_details USING btree (accepted_at);
CREATE INDEX sec_issuer_idx ON sec_filing_details USING btree (issuer_company_id);

-- --------------------------------------------------------------------- text units and embeddings
CREATE TABLE embedding_profile (
    profile_id uuid DEFAULT gen_random_uuid() NOT NULL,
    config_hash text NOT NULL,
    model_provider text NOT NULL,
    model_name text NOT NULL,
    model_version text NOT NULL,
    dimensions integer NOT NULL,
    distance_metric text NOT NULL,
    chunking_version text NOT NULL,
    configuration jsonb NOT NULL,
    CONSTRAINT embedding_profile_pkey PRIMARY KEY (profile_id),
    CONSTRAINT embedding_profile_config_hash_key UNIQUE (config_hash),
    CONSTRAINT embedding_profile_config_hash_check CHECK ((config_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT embedding_profile_configuration_check CHECK ((jsonb_typeof(configuration) = 'object'::text)),
    CONSTRAINT embedding_profile_dimensions_check CHECK ((dimensions > 0)),
    CONSTRAINT embedding_profile_distance_metric_check
        CHECK ((distance_metric = ANY (ARRAY['cosine'::text, 'l2'::text, 'inner_product'::text])))
);

CREATE TABLE text_unit (
    unit_id uuid DEFAULT gen_random_uuid() NOT NULL,
    revision_id uuid NOT NULL,
    profile_id uuid NOT NULL,
    unit_kind text NOT NULL,
    ordinal integer NOT NULL,
    input_text text NOT NULL,
    input_hash text NOT NULL,
    location jsonb DEFAULT '{}'::jsonb NOT NULL,
    embedding_status text DEFAULT 'pending'::text NOT NULL,
    retry_count integer DEFAULT 0 NOT NULL,
    last_error text,
    CONSTRAINT text_unit_pkey PRIMARY KEY (unit_id),
    CONSTRAINT text_unit_revision_id_profile_id_unit_kind_ordinal_key UNIQUE (revision_id, profile_id, unit_kind, ordinal),
    CONSTRAINT text_unit_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES embedding_profile(profile_id),
    CONSTRAINT text_unit_revision_id_fkey FOREIGN KEY (revision_id) REFERENCES document_revision(revision_id),
    CONSTRAINT text_unit_embedding_status_check
        CHECK ((embedding_status = ANY (ARRAY['pending'::text, 'processing'::text, 'completed'::text, 'failed'::text]))),
    CONSTRAINT text_unit_input_hash_check CHECK ((input_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT text_unit_input_text_check CHECK ((btrim(input_text) <> ''::text)),
    CONSTRAINT text_unit_location_check CHECK ((jsonb_typeof(location) = 'object'::text)),
    CONSTRAINT text_unit_ordinal_check CHECK ((ordinal >= 0)),
    CONSTRAINT text_unit_retry_count_check CHECK ((retry_count >= 0)),
    CONSTRAINT text_unit_unit_kind_check
        CHECK ((unit_kind = ANY (ARRAY['document'::text, 'chunk'::text, 'overview'::text])))
);

CREATE INDEX text_unit_profile_idx ON text_unit USING btree (profile_id);
CREATE INDEX text_unit_work_idx ON text_unit USING btree (embedding_status)
    WHERE (embedding_status = ANY (ARRAY['pending'::text, 'failed'::text]));

-- Dimensionless: each embedding profile fixes its own dimension, checked by trigger.
CREATE TABLE embedding (
    unit_id uuid NOT NULL,
    embedding vector NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT embedding_pkey PRIMARY KEY (unit_id),
    CONSTRAINT embedding_unit_id_fkey FOREIGN KEY (unit_id) REFERENCES text_unit(unit_id)
);

-- Unused by the application (collection cursors live in output/state/ledger.json); part of the ERD.
CREATE TABLE ingestion_state (
    provider_id smallint NOT NULL,
    stream_key text NOT NULL,
    is_sample boolean DEFAULT false NOT NULL,
    checkpoint jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ingestion_state_pkey PRIMARY KEY (provider_id, stream_key, is_sample),
    CONSTRAINT ingestion_state_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES provider(provider_id),
    CONSTRAINT ingestion_state_checkpoint_check CHECK ((jsonb_typeof(checkpoint) = 'object'::text))
);

-- --------------------------------------------------------------------- integrity triggers
CREATE TRIGGER revision_immutable BEFORE UPDATE ON document_revision
    FOR EACH ROW EXECUTE FUNCTION reject_snapshot_update();
CREATE TRIGGER sec_details_identity_check BEFORE INSERT ON sec_filing_details
    FOR EACH ROW EXECUTE FUNCTION validate_sec_details();
CREATE TRIGGER sec_details_immutable BEFORE UPDATE ON sec_filing_details
    FOR EACH ROW EXECUTE FUNCTION reject_snapshot_update();
CREATE TRIGGER profile_immutable BEFORE UPDATE ON embedding_profile
    FOR EACH ROW EXECUTE FUNCTION reject_snapshot_update();
CREATE TRIGGER text_unit_input_immutable BEFORE UPDATE ON text_unit
    FOR EACH ROW EXECUTE FUNCTION protect_text_unit_input();
CREATE TRIGGER embedding_dimensions_check BEFORE INSERT OR UPDATE ON embedding
    FOR EACH ROW EXECUTE FUNCTION validate_embedding_dimensions();

-- --------------------------------------------------------------------- read view (hides sample rows)
CREATE VIEW current_live_document AS
 SELECT d.document_id,
    d.record_type,
    d.source_record_id,
    r.revision_id,
    r.title,
    r.summary_text,
    r.body_text,
    r.original_url,
    r.source_category,
        CASE r.source_category
            WHEN 'official_filing'::text THEN 'Official filing'::text
            WHEN 'media_report'::text THEN 'Media report'::text
            WHEN 'company_announcement'::text THEN 'Company announcement'::text
            ELSE 'Other'::text
        END AS source_badge,
    p.code AS api_provider,
    pub.name AS publisher_name,
    COALESCE(pub.credibility_tier, 'unreviewed'::text) AS publisher_credibility_tier,
    c.name AS filing_issuer,
        CASE
            WHEN (d.record_type = 'sec_filing'::text) THEN s.accepted_at
            ELSE r.published_at
        END AS source_time,
    r.retrieved_at,
    r.content_status,
    r.content_scope
   FROM (((((document d
     JOIN document_revision r ON (((r.revision_id = d.current_revision_id) AND (r.document_id = d.document_id))))
     JOIN provider p ON ((p.provider_id = d.provider_id)))
     LEFT JOIN publisher pub ON ((pub.publisher_id = r.publisher_id)))
     LEFT JOIN sec_filing_details s ON ((s.revision_id = r.revision_id)))
     LEFT JOIN company c ON ((c.company_id = s.issuer_company_id)))
  WHERE (d.is_sample = false);

-- --------------------------------------------------------------------- reference data
-- Looked up by code; never created by the application ("provider ... is not registered").
INSERT INTO provider (code, name) VALUES ('sec_edgar', 'SEC EDGAR'), ('alpaca', 'Alpaca');

INSERT INTO company (name, cik) VALUES ('NVIDIA Corporation', '0001045810');
INSERT INTO company_symbol (company_id, symbol, market)
    SELECT company_id, 'NVDA', 'NASDAQ' FROM company WHERE cik = '0001045810';
