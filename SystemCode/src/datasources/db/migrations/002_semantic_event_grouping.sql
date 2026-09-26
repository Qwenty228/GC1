-- Additive migration: semantic news comparison and event grouping.
--
-- Reuses the existing `text_unit`, `embedding` and `embedding_profile` tables
-- (and their immutability / dimension triggers) and adds only what they cannot
-- hold:
--
--   text_unit.search_tsv    generated tsvector + GIN index for lexical retrieval
--   revision_analysis       analysis status per (revision, embedding profile,
--                           pipeline version). Mutable on purpose: status,
--                           attempts and errors change on retry, and
--                           `document_revision` is immutable by trigger.
--   news_event              one identifiable occurrence/transaction and its
--                           later developments, partitioned by is_sample and by
--                           analysis space (embedding profile + pipeline version)
--   event_comparison        each comparison decision, its evidence and the
--                           model / prompt that produced it (immutable)
--   event_membership        revision -> event links (origin, repeated, update,
--                           conflict, or a pending 'uncertain' relationship)
--   event_membership_unit   the text units that support each membership
--   retrieval_candidate     semantic and lexical scores/ranks per candidate
--
-- Nothing existing is dropped, altered in type, or backfilled.

-- --------------------------------------------------------------------- lexical search
ALTER TABLE text_unit
    ADD COLUMN IF NOT EXISTS search_tsv tsvector
    GENERATED ALWAYS AS (to_tsvector('english'::regconfig, input_text)) STORED;

CREATE INDEX IF NOT EXISTS text_unit_search_idx ON text_unit USING gin (search_tsv);
CREATE INDEX IF NOT EXISTS text_unit_revision_profile_idx ON text_unit (revision_id, profile_id);
CREATE INDEX IF NOT EXISTS text_unit_profile_hash_idx ON text_unit (profile_id, input_hash);

-- --------------------------------------------------------------------- analysis status
CREATE TABLE IF NOT EXISTS revision_analysis (
    analysis_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    revision_id       uuid NOT NULL,
    document_id       uuid NOT NULL,
    profile_id        uuid NOT NULL REFERENCES embedding_profile(profile_id),
    pipeline_version  text NOT NULL,
    -- completed | partial (limits reached or index coverage incomplete) |
    -- pending (a stage failed; retry later) | failed (gave up) | skipped (no usable text)
    status            text NOT NULL,
    stage_status      jsonb NOT NULL DEFAULT '{}'::jsonb,
    skip_reason       text,
    evidence_level    text,
    anchor_time       timestamptz,
    anchor_source     text,
    index_coverage    jsonb NOT NULL DEFAULT '{}'::jsonb,
    limitations       jsonb NOT NULL DEFAULT '[]'::jsonb,
    usage             jsonb NOT NULL DEFAULT '{}'::jsonb,
    comparison_model  text,
    prompt_version    text,
    attempts          integer NOT NULL DEFAULT 0,
    last_error        text,
    next_retry_at     timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    completed_at      timestamptz,
    CONSTRAINT revision_analysis_revision_fk
        FOREIGN KEY (document_id, revision_id) REFERENCES document_revision(document_id, revision_id),
    CONSTRAINT revision_analysis_identity_key UNIQUE (revision_id, profile_id, pipeline_version),
    CONSTRAINT revision_analysis_status_check
        CHECK (status IN ('completed', 'partial', 'pending', 'failed', 'skipped')),
    CONSTRAINT revision_analysis_skip_check CHECK ((status = 'skipped') = (skip_reason IS NOT NULL)),
    CONSTRAINT revision_analysis_evidence_check
        CHECK (evidence_level IS NULL OR evidence_level IN ('full', 'summary_only', 'headline_only')),
    CONSTRAINT revision_analysis_attempts_check CHECK (attempts >= 0),
    CONSTRAINT revision_analysis_stage_check CHECK (jsonb_typeof(stage_status) = 'object'),
    CONSTRAINT revision_analysis_coverage_check CHECK (jsonb_typeof(index_coverage) = 'object'),
    CONSTRAINT revision_analysis_limitations_check CHECK (jsonb_typeof(limitations) = 'array'),
    CONSTRAINT revision_analysis_usage_check CHECK (jsonb_typeof(usage) = 'object')
);

CREATE INDEX IF NOT EXISTS revision_analysis_work_idx
    ON revision_analysis (profile_id, pipeline_version, status, next_retry_at)
    WHERE status IN ('pending', 'partial');
CREATE INDEX IF NOT EXISTS revision_analysis_document_idx ON revision_analysis (document_id);

COMMENT ON TABLE revision_analysis IS
    'Semantic-analysis status of one revision under one embedding profile and pipeline version. '
    'Ingestion completion (the file ledger) is tracked separately from analysis completion.';

-- --------------------------------------------------------------------- events
CREATE TABLE IF NOT EXISTS news_event (
    event_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    is_sample           boolean NOT NULL,
    profile_id          uuid NOT NULL REFERENCES embedding_profile(profile_id),
    pipeline_version    text NOT NULL,
    -- provisional: one source so far | corroborated: two or more documents |
    -- superseded: a single-source provisional event later found to duplicate another
    status              text NOT NULL DEFAULT 'provisional',
    title               text NOT NULL,
    event_facts         jsonb NOT NULL DEFAULT '{}'::jsonb,
    flags               jsonb NOT NULL DEFAULT '[]'::jsonb,
    first_source_time   timestamptz,
    last_source_time    timestamptz,
    origin_revision_id  uuid NOT NULL REFERENCES document_revision(revision_id),
    origin_analysis_id  uuid NOT NULL REFERENCES revision_analysis(analysis_id),
    origin_segment_key  text NOT NULL,
    superseded_by       uuid REFERENCES news_event(event_id),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT news_event_origin_key UNIQUE (origin_analysis_id, origin_segment_key),
    CONSTRAINT news_event_status_check CHECK (status IN ('provisional', 'corroborated', 'superseded')),
    CONSTRAINT news_event_superseded_check CHECK ((status = 'superseded') = (superseded_by IS NOT NULL)),
    CONSTRAINT news_event_not_self_check CHECK (superseded_by IS NULL OR superseded_by <> event_id),
    CONSTRAINT news_event_title_check CHECK (btrim(title) <> ''),
    CONSTRAINT news_event_facts_check CHECK (jsonb_typeof(event_facts) = 'object'),
    CONSTRAINT news_event_flags_check CHECK (jsonb_typeof(flags) = 'array')
);

CREATE INDEX IF NOT EXISTS news_event_space_idx
    ON news_event (is_sample, profile_id, pipeline_version, status);

COMMENT ON TABLE news_event IS
    'One identifiable occurrence or transaction and its subsequent developments. Recurring '
    'occurrences (e.g. each fiscal quarter''s results) are separate events. Grouping compares '
    'reported claims; it does not establish which source is true.';

-- --------------------------------------------------------------------- comparisons
CREATE TABLE IF NOT EXISTS event_comparison (
    comparison_id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    analysis_id             uuid NOT NULL REFERENCES revision_analysis(analysis_id),
    revision_id             uuid NOT NULL REFERENCES document_revision(revision_id),
    segment_key             text NOT NULL,
    candidate_event_id      uuid NOT NULL REFERENCES news_event(event_id),
    candidate_revision_ids  uuid[] NOT NULL DEFAULT '{}',
    label                   text NOT NULL,
    method                  text NOT NULL,
    incoming_unit_ids       uuid[] NOT NULL DEFAULT '{}',
    candidate_unit_ids      uuid[] NOT NULL DEFAULT '{}',
    matching_facts          jsonb NOT NULL DEFAULT '[]'::jsonb,
    differences             jsonb NOT NULL DEFAULT '[]'::jsonb,
    evidence                jsonb NOT NULL DEFAULT '{}'::jsonb,
    explanation             text NOT NULL DEFAULT '',
    insufficient_context    boolean NOT NULL DEFAULT false,
    uncertainty             text,
    validation_notes        jsonb NOT NULL DEFAULT '[]'::jsonb,
    model                   text,
    prompt_version          text,
    usage                   jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT event_comparison_pair_key UNIQUE (analysis_id, segment_key, candidate_event_id),
    CONSTRAINT event_comparison_label_check CHECK (label IN (
        'same_event_repeated', 'same_event_update', 'same_event_conflict', 'different_event', 'uncertain')),
    CONSTRAINT event_comparison_method_check CHECK (method IN ('llm', 'exact_content')),
    CONSTRAINT event_comparison_facts_check CHECK (jsonb_typeof(matching_facts) = 'array'),
    CONSTRAINT event_comparison_differences_check CHECK (jsonb_typeof(differences) = 'array'),
    CONSTRAINT event_comparison_evidence_check CHECK (jsonb_typeof(evidence) = 'object'),
    CONSTRAINT event_comparison_notes_check CHECK (jsonb_typeof(validation_notes) = 'array'),
    CONSTRAINT event_comparison_usage_check CHECK (jsonb_typeof(usage) = 'object')
);

CREATE INDEX IF NOT EXISTS event_comparison_event_idx ON event_comparison (candidate_event_id);

-- --------------------------------------------------------------------- memberships
CREATE TABLE IF NOT EXISTS event_membership (
    membership_id  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id       uuid NOT NULL REFERENCES news_event(event_id),
    revision_id    uuid NOT NULL,
    document_id    uuid NOT NULL,
    analysis_id    uuid NOT NULL REFERENCES revision_analysis(analysis_id),
    segment_key    text NOT NULL,
    relation       text NOT NULL,
    -- 'pending' holds an uncertain relationship without merging the revision into the event
    state          text NOT NULL DEFAULT 'active',
    comparison_id  uuid REFERENCES event_comparison(comparison_id),
    created_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT event_membership_revision_fk
        FOREIGN KEY (document_id, revision_id) REFERENCES document_revision(document_id, revision_id),
    CONSTRAINT event_membership_pair_key UNIQUE (event_id, revision_id),
    CONSTRAINT event_membership_relation_check CHECK (relation IN (
        'origin', 'same_event_repeated', 'same_event_update', 'same_event_conflict', 'uncertain')),
    CONSTRAINT event_membership_state_check CHECK (state IN ('active', 'pending')),
    CONSTRAINT event_membership_pending_check CHECK ((relation = 'uncertain') = (state = 'pending')),
    CONSTRAINT event_membership_comparison_check CHECK ((relation = 'origin') = (comparison_id IS NULL))
);

CREATE INDEX IF NOT EXISTS event_membership_revision_idx ON event_membership (revision_id);
CREATE INDEX IF NOT EXISTS event_membership_document_idx ON event_membership (document_id);

CREATE TABLE IF NOT EXISTS event_membership_unit (
    membership_id  uuid NOT NULL REFERENCES event_membership(membership_id),
    unit_id        uuid NOT NULL REFERENCES text_unit(unit_id),
    PRIMARY KEY (membership_id, unit_id)
);

CREATE INDEX IF NOT EXISTS event_membership_unit_unit_idx ON event_membership_unit (unit_id);

-- --------------------------------------------------------------------- retrieval scores
CREATE TABLE IF NOT EXISTS retrieval_candidate (
    analysis_id            uuid NOT NULL REFERENCES revision_analysis(analysis_id),
    incoming_unit_id       uuid NOT NULL REFERENCES text_unit(unit_id),
    candidate_unit_id      uuid NOT NULL REFERENCES text_unit(unit_id),
    candidate_revision_id  uuid NOT NULL REFERENCES document_revision(revision_id),
    semantic_rank          integer,
    semantic_similarity    double precision,
    lexical_rank           integer,
    lexical_score          double precision,
    fused_score            double precision NOT NULL,
    fused_rank             integer NOT NULL,
    candidate_event_ids    uuid[] NOT NULL DEFAULT '{}',
    shortlisted            boolean NOT NULL DEFAULT false,
    not_compared_reason    text,
    created_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (analysis_id, incoming_unit_id, candidate_unit_id),
    CONSTRAINT retrieval_candidate_channel_check CHECK (semantic_rank IS NOT NULL OR lexical_rank IS NOT NULL),
    CONSTRAINT retrieval_candidate_not_self_check CHECK (incoming_unit_id <> candidate_unit_id)
);

-- --------------------------------------------------------------------- partition guards
-- Sample and live data must never be grouped together, and an analysis space
-- (embedding profile + pipeline version) must never mix with another one.
CREATE OR REPLACE FUNCTION validate_news_event() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.revision_analysis a
        JOIN public.document d ON d.document_id = a.document_id
        WHERE a.analysis_id = NEW.origin_analysis_id
          AND a.revision_id = NEW.origin_revision_id
          AND a.profile_id = NEW.profile_id
          AND a.pipeline_version = NEW.pipeline_version
          AND d.is_sample = NEW.is_sample
    ) THEN
        RAISE EXCEPTION 'news_event must match its origin analysis space and sample/live partition';
    END IF;
    IF NEW.superseded_by IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM public.news_event e
        WHERE e.event_id = NEW.superseded_by
          AND e.is_sample = NEW.is_sample
          AND e.profile_id = NEW.profile_id
          AND e.pipeline_version = NEW.pipeline_version
    ) THEN
        RAISE EXCEPTION 'an event can only be superseded by an event in the same partition';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION validate_event_link() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    target uuid;
BEGIN
    -- Separate branches: PL/pgSQL only plans the branch it runs, and each table
    -- has only one of these columns.
    IF TG_TABLE_NAME = 'event_membership' THEN
        target := NEW.event_id;
    ELSE
        target := NEW.candidate_event_id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.revision_analysis a
        JOIN public.document d ON d.document_id = a.document_id
        JOIN public.news_event e ON e.event_id = target
        WHERE a.analysis_id = NEW.analysis_id
          AND a.revision_id = NEW.revision_id
          AND a.profile_id = e.profile_id
          AND a.pipeline_version = e.pipeline_version
          AND d.is_sample = e.is_sample
    ) THEN
        RAISE EXCEPTION '% must link a revision to an event in the same analysis space and sample/live partition',
            TG_TABLE_NAME;
    END IF;
    RETURN NEW;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'news_event_partition_check'
                   AND tgrelid = 'public.news_event'::regclass) THEN
        CREATE TRIGGER news_event_partition_check
            BEFORE INSERT OR UPDATE ON news_event
            FOR EACH ROW EXECUTE FUNCTION validate_news_event();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'event_membership_partition_check'
                   AND tgrelid = 'public.event_membership'::regclass) THEN
        CREATE TRIGGER event_membership_partition_check
            BEFORE INSERT OR UPDATE ON event_membership
            FOR EACH ROW EXECUTE FUNCTION validate_event_link();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'event_comparison_partition_check'
                   AND tgrelid = 'public.event_comparison'::regclass) THEN
        CREATE TRIGGER event_comparison_partition_check
            BEFORE INSERT ON event_comparison
            FOR EACH ROW EXECUTE FUNCTION validate_event_link();
    END IF;
    -- A comparison is a record of what was decided at the time: never rewritten.
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'event_comparison_immutable'
                   AND tgrelid = 'public.event_comparison'::regclass) THEN
        CREATE TRIGGER event_comparison_immutable
            BEFORE UPDATE ON event_comparison
            FOR EACH ROW EXECUTE FUNCTION reject_snapshot_update();
    END IF;
END
$$;

-- --------------------------------------------------------------------- current views
-- Only the CURRENT revision of each document counts: a superseded revision is
-- never an additional independent article, and superseded events are hidden.
CREATE OR REPLACE VIEW current_event_membership AS
SELECT m.membership_id, m.event_id, m.revision_id, m.document_id, m.relation, m.segment_key,
       m.analysis_id, m.comparison_id, e.is_sample, e.status AS event_status, d.record_type,
       r.publisher_id,
       COALESCE(r.published_at, s.accepted_at, r.source_updated_at, r.retrieved_at) AS source_time
  FROM event_membership m
  JOIN news_event e ON e.event_id = m.event_id
  JOIN document d ON d.document_id = m.document_id AND d.current_revision_id = m.revision_id
  JOIN document_revision r ON r.revision_id = m.revision_id
  LEFT JOIN sec_filing_details s ON s.revision_id = m.revision_id
 WHERE m.state = 'active' AND e.status <> 'superseded';

CREATE OR REPLACE VIEW current_live_event AS
SELECT e.event_id, e.title, e.status, e.event_facts, e.flags,
       min(m.source_time) AS first_source_time,
       max(m.source_time) AS last_source_time,
       count(DISTINCT m.document_id) AS documents,
       count(DISTINCT m.document_id) FILTER (WHERE m.record_type = 'news_article') AS articles,
       count(DISTINCT m.document_id) FILTER (WHERE m.record_type = 'sec_filing') AS filings,
       count(DISTINCT m.publisher_id) AS publishers,
       bool_or(m.relation = 'same_event_update') AS has_updates,
       bool_or(m.relation = 'same_event_conflict') AS has_conflicts,
       (SELECT count(*) FROM event_membership p
         WHERE p.event_id = e.event_id AND p.state = 'pending') AS pending_relations,
       e.profile_id, e.pipeline_version
  FROM news_event e
  JOIN current_event_membership m ON m.event_id = e.event_id
 WHERE e.is_sample = false AND e.status <> 'superseded'
 GROUP BY e.event_id;

CREATE OR REPLACE VIEW current_revision_analysis AS
SELECT d.document_id, d.record_type, d.is_sample, d.current_revision_id AS revision_id,
       a.analysis_id, a.profile_id, a.pipeline_version,
       COALESCE(a.status, 'not_analyzed') AS analysis_status,
       a.skip_reason, a.evidence_level, a.index_coverage, a.attempts, a.last_error, a.updated_at
  FROM document d
  LEFT JOIN revision_analysis a ON a.revision_id = d.current_revision_id;
