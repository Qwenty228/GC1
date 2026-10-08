-- Additive migration: event summaries, statement-level citations, conflict review.
--
-- Migration history note
--   The stock_news database may already record `003_event_match_routing.sql`,
--   applied from a branch whose code was later reverted. That migration created
--   `event_summary`, `event_summary_citation`, `event_conflict_review` and the
--   `current_event_summary` view (and widened some migration-002 checks, which
--   this migration leaves alone). This file is numbered 004 so it never collides
--   with that history. Every statement below is idempotent: on a database that
--   has 003, the CREATE ... IF NOT EXISTS statements for the four designed
--   objects are no-ops and their existing definitions are reused unchanged; on a
--   fresh database they are created with the same definitions.
--
-- Designed objects (reused, or created here when absent)
--   event_summary           versioned summary per (event, language) + generation metadata
--   event_summary_citation  statement -> text_unit evidence with a verbatim quote
--   event_conflict_review   one human review per same_event_conflict comparison
--   current_event_summary   published summary per (event, language) + under-review flag
--
-- Added by this migration (what the four objects cannot hold)
--   event_summary.statements / input_manifest / limitations
--                           the stable statement ids and their text, which
--                           revisions and text units were given to the model,
--                           and evidence-selection limitations
--   event_summary_job       durable per-(event, language) work item: pending /
--                           running (lease) / completed / failed / awaiting_review,
--                           attempts, backoff and last error. Independent of the
--                           ingestion ledger, so summary work stays discoverable if
--                           the process stops after the article was committed.
--   event_summary_attempt   one immutable row per generation attempt, including
--                           invalid or stale output that was NOT published
--   event_review_decision   immutable audit of every review decision, including
--                           'insufficient_evidence' (review stays open) and any
--                           membership corrections
--   event_membership.state  widened with 'detached': a reviewer's different_events
--                           decision detaches the wrong membership instead of
--                           deleting it
--
-- Nothing existing is dropped or backfilled; document_revision.summary_text is
-- not touched (it belongs to one source revision, not to an event).

-- --------------------------------------------------------------------- designed tables (003 definitions)
CREATE TABLE IF NOT EXISTS event_summary (
    summary_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id           uuid NOT NULL REFERENCES news_event(event_id),
    language_code      text NOT NULL DEFAULT 'en',
    version            integer NOT NULL,
    headline           text NOT NULL,
    summary_text       text NOT NULL,
    key_points         jsonb NOT NULL DEFAULT '[]'::jsonb,
    status             text NOT NULL DEFAULT 'draft',
    generation_reason  text NOT NULL,
    model_provider     text NOT NULL,
    model_name         text NOT NULL,
    prompt_version     text NOT NULL,
    input_fingerprint  text NOT NULL,
    usage              jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at         timestamptz NOT NULL DEFAULT now(),
    published_at       timestamptz,
    UNIQUE (event_id, language_code, version),
    CHECK (version > 0),
    CHECK (btrim(headline) <> ''),
    CHECK (btrim(summary_text) <> ''),
    CHECK (jsonb_typeof(key_points) = 'array'),
    CHECK (status IN ('draft', 'published', 'superseded')),
    CHECK (generation_reason IN ('initial', 'new_information', 'review_resolution', 'manual_refresh')),
    CHECK (jsonb_typeof(usage) = 'object'),
    CHECK ((status = 'draft' AND published_at IS NULL)
           OR (status IN ('published', 'superseded') AND published_at IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS event_summary_one_published
    ON event_summary (event_id, language_code) WHERE status = 'published';
CREATE INDEX IF NOT EXISTS event_summary_input_lookup
    ON event_summary (event_id, language_code, input_fingerprint);

CREATE TABLE IF NOT EXISTS event_summary_citation (
    citation_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    summary_id      uuid NOT NULL REFERENCES event_summary(summary_id),
    unit_id         uuid NOT NULL REFERENCES text_unit(unit_id),
    statement_key   text NOT NULL,
    evidence_quote  text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (summary_id, statement_key, unit_id),
    CHECK (btrim(statement_key) <> ''),
    CHECK (btrim(evidence_quote) <> '')
);

CREATE INDEX IF NOT EXISTS event_summary_citation_unit ON event_summary_citation (unit_id);

CREATE TABLE IF NOT EXISTS event_conflict_review (
    review_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    comparison_id   uuid NOT NULL UNIQUE REFERENCES event_comparison(comparison_id),
    status          text NOT NULL DEFAULT 'open',
    resolution      text,
    reviewer_id     text,
    reviewer_notes  text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    resolved_at     timestamptz,
    CHECK (status IN ('open', 'in_review', 'resolved')),
    CHECK (resolution IN ('confirmed_conflict', 'not_a_conflict', 'different_events')),
    CHECK ((status IN ('open', 'in_review') AND resolution IS NULL AND resolved_at IS NULL)
           OR (status = 'resolved' AND resolution IS NOT NULL AND NULLIF(btrim(reviewer_id), '') IS NOT NULL
               AND NULLIF(btrim(reviewer_notes), '') IS NOT NULL AND resolved_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS event_conflict_review_open
    ON event_conflict_review (created_at) WHERE status IN ('open', 'in_review');

-- --------------------------------------------------------------------- summary structure
ALTER TABLE event_summary ADD COLUMN IF NOT EXISTS statements jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE event_summary ADD COLUMN IF NOT EXISTS input_manifest jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE event_summary ADD COLUMN IF NOT EXISTS limitations jsonb NOT NULL DEFAULT '[]'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'event_summary_statements_check') THEN
        ALTER TABLE event_summary ADD CONSTRAINT event_summary_statements_check
            CHECK (jsonb_typeof(statements) = 'array');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'event_summary_manifest_check') THEN
        ALTER TABLE event_summary ADD CONSTRAINT event_summary_manifest_check
            CHECK (jsonb_typeof(input_manifest) = 'object');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'event_summary_limitations_check') THEN
        ALTER TABLE event_summary ADD CONSTRAINT event_summary_limitations_check
            CHECK (jsonb_typeof(limitations) = 'array');
    END IF;
END
$$;

COMMENT ON TABLE event_summary IS
    'Versioned summary of one news_event per language. Content is immutable (trigger); only the status moves '
    'draft -> published -> superseded. At most one published row per (event, language).';
COMMENT ON TABLE event_summary_citation IS
    'Statement-level evidence: the text unit a summary statement cites and the verbatim quote from it.';
COMMENT ON TABLE event_conflict_review IS
    'Human review of one same_event_conflict comparison. Open or in_review reviews block automatic publication '
    'of a replacement summary for the compared event.';

-- --------------------------------------------------------------------- durable summary work
CREATE TABLE IF NOT EXISTS event_summary_job (
    job_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id          uuid NOT NULL REFERENCES news_event(event_id),
    language_code     text NOT NULL DEFAULT 'en',
    -- pending: work to do (next_retry_at = when) | running: leased by lease_owner until
    -- lease_expires_at | completed: the published summary covers the evidence |
    -- failed: attempts exhausted | awaiting_review: an open conflict review blocks it
    status            text NOT NULL DEFAULT 'pending',
    reason            text NOT NULL,
    attempts          integer NOT NULL DEFAULT 0,
    next_retry_at     timestamptz,
    lease_owner       text,
    lease_expires_at  timestamptz,
    last_error        text,
    last_input_fingerprint text,
    summary_id        uuid REFERENCES event_summary(summary_id),
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    completed_at      timestamptz,
    CONSTRAINT event_summary_job_key UNIQUE (event_id, language_code),
    CONSTRAINT event_summary_job_status_check
        CHECK (status IN ('pending', 'running', 'completed', 'failed', 'awaiting_review')),
    CONSTRAINT event_summary_job_reason_check
        CHECK (reason IN ('initial', 'new_information', 'review_resolution', 'manual_refresh')),
    CONSTRAINT event_summary_job_attempts_check CHECK (attempts >= 0),
    CONSTRAINT event_summary_job_lease_check
        CHECK ((status = 'running') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS event_summary_job_work_idx
    ON event_summary_job (status, next_retry_at) WHERE status IN ('pending', 'running', 'failed');

COMMENT ON TABLE event_summary_job IS
    'Durable summary work item per (event, language), independent of the ingestion ledger. Leases stop two '
    'processes from generating the same summary at once; an expired lease can be claimed again.';

CREATE TABLE IF NOT EXISTS event_summary_attempt (
    attempt_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                uuid NOT NULL REFERENCES event_summary_job(job_id),
    event_id              uuid NOT NULL REFERENCES news_event(event_id),
    language_code         text NOT NULL,
    attempt_number        integer NOT NULL,
    -- published | invalid (failed validation) | error (model/API failure) |
    -- stale (event changed during generation) | blocked (a conflict opened meanwhile)
    outcome               text NOT NULL,
    generation_reason     text NOT NULL,
    input_fingerprint     text NOT NULL,
    evidence_fingerprint  text NOT NULL,
    base_summary_id       uuid REFERENCES event_summary(summary_id),
    summary_id            uuid REFERENCES event_summary(summary_id),
    model_provider        text,
    model_name            text,
    prompt_version        text,
    output                jsonb,
    validation_errors     jsonb NOT NULL DEFAULT '[]'::jsonb,
    error_kind            text,
    error_message         text,
    usage                 jsonb NOT NULL DEFAULT '{}'::jsonb,
    input_manifest        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at            timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT event_summary_attempt_outcome_check
        CHECK (outcome IN ('published', 'invalid', 'error', 'stale', 'blocked')),
    CONSTRAINT event_summary_attempt_published_check CHECK ((outcome = 'published') = (summary_id IS NOT NULL)),
    CONSTRAINT event_summary_attempt_number_check CHECK (attempt_number > 0),
    CONSTRAINT event_summary_attempt_errors_check CHECK (jsonb_typeof(validation_errors) = 'array'),
    CONSTRAINT event_summary_attempt_usage_check CHECK (jsonb_typeof(usage) = 'object'),
    CONSTRAINT event_summary_attempt_manifest_check CHECK (jsonb_typeof(input_manifest) = 'object')
);

CREATE INDEX IF NOT EXISTS event_summary_attempt_event_idx ON event_summary_attempt (event_id, created_at);

COMMENT ON TABLE event_summary_attempt IS
    'Every summary generation attempt, with its output. Invalid, stale and blocked output is kept here and never '
    'published. Immutable.';

-- --------------------------------------------------------------------- review audit
CREATE TABLE IF NOT EXISTS event_review_decision (
    decision_id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    review_id               uuid NOT NULL REFERENCES event_conflict_review(review_id),
    decision                text NOT NULL,
    reviewer_id             text NOT NULL,
    explanation             text NOT NULL,
    resulting_status        text NOT NULL,
    membership_corrections  jsonb NOT NULL DEFAULT '[]'::jsonb,
    queued_jobs             jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT event_review_decision_check CHECK (decision IN (
        'not_a_conflict', 'confirmed_conflict', 'different_events', 'insufficient_evidence')),
    CONSTRAINT event_review_decision_status_check CHECK (resulting_status IN ('open', 'in_review', 'resolved')),
    CONSTRAINT event_review_decision_reviewer_check CHECK (btrim(reviewer_id) <> ''),
    CONSTRAINT event_review_decision_explanation_check CHECK (btrim(explanation) <> ''),
    CONSTRAINT event_review_decision_corrections_check CHECK (jsonb_typeof(membership_corrections) = 'array'),
    CONSTRAINT event_review_decision_jobs_check CHECK (jsonb_typeof(queued_jobs) = 'array')
);

CREATE INDEX IF NOT EXISTS event_review_decision_review_idx ON event_review_decision (review_id, created_at);

COMMENT ON TABLE event_review_decision IS
    'Audit of every conflict-review decision (who, why, what) and of the membership corrections it made. Immutable.';

-- --------------------------------------------------------------------- membership correction
-- 'detached' = a reviewer decided this revision reports a different event. The row
-- is kept (with its supporting units) for audit; current views only read 'active'.
ALTER TABLE event_membership DROP CONSTRAINT IF EXISTS event_membership_state_check;
ALTER TABLE event_membership ADD CONSTRAINT event_membership_state_check
    CHECK (state IN ('active', 'pending', 'detached'));

-- --------------------------------------------------------------------- integrity triggers
-- Summary content never changes after insert; only the publication status moves forward.
CREATE OR REPLACE FUNCTION protect_event_summary() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.summary_id, NEW.event_id, NEW.language_code, NEW.version, NEW.headline, NEW.summary_text,
           NEW.key_points, NEW.statements, NEW.generation_reason, NEW.model_provider, NEW.model_name,
           NEW.prompt_version, NEW.input_fingerprint, NEW.input_manifest, NEW.limitations, NEW.usage,
           NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.summary_id, OLD.event_id, OLD.language_code, OLD.version, OLD.headline, OLD.summary_text,
           OLD.key_points, OLD.statements, OLD.generation_reason, OLD.model_provider, OLD.model_name,
           OLD.prompt_version, OLD.input_fingerprint, OLD.input_manifest, OLD.limitations, OLD.usage,
           OLD.created_at) THEN
        RAISE EXCEPTION 'event_summary content is immutable; insert a new version';
    END IF;
    IF OLD.status = 'superseded' AND NEW.status <> 'superseded' THEN
        RAISE EXCEPTION 'a superseded event_summary cannot be republished';
    END IF;
    IF OLD.status = 'published' AND NEW.status = 'draft' THEN
        RAISE EXCEPTION 'a published event_summary cannot return to draft';
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'event_summary.published_at is set once';
    END IF;
    RETURN NEW;
END;
$$;

-- A citation must point at a text unit of the same sample/live partition as the event.
CREATE OR REPLACE FUNCTION validate_summary_citation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.event_summary s
        JOIN public.news_event e ON e.event_id = s.event_id
        JOIN public.text_unit t ON t.unit_id = NEW.unit_id
        JOIN public.document_revision r ON r.revision_id = t.revision_id
        JOIN public.document d ON d.document_id = r.document_id
        WHERE s.summary_id = NEW.summary_id AND d.is_sample = e.is_sample
    ) THEN
        RAISE EXCEPTION 'event_summary_citation must cite a text unit in the event''s sample/live partition';
    END IF;
    RETURN NEW;
END;
$$;

-- A resolved review is final: a new decision needs a new comparison.
CREATE OR REPLACE FUNCTION protect_conflict_review() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.comparison_id IS DISTINCT FROM OLD.comparison_id THEN
        RAISE EXCEPTION 'event_conflict_review.comparison_id is immutable';
    END IF;
    IF OLD.status = 'resolved' THEN
        RAISE EXCEPTION 'event_conflict_review % is already resolved', OLD.review_id;
    END IF;
    RETURN NEW;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'event_summary_content_immutable'
                   AND tgrelid = 'public.event_summary'::regclass) THEN
        CREATE TRIGGER event_summary_content_immutable
            BEFORE UPDATE ON event_summary
            FOR EACH ROW EXECUTE FUNCTION protect_event_summary();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'event_summary_citation_partition_check'
                   AND tgrelid = 'public.event_summary_citation'::regclass) THEN
        CREATE TRIGGER event_summary_citation_partition_check
            BEFORE INSERT ON event_summary_citation
            FOR EACH ROW EXECUTE FUNCTION validate_summary_citation();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'event_summary_citation_immutable'
                   AND tgrelid = 'public.event_summary_citation'::regclass) THEN
        CREATE TRIGGER event_summary_citation_immutable
            BEFORE UPDATE ON event_summary_citation
            FOR EACH ROW EXECUTE FUNCTION reject_snapshot_update();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'event_summary_attempt_immutable'
                   AND tgrelid = 'public.event_summary_attempt'::regclass) THEN
        CREATE TRIGGER event_summary_attempt_immutable
            BEFORE UPDATE ON event_summary_attempt
            FOR EACH ROW EXECUTE FUNCTION reject_snapshot_update();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'event_review_decision_immutable'
                   AND tgrelid = 'public.event_review_decision'::regclass) THEN
        CREATE TRIGGER event_review_decision_immutable
            BEFORE UPDATE ON event_review_decision
            FOR EACH ROW EXECUTE FUNCTION reject_snapshot_update();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'event_conflict_review_final'
                   AND tgrelid = 'public.event_conflict_review'::regclass) THEN
        CREATE TRIGGER event_conflict_review_final
            BEFORE UPDATE ON event_conflict_review
            FOR EACH ROW EXECUTE FUNCTION protect_conflict_review();
    END IF;
END
$$;

-- --------------------------------------------------------------------- current view (003 definition)
CREATE OR REPLACE VIEW current_event_summary AS
SELECT s.summary_id, s.event_id, s.language_code, s.version, s.headline, s.summary_text, s.key_points,
       s.status, s.generation_reason, s.model_provider, s.model_name, s.prompt_version, s.input_fingerprint,
       s.usage, s.created_at, s.published_at, e.is_sample,
       COALESCE(r.open_review_count, 0::bigint) AS open_review_count,
       COALESCE(r.open_review_count, 0::bigint) > 0 AS under_review
  FROM event_summary s
  JOIN news_event e ON e.event_id = s.event_id
  LEFT JOIN (SELECT c.candidate_event_id AS event_id, count(*) AS open_review_count
               FROM event_conflict_review r_1
               JOIN event_comparison c ON c.comparison_id = r_1.comparison_id
              WHERE r_1.status IN ('open', 'in_review')
              GROUP BY c.candidate_event_id) r ON r.event_id = s.event_id
 WHERE s.status = 'published';
