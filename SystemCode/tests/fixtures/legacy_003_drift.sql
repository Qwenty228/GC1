-- TEST FIXTURE, NOT A MIGRATION. Never run it against a real database.
--
-- Reproduces what the reverted `003_event_match_routing.sql` branch left in stock_news, copied from a
-- read-only schema dump of stock_news (2026-09-30). tests/test_migrations.py applies it to a temporary
-- database (after 002) so that baseline adoption and `verify` are tested against the legacy state that
-- stock_news and its copies really have. There is deliberately no 003 file in migrations/: see
-- datasources/db/schema.py (LEGACY_NOTES) for how these objects are reported.

ALTER TABLE event_comparison ADD COLUMN cache_key text;
CREATE INDEX event_comparison_cache_idx ON event_comparison USING btree (cache_key) WHERE (cache_key IS NOT NULL);

ALTER TABLE event_comparison DROP CONSTRAINT event_comparison_label_check;
ALTER TABLE event_comparison ADD CONSTRAINT event_comparison_label_check
    CHECK ((label = ANY (ARRAY['same_event_repeated'::text, 'same_event_update'::text, 'same_event_conflict'::text,
                               'same_event_unassessed'::text, 'different_event'::text, 'uncertain'::text])));
ALTER TABLE event_comparison DROP CONSTRAINT event_comparison_method_check;
ALTER TABLE event_comparison ADD CONSTRAINT event_comparison_method_check
    CHECK ((method = ANY (ARRAY['llm'::text, 'exact_content'::text, 'local_classifier'::text])));
ALTER TABLE event_membership DROP CONSTRAINT event_membership_relation_check;
ALTER TABLE event_membership ADD CONSTRAINT event_membership_relation_check
    CHECK ((relation = ANY (ARRAY['origin'::text, 'same_event_repeated'::text, 'same_event_update'::text,
                                  'same_event_conflict'::text, 'same_event_unassessed'::text, 'uncertain'::text])));

CREATE FUNCTION validate_routing_decision() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.revision_analysis a
        JOIN public.document d ON d.document_id = a.document_id
        WHERE a.analysis_id = NEW.analysis_id
          AND a.revision_id = NEW.revision_id
          AND d.is_sample = NEW.is_sample
    ) THEN
        RAISE EXCEPTION 'event_routing_decision must match its analysis revision and sample/live partition';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TABLE event_routing_decision (
    decision_id uuid DEFAULT gen_random_uuid() NOT NULL,
    analysis_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    is_sample boolean NOT NULL,
    attempt integer NOT NULL,
    mode text NOT NULL,
    classifier text,
    feature_version text,
    thresholds jsonb DEFAULT '{}'::jsonb NOT NULL,
    segments jsonb DEFAULT '[]'::jsonb NOT NULL,
    llm jsonb DEFAULT '{}'::jsonb NOT NULL,
    stats jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT event_routing_decision_pkey PRIMARY KEY (decision_id),
    CONSTRAINT event_routing_decision_attempt_key UNIQUE (analysis_id, attempt),
    CONSTRAINT event_routing_decision_analysis_id_fkey FOREIGN KEY (analysis_id) REFERENCES revision_analysis(analysis_id),
    CONSTRAINT event_routing_decision_revision_id_fkey FOREIGN KEY (revision_id) REFERENCES document_revision(revision_id),
    CONSTRAINT event_routing_decision_attempt_check CHECK ((attempt >= 1)),
    CONSTRAINT event_routing_decision_llm_check CHECK ((jsonb_typeof(llm) = 'object'::text)),
    CONSTRAINT event_routing_decision_mode_check CHECK ((mode = ANY (ARRAY['off'::text, 'shadow'::text, 'enforce'::text]))),
    CONSTRAINT event_routing_decision_segments_check CHECK ((jsonb_typeof(segments) = 'array'::text)),
    CONSTRAINT event_routing_decision_stats_check CHECK ((jsonb_typeof(stats) = 'object'::text)),
    CONSTRAINT event_routing_decision_thresholds_check CHECK ((jsonb_typeof(thresholds) = 'object'::text))
);
CREATE INDEX event_routing_decision_budget_idx ON event_routing_decision USING btree (is_sample, created_at);
CREATE INDEX event_routing_decision_revision_idx ON event_routing_decision USING btree (revision_id);
CREATE TRIGGER event_routing_decision_immutable BEFORE UPDATE ON event_routing_decision
    FOR EACH ROW EXECUTE FUNCTION reject_snapshot_update();
CREATE TRIGGER event_routing_decision_partition_check BEFORE INSERT ON event_routing_decision
    FOR EACH ROW EXECUTE FUNCTION validate_routing_decision();

CREATE OR REPLACE VIEW current_live_event AS
 SELECT e.event_id,
    e.title,
    e.status,
    e.event_facts,
    e.flags,
    min(m.source_time) AS first_source_time,
    max(m.source_time) AS last_source_time,
    count(DISTINCT m.document_id) AS documents,
    count(DISTINCT m.document_id) FILTER (WHERE (m.record_type = 'news_article'::text)) AS articles,
    count(DISTINCT m.document_id) FILTER (WHERE (m.record_type = 'sec_filing'::text)) AS filings,
    count(DISTINCT m.publisher_id) AS publishers,
    bool_or((m.relation = 'same_event_update'::text)) AS has_updates,
    bool_or((m.relation = 'same_event_conflict'::text)) AS has_conflicts,
    ( SELECT count(*) AS count
           FROM event_membership p
          WHERE ((p.event_id = e.event_id) AND (p.state = 'pending'::text))) AS pending_relations,
    e.profile_id,
    e.pipeline_version,
    count(*) FILTER (WHERE (m.relation = 'same_event_unassessed'::text)) AS unassessed_claims
   FROM (news_event e
     JOIN current_event_membership m ON ((m.event_id = e.event_id)))
  WHERE ((e.is_sample = false) AND (e.status <> 'superseded'::text))
  GROUP BY e.event_id;
