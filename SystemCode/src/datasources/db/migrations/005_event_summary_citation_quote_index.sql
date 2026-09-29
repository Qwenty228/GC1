-- Additive migration: several verified quotes per (summary, statement, text unit).
--
-- A statement may cite one text unit with more than one quote (say, one sentence
-- for the price and another for the closing date). The key used to be
-- (summary_id, statement_key, unit_id), so only one quote per unit could be
-- stored and the others were dropped. `quote_index` numbers the distinct quotes
-- of one (summary, statement, unit) from 1, and the key becomes
-- (summary_id, statement_key, unit_id, quote_index).
--
-- Migration history note
--   On stock_news this change was first applied by hand: the column, the check
--   `event_summary_citation_quote_index_check` and the key
--   `event_summary_citation_quote_position_key`. Every statement below is
--   conditional, so there it changes nothing but the comments, and on a database
--   that reached 004 through the migration files it performs the change.
--
-- Existing citations keep their content and get quote_index = 1 (the column
-- default). Adding a column is DDL, so the row-immutability trigger
-- (`event_summary_citation_immutable`) is neither fired nor changed. Nothing is
-- dropped except the superseded three-column key, which is found by its columns
-- because PostgreSQL generated its name.

ALTER TABLE event_summary_citation ADD COLUMN IF NOT EXISTS quote_index integer NOT NULL DEFAULT 1;

DO $$
DECLARE
    old_key text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'public.event_summary_citation'::regclass AND contype = 'c'
                      AND (conname = 'event_summary_citation_quote_index_check'
                           OR pg_get_constraintdef(oid) = 'CHECK ((quote_index >= 1))')) THEN
        ALTER TABLE event_summary_citation
            ADD CONSTRAINT event_summary_citation_quote_index_check CHECK (quote_index >= 1);
    END IF;

    -- The new key first, so the table is never without one.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint c
                    WHERE c.conrelid = 'public.event_summary_citation'::regclass AND c.contype = 'u'
                      AND ARRAY(SELECT a.attname::text FROM pg_attribute a
                                 WHERE a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey) ORDER BY a.attname)
                          = ARRAY['quote_index', 'statement_key', 'summary_id', 'unit_id']) THEN
        ALTER TABLE event_summary_citation ADD CONSTRAINT event_summary_citation_quote_position_key
            UNIQUE (summary_id, statement_key, unit_id, quote_index);
    END IF;

    FOR old_key IN
        SELECT c.conname FROM pg_constraint c
         WHERE c.conrelid = 'public.event_summary_citation'::regclass AND c.contype = 'u'
           AND ARRAY(SELECT a.attname::text FROM pg_attribute a
                      WHERE a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey) ORDER BY a.attname)
               = ARRAY['statement_key', 'summary_id', 'unit_id']
    LOOP
        EXECUTE format('ALTER TABLE event_summary_citation DROP CONSTRAINT %I', old_key);
    END LOOP;
END
$$;

COMMENT ON TABLE event_summary_citation IS
    'Statement-level evidence: each distinct verbatim quote a summary statement cites from a text unit.';
COMMENT ON COLUMN event_summary_citation.quote_index IS
    'Position (from 1) of this quote among the distinct verified quotes one statement cites from one text unit, in '
    'the order the model gave them. Quotes identical after the validator''s quote normalization are stored once.';
