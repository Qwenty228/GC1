-- Additive migration: an append-only log of evaluation fixture loads.
--
-- `datasources-db load-evaluation` (run by `make db`) appends one row per file it
-- inserts or updates in evaluation.fixture_article; unchanged files add nothing, so
-- running it again leaves the log as it was. `--force` rewrites every file and logs
-- each as 'reload'. Rows are never changed or deleted (trigger below), like
-- schema_migration is for migrate.

CREATE TABLE IF NOT EXISTS evaluation.load_log (
    log_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    filename      text NOT NULL,
    file_checksum text NOT NULL,
    action        text NOT NULL CHECK (action IN ('insert', 'update', 'reload')),
    loaded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS load_log_filename_idx ON evaluation.load_log (filename, loaded_at);

CREATE OR REPLACE FUNCTION evaluation.load_log_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'evaluation.load_log is append-only';
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'load_log_append_only'
                    AND tgrelid = 'evaluation.load_log'::regclass) THEN
        CREATE TRIGGER load_log_append_only BEFORE UPDATE OR DELETE ON evaluation.load_log
            FOR EACH ROW EXECUTE FUNCTION evaluation.load_log_append_only();
    END IF;
END;
$$;

COMMENT ON TABLE evaluation.load_log IS
    'Append-only: one row per fixture file inserted, updated or force-reloaded by load-evaluation';
