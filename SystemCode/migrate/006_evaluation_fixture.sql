-- Additive migration: the `evaluation` schema, the answer key for labelled test files.
--
-- The test articles themselves stay as JSON files in `populate/event_grouping/` and
-- reach the pipeline the way real news does, through its file input
-- (`datasources-ingest process-file`). This table only says which files exist and what
-- the pipeline is expected to do with each, so results can be compared against it.
-- It lives in its own PostgreSQL schema: the pipeline never reads it, and
-- `datasources-db verify` compares only `public`. `populate/001_evaluation_fixtures.sql`
-- fills it.
--
-- The team's file name carries the expected answer:
--   test_{number}_{type}_group_{group}.json   e.g. test_104_conflict_group_041.json
-- Never pass the file name or these columns to the model, only the file's contents.

CREATE SCHEMA IF NOT EXISTS evaluation;

CREATE TABLE IF NOT EXISTS evaluation.fixture_file (
    filename       text PRIMARY KEY,
    test_number    integer NOT NULL UNIQUE CHECK (test_number > 0),
    batch          integer GENERATED ALWAYS AS ((test_number - 1) / 5 + 1) STORED,
    fixture_type   text NOT NULL
                   CHECK (fixture_type IN ('original', 'repeat', 'update', 'conflict', 'different')),
    expected_group integer NOT NULL CHECK (expected_group > 0),
    file_checksum  text NOT NULL,
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fixture_file_batch_idx ON evaluation.fixture_file (batch);
CREATE INDEX IF NOT EXISTS fixture_file_expected_group_idx ON evaluation.fixture_file (expected_group);

COMMENT ON SCHEMA evaluation IS 'Answer keys for evaluating the pipeline; never read by the pipeline itself';
COMMENT ON TABLE evaluation.fixture_file IS
    'One row per labelled test file in populate/event_grouping/: which file, and the expected answer from its name';
COMMENT ON COLUMN evaluation.fixture_file.batch IS
    'Batch of five consecutive test numbers (001-005 = 1, 006-010 = 2, ...); each batch is evaluated on its own';
COMMENT ON COLUMN evaluation.fixture_file.fixture_type IS
    'Expected relation, from the file name: original | repeat | update | conflict | different';
COMMENT ON COLUMN evaluation.fixture_file.expected_group IS
    'Expected event, from the file name: files sharing a number describe one event';
COMMENT ON COLUMN evaluation.fixture_file.file_checksum IS
    'SHA-256 of the JSON file (line endings normalized): a changed file is detectable, and its row is updated';
