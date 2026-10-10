-- Additive migration: the `evaluation` schema for labelled test articles.
--
-- Alpaca-format test articles used to evaluate event grouping live apart from live
-- data, in their own PostgreSQL schema. Nothing in the pipeline reads it, and
-- `datasources-db verify` compares only `public`, so this file does not change the
-- expected-schema manifest. `datasources-db load-evaluation` fills it from
-- `populate/event_grouping/`.
--
-- The team's file name carries the expected answer:
--   test_{number}_{type}_group_{group}.json   e.g. test_104_conflict_group_041.json
-- It is split into columns here so answers can be queried and compared. The
-- article itself is `payload`, exactly as the file holds it; only `payload` may
-- ever be fed to the pipeline, never the file name or the other columns.

CREATE SCHEMA IF NOT EXISTS evaluation;

CREATE TABLE IF NOT EXISTS evaluation.fixture_article (
    filename       text PRIMARY KEY,
    test_number    integer NOT NULL UNIQUE CHECK (test_number > 0),
    batch          integer GENERATED ALWAYS AS ((test_number - 1) / 5 + 1) STORED,
    fixture_type   text NOT NULL
                   CHECK (fixture_type IN ('original', 'repeat', 'update', 'conflict', 'different')),
    expected_group integer NOT NULL CHECK (expected_group > 0),
    article_id     bigint NOT NULL UNIQUE,
    published_at   timestamptz,
    payload        jsonb NOT NULL,
    file_checksum  text NOT NULL,
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fixture_article_expected_group_idx ON evaluation.fixture_article (expected_group);
CREATE INDEX IF NOT EXISTS fixture_article_batch_idx ON evaluation.fixture_article (batch);

COMMENT ON SCHEMA evaluation IS 'Labelled test data for evaluating the pipeline; never read by the pipeline itself';
COMMENT ON TABLE evaluation.fixture_article IS
    'One Alpaca-format test article per file in populate/event_grouping/, with the expected answer from its file name';
COMMENT ON COLUMN evaluation.fixture_article.batch IS
    'Batch of five consecutive test numbers (001-005 = 1, 006-010 = 2, ...); each batch is evaluated on its own';
COMMENT ON COLUMN evaluation.fixture_article.fixture_type IS
    'Expected relation, from the file name: original | repeat | update | conflict | different';
COMMENT ON COLUMN evaluation.fixture_article.expected_group IS
    'Expected event, from the file name: articles sharing a number belong to one event';
COMMENT ON COLUMN evaluation.fixture_article.article_id IS 'payload->id: the Alpaca id, or a reserved 99... test id';
COMMENT ON COLUMN evaluation.fixture_article.published_at IS 'payload->created_at';
COMMENT ON COLUMN evaluation.fixture_article.file_checksum IS
    'SHA-256 of the file (line endings normalized); a changed file updates its row on the next load';
