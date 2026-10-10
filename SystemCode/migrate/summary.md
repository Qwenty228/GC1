# Database handoff: `migrate/`, `populate/` and `make db`

Branch `feature/testing-metric`, October 2026. Read this before reviewing/merging the branch,
changing `migrate/`, adding data to `populate/`, or touching `compose.yaml` / the `db` Makefile
targets. You own the ER diagram, so the schema is yours to reshape. This note lists everything
the branch changes, what the pipeline and the evaluation depend on, and the conventions that
keep `make db` working for everyone.

## TL;DR

* `make db` sets up everything and is safe to run any time: it starts Docker
  Postgres, applies new `migrate/*.sql` files, runs new or changed
  `populate/*.sql` files, and prints both logs. Running it twice changes nothing.
* `migrate/` = **structure** (DDL). Applied files are never edited: change the
  schema with a new, higher-numbered file.
* `populate/` = **data** (INSERTs). Scripts must be safe to run twice; an edited
  script runs again.
* `make db FORCE=1` drops and recreates the database, then rebuilds it from
  `migrate/` + `populate/`. It deletes all data in that database and asks you to
  type its name first.
* Test against a database whose name contains `test`, never `stock_news`.
* No pipeline logic changed (see below): database tooling, an evaluation harness and one new LLM
  backend were added, and `process-file` now also accepts a single article object.

## What this branch changes

### Database tooling

* `make db`: one shot and idempotent. Container up (only if needed), `migrate`, `populate`, then the logs.
* `make db-log`: prints `schema_migration` and `populate_log`.
* `make db FORCE=1` (calls `make db-reset`): destructive drop and recreate, then migrate + populate. Asks you
  to type the database name; `CONFIRM=<db>` answers for scripts.
* `make db-populate`: runs only the `populate/` step.
* `make db-rerun FILE=<migrate file>`: recreates objects dropped by hand. Re-runs one applied `migrate/` file
  in a single transaction (a re-runnable file only recreates what is missing), then `populate --force`.
* Every db target uses the app's database: `DBNAME` in the Makefile = `PGDATABASE` from the shell, else `.env`,
  else `stock_news`.
* Also new in the Makefile: `make eval EXP=<file>` and `make eval-compare`. `README.md` and `migrate/README.md` were updated to match.

### Schema: `migrate/006_evaluation_fixture.sql`

* Schema `evaluation`, table `evaluation.fixture_file` (answer key only): `filename`, `test_number`,
  generated `batch`, `fixture_type`, `expected_group`, `file_checksum` (plus `loaded_at`, `updated_at`).
* Not in `public`, so `datasources-db verify` / `make db-verify` ignores it. Additive and re-runnable.

### `populate/` runner (`src/datasources/db/populate.py`)

* Numbered `populate/*.sql` data scripts, top level only. A script runs when new or when its checksum changed.
  Every run is appended to `populate_log`.
* `populate_log` is bookkeeping like `schema_migration`: `schema.py` gains `BOOKKEEPING_TABLES`
  (`schema_migration`, `populate_log`), and `migrate.py` has a one-line change to use it.
* Commands: `uv run datasources-db populate [--dry-run] [--force] [--json]`.

### Evaluation data

* `populate/event_grouping/*.json`: 125 Alpaca-format test articles named
  `test_{number}_{type}_group_{group}.json`.
* `populate/001_evaluation_fixtures.sql` is generated from them by `uv run datasources-db export-evaluation`
  and committed. A test fails if it is stale. `001` is reserved: your scripts start at `002`.
* Module `src/datasources/db/evaluation.py` (`FixtureType` enum, name parsing, export).

### Evaluation harness

* `src/datasources/evaluation/`: `consolidation.py` (`run()`, `run_experiment()`), `metrics.py`,
  `cli.py` (`datasources-eval compare`, registered in `pyproject.toml`).
* Experiment files and results live in `SystemCode/evaluation/` ([README](../evaluation/README.md)).
* It runs the **unchanged** production graph (`ingest/graph.py`), feeding each test file through the pipeline's
  file input: `_process_alpaca_payload` in `ingest/cli.py`, exactly what `datasources-ingest process-file` runs.
  The experiment's comparator is injected with `build_runtime(..., comparator=...)`. No summarizer is attached.
* Each batch of 5 runs in its own throwaway database `stock_news_eval_<name>_bNN_<id>_test`.

### `src/datasources/llm_openai.py`

* OpenAI-compatible comparison and summary backends (OpenAI, Gemini, Groq, Ollama, LM Studio, vLLM)
  using the production prompts and output schemas.
* Not selectable through `.env` for the live pipeline. Only experiment files use it.
  `SIMILARITY_COMPARISON_BACKEND` still accepts `anthropic` / `mock` only.

### `process-file` accepts a single article (`src/datasources/ingest/adapters.py`)

* `unwrap_payload(..., provider="alpaca")` gains one shape, `alpaca_article`: one article object on its own
  (or one WebSocket `"T": "n"` message). Before, `datasources-ingest process-file x.json --provider alpaca`
  refused a file holding a single article ("payload is not a recognised Alpaca shape"), which is what every
  test file is. All existing shapes behave as before; dicts without an `id` and a headline/summary/content
  are still refused.

### Tests and config

* `tests/test_evaluation_fixtures.py` (new), `tests/test_evaluation_harness.py` (new),
  `tests/test_ingest_graph.py` (+1 test for the single-article shape).
* `tests/test_migrations.py`: `PINNED` checksum for `006`, and the two lists that count pending migration files
  (5 -> 6 `execute` actions; the adopt-baseline lists gain `006`).
* `.env.example`: commented `GEMINI_API_KEY` / `OPENAI_API_KEY` for experiments.

### Source files changed outside the new modules

`git diff --stat main..HEAD -- src` lists exactly 11 files:

| File | Change |
| --- | --- |
| `ingest/adapters.py` | the `alpaca_article` shape (a single article object), +4 lines |
| `db/cli.py` | `populate` and `export-evaluation` subcommands (+48) |
| `db/migrate.py` | 1 line: `BOOKKEEPING_TABLE` -> `BOOKKEEPING_TABLES` |
| `db/schema.py` | `BOOKKEEPING_TABLES`; `snapshot()` and `managed_tables()` use it |
| `db/populate.py`, `db/evaluation.py` | new |
| `evaluation/__init__.py`, `cli.py`, `consolidation.py`, `metrics.py` | new |
| `llm_openai.py` | new |

Apart from that one adapter shape, `ingest/`, `similarity/` and `summary/` are untouched.

### History note

The branch history was rewritten once (force-pushed) to remove a 550 KB generated SQL file. Anyone who pulled
the branch before should reset their local branch to origin.

## What would break evaluation if you change the pipeline

Keep these in mind, or update `src/datasources/evaluation/consolidation.py` and its tests together:

* `build_runtime(database, settings, *, embedder=None, comparator=None)` in `similarity/pipeline.py`:
  the injection point for the experiment's model and embedder.
* The comparator contract: an object with `compare(request) -> ComparisonCall` (`similarity/comparison.py`),
  and `build_output(...)` which the template experiment uses.
* `_process_alpaca_payload(service, payload, *, source, restrict_symbols)` in `ingest/cli.py` (the
  `process-file` path, which calls `unwrap_payload`, `IngestionService.alpaca_items` and `process_item`).
  The runner calls it once per test file.
* `ALPACA_NAMESPACE` (`db/repository.py`) and the columns read by `PREDICTIONS_SQL`:
  `document.source_namespace`, `source_record_id`, `current_revision_id`;
  `revision_analysis.status`, `last_error`;
  `event_membership.event_id`, `relation`, `state`, `created_at`;
  `event_comparison.usage` (JSON with `input_tokens` / `output_tokens`).
* The relation values `origin`, `uncertain`, and the five comparison labels the scorer maps to.
* `coverage_companies` / `register_companies` (`db/companies.py`): each test database registers the coverage
  companies before ingesting, as production has them.
* The whole `migrate/` chain must build an empty database, since every batch migrates a fresh one.
  After changing `migrate/` or the pipeline, run one batch:
  `uv run python evaluation/consolidation/experiments/baseline_rule.py`.

## Layout

```
SystemCode/
  compose.yaml                    Postgres 18 + pgvector in Docker (reads PG* from .env)
  .env                            PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (not committed)
  Makefile                        make db, db-log, db-reset, db-rerun, db-populate, db-verify, db-psql, ...
  migrate/                        schema, run once per file, logged in schema_migration
    000_base_schema.sql           baseline: the original ERD + pgvector + reference rows
    001, 002, 004, 005            pipeline additions (there is no 003: a reverted branch)
    006_evaluation_fixture.sql    `evaluation` schema for labelled test data
    README.md                     migrate rules
    summary.md                    this file
  populate/                       data, run when new or changed, logged in populate_log
    001_evaluation_fixtures.sql   answer key for the 125 test files (generated by export-evaluation, committed)
    event_grouping/*.json         the labelled test articles themselves (never executed)
    README.md                     populate rules
  src/datasources/db/
    migrate.py, populate.py       the two runners
    schema_expected.json          what `make db-verify` compares the public schema against
  src/datasources/evaluation/    the evaluation harness (see "What this branch changes")
  evaluation/                    experiment files + saved results (own README)
```

## What `make db` does

```
docker compose up -d --wait postgres     # starts the container only if needed
uv run datasources-db migrate            # pending migrate/*.sql, each in a transaction
uv run datasources-db populate           # new/changed populate/*.sql, each in a transaction
make db-log                              # schema_migration + populate_log
```

| | `migrate/` | `populate/` |
| --- | --- | --- |
| Contains | `CREATE` / `ALTER` (structure) | `INSERT` / `UPDATE` (data) |
| Runs a file | once, ever | when new, or when its checksum changed |
| Log table | `schema_migration` (filename, checksum, method, applied_at) | `populate_log`, append-only (filename, checksum, applied_at), one row per run |
| Edited file | **refused**: migrate stops on a changed checksum | **runs again** |
| Dry run | `uv run datasources-db migrate --dry-run` | `uv run datasources-db populate --dry-run` |
| Force | `make db FORCE=1` (full rebuild) | `uv run datasources-db populate --force` (rerun all) |
| Recreate a dropped table | `make db-rerun FILE=<file>` | (included: it ends with `populate --force`) |

`make db FORCE=1` = drop database, create it empty, then `migrate` + `populate`
from scratch. `CONFIRM=<dbname>` answers the confirmation for scripts. All `db`
targets use the database the app uses: `PGDATABASE` from your shell, else `.env`.

**One-way:** both runners check the logs ("has this file run?"), never whether the
tables still exist. Don't drop tables by hand: the log still says done, so `make db`
won't recreate them. Recover with `make db-rerun FILE=<the migrate/ file>` (re-runs it in one
transaction, then `populate --force`), or rebuild with `make db FORCE=1`. Write new migrations
re-runnable (`IF NOT EXISTS`) so `db-rerun` works for them too.

## Changing the schema (`migrate/`)

1. **Add** `migrate/007_your_change.sql` (next free number). Never edit or delete a
   file that is pushed: other people's `schema_migration` has its checksum and
   `migrate` refuses a mismatch.
2. Prefer additive, re-runnable DDL: `CREATE TABLE IF NOT EXISTS`,
   `ADD COLUMN IF NOT EXISTS`, conditional `DO $$ ... $$` blocks (see `005`).
3. Regenerate the expected schema so `make db-verify` knows your change:
   `DATASOURCES_UPDATE_SCHEMA_MANIFEST=1 uv run pytest tests/test_migrations.py`.
4. Add your file's checksum to `PINNED` in `tests/test_migrations.py`, and extend
   the two lists there that name every pending file (the tests tell you which).
5. Run `PGDATABASE=stock_news_yourname_test uv run pytest tests/test_migrations.py`.

### What the pipeline code depends on

These public tables are read and written by the ingestion, similarity and summary
code (`src/datasources/db/repository.py`, `similarity/store.py`, `summary/store.py`,
`db/read.py`):

`provider`, `publisher`, `company`, `company_symbol`, `document`,
`document_revision`, `document_revision_source`, `revision_company`,
`sec_filing_details`, `ingestion_state`, `text_unit`, `embedding`,
`embedding_profile`, `revision_analysis`, `news_event`, `event_membership`,
`event_membership_unit`, `event_comparison`, `retrieval_candidate`,
`event_summary`, `event_summary_citation`, `event_summary_job`,
`event_summary_attempt`, `event_conflict_review`, `event_review_decision`.

Adding tables or nullable columns is safe. **Renaming or dropping** any of the
above, changing a column's type, or tightening a constraint breaks the pipeline
until the matching Python is changed too. `grep -rn "<table_name>" src/` finds the
code; the test suite catches most breakage. Reference rows the code requires:
`provider` must contain `sec_edgar` and `alpaca` (seeded by `000`).

If your ERD differs a lot from `000`, it is fine to restructure, but do it as new
numbered files (or agree a reset with the team: everyone then runs
`make db FORCE=1`, which loses local data).

## Adding data (`populate/`)

* `populate/NNN_description.sql`, starting at `002` (`001` is the generated evaluation
  file); runs in name order.
* Data only, into tables `migrate/` created. Structure changes go in `migrate/`.
* Safe to run twice: `INSERT ... ON CONFLICT ... DO NOTHING` (or `DO UPDATE`).
* No `TRUNCATE`, no deleting data others rely on.
* Export from your own database in that shape:

```bash
pg_dump --data-only --inserts --on-conflict-do-nothing --no-owner --no-privileges \
        --table=company --table=company_symbol  your_db  > populate/002_companies.sql
```

Order matters for foreign keys: a file must come after the files whose rows it
references.

## The evaluation data (already in)

* The test articles are JSON files in `populate/event_grouping/`
  (`test_{number}_{type}_group_{group}.json`; the name is the answer key). They stay
  files: an evaluation run feeds each one to the pipeline through its file input (the code
  `datasources-ingest process-file` runs), then compares the result. You can also feed one by hand:
  `uv run datasources-ingest process-file populate/event_grouping/<file>.json --provider alpaca`.
* `migrate/006_evaluation_fixture.sql` creates schema `evaluation` with table
  `evaluation.fixture_file`, the answer key only: `filename`, `test_number`,
  generated `batch` (five tests per batch), `fixture_type`, `expected_group`,
  `file_checksum`. No article content. Own schema, so the pipeline never sees it
  and `make db-verify` (which checks `public`) ignores it.
* `populate/001_evaluation_fixtures.sql` fills it. Generated from the folder by
  `uv run datasources-db export-evaluation` (run it after adding a batch, commit
  the result); a test fails if the committed SQL is stale.

## Evaluation runs

`evaluation/` (see its README) scores the pipeline on the labelled batches with a
swappable model: `uv run python evaluation/consolidation/experiments/<file>.py`. Each
batch runs in its own throwaway `stock_news_eval_..._test` database built from
`migrate/` + the coverage companies, so a schema change that breaks the pipeline also
breaks evaluation; run one batch after changing `migrate/`.

## Testing without touching real data

* `stock_news` is the shared working database (news, events, summaries). The test
  suite refuses to run destructive tests against it, or against any database whose
  name lacks `test` or that holds live documents.
* Make one (use your PGUSER if it is not `postgres`): `docker compose exec postgres createdb -U postgres stock_news_yourname_test`,
  then `PGDATABASE=stock_news_yourname_test make db` and
  `PGDATABASE=stock_news_yourname_test uv run pytest`.
* Add `--ignore=tests/test_launcher.py` for a clean run (see Open items).

## Open items / known issues

* `tests/test_launcher.py` can hang or fail at interpreter shutdown (a stdin lock). It does so without this
  branch too. Run `uv run pytest --ignore=tests/test_launcher.py` for a clean run.
* The OpenAI-compatible backend is not wired into `.env` for production (see above).
* Evaluation scores depend on the `SIMILARITY_*` retrieval settings in each person's `.env`
  (e.g. `SIMILARITY_MAX_CANDIDATE_EVENTS`, `SIMILARITY_LEXICAL_PLAUSIBLE_RANK`). Every result file records
  them and `datasources-eval compare` shows the two that matter most; the team should use the
  `.env.example` values. With them, even the always-"same event" baseline gets grouping F1 0.954 (most
  "different" articles never pass the 0.78 similarity gate), so the label macro F1 is the model comparison.
* Summarization evaluation is not built: it needs labelled data and agreed metrics
  (see `evaluation/summarization/README.md`).
* In the baseline run (always "same event"), 2 of the 100 scored articles (every article except the first of
  its batch) got no retrieval candidates, so no model was asked: grouping recall 0.96. Worth investigating in
  retrieval.

## What changed on this branch

`git log --oneline main..HEAD`, oldest first:

| Commit | Change |
| --- | --- |
| `02c7dc5` | Add one-shot `make db` target and migration log |
| `97146df` | Add evaluation schema and loader for labelled event-grouping fixtures |
| `e60b5e1` | Make `make db` idempotent for fixtures, log loads, add `FORCE=1` rebuild |
| `517a605` | Test data (the 125 files in `populate/event_grouping/`) |
| `0fa215c` | Run `populate/*.sql` as logged data scripts; add database handoff notes |
| `b79230c` | Generate the evaluation populate script on every run instead of committing it |
| `487f476` | Explain that `make db` trusts its logs, not table existence |
| `17d3c33` | Keep only the answer key in `evaluation.fixture_file`; commit the generated SQL |
| `e456446` | Add evaluation harness with plug-in experiment files and an OpenAI-compatible backend |
| `1429394` | Groq experiment; explain in the template that the pipeline calls the experiment |
| `1269d08` | Make every experiment file runnable on its own |
| `e880f94` | Rewrite the evaluation README for first-time users and complete this handoff |
| `b90c184` | Let `process-file` ingest a single article; feed evaluation through it |
| `0a47f44` | Handoff: list the latest commits |
| `2f733cb` | Explain the evaluation's purpose and scores; record retrieval settings per run; reference baseline rerun with `.env.example` settings |
| `fd64869` | README: conflict detection is how fake AI-generated news is caught |
| `57e9782` | Edit evaluation README (SEC filings as a future ground-truth test) |
| `5a57a25` | Add `make db-rerun` to recreate tables dropped by hand |
