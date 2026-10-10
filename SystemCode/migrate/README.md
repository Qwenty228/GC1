# migrate/

The PostgreSQL schema, as ordered SQL migrations. `uv run datasources-db migrate`
(`make db-migrate`) runs each file once, in filename order, and records it with its
SHA-256 in `schema_migration`.

| File | Adds |
| --- | --- |
| `000_base_schema.sql` | Baseline: the project ERD, `CREATE EXTENSION vector`, reference rows |
| `001_document_revision_source.sql` | Revision source tracking |
| `002_semantic_event_grouping.sql` | Embeddings + event grouping (pgvector) |
| `004_event_summary.sql` | Event summaries |
| `005_event_summary_citation_quote_index.sql` | Citation quote index |
| `006_evaluation_fixture.sql` | `evaluation` schema: labelled test articles (filled from `populate/`) |
| `007_evaluation_load_log.sql` | Append-only log of evaluation fixture loads |

There is no `003`: it came from a reverted branch (see `KNOWN_UNFILED` in
`src/datasources/db/migrate.py`).

Rules:

* **Never edit a file that has been applied.** Migrate compares checksums and stops
  on a changed file. Add a new, higher-numbered file instead.
* Every file runs in its own transaction; nothing here drops tables or deletes data.
* After adding a migration, regenerate `src/datasources/db/schema_expected.json`
  (`DATASOURCES_UPDATE_SCHEMA_MANIFEST=1 uv run pytest tests/test_migrations.py`) so
  `datasources-db verify` knows the new structure.

Quick start with the shared Docker database (from `SystemCode/`):

```bash
cp .env.example .env      # set PGPASSWORD
make db                   # Docker PostgreSQL 18 + pgvector, applies pending files, prints the log
make db-verify            # expect "Result: OK"
```
