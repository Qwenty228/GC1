# populate/

Data that goes *into* the database. [`migrate/`](../migrate/) defines the structure
(tables, columns, constraints); `populate/` fills it. `make db` runs both, in that
order. See [`migrate/summary.md`](../migrate/summary.md) for the full picture.

| File | Fills | Source |
| --- | --- | --- |
| `001_evaluation_fixtures.sql` | `evaluation.fixture_file` (answer key: which test file, expected type/group) | generated from `event_grouping/*.json` by `uv run datasources-db export-evaluation`; committed |

## Rules for a populate script

* **Name:** `NNN_short_description.sql`, numbered after the last file. Files run in
  filename order, so a script may rely on data from lower numbers.
* **Data only:** `INSERT` (or `UPDATE`) into tables that `migrate/` created. No
  `CREATE`/`ALTER`/`DROP`: structure changes go in a new `migrate/` file.
* **Safe to run twice:** every `INSERT` needs `ON CONFLICT ... DO NOTHING` (or
  `DO UPDATE` when a corrected file should overwrite). An edited file runs again.
* **No deletes** of data other people rely on. Never `TRUNCATE`.
* Top-level `*.sql` only: subfolders (like `event_grouping/`) are source data and
  are never executed.

Exporting from your own PostgreSQL database in that form:

```bash
pg_dump --data-only --inserts --on-conflict-do-nothing --no-owner --no-privileges \
        --table=company --table=company_symbol  your_db  > populate/002_companies.sql
```

## How `make db` runs them

`uv run datasources-db populate` (part of `make db`):

| File state | What happens |
| --- | --- |
| new | runs, one row appended to `populate_log` |
| edited (checksum changed) | runs again, another row appended |
| unchanged | skipped |

`populate_log` is append-only (filename, checksum, time per run), so running
`make db` twice changes nothing. `uv run datasources-db populate --dry-run` shows
what would run; `--force` runs every file again; `make db FORCE=1` rebuilds the
whole database and runs everything from scratch.

## event_grouping/

Labelled Alpaca-format articles for testing event grouping, one flat folder,
named by the team convention:

```
test_{number}_{type}_group_{group}.json      e.g. test_104_conflict_group_041.json
```

* `type`: expected relation, `original`, `repeat`, `update`, `conflict` or `different`.
* `group`: expected event; files sharing a group number describe one event.
* `batch`: five consecutive test numbers (001-005 = batch 1); generated in the table.
* Contents: the article exactly in Alpaca's REST shape. The articles stay in these
  files; the pipeline reads them through its file input (`datasources-ingest
  process-file`), like real news. The database only stores the answer key.

The file name is the answer key. Feed only the file *contents* to the pipeline.

Adding a batch (or renaming/editing a file):

```bash
uv run datasources-db export-evaluation   # rewrites 001_evaluation_fixtures.sql from this folder
git add populate/                         # commit the JSON and the SQL together
make db                                   # 001 changed, so it runs again and loads the new rows
```

A test fails while the committed SQL does not match the folder. The number `001`
is taken by this file; other data scripts start at `002`.
