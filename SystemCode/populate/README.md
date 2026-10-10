# populate/

Data that goes *into* the database, as opposed to [`migrate/`](../migrate/), which
defines its structure. Nothing here is loaded by `make db`; each dataset has its own
command.

| Folder | Loaded into | Command |
| --- | --- | --- |
| `event_grouping/` | `evaluation.fixture_article` (from `migrate/006_evaluation_fixture.sql`) | `make db-eval-load` |

## event_grouping/

Labelled Alpaca-format articles for testing event grouping, all in one flat folder.
Each file is one article, named by the team convention:

```
test_{number}_{type}_group_{group}.json      e.g. test_104_conflict_group_041.json
```

* `type` is the expected relation: `original`, `repeat`, `update`, `conflict` or `different`.
* `group` is the expected event: files sharing a group number describe one event.
* The file holds the article exactly in Alpaca's REST shape (`id`, `headline`,
  `summary`, `content`, `author`, `created_at`, `updated_at`, `url`, `symbols`,
  `source`, `images`).

The file name is the answer key. Feed only the file *contents* to the pipeline;
never the name or the `evaluation.fixture_article` columns derived from it.

```bash
make db                                           # migrates if needed, then loads new/changed files
uv run datasources-db load-evaluation --dry-run   # what would be inserted / updated (read-only)
make db-eval-load                                 # just the load; repeatable, never deletes
uv run datasources-db load-evaluation --force     # rewrite every row (logged as 'reload')
```

A new file is inserted, an edited file updates its row (by checksum), and an
unchanged file is skipped, so running it again changes nothing. Every insert,
update or reload is appended to `evaluation.load_log`, which is never edited.
`make db FORCE=1` rebuilds the whole database and loads everything again. Test numbers and article ids must be unique across the
folder; the loader stops before writing anything if they are not.
