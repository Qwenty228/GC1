# migrate/

Handoff and conventions for `migrate/`, `populate/` and `make db`: [summary.md](summary.md).

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
| `006_evaluation_fixture.sql` | `evaluation` schema: answer key for the labelled test files (filled from `populate/`) |

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

## Migration mechanics and legacy objects

`datasources-db migrate` works out what to do before it changes anything:

| Database | What migrate does |
| --- | --- |
| none of the repository's tables | a new database: runs every file from `000` |
| `000` recorded | runs the files not recorded yet |
| the base tables exist, `000` not recorded, every base object matches `000` | refuses until `--adopt-baseline`; then records `000` as `adopted` (never executed) and runs the rest |
| some base objects missing or different | refuses and lists them; nothing is recorded or applied |

Each file runs in its own transaction and is recorded in `schema_migration` with the SHA-256 of its text (line
endings normalized) and how it was recorded (`executed` / `adopted`); rows recorded before this have neither.
A recorded file whose text changed makes migrate stop: an applied migration is never edited, changes go into a
new file, and `tests/test_migrations.py` pins the committed files' checksums. Running `migrate` on an
up-to-date database changes nothing (it only reads). Nothing in the normal upgrade path drops a table, a
column or data.

`datasources-db verify` compares the database with `src/datasources/db/schema_expected.json`: snapshots of a
database built from the files alone (after `000`, and after every file), generated on PostgreSQL 18.6.
Definitions are compared as PostgreSQL prints them, so another major version may print some differently.
`tests/test_migrations.py` rebuilds both snapshots in a temporary database and fails when the file is out of
date; `DATASOURCES_UPDATE_SCHEMA_MANIFEST=1` regenerates it.

**Legacy objects.** `stock_news` (and databases copied from it) recorded `003_event_match_routing.sql` from a
branch whose code was reverted. What that branch left is unused by the current code, holds no rows there, and
is reported by `verify` and the migrate preview as legacy, never changed:

| Object | Note |
| --- | --- |
| table `event_routing_decision` (+ its indexes, triggers and `validate_routing_decision()`) | routing audit of the reverted local-classifier branch; 0 rows |
| column `event_comparison.cache_key` (+ index `event_comparison_cache_idx`) | always NULL |
| checks `event_comparison_label_check`, `event_comparison_method_check`, `event_membership_relation_check` | wider than required: also allow `same_event_unassessed` / `local_classifier`, which current code never writes |
| view `current_live_event` | an extra trailing column `unassessed_claims`, always 0 |

A new database built from the files has none of them. Removing them from `stock_news` would be a separate,
deliberate step; no migration does it.

## What the tables hold

### Table mapping

| Table | Holds | Alpaca | SEC EDGAR |
| --- | --- | --- | --- |
| `provider` | the API we fetched from | looked up by `code='alpaca'` | `code='sec_edgar'` |
| `document` | one row per **source record** | `source_namespace='news'`, `source_record_id`=article id | `source_namespace`=CIK10, `source_record_id`=accession |
| `document_revision` | one row per distinct payload | title, author, summary, body, `analysis_text`, timestamps, `provenance` | generated title, extracted text, `analysis_text` |
| `document_revision_source` | original payload + original HTML | article JSON as received | submissions row + primary-document HTML |
| `revision_company` | company links | resolved from `symbols` via `company_symbol` | issuer via `company.cik`, plus tickers |
| `sec_filing_details` | filing metadata | — | CIK, accession, form, dates, items, index/document URLs, exhibits |
| `publisher` | the **original publisher** | article `source` (e.g. `benzinga`) | not set (filings have an issuer, not a publisher) |

`provider` and `publisher` stay distinct: `alpaca` is where the article came
from, `benzinga` is who wrote it. `publisher.credibility_tier` keeps its
`unreviewed` default — no credibility score is invented, and an existing tier is
never overwritten.

The pipeline's content flags are mapped onto the schema's constrained values, so
a metadata-only record never looks like a fully extracted document:

| Pipeline | `content_status` | `content_scope` |
| --- | --- | --- |
| Alpaca, body present | `available` | `full_article` |
| Alpaca, summary only | `partial` | `title_summary_only` |
| Alpaca, headline only | `missing` | `title_summary_only` |
| Alpaca, nothing | `missing` | `metadata_only` |
| SEC, extraction ok | `available` | `primary_document_only` |
| SEC, extraction failed | `extraction_failed` | `primary_document_only` |

### Duplicate prevention in the database

Idempotency is enforced by the database, not by application logic:

| Constraint | Prevents |
| --- | --- |
| `document (provider_id, source_namespace, source_record_id, is_sample)` | a second row for the same source record |
| `document_revision (document_id, source_snapshot_hash, preprocessing_version)` | a second revision of an unchanged payload |
| `revision_company (revision_id, company_id)` primary key | duplicate company links |
| `sec_filing_details (revision_id)` primary key | duplicate filing details |

`source_snapshot_hash` is the pipeline's existing
`provenance.dedup.content_fingerprint` (a full SHA-256), so replaying an
unchanged payload conflicts and inserts nothing. A genuinely changed payload
inserts a **new revision** and `document.current_revision_id` moves to it; the
old revision is kept, and `document_revision` is immutable by trigger. An 8-K/A
has its own accession, so it is its own `document`. Documents are identified by
source identity only: semantic similarity (below) groups documents into events,
but never merges, replaces or deletes a document.

### Schema (migration 002, additive)

| Object | Purpose |
| --- | --- |
| `text_unit.search_tsv` + GIN index | Generated `to_tsvector('english', input_text)` for lexical retrieval |
| `revision_analysis` | Status per (revision, embedding profile, pipeline version): `completed` / `partial` / `pending` / `failed` / `skipped`, stage status, skip reason, evidence level, anchor, index coverage, limitations, usage, model + prompt version, attempts, sanitized last error, next retry. Mutable, because `document_revision` is immutable. |
| `news_event` | Event: partition (`is_sample`), analysis space (profile + pipeline version), status (`provisional` / `corroborated` / `superseded`), title, facts, flags, first/last source time, origin |
| `event_membership` + `event_membership_unit` | Revision → event: `origin` / `same_event_*` / `uncertain` (pending), one per (event, revision), with supporting units |
| `event_comparison` | Each decision: label, method (`llm` / `exact_content`), matching facts, differences, evidence from both sides, explanation, uncertainty, validation notes, model, prompt version, usage. Immutable. |
| `retrieval_candidate` | Semantic rank/similarity, lexical rank/score, fused score/rank, shortlist flag, not-compared reason |
| `current_event_membership`, `current_live_event`, `current_revision_analysis` | Views over current revisions only: a superseded revision is never an additional source |

Triggers reject any event, membership or comparison that would mix the sample
and live partitions or two analysis spaces. The existing tables and triggers
(`text_unit`, `embedding`, `embedding_profile`) are reused unchanged, and
publisher credibility tiers are not touched.

### Schema (migration 004, additive)

The database already recorded `003_event_match_routing.sql` from a reverted
branch, which had created `event_summary`, `event_summary_citation`,
`event_conflict_review` and `current_event_summary`. Migration 004 reuses those
unchanged (and creates them with the same definitions on a database without
003), then adds only:

| Object | Why |
| --- | --- |
| `event_summary.statements`, `.input_manifest`, `.limitations` | Stable statement ids and text; which revisions/units were given to the model (and which were omitted); evidence-selection limitations |
| `event_summary_job` | Durable, lease-protected summary work and retry state, independent of the ledger |
| `event_summary_attempt` | Every attempt with its output, including invalid/stale/blocked output that was never published (immutable) |
| `event_review_decision` | Audit of every review decision and membership correction (immutable) |
| `event_membership.state = 'detached'` | Widened check so a `different_events` correction keeps the row instead of deleting it |
| Triggers | Summary content immutable; citations immutable and in the event's sample/live partition; a resolved review is final |

`document_revision.summary_text` is not used: it belongs to one source revision.

**Migration 005** (`005_event_summary_citation_quote_index.sql`) adds
`event_summary_citation.quote_index` (default 1, `>= 1`) and replaces the key
`(summary_id, statement_key, unit_id)` with
`(summary_id, statement_key, unit_id, quote_index)`, so one statement can keep
several quotes from the same text unit. Every step is conditional: on
stock_news, where the change was first applied by hand, it only adds comments.
Existing citations keep their content with `quote_index = 1`; the immutability
triggers are untouched. Summaries published earlier keep the one quote per unit
they were stored with (their other quotes remain in `event_summary_attempt.output`).

## Inspecting the data (pgAdmin / psql)

### Inspecting stored news (pgAdmin / psql)

```sql
-- Everything currently stored, newest first (this view hides is_sample rows).
SELECT source_time, source_badge, api_provider, publisher_name, filing_issuer,
       title, content_status, original_url
  FROM current_live_document
 ORDER BY source_time DESC NULLS LAST
 LIMIT 50;

-- Stored NVIDIA 8-K filings with their extracted text length.
SELECT s.accession_number, s.form, s.filing_date, s.accepted_at, s.item_codes,
       length(r.body_text) AS extracted_chars,
       jsonb_array_length(s.exhibits) AS exhibit_links,
       s.primary_document_url
  FROM sec_filing_details s
  JOIN document_revision r ON r.revision_id = s.revision_id
  JOIN company c ON c.company_id = s.issuer_company_id
 WHERE c.cik = '0001045810'
 ORDER BY s.filing_date DESC;

-- Articles per publisher, with the credibility tier as stored (never invented).
SELECT p.name AS publisher, p.credibility_tier, count(*) AS articles
  FROM document_revision r
  JOIN publisher p ON p.publisher_id = r.publisher_id
 GROUP BY 1, 2 ORDER BY articles DESC;

-- DUPLICATE CHECK 1: two rows for the same source record. Must return 0 rows.
SELECT p.code, d.source_namespace, d.source_record_id, count(*) AS rows
  FROM document d JOIN provider p USING (provider_id)
 GROUP BY 1, 2, 3 HAVING count(*) > 1;

-- DUPLICATE CHECK 2: the same payload stored twice. Must return 0 rows.
SELECT document_id, source_snapshot_hash, preprocessing_version, count(*) AS rows
  FROM document_revision
 GROUP BY 1, 2, 3 HAVING count(*) > 1;

-- Documents that have more than one revision (a genuinely updated article).
SELECT d.source_record_id, count(*) AS revisions,
       min(r.retrieved_at) AS first_seen, max(r.retrieved_at) AS last_seen
  FROM document d JOIN document_revision r USING (document_id)
 GROUP BY 1 HAVING count(*) > 1 ORDER BY revisions DESC;

-- The original payload kept for one article, alongside the cleaned text.
SELECT d.source_record_id, r.title, left(r.body_text, 200) AS cleaned_text,
       s.raw_payload, s.raw_file_refs
  FROM document d
  JOIN document_revision r ON r.revision_id = d.current_revision_id
  JOIN document_revision_source s ON s.revision_id = r.revision_id
 WHERE d.record_type = 'news_article'
 LIMIT 1;

-- Semantic analysis coverage of current revisions ('not_analyzed' while similarity is off).
SELECT record_type, analysis_status, count(*) FROM current_revision_analysis
 WHERE NOT is_sample GROUP BY 1, 2 ORDER BY 1, 2;
```

### Inspecting events

```sql
-- Live events with the most independent documents (current revisions only).
SELECT title, status, documents, articles, filings, publishers,
       has_updates, has_conflicts, pending_relations, first_source_time
  FROM current_live_event ORDER BY documents DESC, first_source_time DESC LIMIT 20;

-- Everything linked to one event, with how each revision relates to it.
SELECT m.relation, m.state, d.record_type, r.title, r.published_at, c.label, c.explanation
  FROM event_membership m
  JOIN document_revision r ON r.revision_id = m.revision_id
  JOIN document d ON d.document_id = m.document_id
  LEFT JOIN event_comparison c ON c.comparison_id = m.comparison_id
 WHERE m.event_id = '<event uuid>' ORDER BY r.published_at;

-- Conflicting claims, with the evidence quoted from both sides.
SELECT e.title, c.differences, c.evidence FROM event_comparison c
  JOIN news_event e ON e.event_id = c.candidate_event_id
 WHERE c.label = 'same_event_conflict' AND NOT e.is_sample;

-- Analyses waiting for a retry, and why.
SELECT a.status, a.attempts, a.next_retry_at, a.last_error, r.title
  FROM revision_analysis a JOIN document_revision r ON r.revision_id = a.revision_id
 WHERE a.status IN ('pending', 'partial') ORDER BY a.next_retry_at NULLS FIRST;
```

