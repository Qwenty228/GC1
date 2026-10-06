# Data Sources

Node 1 of the pipeline collects **SEC EDGAR 8-K filings** and **Alpaca news** for a
configurable list of symbols (default `NVDA`), runs each record through a
**LangGraph** graph, writes one versioned JSON record per filing / article
revision under `output/`, and stores every record in the local **PostgreSQL**
`stock_news` database.

Optionally (`SIMILARITY_ENABLED=true`, off by default) the same graph compares
each new article or filing with the news already stored — local embeddings,
pgvector plus full-text retrieval, then an LLM comparison — **before** storing
it, and groups the records into *events*. See
[Semantic comparison and event grouping](#semantic-comparison-and-event-grouping).
No summarisation, sentiment or recommendations.

**Daily use:** `uv run datasources-dev` starts the web page, the API and the
collectors together; see [Local application](#local-application-one-command).

The original pieces still work as before:

* `uv run datasources-server`: web UI + read API only (no collectors)
* `uv run datasources edgar NVDA`: backend CLI
* `uv run datasources-news-stream`: standalone Alpaca WebSocket printer

## Setup

```powershell
uv sync --locked            # Python 3.13.7 + locked dependencies
Copy-Item .env.example .env # then fill in credentials
```

Minimum `.env` for live ingestion:

```text
ALPACA_API_KEY=...
ALPACA_SECRET_KEY=...
SEC_USER_AGENT=YourAppName you@example.com
INGEST_SYMBOLS=NVDA
```

All other settings are documented with defaults in [.env.example](.env.example).
Never commit `.env` or put secrets in logs, screenshots or support requests.
The worker never logs request headers, and it masks credential values if they
ever appear in a log line.

Semantic event grouping needs extra, explicit setup steps (optional packages, a
model download and an API key); they are listed in
[Setting up semantic comparison](#setting-up-semantic-comparison).

### Alpaca authentication

| `ALPACA_AUTH_MODE` | Credentials | Sent as |
| --- | --- | --- |
| `trading` (default, what this repo already used) | `ALPACA_API_KEY`, `ALPACA_SECRET_KEY` | `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY` headers (REST and WebSocket) |
| `broker` | `ALPACA_BROKER_CLIENT_ID`, `ALPACA_BROKER_CLIENT_SECRET` | OAuth2 client-credentials token from `https://authx.alpaca.markets/v1/oauth2/token`, cached until shortly before its 15-minute expiry, sent as `Authorization: Bearer` |

The repository had no Broker flow before this change. `broker` mode implements
the documented flow and is unit-tested with a mocked token endpoint, but it has
not been exercised against a live Broker account.

Missing credentials are a **configuration error**, not a silent fallback. Sample
data is served only in explicit demo mode (see below).

## Local application: one command

After the one-time prerequisites below, daily startup is one command in PowerShell:

```powershell
uv run datasources-dev
```

Then open **http://127.0.0.1:8000**. Ctrl+C in that window stops everything.

The launcher runs two child processes and prefixes every log line with its name:

| Service | What runs |
| --- | --- |
| `[web]` | FastAPI serving the page and the read-only API on 127.0.0.1 (no auto-reload). It never starts collectors or calls a model. |
| `[ingest]` | The existing worker, `datasources-ingest run`: Alpaca WebSocket with REST recovery at startup and after every reconnect, SEC EDGAR polling, and whichever inline steps `.env` enables (event grouping with `SIMILARITY_ENABLED`, event summaries with `SUMMARY_ENABLED`). |

Before it starts anything, the launcher checks the configuration, the database
connection and schema, the port, and whether another launcher, ingestion worker
or graph writer is already running. It then prints a summary of what will run,
including whether paid model calls are possible with the current settings. It
never prints credentials (only whether they are present), installs anything,
changes the schema, applies migrations or runs a backfill: when something is
missing, it names the command that fixes it and does not start.

### One-time prerequisites

1. PostgreSQL with pgvector and the `stock_news` database (see [PostgreSQL storage](#postgresql-storage)).
2. Dependencies, including the local embedding backend used by event grouping:
   ```powershell
   uv sync --locked --extra local-embeddings
   ```
3. `Copy-Item .env.example .env`, then fill in the Alpaca credentials, `SEC_USER_AGENT`, the `PG*` settings and,
   if grouping or summaries use Anthropic, `ANTHROPIC_API_KEY`.
4. The schema: `uv run datasources-db migrate` (additive and idempotent; the launcher only checks it).
5. When `SIMILARITY_ENABLED=true` with the sentence-transformers backend: `uv run datasources-similarity download-model`.
6. `uv run datasources-dev --check` runs every startup check and prints the summary without starting anything.

### Model calls and cost

Page views, API polling and extra browser tabs never call a model and never start
collectors: the page only reads what is stored. Model calls happen only inside
the ingestion worker, for records it ingests:

* `SIMILARITY_ENABLED=true` with `SIMILARITY_COMPARISON_BACKEND=anthropic`: each new article or filing can send
  up to `SIMILARITY_LLM_MAX_CALLS_PER_RECORD` comparison requests (default 3). Each request can be retried by
  the SDK up to `SIMILARITY_LLM_MAX_RETRIES` times.
* `SUMMARY_ENABLED=true`: each new record can send up to `SUMMARY_MAX_REQUESTS_PER_RUN` summary requests
  (default 5), each retried up to `SUMMARY_LLM_MAX_RETRIES` times.
* At startup the worker recovers Alpaca news since its last sync (at most `ALPACA_RECOVERY_MAX_HOURS`) and SEC
  filings from its lookback window that are not stored yet. Recovered records go through the same steps, so
  the first start after a long break can make many more calls than a normal hour.

To run without paid calls, set `SIMILARITY_ENABLED=false` and `SUMMARY_ENABLED=false`, or use
`uv run datasources-dev --no-collectors` to browse stored data only.

**Deferred work is not scheduled.** The launcher adds no retry loop. Records whose grouping is still pending
wait for `uv run datasources-similarity retry` (run it with the worker stopped). Queued or failed summaries wait
for `uv run datasources-summary retry --confirm-paid-calls`, or for a later record that touches the same
event. Both commands can make paid calls.

### Options

| Option | Effect |
| --- | --- |
| `--check` | Run the startup checks and print the summary; start nothing |
| `--no-collectors` | Web app and API only |
| `--no-sec` / `--no-alpaca` | Leave one collector out |
| `--port 8001` | Serve the page on another port |

### Duplicates, failures and shutdown

* A second `datasources-dev` for the same output directory refuses to start and reports where the first one
  is running. A stray `datasources-ingest run` or `datasources-similarity backfill`/`retry` is caught by the
  existing locks (`output/state/ingest.lock` and the PostgreSQL graph-writer lock), which the launcher checks
  before starting.
* If a service exits during startup or later, the launcher reports it with its exit code and stops the other
  one. It never keeps running with a dead collector. An occupied port is reported before anything starts.
* Ctrl+C stops the worker first (it finishes the record it is writing; queued work stays retryable), then the
  web server. A second Ctrl+C terminates them immediately. The services also stop if the launcher window is
  closed or the launcher is killed.
* The worker writes `output/state/worker_status.json` every 15 seconds. The page's collector status comes only
  from this file. After 60 seconds without a heartbeat, the status shows as unknown.

### The page

**News summaries** (`/`) is a single feed for readers, newest story first, with a company selector.
Each card shows the published summary headline (the event title while no summary exists), the ticker, one
labelled time ("Published" for one source, "Latest report" for several) and a short preview marked
*AI summary*: two or three stored key points when they are short, otherwise the overview's opening sentences,
cut at a sentence boundary. Nothing is rewritten or generated for the page.

* **Read more** / **Show less** shows the complete stored summary in the card.
* **Sources (N)** shows each original source (publisher, link, exact time), the quote behind each summary
  statement, the summary's version history and resolved reviews.
* A conflict shows one line on the card ("Conflicting reports—under review", "Sources disagree" once a
  review confirms it, or "Grouping correction in progress"); **Compare the claims** opens the competing
  claims with their quotes and sources.
* A story without a summary says so briefly ("Preparing summary…", on hold during a review, failed) and links
  the most recent original source.
* The page refreshes every 20 seconds without reloading and shows "Updated … ago". It pauses while the tab is
  hidden, backs off after failures (up to 5 minutes), and keeps showing the last data with a notice when a
  refresh fails. A note says when collectors are not running (the feed cannot get newer). Open summaries and
  sources, focus and scroll position are kept.
* Sample records never appear in the feed. Add `#sample=1` to the address for demos and tests; sample
  stories are then marked *Sample*.

**Data tools** (`/tools.html`, not linked from the feed) keeps the developer views: collector status, every
stored news article and SEC filing (original headline, publisher or issuer, the source's own times, the
excerpt supplied with the article, labelled as not AI-generated, and its grouping status), with company,
source and sample filters, and the original Alpaca / EDGAR data-dictionary **source explorer**, which makes
live requests only when you press a button.

## Run

```powershell
# Automatic ingestion: EDGAR polling + Alpaca WebSocket + REST backfill/recovery
uv run datasources-ingest run

# One pass: one SEC poll per company + one Alpaca REST backfill, then exit
uv run datasources-ingest run --once

# Only one source
uv run datasources-ingest run --no-alpaca
uv run datasources-ingest run --no-sec

# Offline demo over bundled fixtures -> output/demo (is_sample = true)
uv run datasources-ingest demo
# ...also stored in PostgreSQL as sample rows, grouped into events if SIMILARITY_ENABLED=true
# (and summarized if SUMMARY_ENABLED=true: that calls the summary model)
uv run datasources-ingest demo --with-database

# Print the compiled LangGraph (Mermaid diagram + edge list)
uv run datasources-ingest graph

# Ingest a saved payload (transport = file): REST page, /api/* wrapper,
# WebSocket batch, or the existing JSONL archive of WebSocket frames
uv run datasources-ingest process-file output/alpaca_news_stream.jsonl --provider alpaca
uv run datasources-ingest process-file data/submissions_CIK0001045810.json --provider sec_edgar --sec-html-dir data/8k --demo

# Ledger counts and cursors
uv run datasources-ingest status
```

Stop the worker with Ctrl-C. It stops the collectors, finishes the record it is
writing, and leaves queued work unmarked so the next start picks it up again.

Run **one worker per output directory**; a file lock (`output/state/ingest.lock`)
enforces this. The worker owns the Alpaca WebSocket, so don't also run
`datasources-news-stream`: it checks the same lock and refuses to start while the
worker runs. Don't start collectors from FastAPI. The web server stays a
read-only view and starts no background tasks.

Run **one graph writer per database partition** (live or sample). A PostgreSQL
advisory lock, held for the life of the process, enforces it across output
directories and across `datasources-similarity backfill` / `retry`, so stop the
worker before running those. It is released automatically if the process dies.

Makefile shortcuts: `make ingest`, `make ingest-once`, `make ingest-demo`, `make graph`, `make test`,
`make similarity-status`, `make similarity-backfill LIMIT=100`, `make similarity-retry LIMIT=100`,
`make summary-check`, `make summary-status`, `make summary-pending`, `make summary-reviews` (no paid calls;
the paid `datasources-summary` commands have no shortcut on purpose).

## How it works

```text
SEC poller (per company, ~60 s) ─┐  fresh submissions JSON (bypasses the 12 h UI cache,
                                 │  then refreshes it), aligned-array validation,
                                 │  8-K / 8-K/A selection within the scope window
Alpaca WebSocket (existing       │
  alpaca_news.client) ───────────┤──> bounded queue ──> single writer
Alpaca REST backfill (startup,   │                        1. ledger decision (source identity)
  every (re)subscription) ───────┘                        2. SEC: download primary 8-K HTML, save raw
                                                          3. LangGraph (below): prepare, compare,
                                                             group, then ONE PostgreSQL transaction
                                                          4. atomic write processed/<...>.json
                                                          5. atomic ledger update
```

* **Graph** (`src/datasources/ingest/graph.py`): a `StateGraph` with typed state,
  compiled once and reused; the database, embedding model and comparison
  backend arrive through LangGraph's runtime context.

  ```text
  START -> prepare_data --(no valid record)--> END
  prepare_data -> create_text_units -> generate_embeddings -> retrieve_candidates -> route_candidates
  route_candidates --(no plausible candidate)--> propose_new_event ---------> resolve_event_memberships
  route_candidates --(plausible candidates)----> compare_with_llm ----------> resolve_event_memberships
  route_candidates --(nothing to decide)-------------------------------------> resolve_event_memberships
  resolve_event_memberships -> persist_result -> route_event_summary
  route_event_summary --(generate)--> generate_event_summary -> validate_event_summary
                                          -> persist_event_summary -> route_event_summary
  route_event_summary --(open conflict)--> flag_conflict_review -> route_event_summary
  route_event_summary --(nothing new)----> skip_summary ---------> route_event_summary
  route_event_summary --(all affected events done)--> create_text_units (next record) | END
  ```

  The summary nodes run per affected event after the record's commit; see
  [Event summaries](#event-summaries). With summaries disabled,
  `route_event_summary` has nothing to route and passes straight on.

  `prepare_data` validates each `SourceEnvelope` with Pydantic, picks the adapter
  from the envelope's explicit `provider`, and isolates per-record errors: one
  malformed record never discards the rest of the batch. Records are then
  analysed **one at a time**; `persist_result` commits each record (document,
  revision, sources, text units, embeddings, event decisions) before the next
  one is analysed, so the next record already sees it. With similarity
  disabled the analysis nodes pass the record through, `persist_result` stores
  it exactly as before, and its analysis is reported as `disabled`. "Nothing to
  decide" covers disabled or skipped analysis, a failed earlier stage, a replay
  of an already stored record, and an exact-content match. Polling, WebSocket,
  downloads, source-identity tracking and file writing all run outside the
  graph.
* **Envelope** (`schema.py`): provider (`sec_edgar`/`alpaca`), transport
  (`rest`/`websocket`/`file`), API endpoint, retrieval time, requested symbols,
  sample marker, raw record, raw-file references and, for SEC, the downloaded
  primary-document HTML and its URL.
* **Adapters** (`adapters.py`) accept every shape in this repo: raw submissions
  JSON and historical submissions files, the `/api/edgar` wrapper, Alpaca REST
  pages, the `/api/alpaca` wrapper, WebSocket batches and JSONL archives.
  Data-dictionary files (`*_datadict.json`) are recognised and ignored as content.

### SEC EDGAR

* Submissions: `https://data.sec.gov/submissions/CIK##########.json` (10-digit, zero-padded).
* `filings.recent` is read as aligned arrays. If a required column
  (`accessionNumber`, `form`, `filingDate`, `primaryDocument`) is missing or has
  a different length, the payload is rejected rather than silently truncated.
  A misaligned or missing optional column becomes `null` with a warning.
* URLs: archive directory `https://www.sec.gov/Archives/edgar/data/<CIK without zeros>/<accession without hyphens>/`,
  index `<accession>-index.htm`, primary document `<validated primaryDocument>`.
* Downloads use the configured `SEC_USER_AGENT`, a shared **2 requests/second**
  budget covering submissions, historical files and documents (SEC publishes
  10/s as the aggregate limit), timeouts, streamed size caps, content-type checks
  and manual redirects that must stay on `www.sec.gov` / `sec.gov` / `data.sec.gov`.
* Retries are bounded (default 3) with exponential backoff, jitter, and
  `Retry-After` support. 403/404 and other 4xx responses are not retried.
* A 403, an SEC error or throttling page, an empty body or an empty extraction
  never counts as a successful download. The filing is recorded as `failed`
  in the ledger with a backoff (from 60 s doubling to 6 h, 1 h+ for 403) and
  retried on a later poll.
* Extraction scope is `primary_document_only`. SEC-hosted exhibit links found in
  the document are kept in `exhibit_urls` / `sec_metadata.exhibit_links` with
  `exhibits_extracted: false`. They are not followed.
* `published_at` = `acceptanceDateTime` when valid. `filing_date` and
  `report_date` are kept separately as dates; a date-only value is never turned
  into a timestamp.

**Scope and recovery.** On the first run for a company the worker inspects
filings from the last `SEC_INITIAL_LOOKBACK_DAYS` (30). That floor is stored in
the ledger and never moves forward, so filings that failed or were still queued
at shutdown are inspected again (completed ones are skipped in memory, with no
request). The floor is clamped to `SEC_RECOVERY_MAX_DAYS` (365). When the floor
is older than the oldest filing in `filings.recent`, up to
`SEC_MAX_HISTORICAL_FILES` (2) older `filings.files` submissions files are
fetched once and cached under `raw/`. Every relevant filing in scope is
inspected, not only the latest 10 the UI shows.

### Alpaca

* **WebSocket**: the existing `alpaca_news.client.run_forever` / `stream_once`
  were extended with callbacks (`on_articles`, `on_subscribed`, `on_frame`).
  Only `T="n"` messages are processed. `success` / `subscription` / `error`
  frames are handled as operational messages; only their status text is logged.
  After non-transient errors (402 auth failed, 405 symbol limit, 406 connection
  limit, 409 insufficient subscription) the client waits 300 s before
  reconnecting, to avoid a reconnect storm.
* **REST**: `/v1beta1/news` with `include_content=true`, explicit `start`/`end`,
  `sort=asc`, `limit=50` (documented maximum) and `next_page_token` pagination.
  Articles without `content` are kept (`content_status: summary_only` or `headline_only`).
* **Backfill / recovery**: at startup and after every (re)subscription, the
  worker fetches `[cursor − ALPACA_RECONNECT_OVERLAP_MINUTES (15), now]`. The
  cursor advances only after the window's articles are saved, as WebSocket
  articles are processed, and on an idle-while-connected heartbeat. With no
  cursor it starts at `now − ALPACA_INITIAL_LOOKBACK_HOURS (24)`. It is clamped to
  `ALPACA_RECOVERY_MAX_HOURS (72)` and to `ALPACA_BACKFILL_MAX_PAGES (40)` pages.
  **This recovery is bounded**: it re-reads articles created or updated inside
  the window, but it does not guarantee recovery of every old article revision
  published while the worker was down.
* Text cleaning: HTML parsed and entities decoded; paragraph, list and table
  structure kept; Unicode NFC and whitespace normalised; case, punctuation,
  numbers and negation preserved; no stemming or stop-word removal. Symbols are
  upper-cased without damaging `BRK.B` / `BTC/USD`. Timestamps are converted to
  UTC; invalid or offset-less timestamps become `null` with a warning, never guessed.
* `publisher` is the article's `source` (e.g. `benzinga`), never `alpaca_live`.

## PostgreSQL storage

Ingested records are also stored in the local **`stock_news`** database. The
schema already existed (it is the project ERD); ingestion was mapped onto it
rather than the other way round.

```powershell
uv run datasources-db check      # which database, and is pgvector enabled
uv run datasources-db migrate    # apply additive migrations (idempotent)
uv run datasources-db inspect    # row counts per table
uv run datasources-db backfill   # store existing output/processed records (idempotent)
uv run datasources-db smoke      # bounded live end-to-end test (see Tests)
```

`datasources-db backfill` and `smoke` store records without semantic analysis;
`uv run datasources-similarity backfill` analyses stored records afterwards.

Configuration uses the standard libpq variable names, so `psql`, pgAdmin and
this project share one `.env`. `PGPASSWORD` belongs in `.env` only, which is
git-ignored; `.env.example` carries placeholders. `DATASOURCES_DB_ENABLED=false`
runs ingestion to files only.

```text
PGHOST=localhost
PGPORT=5432
PGDATABASE=stock_news
PGUSER=postgres
PGPASSWORD=<set locally>
```

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

### Transactions and async safety

Everything for one record — document, revision, company links, SEC details, raw
source and, when enabled, its text units, embeddings, analysis status and event
decisions — is written by the graph's `persist_result` node in **one
transaction**, so a failed related insert leaves no partial article
(`tests/test_db_persistence.py` proves this with a real check violation, and
`tests/test_similarity_pipeline.py` for the analysis rows). No transaction is
held open while an embedding model or the LLM runs. All SQL is parameterised.

The writer runs `IngestionService.process_sec` / `process_alpaca` (and so the
whole graph) off the event loop via `asyncio.to_thread`, so neither the
synchronous driver nor a model call blocks asyncio. Persistence happens
**before** the processed file is written and before the ledger is marked, so a
database failure leaves the record pending for the next poll instead of marked
done with nothing stored. If PostgreSQL commits but the file or ledger write
then fails, the redelivery finds the stored revision and its analysis, reuses
them, and calls no model again.

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

More event queries are in [Inspecting events](#inspecting-events).

## Semantic comparison and event grouping

Off unless `SIMILARITY_ENABLED=true`. When on, each eligible article or filing is
compared with previously **committed** news inside the ingestion graph, and only
then stored — together with its text units, embeddings and event decisions — in
one transaction. Collection, source-identity checks, raw payload capture and SEC
downloads stay outside the graph, unchanged.

### Setting up semantic comparison

```powershell
uv sync --locked --extra local-embeddings     # CPU-only torch + sentence-transformers (large download)
uv run datasources-db migrate                 # migration 002: analysis, event and retrieval tables
uv run datasources-similarity download-model  # explicit model download (~130 MB, Hugging Face cache)
# in .env: SIMILARITY_ENABLED=true and ANTHROPIC_API_KEY=<your key>
uv run datasources-similarity check           # validates DB, pgvector, schema, model, credentials; writes nothing
uv run datasources-similarity backfill --limit 100   # bootstrap stored records; repeat until "selected": 0
uv run datasources-ingest run                 # the worker now compares and groups inline
```

* The embedding stack is an optional extra because torch is large. `uv run`
  keeps it installed, but a plain `uv sync --locked` (without
  `--extra local-embeddings`) removes it again.
* The model is **never** downloaded implicitly: loading uses local files only,
  and a missing model is a startup error that names `download-model`.
* With `SIMILARITY_ENABLED=true`, startup fails with a clear message if the
  database is disabled, pgvector or migration 002 is missing, the model is not
  downloaded, or no Anthropic credentials are found. Outages *after* startup
  never stop ingestion: the article is stored and its analysis is `pending`.
* `SIMILARITY_COMPARISON_BACKEND=mock` runs everything except the LLM: every
  compared pair becomes an `uncertain` pending relation, so nothing is ever
  linked. It exists for offline demos and plumbing checks.

### The analysis nodes

| Node | Does | Model / DB access |
| --- | --- | --- |
| `create_text_units` | Detects a replay of an already stored revision (then nothing is recomputed), finds the document's previous revision, builds text units | reads |
| `generate_embeddings` | Reuses stored vectors for identical input under the same profile, embeds the rest in batches, validates dimensions | embedding model |
| `retrieve_candidates` | Semantic + lexical search over committed rows, fusion, event mapping, index coverage | reads |
| `route_candidates` | Exact-content shortcut; decides `compare_with_llm`, `propose_new_event`, or "nothing to decide" | — |
| `propose_new_event` | No plausible candidate: drafts provisional events, **no LLM call** | — |
| `compare_with_llm` | Bounded, validated comparison of incoming segments with candidate events | LLM |
| `resolve_event_memberships` | Applies the rules below; sets the analysis status | — |
| `persist_result` | One transaction: document, revision, sources, units, vectors, analysis, comparisons, events, memberships | writes |

### Text units

* **Articles** use the existing `analysis_text` (headline, the summary only when
  the body does not already contain it, then the body). An article that fits
  the model limit is one `document` unit; a longer one becomes ordered `chunk`
  units, split at paragraph and sentence boundaries (words only as a last
  resort) with a modest overlap (`SIMILARITY_CHUNK_OVERLAP_TOKENS`, 48) inside a
  section. Later chunks carry the headline as a short context prefix. Promo
  lines such as "Read Also: …" and image credits are not embedded.
* **SEC filings** use the extracted primary-document `body_text`, never the
  generated metadata header, split at `Item x.xx` headings. The cover page, the
  signature block, "furnished … shall not be deemed filed" paragraphs,
  forward-looking-statement safe harbours, "does not purport to be complete"
  sentences and exhibit lists that only name the XBRL cover page (101/104) are
  not embedded. Each unit is prefixed with `<issuer> Form 8-K filed <date>`.
  Exhibits are still not downloaded or extracted.
* Every unit records the exact source spans it came from (`location.spans`), its
  section, token count and overlap, so a passage is always traceable. Nothing
  is truncated: each chunk fits the tokenizer limit, and every passage that is
  not embedded is recorded with its section, reason and source spans in
  `revision_analysis.stage_status.skipped_passages`.
* **Segments** are the parts of a revision that can describe one event: a news
  article is one segment; each substantive SEC Item is one. Items 7.01 and 9.01
  (and an exhibit index) accompany a single primary Item; an Item that only
  says "the information in Item X is incorporated here" joins Item X. Several
  chunks of one segment support one event, never several copies of it.
* Headline-only and summary-only articles are one unit with a *limited* evidence
  level, passed to the comparison as such. A revision with no usable text is
  stored with analysis `skipped` and a `skip_reason` (`no_text`,
  `insufficient_text`, `boilerplate_only`, `extraction_failed`).

### Embeddings

| | |
| --- | --- |
| Backend | `sentence-transformers` 6.1 on CPU (PyTorch 2.14 CPU wheels from the PyTorch index, configured in `pyproject.toml` per uv's PyTorch guide) |
| Model | [`BAAI/bge-small-en-v1.5`](https://huggingface.co/BAAI/bge-small-en-v1.5), MIT licence, pinned to commit `5c38ec7c405ec4b44b94cc5a9bb96e735b38267a` |
| Dimensions / distance | 384, L2-normalised output, cosine distance (pgvector `<=>`) |
| Input limit | 512 tokens (BERT WordPiece, special tokens included); chunks target 384 so the context prefix always fits |
| Query instruction | none: the model card says passages need no instruction, and this is passage-to-passage comparison |
| Measured speed | about 27 units/s on this development machine's CPU (159 units from the 58 stored revisions in 5.9 s) |
| Test backend | `hashing`: deterministic bag-of-words hashing, its own profile, for tests and offline demos only |

Each embedding space is one row of the existing `embedding_profile` table:
model provider, name and version (the pinned commit), dimensions, distance
metric, text-preparation version (`textunits-1`) and the encoding and chunking
settings, identified by a SHA-256 `config_hash`. Retrieval only ever compares
vectors of the same profile, never vectors that merely have the same length.
A new model, revision or chunking setting creates a new profile; its work is
versioned and has to be rebuilt with `backfill`. Vectors are validated
(dimensions, finite, non-zero) before storage, and the existing
`embedding_dimensions_check` trigger validates them again. Identical input text
under the same profile reuses the stored vector instead of recomputing it.

### Retrieval

* **Candidate pool** (committed rows only): same sample/live partition, same
  embedding profile, the **current** revision of a **different** document, at
  least one shared company (`revision_company`), and a source time within
  `SIMILARITY_WINDOW_DAYS` (7, an experimental starting point) of the incoming
  record's anchor time.
* **Anchor time** is the record's own time, never the worker's clock:
  `published_at` (Alpaca `created_at`, SEC `acceptanceDateTime`) → source
  `updated_at` → SEC filing date (start of that UTC day; used for the window only,
  never stored as a timestamp) → retrieval time. The fallback used is recorded.
  The window is symmetric, so an article that arrives late (out of order) still
  finds coverage from before and after it.
* **Bounded fallbacks.** No resolvable company link: no company filter, but the
  window narrows to `SIMILARITY_NO_COMPANY_WINDOW_HOURS` (48); recorded as a
  limitation. A new revision of an already stored document never finds its own
  earlier revisions as independent sources; the previous revision's events are
  compared deliberately and presented to the model as "an earlier revision of
  this same article".
* **Two independent channels.** Semantic: exact pgvector cosine search over the
  pool, top `SIMILARITY_SEMANTIC_TOP_K` (20) per incoming unit. Lexical:
  PostgreSQL full-text search on a generated `text_unit.search_tsv` column (GIN
  index), an OR-query of the unit's most frequent English lexemes, ranked with
  `ts_rank_cd`, top `SIMILARITY_LEXICAL_TOP_K` (20). Neither channel gates the
  other, and there is no Levenshtein filter.
* **Fusion.** Reciprocal rank fusion, `score = Σ 1 / (60 + rank)` over the
  channels a candidate appears in (`SIMILARITY_RRF_K`). Each channel's rank and
  score are stored in `retrieval_candidate` for every hit, with the reason a hit
  was not compared.
* **Plausibility** decides what may reach the LLM, never what is "the same
  event": cosine ≥ `SIMILARITY_MIN_SEMANTIC_SIMILARITY` (0.78) **or** a top-3
  full-text hit (`SIMILARITY_LEXICAL_PLAUSIBLE_RANK`). Calibration on the 58
  stored NVIDIA revisions (1,653 document pairs, max unit-to-unit cosine):
  median 0.69, 90th percentile 0.77, 99th 0.86 — and three recurring "whale
  activity" articles from different days scored 0.98–0.999 while genuinely
  related IonQ/NVIDIA coverage scored 0.84–0.89. A high score alone therefore
  never links anything. The shortlist keeps the best `SIMILARITY_MAX_CANDIDATE_EVENTS`
  (6) events.
* **Self-matches** are impossible: the incoming document is excluded from the
  pool and events the revision already belongs to are never candidates.
* **Coverage.** Each analysis records how many revisions in its window have no
  embeddings under the profile. If any are unindexed, "no match" is not proof of
  uniqueness: the analysis is `partial` and new events carry the flag
  `index_coverage_partial`. Revisions that can never be indexed (analysis
  `skipped` for lack of text, or `failed`) are counted separately and do not keep
  their neighbours partial. Candidates whose own grouping is still pending are
  not compared; they compare against this record when their turn comes.
* A retrieval error leaves the analysis `pending`. It is never read as "no
  matches".

### LLM comparison

The comparison node sends Claude (the official `anthropic` SDK; model
`SIMILARITY_COMPARISON_MODEL`, default `claude-opus-5`) the incoming segments and
their units, and for each shortlisted event its defining evidence (title, known
facts, origin passage) plus the matched passages, with document, revision, unit
and event IDs replaced by short references. It compares participants, action,
object, event date or fiscal period (separately from publication date),
figures (metric, units, currency, period), status (planned, announced,
completed, cancelled, denied) and additional or incompatible claims. Missing
facts are unknown, never contradictions.

| Label | Meaning | Resolution |
| --- | --- | --- |
| `same_event_repeated` | Same occurrence, nothing new (paraphrase, syndicated copy, missing details) | linked to the event |
| `same_event_update` | Same occurrence, new facts or a later development (announced → completed / cancelled) | linked, marked as an update |
| `same_event_conflict` | Same occurrence, a comparable claim is incompatible (same metric, period and context; or a denial) | linked and flagged; both sources kept |
| `different_event` | A different occurrence (participants, action, object or period differ) | not linked |
| `uncertain` | Not enough context to decide | a *pending* relation, never a merge |

**Event grouping convention (MVP):** an event is one identifiable occurrence or
transaction plus its later developments. Recurring occurrences — results for
different fiscal quarters, different trading sessions, different monthly
figures — are separate events. The model compares claims as reported; it does
not decide which source is true.

* **Structured output:** JSON-schema structured outputs (`output_config.format`)
  with a Pydantic mirror of the schema. `stop_reason` is checked before any
  content is read; a refusal or a truncated answer is an error, never a result.
* **Validation:** every returned reference must be one that was supplied, every
  evidence quote must occur verbatim in the passage it cites, and a same-event
  label without verified evidence on both sides is downgraded to `uncertain`.
  Requested pairs the model did not judge are recorded as unevaluated.
* **Untrusted input:** all article and filing text is XML-escaped inside
  delimited elements, and the system prompt tells the model to treat it as data
  and never follow instructions inside it.
* **Bounds:** at most `SIMILARITY_LLM_MAX_EVENTS_PER_CALL` (6) events and
  `SIMILARITY_LLM_MAX_INPUT_CHARS` (30,000) characters per call,
  `SIMILARITY_LLM_MAX_OUTPUT_TOKENS` (16,000), a
  `SIMILARITY_LLM_TIMEOUT_SECONDS` (180) timeout, `SIMILARITY_LLM_MAX_RETRIES`
  (2, the SDK's retries for 429/5xx/connection errors), and per record
  `SIMILARITY_LLM_MAX_CALLS_PER_RECORD` (3) calls and
  `SIMILARITY_LLM_TOKEN_BUDGET_PER_RECORD` tokens. Incoming units that do not fit
  a prompt are listed as omitted. Pairs left unevaluated by a limit make the
  analysis `partial`; pairs left by a failure make it `pending`.
* **Usage** (input/output/cache tokens, served model, request IDs) is stored per
  comparison and summed per analysis.
* **Refusal fallback:** requests opt in to the API's server-side fallback
  (`fallbacks: "default"`, beta header `server-side-fallback-2026-07-01`), which
  re-runs a declined request on Anthropic's recommended fallback model. Set
  `SIMILARITY_COMPARISON_FALLBACKS=none` for a model that does not support it.
* **Exact-content shortcut:** a segment whose whitespace-normalised text is
  identical to a passage already in a candidate event, has at least
  `SIMILARITY_EXACT_MATCH_MIN_CHARS` (400) characters after boilerplate removal,
  and has full text on both sides, is linked as `same_event_repeated` without a
  model call (typically syndicated copy, or a new revision whose text did not
  change). Identical boilerplate or headlines never qualify.

### Resolution rules

* A segment is linked to every candidate event it was judged the same as, with
  the supporting units recorded (`event_membership_unit`). One membership per
  (event, revision): overlapping chunks and several segments collapse into it,
  keeping the most informative relation (conflict > update > repeated).
* A segment gets a **provisional** new event only after **every** shortlisted
  candidate was evaluated and none matched. Rejecting one candidate does not
  reject the others; an unevaluated candidate leaves the segment unresolved.
* `uncertain` keeps a pending relation to the candidate event and gives the
  segment its own provisional event, so the article stays visible without being
  merged.
* Links are made only to events whose defining evidence was compared directly:
  A ~ B and B ~ C never puts A and C together.
* A provisional event becomes `corroborated` once a second document joins it. A
  retry that links a segment elsewhere marks its own single-source provisional
  event `superseded` (kept, hidden from current views).
* Multi-event filings get one event per substantive Item.

### Failures and recovery

| Situation | Result |
| --- | --- |
| Embedding, retrieval or LLM failure (after the SDK's retries) | Article stored; analysis `pending` with the sanitized error, attempts and `next_retry_at`; whatever was valid (units, vectors, scores, judgments) kept |
| Limits reached (call/token budget, unjudged pairs) or incomplete index coverage | Article stored; analysis `partial` |
| Database failure in `persist_result` | Nothing for that record is committed; ingestion stays incomplete and the record is retried |
| Commit succeeded, file or ledger write failed | Redelivery reuses the stored revision and analysis: no duplicate events, no model call |
| `pending` too many times (`SIMILARITY_MAX_ANALYSIS_ATTEMPTS`, 5) | `failed` |

Ingestion completion (the file ledger) and analysis completion (`revision_analysis`)
are tracked separately: a stored article with pending analysis is complete for
ingestion and is finished by `retry`, which never depends on the ledger
rediscovering it.

### Maintenance commands

```powershell
uv run datasources-similarity check                        # configuration report, no writes
uv run datasources-similarity download-model               # explicit model download (network)
uv run datasources-similarity backfill --limit 100         # stored revisions without analysis: index all, then group
uv run datasources-similarity backfill --limit 100 --index-only
uv run datasources-similarity retry --limit 100            # pending / partial analyses that are due
uv run datasources-similarity retry --all --revision-id <revision uuid>
uv run datasources-similarity status                       # analysis status, units, events, coverage
uv run datasources-similarity inspect --revision-id <revision uuid>   # units, candidates + scores, comparisons, events
uv run datasources-similarity evaluate --limit 10 --confirm-paid-calls   # real backend on labelled examples
uv run datasources-ingest graph                            # the compiled graph
```

Add `--sample` to `backfill`, `retry` and `status` to work on sample data.

* **Bootstrap** (`backfill`): the 58 revisions stored before this feature have no
  embeddings. `backfill` selects up to `--limit` current revisions without an
  analysis, oldest first, and runs the same graph twice: an index-only pass that
  creates units and vectors for the whole selection, then a grouping pass in
  source-time order. Indexing first means no historical record is declared new
  merely because its neighbours were not embedded yet. It is bounded and
  idempotent; repeat it until it selects 0.
* **Retry** re-runs the graph for stored revisions (never rewriting them):
  stored units and vectors are reused, pairs that were already judged are not
  sent again, and unique constraints make a repeated retry a no-op.
* These are maintenance tools, not a replacement for the inline analysis.

### Throughput

Model calls run inline in the single writer, so they bound ingestion
throughput: one article costs a few embedding milliseconds plus, when it has
plausible candidates, one or more LLM calls (seconds each). For NVIDIA news
most articles have plausible same-company candidates within a week. The queue
stays bounded and is never drained by dropping records: when it is full,
collectors wait, and the pressure is reported (`queue_full_waits`,
`queue_wait_ms`, `queue_max_depth`, and a warning at most once a minute; the
periodic stats line shows `queue <depth>/<size>`). Keep
`SIMILARITY_LLM_MAX_CALLS_PER_RECORD` and `SIMILARITY_MAX_CANDIDATE_EVENTS` modest
if throughput matters more than coverage.

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

## Event summaries

Off unless `SUMMARY_ENABLED=true` (which also needs `SIMILARITY_ENABLED=true`).
The website shows **one summary per event**, written from the event's original
articles and SEC filings, with every statement citing the passages it came
from. Summaries are generated **after** a record and its grouping are
committed, never on a page view; the API only reads what is stored.

```powershell
uv run datasources-db migrate                 # migration 004 (see below)
uv run datasources-summary check              # configuration, schema, credentials; no writes, no calls
uv run datasources-summary pending            # what would be summarized (no calls)
uv run datasources-summary run-event --event-id <event uuid> --confirm-paid-calls   # ONE event, at most one request
# then, when the output reads well: SUMMARY_ENABLED=true in .env, and the worker summarizes inline
```

### The summary nodes

```
persist_result -> route_event_summary
   route_event_summary --(generate)--> generate_event_summary -> validate_event_summary -> persist_event_summary --+
   route_event_summary --(conflict)--> flag_conflict_review -------------------------------------------------------+
   route_event_summary --(skip)------> skip_summary ---------------------------------------------------------------+
   (back to route_event_summary for the record's next event; when none is left: next record or END)
```

`route_event_summary` runs once for **every event the record is linked to** (a
multi-Item 8-K can touch several; each is summarized only from its own
evidence). A record's summary work finishes, or is durably queued, before the
next record is analysed. With summaries disabled the node has nothing to route.

| Situation (checked in this order) | Route |
| --- | --- |
| The record's relation is a pending `uncertain` one | skip: unresolved evidence never enters a summary |
| An open (or not yet reviewed) `same_event_conflict` on the event | `flag_conflict_review`: one review per comparison, the published summary stays up with an under-review flag, no replacement is published; with no summary yet the event shows `awaiting_review` |
| Another process holds the event's summary lease | skip |
| No published summary | generate the first summary, even for a single article |
| `same_event_update` | generate a new version |
| Summary work already pending (an earlier failure, a review resolution, a deferral) | generate, even when the new record is only a repeat |
| `same_event_repeated` or an exact-content copy | skip: the existing summary is retained |

`different_event` needs no rule: grouping gives the record its own event, which
has no summary and is summarized as a new event. Routing reuses grouping's
stored labels; no model call decides anything here.

### Evidence, output and validation

* **Evidence** comes only from the event's **active** memberships on **current**
  revisions (`current_event_membership`) and the text units that support them,
  shown as the original source passages. Pending relations, detached members and
  superseded revisions are excluded. Passages are never cut; selection takes the
  best passage of every source first (origin, then the update/conflict passages
  the comparison cited, then repeats), gives syndicated duplicates once, and
  records what did not fit `SUMMARY_MAX_INPUT_CHARS` / `SUMMARY_MAX_PASSAGES` as a
  limitation on the summary.
* The prompt also carries each source's type (an SEC filing is labelled a
  company disclosure, not verified by the SEC), grouping's comparison notes, any
  resolved reviews, and the previous version when updating. Everything copied
  from sources, earlier model output or reviewer notes is escaped and described
  as untrusted data. The previous summary and comparison notes are context;
  every statement must cite passages.
* **Output** (structured JSON): a headline `H1`, an overview of one to three
  sentences `O1`..`O3`, up to three key points `K1`..`K3`, each with passage refs
  and verbatim quotes; plus facts the sources do not state (`evidence_gaps`).
* **Validation** establishes traceability, not truth: schema and ids; every
  statement cited; every ref was provided (so it belongs to this event's active
  members, in its sample/live partition, re-checked in SQL at publication and by
  a trigger); every quote occurs in its passage after normalization (HTML
  entities unescaped, curly quotes / dashes / non-breaking spaces folded,
  whitespace collapsed, case-folded; nothing fuzzier); every number in a statement
  occurs in its cited passages; no investment-advice phrasing; and for a
  reviewer-confirmed conflict, passages from both sides are cited. It does not
  prove a statement is entailed by its quotes. Invalid output is stored as an
  `invalid` attempt and never published.
* **Citations kept:** every distinct verified quote, each as its own row (never
  joined into one quotation). A statement may cite one text unit with several
  quotes; they are numbered `quote_index` 1, 2, ... in the order the model gave
  them. Quotes identical after the normalization above are stored once.

### Publication, versions and recovery

* Publishing is one transaction that locks the event row, re-checks the evidence
  fingerprint, open conflicts and the published version, then supersedes the old
  version and inserts the new one with its citations. A draft whose event changed
  during generation is `stale` and requeued; one overtaken by a new conflict is
  `blocked`. No transaction is open during the model request. Old versions and
  their citations are kept; summary content is immutable (trigger).
* **Fingerprints.** A summary whose input fingerprint (evidence + prompt version +
  model + limits) matches the published one is not regenerated (`unchanged`, no
  call). Replays of an already-stored record therefore cost nothing.
* **Durable jobs** (`event_summary_job`, one per event and language) are
  independent of the ingestion ledger: pending / running (with a lease that
  expires if the process dies) / completed / failed / awaiting_review, with
  attempts, backoff (`SUMMARY_RETRY_BASE_MINUTES`, doubling) and the last error.
  `SUMMARY_MAX_ATTEMPTS` failures make a job `failed`; new evidence resets it.
* **Cost limits:** `SUMMARY_MAX_REQUESTS_PER_RUN` caps model requests per graph
  run (one ingested record) and per CLI command; work beyond it is queued, not
  dropped. Commands that can call the model need `--confirm-paid-calls`.
* If the process stops after an article is committed but before its summary job
  is written, `datasources-summary pending` / `backfill` still find the event:
  it has no summary, or its summary predates an origin/update member.

### Conflict review (CLI only)

```powershell
uv run datasources-summary review list                       # open reviews (--all includes resolved)
uv run datasources-summary review show --review-id <uuid>    # both claims, quoted evidence, every source
uv run datasources-summary review decide --review-id <uuid> --reviewer <name> `
    --decision not_a_conflict --explanation "the $3.4bn figure includes assumed debt"
```

| Decision | Effect |
| --- | --- |
| `not_a_conflict` | Review resolved; the summary reports the facts normally |
| `confirmed_conflict` | Review resolved; the regenerated summary must state both claims, each attributed, citing both sides; no winner is chosen |
| `different_events` | In the same transaction: the incoming revision's membership is **detached** (kept for audit), its segment gets its own provisional event, event statistics are recomputed; only then is regeneration queued for both events |
| `insufficient_evidence` | Nothing decided: the review moves to `in_review` and keeps blocking publication |

Every decision is appended to `event_review_decision` (reviewer, explanation,
resulting status, membership corrections, queued jobs); a resolved review
cannot be decided again. Once an event has no open review, its summary is
queued (`review_resolution`); it runs when a later record touches the event or
with `datasources-summary retry`. There is no HTTP write endpoint for reviews.

### Commands

```powershell
uv run datasources-summary status                  # jobs, versions, attempts (+ token totals), reviews
uv run datasources-summary pending --limit 20      # missing / behind / due / failed / awaiting review (no calls)
uv run datasources-summary show --event-id <uuid>  # every version, citation, attempt and review decision
uv run datasources-summary run-event --event-id <uuid> --confirm-paid-calls [--refresh]
uv run datasources-summary backfill --limit 10 --confirm-paid-calls   # events without a summary, or behind their members
uv run datasources-summary retry --limit 10 --confirm-paid-calls [--include-failed] [--all] [--event-id <uuid>]
```

Add `--sample` for sample data. These commands use job leases rather than the
graph-writer lock, so they can run next to the worker without generating the
same summary twice.

### Read API

`GET /api/events?limit=20&offset=0&state=<summary_state>` and
`GET /api/events/{event_id}` read persisted rows only. Sample events are hidden
unless `include_sample=true`. The detail response has the current headline,
overview and key points with statement-level citations (every quote with its
`quote_index` and source link, in the order the statement cites its units),
the version, `published_at`, `generation_reason` and limitations; the
`summary_state` (`published`, `awaiting_review`, `pending`, `failed`,
`not_available`, `not_requested`), `under_review` and `update_pending` flags; the
version history; open/resolved reviews; and every current source with whether
it was cited, given to the model, or added after the current version (a repeat
that arrived later is listed but never shown as evidence of an older version).
`/api/events` also accepts `symbol=<ticker>`: events with a current source linked to that company. Each
listed event carries `symbols` (the tickers of its current sources) and `latest_source` (`title`, `name`,
`record_type`, `published_at` and an http(s) `url` or `null`), which the feed shows before a summary exists.

Two more read-only endpoints serve the page:

* `GET /api/articles?limit=50&offset=0&source=all|news|sec&symbol=NVDA&include_sample=false` returns stored
  records as their current revision, newest first by source time. Each has the original headline, `provider`
  (`alpaca` / `sec_edgar`), `publisher` (news) or `issuer` (SEC), the source's `published_at`,
  `source_updated_at` and `retrieved_at`, the source-supplied `excerpt` (trimmed to 600 characters; SEC
  records have none), the original `url` (http/https only, otherwise `null`), form and item codes for
  filings, the number of stored `revisions`, `grouping_status` and the `events` it belongs to, plus a `total`.
* `GET /api/status` returns `collector` (from the worker's status file: `running`, `stale`, `stopped` or
  `not_running`, with the times of its last WebSocket subscription and SEC poll), the configured
  `features` flags, `database` availability and stored `counts` (records, events, grouping statuses).

The existing `/api/edgar` and `/api/alpaca` endpoints are unchanged. API responses are sent with
`Cache-Control: no-store`.

### Conflict notices

A published summary stays up while a conflict review is open, so both endpoints
also say when sources disagree. Everything comes from stored records
(`event_conflict_review`, `event_comparison.differences` and `.evidence`,
`event_membership`, `event_review_decision`); no model is called and no new
table was needed (`summary/conflicts.py`).

* **List**: each event has `conflict: {status, count, reviews}`: `count` is the
  number of disputes that still need a notice, `reviews` all reviews ever opened.
* **Detail**: `conflict` adds `summary_published_at`,
  `summary_not_updated_for_conflict` (true only when every active dispute was
  detected after the summary was published), `pending_update` (a final review
  decision is newer than the published summary and its regeneration is
  `queued` / `generating` / `failed` / `blocked_by_review`), and `disputes`
  (active first). A dispute has its review id and status, `detected_at`, the
  outcome (`decision`, `decided_at`; for `different_events` also
  `correction_complete` and `moved_to_event_id`), and `sources`: each with
  publisher, title, publication time, validated `http(s)` URL, its verbatim
  quotes and, for a precise claim pair, its claim (`{aspect, value}`).

A dispute is a precise `claim_pair` only when a well-formed `kind: "conflict"`
difference exists and both sides have a quote attributed to a source. The event
side's quotes are attributed by finding each quote in the compared units' text
(the refs stored in `evidence` are prompt-local); a quote found nowhere is
returned as `unattributed_quotes`. Otherwise the dispute is `quotes_only` with a
generic notice and no claim is attributed. Raw prompts, model explanations,
reviewer names and reviewer notes are never returned.

| Review | Display status | Notice |
| --- | --- | --- |
| `open`, `in_review` (incl. `insufficient_evidence`) | `under_review` | "Conflicting reports—under review" |
| `confirmed_conflict` | `disputed` | "Sources disagree" stays after the review closes; it does not say which source is right |
| `different_events`, report still an active member (or in the published summary) | `correction_pending` | correction not complete |
| `different_events`, report detached | `separated` | history only |
| `not_a_conflict` | `cleared` | history only |

An event takes the most urgent status of all its reviews
(`under_review` > `correction_pending` > `disputed` > `resolved` > `none`), so
closing one review never hides another. The web page (`web/render.js`) shows the
status line on the collapsed card and, under **Sources**, the notice below the
unchanged summary with each source's claim, quotes and link, when the conflict
was detected and when the summary was published. Open an event's sources
directly with `http://127.0.0.1:8000/#event=<event_id>&sample=1` (`sample=1`
only for sample events).

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

## Duplicate tracking

Tracking uses source identity only. Different accessions and different article
IDs always stay separate documents, even with identical text. Semantic
comparison never deduplicates: it links separate documents to a shared event
and records how they relate (repeated, update, conflict, uncertain).

| Provider | Identity | Rule |
| --- | --- | --- |
| SEC | `sec_edgar:<cik10>:<accession>` | Skip once `completed` (extracted and saved). Failures are never marked completed. 8-K/A has its own accession, so it is its own record. |
| Alpaca | `alpaca:<article id>` | Unchanged payload (same fingerprint for this id) → skip. Newer `updated_at` → new revision file. Older `updated_at` → stale, skip. Same `updated_at` but richer payload (content where there was only a summary) → processed. The fingerprint is compared only within the same id. |

Every decision is made by the single writer, in FIFO order, against the ledger.
An in-flight key set also keeps duplicate queue entries out, so REST backfill
and WebSocket delivery can't process the same revision concurrently. Tracking
persists in `output/state/ledger.json` and works across REST, WebSocket and
restarts.

**Crash safety.** Each processed record goes to a temp file in its destination
directory, is fsync'd, then renamed into place with `os.replace`. Only after
that is the ledger atomically rewritten the same way. The per-record files are
authoritative. On startup the ledger is reconciled against them: stray `*.tmp`
files are removed, records missing from the ledger are added, and ledger entries
whose file is missing are dropped so they get retried. A crash between writing a
record and updating the ledger therefore never produces a second output. A
corrupt ledger is moved aside and rebuilt. Output paths are deterministic per
record or revision.

## Output layout

Paths below are relative to `output/` (live) or `output/demo/` (demo). Both are git-ignored.

| Path | Content |
| --- | --- |
| `raw/sec/submissions/CIK##########/<sha12>.json` | Submissions JSON snapshot (only when it changed) |
| `raw/sec/submissions/CIK##########/historical/*.json` | Older submissions files used for recovery |
| `raw/sec/documents/<cik10>/<accession>/<primaryDocument>` | **Downloaded 8-K primary HTML** |
| `raw/alpaca/rest/<YYYYMMDD>/<HHMMSS>_<sha12>.json` | REST pages as received (+ request URL, no secrets) |
| `raw/alpaca/websocket/<YYYYMMDD>.jsonl` | WebSocket frames (diagnostic log) |
| `raw/alpaca/articles/<id>/<sha12>.json` | Each distinct article payload |
| `processed/sec/<cik10>/<accession>.json` | **Authoritative SEC records** |
| `processed/alpaca/<id>/<updated_at>_<sha12>.json` | **Authoritative article records, one per revision** |
| `state/ledger.json`, `state/ingest.lock` | Tracking ledger, cursors, worker lock |
| `errors/<provider>/<identity>/<timestamp>.json` | Per-record failures (retryable ones are retried) |
| `logs/ingest.log` | Rotating worker log |

The legacy `output/alpaca_news_stream.jsonl` written by `datasources-news-stream`
remains a diagnostic log only.

### Demo vs live

`datasources-ingest demo` (or `process-file --demo`) uses bundled fixtures,
marks every record `is_sample: true`, and writes only to `output/demo/`, with its
own ledger. Live runs refuse demo mode and fail with a configuration error when
credentials are missing. For the UI, `/api/alpaca?demo=true` or
`DATASOURCES_DEMO_MODE=true` returns the synthetic sample labelled
`live: false, demo: true`; otherwise missing credentials return HTTP 503 with a
configuration error.

## Common output schema (`schema_version` 1.0)

`record_id`, `record_type` (`sec_filing` | `news_article`), `provider`,
`source_record_id`, `publisher`, `issuer_name`, `author`, `symbols`, `title`,
`title_is_generated`, `summary_text`, `body_text`, `analysis_text`,
`content_status`, `content_scope`, `published_at`, `updated_at`, `retrieved_at`,
`original_url`, `filing_index_url`, `primary_document_url`, `exhibit_urls`,
`is_sample`, `provenance` (provider, transport, API endpoint, retrieval time,
requested symbols, adapter, dedup identity/fingerprint), `raw_file_refs`,
`warnings`, `sec_metadata`.

* SEC `title` is generated from metadata and labelled `title_is_generated: true`.
  `analysis_text` is a labelled metadata header followed by the full extracted text.
* Alpaca `analysis_text` = headline + summary (only if not already in the body) + body.
* Missing content stays visibly missing: `null` fields plus `content_status`.

**Example: SEC (demo run over the bundled *real* NVIDIA 8-K fixture; long text truncated here with "…")**

```json
{
  "schema_version": "1.0",
  "record_id": "sec_edgar:0001045810:0001045810-26-000073",
  "record_type": "sec_filing",
  "provider": "sec_edgar",
  "source_record_id": "0001045810-26-000073",
  "publisher": null,
  "issuer_name": "NVIDIA CORP",
  "author": null,
  "symbols": ["NVDA"],
  "title": "NVIDIA CORP Form 8-K filed 2026-08-26 (Items 2.02, 9.01)",
  "title_is_generated": true,
  "summary_text": null,
  "body_text": "UNITED STATES\n\nSECURITIES AND EXCHANGE COMMISSION\n\n… ## Item 2.02 Results of Operations and Financial Condition.\n\nOn August 26, 2026, NVIDIA Corporation … issued a press release …",
  "analysis_text": "[SEC EDGAR filing metadata]\nIssuer: NVIDIA CORP (CIK 0001045810)\nForm: 8-K\nAccession number: 0001045810-26-000073\nFiling date: 2026-08-26\nReport date: 2026-08-26\nAccepted: 2026-08-26T20:21:19+00:00\nItems: 2.02 Results of Operations and Financial Condition, 9.01 Financial Statements and Exhibits\nPrimary document: nvda-20260826.htm\nSource URL: https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/nvda-20260826.htm\nContent scope: primary document only; exhibits are linked, not extracted\n\n[Primary document text]\nUNITED STATES\n…",
  "content_status": "full_text",
  "content_scope": "primary_document_only",
  "published_at": "2026-08-26T20:21:19Z",
  "updated_at": null,
  "retrieved_at": "2026-09-22T04:15:47.759382Z",
  "original_url": "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/nvda-20260826.htm",
  "filing_index_url": "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/0001045810-26-000073-index.htm",
  "primary_document_url": "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/nvda-20260826.htm",
  "exhibit_urls": [
    "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/q2fy27pr.htm",
    "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/q2fy27cfocommentary.htm"
  ],
  "is_sample": true,
  "provenance": {
    "provider": "sec_edgar", "transport": "file",
    "api_endpoint": "file:data/submissions_CIK0001045810.json",
    "retrieved_at": "2026-09-22T04:15:47.759382Z", "requested_symbols": ["NVDA"],
    "adapter": "sec_edgar.submissions_8k", "pipeline_version": "ingest-1",
    "dedup": {"identity": "sec_edgar:0001045810:0001045810-26-000073", "revision_updated_at": null,
              "content_fingerprint": "33f12376…", "content_level": 2}
  },
  "raw_file_refs": ["file:data/submissions_CIK0001045810.json",
                    "raw/sec/documents/0001045810/0001045810-26-000073/nvda-20260826.htm"],
  "warnings": ["removed 1 hidden element(s)",
               "2 exhibit link(s) preserved but not extracted (content_scope=primary_document_only)"],
  "sec_metadata": {
    "cik": "0001045810", "accession_number": "0001045810-26-000073", "form": "8-K", "is_amendment": false,
    "filing_date": "2026-08-26", "report_date": "2026-08-26", "acceptance_datetime_raw": "2026-08-26T20:21:19.000Z",
    "items": ["2.02", "9.01"],
    "item_descriptions": {"2.02": "Results of Operations and Financial Condition", "9.01": "Financial Statements and Exhibits"},
    "primary_document": "nvda-20260826.htm", "primary_document_description": "8-K",
    "file_number": "000-23985", "film_number": "261323290", "act": "34", "size": 802000,
    "is_xbrl": true, "is_inline_xbrl": true,
    "exhibit_links": [
      {"url": "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/q2fy27pr.htm",
       "text": "Press Release, dated August 26, 2026, entitled \"NVIDIA Announces Financial Results for Second Quarter Fiscal 2027\""},
      {"url": "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/q2fy27cfocommentary.htm",
       "text": "CFO Commentary on Second Quarter Fiscal 2027 Results"}
    ],
    "exhibits_extracted": false,
    "extraction": {"status": "ok", "parser": "beautifulsoup4+lxml", "char_count": 3990, "table_count": 5,
                   "inline_xbrl_fact_count": 20,
                   "section_headings": ["Item 2.02 Results of Operations and Financial Condition.", "Item 9.01 Financial Statements and Exhibits.", "SIGNATURE"],
                   "warnings": ["removed 1 hidden element(s)"]}
  }
}
```

**Example: Alpaca (SYNTHETIC article from `tests/fixtures/synthetic_alpaca_ws_batch.json`, WebSocket transport)**

```json
{
  "schema_version": "1.0",
  "record_id": "alpaca:91000001:20260921T140500Z_6005df156fc0",
  "record_type": "news_article",
  "provider": "alpaca",
  "source_record_id": "91000001",
  "publisher": "benzinga",
  "issuer_name": null,
  "author": "Synthetic Reporter",
  "symbols": ["NVDA", "BRK.B"],
  "title": "Synthetic Co & NVIDIA announce test partnership",
  "title_is_generated": false,
  "summary_text": "Synthetic summary: revenue won't decline, per the test fixture.",
  "body_text": "First paragraph with revenue of $1.2 billion, up 12%.\n\n- Point one\n- Point two is not negative\n\nQ1 | $(3.4)",
  "analysis_text": "Synthetic Co & NVIDIA announce test partnership\n\nSynthetic summary: revenue won't decline, per the test fixture.\n\nFirst paragraph with revenue of $1.2 billion, up 12%.\n\n- Point one\n- Point two is not negative\n\nQ1 | $(3.4)",
  "content_status": "full_text",
  "content_scope": "provider_article_payload",
  "published_at": "2026-09-21T14:00:00Z",
  "updated_at": "2026-09-21T14:05:00Z",
  "retrieved_at": "2026-09-21T14:05:02Z",
  "original_url": "https://news.example.test/synthetic-nvda-partnership",
  "filing_index_url": null,
  "primary_document_url": null,
  "exhibit_urls": [],
  "is_sample": false,
  "provenance": {
    "provider": "alpaca", "transport": "websocket",
    "api_endpoint": "wss://stream.data.alpaca.markets/v1beta1/news",
    "retrieved_at": "2026-09-21T14:05:02Z", "requested_symbols": ["NVDA"],
    "adapter": "alpaca.news_websocket", "pipeline_version": "ingest-1",
    "dedup": {"identity": "alpaca:91000001", "revision_updated_at": "2026-09-21T14:05:00Z",
              "content_fingerprint": "6005df15…", "content_level": 2}
  },
  "raw_file_refs": ["raw/alpaca/websocket/20260921.jsonl", "raw/alpaca/articles/91000001/6005df156fc0.json"],
  "warnings": [],
  "sec_metadata": null
}
```

## Documented schemas vs. repository fixtures

| Source | Documented | Observed in this repo |
| --- | --- | --- |
| SEC submissions | Entity metadata + `filings.recent` compact columnar arrays (≥1 year or 1,000 filings) + `filings.files` for older filings | `data/submissions_CIK0001045810.json` (**real**): 16 aligned columns × 1,001 rows, including `isXBRLNumeric`, which the repo's field dictionary does not describe; `filings.files` lists one historical file (1998–2020). `reportDate`/`items` may be empty strings, which are normalised to `null`. `acceptanceDateTime` is ISO-8601 with `Z`. |
| 8-K primary document | HTML / inline XBRL | `data/8k/*.htm` (**real** NVIDIA 8-Ks): XHTML with an `<?xml?>` prolog, a hidden `ix:header` inside `display:none`, visible `ix:nonNumeric` facts, layout tables, relative exhibit links split across several `<a>` tags. |
| Alpaca REST news | `id, headline, author, created_at, updated_at, summary, content, url, symbols, source, images`; `next_page_token`; `limit` 1–50 | Live responses checked during testing had exactly these keys. `data/alpaca_news_sample.json` is **synthetic** (example.com URLs); it has the same keys and is used only for demo mode. |
| Alpaca WebSocket news | `T="n"` + the same article fields (no `images`); `success`/`subscription`/`error` control frames | Live stream sent `success: connected`, `success: authenticated`, `subscription` as documented. `tests/fixtures/synthetic_*` are **synthetic** (see [tests/fixtures/README.md](tests/fixtures/README.md)). |

## Tests

```powershell
uv run pytest                                      # offline fixtures + mocked network; database tests are
                                                   # skipped unless PGDATABASE names an isolated test database
$env:PGDATABASE = "stock_news_conflict_test"; uv run pytest   # database tests too (needs migrations 002-005)
$env:RUN_LIVE_TESTS = "1"; uv run pytest -m live   # opt-in: real SEC + Alpaca (needs .env)

# Bounded live end-to-end test: real NVDA news + one real 8-K -> PostgreSQL -> read back -> replay
uv run datasources-db smoke --symbol NVDA --max-articles 5 --lookback-hours 72
```

Database tests delete every sample row they may have written, so they never run
against `stock_news` (which keeps retained sample records). Before any setup,
`conftest.verify_isolated_database` checks the database the **server** reports
(`current_database()`, not just `PGDATABASE`) and refuses a protected name, a
name without `test`, a mismatch with the configured name, or any database that
holds live (non-sample) documents; `cleanup_sample_rows` repeats the check. To
create an empty test database with the full schema, copy a migrated empty one
(`CREATE DATABASE stock_news_conflict_test TEMPLATE stock_news_test_fresh`) or
create one and run `uv run datasources-db migrate` against it.

`tests/test_conflict_notice.py` replays the fictional Veltrani Photonics
acquisition (`tests/fixtures/synthetic_conflict_notice_veltrani.json`: the
recorded comparison judgments and summary statements) through the real graph
with scripted backends and checks the notice for every review outcome, several
disputes, a pending regeneration, missing summaries, incomplete evidence, sample
filtering, that reads make no model call (model clients and non-loopback
sockets fail the test), and, in headless Edge/Chrome when available, that the
page renders the notice and shows hostile article text and `javascript:` links
as inert text.

`datasources-db smoke` uses a **bounded historical** REST window, so it does not
wait for a live news event, and it writes its files to a temporary directory
while keeping the real records in the database. It fails loudly if it is pointed
at the wrong database, if a record cannot be read back, or if replaying the same
payloads changes any row count. When Alpaca or SEC is unreachable it says so and
does not report an unavailable API as a successful live test.

`tests/test_db_persistence.py` covers the mapping offline and, against a real
database, the round trip, replay idempotency, the revision model for changed
payloads, 8-K/A staying a separate filing, identical text under different ids
staying separate, unknown tickers being reported rather than invented, the
storage layer alone creating no embeddings, and a **rollback test**: a related
insert that violates a real check constraint must leave no document or revision
behind. Every record it writes is `is_sample = true` and is deleted afterwards.

Semantic comparison is tested at three levels, all offline apart from the local
database, with no model download and no API call:

* `tests/test_similarity_offline.py` — text units on the real NVIDIA 8-Ks
  (cover/signature/safe-harbour/XBRL-only exhibits skipped, Item segmentation,
  cross-reference Items, traceable spans, token limits, overlap, nothing
  truncated), limited evidence and skip reasons, profile identity, vector
  validation, the prompt's escaping of untrusted text, the response schema,
  reference and evidence validation, the Anthropic adapter against a fake client
  (request shape, refusal, `max_tokens`, invalid JSON, timeouts), RRF fusion and
  plausibility (no lexical gate), anchor precedence, routing, the resolution
  rules, request budgets, and pending vs partial.
* `tests/test_similarity_pipeline.py` — the **real compiled graph** against
  PostgreSQL with a deterministic hashing embedder and a scripted model:
  the node path of the no-candidate route (no model call) and of the compare
  route; comparison happening **before** the incoming document exists in the
  database; a second sequential article finding the first, also inside one
  batch with a malformed record in the middle; model and embedding failures
  stored as `pending` and completed by retry without duplicate memberships;
  same-source replay (no new rows, no model call); identical text from two
  sources staying two documents (linked by the exact-content shortcut); a
  changed revision keeping its history and counting once; profile isolation
  (same and different dimensions); sample/live isolation (live rows only inside
  a rolled-back transaction); a multi-event filing and overlapping chunks;
  no transitive merging; budget exhaustion; every labelled example; the
  bootstrap's index-then-group passes without self-matches; partial index
  coverage; a transaction rollback; a commit followed by a file-write crash; and
  the writer lock.
* `tests/test_similarity_cli.py` — `backfill`, `retry`, `status` and `inspect`
  end-to-end on sample data, the lock refusal, and `evaluate` refusing to spend
  money without `--confirm-paid-calls`.

Event summaries are tested the same way, offline apart from the local
database, with scripted model output:

* `tests/test_summary_offline.py` — settings (off by default, separate model),
  every routing rule, validation (unknown refs, fabricated quotes, missing
  citations, wrong ids, altered figures, advice, one-sided confirmed conflicts,
  the documented quote normalization), prompt escaping and filing attribution,
  the Anthropic adapter's request shape and typed errors, bounded evidence
  selection (every source first, whole passages, recorded omissions, duplicates
  once) and fingerprints.
* `tests/test_event_summary.py` — through the real graph and PostgreSQL: a
  first summary for a single article; an update making version 2 while version 1
  and its citations stay intact; repeats and exact copies making no call yet
  appearing as sources; a two-Item filing summarized per event from its own
  evidence (and cross-event evidence refused at publication); a conflict blocking
  publication (also on an event with no summary) and replays not duplicating the
  review; every review decision including the membership correction and
  regeneration; uncertain relations excluded; failures and invalid output
  keeping the article and grouping; replay, crash recovery and a failed update
  retried on a later repeat without duplicate calls; stale drafts, a rival
  publisher and a held lease; the per-run request cap; the partition trigger; the
  read endpoints' published / pending / review states; and the CLI, including the
  paid-call consent check.

These are workflow tests. They say nothing about summary quality: that needs a
real run on a real event (`run-event ... --confirm-paid-calls`) read by a
person against its cited sources.

The scripted model proves orchestration, not accuracy. The real backend is
evaluated separately and explicitly:

```powershell
uv run datasources-similarity evaluate --limit 10 --confirm-paid-calls
```

It runs the configured model (one paid call per example) and the local
embedding model on `tests/fixtures/synthetic_similarity_labelled_examples.json`
(paraphrase, missing price, different fiscal quarters, completion, cancellation,
comparable-figure conflict, denial, different metric of the same results, vague
headline, different partner) and reports accuracy per expected label, a
confusion table, cosine similarity per pair and token usage. It writes nothing to
the database. The examples are synthetic and few: treat the result as a smoke
signal, not a benchmark.

Offline tests cover SEC array parsing and URL generation, primary 8-K
extraction (tables, visible inline XBRL, hidden content), download and
extraction failures staying retryable, 403 without retries, error pages,
redirect and host validation, size limits, same-accession skips, identical text
across different accessions and article IDs, repeated REST/WebSocket delivery,
new revisions, stale arrivals, richer payloads, restarts, interrupted writes,
corrupt ledgers, polling that bypasses the 12-hour cache, provenance and URL
preservation, demo/live separation, REST pagination, WebSocket callbacks, the
process lock, and clean shutdown.

## Limitations

* **SEC extraction covers the primary 8-K document only.** Exhibits (for example
  EX-99.1 press releases, which often hold the financial detail) are linked, not
  downloaded or extracted.
* **Alpaca reconnect recovery is bounded** by the overlap window, the 72 h
  recovery cap and the page cap. Article revisions published and superseded
  while the worker was down may not be recovered.
* SEC recovery is capped by `SEC_RECOVERY_MAX_DAYS` and `SEC_MAX_HISTORICAL_FILES`.
* Nested list indentation is flattened, and empty table cells are dropped (cell
  order is kept).
* `broker` auth mode is untested against a live Broker account.
* The web server's own `/api/edgar` requests are not counted in the worker's SEC
  request budget (they are cached for 12 h and triggered manually).
* `langgraph`'s dependencies cap `websockets` below 17, so the lock resolves
  `websockets` 16.1.1 (the project only requires `>=15`).
* **Tickers other than the configured symbols are not linked.** An article that
  also mentions `AAPL` links only the companies present in `company_symbol`;
  unknown tickers are counted (`db_unresolved_symbols`) and kept in
  `provenance.requested_symbols` rather than guessed into new `company` rows.
* Records completed in the file ledger **before** the database existed are not
  re-persisted by a normal run (the ledger already calls them done). Run
  `uv run datasources-db backfill` once for those.
* Collection cursors stay in `output/state/ledger.json`; the `ingestion_state`
  table is left empty so there is one source of truth for them.
* **Semantic grouping is an MVP.** Retrieval defaults (7-day window, 0.78 cosine
  floor, top-3 lexical hits, 6 candidate events) are experimental starting points
  calibrated on 58 stored NVIDIA revisions, not tuned values. Comparison accuracy
  depends on the model and has only been exercised with scripted responses here
  (see `evaluate`).
* Without a matching candidate, provisional events are proposed per segment
  without a model: an 8-K with two unrelated substantive Items gets two events,
  but a long multi-topic article is one event, and two Items about one
  occurrence (for example 1.01 for a merger agreement and 8.01 announcing the
  same deal) start as two.
  Events are never merged automatically; only a single-source provisional event
  is superseded, and only when a later comparison links its segment elsewhere.
* The lexical channel ranks with PostgreSQL's `ts_rank_cd` on an OR-query; it has
  no corpus-level IDF, so common same-company words raise many scores. Only its
  rank is used for plausibility.
* Candidates whose own analysis is still pending are not compared; the pair is
  compared when that candidate is analysed or retried.
* Semantic retrieval is exact (sequential scan over the company/time-scoped
  pool). The `embedding.embedding` column has no fixed dimension, so an ANN
  index (HNSW / IVFFlat) would need a per-profile expression index; that is a
  later step if the pool grows large.
* Inline model calls bound ingestion throughput (see [Throughput](#throughput)).
* `demo --with-database` rows are sample data; the database tests delete all
  sample rows when they finish.
* **Event summaries are validated for traceability, not truth.** Quotes must
  occur in the cited passages and numbers must appear there, but whether a
  statement is fully supported by its quotes is not checked automatically.
  Summary quality has only been exercised with scripted output here.
* The figures check is strict: a statement that rewrites "first quarter" as "Q1"
  or "three" as "3" is rejected and retried, and a retry does not tell the model
  why the previous output was rejected.
* Only English summaries are generated; there is no translation.
* Summary work queued by a review decision, a deferral or a failure runs when a
  later record touches the event or with `datasources-summary retry`; the
  worker has no periodic summary sweep.
* A conflict review is created per comparison; a later revision of the same
  article does not close an older revision's open review automatically.
* Reprocessing through `datasources-similarity retry`/`backfill` does not
  summarize inline; `datasources-summary backfill` picks those events up.

## Postman WebSocket check

1. Create a new WebSocket request to `wss://stream.data.alpaca.markets/v1beta1/news`.
2. Add the headers `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY`.
3. Connect and send `{"action":"subscribe","news":["NVDA"]}`.

Stop `datasources-ingest` first: Alpaca may reject a second concurrent connection (error 406).

## References

* [SEC EDGAR application programming interfaces (submissions API)](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
* [SEC developer resources (fair access, 10 requests/second)](https://www.sec.gov/about/developer-resources)
* [Example NVIDIA submissions JSON](https://data.sec.gov/submissions/CIK0001045810.json)
* [Alpaca REST news](https://docs.alpaca.markets/us/reference/news-3)
* [Alpaca real-time news WebSocket](https://docs.alpaca.markets/us/docs/streaming-real-time-news)
* [Alpaca market data authentication](https://docs.alpaca.markets/us/docs/about-market-data-api)
* [LangGraph](https://docs.langchain.com/oss/python/langgraph/overview)
* [uv dependency management](https://docs.astral.sh/uv/concepts/projects/dependencies/)
* [uv with PyTorch (CPU-only index)](https://docs.astral.sh/uv/guides/integration/pytorch/)
* [Sentence Transformers installation](https://sbert.net/docs/installation.html)
* [BAAI/bge-small-en-v1.5 model card](https://huggingface.co/BAAI/bge-small-en-v1.5)
* [pgvector](https://github.com/pgvector/pgvector)
* [PostgreSQL full-text search](https://www.postgresql.org/docs/current/textsearch.html)
* [Reciprocal rank fusion (Cormack, Clarke & Büttcher, SIGIR 2009)](https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf)
* [Claude API structured outputs](https://platform.claude.com/docs/en/build-with-claude/structured-outputs)
