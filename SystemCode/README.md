# Data Sources

Node 1 of the pipeline collects **SEC EDGAR 8-K filings** and **Alpaca news** for a
configurable list of symbols (default `NVDA`), preprocesses them with a small
deterministic **LangGraph** graph, and writes one versioned JSON record per
filing / article revision under `output/`. No database, no LLM calls, no
summarisation, sentiment or recommendations.

The original pieces still work as before:

* `uv run datasources-server`: web UI + `/api/edgar`, `/api/alpaca`, `/api/companies`
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

Makefile shortcuts: `make ingest`, `make ingest-once`, `make ingest-demo`, `make test`.

## How it works

```text
SEC poller (per company, ~60 s) ─┐  fresh submissions JSON (bypasses the 12 h UI cache,
                                 │  then refreshes it), aligned-array validation,
                                 │  8-K / 8-K/A selection within the scope window
Alpaca WebSocket (existing       │
  alpaca_news.client) ───────────┤──> bounded queue ──> single writer
Alpaca REST backfill (startup,   │                        1. ledger decision (source identity)
  every (re)subscription) ───────┘                        2. SEC: download primary 8-K HTML, save raw
                                                          3. LangGraph  START -> prepare_data -> END
                                                          4. atomic write processed/<...>.json
                                                          5. atomic ledger update
```

* **Graph** (`src/datasources/ingest/graph.py`): a `StateGraph` with typed state
  and a single `prepare_data` node, compiled once and reused. The node validates
  each `SourceEnvelope` with Pydantic, picks the adapter from the envelope's
  explicit `provider`, and returns prepared records plus per-record errors. One
  malformed record never discards the rest of the batch. Polling, WebSocket,
  downloads, duplicate tracking and file writing all run outside the graph.
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

## Duplicate tracking

Tracking uses source identity only. There is no semantic or similarity
deduplication, no embeddings, and no merging of different records about the
same event. Different accessions and different article IDs always stay
separate, even with identical text.

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
uv run pytest                                      # offline: fixtures + mocked network
$env:RUN_LIVE_TESTS = "1"; uv run pytest -m live   # opt-in: real SEC + Alpaca (needs .env)
```

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
