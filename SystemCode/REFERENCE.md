# StockEvent Lens: implementation reference

Detail behind [README.md](README.md): behaviour, design choices and formats that you need when changing or
debugging the system, not to run it. Database schema, table mapping and SQL inspection queries are in
[migrate/README.md](migrate/README.md); evaluation in [evaluation/README.md](evaluation/README.md).

## Command reference

```powershell
# Ingestion
uv run datasources-ingest run [--once] [--no-sec] [--no-alpaca]   # the worker; --once = one pass, then exit
uv run datasources-ingest demo [--with-database]                  # offline demo over bundled fixtures (sample rows)
uv run datasources-ingest process-file <file> --provider alpaca|sec_edgar [--demo]
uv run datasources-ingest graph                                   # print the compiled LangGraph
uv run datasources-ingest status                                  # ledger counts and cursors

# Database
uv run datasources-db check | migrate [--dry-run|--adopt-baseline] | verify | inspect
uv run datasources-db register-companies [--dry-run] [--symbols MSFT,AAPL] | relink-companies [--dry-run]
uv run datasources-db backfill                                    # store output/processed records (idempotent)
uv run datasources-db smoke --symbol NVDA --max-articles 5 --lookback-hours 72   # bounded live end-to-end test

# Event grouping (add --sample for sample data)
uv run datasources-similarity check | download-model | status
uv run datasources-similarity backfill --limit 100 [--index-only]   # repeat until "selected": 0
uv run datasources-similarity retry --limit 100                     # pending / partial analyses that are due
uv run datasources-similarity inspect --revision-id <uuid>          # units, candidates, scores, comparisons

# Event summaries (paid commands need --confirm-paid-calls)
uv run datasources-summary check | status | pending | show --event-id <uuid>
uv run datasources-summary run-event --event-id <uuid> --confirm-paid-calls [--refresh]
uv run datasources-summary backfill|retry --limit 10 --confirm-paid-calls
uv run datasources-summary review list | show --review-id <uuid> | decide --review-id <uuid> --reviewer <name> --decision <d> --explanation "..."

# Standalone pieces
uv run datasources-server                                         # web UI + read API only (no collectors)
uv run datasources edgar NVDA                                     # backend CLI
uv run datasources-news-stream                                    # standalone Alpaca WebSocket printer (not with the worker)

# Evaluation: see evaluation/README.md
uv run datasources-eval report <run> | compare
```

Make shortcuts: `make ingest`, `make ingest-once`, `make ingest-demo`, `make graph`, `make test`, `make db*`,
`make similarity-status|backfill|retry`, `make summary-check|status|pending|reviews`. Paid summary commands have no
shortcut on purpose.

## Running the application

### Company coverage and registration

The list lives in [data/ticker_cik_map.json](data/ticker_cik_map.json) (ticker -> CIK, name, market), the same
file the SEC pollers resolve CIKs from. CIK, ticker and exchange were checked against SEC EDGAR on 2026-10-07: the
SEC ticker table (`company_tickers_exchange.json`), each company's submissions JSON and the registrant tagged on its
latest 8-K cover. That record is kept in
[tests/fixtures/sec_company_identifiers.json](tests/fixtures/sec_company_identifiers.json) and
`tests/test_company_coverage.py` checks the list against it. Names are display labels: SEC's own spellings differ
in case and punctuation (`AMAZON COM INC`), and a company row that already exists keeps its stored name.

**Registering the companies.** Articles and filings are linked to companies through `company` / `company_symbol`
(an article is linked to every registered company among its tickers). Registration copies the list into those
tables:

```powershell
uv run datasources-db register-companies --dry-run   # READ ONLY: what would be added, and any conflict
uv run datasources-db register-companies             # add what is missing; repeatable
uv run datasources-db register-companies --symbols MSFT,AAPL   # only some of them
```

It only inserts. A company that already exists (matched by CIK, for example an issuer created by an earlier SEC
filing) and an existing ticker row are kept as they are; documents, revisions and company links are never touched,
so existing NVDA data stays exactly as it is. A ticker already registered for another company is reported as a
conflict (exit code 1) and left alone. It is not a migration and changes no schema, so it also works on a
`stock_news` whose baseline is not adopted yet. Registration starts no collector.

Links are made when an article is stored, so articles stored **before** a company was registered are not linked
to it. `relink-companies` adds those links from the tickers in each article's stored original payload, resolved
exactly as ingestion resolves them:

```powershell
uv run datasources-db relink-companies --dry-run   # READ ONLY: the links each stored article would gain
uv run datasources-db relink-companies             # add them (--sample for the sample partition)
```

It only inserts `revision_company` rows: no link is removed, and articles, events, summaries and reviews are not
changed (no model call). SEC filings need nothing: they are linked to their issuer by CIK. It is safe to run while
the worker runs, and repeatable.

**Collecting all 11 companies** (not enabled by default; `.env.example` keeps `INGEST_SYMBOLS=NVDA`):

```powershell
uv run datasources-db register-companies --dry-run
uv run datasources-db register-companies
# in .env:  INGEST_SYMBOLS=NVDA,MSFT,AAPL,GOOGL,AMZN,META,NFLX,DIS,JPM,V,GS
#           INGEST_LIVE_ONLY=true     (optional: no historical catch-up, see Live-only collection)
uv run datasources-dev --check        # lists the symbols; warns about any without a company row
uv run datasources-dev
```

What changes with more symbols:

* **SEC**: one poller per company, staggered across `SEC_POLL_INTERVAL_SECONDS`, all sharing the one
  `SEC_REQUESTS_PER_SECOND` budget (2/s): about 11 submissions requests a minute plus the 8-K documents. A failing
  company never stops another, and each company keeps its own scope floor and retry state, so a company added
  later inspects its own last `SEC_INITIAL_LOOKBACK_DAYS` (30) on its first poll.
* **Alpaca**: one WebSocket subscription and one REST recovery window cover all configured symbols. An article
  tagged with several companies is one article id: it is stored once and linked to each registered company it
  names; tickers that are not registered are counted (`db_unresolved_symbols`), never invented.
* **Paid model calls**: with `SIMILARITY_ENABLED` / `SUMMARY_ENABLED` on, each new record can make paid calls (see
  [Model calls and cost](#model-calls-and-cost)), and 11 large companies bring many times NVIDIA's article volume.
  The worker has no run-wide cap. Collect with both switched off first, or measure with the
  [bounded live test](#bounded-live-test-paid-hard-capped) before turning them on.

Coverage, registration and a reader's watchlist are separate: `INGEST_SYMBOLS` decides what is collected, the
registered rows decide what can be linked, and following a company on the page only filters what is stored.

### Model calls and cost

Page views, API polling and extra browser tabs never call a model and never start
collectors: the page only reads what is stored. Model calls happen only inside
the ingestion worker, for records it ingests, and in the startup summary catch-up:

* `SIMILARITY_ENABLED=true` with `SIMILARITY_COMPARISON_BACKEND=anthropic`: each new article or filing can send
  up to `SIMILARITY_LLM_MAX_CALLS_PER_RECORD` comparison requests (default 3). Each request can be retried by
  the SDK up to `SIMILARITY_LLM_MAX_RETRIES` times.
* `SUMMARY_ENABLED=true`: each new record can send up to `SUMMARY_MAX_REQUESTS_PER_RUN` summary requests
  (default 5), each retried up to `SUMMARY_LLM_MAX_RETRIES` times.
* At startup the worker recovers Alpaca news since its last sync (at most `ALPACA_RECOVERY_MAX_HOURS`) and SEC
  filings from its lookback window that are not stored yet. Recovered records go through the same steps, so
  the first start after a long break can make many more calls than a normal hour.
* `SUMMARY_ENABLED=true` and `SUMMARY_STARTUP_CATCHUP_DAYS` > 0 (default 3): every `datasources-dev` start, **with
  or without collectors**, runs the [startup summary catch-up](#startup-summary-catch-up) once: at most
  `SUMMARY_STARTUP_CATCHUP_MAX_REQUESTS` summary requests (default 10), SDK retries counted and capped too.

With the defaults, one record can therefore cost up to 3 × (1 + 2) + 5 × (1 + 2) = 24 HTTP requests in the
worst case; a record with no plausible candidate usually costs one summary request, and one that matches an
event one comparison and one summary request. **The worker has no run-wide cap**: the Alpaca recovery alone
can bring up to 40 pages of 50 articles after a long break (72 h at most). Before enabling paid steps for the
worker, run the [bounded live test](#bounded-live-test-paid-hard-capped), which has a hard cap.

To run without paid calls, set `SIMILARITY_ENABLED=false` and `SUMMARY_ENABLED=false`, or use
`uv run datasources-dev --no-collectors` with `SUMMARY_STARTUP_CATCHUP_DAYS=0` to browse stored data only.

**Deferred work is not scheduled while running.** The launcher adds no retry loop; only the startup catch-up
runs, once per start. Records whose grouping is still pending wait for `uv run datasources-similarity retry`
(run it with the worker stopped) or a bounded run with `--source stored`. Queued summaries from the catch-up
window are retried at the next start; failed ones (attempts used up) and older ones wait for
`uv run datasources-summary retry --confirm-paid-calls` (add `--max-model-attempts N` for a hard cap), or for a
later record that touches the same event. These commands can make paid calls.

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
* To stop: Ctrl+C in the launcher window, once, and wait for `web stopped` / `ingest stopped` (the worker may
  finish a model call first: up to `SIMILARITY_LLM_TIMEOUT_SECONDS` per attempt). Records already queued but
  not written stay unmarked and are collected again on the next start.

### Locks and worker rules

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

### Startup summary catch-up

Each `uv run datasources-dev` start, with or without `--no-collectors`, starts a one-shot `[summary]` task next
to the web server (which itself still never calls a model). It looks at live events with an article published in
the last `SUMMARY_STARTUP_CATCHUP_DAYS` days (default 3), newest first, and summarizes the ones that need it:

| Event | What the catch-up does |
| --- | --- |
| No published summary | First summary (queued as a durable job first) |
| Summary older than a newer member | New version |
| Pending job whose retry backoff is over | Retried |
| `running` job whose lease expired (a process was stopped mid-summary) | Taken over and finished |
| Pending job still in its retry backoff | Left for a later start (no call) |
| `failed` job (attempts used up), open conflict review | Left alone (`summary retry --include-failed`, `summary review`) |
| Article not grouped into an event yet | Counted and reported only; grouping does not run here |

At most `SUMMARY_STARTUP_CATCHUP_MAX_REQUESTS` requests (default 10) per start, with a hard cap on HTTP attempts
that counts SDK retries; the rest stays queued for the next start. Ctrl+C lets the summary in progress finish
(within 90 s) and starts no other; a summary cut off by the timeout is taken over at a later start once its lease
expires. The task logs one line per event and a total (requests, attempts, tokens) to the console and to
`output/logs/summary_catchup.log`, then exits; the other services keep running. It is off with
`SUMMARY_ENABLED=false`, `SUMMARY_STARTUP_CATCHUP_DAYS=0`, without the database, or without `ANTHROPIC_API_KEY`;
`--check` shows which on its *Summary catch-up* line.

### Live-only collection (no backfill, no recovery)

`INGEST_LIVE_ONLY=true` makes the worker collect only what is published while it runs:

| | Normal (`false`, default) | Live-only (`true`) |
| --- | --- | --- |
| Start | Alpaca REST backfill since the saved cursor (bounded); SEC filings since each company's saved scope floor (a new company: the last `SEC_INITIAL_LOOKBACK_DAYS`) | A fresh UTC cutoff at **every** startup (logged, shown in `worker_status.json`, never saved); nothing published before it is collected |
| SEC | Every filing in scope not stored yet, including older `filings.files` when the floor needs them | Only filings accepted at or after the cutoff (a filing without an acceptance time: only if dated the cutoff's day or later); no older submissions files; pending retries of older filings wait |
| Alpaca stream | Everything delivered | Only articles published at or after the cutoff; an update to an older article is skipped |
| Reconnect | REST backfill since the cursor | No backfill; the news cutoff moves to the reconnect, so the stream resumes with newly published articles only |
| Saved state | The Alpaca cursor advances; floors are set | Neither read nor changed: the cursor, the SEC scope floors and pending retries stay as they were |

Polling while running, duplicate prevention, storage, grouping and summaries are the same in both modes. Records
published while the worker is stopped or the stream is disconnected are **not** collected in live-only mode.
Setting `INGEST_LIVE_ONLY=false` later resumes normal recovery from the saved state; that first start then
backfills Alpaca since the saved cursor (at most `ALPACA_RECOVERY_MAX_HOURS`) and re-inspects each company's saved
SEC scope. Lowering `SEC_INITIAL_LOOKBACK_DAYS` or `ALPACA_INITIAL_LOOKBACK_HOURS` to zero does not give this
behaviour: they only apply to a company or a cursor that has no saved state yet, SEC dates have no time of day, and
reconnect backfills and stream updates of older articles would still be processed. The mode applies to the worker
(`datasources-ingest run`, also under `datasources-dev`); `datasources-ingest bounded-run --source collect` and
`process-file` keep their own behaviour. `uv run datasources-dev --check` shows the mode on its *Recovery* line,
with the saved state it keeps.

### The page

**News summaries** (`/`) is a single feed for readers, newest story first, in two views:

* **All companies**, optionally narrowed to one company with the *Company* selector (`#company=NVDA`).
* **My watchlist** (`#view=watchlist`): stories about any company the reader follows. **Follow companies** in
  the header opens a list of the supported companies (those registered with a CIK and a ticker; ones the
  collectors are not configured for are marked *Not collected now*). The watchlist is **saved in this browser on
  this device only** (`localStorage`, by SEC CIK): it is not an account setting and does not sync anywhere. It
  survives reloads, the last view chosen is reopened, and other tabs follow changes. An empty watchlist shows a
  message with *Choose companies to follow*. A saved value that cannot be read, or names a company the site no
  longer offers, is ignored with a note and cleaned up on the next change; a browser that blocks storage keeps the
  watchlist for the current page only. The filter is sent to the API (`company=<cik>`), which applies it before
  paging, so no story is missed and a story about several followed companies is one card. Followed companies'
  tickers are starred on the cards. Following changes nothing on the server and calls no model.

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
* **Story & timeline** opens the story's own page, `/events/<event id>` (stable; add `?sample=1` for a sample
  story). It shows the headline, the companies, the conflict/review status, the latest published summary with the
  quote behind each statement, the original sources, and a **timeline** built from stored records only: *First
  report* (the report the story was created from), *Additional coverage* (repeats known information; *Revised
  report* when a source re-published without new information), *Substantive update* and *Conflicting report*
  (grouping's stored labels, with the verbatim quotes grouping verified), *Conflict detected* and *Review
  completed* (when they were recorded; reviewer names and notes are never shown), and *AI summary published /
  revised*. Each entry says which time it uses: the source's publication or update time, a filing's SEC acceptance
  time (or its filing date, shown as a date only), or when the site detected, decided or published something. A
  report with no stated time is listed under *Without a stated time*, with the collection time labelled as such.
  A filing that covers several stories lists only its sections about this one. *Back to the news* returns to the
  feed view the reader came from, at that story. Unknown or malformed links, loading and API errors have their own
  states.
* **Search and filters** sit above the feed and live in the address, so a filtered view can be shared:
  `q=<text>` (every word must start a word in the headline, the summary, a source title, or a company name or
  ticker; case-insensitive, special characters literal, at most 200 characters), `when=24h|7d|30d|custom` (with
  `from`/`to=YYYY-MM-DD`), `filing=1` (*With SEC filings*) and `disagree=1` (*Sources disagree*: an open review, a
  confirmed disagreement or an unfinished correction). The server applies them in SQL before paging, together with
  the company or watchlist view.
* Stories are grouped under **day headers** by their latest report. **New** and **Updated** mark stories since the
  reader's last visit, remembered in this browser only (`localStorage` key `stocknews.visits`). A visit ends after
  30 minutes with no open feed page; *Mark all as read* moves the baseline to now. The times are the sources' own
  report times, and the first visit ever shows no markers.
* A card with an SEC filing among its sources shows a filing line, for example "SEC 8-K: Earnings and financial
  results (Item 2.02)": plain names for the 8-K items, with the official caption as the tooltip. Item 9.01 (the
  exhibit list) is left out when the filing reports anything else. A card also shows the **latest update**: the
  newest substantive update's time, source and what it changed.
* The **Tour** (header button) is a spotlight walk-through of the feed or the story page. The feed opens it once by
  itself on the first visit in a browser (never in sample mode or when a link opens a story); which tours were
  offered is remembered in this browser only (`stocknews.tour`).
* Sample records never appear in the feed. Add `#sample=1` to the address for demos and tests; sample
  stories are then marked *Sample*.

**Data tools** (`/tools.html`, linked from the header of the feed and of each story page) keeps the developer views:
collector status, the live processing activity, every
stored news article and SEC filing (original headline, publisher or issuer, the source's own times, the
excerpt supplied with the article, labelled as not AI-generated, and its grouping status), with company,
source and sample filters, and the original Alpaca / EDGAR data-dictionary **source explorer**, which makes
live requests only when you press a button.

**Processing activity** (`/tools.html#activity`) shows, live, what the ingestion worker and the
[startup summary catch-up](#startup-summary-catch-up) are doing: the article, filing or story in progress with
each step (*Looking for related stories*, *Comparing with N stories*, *Saving and grouping*, *Writing the
summary*), which steps use an AI model, how long the running step has taken, and the last 25 finished items with
their results (for example "same story, new details", "published as version 2" or "rejected by the citation
checks"). The section refreshes every 2 seconds while the page is visible. While something is being processed,
the news page shows the running step in one line under its heading. Each process writes its own
`output/state/activity_worker.json` / `activity_summary.json` (titles, ids, model names and outcomes only; no
article text, prompts or model output), rewritten every 10 s while it runs; a file not rewritten for 45 s is shown
as *not answering* (the process was stopped or hung).

### Bounded live test (paid, hard-capped)

`datasources-ingest bounded-run` sends a small, explicit number of **real** records through grouping and
summaries, with a hard cap on the HTTP requests sent to the model API. It runs the same code as the worker (the
ingestion service, the compiled graph, the reanalysis path of `datasources-similarity retry`, inline
summaries); only the limits are new. Stop `datasources-dev` first: the run takes the same locks.

```powershell
# Without --confirm-paid-calls it prints the models and the cap, then stops before any request:
uv run datasources-ingest bounded-run --source stored --max-records 2 --max-model-attempts 6
# Two stored records whose grouping never ran (oldest first), at most 6 HTTP requests in total:
uv run datasources-ingest bounded-run --source stored --max-records 2 --max-model-attempts 6 --confirm-paid-calls
# Three new records from one collection pass (SEC poll + Alpaca REST window since the last sync):
uv run datasources-ingest bounded-run --source collect --max-records 3 --max-model-attempts 10 --confirm-paid-calls
# Summaries a run left queued, under the same kind of cap:
uv run datasources-summary retry --confirm-paid-calls --max-model-attempts 4 --limit 2
```

| Limit | What it counts | Where it is enforced |
| --- | --- | --- |
| `--max-records N` (1-100) | records started through the graph; duplicates the ingestion ledger skips do not count | before each record |
| `--max-model-attempts M` (1-1000) | HTTP requests actually sent to the Anthropic API: comparison and summary requests together, **every SDK retry included** | at the HTTP transport, before sending: a request or retry beyond the cap is never sent |
| `SIMILARITY_LLM_MAX_CALLS_PER_RECORD`, `SUMMARY_MAX_REQUESTS_PER_RUN` | logical calls per record (a call becomes up to 1 + `*_LLM_MAX_RETRIES` requests) | as in the worker |
| Money | **not capped** | token usage is read from each response and reported; a timed-out or dropped attempt reports none but may still be billed |

`--source stored` works on stored live records whose grouping is unfinished (pending or partial and due, or
never analysed), oldest first; nothing is collected and no cursor is involved. `--source collect` makes one
collection pass (`--no-sec` / `--no-alpaca` leave one out). `--no-summaries` groups only. `--sample` rehearses
`--source stored` on sample records. `SIMILARITY_ENABLED` / `SUMMARY_ENABLED` only switch the worker: the
bounded run uses the configured backends and models either way.

What a run leaves behind:

* A record starts only while attempts remain. When they run out, the record in progress finishes in an
  accurate state (grouping `partial`, summary job `pending`) and nothing else starts.
* Records not started are not marked in the ingestion ledger. The Alpaca cursor moves only when every article
  of the window was stored or skipped as a duplicate, so nothing is skipped over; SEC filings are tracked per
  accession. A record that is stored but whose grouping or summary is unfinished is ingested (it is in the
  database) and its unfinished analysis is recorded as such.
* Replays reuse stored work: a stored record reuses its analysis without a model call, judged pairs are not
  sent again, and an unchanged summary fingerprint makes no call. Running the same command again continues
  where the last one stopped.
* Ctrl+C finishes the record in progress and starts no other; a second Ctrl+C abandons it (nothing
  uncommitted is recorded; a summary it had started stays leased for `SUMMARY_LEASE_MINUTES`).
* `output/bounded-runs/<run id>/report.json`, also printed at the end: `stop_reason` (`completed`,
  `record_limit`, `attempt_budget`, `interrupted`, `error`); `records` (started, completed, pending, failed,
  skipped as duplicates, not started) with each record's grouping and summary results; `logical_model_calls_sent`;
  `http_attempts` (used, not sent because of the cap, SDK retries, outcomes, reported tokens, attempts without
  usage, served models); the Alpaca `cursor`. `model_attempts.json` lists every attempt, written before it is sent.

## Ingestion

### Processing graph

The compiled LangGraph (`src/datasources/ingest/graph.py`); print it with `uv run datasources-ingest graph`.

```text
START -> prepare_data --(no valid record)--> END
prepare_data -> create_text_units -> generate_embeddings -> retrieve_candidates -> route_candidates
route_candidates --(no plausible candidate)--> propose_new_event ---> resolve_event_memberships
route_candidates --(plausible candidates)----> compare_with_llm ----> resolve_event_memberships
route_candidates --(nothing to decide)-------------------------------> resolve_event_memberships
resolve_event_memberships -> persist_result -> route_event_summary
route_event_summary --(generate)--> generate_event_summary -> validate_event_summary -> persist_event_summary
route_event_summary --(open conflict)--> flag_conflict_review     (each loops back to route_event_summary)
route_event_summary --(nothing new)----> skip_summary
route_event_summary --(all affected events done)--> next record | END
```

### Graph, envelope and adapters

* **Graph** (`src/datasources/ingest/graph.py`): a `StateGraph` with typed state,
  compiled once and reused; the database, embedding model and comparison
  backend arrive through LangGraph's runtime context.

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

### Duplicate tracking

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

## Event grouping

### Setup notes

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
  event": cosine ≥ `SIMILARITY_MIN_SEMANTIC_SIMILARITY` (0.78). A top-N
  full-text hit qualifies on its own only when `SIMILARITY_LEXICAL_PLAUSIBLE_RANK`
  is above 0 (default 0, off): the OR-query ranks some same-company article in the
  top N almost every time, so that route sent nearly every record to the model.
  Full-text hits still shape the fused ranking. Calibration on the 58
  stored NVIDIA revisions (1,653 document pairs, max unit-to-unit cosine):
  median 0.69, 90th percentile 0.77, 99th 0.86 — and three recurring "whale
  activity" articles from different days scored 0.98–0.999 while genuinely
  related IonQ/NVIDIA coverage scored 0.84–0.89. A high score alone therefore
  never links anything. The shortlist keeps the best `SIMILARITY_MAX_CANDIDATE_EVENTS`
  (3) events: about as many as fit one comparison call at the default
  `SIMILARITY_LLM_MAX_INPUT_CHARS`, so a record rarely needs a second call that
  repeats its text.
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
* **Brief answers** (prompt `compare-2`): output tokens are most of a comparison's
  cost, so the prompt asks for one quote per side (at most 150 characters) and at
  most 3 matching facts on same-event labels; differences only on updates and
  conflicts (the kinds the summary, timeline and conflict review read); empty
  facts and evidence on `different_event` / `uncertain`; and a one-sentence
  explanation.
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

### Bootstrap and retry

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

## Event summaries

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
  The prompt (`summary-2`) asks for the shortest quote that supports a statement
  (a sentence or less, at most 300 characters) and for no calculated figures;
  validation rejects quotes over 600 characters. With `summary-1`, whose 300 was
  also the hard limit, the model copied whole sentences that ran just over it
  (up to 354 characters) and 40% of summaries were rejected and paid for again.
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

### Summary commands and leases

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

Search and filters, all applied in SQL before `limit`/`offset`: `q` (search text, see [The page](#the-page)),
`since` / `until` (ISO date-times on the latest report; `since` must be earlier), `has_filing=true` (an SEC filing
among the sources) and `conflict=true` (the sources disagree). The response echoes the applied values under
`filters`. Each listed event also carries `filings` (form and the 8-K items of the event's own part of a filing,
with their official captions) and `latest_update` (the newest substantive update's time, source and stored
aspects).

A watchlist is `company=<CIK>`, repeated or comma-separated (up to 50; digits only, zero-padded to 10): events
with a current source linked to **any** of those companies. It is applied in SQL before `limit`/`offset`, an event
linked to several of them is returned once, and it combines with `symbol` and `state`. Listed events and the detail
response also carry `companies` (`cik`, `name`, `symbols`) for the companies their current sources are linked to.

* `GET /api/coverage`: the companies a reader can follow (`cik`, `name`, `symbol`, `symbols`, `markets`) and, from
  `INGEST_SYMBOLS`, whether each is `collecting`.
* `GET /api/events/{event_id}/timeline?include_sample=false`: the timeline described under
  [The page](#the-page). Each entry has `kind` (`first_report`, `additional_coverage`, `substantive_update`,
  `conflicting_report`, `conflict_detected`, `review_decision`, `summary_published`, `summary_revised`), `time` and
  `time_basis` (`published`, `source_updated`, `sec_accepted`, `sec_filing_date` with a date-only `date`,
  `missing`, `detected`, `decided`, `summary_published`). Reports add `relation`, `revised_report`, `match`
  (`identical_text` for an exact copy), `source` (titles, links, the source's own times, `retrieved_at`, revision
  number), the filing `sections` about this event, the comparison's `aspects` and verbatim `quotes` for updates and
  conflicts, and the conflict `review`. `excluded` counts pending relations and reports moved out by a review;
  `counts` has the reports, report versions and undated entries. Built in `summary/timeline.py` from memberships,
  revisions, comparisons, summary versions and review decisions: no new table, no model call, no reviewer
  identities, notes or model explanations.

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
* `GET /api/activity` returns `worker` and `summary`, each with `state` (`running`, `stale`, `stopped`,
  `finished` or `not_running`), the `current` item with its `steps` (`key`, `label`, `model`, `state`, `result`,
  times), the `recent` finished items, the catch-up's `progress` and its final `note`. It only reads the activity
  files.

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

## Output files and formats

### Output layout

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

#### Demo vs live

`datasources-ingest demo` (or `process-file --demo`) uses bundled fixtures,
marks every record `is_sample: true`, and writes only to `output/demo/`, with its
own ledger. Live runs refuse demo mode and fail with a configuration error when
credentials are missing. For the UI, `/api/alpaca?demo=true` or
`DATASOURCES_DEMO_MODE=true` returns the synthetic sample labelled
`live: false, demo: true`; otherwise missing credentials return HTTP 503 with a
configuration error.

### Common output schema (`schema_version` 1.0)

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

### Documented schemas vs. repository fixtures

| Source | Documented | Observed in this repo |
| --- | --- | --- |
| SEC submissions | Entity metadata + `filings.recent` compact columnar arrays (≥1 year or 1,000 filings) + `filings.files` for older filings | `data/submissions_CIK0001045810.json` (**real**): 16 aligned columns × 1,001 rows, including `isXBRLNumeric`, which the repo's field dictionary does not describe; `filings.files` lists one historical file (1998–2020). `reportDate`/`items` may be empty strings, which are normalised to `null`. `acceptanceDateTime` is ISO-8601 with `Z`. |
| 8-K primary document | HTML / inline XBRL | `data/8k/*.htm` (**real** NVIDIA 8-Ks): XHTML with an `<?xml?>` prolog, a hidden `ix:header` inside `display:none`, visible `ix:nonNumeric` facts, layout tables, relative exhibit links split across several `<a>` tags. |
| Alpaca REST news | `id, headline, author, created_at, updated_at, summary, content, url, symbols, source, images`; `next_page_token`; `limit` 1–50 | Live responses checked during testing had exactly these keys. `data/alpaca_news_sample.json` is **synthetic** (example.com URLs); it has the same keys and is used only for demo mode. |
| Alpaca WebSocket news | `T="n"` + the same article fields (no `images`); `success`/`subscription`/`error` control frames | Live stream sent `success: connected`, `success: authenticated`, `subscription` as documented. `tests/fixtures/synthetic_*` are **synthetic** (see [tests/fixtures/README.md](tests/fixtures/README.md)). |

## Tests in detail

**Keep test settings out of the terminals that run the application.** `$env:PGDATABASE = ...` (like any
variable set in a PowerShell window) lasts until that window closes, and `.env` never overrides a variable that
is already set: `datasources-dev`, the CLIs and the web server started from that window then use the test
database too. Run the tests in their own terminal, or clear the override afterwards with
`Remove-Item Env:PGDATABASE` (likewise any other override, such as `$env:SIMILARITY_ENABLED`).
`uv run datasources-db check` prints which database a terminal reaches.

Database tests delete every sample row they may have written, so they never run
against `stock_news` (which keeps retained sample records). Before any setup,
`conftest.verify_isolated_database` checks the database the **server** reports
(`current_database()`, not just `PGDATABASE`) and refuses a protected name, a
name without `test`, a mismatch with the configured name, or any database that
holds live (non-sample) documents; `cleanup_sample_rows` repeats the check. To
create an empty test database with the full schema from the repository files:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\createdb.exe" -U postgres stock_news_mytest
$env:PGDATABASE = "stock_news_mytest"; uv run datasources-db migrate; uv run pytest
```

`tests/test_migrations.py` builds its own temporary databases (`stock_news_migtest_<random>_test`, dropped
afterwards; the role needs CREATEDB) on the server of the configured test database: a new database from the
repository files alone (and that `schema_expected.json` matches it), `migrate` a second time changing nothing,
the `stock_news` legacy state (`tests/fixtures/legacy_003_drift.sql`) adopted only with `--adopt-baseline` and
with every record kept byte for byte, a mismatched or partial base schema refused with the differences, 005 on
a real migration-004 database and on a schema where it was applied by hand, and edited migrations refused.

`tests/test_bounded_run.py` runs the real Anthropic SDK and the real comparison/summary adapters against a fake
network: SDK retries counted as attempts, a retry beyond the cap never sent, comparison and summary sharing one
cap, a summary deferred without a failed attempt when the cap runs out, a timed-out comparison left stored and
resumable, an interruption during a model call leaving nothing half recorded, re-runs duplicating nothing and
repeating no model call, the Alpaca cursor never moving past unprocessed articles, and consent required for
paid calls.

`tests/test_conflict_notice.py` replays the fictional Veltrani Photonics
acquisition (`tests/fixtures/synthetic_conflict_notice_veltrani.json`: the
recorded comparison judgments and summary statements) through the real graph
with scripted backends and checks the notice for every review outcome, several
disputes, a pending regeneration, missing summaries, incomplete evidence, sample
filtering, that reads make no model call (model clients and non-loopback
sockets fail the test), and, in headless Edge/Chrome when available, that the
page renders the notice and shows hostile article text and `javascript:` links
as inert text.

Company coverage, the watchlist and the story timeline have their own tests (no model calls, no outbound
network; the guard above applies):

* `tests/test_company_coverage.py`: the coverage list against the recorded SEC identifiers; SEC polling and
  recovery per company with one shared rate limit; the Alpaca subscription and REST window carrying every
  configured symbol; registration twice on a temporary database (`stock_news_coverage_<random>_test`, dropped
  afterwards) with pre-existing NVDA and SEC-issuer rows kept byte for byte, a conflicting ticker reported, the
  CLI's dry run writing nothing; a multi-company article delivered by REST and the stream stored once and linked
  to each registered company; the launcher and worker warnings for unregistered tickers.
* `tests/test_watchlist_api.py` (temporary database): the watchlist filter applied before paging, a story about
  several followed companies listed once, the single-company filter unchanged, input validation, sample stories
  hidden, `/api/coverage`, and reads changing no row and calling no model.
* `tests/test_event_timeline.py` (isolated test database): labels from stored relations, publication-time order,
  an earlier report collected later, revised reports (a new timestamp alone is never an update), conflict
  detection and review decisions at their own times without reviewer identity, summary versions, missing
  timestamps and date-only filings, a two-Item filing showing only its own section per event, no duplicates from
  chunks or repeated delivery, sample hiding, no model calls.
* `tests/test_live_only.py` (mocked SEC and Alpaca): only filings and articles published after the startup cutoff
  are collected, no historical submissions file or REST request is made, the worker loop resumes after a
  reconnect with newly published news only, duplicates are still caught, a fresh cutoff is taken at every
  startup, the saved cursor, scope floors and pending retries are left untouched, switching the mode off resumes
  recovery from them, and the startup summary describes the mode.
* `tests/test_reader_browser.py` (headless Edge or Chrome over DevTools, `tests/cdp_browser.py`; skipped without
  one): following companies, both views, persistence across reloads, empty / unreadable / outdated saved
  watchlists, blocked storage, the story page from a card and back to the same view, direct loads, unknown and
  malformed links, an API failure and retry, and a 375 px wide screen without sideways scrolling. On this
  machine the browser tests run from PowerShell; a browser started from Git Bash produced no output.

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

## Limitations (full list)

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
* **Only registered tickers are linked.** An article links only the companies present in `company_symbol`
  (`datasources-db register-companies` adds the 11 coverage companies); other tickers are counted
  (`db_unresolved_symbols`) and kept in the stored payload rather than guessed into new `company` rows. Only the
  listed ticker of each company is registered: an article tagged only with Alphabet's Class C ticker `GOOG`, or
  with a preferred-share ticker such as `JPM-PC`, is not linked (SEC filings still link through the issuer's CIK).
  Linking applies when a record is stored; `datasources-db relink-companies` links articles stored before a
  company was registered.
* Collecting 11 companies is configured, documented and tested offline, but has not been run live here: Alpaca's
  per-account limits on stream symbols (error 405) and the real article volume for all 11 were not measured.
* **The watchlist is per browser.** It lives in `localStorage` on one device; there are no accounts, and it does
  not sync between browsers or devices. Clearing site data clears it.
* The timeline orders entries by the times the sources state. Dates without a time (a filing date) are placed at
  the start of that UTC day, and records without any stated time are listed apart. Labels are grouping's stored
  judgments (validated for traceability, not truth); a report collected before grouping was enabled has no
  timeline entry until it is grouped.
* Records completed in the file ledger **before** the database existed are not
  re-persisted by a normal run (the ledger already calls them done). Run
  `uv run datasources-db backfill` once for those.
* Collection cursors stay in `output/state/ledger.json`; the `ingestion_state`
  table is left empty so there is one source of truth for them.
* **Semantic grouping is an MVP.** Retrieval defaults (7-day window, 0.78 cosine
  floor, lexical plausibility route off, 3 candidate events) are experimental starting points
  calibrated on 58 stored NVIDIA revisions, not tuned values. Comparison accuracy
  depends on the model; it is measured on 125 labelled test articles in
  [evaluation/](evaluation/README.md).
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
  Summary quality is measured separately in
  [evaluation/summarization/](evaluation/summarization/README.md).
* The figures check is strict: a statement that rewrites "first quarter" as "Q1"
  or "three" as "3" is rejected and retried, and a retry does not tell the model
  why the previous output was rejected.
* Only English summaries are generated; there is no translation.
* Summary work queued by a review decision, a deferral or a failure runs when a
  later record touches the event, at the next `datasources-dev` start (events
  from the catch-up window only) or with `datasources-summary retry`; the
  worker has no periodic summary sweep while it runs.
* A conflict review is created per comparison; a later revision of the same
  article does not close an older revision's open review automatically.
* Reprocessing through `datasources-similarity retry`/`backfill` does not
  summarize inline; `datasources-summary backfill` picks those events up.

