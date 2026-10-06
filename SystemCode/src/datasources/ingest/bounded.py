"""`datasources-ingest bounded-run`: a deliberate, bounded live test of grouping and summaries.

    uv run datasources-ingest bounded-run --source stored  --max-records 3 --max-model-attempts 10 --confirm-paid-calls
    uv run datasources-ingest bounded-run --source collect --max-records 3 --max-model-attempts 10 --confirm-paid-calls

It runs the same code as the worker and the maintenance commands (the ingestion service, the compiled graph,
the reanalysis path of `datasources-similarity retry`, inline summaries); only the limits are new.

Sources
    stored   live records already in PostgreSQL whose grouping is unfinished (pending or partial and due, or never
             analysed), oldest first. Nothing is collected and no ingestion cursor is touched.
    collect  one collection pass like `run --once` (an SEC poll per symbol, then the Alpaca REST window since the
             last cursor), processing at most --max-records new records and fetching no further Alpaca page once
             it has them.

Limits (all explicit; there is no unlimited recovery or backfill)
    --max-records N         records started through the graph (duplicates the ledger skips are not counted)
    --max-model-attempts M  HTTP requests actually sent to the model API: comparison and summary calls together,
                            every SDK retry included, enforced at the HTTP transport before a request is sent
    Per record, SIMILARITY_LLM_MAX_CALLS_PER_RECORD and SUMMARY_MAX_REQUESTS_PER_RUN still cap logical calls.
    Spending is not capped in money: the report gives the token usage the API returned.

What it leaves behind
    * A record starts only while attempts remain. When they run out, the record in progress finishes in an
      accurate state (grouping 'partial', summary job 'pending') and no further record starts.
    * Records not started are not marked in the ingestion ledger, and the Alpaca cursor moves only when every
      article of the window was stored or skipped as a duplicate, so nothing is skipped over. SEC filings are
      tracked per accession and their scope floor never moves forward.
    * Replays reuse stored work: a stored record reuses its analysis without a model call, already judged pairs
      are not sent again, and an unchanged summary fingerprint makes no call.
    * Ctrl+C finishes the record in progress and starts no other; a second Ctrl+C abandons it (anything not
      committed stays unrecorded and is picked up by the next run).
    * `<INGEST_OUTPUT_DIR>/bounded-runs/<run id>/report.json` (limits, per-record results, attempts, token usage)
      and `model_attempts.json` (every attempt, written before it is sent).
"""

from __future__ import annotations

import json
import logging
import secrets
import sys
import threading
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..llm_budget import AttemptLedger, counting_anthropic
from .service import ALPACA_CURSOR, IngestionService, WorkItem
from .settings import ConfigurationError
from .store import atomic_write_json

LOGGER = logging.getLogger(__name__)
MAX_RECORDS_LIMIT = 100
MAX_ATTEMPTS_LIMIT = 1000
PENDING_SUMMARY_RESULTS = ("deferred", "error", "invalid", "stale", "blocked", "claimed_elsewhere")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_run_id() -> str:
    return f"bounded-{_utcnow().strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(2)}"


def _skips(service: IngestionService, prefix: str) -> int:
    return sum(v for k, v in service.stats.items() if k.startswith(prefix))


# ----------------------------------------------------------------------------- the run and its report
@dataclass
class BoundedRun:
    run_id: str
    source: str
    max_records: int
    ledger: AttemptLedger
    report_path: Path | None = None
    started_at: datetime = field(default_factory=_utcnow)
    records: list[dict[str, Any]] = field(default_factory=list)
    not_started: list[str] = field(default_factory=list)
    duplicates_skipped: int = 0
    stop_reason: str | None = None
    stop_requested: bool = False
    notes: list[str] = field(default_factory=list)
    cursor: dict[str, Any] = field(default_factory=dict)
    settings: dict[str, Any] = field(default_factory=dict)
    say: Callable[[str], None] = print

    @property
    def started(self) -> int:
        return len(self.records)

    def room(self) -> str | None:
        """Why no further record may start (None = one may)."""
        if self.stop_requested:
            return "interrupted"
        if self.started >= self.max_records:
            return "record_limit"
        if self.ledger.exhausted():
            return "attempt_budget"
        return None

    def add(self, entry: dict[str, Any]) -> None:
        self.records.append(entry)
        summaries = ", ".join(entry.get("summaries") or []) or "-"
        self.say(f"  [{self.started}/{self.max_records}] {entry['status'].upper():<9} {entry['record']}: grouping "
                 f"{entry.get('grouping') or '-'}; summaries {summaries}; model attempts so far "
                 f"{self.ledger.used}/{self.ledger.limit}")
        self.save()

    def counts(self) -> dict[str, int]:
        counts = Counter(r["status"] for r in self.records)
        return {"started": self.started, "completed": counts.get("completed", 0),
                "pending": counts.get("pending", 0), "failed": counts.get("failed", 0),
                "skipped_as_duplicates": self.duplicates_skipped, "not_started": len(self.not_started)}

    def report(self) -> dict[str, Any]:
        attempts = self.ledger.summary()
        return {
            "run_id": self.run_id, "source": self.source,
            "started_at": self.started_at.isoformat(), "finished_at": _utcnow().isoformat(),
            "stop_reason": self.stop_reason,
            "limits": {"max_records": self.max_records, "max_model_attempts": self.ledger.limit, **self.settings},
            "records": self.counts(),
            "logical_model_calls_sent": attempts["logical_calls_sent"],
            "http_attempts": attempts,
            "cursor": self.cursor,
            "per_record": self.records,
            "not_started": self.not_started,
            "notes": self.notes,
            "spending": "not capped in money: this run caps HTTP attempts; token counts are as the API reported "
                        "them, and attempts without usage (timeouts, dropped connections) may still be billed",
        }

    def save(self) -> None:
        if self.report_path is not None:
            atomic_write_json(self.report_path, self.report())


def classify(outcome, error, process_result: str | None = None) -> dict[str, Any]:
    """One record's result as completed / pending / failed, with what is left to do."""
    if error is not None:
        return {"status": "failed", "reason": f"{error.stage}: {error.message}"[:300],
                "retryable": bool(getattr(error, "retryable", False))}
    if outcome is None:
        return {"status": "failed" if process_result == "failed" else "pending",
                "reason": "not stored (see output/errors); it stays unmarked in the ledger and is retried"}
    if outcome.error is not None:
        return {"status": "failed", "reason": f"not stored: {outcome.error.get('message')}"[:300], "retryable": True}
    from ..summary.pipeline import LEASED_ELSEWHERE

    grouping = outcome.analysis_status
    summaries = [s.get("result") or "none" for s in outcome.summaries]
    base = grouping.split(":", 1)[-1]
    left = [f"grouping {base}"] if base in ("pending", "partial") else []
    left += [f"summary {result}" for result in summaries if result in PENDING_SUMMARY_RESULTS]
    # Another process holds the event's summary lease (or a run that stopped did: it expires after
    # SUMMARY_LEASE_MINUTES), so the summary was not done here.
    left += ["summary held by another lease" for s in outcome.summaries
             if s.get("result") == "skipped" and s.get("reason") == LEASED_ELSEWHERE]
    status = "failed" if base == "failed" else ("pending" if left else "completed")
    entry = {"status": status, "grouping": grouping, "summaries": summaries, "left_to_do": left,
             "revision_id": outcome.revision_id, "document_id": outcome.document_id,
             "persist": outcome.persist_outcome, "comparison_calls": int(outcome.analysis.get("llm_calls") or 0),
             "events": outcome.analysis.get("event_ids", [])}
    if "awaiting_review" in summaries:
        entry["note"] = "a conflict review is open: the summary waits for a reviewer, not for this command"
    return entry


# ----------------------------------------------------------------------------- one record, interruptible
def run_interruptibly(run: BoundedRun, fn: Callable[[], Any]) -> Any:
    """Run one record in a helper thread so a first Ctrl+C lets it finish (and nothing new starts)."""
    result: dict[str, Any] = {}

    def target() -> None:
        try:
            result["value"] = fn()
        except BaseException as error:  # re-raised in the main thread
            result["error"] = error

    thread = threading.Thread(target=target, name="bounded-record", daemon=True)
    thread.start()
    while thread.is_alive():
        try:
            thread.join(0.2)
        except KeyboardInterrupt:
            if run.stop_requested:
                run.say("second Ctrl+C: abandoning the record in progress (nothing uncommitted is recorded)")
                raise
            run.stop_requested = True
            run.say("Ctrl+C: finishing the record in progress, then stopping (Ctrl+C again to abandon it)")
    if "error" in result:
        raise result["error"]
    return result.get("value")


# ----------------------------------------------------------------------------- source: stored records
def run_stored(run: BoundedRun, database, analyzer, summarizer, *, is_sample: bool) -> None:
    """Group (and summarize) up to max_records stored revisions whose grouping is unfinished, oldest first."""
    from ..similarity import store
    from ..similarity.pipeline import PipelineContext
    from .graph import run_reanalysis

    space = {"profile_id": analyzer.profile_id, "pipeline_version": analyzer.pipeline_version,
             "is_sample": is_sample}
    with database.connection() as conn:
        due = store.select_retry(conn, **space, limit=run.max_records, now=_utcnow())
        fresh = store.select_unanalyzed(conn, **space, limit=run.max_records)
        order = [str(r["revision_id"]) for r in conn.execute(
            "SELECT r.revision_id FROM document_revision r LEFT JOIN sec_filing_details s USING (revision_id) "
            "WHERE r.revision_id = ANY(%s::uuid[]) "
            "ORDER BY COALESCE(r.published_at, s.accepted_at, r.source_updated_at, r.retrieved_at), r.revision_id",
            (list(set(due + fresh)),))][: run.max_records]
    fresh_ids = [rid for rid in order if rid in set(fresh)]
    run.say(f"selected {len(order)} stored revision(s) with unfinished grouping, oldest first "
            f"({len(fresh_ids)} never analysed)")
    if fresh_ids:
        # As `datasources-similarity backfill` does: index first (text units + local embeddings, no model call),
        # so no selected record is declared new only because a neighbour had no embeddings yet.
        run_reanalysis(fresh_ids, PipelineContext(database, analyzer, index_only=True))
    try:
        for position, revision_id in enumerate(order):
            why = run.room()
            if why is not None:
                run.stop_reason = why
                run.not_started.extend(f"revision:{rid}" for rid in order[position:])
                return
            context = PipelineContext(database, analyzer, summarizer=summarizer, model_budget=run.ledger)
            result = run_interruptibly(run, lambda: run_reanalysis([revision_id], context))
            outcome = result.outcomes[0] if result.outcomes else None
            error = result.errors[0] if result.errors else None
            run.add({"record": f"revision:{revision_id}", **classify(outcome, error)})
        run.stop_reason = "interrupted" if run.stop_requested else "completed"
    finally:
        try:
            with database.connection() as conn:
                left = len(store.select_retry(conn, **space, limit=100000, now=_utcnow(), include_not_due=True)) + \
                    len(store.select_unanalyzed(conn, **space, limit=100000))
            run.notes.append(f"{left} stored revision(s) of this partition still have unfinished grouping")
        except Exception as error:  # reporting only; never hide the run's own outcome
            run.notes.append(f"could not count the remaining unfinished revisions: {error}")


# ----------------------------------------------------------------------------- source: live collection
def _process(run: BoundedRun, service: IngestionService, item: WorkItem) -> None:
    service.last_graph = None
    before = _skips(service, "sec_skip_" if item.kind == "sec" else "alpaca_skip_")
    result = run_interruptibly(run, lambda: service.process_item(item))
    if result == "skipped" and _skips(service, "sec_skip_" if item.kind == "sec" else "alpaca_skip_") > before:
        run.duplicates_skipped += 1
        return
    outcome, error = service.last_graph or (None, None)
    record = item.key.rsplit(":", 1)[0] if item.kind == "alpaca" else item.key
    run.add({"record": record, "stored_as": result, **classify(outcome, error, result)})


def run_collect(run: BoundedRun, service: IngestionService, *, sec: bool, alpaca: bool,
                iter_pages: Callable[..., Any] | None = None) -> None:
    """One bounded collection pass: SEC first, then the Alpaca REST window, oldest first."""
    from ..alpaca import iter_news_pages
    from .sec_client import SecFetchError

    iter_pages = iter_pages or iter_news_pages
    settings = service.settings
    service.startup()  # ledger reconciliation, exactly as the worker starts
    if sec:
        for symbol in settings.symbols:
            if run.room() is not None:
                run.notes.append(f"SEC {symbol}: not polled ({run.room()})")
                continue
            before = _skips(service, "sec_skip_")
            try:
                items = service.sec_poll_once(symbol)
            except SecFetchError as error:
                run.notes.append(f"SEC {symbol}: poll failed ({error.kind}); nothing marked, retried next run")
                continue
            run.duplicates_skipped += _skips(service, "sec_skip_") - before
            run.say(f"SEC {symbol}: {len(items)} filing(s) to process in scope")
            for position, item in enumerate(items):
                if run.room() is not None:
                    run.not_started.extend(i.key for i in items[position:])
                    break
                _process(run, service, item)
    if not alpaca:
        run.stop_reason = run.room() or "completed"
        return
    before_cursor = service.ledger.get_cursor(ALPACA_CURSOR)
    start, end = service.backfill_window()
    run.cursor = {"alpaca_cursor_before": before_cursor.isoformat() if before_cursor else None,
                  "window": [start.isoformat(), end.isoformat()], "advanced": False, "pages_fetched": 0}
    if run.room() is not None:
        run.cursor["why_not_advanced"] = f"no Alpaca page fetched ({run.room()})"
        run.stop_reason = run.room()
        return
    complete, why_incomplete = True, ""
    pages = iter_pages(service.alpaca_auth, settings.symbols, start=start, end=end,
                       max_pages=settings.alpaca_backfill_max_pages, page_size=settings.alpaca_page_size,
                       session=service.alpaca_session, timeout=settings.http_timeout_seconds)
    try:
        for payload, request_url in pages:
            run.cursor["pages_fetched"] += 1
            retrieved_at = _utcnow()
            page_ref = service.store.save_raw_alpaca_page(payload, request_url, retrieved_at)
            before = _skips(service, "alpaca_skip_")
            items = service.alpaca_items(payload.get("news", []), transport="rest", endpoint=request_url,
                                         retrieved_at=retrieved_at, raw_refs=[page_ref])
            run.duplicates_skipped += _skips(service, "alpaca_skip_") - before
            for position, item in enumerate(items):
                if run.room() is not None:
                    run.not_started.extend(i.key.rsplit(":", 1)[0] for i in items[position:])
                    complete, why_incomplete = False, "articles of this window were not started"
                    break
                _process(run, service, item)
                if run.records and run.records[-1]["status"] == "failed":
                    complete, why_incomplete = False, "an article of this window failed and must be retried"
            if run.room() is not None:
                if payload.get("next_page_token"):
                    complete = False
                    why_incomplete = why_incomplete or "later pages of this window were not fetched"
                break
            if run.cursor["pages_fetched"] >= settings.alpaca_backfill_max_pages and payload.get("next_page_token"):
                complete, why_incomplete = False, "ALPACA_BACKFILL_MAX_PAGES reached before the end of the window"
    except KeyboardInterrupt:
        raise
    except Exception as error:  # a failed page: what follows it is unknown, so the cursor stays
        complete, why_incomplete = False, f"an Alpaca REST page failed: {error}"
        run.notes.append(f"Alpaca REST fetch failed after {run.cursor['pages_fetched']} page(s): {error}")
    if complete:
        service._advance_cursor(end)
        run.cursor.update({"advanced": True, "alpaca_cursor_after": end.isoformat()})
    else:
        run.cursor["why_not_advanced"] = why_incomplete
    run.stop_reason = run.room() or "completed"


# ----------------------------------------------------------------------------- runtimes with counted clients
def counted_runtimes(database, ledger: AttemptLedger, *, summaries: bool, similarity_settings=None,
                     summary_settings=None, transport_factory=None):
    """Grouping and summary runtimes whose Anthropic clients count every HTTP attempt in `ledger`.

    A non-Anthropic comparison backend (``mock``) makes no request and needs no counting."""
    from ..similarity.comparison import AnthropicComparisonBackend, anthropic_credentials_available
    from ..similarity.pipeline import build_runtime
    from ..similarity.settings import load_similarity_settings
    from ..summary.generation import AnthropicSummaryBackend
    from ..summary.pipeline import build_summary_runtime
    from ..summary.settings import load_summary_settings

    sim = similarity_settings or load_similarity_settings()
    comparator = None
    if sim.comparison_backend == "anthropic":
        if transport_factory is None and not anthropic_credentials_available():
            raise ConfigurationError("SIMILARITY_COMPARISON_BACKEND=anthropic needs ANTHROPIC_API_KEY")
        comparator = AnthropicComparisonBackend(sim, client=counting_anthropic(
            ledger, "comparison", timeout=sim.llm_timeout_seconds, max_retries=sim.llm_max_retries,
            transport_factory=transport_factory))
    analyzer = build_runtime(database, sim, comparator=comparator)
    summarizer = None
    if summaries:
        summ = summary_settings or load_summary_settings()
        if transport_factory is None and not anthropic_credentials_available():
            raise ConfigurationError("SUMMARY_BACKEND=anthropic needs ANTHROPIC_API_KEY")
        backend = AnthropicSummaryBackend(summ, client=counting_anthropic(
            ledger, "summary", timeout=summ.llm_timeout_seconds, max_retries=summ.llm_max_retries,
            transport_factory=transport_factory))
        summarizer = build_summary_runtime(database, summ, backend=backend, grouping_enabled=True)
    return analyzer, summarizer


def describe_limits(analyzer, summarizer) -> dict[str, Any]:
    sim = analyzer.settings
    out: dict[str, Any] = {"comparison": f"{sim.comparison_backend}:{sim.comparison_model}",
                           "comparison_calls_per_record_max": sim.llm_max_calls_per_record,
                           "comparison_sdk_retries": sim.llm_max_retries,
                           "comparison_timeout_seconds": sim.llm_timeout_seconds,
                           "summaries": "off (--no-summaries)"}
    if summarizer is not None:
        s = summarizer.settings
        out.update({"summaries": f"{s.backend}:{s.model}", "summary_requests_per_record_max": s.max_requests_per_run,
                    "summary_sdk_retries": s.llm_max_retries, "summary_timeout_seconds": s.llm_timeout_seconds})
    return out


# ----------------------------------------------------------------------------- command
def main(args, *, settings) -> int:
    """`datasources-ingest bounded-run` (argparse args, loaded IngestSettings)."""
    from ..db import Database, load_db_settings
    from ..similarity.lock import GraphWriterLock
    from ..similarity.settings import load_similarity_settings
    from ..summary.settings import load_summary_settings
    from .service import ProcessLock

    if not 1 <= args.max_records <= MAX_RECORDS_LIMIT:
        raise ConfigurationError(f"--max-records must be between 1 and {MAX_RECORDS_LIMIT}")
    if not 1 <= args.max_model_attempts <= MAX_ATTEMPTS_LIMIT:
        raise ConfigurationError(f"--max-model-attempts must be between 1 and {MAX_ATTEMPTS_LIMIT}")
    if args.source == "collect" and args.sample:
        raise ConfigurationError("--sample works with --source stored; collected records are always live")
    if args.source == "collect" and args.no_sec and args.no_alpaca:
        raise ConfigurationError("--no-sec and --no-alpaca together leave nothing to collect")
    sim, summ = load_similarity_settings(), load_summary_settings()
    summaries = not args.no_summaries
    paid = [f"comparison {sim.comparison_model}"] if sim.comparison_backend == "anthropic" else []
    if summaries and summ.backend == "anthropic":
        paid.append(f"summaries {summ.model}")
    print(f"bounded run, source {args.source}: at most {args.max_records} record(s) and {args.max_model_attempts} "
          f"model HTTP attempt(s) in total, SDK retries included; paid backends: {'; '.join(paid) or 'none'}")
    if not sim.enabled or (summaries and not summ.enabled):
        print("note: SIMILARITY_ENABLED / SUMMARY_ENABLED only switch the worker; this command uses the configured "
              "backends either way")
    if paid and not args.confirm_paid_calls:
        print(f"This sends paid requests to the Anthropic API ({'; '.join(paid)}): at most {args.max_model_attempts} "
              "HTTP attempt(s). Re-run with --confirm-paid-calls to proceed.", file=sys.stderr)
        return 2

    run_id = new_run_id()
    ledger = AttemptLedger(args.max_model_attempts)
    run = BoundedRun(run_id, args.source, args.max_records, ledger)
    database = Database(load_db_settings())
    lock = process_lock = None
    try:
        is_sample = bool(args.sample)
        # Locks first: a running worker, retry or backfill is reported before anything is loaded or written.
        lock = GraphWriterLock(database, is_sample=is_sample, purpose=f"bounded run {run_id}").acquire()
        if args.source == "collect":
            process_lock = ProcessLock(settings.root).__enter__()
        analyzer, summarizer = counted_runtimes(database, ledger, summaries=summaries, similarity_settings=sim,
                                                summary_settings=summ)
        run.settings = describe_limits(analyzer, summarizer)
        run_dir = settings.root / "bounded-runs" / run_id
        ledger.persist_to(run_dir / "model_attempts.json")
        run.report_path = run_dir / "report.json"
        print(f"run {run_id}: report and attempt log in {run_dir}")
        if args.source == "stored":
            run_stored(run, database, analyzer, summarizer, is_sample=is_sample)
        else:
            from .cli import live_collectors

            sec_client, auth = live_collectors(settings, enable_sec=not args.no_sec, enable_alpaca=not args.no_alpaca)
            service = IngestionService(settings, sec_client=sec_client, alpaca_auth=auth, database=database,
                                       analyzer=analyzer, summarizer=summarizer, model_budget=ledger)
            run_collect(run, service, sec=not args.no_sec, alpaca=not args.no_alpaca)
    except KeyboardInterrupt:
        run.stop_reason = "interrupted"
        run.notes.append("stopped by Ctrl+C; anything not committed was not recorded and is picked up next run")
    except Exception as error:
        run.stop_reason = "error"
        run.notes.append(f"stopped by an error: {type(error).__name__}: {error}"[:500])
        raise
    finally:
        run.save()
        if lock is not None:
            lock.release()
        if process_lock is not None:
            process_lock.__exit__(None, None, None)
        database.close()
    report = run.report()
    print(json.dumps({k: report[k] for k in ("run_id", "stop_reason", "records", "logical_model_calls_sent",
                                              "http_attempts", "cursor", "not_started", "notes")},
                     indent=2, default=str))
    print(f"full report: {run.report_path}")
    return 130 if run.stop_reason == "interrupted" else 0
