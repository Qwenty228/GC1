"""Startup catch-up: summarize recent events that have no current summary (run once per `datasources-dev` start).

`datasources-dev` runs this in its own child process at every start, with or without collectors, next to the
read-only web server (which still never calls a model). It looks at live events with an article whose source
time falls in the last SUMMARY_STARTUP_CATCHUP_DAYS days and runs the same summary steps as
`datasources-summary backfill` and `retry`, newest event first:

    no published summary              -> first summary (queued as a durable job first)
    summary older than a new member   -> new version
    pending job that is due           -> retried (its retry backoff is respected)
    running job whose lease expired   -> taken over (a process was stopped mid-summary)

Left alone: failed jobs (attempts used up; `datasources-summary retry --include-failed`), events waiting for a
conflict review, and articles not grouped into an event yet (counted and reported; grouping does not run here).
At most SUMMARY_STARTUP_CATCHUP_MAX_REQUESTS model requests per start, and a hard cap on HTTP attempts that
counts SDK retries; work beyond the cap stays queued for the next start. A stop request lets the summary in
progress finish and starts no other.
"""

from __future__ import annotations

import logging
import signal
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from . import store
from .models import INITIAL, NEW_INFORMATION
from .pipeline import SummaryBudget, SummaryContext, SummaryRuntime, process_event

LOGGER = logging.getLogger("datasources.summary.catchup")
SCAN_LIMIT = 500  # events looked at per start; the request cap bounds the model calls


def run_catchup(database, runtime: SummaryRuntime, *, clock: Callable[[], datetime] | None = None,
                stop: Callable[[], bool] = lambda: False, attempts=None, is_sample: bool = False,
                activity=None) -> dict[str, Any]:
    settings = runtime.settings
    now = clock() if clock else datetime.now(timezone.utc)
    since = now - timedelta(days=settings.startup_catchup_days)
    lang = settings.language
    with database.transaction() as conn:
        due = store.select_due_jobs(conn, is_sample=is_sample, language=lang, limit=SCAN_LIMIT, now=now, since=since)
        missing = store.select_missing(conn, is_sample=is_sample, language=lang, limit=SCAN_LIMIT, since=since)
        uncovered = store.select_uncovered(conn, is_sample=is_sample, language=lang, limit=SCAN_LIMIT, since=since)
        # Queued first, as `backfill` does, so whatever the request cap leaves over is found again next start.
        for event_id in missing:
            job = store.get_job(conn, event_id, lang)
            if job is None or job["status"] == "completed":
                store.request_job(conn, event_id, lang, INITIAL, now=now, reset_attempts=True)
        for event_id in uncovered:
            store.request_job(conn, event_id, lang, NEW_INFORMATION, now=now, reset_attempts=True)
        events = _newest_first(conn, {*due, *missing, *uncovered})
    try:
        with database.connection() as conn:
            ungrouped = _ungrouped_articles(conn, since=since, is_sample=is_sample)
    except Exception as error:  # only a report line; never a reason to skip the summaries
        LOGGER.warning("summary catch-up: could not count ungrouped articles: %s", error)
        ungrouped = 0

    days = f"{settings.startup_catchup_days:g} day(s)"
    LOGGER.info("summary catch-up: %d event(s) with articles from the last %s need summary work (%d without a "
                "summary, %d with new articles, %d queued or interrupted); at most %d model request(s)",
                len(events), days, len(missing), len(uncovered), len(due), settings.startup_catchup_max_requests)
    sctx = SummaryContext(database, runtime, SummaryBudget(settings.startup_catchup_max_requests, attempts=attempts),
                          clock, activity)
    reports: list[dict[str, Any]] = []
    left = 0
    for position, (event_id, title) in enumerate(events):
        if stop() or not sctx.budget.available():
            left = len(events) - position
            why = "stop requested" if stop() else sctx.budget.exhausted_reason()
            LOGGER.info("summary catch-up: %s; %d event(s) left queued for the next start", why, left)
            break
        if activity is not None:
            activity.progress(position, len(events), f"events from the last {settings.startup_catchup_days:g} "
                                                     "day(s) that need a summary")
            activity.start_item("event", title, event_id=event_id)
        report = process_event(event_id, sctx).report()
        if activity is not None:
            activity.finish_item()
        report["title"] = title
        reports.append(report)
        LOGGER.info("summary catch-up: %s", _describe(report))
    if activity is not None:
        activity.progress(len(reports), len(events), f"events from the last {settings.startup_catchup_days:g} "
                                                     "day(s) that need a summary")

    counts: dict[str, int] = {}
    for report in reports:
        counts[report["result"] or "none"] = counts.get(report["result"] or "none", 0) + 1
    out: dict[str, Any] = {"window_days": settings.startup_catchup_days, "since": since.isoformat(),
                           "selected": len(events), "results": counts, "left_queued": left,
                           "model_requests": sctx.budget.used, "request_cap": sctx.budget.limit,
                           "ungrouped_articles": ungrouped, "events": reports}
    if attempts is not None:
        out["http_attempts"] = attempts.summary()
    LOGGER.info("summary catch-up done: %s", _totals(out))
    if ungrouped:
        LOGGER.info("summary catch-up: %d article(s) from the last %s are not grouped into an event yet, so they "
                    "cannot be summarized; `uv run datasources-similarity backfill` / `retry` group them (with the "
                    "worker stopped)", ungrouped, days)
    return out


def _newest_first(conn, event_ids: set[str]) -> list[tuple[str, str]]:
    if not event_ids:
        return []
    rows = conn.execute(
        "SELECT e.event_id::text AS event_id, e.title, max(m.source_time) AS latest FROM news_event e "
        "LEFT JOIN current_event_membership m ON m.event_id = e.event_id WHERE e.event_id = ANY(%s::uuid[]) "
        "GROUP BY e.event_id, e.title ORDER BY latest DESC NULLS LAST, e.event_id",
        (sorted(event_ids),),
    ).fetchall()
    return [(r["event_id"], r["title"] or "") for r in rows]


def _ungrouped_articles(conn, *, since: datetime, is_sample: bool) -> int:
    """Current revisions from the window whose grouping has not finished (no event yet, or an incomplete one)."""
    return int(conn.execute(
        """
        SELECT count(*) AS n
          FROM document d
          JOIN document_revision r ON r.revision_id = d.current_revision_id
          LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
         WHERE d.is_sample = %s
           AND COALESCE(r.published_at, s.accepted_at, r.source_updated_at, r.retrieved_at) >= %s
           AND NOT EXISTS (SELECT 1 FROM revision_analysis a WHERE a.revision_id = r.revision_id
                             AND a.status IN ('completed', 'skipped'))
        """,
        (is_sample, since),
    ).fetchone()["n"])


def _describe(report: dict[str, Any]) -> str:
    title = report.get("title") or report["event_id"]
    text = f"{report['result'] or report['route']}"
    if report.get("version") is not None:
        text += f" v{report['version']}"
    text += f": {title[:90]} ({report['event_id'][:8]})"
    if report.get("validation_errors"):
        text += f"; rejected by validation: {'; '.join(report['validation_errors'])[:300]}"
    elif report.get("error"):
        text += f"; {report['error'].get('message', '')[:300]}"
    elif report["result"] not in ("published", "unchanged"):
        text += f"; {report.get('reason') or ''}"
    return text


def _totals(out: dict[str, Any]) -> str:
    results = ", ".join(f"{name} {n}" for name, n in sorted(out["results"].items())) or "nothing to do"
    text = f"{results}; {out['model_requests']} of at most {out['request_cap']} model request(s)"
    attempts = out.get("http_attempts")
    if attempts:
        tokens = attempts["tokens_reported"]
        text += (f"; {attempts['used']} HTTP attempt(s) of at most {attempts['limit']}, tokens in "
                 f"{tokens['input_tokens']} / out {tokens['output_tokens']}")
        if attempts["attempts_without_usage_that_may_be_billed"]:
            text += f" ({attempts['attempts_without_usage_that_may_be_billed']} without usage, may be billed)"
    if out["left_queued"]:
        text += f"; {out['left_queued']} left queued for the next start"
    return text


# ----------------------------------------------------------------------------- `datasources-dev` child
def main() -> int:
    """Entry point of the launcher's one-shot `summary` child. 0 = done (or off), 2 = configuration problem."""
    from ..activity import ActivityTracker
    from ..db import Database, DatabaseError, db_enabled, load_db_settings
    from ..ingest.cli import configure_logging
    from ..ingest.settings import ConfigurationError, load_settings
    from ..llm_budget import AttemptLedger, counting_anthropic
    from .generation import AnthropicSummaryBackend, credentials_available
    from .pipeline import build_summary_runtime
    from .settings import load_summary_settings

    stop = threading.Event()

    def on_interrupt(*_args) -> None:
        if stop.is_set():
            raise KeyboardInterrupt  # second request: abandon the summary in progress (its lease expires)
        stop.set()
        LOGGER.info("summary catch-up: stop requested; finishing the summary in progress, starting no other")

    signal.signal(signal.SIGINT, on_interrupt)
    try:
        configure_logging(load_settings().root, filename="summary_catchup.log")
        settings = load_summary_settings()
        if not settings.enabled or settings.startup_catchup_days <= 0:
            LOGGER.info("summary catch-up: off (SUMMARY_ENABLED=%s, SUMMARY_STARTUP_CATCHUP_DAYS=%g)",
                        str(settings.enabled).lower(), settings.startup_catchup_days)
            return 0
        if not db_enabled():
            LOGGER.info("summary catch-up: off (DATASOURCES_DB_ENABLED=false)")
            return 0
        ledger = backend = None
        if settings.backend == "anthropic":
            if not credentials_available():
                raise ConfigurationError("SUMMARY_BACKEND=anthropic needs ANTHROPIC_API_KEY in .env")
            # Every HTTP request counts, SDK retries included, and none is sent past the cap.
            ledger = AttemptLedger(settings.startup_catchup_max_requests * (settings.llm_max_retries + 1))
            backend = AnthropicSummaryBackend(settings, client=counting_anthropic(
                ledger, "summary", timeout=settings.llm_timeout_seconds, max_retries=settings.llm_max_retries))
        with Database(load_db_settings()) as database:
            runtime = build_summary_runtime(database, settings, backend=backend)
            activity = ActivityTracker(load_settings().root, "summary")  # its progress, live on the page
            state, note = "stopped", "stopped before it finished"
            try:
                out = run_catchup(database, runtime, stop=stop.is_set, attempts=ledger, activity=activity)
                state, note = "finished", _totals(out)
            finally:
                activity.close(state, note=note)
        return 0
    except (ConfigurationError, DatabaseError) as error:
        LOGGER.error("summary catch-up not run: %s", error)
        return 2
