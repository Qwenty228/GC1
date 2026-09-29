"""`datasources-summary`: event summaries and conflict reviews.

    uv run datasources-summary check                                   # configuration + schema, no writes, no calls
    uv run datasources-summary status                                  # jobs, versions, attempts, reviews
    uv run datasources-summary pending --limit 20                      # what backfill/retry would do (no calls)
    uv run datasources-summary show --event-id <uuid>                  # versions, citations, attempts, reviews
    uv run datasources-summary run-event --event-id <uuid> --confirm-paid-calls   # ONE event, at most one request
    uv run datasources-summary backfill --limit 10 --confirm-paid-calls           # events missing a summary
    uv run datasources-summary retry --limit 10 --confirm-paid-calls              # due failed/deferred/queued work
    uv run datasources-summary review list
    uv run datasources-summary review show --review-id <uuid>
    uv run datasources-summary review decide --review-id <uuid> --reviewer <name> \\
        --decision not_a_conflict|confirmed_conflict|different_events|insufficient_evidence --explanation "..."

Commands that can call the model need --confirm-paid-calls and are bounded by
--limit and SUMMARY_MAX_REQUESTS_PER_RUN. They use job leases, so they may run
next to the ingestion worker without generating the same summary twice. Add
--sample to work on sample data.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from ..db import Database, DatabaseError, load_db_settings
from ..ingest.settings import ConfigurationError
from ..similarity.store import missing_schema as similarity_missing_schema
from . import review, store
from .generation import credentials_available
from .models import INITIAL, NEW_INFORMATION
from .pipeline import SummaryBudget, SummaryContext, build_summary_runtime, process_event
from .settings import load_summary_settings

LOGGER = logging.getLogger("datasources.summary")


def _print(value) -> None:
    print(json.dumps(value, indent=2, default=str))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _runtime(database):
    settings = load_summary_settings()
    if not settings.enabled:
        print("note: SUMMARY_ENABLED is false, so the ingestion worker does not summarize new records; this command "
              "still uses the configured model.", file=sys.stderr)
    return build_summary_runtime(database, settings)


def _context(database, args, *, limit: int | None = None) -> SummaryContext | None:
    runtime = _runtime(database)
    if runtime.provider == "anthropic" and not args.confirm_paid_calls:
        print(f"This can send up to {limit or runtime.settings.max_requests_per_run} request(s) to "
              f"{runtime.model} (paid API calls). Re-run with --confirm-paid-calls to proceed; "
              "`datasources-summary pending` shows what would run.", file=sys.stderr)
        return None
    cap = runtime.settings.max_requests_per_run if limit is None else min(limit, runtime.settings.max_requests_per_run)
    return SummaryContext(database, runtime, SummaryBudget(cap))


def _run(event_ids: list[str], sctx: SummaryContext, *, trigger: str = "job", refresh: bool = False,
         ignore_backoff: bool = False) -> dict:
    reports = [process_event(e, sctx, trigger=trigger, refresh=refresh, ignore_backoff=ignore_backoff).report()
               for e in event_ids]
    counts: dict[str, int] = {}
    for report in reports:
        counts[report["result"] or "none"] = counts.get(report["result"] or "none", 0) + 1
    return {"selected": len(event_ids), "results": counts, "model_requests": sctx.budget.used,
            "request_cap": sctx.budget.limit, "events": reports}


# ----------------------------------------------------------------------------- commands
def cmd_check(database: Database) -> int:
    settings = load_summary_settings()
    report: dict = {"summary_enabled": settings.enabled, "problems": [],
                    "model": {"backend": settings.backend, "model": settings.model,
                              "effort": settings.effort or "model default", "fallbacks": settings.fallbacks,
                              "language": settings.language},
                    "limits": {"max_input_chars": settings.max_input_chars, "max_passages": settings.max_passages,
                               "max_output_tokens": settings.max_output_tokens,
                               "timeout_seconds": settings.llm_timeout_seconds,
                               "sdk_retries": settings.llm_max_retries,
                               "max_requests_per_run": settings.max_requests_per_run,
                               "max_attempts": settings.max_attempts}}
    report["database"] = database.check().describe()
    with database.connection() as conn:
        missing = similarity_missing_schema(conn) + store.missing_schema(conn)
    if missing:
        report["problems"].append(f"schema missing {missing}: run `uv run datasources-db migrate`")
    if settings.backend == "anthropic":
        available = credentials_available()
        report["model"]["credentials"] = "available" if available else "missing"
        if not available:
            report["problems"].append("no Anthropic credentials: set ANTHROPIC_API_KEY in .env")
    _print(report)
    return 0 if not report["problems"] else 2


def cmd_status(database: Database, args) -> int:
    settings = load_summary_settings()
    with database.connection() as conn:
        _require_schema(conn)
        report = {"summary_enabled": settings.enabled, "partition": "sample" if args.sample else "live",
                  "model": f"{settings.backend}:{settings.model}", "language": settings.language,
                  **store.status_summary(conn, is_sample=args.sample, language=settings.language)}
    _print(report)
    return 0


def cmd_pending(database: Database, args) -> int:
    settings = load_summary_settings()
    now = _now()
    with database.connection() as conn:
        _require_schema(conn)
        lang = settings.language
        report = {
            "partition": "sample" if args.sample else "live",
            "missing_summary": store.select_missing(conn, is_sample=args.sample, language=lang, limit=args.limit),
            "summary_behind_new_members": store.select_uncovered(conn, is_sample=args.sample, language=lang,
                                                                 limit=args.limit),
            "due_jobs": store.select_due_jobs(conn, is_sample=args.sample, language=lang, limit=args.limit, now=now),
            "failed_jobs": [str(r["event_id"]) for r in conn.execute(
                "SELECT j.event_id FROM event_summary_job j JOIN news_event e ON e.event_id = j.event_id "
                "WHERE e.is_sample = %s AND j.status = 'failed' AND j.language_code = %s LIMIT %s",
                (args.sample, lang, args.limit))],
            "awaiting_review": [str(r["event_id"]) for r in conn.execute(
                "SELECT j.event_id FROM event_summary_job j JOIN news_event e ON e.event_id = j.event_id "
                "WHERE e.is_sample = %s AND j.status = 'awaiting_review' AND j.language_code = %s LIMIT %s",
                (args.sample, lang, args.limit))],
            "note": "no model was called; backfill handles the first two lists, retry the due and failed jobs",
        }
    _print(report)
    return 0


def cmd_backfill(database: Database, args) -> int:
    sctx = _context(database, args, limit=args.limit)
    if sctx is None:
        return 2
    now, lang = _now(), sctx.runtime.settings.language
    with database.transaction() as conn:
        missing = store.select_missing(conn, is_sample=args.sample, language=lang, limit=args.limit)
        uncovered = store.select_uncovered(conn, is_sample=args.sample, language=lang,
                                           limit=max(args.limit - len(missing), 0))
        for event_id in missing:
            job = store.get_job(conn, event_id, lang)
            if job is None or job["status"] == "completed":
                store.request_job(conn, event_id, lang, INITIAL, now=now, reset_attempts=True)
        for event_id in uncovered:
            store.request_job(conn, event_id, lang, NEW_INFORMATION, now=now, reset_attempts=True)
    print(f"selected {len(missing)} event(s) without a summary and {len(uncovered)} with new members",
          file=sys.stderr)
    _print(_run(missing + uncovered, sctx))
    return 0


def cmd_retry(database: Database, args) -> int:
    sctx = _context(database, args, limit=args.limit)
    if sctx is None:
        return 2
    lang = sctx.runtime.settings.language
    with database.transaction() as conn:
        requeued = store.requeue_failed(conn, is_sample=args.sample, language=lang, limit=args.limit,
                                        event_ids=args.event_id) if args.include_failed else []
        due = store.select_due_jobs(conn, is_sample=args.sample, language=lang, limit=args.limit, now=_now(),
                                    ignore_backoff=args.all, event_ids=args.event_id)
    result = _run(due, sctx, ignore_backoff=args.all)
    result["requeued_failed"] = len(requeued)
    _print(result)
    return 0


def cmd_run_event(database: Database, args) -> int:
    sctx = _context(database, args, limit=1)
    if sctx is None:
        return 2
    _print(_run([args.event_id], sctx, refresh=args.refresh, ignore_backoff=True))
    return 0


def cmd_show(database: Database, args) -> int:
    with database.connection() as conn:
        _print(store.inspect_event(conn, args.event_id))
    return 0


def cmd_review(database: Database, args) -> int:
    settings = load_summary_settings()
    if args.review_command == "list":
        with database.connection() as conn:
            _print({"partition": "sample" if args.sample else "live",
                    "reviews": store.open_reviews(conn, is_sample=args.sample, include_resolved=args.all,
                                                  limit=args.limit)})
        return 0
    if args.review_command == "show":
        with database.connection() as conn:
            _print(review.show(conn, args.review_id))
        return 0
    _print(review.decide(database, args.review_id, decision=args.decision, reviewer=args.reviewer,
                         explanation=args.explanation, language=settings.language))
    return 0


def _require_schema(conn) -> None:
    missing = store.missing_schema(conn)
    if missing:
        raise ConfigurationError(f"schema missing {missing}: run `uv run datasources-db migrate`")


# ----------------------------------------------------------------------------- entry point
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="datasources-summary", description=__doc__.split("\n\n")[0])
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("check", help="Validate summary configuration and schema (no writes, no model calls)")
    p_status = sub.add_parser("status", help="Summary jobs, versions, attempts and reviews")
    p_status.add_argument("--sample", action="store_true")
    p_pending = sub.add_parser("pending", help="List summary work without doing it (no model calls)")
    p_pending.add_argument("--sample", action="store_true")
    p_pending.add_argument("--limit", type=int, default=50)
    p_show = sub.add_parser("show", help="One event's summary versions, citations, attempts and reviews")
    p_show.add_argument("--event-id", required=True)
    paid = argparse.ArgumentParser(add_help=False)
    paid.add_argument("--confirm-paid-calls", action="store_true", help="Allow requests to the configured model")
    paid.add_argument("--sample", action="store_true")
    p_run = sub.add_parser("run-event", parents=[paid], help="Summarize ONE event now (at most one model request)")
    p_run.add_argument("--event-id", required=True)
    p_run.add_argument("--refresh", action="store_true", help="Regenerate even if the evidence is unchanged")
    p_backfill = sub.add_parser("backfill", parents=[paid],
                                help="Summarize grouped events that have no summary (or new members)")
    p_backfill.add_argument("--limit", type=int, default=10)
    p_retry = sub.add_parser("retry", parents=[paid], help="Run due summary jobs (failed attempts, deferrals, "
                                                           "review resolutions)")
    p_retry.add_argument("--limit", type=int, default=10)
    p_retry.add_argument("--include-failed", action="store_true", help="Also requeue jobs that gave up")
    p_retry.add_argument("--all", action="store_true", help="Ignore retry backoff")
    p_retry.add_argument("--event-id", action="append", help="Only these events (repeatable)")
    p_review = sub.add_parser("review", help="Conflict reviews")
    review_sub = p_review.add_subparsers(dest="review_command", required=True)
    p_list = review_sub.add_parser("list", help="Open (or --all) conflict reviews")
    p_list.add_argument("--sample", action="store_true")
    p_list.add_argument("--all", action="store_true", help="Include resolved reviews")
    p_list.add_argument("--limit", type=int, default=100)
    p_rshow = review_sub.add_parser("show", help="Both claims, their sources and the quoted evidence")
    p_rshow.add_argument("--review-id", required=True)
    p_decide = review_sub.add_parser("decide", help="Record a reviewer decision (audited)")
    p_decide.add_argument("--review-id", required=True)
    p_decide.add_argument("--reviewer", required=True, help="Reviewer identity (name or id)")
    p_decide.add_argument("--decision", required=True, choices=review.DECISIONS)
    p_decide.add_argument("--explanation", required=True)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING,
                        format="%(levelname)s %(name)s %(message)s")
    command = args.command or "status"
    if command == "status" and not hasattr(args, "sample"):
        args.sample = False
    database: Database | None = None
    try:
        if getattr(args, "limit", 1) is not None and getattr(args, "limit", 1) <= 0:
            raise ConfigurationError("--limit must be greater than 0")
        database = Database(load_db_settings())
        handlers = {"check": lambda: cmd_check(database), "status": lambda: cmd_status(database, args),
                    "pending": lambda: cmd_pending(database, args), "show": lambda: cmd_show(database, args),
                    "run-event": lambda: cmd_run_event(database, args),
                    "backfill": lambda: cmd_backfill(database, args), "retry": lambda: cmd_retry(database, args),
                    "review": lambda: cmd_review(database, args)}
        return handlers[command]()
    except (ConfigurationError, DatabaseError) as error:
        sys.stderr.write(f"Configuration error: {error}\n")
        return 2
    finally:
        if database is not None:
            database.close()


if __name__ == "__main__":
    raise SystemExit(main())
