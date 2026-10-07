"""Summary steps run by the ingestion graph's summary nodes (and by ``datasources-summary``).

After ``persist_result`` has COMMITTED a record and its grouping, every event the
record is linked to is routed on its own:

    route_event_summary --(generate)--> generate_event_summary -> validate_event_summary -> persist_event_summary
                        --(conflict)--> flag_conflict_review
                        --(skip)------> skip_summary

Routing rules (evaluated in this order, reusing grouping's stored decisions; no
model call is made to decide anything here)
    * pending ``uncertain`` relation   -> skip: unresolved evidence never enters a summary
    * an open or unreviewed conflict   -> flag_conflict_review: one review per comparison,
                                          the published summary stays up, no replacement
    * another process holds the lease  -> skip
    * no published summary             -> generate the first summary (even for one article)
    * same_event_update                -> generate a new version
    * summary work already pending     -> generate (e.g. a prior failure is retried even when
      (earlier failure, review done)      the new record only repeats known information)
    * same_event_repeated / exact copy -> skip: the existing summary is retained
    ``different_event`` needs no rule: grouping gives the record its own event,
    which then has no summary and is summarized as a new event.

Failures never propagate: the record and its grouping were committed before
any of this runs, and a summary failure only moves the durable job
(``event_summary_job``) to pending-with-backoff or failed.
"""

from __future__ import annotations

import logging
import os
import re
import socket
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from ..ingest.settings import ConfigurationError
from ..similarity.models import sanitize
from . import evidence, store
from .generation import SummaryError, SummaryRequest, build_summary_backend, credentials_available
from .models import (
    CONFLICT,
    GENERATE,
    INITIAL,
    MANUAL_REFRESH,
    NEW_INFORMATION,
    REVIEW_RESOLUTION,
    SKIP,
    SummaryTask,
)
from .settings import SummarySettings
from .validation import validate_summary

LOGGER = logging.getLogger(__name__)
MAX_EVENTS_PER_RECORD = 10  # more are queued as durable pending jobs (bounded graph recursion)
NON_RETRYABLE_KINDS = frozenset({"authentication", "model_not_found", "bad_request", "refusal"})
LEASED_ELSEWHERE = "summary work is running in another process"  # skip reason while another lease is live


# ----------------------------------------------------------------------------- runtime
@dataclass
class SummaryRuntime:
    settings: SummarySettings
    backend: Any
    owner: str = field(default_factory=lambda: f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}")

    @property
    def provider(self) -> str:
        return getattr(self.backend, "name", "unknown")

    @property
    def model(self) -> str | None:
        return getattr(self.backend, "model", None)

    def describe(self) -> dict[str, Any]:
        s = self.settings
        return {"backend": f"{self.provider}:{self.model}", "language": s.language,
                "max_requests_per_run": s.max_requests_per_run, "max_input_chars": s.max_input_chars,
                "max_attempts": s.max_attempts}


def build_summary_runtime(database, settings: SummarySettings, *, backend=None,
                          grouping_enabled: bool = True) -> SummaryRuntime:
    """Validate the summary configuration at startup (not per record)."""
    if database is None:
        raise ConfigurationError("SUMMARY_ENABLED=true requires PostgreSQL (DATASOURCES_DB_ENABLED=true)")
    if not grouping_enabled:
        raise ConfigurationError("SUMMARY_ENABLED=true summarizes grouped events and needs SIMILARITY_ENABLED=true "
                                 "(or set SUMMARY_ENABLED=false)")
    with database.connection() as conn:
        missing = store.missing_schema(conn)
    if missing:
        raise ConfigurationError(f"database schema is missing {', '.join(missing)}; run `uv run datasources-db "
                                 "migrate` (migrations 004-005)")
    if backend is None:
        if settings.backend == "anthropic" and not credentials_available():
            raise ConfigurationError("SUMMARY_BACKEND=anthropic needs API credentials: set ANTHROPIC_API_KEY in .env "
                                     "(or log in with `ant auth login`)")
        backend = build_summary_backend(settings)
    return SummaryRuntime(settings, backend)


class SummaryBudget:
    """Cap on summary requests for one run (one graph invocation or one CLI command).

    ``attempts`` is an optional run-wide cap on actual model HTTP attempts (``llm_budget.AttemptLedger``),
    shared with the comparison calls: once it is used up, no further request is started.
    """

    def __init__(self, limit: int, attempts=None):
        self.limit = limit
        self.used = 0
        self.attempts = attempts

    def available(self) -> bool:
        return self.used < self.limit and (self.attempts is None or not self.attempts.exhausted())

    def exhausted_reason(self) -> str:
        if self.attempts is not None and self.attempts.exhausted():
            return f"the run's model-attempt budget ({self.attempts.limit} HTTP attempts) is used up"
        return f"this run's cap of {self.limit} summary request(s) reached (SUMMARY_MAX_REQUESTS_PER_RUN / --limit)"

    def spend(self) -> None:
        self.used += 1


@dataclass
class SummaryContext:
    database: Any
    runtime: SummaryRuntime
    budget: SummaryBudget
    now: Any = None  # callable returning an aware datetime (tests)
    activity: Any = None  # live progress for the page (`activity.ActivityTracker`); None = not reported

    def clock(self) -> datetime:
        return self.now() if self.now else datetime.now(timezone.utc)


def context_from(ctx) -> SummaryContext | None:
    """The summary context of a graph run (``PipelineContext.summarizer``), created once per run."""
    runtime = getattr(ctx, "summarizer", None)
    if runtime is None or getattr(ctx, "database", None) is None or not getattr(ctx, "persist", True):
        return None
    if getattr(ctx, "summary_budget", None) is None:
        ctx.summary_budget = SummaryBudget(runtime.settings.max_requests_per_run,
                                           attempts=getattr(ctx, "model_budget", None))
    return SummaryContext(ctx.database, runtime, ctx.summary_budget, getattr(ctx, "now", None),
                          getattr(ctx, "activity", None))


def _error(stage: str, error: BaseException) -> dict[str, Any]:
    return {"stage": stage, "error_type": type(error).__name__, "message": sanitize(str(error))[:500],
            "kind": getattr(error, "kind", None), "retryable": getattr(error, "retryable", True)}


# ----------------------------------------------------------------------------- targets
def targets(outcome, sctx: SummaryContext | None, *, profile_id: str | None = None,
            pipeline_version: str | None = None) -> list[SummaryTask]:
    """Every event the just-committed record is linked to (a multi-Item filing gives several)."""
    if sctx is None or outcome is None or not getattr(outcome, "persisted", False) or outcome.error:
        return []
    if not outcome.revision_id:
        return []
    language = sctx.runtime.settings.language
    try:
        with sctx.database.connection() as conn:
            rows = store.record_targets(conn, outcome.revision_id)
            if profile_id is not None:
                space = {str(r["event_id"]) for r in conn.execute(
                    "SELECT event_id FROM news_event WHERE event_id = ANY(%s::uuid[]) AND profile_id = %s "
                    "AND pipeline_version = %s", ([str(r["event_id"]) for r in rows], profile_id, pipeline_version))}
                rows = [r for r in rows if str(r["event_id"]) in space]
    except Exception as error:  # the record is committed; its summary work is found again by `backfill`
        LOGGER.warning("summary targets for %s could not be read: %s", outcome.revision_id, sanitize(str(error)))
        outcome.summaries.append({"route": None, "result": "error", "error": _error("targets", error)})
        return []
    tasks = [SummaryTask(event_id=str(r["event_id"]), language=language, revision_id=outcome.revision_id,
                         relation=r["relation"], membership_state=r["state"], comparison_label=r["label"],
                         comparison_method=r["method"]) for r in rows]
    active = [t for t in tasks if t.membership_state == "active"]
    if len(active) > MAX_EVENTS_PER_RECORD:
        overflow = active[MAX_EVENTS_PER_RECORD:]
        _defer_overflow(overflow, sctx)
        for task in overflow:
            outcome.summaries.append(task.report())
        tasks = [t for t in tasks if t not in overflow]
    return tasks


def _defer_overflow(tasks: list[SummaryTask], sctx: SummaryContext) -> None:
    now = sctx.clock()
    for task in tasks:
        task.route, task.reason, task.result = SKIP, "more events than one record processes inline", "deferred"
        try:
            with sctx.database.transaction() as conn:
                has_summary = evidence.published_summary(conn, task.event_id, task.language) is not None
                store.request_job(conn, task.event_id, task.language, NEW_INFORMATION if has_summary else INITIAL,
                                  now=now, reset_attempts=False)
        except Exception as error:
            task.error = _error("defer", error)


# ----------------------------------------------------------------------------- node: route_event_summary
def decide(task: SummaryTask, *, has_published: bool, blocking_conflicts: int, job: dict | None, now: datetime,
           trigger: str = "record") -> tuple[str, str, str | None]:
    """Pure routing decision: (route, reason, generation_reason). See the module docstring."""
    if task.membership_state == "pending":
        return SKIP, "uncertain relation: not incorporated into the event summary", None
    if task.membership_state == "detached":
        return SKIP, "membership detached by a review decision", None
    if blocking_conflicts:
        return CONFLICT, f"{blocking_conflicts} open conflict review(s) block a new summary", None
    status = job["status"] if job else None
    if status == "running" and job.get("lease_expires_at") and job["lease_expires_at"] > now:
        return SKIP, LEASED_ELSEWHERE, None
    queued_reason = job["reason"] if job and status in ("pending", "awaiting_review", "running") else None
    if trigger == "manual":
        return GENERATE, "manual refresh", MANUAL_REFRESH
    if not has_published:
        if status == "failed" and task.relation in (None, "same_event_repeated", "origin"):
            return SKIP, "summary generation failed repeatedly; run `datasources-summary retry --include-failed`", None
        reason = queued_reason if queued_reason == REVIEW_RESOLUTION else INITIAL
        return GENERATE, "the event has no published summary", reason
    if task.relation == "same_event_update":
        return GENERATE, "the record adds new information to the event", NEW_INFORMATION
    if queued_reason is not None:
        return GENERATE, "summary work is queued (earlier failure, review decision or deferral)", queued_reason
    if task.relation == "same_event_repeated":
        how = "exact copy" if task.comparison_method == "exact_content" else "repeated information"
        return SKIP, f"{how}: the published summary is retained", None
    return SKIP, "the published summary already covers this record", None


def route(task: SummaryTask, sctx: SummaryContext, *, trigger: str = "record") -> str:
    try:
        with sctx.database.connection() as conn:
            has_published = evidence.published_summary(conn, task.event_id, task.language) is not None
            blocking = _blocking_conflicts(conn, task.event_id)
            task.job = store.get_job(conn, task.event_id, task.language)
    except Exception as error:
        task.route, task.reason, task.error = SKIP, "could not read the event's summary state", _error("route", error)
        return task.route
    task.route, task.reason, task.generation_reason = decide(task, has_published=has_published,
                                                             blocking_conflicts=blocking, job=task.job,
                                                             now=sctx.clock(), trigger=trigger)
    return task.route


def _blocking_conflicts(conn, event_id: str) -> int:
    unreviewed = conn.execute(
        """
        SELECT count(DISTINCT c.comparison_id) AS n
          FROM event_comparison c
          JOIN current_event_membership m ON m.comparison_id = c.comparison_id
          LEFT JOIN event_conflict_review v ON v.comparison_id = c.comparison_id
         WHERE c.candidate_event_id = %s AND c.label = 'same_event_conflict' AND v.review_id IS NULL
        """,
        (event_id,),
    ).fetchone()["n"]
    return int(unreviewed) + evidence.open_review_count(conn, event_id)


# ----------------------------------------------------------------------------- node: flag_conflict_review
def flag_conflict_review(task: SummaryTask, sctx: SummaryContext) -> None:
    try:
        with sctx.database.transaction() as conn:
            task.reviews_created = store.ensure_conflict_reviews(conn, task.event_id)
            has_summary = evidence.published_summary(conn, task.event_id, task.language) is not None
            task.job = store.mark_awaiting_review(conn, task.event_id, task.language,
                                                  NEW_INFORMATION if has_summary else INITIAL, now=sctx.clock())
        task.result = "awaiting_review"
    except Exception as error:
        LOGGER.warning("conflict review for event %s could not be recorded: %s", task.event_id, error)
        task.result, task.error = "error", _error("flag_conflict_review", error)


def skip_summary(task: SummaryTask, sctx: SummaryContext | None = None) -> None:
    task.result = task.result or "skipped"


# ----------------------------------------------------------------------------- node: generate_event_summary
def generate(task: SummaryTask, sctx: SummaryContext, *, refresh: bool = False, ignore_backoff: bool = False) -> None:
    """Claim the job, read bounded evidence, and call the model with NO transaction open."""
    runtime, now = sctx.runtime, sctx.clock()
    try:
        with sctx.database.transaction() as conn:
            job = store.get_job(conn, task.event_id, task.language, lock=True)
            status = job["status"] if job else None
            reason = task.generation_reason or INITIAL
            if status in (None, "completed", "failed", "awaiting_review") or refresh:
                store.request_job(conn, task.event_id, task.language, reason, now=now, reset_attempts=True)
            elif status == "pending" and task.relation == "same_event_update":
                # New evidence while earlier work waits (e.g. in backoff): due now, attempts reset.
                keep = job["reason"] if job["reason"] in (INITIAL, REVIEW_RESOLUTION) else NEW_INFORMATION
                store.request_job(conn, task.event_id, task.language, keep, now=now, reset_attempts=True)
            # Otherwise a pending job is retried as it is: its backoff and attempt count are kept.
            if not sctx.budget.available():
                task.result = "deferred"
                task.reason += f"; {sctx.budget.exhausted_reason()}"
                task.job = store.get_job(conn, task.event_id, task.language)
                return
            task.job = store.claim_job(conn, task.event_id, task.language, owner=runtime.owner,
                                       lease_minutes=runtime.settings.lease_minutes, now=now,
                                       ignore_backoff=ignore_backoff or refresh)
            if task.job is None:
                current = store.get_job(conn, task.event_id, task.language)
                if current and current["status"] == "running":
                    task.result = "claimed_elsewhere"
                else:
                    task.result = "deferred"
                    when = current["next_retry_at"].isoformat() if current and current["next_retry_at"] else "later"
                    task.reason += f"; retry backoff until {when}"
                return
            task.generation_reason = task.job["reason"]
            task.lease_owner = runtime.owner
        with sctx.database.connection() as conn:
            task.pack = evidence.build_pack(conn, task.event_id, runtime.settings)
            published = evidence.published_summary(conn, task.event_id, task.language)
    except evidence.EvidenceError as error:
        _finish(task, sctx, status="completed", last_error=f"nothing to summarize: {error}")
        task.result, task.reason = "skipped", str(error)
        return
    except Exception as error:
        LOGGER.warning("summary preparation failed for event %s: %s", task.event_id, sanitize(str(error)))
        task.error = _error("prepare", error)
        if task.lease_owner:
            _finish(task, sctx, status="pending", next_retry_at=now, last_error=task.error["message"])
        task.result = "error"
        return

    if not refresh and published is not None and published["input_fingerprint"] == task.pack.input_fingerprint:
        _finish(task, sctx, status="completed", summary_id=str(published["summary_id"]),
                fingerprint=task.pack.input_fingerprint)
        task.result, task.version = "unchanged", int(published["version"])
        return

    sctx.budget.spend()
    task.model = runtime.model
    if sctx.activity is not None:
        sctx.activity.step("summary", "Writing the summary", model=runtime.model)
    try:
        call = runtime.backend.summarize(SummaryRequest(task.pack))
        task.output, task.model, task.usage = call.output, call.model or runtime.model, dict(call.usage)
        if call.request_id:
            task.usage["request_id"] = call.request_id
    except SummaryError as error:
        task.error = _error("model", error)
        if error.raw_output is not None:
            task.output = {"raw_text": error.raw_output}
    except Exception as error:
        LOGGER.exception("summary model call failed for event %s", task.event_id)
        task.error = _error("model", error)


# ----------------------------------------------------------------------------- node: validate_event_summary
def validate(task: SummaryTask, sctx: SummaryContext | None = None) -> None:
    if task.result is not None or task.error is not None or task.pack is None:
        return
    result = validate_summary(task.pack, task.output)
    task.draft, task.validation_errors = result.draft, result.errors


# ----------------------------------------------------------------------------- node: persist_event_summary
def persist(task: SummaryTask, sctx: SummaryContext) -> None:
    """Publish a valid draft atomically; otherwise keep the attempt and schedule a bounded retry."""
    if task.result is not None or task.pack is None or task.job is None:
        return
    runtime, now = sctx.runtime, sctx.clock()
    attempt = int(task.job["attempts"]) + 1
    if task.error is None and task.draft is not None:
        try:
            with sctx.database.transaction() as conn:
                published = store.publish(conn, task, provider=runtime.provider, now=now)
                if published.result == "published":
                    store.record_attempt(conn, task, outcome="published", attempt_number=attempt,
                                         provider=runtime.provider, summary_id=published.summary_id)
                    store.finish_job(conn, str(task.job["job_id"]), runtime.owner, status="completed", now=now,
                                     attempts=attempt, fingerprint=task.pack.input_fingerprint,
                                     summary_id=published.summary_id)
                    task.result, task.summary_id, task.version = "published", published.summary_id, published.version
                    return
            # Not published: the transaction above committed nothing but the event lock.
            with sctx.database.transaction() as conn:
                store.record_attempt(conn, task, outcome=published.result, attempt_number=attempt,
                                     provider=runtime.provider, error_message=published.detail)
                if published.result == "blocked":
                    store.ensure_conflict_reviews(conn, task.event_id)
                    store.finish_job(conn, str(task.job["job_id"]), runtime.owner, status="awaiting_review",
                                     now=now, attempts=attempt, last_error=published.detail)
                else:
                    status = "failed" if attempt >= runtime.settings.max_attempts else "pending"
                    store.finish_job(conn, str(task.job["job_id"]), runtime.owner, status=status, now=now,
                                     attempts=attempt, next_retry_at=now if status == "pending" else None,
                                     last_error=published.detail)
            task.result, task.reason = published.result, published.detail
            return
        except Exception as error:
            LOGGER.warning("summary publication failed for event %s: %s", task.event_id, sanitize(str(error)))
            task.error = _error("publish", error)

    outcome = "invalid" if task.error is None else "error"
    retryable = task.error is None or (task.error.get("retryable", True)
                                       and task.error.get("kind") not in NON_RETRYABLE_KINDS)
    status = "pending" if retryable and attempt < runtime.settings.max_attempts else "failed"
    delay = runtime.settings.retry_base_minutes * 2 ** max(attempt - 1, 0)
    message = "; ".join(task.validation_errors)[:1000] if outcome == "invalid" else task.error["message"]
    try:
        with sctx.database.transaction() as conn:
            store.record_attempt(conn, task, outcome=outcome, attempt_number=attempt, provider=runtime.provider,
                                 error_kind=(task.error or {}).get("kind"), error_message=message)
            store.finish_job(conn, str(task.job["job_id"]), runtime.owner, status=status, now=now, attempts=attempt,
                             next_retry_at=now + timedelta(minutes=delay) if status == "pending" else None,
                             last_error=f"{outcome}: {message}")
    except Exception as error:  # the lease expires, so the job is claimable again later
        LOGGER.warning("could not record the failed summary attempt for event %s: %s", task.event_id,
                       sanitize(str(error)))
    task.result = outcome


def report_activity(task: SummaryTask, sctx: SummaryContext | None) -> None:
    """Show how this event's summary step ended on the page (`activity`): the step started before the model call
    is closed with the outcome; a decision made without a model call becomes a step of its own."""
    track = getattr(sctx, "activity", None)
    if track is None:
        return
    result = task.result
    if result == "published":
        state, label, text = "done", "Summary written", f"published as version {task.version}"
    elif result == "invalid":
        first = task.validation_errors[0] if task.validation_errors else "unknown problem"
        state, label, text = "failed", "Summary written", f"rejected by the citation checks ({first}); retried later"
    elif result == "error":
        message = (task.error or {}).get("message") or "unknown error"
        state, label, text = "failed", "Summary", f"failed: {message[:160]}"
    elif result == "unchanged":
        state, label, text = "skipped", "Summary up to date", "the sources have not changed"
    elif result == "stale":
        state, label, text = "skipped", "Summary written", "the story changed meanwhile; it is written again"
    elif result in ("blocked", "awaiting_review"):
        state, label, text = "skipped", "Summary on hold", "conflicting reports: waiting for a reviewer"
    elif result == "deferred":
        state, label, text = "skipped", "Summary postponed", _postponed(task.reason or "")
    elif result == "claimed_elsewhere" or task.reason == LEASED_ELSEWHERE:
        state, label, text = "skipped", "Summary", "being written by another process"
    else:
        state, label, text = "skipped", "Summary not needed", task.reason or result or "nothing to do"
    if track.running("summary"):
        track.end_step(state, text, key="summary")
    else:
        track.note("summary", label, state, text)


def _postponed(reason: str) -> str:
    """The page's wording for a deferral: a retry wait (with its end in UTC) or a used-up request cap."""
    match = re.search(r"retry backoff until (\S+)", reason)
    if match:
        try:
            until = datetime.fromisoformat(match.group(1)).astimezone(timezone.utc)
            return f"waiting before the next try (until {until:%H:%M} UTC)"
        except ValueError:
            return "waiting before the next try"
    if "budget" in reason or "cap of" in reason:
        return "this run's limit on AI requests is used up; it stays queued"
    return reason or "postponed"


def _finish(task: SummaryTask, sctx: SummaryContext, *, status: str, next_retry_at: datetime | None = None,
            last_error: str | None = None, summary_id: str | None = None, fingerprint: str | None = None) -> None:
    if task.job is None or task.lease_owner is None:
        return
    try:
        with sctx.database.transaction() as conn:
            store.finish_job(conn, str(task.job["job_id"]), task.lease_owner, status=status, now=sctx.clock(),
                             next_retry_at=next_retry_at, last_error=last_error, summary_id=summary_id,
                             fingerprint=fingerprint)
    except Exception as error:
        LOGGER.warning("could not release summary job for event %s: %s", task.event_id, sanitize(str(error)))


# ----------------------------------------------------------------------------- one event, end to end (CLI)
def process_event(event_id: str, sctx: SummaryContext, *, trigger: str = "job", refresh: bool = False,
                  ignore_backoff: bool = False) -> SummaryTask:
    """Route and run one event outside the ingestion graph (retry, backfill, run-event)."""
    task = SummaryTask(event_id=event_id, language=sctx.runtime.settings.language, membership_state="active")
    route(task, sctx, trigger="manual" if refresh else trigger)
    if task.route == GENERATE:
        generate(task, sctx, refresh=refresh, ignore_backoff=ignore_backoff)
        validate(task, sctx)
        persist(task, sctx)
    elif task.route == CONFLICT:
        flag_conflict_review(task, sctx)
    else:
        skip_summary(task, sctx)
    report_activity(task, sctx)
    return task
