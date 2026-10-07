"""SQL for event summaries: durable jobs, attempts, publication, conflict reviews, status.

Transactions are short and never span a model call. Generation reads evidence
in one read-only transaction, calls the model with no transaction open, and
publishes in a separate transaction that locks the event row, re-checks the
evidence fingerprint, open conflicts and the published version, and only then
supersedes the old version and inserts the new one.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from . import evidence
from .models import SummaryTask

LOGGER = logging.getLogger(__name__)
REQUIRED_RELATIONS = ("event_summary", "event_summary_citation", "event_conflict_review", "event_summary_job",
                      "event_summary_attempt", "event_review_decision", "current_event_summary")
REQUIRED_COLUMNS = (("event_summary", "statements"), ("event_summary", "input_manifest"),
                    ("event_summary", "limitations"), ("event_summary_citation", "quote_index"))
OPEN_REVIEW = ("open", "in_review")


def missing_schema(conn: Connection) -> list[str]:
    missing = [name for name in REQUIRED_RELATIONS
               if not conn.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (f"public.{name}",)).fetchone()["ok"]]
    for table, column in REQUIRED_COLUMNS:
        if table in missing:
            continue
        if conn.execute("SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' "
                        "AND table_name = %s AND column_name = %s", (table, column)).fetchone() is None:
            missing.append(f"{table}.{column}")
    detached = conn.execute("SELECT pg_get_constraintdef(oid) AS d FROM pg_constraint "
                            "WHERE conname = 'event_membership_state_check'").fetchone()
    if detached is None or "detached" not in detached["d"]:
        missing.append("event_membership.state 'detached'")
    return missing


# ----------------------------------------------------------------------------- affected events
def record_targets(conn: Connection, revision_id: str) -> list[dict[str, Any]]:
    """Every event this revision is linked to, with how it is linked (active and pending)."""
    return conn.execute(
        """
        SELECT m.event_id, m.relation, m.state, c.label, c.method, e.status AS event_status
          FROM event_membership m
          JOIN news_event e ON e.event_id = m.event_id
          LEFT JOIN event_comparison c ON c.comparison_id = m.comparison_id
         WHERE m.revision_id = %s AND e.status <> 'superseded'
         ORDER BY m.created_at, m.event_id
        """,
        (revision_id,),
    ).fetchall()


# ----------------------------------------------------------------------------- jobs
def get_job(conn: Connection, event_id: str, language: str, *, lock: bool = False) -> dict[str, Any] | None:
    return conn.execute(
        f"SELECT * FROM event_summary_job WHERE event_id = %s AND language_code = %s{' FOR UPDATE' if lock else ''}",
        (event_id, language),
    ).fetchone()


def request_job(conn: Connection, event_id: str, language: str, reason: str, *, now: datetime,
                reset_attempts: bool) -> dict[str, Any]:
    """Queue summary work (status ``pending``, due now). A job another process is running keeps its lease:
    that process re-checks the evidence before publishing and requeues itself if it changed."""
    row = conn.execute(
        """
        INSERT INTO event_summary_job (event_id, language_code, status, reason, next_retry_at)
        VALUES (%(event)s, %(lang)s, 'pending', %(reason)s, %(now)s)
        ON CONFLICT (event_id, language_code) DO UPDATE
           SET status = 'pending', reason = EXCLUDED.reason, next_retry_at = EXCLUDED.next_retry_at,
               attempts = CASE WHEN %(reset)s THEN 0 ELSE event_summary_job.attempts END,
               last_error = CASE WHEN %(reset)s THEN NULL ELSE event_summary_job.last_error END,
               lease_owner = NULL, lease_expires_at = NULL, completed_at = NULL, updated_at = now()
         WHERE NOT (event_summary_job.status = 'running' AND event_summary_job.lease_expires_at > %(now)s)
        RETURNING *
        """,
        {"event": event_id, "lang": language, "reason": reason, "now": now, "reset": reset_attempts},
    ).fetchone()
    return row or get_job(conn, event_id, language)


def mark_awaiting_review(conn: Connection, event_id: str, language: str, reason: str, *,
                         now: datetime) -> dict[str, Any]:
    row = conn.execute(
        """
        INSERT INTO event_summary_job (event_id, language_code, status, reason)
        VALUES (%(event)s, %(lang)s, 'awaiting_review', %(reason)s)
        ON CONFLICT (event_id, language_code) DO UPDATE
           SET status = 'awaiting_review',
               reason = CASE WHEN event_summary_job.status IN ('pending', 'failed', 'awaiting_review')
                             THEN event_summary_job.reason ELSE EXCLUDED.reason END,
               next_retry_at = NULL, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
         WHERE NOT (event_summary_job.status = 'running' AND event_summary_job.lease_expires_at > %(now)s)
        RETURNING *
        """,
        {"event": event_id, "lang": language, "reason": reason, "now": now},
    ).fetchone()
    return row or get_job(conn, event_id, language)


def claim_job(conn: Connection, event_id: str, language: str, *, owner: str, lease_minutes: float, now: datetime,
              ignore_backoff: bool = False) -> dict[str, Any] | None:
    """Lease a due pending job (or one whose lease expired). None = nothing due, or leased elsewhere."""
    return conn.execute(
        """
        UPDATE event_summary_job
           SET status = 'running', lease_owner = %(owner)s, lease_expires_at = %(until)s, updated_at = now()
         WHERE event_id = %(event)s AND language_code = %(lang)s
           AND ((status = 'pending' AND (%(ignore)s OR next_retry_at IS NULL OR next_retry_at <= %(now)s))
                OR (status = 'running' AND lease_expires_at <= %(now)s))
        RETURNING *
        """,
        {"owner": owner, "until": now + timedelta(minutes=lease_minutes), "event": event_id, "lang": language,
         "ignore": ignore_backoff, "now": now},
    ).fetchone()


def finish_job(conn: Connection, job_id: str, owner: str, *, status: str, now: datetime, attempts: int | None = None,
               next_retry_at: datetime | None = None, last_error: str | None = None,
               fingerprint: str | None = None, summary_id: str | None = None) -> bool:
    """Release the lease with a final state. False = the lease was lost (another process took over)."""
    row = conn.execute(
        """
        UPDATE event_summary_job
           SET status = %(status)s, attempts = COALESCE(%(attempts)s, attempts), next_retry_at = %(next)s,
               last_error = %(error)s, last_input_fingerprint = COALESCE(%(fp)s, last_input_fingerprint),
               summary_id = COALESCE(%(summary)s, summary_id), lease_owner = NULL, lease_expires_at = NULL,
               completed_at = CASE WHEN %(status)s = 'completed' THEN %(now)s ELSE NULL END, updated_at = now()
         WHERE job_id = %(job)s AND lease_owner = %(owner)s AND status = 'running'
        RETURNING job_id
        """,
        {"status": status, "attempts": attempts, "next": next_retry_at, "error": (last_error or None) and
         last_error[:1000], "fp": fingerprint, "summary": summary_id, "now": now, "job": job_id, "owner": owner},
    ).fetchone()
    if row is None:
        LOGGER.warning("summary job %s: lease lost before it could be released", job_id)
    return row is not None


def record_attempt(conn: Connection, task: SummaryTask, *, outcome: str, attempt_number: int,
                   provider: str | None, summary_id: str | None = None, error_kind: str | None = None,
                   error_message: str | None = None) -> str:
    pack = task.pack
    assert pack is not None and task.job is not None
    row = conn.execute(
        """
        INSERT INTO event_summary_attempt (job_id, event_id, language_code, attempt_number, outcome,
            generation_reason, input_fingerprint, evidence_fingerprint, base_summary_id, summary_id, model_provider,
            model_name, prompt_version, output, validation_errors, error_kind, error_message, usage, input_manifest)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING attempt_id
        """,
        (task.job["job_id"], task.event_id, task.language, attempt_number, outcome, task.generation_reason,
         pack.input_fingerprint, pack.evidence_fingerprint, pack.previous.summary_id if pack.previous else None,
         summary_id, provider, task.model, _prompt_version(), Jsonb(task.output) if task.output is not None else None,
         Jsonb(task.validation_errors), error_kind, (error_message or None) and error_message[:1000],
         Jsonb(task.usage or {}), Jsonb(pack.manifest())),
    ).fetchone()
    return str(row["attempt_id"])


def _prompt_version() -> str:
    from .settings import PROMPT_VERSION

    return PROMPT_VERSION


# ----------------------------------------------------------------------------- publication
class PublishOutcome:
    def __init__(self, result: str, summary_id: str | None = None, version: int | None = None, detail: str = ""):
        self.result = result  # published | stale | blocked
        self.summary_id = summary_id
        self.version = version
        self.detail = detail


def publish(conn: Connection, task: SummaryTask, *, provider: str, now: datetime) -> PublishOutcome:
    """Publish a validated draft atomically (inside the caller's transaction).

    The event row is locked, so two processes publishing for the same event are
    serialized; the partial unique index guarantees one published row per
    (event, language) in any case. The draft is refused as ``stale`` when the
    event's evidence or its published version changed since the evidence was
    read, and as ``blocked`` when a conflict review opened meanwhile.
    """
    pack, draft = task.pack, task.draft
    assert pack is not None and draft is not None
    event = evidence.event_row(conn, task.event_id, lock=True)
    if event is None or event["status"] == "superseded":
        return PublishOutcome("stale", detail="the event was superseded during generation")
    if evidence.open_review_count(conn, task.event_id):
        return PublishOutcome("blocked", detail="a conflict review opened during generation")
    if evidence.current_evidence_fingerprint(conn, task.event_id, task.language) != pack.evidence_fingerprint:
        return PublishOutcome("stale", detail="the event's evidence changed during generation")
    current = evidence.published_summary(conn, task.event_id, task.language, lock=True)
    base = pack.previous.summary_id if pack.previous else None
    if (str(current["summary_id"]) if current else None) != base:
        return PublishOutcome("stale", detail="another summary version was published during generation")

    unit_ids = sorted({c.unit_id for s in draft.statements for c in s.citations})
    allowed = {str(r["unit_id"]) for r in conn.execute(
        """
        SELECT mu.unit_id FROM current_event_membership m
          JOIN event_membership_unit mu ON mu.membership_id = m.membership_id
          JOIN text_unit t ON t.unit_id = mu.unit_id
          JOIN document_revision r ON r.revision_id = t.revision_id
          JOIN document d ON d.document_id = r.document_id
         WHERE m.event_id = %s AND d.is_sample = %s AND mu.unit_id = ANY(%s::uuid[])
        """,
        (task.event_id, event["is_sample"], unit_ids),
    )}
    if set(unit_ids) - allowed:
        return PublishOutcome("stale", detail="cited evidence is no longer an active member of this event")

    version = int(conn.execute("SELECT COALESCE(max(version), 0) + 1 AS v FROM event_summary "
                               "WHERE event_id = %s AND language_code = %s",
                               (task.event_id, task.language)).fetchone()["v"])
    if current is not None:
        conn.execute("UPDATE event_summary SET status = 'superseded' WHERE summary_id = %s", (current["summary_id"],))
    limitations = [*pack.limitations, *(f"not stated by the sources: {gap}" for gap in draft.evidence_gaps)]
    summary_id = str(conn.execute(
        """
        INSERT INTO event_summary (event_id, language_code, version, headline, summary_text, key_points, status,
            generation_reason, model_provider, model_name, prompt_version, input_fingerprint, usage, published_at,
            statements, input_manifest, limitations)
        VALUES (%s, %s, %s, %s, %s, %s, 'published', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING summary_id
        """,
        (task.event_id, task.language, version, draft.headline, draft.overview, Jsonb(draft.key_points),
         task.generation_reason, provider, task.model or "unknown", _prompt_version(), pack.input_fingerprint,
         Jsonb(task.usage or {}), now, Jsonb([s.as_dict() for s in draft.statements]), Jsonb(pack.manifest()),
         Jsonb(limitations)),
    ).fetchone()["summary_id"])
    # Every distinct quote is its own row (key: summary, statement, unit, quote_index). No ON CONFLICT: the
    # summary is new in this transaction, so a clash can only be a numbering bug and must fail the publication.
    for statement in draft.statements:
        for citation in statement.citations:
            conn.execute(
                "INSERT INTO event_summary_citation (summary_id, unit_id, statement_key, quote_index, evidence_quote) "
                "VALUES (%s, %s, %s, %s, %s)",
                (summary_id, citation.unit_id, statement.id, citation.quote_index, citation.quote),
            )
    return PublishOutcome("published", summary_id, version)


# ----------------------------------------------------------------------------- conflict reviews
def ensure_conflict_reviews(conn: Connection, event_id: str) -> int:
    """One review per same_event_conflict comparison of a current member (idempotent)."""
    rows = conn.execute(
        """
        INSERT INTO event_conflict_review (comparison_id)
        SELECT DISTINCT c.comparison_id
          FROM event_comparison c
          JOIN current_event_membership m ON m.comparison_id = c.comparison_id
         WHERE c.candidate_event_id = %s AND c.label = 'same_event_conflict'
        ON CONFLICT (comparison_id) DO NOTHING
        RETURNING review_id
        """,
        (event_id,),
    ).fetchall()
    return len(rows)


def open_reviews(conn: Connection, *, is_sample: bool, include_resolved: bool = False,
                 limit: int = 100) -> list[dict[str, Any]]:
    return conn.execute(
        """
        SELECT v.review_id, v.status, v.resolution, v.created_at, v.reviewer_id, e.event_id, e.title AS event_title,
               c.comparison_id, c.revision_id, r.title AS incoming_title, left(c.explanation, 240) AS explanation
          FROM event_conflict_review v
          JOIN event_comparison c ON c.comparison_id = v.comparison_id
          JOIN news_event e ON e.event_id = c.candidate_event_id
          JOIN document_revision r ON r.revision_id = c.revision_id
         WHERE e.is_sample = %s AND (%s OR v.status IN ('open', 'in_review'))
         ORDER BY v.created_at, v.review_id
         LIMIT %s
        """,
        (is_sample, include_resolved, limit),
    ).fetchall()


# ----------------------------------------------------------------------------- discovery
# ``since``: only events with a current member whose source time is at or after it (None = every event).
_RECENT_MEMBER = """(%(since)s::timestamptz IS NULL OR EXISTS (
               SELECT 1 FROM current_event_membership recent
                WHERE recent.event_id = {event} AND recent.source_time >= %(since)s::timestamptz))"""


def select_missing(conn: Connection, *, is_sample: bool, language: str, limit: int,
                   since: datetime | None = None) -> list[str]:
    """Active events with current members but no published summary and nothing blocking or failed."""
    rows = conn.execute(
        f"""
        SELECT e.event_id
          FROM news_event e
         WHERE e.is_sample = %(sample)s AND e.status <> 'superseded'
           AND EXISTS (SELECT 1 FROM current_event_membership m WHERE m.event_id = e.event_id)
           AND NOT EXISTS (SELECT 1 FROM event_summary s WHERE s.event_id = e.event_id AND s.language_code = %(lang)s
                             AND s.status = 'published')
           AND NOT EXISTS (SELECT 1 FROM event_summary_job j WHERE j.event_id = e.event_id
                             AND j.language_code = %(lang)s AND j.status IN ('running', 'failed', 'awaiting_review'))
           AND {_RECENT_MEMBER.format(event="e.event_id")}
         ORDER BY e.first_source_time NULLS LAST, e.event_id
         LIMIT %(limit)s
        """,
        {"sample": is_sample, "lang": language, "since": since, "limit": limit},
    ).fetchall()
    return [str(r["event_id"]) for r in rows]


def select_uncovered(conn: Connection, *, is_sample: bool, language: str, limit: int,
                     since: datetime | None = None) -> list[str]:
    """Events whose published summary predates an update or origin member (e.g. the process
    stopped after the article was stored, before its summary job was written)."""
    rows = conn.execute(
        f"""
        SELECT DISTINCT s.event_id, s.published_at
          FROM event_summary s
          JOIN news_event e ON e.event_id = s.event_id
          JOIN current_event_membership m ON m.event_id = s.event_id
         WHERE e.is_sample = %(sample)s AND s.language_code = %(lang)s AND s.status = 'published'
           AND m.relation IN ('origin', 'same_event_update', 'same_event_conflict')
           AND NOT COALESCE(s.input_manifest -> 'member_revision_ids', '[]'::jsonb) ? m.revision_id::text
           AND NOT EXISTS (SELECT 1 FROM event_summary_job j WHERE j.event_id = s.event_id
                             AND j.language_code = s.language_code
                             AND j.status IN ('pending', 'running', 'failed', 'awaiting_review'))
           AND {_RECENT_MEMBER.format(event="s.event_id")}
         ORDER BY s.published_at, s.event_id
         LIMIT %(limit)s
        """,
        {"sample": is_sample, "lang": language, "since": since, "limit": limit},
    ).fetchall()
    return [str(r["event_id"]) for r in rows]


def select_due_jobs(conn: Connection, *, is_sample: bool, language: str, limit: int, now: datetime,
                    ignore_backoff: bool = False, event_ids: list[str] | None = None,
                    since: datetime | None = None) -> list[str]:
    rows = conn.execute(
        f"""
        SELECT j.event_id
          FROM event_summary_job j JOIN news_event e ON e.event_id = j.event_id
         WHERE e.is_sample = %(sample)s AND j.language_code = %(lang)s
           AND ((j.status = 'pending' AND (%(ignore)s OR j.next_retry_at IS NULL OR j.next_retry_at <= %(now)s))
                OR (j.status = 'running' AND j.lease_expires_at <= %(now)s))
           AND (%(ids)s::uuid[] IS NULL OR j.event_id = ANY(%(ids)s::uuid[]))
           AND {_RECENT_MEMBER.format(event="j.event_id")}
         ORDER BY j.next_retry_at NULLS FIRST, j.updated_at, j.event_id
         LIMIT %(limit)s
        """,
        {"sample": is_sample, "lang": language, "ignore": ignore_backoff, "now": now, "ids": event_ids,
         "since": since, "limit": limit},
    ).fetchall()
    return [str(r["event_id"]) for r in rows]


def requeue_failed(conn: Connection, *, is_sample: bool, language: str, limit: int,
                   event_ids: list[str] | None = None) -> list[str]:
    rows = conn.execute(
        """
        WITH chosen AS (
            SELECT j.job_id FROM event_summary_job j JOIN news_event e ON e.event_id = j.event_id
             WHERE e.is_sample = %(sample)s AND j.language_code = %(lang)s AND j.status = 'failed'
               AND (%(ids)s::uuid[] IS NULL OR j.event_id = ANY(%(ids)s::uuid[]))
             ORDER BY j.updated_at, j.job_id LIMIT %(limit)s)
        UPDATE event_summary_job j SET status = 'pending', attempts = 0, next_retry_at = now(), updated_at = now()
          FROM chosen WHERE j.job_id = chosen.job_id
        RETURNING j.event_id
        """,
        {"sample": is_sample, "lang": language, "ids": event_ids, "limit": limit},
    ).fetchall()
    return [str(r["event_id"]) for r in rows]


# ----------------------------------------------------------------------------- status and inspection
def status_summary(conn: Connection, *, is_sample: bool, language: str) -> dict[str, Any]:
    def grouped(sql: str) -> dict[str, int]:
        return {str(r["k"]): int(r["n"]) for r in conn.execute(sql, (is_sample,))}

    events = conn.execute(
        """
        SELECT count(*) AS active_events,
               count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM event_summary s WHERE s.event_id = e.event_id
                                                   AND s.status = 'published' AND s.language_code = %s))
                   AS without_published_summary
          FROM news_event e
         WHERE e.is_sample = %s AND e.status <> 'superseded'
           AND EXISTS (SELECT 1 FROM current_event_membership m WHERE m.event_id = e.event_id)
        """,
        (language, is_sample),
    ).fetchone()
    usage = conn.execute(
        """
        SELECT count(*) AS attempts,
               COALESCE(sum((a.usage->>'input_tokens')::bigint), 0)::bigint AS input_tokens,
               COALESCE(sum((a.usage->>'output_tokens')::bigint), 0)::bigint AS output_tokens
          FROM event_summary_attempt a JOIN news_event e ON e.event_id = a.event_id WHERE e.is_sample = %s
        """,
        (is_sample,),
    ).fetchone()
    return {
        "events": dict(events),
        "jobs_by_status": grouped("SELECT j.status AS k, count(*) AS n FROM event_summary_job j JOIN news_event e "
                                  "ON e.event_id = j.event_id WHERE e.is_sample = %s GROUP BY 1 ORDER BY 1"),
        "summaries_by_status": grouped("SELECT s.status AS k, count(*) AS n FROM event_summary s JOIN news_event e "
                                       "ON e.event_id = s.event_id WHERE e.is_sample = %s GROUP BY 1 ORDER BY 1"),
        "attempts_by_outcome": grouped("SELECT a.outcome AS k, count(*) AS n FROM event_summary_attempt a "
                                       "JOIN news_event e ON e.event_id = a.event_id WHERE e.is_sample = %s "
                                       "GROUP BY 1 ORDER BY 1"),
        "reviews_by_status": grouped("SELECT v.status AS k, count(*) AS n FROM event_conflict_review v "
                                     "JOIN event_comparison c ON c.comparison_id = v.comparison_id "
                                     "JOIN news_event e ON e.event_id = c.candidate_event_id "
                                     "WHERE e.is_sample = %s GROUP BY 1 ORDER BY 1"),
        "model_usage": dict(usage),
    }


def inspect_event(conn: Connection, event_id: str) -> dict[str, Any]:
    event = conn.execute("SELECT event_id, is_sample, status, title, first_source_time, last_source_time "
                         "FROM news_event WHERE event_id = %s", (event_id,)).fetchone()
    if event is None:
        return {"event_id": event_id, "error": "not found"}
    summaries = conn.execute(
        "SELECT summary_id, language_code, version, status, generation_reason, headline, summary_text, statements, "
        "limitations, input_manifest, model_provider, model_name, prompt_version, usage, created_at, published_at "
        "FROM event_summary WHERE event_id = %s ORDER BY language_code, version", (event_id,)).fetchall()
    citations = conn.execute(
        """
        SELECT c.summary_id, c.statement_key, c.unit_id, c.quote_index, c.evidence_quote, t.revision_id, r.title,
               r.original_url
          FROM event_summary_citation c
          JOIN event_summary s ON s.summary_id = c.summary_id
          JOIN text_unit t ON t.unit_id = c.unit_id
          JOIN document_revision r ON r.revision_id = t.revision_id
         WHERE s.event_id = %s ORDER BY s.version, c.statement_key, c.unit_id, c.quote_index
        """,
        (event_id,),
    ).fetchall()
    attempts = conn.execute(
        "SELECT attempt_id, attempt_number, outcome, generation_reason, error_kind, error_message, "
        "validation_errors, model_name, usage, created_at, summary_id FROM event_summary_attempt "
        "WHERE event_id = %s ORDER BY created_at", (event_id,)).fetchall()
    reviews = conn.execute(
        """
        SELECT v.*, (SELECT json_agg(d ORDER BY d.created_at) FROM event_review_decision d
                      WHERE d.review_id = v.review_id) AS decisions
          FROM event_conflict_review v JOIN event_comparison c ON c.comparison_id = v.comparison_id
         WHERE c.candidate_event_id = %s ORDER BY v.created_at
        """,
        (event_id,),
    ).fetchall()
    return {"event": event, "jobs": conn.execute("SELECT * FROM event_summary_job WHERE event_id = %s",
                                                 (event_id,)).fetchall(),
            "summaries": summaries, "citations": citations, "attempts": attempts, "reviews": reviews}
