"""Human review of same_event_conflict comparisons (CLI only; there is no write endpoint).

Decisions
    not_a_conflict       the claims are compatible: the summary reports them normally
    confirmed_conflict   the sources disagree: the summary states both claims, each
                         attributed, and names no winner
    different_events     the incoming revision reports a different event: its
                         membership is DETACHED from the compared event (kept for
                         audit, no longer evidence) and its segment gets its own
                         provisional event, all in the same transaction as the
                         decision, before any regeneration is queued
    insufficient_evidence  nothing is decided: the review moves to ``in_review`` and
                         keeps blocking automatic publication

Every decision is appended to ``event_review_decision`` (reviewer, explanation,
resulting status, membership corrections, queued jobs). When a resolution
leaves an event with no open review, its summary job is queued
(``review_resolution``; a new event from a correction gets ``initial``). The
queued work runs in the worker when a later record touches the event, or with
``datasources-summary retry``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from psycopg.types.json import Jsonb

from ..ingest.settings import ConfigurationError
from ..similarity.store import SOURCE_TIME, refresh_event_stats
from . import evidence, store
from .models import INITIAL, REVIEW_RESOLUTION

DECISIONS = ("not_a_conflict", "confirmed_conflict", "different_events", "insufficient_evidence")


class ReviewError(ConfigurationError):
    """A review decision that cannot be applied (reported to the reviewer, nothing is written)."""


def decide(database, review_id: str, *, decision: str, reviewer: str, explanation: str, language: str,
           now: datetime | None = None) -> dict[str, Any]:
    if decision not in DECISIONS:
        raise ReviewError(f"--decision must be one of {', '.join(DECISIONS)}")
    reviewer, explanation = (reviewer or "").strip(), (explanation or "").strip()
    if not reviewer or not explanation:
        raise ReviewError("a decision needs --reviewer and a non-empty --explanation")
    now = now or datetime.now(timezone.utc)
    with database.transaction() as conn:
        review = conn.execute(
            """
            SELECT v.review_id, v.status, c.comparison_id, c.candidate_event_id, c.revision_id, c.analysis_id,
                   c.segment_key
              FROM event_conflict_review v JOIN event_comparison c ON c.comparison_id = v.comparison_id
             WHERE v.review_id = %s FOR UPDATE OF v
            """,
            (review_id,),
        ).fetchone()
        if review is None:
            raise ReviewError(f"review {review_id} not found")
        if review["status"] == "resolved":
            raise ReviewError(f"review {review_id} is already resolved")
        event_id = str(review["candidate_event_id"])
        event = evidence.event_row(conn, event_id, lock=True)
        corrections: list[dict[str, Any]] = []
        if decision == "insufficient_evidence":
            resulting = "in_review"
            conn.execute("UPDATE event_conflict_review SET status = 'in_review', reviewer_id = %s, "
                         "reviewer_notes = %s WHERE review_id = %s", (reviewer, explanation, review_id))
        else:
            resulting = "resolved"
            if decision == "different_events":
                corrections.append(_detach(conn, review, event))
            conn.execute("UPDATE event_conflict_review SET status = 'resolved', resolution = %s, reviewer_id = %s, "
                         "reviewer_notes = %s, resolved_at = %s WHERE review_id = %s",
                         (decision, reviewer, explanation, now, review_id))

        queued: list[dict[str, Any]] = []
        if resulting == "resolved" and event["status"] != "superseded":
            targets = [(event_id, REVIEW_RESOLUTION)]
            targets += [(c["new_event_id"], INITIAL if c["new_event_action"] == "created" else REVIEW_RESOLUTION)
                        for c in corrections]
            for target, reason in targets:
                remaining = evidence.open_review_count(conn, target)
                if remaining:
                    queued.append({"event_id": target, "queued": False,
                                   "reason": f"{remaining} other open review(s) still block it"})
                    continue
                store.request_job(conn, target, language, reason, now=now, reset_attempts=True)
                queued.append({"event_id": target, "queued": True, "reason": reason})
        decision_id = conn.execute(
            """
            INSERT INTO event_review_decision (review_id, decision, reviewer_id, explanation, resulting_status,
                                               membership_corrections, queued_jobs)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING decision_id
            """,
            (review_id, decision, reviewer, explanation, resulting, Jsonb(corrections), Jsonb(queued)),
        ).fetchone()["decision_id"]
    return {"review_id": review_id, "decision_id": str(decision_id), "decision": decision,
            "resulting_status": resulting, "membership_corrections": corrections, "queued_jobs": queued}


def _detach(conn, review: dict, event: dict) -> dict[str, Any]:
    """Detach the incoming revision from the compared event and give its segment its own event."""
    event_id, revision_id = str(review["candidate_event_id"]), str(review["revision_id"])
    membership = conn.execute("SELECT * FROM event_membership WHERE event_id = %s AND revision_id = %s FOR UPDATE",
                              (event_id, revision_id)).fetchone()
    if membership is None or membership["state"] != "active":
        raise ReviewError("the conflicting revision is not an active member of the event; nothing to correct")
    if membership["relation"] == "origin":
        raise ReviewError("the conflicting revision defines this event (origin); it cannot be detached")
    unit_ids = [str(r["unit_id"]) for r in conn.execute(
        "SELECT unit_id FROM event_membership_unit WHERE membership_id = %s ORDER BY unit_id",
        (membership["membership_id"],))]
    conn.execute("UPDATE event_membership SET state = 'detached' WHERE membership_id = %s",
                 (membership["membership_id"],))

    analysis_id, segment_key = str(membership["analysis_id"]), membership["segment_key"]
    own = conn.execute("SELECT event_id, status FROM news_event WHERE origin_analysis_id = %s "
                       "AND origin_segment_key = %s FOR UPDATE", (analysis_id, segment_key)).fetchone()
    if own is not None:
        new_event_id = str(own["event_id"])
        action = "reinstated" if own["status"] == "superseded" else "existing"
        if own["status"] == "superseded":
            conn.execute("UPDATE news_event SET status = 'provisional', superseded_by = NULL, "
                         "flags = flags || '[\"reinstated_by_review\"]'::jsonb, updated_at = now() "
                         "WHERE event_id = %s", (new_event_id,))
    else:
        action = "created"
        source = conn.execute(
            f"""
            SELECT d.record_type, r.title, {SOURCE_TIME} AS source_time, s.form, s.filing_date,
                   COALESCE(c.name, 'CIK ' || s.cik) AS issuer,
                   (SELECT t.location->>'section' FROM event_membership_unit mu JOIN text_unit t
                     ON t.unit_id = mu.unit_id WHERE mu.membership_id = %s ORDER BY t.ordinal LIMIT 1) AS section
              FROM document_revision r JOIN document d ON d.document_id = r.document_id
              LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
              LEFT JOIN company c ON c.company_id = s.issuer_company_id
             WHERE r.revision_id = %s
            """,
            (membership["membership_id"], revision_id),
        ).fetchone()
        if source["record_type"] == "sec_filing":
            title = (f"{source['issuer']} {source['form'] or '8-K'} {source['section'] or segment_key} "
                     f"(filed {source['filing_date']})")
        else:
            title = (source["title"] or "").strip() or "Untitled article"
        new_event_id = str(conn.execute(
            """
            INSERT INTO news_event (is_sample, profile_id, pipeline_version, title, flags, first_source_time,
                                    last_source_time, origin_revision_id, origin_analysis_id, origin_segment_key)
            VALUES (%s, %s, %s, %s, '["split_by_review"]'::jsonb, %s, %s, %s, %s, %s)
            RETURNING event_id
            """,
            (event["is_sample"], event["profile_id"], event["pipeline_version"], title[:500], source["source_time"],
             source["source_time"], revision_id, analysis_id, segment_key),
        ).fetchone()["event_id"])
        origin_id = conn.execute(
            """
            INSERT INTO event_membership (event_id, revision_id, document_id, analysis_id, segment_key, relation,
                                          state)
            VALUES (%s, %s, %s, %s, %s, 'origin', 'active') RETURNING membership_id
            """,
            (new_event_id, revision_id, membership["document_id"], analysis_id, segment_key),
        ).fetchone()["membership_id"]
        for unit_id in unit_ids:
            conn.execute("INSERT INTO event_membership_unit (membership_id, unit_id) VALUES (%s, %s)",
                         (origin_id, unit_id))
    refresh_event_stats(conn, [event_id, new_event_id])
    return {"action": "detached", "membership_id": str(membership["membership_id"]), "event_id": event_id,
            "revision_id": revision_id, "previous_state": "active", "relation": membership["relation"],
            "segment_key": segment_key, "unit_ids": unit_ids, "new_event_id": new_event_id,
            "new_event_action": action}


def show(conn, review_id: str) -> dict[str, Any]:
    """Everything a reviewer needs: both claims, the quoted evidence, and every source involved."""
    review = conn.execute(
        """
        SELECT v.*, c.candidate_event_id, c.revision_id, c.segment_key, c.label, c.method, c.matching_facts,
               c.differences, c.evidence, c.explanation, c.uncertainty, c.model, c.candidate_revision_ids,
               c.incoming_unit_ids, c.candidate_unit_ids, e.title AS event_title, e.status AS event_status,
               e.is_sample
          FROM event_conflict_review v
          JOIN event_comparison c ON c.comparison_id = v.comparison_id
          JOIN news_event e ON e.event_id = c.candidate_event_id
         WHERE v.review_id = %s
        """,
        (review_id,),
    ).fetchone()
    if review is None:
        raise ReviewError(f"review {review_id} not found")

    def sources(revision_ids: list) -> list[dict]:
        return conn.execute(
            f"""
            SELECT r.revision_id, d.record_type, r.title, r.original_url, COALESCE(pub.name, co.name) AS source,
                   {SOURCE_TIME} AS source_time, r.source_category
              FROM document_revision r JOIN document d ON d.document_id = r.document_id
              LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
              LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
              LEFT JOIN company co ON co.company_id = s.issuer_company_id
             WHERE r.revision_id = ANY(%s::uuid[]) ORDER BY {SOURCE_TIME}
            """,
            ([str(x) for x in revision_ids],),
        ).fetchall()

    def passages(unit_ids: list) -> list[dict]:
        return conn.execute("SELECT unit_id, revision_id, left(input_text, 1200) AS text FROM text_unit "
                            "WHERE unit_id = ANY(%s::uuid[]) ORDER BY revision_id, ordinal",
                            ([str(u) for u in unit_ids],)).fetchall()

    decisions = conn.execute("SELECT decision, reviewer_id, explanation, resulting_status, membership_corrections, "
                             "queued_jobs, created_at FROM event_review_decision WHERE review_id = %s "
                             "ORDER BY created_at", (review_id,)).fetchall()
    summary = conn.execute("SELECT version, headline, published_at FROM event_summary WHERE event_id = %s "
                           "AND status = 'published'", (review["candidate_event_id"],)).fetchall()
    return {
        "review": {k: review[k] for k in ("review_id", "status", "resolution", "reviewer_id", "reviewer_notes",
                                          "created_at", "resolved_at")},
        "event": {"event_id": review["candidate_event_id"], "title": review["event_title"],
                  "status": review["event_status"], "is_sample": review["is_sample"],
                  "published_summary": summary},
        "comparison": {k: review[k] for k in ("comparison_id", "label", "method", "segment_key", "matching_facts",
                                              "differences", "evidence", "explanation", "uncertainty", "model")},
        "conflicting_claims": [d for d in review["differences"] if d.get("kind") == "conflict"],
        "incoming_source": sources([review["revision_id"]]),
        "incoming_passages": passages(review["incoming_unit_ids"]),
        "event_sources_compared": sources(review["candidate_revision_ids"]),
        "event_passages_compared": passages(review["candidate_unit_ids"]),
        "decisions": decisions,
        "how_to_decide": ("uv run datasources-summary review decide --review-id <id> --reviewer <name> "
                          "--decision not_a_conflict|confirmed_conflict|different_events|insufficient_evidence "
                          "--explanation \"...\""),
    }
