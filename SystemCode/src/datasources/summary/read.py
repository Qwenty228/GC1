"""Read-only queries behind the event summary endpoints. Nothing here writes or calls a model.

``summary_state`` values
    published        a summary is published (``under_review`` and ``update_pending`` may also be true)
    awaiting_review  no summary yet, and a conflict review blocks the first one
    pending          summary work is queued or running
    failed           summary generation gave up (see ``datasources-summary show``)
    not_available    the event has nothing summarizable
    not_requested    no summary work exists yet (e.g. summarization is disabled)

``conflict`` (see ``conflicts.py``) is built from the stored review and comparison records: a compact
status and count in the list, and every dispute with its claims, verbatim quotes and sources in the detail.
"""

from __future__ import annotations

from typing import Any

from psycopg import Connection

from ..similarity.store import SOURCE_TIME
from . import conflicts

_EVENT_COLUMNS = f"""
    e.event_id, e.title, e.status, e.is_sample, e.first_source_time, e.last_source_time, e.superseded_by,
    s.summary_id, s.version, s.headline, s.summary_text, s.key_points, s.statements, s.limitations,
    s.input_manifest, s.generation_reason, s.published_at, s.model_name,
    j.status AS job_status, j.reason AS job_reason, j.next_retry_at AS job_next_retry_at, j.attempts AS job_attempts,
    (SELECT count(*) FROM event_conflict_review v JOIN event_comparison c ON c.comparison_id = v.comparison_id
      WHERE c.candidate_event_id = e.event_id AND v.status IN ('open', 'in_review')) AS open_reviews,
    (SELECT count(DISTINCT m.document_id) FROM current_event_membership m WHERE m.event_id = e.event_id) AS documents,
    (SELECT count(*) FROM event_membership p WHERE p.event_id = e.event_id AND p.state = 'pending')
        AS pending_relations
"""
_EVENT_FROM = """
  FROM news_event e
  LEFT JOIN event_summary s ON s.event_id = e.event_id AND s.status = 'published' AND s.language_code = %(lang)s
  LEFT JOIN event_summary_job j ON j.event_id = e.event_id AND j.language_code = %(lang)s
"""


SUMMARY_STATES = ("published", "awaiting_review", "pending", "failed", "not_available", "not_requested")
# The same precedence as the CASE below: keep the two in step.
_STATE_SQL = """
    CASE WHEN x.summary_id IS NOT NULL THEN 'published'
         WHEN x.open_reviews > 0 THEN 'awaiting_review'
         WHEN x.job_status IN ('pending', 'running') THEN 'pending'
         WHEN x.job_status = 'failed' THEN 'failed'
         WHEN x.job_status = 'completed' THEN 'not_available'
         ELSE 'not_requested' END
"""


def summary_state(row: dict[str, Any]) -> dict[str, Any]:
    job = row.get("job_status")
    open_reviews = int(row.get("open_reviews") or 0)
    if row.get("summary_id"):
        state = "published"
    elif open_reviews:
        state = "awaiting_review"
    elif job in ("pending", "running"):
        state = "pending"
    elif job == "failed":
        state = "failed"
    elif job == "completed":
        state = "not_available"
    else:
        state = "not_requested"
    return {"summary_state": state, "under_review": open_reviews > 0, "open_reviews": open_reviews,
            "update_pending": bool(row.get("summary_id")) and job in ("pending", "running", "awaiting_review"),
            "retry_after": row.get("job_next_retry_at") if job == "pending" else None}


def _in_citation_order(rows: list[dict], cited: list[dict]) -> list[dict]:
    """Units in the order the statement cites them (its stored ``statements`` entry), each unit's quotes by
    quote_index. Every stored quote is returned separately; none are merged."""
    rank: dict[str, int] = {}
    for position, citation in enumerate(cited):
        rank.setdefault(str(citation.get("unit_id")), position)
    return sorted(rows, key=lambda c: (rank.get(c["unit_id"], len(rank)), c["unit_id"], c["quote_index"]))


def list_events(conn: Connection, *, include_sample: bool, language: str, limit: int, offset: int,
                state: str | None = None, symbol: str | None = None) -> list[dict[str, Any]]:
    """``symbol`` keeps events with at least one current source linked to that company symbol."""
    rows = conn.execute(
        f"""
        SELECT * FROM (
            SELECT {_EVENT_COLUMNS} {_EVENT_FROM}
             WHERE e.status <> 'superseded' AND (%(sample)s OR NOT e.is_sample)
               AND EXISTS (SELECT 1 FROM current_event_membership m WHERE m.event_id = e.event_id)
               AND (%(symbol)s::text IS NULL OR EXISTS (
                    SELECT 1 FROM current_event_membership m
                      JOIN revision_company rc ON rc.revision_id = m.revision_id
                      JOIN company_symbol cs ON cs.company_id = rc.company_id
                     WHERE m.event_id = e.event_id AND cs.symbol = %(symbol)s::text))) x
         WHERE %(state)s::text IS NULL OR {_STATE_SQL} = %(state)s::text
         ORDER BY x.last_source_time DESC NULLS LAST, x.event_id
         LIMIT %(limit)s OFFSET %(offset)s
        """,
        {"lang": language, "sample": include_sample, "limit": limit, "offset": offset, "state": state,
         "symbol": symbol},
    ).fetchall()
    conflict_status = conflicts.list_status(conn, [{**r, "input_manifest": r["input_manifest"] if r["summary_id"]
                                                     else None} for r in rows])
    events = []
    for row in rows:
        item = {"event_id": str(row["event_id"]), "title": row["title"], "event_status": row["status"],
                "is_sample": row["is_sample"], "first_source_time": row["first_source_time"],
                "last_source_time": row["last_source_time"], "documents": int(row["documents"]),
                "pending_relations": int(row["pending_relations"]), **summary_state(row),
                "conflict": conflict_status[str(row["event_id"])], "summary": None}
        if row["summary_id"]:
            item["summary"] = {"version": row["version"], "headline": row["headline"],
                               "overview": row["summary_text"], "key_points": row["key_points"],
                               "published_at": row["published_at"]}
        events.append(item)
    return events


def event_detail(conn: Connection, event_id: str, *, include_sample: bool, language: str) -> dict[str, Any] | None:
    row = conn.execute(f"SELECT {_EVENT_COLUMNS} {_EVENT_FROM} WHERE e.event_id = %(event)s",
                       {"lang": language, "event": event_id}).fetchone()
    if row is None or (row["is_sample"] and not include_sample):
        return None
    detail: dict[str, Any] = {
        "event_id": str(row["event_id"]), "title": row["title"], "event_status": row["status"],
        "is_sample": row["is_sample"], "superseded_by": str(row["superseded_by"]) if row["superseded_by"] else None,
        "first_source_time": row["first_source_time"], "last_source_time": row["last_source_time"],
        "documents": int(row["documents"]), "pending_relations": int(row["pending_relations"]),
        "language": language, **summary_state(row), "summary": None,
    }
    manifest = row["input_manifest"] or {}
    cited_revisions: set[str] = set()
    if row["summary_id"]:
        citations = conn.execute(
            f"""
            SELECT c.statement_key, c.unit_id, c.quote_index, c.evidence_quote, t.revision_id, r.title,
                   r.original_url, d.record_type, COALESCE(pub.name, co.name) AS source_name,
                   {SOURCE_TIME} AS source_time, s.primary_document_url
              FROM event_summary_citation c
              JOIN text_unit t ON t.unit_id = c.unit_id
              JOIN document_revision r ON r.revision_id = t.revision_id
              JOIN document d ON d.document_id = r.document_id
              LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
              LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
              LEFT JOIN company co ON co.company_id = s.issuer_company_id
             WHERE c.summary_id = %s ORDER BY c.statement_key, c.unit_id, c.quote_index
            """,
            (row["summary_id"],),
        ).fetchall()
        by_statement: dict[str, list[dict]] = {}
        for c in citations:
            cited_revisions.add(str(c["revision_id"]))
            by_statement.setdefault(c["statement_key"], []).append({
                "quote": c["evidence_quote"], "quote_index": c["quote_index"], "unit_id": str(c["unit_id"]),
                "source": {"revision_id": str(c["revision_id"]), "title": c["title"],
                           "url": c["original_url"] or c["primary_document_url"], "record_type": c["record_type"],
                           "name": c["source_name"], "published_at": c["source_time"]}})
        statements = [{"id": s["id"], "kind": s["kind"], "text": s["text"],
                       "citations": _in_citation_order(by_statement.get(s["id"], []), s.get("citations") or [])}
                      for s in row["statements"] or []]
        detail["summary"] = {
            "summary_id": str(row["summary_id"]), "version": row["version"], "headline": row["headline"],
            "overview": row["summary_text"], "key_points": [s for s in statements if s["kind"] == "key_point"],
            "statements": statements, "published_at": row["published_at"],
            "generation_reason": row["generation_reason"], "limitations": row["limitations"] or [],
            "model": row["model_name"],
            "note": "Machine-generated from the linked sources; each statement cites its evidence. SEC filings are "
                    "company disclosures, not statements verified by the SEC.",
        }
    input_revisions = set(manifest.get("revision_ids") or [])
    summarized_members = set(manifest.get("member_revision_ids") or [])
    sources = conn.execute(
        f"""
        SELECT m.revision_id, m.document_id, m.relation, d.record_type, r.title, r.original_url, r.source_category,
               COALESCE(pub.name, co.name) AS source_name, {SOURCE_TIME} AS source_time, s.form, s.accession_number,
               s.primary_document_url, s.filing_index_url
          FROM current_event_membership m
          JOIN document d ON d.document_id = m.document_id
          JOIN document_revision r ON r.revision_id = m.revision_id
          LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
          LEFT JOIN sec_filing_details s ON s.revision_id = m.revision_id
          LEFT JOIN company co ON co.company_id = s.issuer_company_id
         WHERE m.event_id = %s
         ORDER BY {SOURCE_TIME} NULLS LAST, m.revision_id
        """,
        (event_id,),
    ).fetchall()
    detail["sources"] = [{
        "revision_id": str(s["revision_id"]), "document_id": str(s["document_id"]), "relation": s["relation"],
        "record_type": s["record_type"], "title": s["title"], "url": s["original_url"] or s["primary_document_url"],
        "filing_index_url": s["filing_index_url"], "source_category": s["source_category"], "name": s["source_name"],
        "published_at": s["source_time"], "form": s["form"], "accession_number": s["accession_number"],
        "cited_in_current_summary": str(s["revision_id"]) in cited_revisions,
        "in_current_summary_input": str(s["revision_id"]) in input_revisions,
        "added_after_current_summary": bool(row["summary_id"]) and str(s["revision_id"]) not in summarized_members,
    } for s in sources]
    detail["reviews"] = [{**r, "review_id": str(r["review_id"])} for r in conn.execute(
        """
        SELECT v.review_id, v.status, v.resolution, v.created_at, v.resolved_at, c.revision_id::text AS revision_id
          FROM event_conflict_review v JOIN event_comparison c ON c.comparison_id = v.comparison_id
         WHERE c.candidate_event_id = %s ORDER BY v.created_at
        """,
        (event_id,),
    )]
    detail["versions"] = conn.execute(
        "SELECT version, status, generation_reason, headline, published_at FROM event_summary "
        "WHERE event_id = %s AND language_code = %s ORDER BY version DESC", (event_id, language)).fetchall()
    detail["conflict"] = conflicts.event_conflict(
        conn, event_id, summary_published_at=row["published_at"] if row["summary_id"] else None,
        manifest=manifest if row["summary_id"] else None, job_status=row["job_status"])
    return detail
