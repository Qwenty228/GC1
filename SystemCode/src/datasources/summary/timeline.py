"""Read-only event timeline: the reports and updates of one event in the order they happened.

Built only from stored records; nothing here writes or calls a model, and no new table was needed:

    event_membership (active)   which revisions belong to the event and how grouping related them
    document_revision           each report's own times, and earlier / later revisions of the same source
    sec_filing_details          a filing's SEC acceptance time and filing date
    text_unit                   the sections of a filing that belong to THIS event (a filing can cover several)
    event_comparison            when a conflict was detected; the verbatim quotes behind an update or a conflict
    event_conflict_review       review status, through ``conflicts`` (reviewer names and notes never leave it)
    event_review_decision       when each review decision was made (the decision and its time only)
    event_summary               when each summary version was published

Entry ``kind`` comes from stored labels only:

    first_report          the event's origin membership: the report the event was created from
    additional_coverage   same_event_repeated: the same occurrence with nothing new found (``match`` is
                          ``identical_text`` when it was linked as an exact copy)
    substantive_update    same_event_update: grouping recorded new facts or a later development
    conflicting_report    same_event_conflict: a comparable claim differs from the event's earlier sources
    conflict_detected     when that conflict was detected (the comparison's time, not the report's)
    review_decision       a conflict review decision (its outcome and time)
    summary_published, summary_revised   a summary version and its publication time

A later revision of a report already in the event (``revised_report``) keeps the label of its own relation: a
source that only changed its timestamp is additional coverage, never an update.

Times are the source's own. A news report is placed at its publication time, and a later revision of it at the
source's update time; a filing at its SEC acceptance time, else its filing date (``date``, date only: no time of day
is invented). When the source gave no usable time, ``time`` is null and ``time_basis`` is ``missing``; the time the
record was collected is reported separately as ``retrieved_at`` and is never used as a publication time. Undated
entries come last. Entries are keyed by stored row ids (one per membership, comparison, decision and summary
version), so joins, text chunks or repeated delivery cannot duplicate them.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any

from psycopg import Connection

from . import conflicts

KINDS = {"origin": "first_report", "same_event_repeated": "additional_coverage",
         "same_event_update": "substantive_update", "same_event_conflict": "conflicting_report"}
_ORDER = {"first_report": 0, "additional_coverage": 1, "substantive_update": 1, "conflicting_report": 1,
          "conflict_detected": 2, "review_decision": 3, "summary_published": 4, "summary_revised": 4}
_UPDATE_KINDS = ("new_information", "temporal_development")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _iso_date(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, date) else None


def report_time(row: dict[str, Any], *, later_revision: bool) -> tuple[datetime | None, str, str | None]:
    """(time, basis, date-only) for one report from its own stored times; retrieval time is never used."""
    if row["record_type"] == "sec_filing":
        accepted = row["published_at"] or row["accepted_at"]
        if accepted:
            return accepted, "sec_accepted", None
        if row["filing_date"]:
            return None, "sec_filing_date", _iso_date(row["filing_date"])
        return None, "missing", None
    if later_revision:
        updated = row["source_updated_at"]
        return (updated, "source_updated", None) if updated else (None, "missing", None)
    if row["published_at"]:
        return row["published_at"], "published", None
    if row["source_updated_at"]:
        return row["source_updated_at"], "source_updated", None
    return None, "missing", None


def _quotes(evidence: Any) -> list[str]:
    """The compared report's own verbatim quotes (incoming side), exactly as stored."""
    return conflicts._quotes(evidence, "incoming")


def _aspects(differences: Any, kinds: tuple[str, ...]) -> list[str]:
    """Stored difference aspects of the given kinds; malformed entries are skipped, never repaired."""
    found = []
    for item in differences if isinstance(differences, list) else []:
        if not isinstance(item, dict) or item.get("kind") not in kinds:
            continue
        aspect = item.get("aspect")
        if isinstance(aspect, str) and aspect.strip() and aspect.strip() not in found:
            found.append(aspect.strip())
    return found


def _sections(conn: Connection, members: list[dict[str, Any]], profile_id: str) -> dict[tuple[str, str], list]:
    """{(revision_id, segment_key): [section headings]} for filings: only the units of the event's own segment."""
    revisions = sorted({str(m["revision_id"]) for m in members if m["record_type"] == "sec_filing"})
    if not revisions:
        return {}
    found: dict[tuple[str, str], list[str]] = {}
    for row in conn.execute(
        """
        SELECT t.revision_id::text AS revision_id, t.location->>'segment_key' AS segment_key,
               t.location->>'section' AS section, t.location->>'item_code' AS item_code, min(t.ordinal) AS first
          FROM text_unit t
         WHERE t.revision_id = ANY(%s::uuid[]) AND t.profile_id = %s
         GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 5
        """,
        (revisions, profile_id),
    ):
        section = (row["section"] or "").strip()
        if row["segment_key"] and section and section not in ("document", "article"):
            found.setdefault((row["revision_id"], row["segment_key"]), []).append(section)
    return found


def event_timeline(conn: Connection, event_id: str, *, include_sample: bool, language: str) -> dict[str, Any] | None:
    event = conn.execute(
        "SELECT event_id::text AS event_id, title, status, is_sample, superseded_by::text AS superseded_by, "
        "profile_id::text AS profile_id FROM news_event WHERE event_id = %s", (event_id,)).fetchone()
    if event is None or (event["is_sample"] and not include_sample):
        return None

    members = conn.execute(
        """
        SELECT m.membership_id::text AS membership_id, m.revision_id::text AS revision_id,
               m.document_id::text AS document_id, m.relation, m.segment_key,
               d.record_type, (d.current_revision_id = m.revision_id) AS is_current,
               r.title, r.original_url, r.published_at, r.source_updated_at, r.retrieved_at,
               COALESCE(pub.name, co.name) AS source_name, s.form, s.accession_number, s.filing_date, s.accepted_at,
               s.primary_document_url, s.filing_index_url,
               (SELECT count(*) FROM document_revision x WHERE x.document_id = m.document_id) AS revisions_total,
               (SELECT count(*) FROM document_revision x WHERE x.document_id = m.document_id
                  AND (x.retrieved_at, x.revision_id) <= (r.retrieved_at, r.revision_id)) AS revision_number,
               c.method, c.differences, c.evidence
          FROM event_membership m
          JOIN document d ON d.document_id = m.document_id
          JOIN document_revision r ON r.revision_id = m.revision_id
          LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
          LEFT JOIN sec_filing_details s ON s.revision_id = m.revision_id
          LEFT JOIN company co ON co.company_id = s.issuer_company_id
          LEFT JOIN event_comparison c ON c.comparison_id = m.comparison_id
         WHERE m.event_id = %s AND m.state = 'active'
        """,
        (event_id,),
    ).fetchall()
    excluded = conn.execute(
        "SELECT count(*) FILTER (WHERE state = 'pending') AS pending, count(*) FILTER (WHERE state = 'detached') "
        "AS detached FROM event_membership WHERE event_id = %s", (event_id,)).fetchone()

    published = conn.execute(
        "SELECT s.published_at, s.input_manifest, j.status AS job_status FROM news_event e "
        "LEFT JOIN event_summary s ON s.event_id = e.event_id AND s.status = 'published' AND s.language_code = %s "
        "LEFT JOIN event_summary_job j ON j.event_id = e.event_id AND j.language_code = %s WHERE e.event_id = %s",
        (language, language, event_id)).fetchone()
    conflict = conflicts.event_conflict(conn, event_id, summary_published_at=published["published_at"],
                                        manifest=published["input_manifest"] if published["published_at"] else None,
                                        job_status=published["job_status"])
    disputes = {d["review_id"]: d for d in conflict["disputes"]}

    entries: list[dict[str, Any]] = []
    sections = _sections(conn, members, event["profile_id"])
    by_document: dict[str, list[dict]] = {}
    for m in members:
        by_document.setdefault(m["document_id"], []).append(m)
    member_revisions = {m["revision_id"] for m in members}
    for rows in by_document.values():
        rows.sort(key=lambda r: int(r["revision_number"]))
        current_elsewhere = not any(r["is_current"] for r in rows)
        for position, m in enumerate(rows):
            later = position > 0
            when, basis, day = report_time(m, later_revision=later)
            kind = KINDS.get(m["relation"], "additional_coverage")
            entry: dict[str, Any] = {
                "id": f"report:{m['revision_id']}", "kind": kind, "relation": m["relation"],
                "time": when, "time_basis": basis, "date": day, "revised_report": later,
                "match": "identical_text" if m["method"] == "exact_content" else None,
                "source": {
                    "revision_id": m["revision_id"], "document_id": m["document_id"], "record_type": m["record_type"],
                    "title": m["title"], "name": m["source_name"],
                    "url": conflicts.safe_url(m["original_url"] or m["primary_document_url"]),
                    "filing_index_url": conflicts.safe_url(m["filing_index_url"]), "form": m["form"],
                    "accession_number": m["accession_number"], "published_at": m["published_at"],
                    "source_updated_at": m["source_updated_at"], "filing_date": _iso_date(m["filing_date"]),
                    "retrieved_at": m["retrieved_at"], "revision_number": int(m["revision_number"]),
                    "revisions_total": int(m["revisions_total"]), "is_current_revision": bool(m["is_current"]),
                    "current_revision_in_event": not current_elsewhere,
                },
                "sections": sections.get((m["revision_id"], m["segment_key"]), []),
                "aspects": [], "quotes": [], "review": None,
            }
            if kind == "substantive_update":
                entry["aspects"] = _aspects(m["differences"], _UPDATE_KINDS)
                entry["quotes"] = _quotes(m["evidence"])
            elif kind == "conflicting_report":
                entry["aspects"] = _aspects(m["differences"], ("conflict",))
                entry["quotes"] = _quotes(m["evidence"])
            entries.append(entry)

    # Conflicts: when each was detected, and every decision on its review.
    detections = conn.execute(
        """
        SELECT c.comparison_id::text AS comparison_id, c.revision_id::text AS revision_id, c.created_at,
               v.review_id::text AS review_id, v.status AS review_status
          FROM event_comparison c LEFT JOIN event_conflict_review v ON v.comparison_id = c.comparison_id
         WHERE c.candidate_event_id = %s AND c.label = 'same_event_conflict'
        """,
        (event_id,),
    ).fetchall()
    reports = conflicts._sources(conn, {d["revision_id"] for d in detections})
    review_of_revision: dict[str, dict] = {}
    for d in detections:
        dispute = disputes.get(d["review_id"]) if d["review_id"] else None
        review = None
        if dispute is not None:
            review = {"review_id": d["review_id"], "review_status": d["review_status"],
                      "display_status": dispute["display_status"], "topic": dispute["topic"],
                      "precision": dispute["precision"]}
            review_of_revision[d["revision_id"]] = review
        report = reports.get(d["revision_id"]) or {}
        entries.append({"id": f"conflict:{d['comparison_id']}", "kind": "conflict_detected", "time": d["created_at"],
                        "time_basis": "detected", "date": None, "review": review,
                        "report": {"revision_id": d["revision_id"], "title": report.get("title"),
                                   "name": report.get("name"), "url": report.get("url"),
                                   "in_event": d["revision_id"] in member_revisions}})
    for entry in entries:
        if entry["kind"] == "conflicting_report":
            entry["review"] = review_of_revision.get(entry["source"]["revision_id"])

    revision_of_review = {d["review_id"]: d["revision_id"] for d in detections if d["review_id"]}
    for number, row in enumerate(conn.execute(
        """
        SELECT d.review_id::text AS review_id, d.decision, d.resulting_status, d.created_at
          FROM event_review_decision d
          JOIN event_conflict_review v ON v.review_id = d.review_id
          JOIN event_comparison c ON c.comparison_id = v.comparison_id
         WHERE c.candidate_event_id = %s ORDER BY d.created_at, d.decision_id
        """,
        (event_id,),
    )):
        outcome = (disputes.get(row["review_id"]) or {}).get("outcome") or {}
        entries.append({"id": f"decision:{row['review_id']}:{number}", "kind": "review_decision",
                        "time": row["created_at"], "time_basis": "decided", "date": None,
                        "decision": row["decision"], "final": row["resulting_status"] == "resolved",
                        "review_id": row["review_id"], "report_revision_id": revision_of_review.get(row["review_id"]),
                        "moved_to_event_id": outcome.get("moved_to_event_id")
                        if row["decision"] == "different_events" else None})

    versions = conn.execute(
        "SELECT version, status, generation_reason, headline, published_at, input_manifest FROM event_summary "
        "WHERE event_id = %s AND language_code = %s AND status IN ('published', 'superseded') ORDER BY version",
        (event_id, language)).fetchall()
    for index, v in enumerate(versions):
        entries.append({"id": f"summary:{v['version']}", "kind": "summary_published" if index == 0 else
                        "summary_revised", "time": v["published_at"], "time_basis": "summary_published",
                        "date": None, "version": v["version"], "generation_reason": v["generation_reason"],
                        "headline": v["headline"], "current": v["status"] == "published",
                        "sources": len(set((v["input_manifest"] or {}).get("member_revision_ids") or []))})

    entries.sort(key=_sort_key)
    reports_dated = [e for e in entries if e["kind"] in KINDS.values() and _sort_time(e) is not None]
    for e in entries:
        if e["kind"] == "first_report":
            # The event was created from this report, but a report collected later may carry an earlier time.
            mine = _sort_time(e)
            e["earlier_report_exists"] = mine is not None and any(_sort_time(o) < mine for o in reports_dated)
    detail_state = conn.execute(
        "SELECT (SELECT count(*) FROM event_conflict_review v JOIN event_comparison c ON c.comparison_id = "
        "v.comparison_id WHERE c.candidate_event_id = %s AND v.status IN ('open', 'in_review')) AS open_reviews",
        (event_id,)).fetchone()
    return {
        "event_id": event["event_id"], "title": event["title"], "event_status": event["status"],
        "is_sample": event["is_sample"], "superseded_by": event["superseded_by"],
        "conflict_status": conflict["status"],
        "under_review": int(detail_state["open_reviews"]) > 0,
        "entries": entries,
        "counts": {"reports": len(by_document), "report_versions": len(members),
                   "undated": sum(_sort_time(e) is None for e in entries)},
        "excluded": {"pending_relations": int(excluded["pending"]), "detached_reports": int(excluded["detached"])},
    }


def _sort_time(entry: dict[str, Any]) -> datetime | None:
    if entry["time"] is not None:
        return entry["time"]
    if entry.get("date"):
        return datetime.combine(date.fromisoformat(entry["date"]), time(0), tzinfo=timezone.utc)
    return None


def _sort_key(entry: dict[str, Any]) -> tuple:
    when = _sort_time(entry)
    return (when is None, when or _EPOCH, _ORDER[entry["kind"]], entry["id"])

