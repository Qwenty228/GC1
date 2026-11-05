"""Read-only conflict notices for the event endpoints, built only from stored review records.

Nothing here writes or calls a model. A notice is assembled from
``event_conflict_review`` (status and resolution), ``event_comparison``
(``differences`` and the verbatim ``evidence`` quotes the comparison validator
verified), ``event_membership`` (for membership corrections) and each source's
own metadata. Raw prompts, model explanations, reviewer names, reviewer notes
and decision explanations are never returned.

Stored comparison shapes (see ``similarity.comparison._validate_pair``)
    differences  [{"aspect", "incoming", "candidate", "kind"}]; kind "conflict" marks an incompatible claim.
                 "incoming" is what the compared report says, "candidate" what the event's earlier sources say.
    evidence     {"incoming": [{"ref": "U#", "quote"}], "candidate": [{"ref": "C#", "quote"}]}. The refs are
                 prompt-local: U# are units of the compared report itself, so its quotes belong to that report;
                 C# are passages of the event's sources, which are located here by finding the quote in the text
                 of ``candidate_unit_ids`` (the same normalization the validator used). A quote that cannot be
                 located is returned without a source rather than guessed.

A dispute has a precise claim pair only when a well-formed ``conflict`` difference exists AND both sides have at
least one quote attributed to a source. Otherwise the available quotes are returned with a generic notice.

Display status per review (``display_status``)
    under_review        open, or in_review (an ``insufficient_evidence`` decision keeps it there)
    disputed            resolved as confirmed_conflict: the sources disagree; the review does not say who is right
    correction_pending  resolved as different_events, but the report is still an active member of the event, or
                        the published summary was built from it: the membership correction is not complete
    separated           resolved as different_events and the correction is complete (history only)
    cleared             resolved as not_a_conflict (history only)

The event's status is the most urgent of its reviews, so closing one review never hides another:
under_review > correction_pending > disputed > resolved (only cleared/separated reviews) > none.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

from psycopg import Connection

from ..similarity.comparison import _fold as fold_quote
from ..similarity.store import SOURCE_TIME

ACTIVE = ("under_review", "correction_pending", "disputed")
_PRECEDENCE = ("under_review", "correction_pending", "disputed")
_UNSTATED = frozenset({"", "unknown", "n/a", "na", "none", "not stated", "not mentioned", "not specified"})
MAX_URL_CHARS = 2048
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
GENERIC_NOTICE = ("These reports may conflict. The stored comparison does not identify the exact claims that "
                  "differ, so compare the quoted passages and the original sources.")


def safe_url(url: Any) -> str | None:
    """An absolute http(s) URL fit for a link, or None. Anything else (javascript:, data:, relative, spaces,
    control or quote characters) is dropped rather than repaired."""
    if not isinstance(url, str):
        return None
    url = url.strip()
    if not url or len(url) > MAX_URL_CHARS or any(ch.isspace() or ord(ch) < 32 or ch in "<>\"'`\\" for ch in url):
        return None
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
        return None
    return url


def display_status(review: dict[str, Any], summary_revisions: set[str] | None) -> str:
    status, resolution = review.get("status"), review.get("resolution")
    if status in ("open", "in_review"):
        return "under_review"
    if resolution == "confirmed_conflict":
        return "disputed"
    if resolution == "not_a_conflict":
        return "cleared"
    if resolution == "different_events":
        still_member = review.get("membership_state") in ("active", "pending")
        in_summary = str(review.get("revision_id")) in (summary_revisions or set())
        return "correction_pending" if still_member or in_summary else "separated"
    return "under_review"  # an unknown state is never shown as resolved


# The SQL twin of ``display_status(...) in ACTIVE`` for review ``v`` of comparison ``c``, used to filter the event
# list ("sources disagree") before paging. ``{manifest}`` is the published summary's input_manifest (NULL when
# there is none). Keep the two in step: tests/test_feed_filters.py checks them against each other.
ACTIVE_REVIEW_SQL = """
    (v.status IN ('open', 'in_review')
     OR v.resolution IS NULL OR v.resolution NOT IN ('not_a_conflict', 'different_events')
     OR (v.resolution = 'different_events' AND (
           EXISTS (SELECT 1 FROM event_membership mm WHERE mm.event_id = c.candidate_event_id
                    AND mm.revision_id = c.revision_id AND mm.state IN ('active', 'pending'))
           OR COALESCE(({manifest}) -> 'member_revision_ids' @> to_jsonb(c.revision_id::text), false))))
"""


def event_status(statuses: list[str]) -> str:
    for status in _PRECEDENCE:
        if status in statuses:
            return status
    return "resolved" if statuses else "none"


def compact(statuses: list[str]) -> dict[str, Any]:
    """The list view's conflict field: overall status, disputes still needing a notice, and all reviews."""
    return {"status": event_status(statuses), "count": sum(s in ACTIVE for s in statuses), "reviews": len(statuses)}


def _summary_revisions(manifest: dict[str, Any] | None) -> set[str] | None:
    if not manifest:
        return None
    return {str(r) for r in manifest.get("member_revision_ids") or []}


# ----------------------------------------------------------------------------- list view
def list_status(conn: Connection, events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """{event_id: compact()} for a page of events (``events`` rows carry event_id and input_manifest)."""
    ids = [str(e["event_id"]) for e in events]
    if not ids:
        return {}
    statuses: dict[str, list[str]] = {i: [] for i in ids}
    manifests = {str(e["event_id"]): _summary_revisions(e.get("input_manifest")) for e in events}
    for row in conn.execute(
        """
        SELECT c.candidate_event_id::text AS event_id, v.status, v.resolution, c.revision_id::text AS revision_id,
               (SELECT m.state FROM event_membership m
                 WHERE m.event_id = c.candidate_event_id AND m.revision_id = c.revision_id) AS membership_state
          FROM event_conflict_review v JOIN event_comparison c ON c.comparison_id = v.comparison_id
         WHERE c.candidate_event_id = ANY(%s::uuid[])
        """,
        (ids,),
    ):
        statuses[row["event_id"]].append(display_status(row, manifests.get(row["event_id"])))
    return {i: compact(s) for i, s in statuses.items()}


# ----------------------------------------------------------------------------- detail view
def _review_rows(conn: Connection, event_id: str) -> list[dict[str, Any]]:
    return conn.execute(
        """
        SELECT v.review_id::text AS review_id, v.status, v.resolution, v.created_at AS review_opened_at,
               v.resolved_at, c.created_at AS detected_at, c.revision_id::text AS revision_id, c.differences,
               c.evidence, c.incoming_unit_ids, c.candidate_unit_ids, c.candidate_revision_ids,
               (SELECT m.state FROM event_membership m
                 WHERE m.event_id = c.candidate_event_id AND m.revision_id = c.revision_id) AS membership_state,
               (SELECT d.membership_corrections FROM event_review_decision d
                 WHERE d.review_id = v.review_id AND d.decision = 'different_events'
                 ORDER BY d.created_at DESC LIMIT 1) AS corrections,
               (SELECT jsonb_build_object('decision', d.decision, 'decided_at', d.created_at)
                  FROM event_review_decision d WHERE d.review_id = v.review_id
                 ORDER BY d.created_at DESC LIMIT 1) AS last_decision
          FROM event_conflict_review v JOIN event_comparison c ON c.comparison_id = v.comparison_id
         WHERE c.candidate_event_id = %s
         ORDER BY c.created_at, v.review_id
        """,
        (event_id,),
    ).fetchall()


def _sources(conn: Connection, revision_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not revision_ids:
        return {}
    rows = conn.execute(
        f"""
        SELECT r.revision_id::text AS revision_id, d.record_type, r.title, r.original_url, s.primary_document_url,
               COALESCE(pub.name, co.name) AS source_name, {SOURCE_TIME} AS source_time
          FROM document_revision r JOIN document d ON d.document_id = r.document_id
          LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
          LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
          LEFT JOIN company co ON co.company_id = s.issuer_company_id
         WHERE r.revision_id = ANY(%s::uuid[])
        """,
        (sorted(revision_ids),),
    ).fetchall()
    return {r["revision_id"]: {"revision_id": r["revision_id"], "name": r["source_name"], "title": r["title"],
                               "published_at": r["source_time"], "record_type": r["record_type"],
                               "url": safe_url(r["original_url"] or r["primary_document_url"])} for r in rows}


def _unit_texts(conn: Connection, unit_ids: set[str]) -> dict[str, dict[str, str]]:
    if not unit_ids:
        return {}
    rows = conn.execute("SELECT unit_id::text AS unit_id, revision_id::text AS revision_id, input_text "
                        "FROM text_unit WHERE unit_id = ANY(%s::uuid[])", (sorted(unit_ids),)).fetchall()
    return {r["unit_id"]: {"revision_id": r["revision_id"], "text": r["input_text"] or ""} for r in rows}


def _quotes(evidence: Any, side: str) -> list[str]:
    """The quotes stored for one side; malformed entries are skipped, never repaired."""
    items = evidence.get(side) if isinstance(evidence, dict) else None
    quotes: list[str] = []
    for item in items if isinstance(items, list) else []:
        quote = item.get("quote") if isinstance(item, dict) else None
        if isinstance(quote, str) and quote.strip() and quote.strip() not in quotes:
            quotes.append(quote.strip())
    return quotes


def _stated(value: Any) -> bool:
    return isinstance(value, str) and value.strip().casefold() not in _UNSTATED


def conflict_claims(differences: Any) -> list[dict[str, str]]:
    """Well-formed ``conflict`` differences only: aspect, incoming and candidate all stated."""
    claims = []
    for item in differences if isinstance(differences, list) else []:
        if (isinstance(item, dict) and item.get("kind") == "conflict"
                and all(_stated(item.get(k)) for k in ("aspect", "incoming", "candidate"))):
            claims.append({"aspect": item["aspect"].strip(), "incoming": item["incoming"].strip(),
                           "candidate": item["candidate"].strip()})
    return claims


def build_dispute(review: dict[str, Any], *, sources: dict[str, dict[str, Any]], units: dict[str, dict[str, str]],
                  summary_published_at: datetime | None, summary_revisions: set[str] | None) -> dict[str, Any]:
    """One review as a reader-facing dispute. Pure: everything it needs is passed in."""
    status = display_status(review, summary_revisions)
    incoming_revision = str(review["revision_id"])
    candidate_units = [str(u) for u in review.get("candidate_unit_ids") or []]

    by_revision: dict[str, dict[str, Any]] = {}

    def source(revision_id: str, side: str) -> dict[str, Any]:
        key = f"{side}:{revision_id}"
        if key not in by_revision:
            meta = sources.get(revision_id) or {"revision_id": revision_id, "name": None, "title": None,
                                                "published_at": None, "record_type": None, "url": None}
            by_revision[key] = {**meta, "side": side, "quotes": [], "claims": []}
        return by_revision[key]

    # The compared report's own quotes (U# refs are units of that report).
    incoming = source(incoming_revision, "incoming")
    incoming["quotes"] = _quotes(review.get("evidence"), "incoming")
    # The event's quotes: attributed to every candidate source whose unit text contains the quote.
    unattributed: list[str] = []
    for quote in _quotes(review.get("evidence"), "candidate"):
        folded = fold_quote(quote)
        found = sorted({units[u]["revision_id"] for u in candidate_units
                        if u in units and folded and folded in fold_quote(units[u]["text"])})
        for revision_id in found:
            source(revision_id, "candidate")["quotes"].append(quote)
        if not found:
            unattributed.append(quote)

    candidates = [s for k, s in by_revision.items() if k.startswith("candidate:")]
    claims = conflict_claims(review.get("differences"))
    precise = bool(claims) and bool(incoming["quotes"]) and bool(candidates)
    if precise:
        incoming["claims"] = [{"aspect": c["aspect"], "value": c["incoming"]} for c in claims]
        for item in candidates:
            item["claims"] = [{"aspect": c["aspect"], "value": c["candidate"]} for c in claims]
    else:
        # Still give readers every source that was compared, with whatever quotes exist.
        for revision_id in (str(r) for r in review.get("candidate_revision_ids") or []):
            if revision_id != incoming_revision:
                source(revision_id, "candidate")
        candidates = [s for k, s in by_revision.items() if k.startswith("candidate:")]

    detected_at = review["detected_at"]
    outcome = None
    if review["status"] == "resolved":
        outcome = {"decision": review["resolution"], "decided_at": review["resolved_at"], "final": True}
        if review["resolution"] == "different_events":
            moved = [c.get("new_event_id") for c in review.get("corrections") or []
                     if isinstance(c, dict) and c.get("revision_id") == incoming_revision]
            outcome["correction_complete"] = status == "separated"
            outcome["moved_to_event_id"] = moved[0] if moved else None
    elif isinstance(review.get("last_decision"), dict):
        # e.g. insufficient_evidence: recorded, but the review stays open (in_review).
        outcome = {"decision": review["last_decision"].get("decision"),
                   "decided_at": review["last_decision"].get("decided_at"), "final": False}

    return {
        "review_id": review["review_id"],
        "review_status": review["status"],
        "display_status": status,
        "active": status in ACTIVE,
        "detected_at": detected_at,
        "review_opened_at": review["review_opened_at"],
        "outcome": outcome,
        "precision": "claim_pair" if precise else "quotes_only",
        "topic": [c["aspect"] for c in claims] if precise else [],
        "notice": None if precise else GENERIC_NOTICE,
        "sources": sorted([incoming] + candidates,
                          key=lambda s: (s["published_at"] is None, s["published_at"] or _EPOCH, s["side"])),
        "unattributed_quotes": unattributed,
        "summary_predates_detection": (summary_published_at < detected_at
                                       if summary_published_at and detected_at else None),
        "summary_includes_report": (incoming_revision in summary_revisions
                                    if summary_revisions is not None else None),
    }


def event_conflict(conn: Connection, event_id: str, *, summary_published_at: datetime | None,
                   manifest: dict[str, Any] | None, job_status: str | None) -> dict[str, Any]:
    """The detail view's ``conflict`` block: status, every dispute (active first), and the summary's timing."""
    reviews = _review_rows(conn, event_id)
    summary_revisions = _summary_revisions(manifest) if summary_published_at else None
    revision_ids: set[str] = set()
    unit_ids: set[str] = set()
    for r in reviews:
        revision_ids.add(str(r["revision_id"]))
        revision_ids.update(str(x) for x in r["candidate_revision_ids"] or [])
        unit_ids.update(str(u) for u in r["candidate_unit_ids"] or [])
    units = _unit_texts(conn, unit_ids)
    revision_ids.update(u["revision_id"] for u in units.values())
    sources = _sources(conn, revision_ids)
    disputes = [build_dispute(r, sources=sources, units=units, summary_published_at=summary_published_at,
                              summary_revisions=summary_revisions) for r in reviews]
    statuses = [d["display_status"] for d in disputes]
    active = [d for d in disputes if d["active"]]

    # Only claim the summary predates the disagreement when every active dispute was detected after it.
    not_updated = (bool(active) and summary_published_at is not None
                   and all(d["summary_predates_detection"] is True for d in active))
    decided = [d["outcome"]["decided_at"] for d in disputes if d["outcome"] and d["outcome"]["final"]]
    last_decision = max(decided) if decided else None
    pending_update = None
    if (last_decision is not None and job_status in ("pending", "running", "failed", "awaiting_review")
            and (summary_published_at is None or summary_published_at < last_decision)):
        pending_update = {"status": {"pending": "queued", "running": "generating", "failed": "failed",
                                     "awaiting_review": "blocked_by_review"}[job_status],
                          "decided_at": last_decision, "summary_includes_review": False}
    return {**compact(statuses), "summary_published_at": summary_published_at,
            "summary_not_updated_for_conflict": not_updated, "pending_update": pending_update,
            "disputes": active + [d for d in disputes if not d["active"]]}
