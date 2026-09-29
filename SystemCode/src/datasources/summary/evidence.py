"""Bounded, deterministic evidence for one event summary, read from committed rows only.

What goes in
    Only ACTIVE memberships of the event's CURRENT revisions (``current_event_membership``):
    a pending ``uncertain`` relation, a detached membership (a reviewer's
    ``different_events`` decision) or a superseded revision never contributes
    evidence. Each member contributes the text units that support its membership
    (``event_membership_unit``), shown as the ORIGINAL source passage (the exact
    source spans, without the context prefix added for embedding).

Selection
    Passages are never cut: a passage is either given whole or omitted. Priority
    tiers: 0 the event's origin evidence, 1 update/conflict passages the
    comparison cited, 2 other update/conflict passages, 3 repeated coverage.
    Exact duplicates (syndicated copies) are given once. A first pass takes the
    best passage of every member so each source is represented; a second pass
    adds the rest in priority order until ``SUMMARY_MAX_INPUT_CHARS`` /
    ``SUMMARY_MAX_PASSAGES``. Whatever does not fit is recorded as a limitation.

Fingerprints
    ``evidence_fingerprint`` covers the members, their relations and comparisons,
    their supporting units (id + input hash) and the conflict reviews. It is
    recomputed right before publishing: a different value means the event changed
    during generation and the draft is stale. ``input_fingerprint`` adds the
    prompt version, model and selection limits; a published summary with the same
    input fingerprint means there is nothing new to summarize.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from psycopg import Connection

from ..similarity.store import SOURCE_TIME, _passage_from_spans
from .models import (
    ComparisonNote,
    EvidencePack,
    EvidencePassage,
    PreviousSummary,
    ReviewContext,
    SourceInfo,
)
from .settings import PROMPT_VERSION, SummarySettings

TIER_ORIGIN, TIER_CITED_CHANGE, TIER_CHANGE, TIER_REPEAT = 0, 1, 2, 3
_WS = re.compile(r"\s+")


class EvidenceError(RuntimeError):
    """The event cannot be summarized (missing, superseded, or no active evidence)."""


def _sha(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode("utf-8")).hexdigest()


# ----------------------------------------------------------------------------- raw rows
def event_row(conn: Connection, event_id: str, *, lock: bool = False) -> dict[str, Any] | None:
    return conn.execute(
        "SELECT event_id, is_sample, status, title, superseded_by, profile_id, pipeline_version "
        f"FROM news_event WHERE event_id = %s{' FOR UPDATE' if lock else ''}",
        (event_id,),
    ).fetchone()


def member_rows(conn: Connection, event_id: str) -> list[dict[str, Any]]:
    """Active members of the event (current revisions only), with source metadata."""
    return conn.execute(
        f"""
        SELECT m.membership_id, m.revision_id, m.document_id, m.relation, m.comparison_id, m.segment_key,
               d.record_type, r.title, r.original_url, r.content_scope, {SOURCE_TIME} AS source_time,
               COALESCE(pub.name, c.name) AS source_name, s.form, s.accession_number, s.item_codes,
               s.primary_document_url
          FROM current_event_membership m
          JOIN news_event e ON e.event_id = m.event_id
          JOIN document d ON d.document_id = m.document_id AND d.is_sample = e.is_sample
          JOIN document_revision r ON r.revision_id = m.revision_id
          LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
          LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
          LEFT JOIN company c ON c.company_id = s.issuer_company_id
         WHERE m.event_id = %s
         ORDER BY {SOURCE_TIME} NULLS LAST, m.revision_id
        """,
        (event_id,),
    ).fetchall()


def unit_rows(conn: Connection, membership_ids: list[str]) -> list[dict[str, Any]]:
    if not membership_ids:
        return []
    return conn.execute(
        """
        SELECT mu.membership_id, t.unit_id, t.revision_id, t.ordinal, t.input_hash, t.input_text, t.location,
               r.analysis_text, r.body_text
          FROM event_membership_unit mu
          JOIN text_unit t ON t.unit_id = mu.unit_id
          JOIN document_revision r ON r.revision_id = t.revision_id
         WHERE mu.membership_id = ANY(%s::uuid[])
         ORDER BY t.revision_id, t.ordinal
        """,
        (membership_ids,),
    ).fetchall()


def review_rows(conn: Connection, event_id: str) -> list[dict[str, Any]]:
    """Conflict reviews of comparisons against this event, with the comparison's claims."""
    return conn.execute(
        """
        SELECT v.review_id, v.comparison_id, v.status, v.resolution, v.reviewer_notes, c.revision_id,
               c.differences, c.incoming_unit_ids, c.candidate_unit_ids
          FROM event_conflict_review v
          JOIN event_comparison c ON c.comparison_id = v.comparison_id
         WHERE c.candidate_event_id = %s
         ORDER BY v.created_at, v.review_id
        """,
        (event_id,),
    ).fetchall()


def comparison_rows(conn: Connection, comparison_ids: list[str]) -> list[dict[str, Any]]:
    if not comparison_ids:
        return []
    return conn.execute(
        """
        SELECT comparison_id, revision_id, label, method, matching_facts, differences, explanation,
               incoming_unit_ids, candidate_unit_ids
          FROM event_comparison WHERE comparison_id = ANY(%s::uuid[])
         ORDER BY created_at, comparison_id
        """,
        (comparison_ids,),
    ).fetchall()


def published_summary(conn: Connection, event_id: str, language: str, *, lock: bool = False) -> dict | None:
    return conn.execute(
        "SELECT summary_id, version, headline, summary_text, statements, input_fingerprint, input_manifest, "
        f"published_at FROM event_summary WHERE event_id = %s AND language_code = %s AND status = 'published'"
        f"{' FOR UPDATE' if lock else ''}",
        (event_id, language),
    ).fetchone()


def evidence_fingerprint(members: list[dict], units: list[dict], reviews: list[dict], language: str) -> str:
    """Only reviews of ACTIVE members count: a review whose revision was detached changes nothing that is
    summarized (open reviews block publication separately)."""
    by_membership: dict[str, list[tuple[str, str]]] = {}
    for unit in units:
        by_membership.setdefault(str(unit["membership_id"]), []).append((str(unit["unit_id"]), unit["input_hash"]))
    member_revisions = {str(m["revision_id"]) for m in members}
    return _sha({
        "language": language,
        "members": sorted((str(m["membership_id"]), str(m["revision_id"]), m["relation"], str(m["comparison_id"]),
                           sorted(by_membership.get(str(m["membership_id"]), []))) for m in members),
        "reviews": sorted((str(r["review_id"]), r["status"], r["resolution"]) for r in reviews
                          if str(r["revision_id"]) in member_revisions),
    })


def current_evidence_fingerprint(conn: Connection, event_id: str, language: str) -> str:
    members = member_rows(conn, event_id)
    units = unit_rows(conn, [str(m["membership_id"]) for m in members])
    return evidence_fingerprint(members, units, review_rows(conn, event_id), language)


def input_fingerprint(evidence_fp: str, settings: SummarySettings) -> str:
    return _sha({"evidence": evidence_fp, "prompt_version": PROMPT_VERSION, "model": settings.model,
                 "max_input_chars": settings.max_input_chars, "max_passages": settings.max_passages,
                 "language": settings.language})


def open_review_count(conn: Connection, event_id: str) -> int:
    return int(conn.execute(
        """
        SELECT count(*) AS n FROM event_conflict_review v
          JOIN event_comparison c ON c.comparison_id = v.comparison_id
         WHERE c.candidate_event_id = %s AND v.status IN ('open', 'in_review')
        """,
        (event_id,),
    ).fetchone()["n"])


# ----------------------------------------------------------------------------- pack
def build_pack(conn: Connection, event_id: str, settings: SummarySettings) -> EvidencePack:
    event = event_row(conn, event_id)
    if event is None:
        raise EvidenceError(f"event {event_id} does not exist")
    if event["status"] == "superseded":
        raise EvidenceError(f"event {event_id} is superseded by {event['superseded_by']}; summarize that one")
    members = member_rows(conn, event_id)
    if not members:
        raise EvidenceError(f"event {event_id} has no active member with a current revision")
    units = unit_rows(conn, [str(m["membership_id"]) for m in members])
    reviews = review_rows(conn, event_id)
    evidence_fp = evidence_fingerprint(members, units, reviews, settings.language)

    sources = {str(m["revision_id"]): SourceInfo(
        revision_id=str(m["revision_id"]), document_id=str(m["document_id"]), membership_id=str(m["membership_id"]),
        relation=m["relation"], comparison_id=str(m["comparison_id"]) if m["comparison_id"] else None,
        record_type=m["record_type"], source_name=m["source_name"], title=m["title"],
        url=m["original_url"] or m["primary_document_url"], source_time=m["source_time"],
        content_scope=m["content_scope"], form=m["form"], accession_number=m["accession_number"],
        item_codes=list(m["item_codes"] or [])) for m in members}
    comparisons = [ComparisonNote(
        comparison_id=str(c["comparison_id"]), revision_id=str(c["revision_id"]), label=c["label"],
        method=c["method"], matching_facts=list(c["matching_facts"]), differences=list(c["differences"]),
        explanation=c["explanation"] or "", incoming_unit_ids=[str(u) for u in c["incoming_unit_ids"]],
        candidate_unit_ids=[str(u) for u in c["candidate_unit_ids"]])
        for c in comparison_rows(conn, [str(m["comparison_id"]) for m in members if m["comparison_id"]])]
    member_revisions = set(sources)
    resolved = [ReviewContext(
        review_id=str(r["review_id"]), comparison_id=str(r["comparison_id"]), revision_id=str(r["revision_id"]),
        status=r["status"], resolution=r["resolution"], reviewer_notes=r["reviewer_notes"],
        conflicting_claims=[d for d in r["differences"] if d.get("kind") == "conflict"] or list(r["differences"]),
        incoming_unit_ids=[str(u) for u in r["incoming_unit_ids"]],
        candidate_unit_ids=[str(u) for u in r["candidate_unit_ids"]])
        for r in reviews if r["status"] == "resolved" and r["resolution"] in ("not_a_conflict", "confirmed_conflict")
        and str(r["revision_id"]) in member_revisions]

    previous = None
    row = published_summary(conn, event_id, settings.language)
    if row is not None:
        previous = PreviousSummary(str(row["summary_id"]), int(row["version"]), row["headline"],
                                   list(row["statements"] or []))

    pack = EvidencePack(event_id=str(event["event_id"]), is_sample=event["is_sample"], language=settings.language,
                        event_title=event["title"], event_status=event["status"], sources=sources, passages=[],
                        comparisons=comparisons, reviews=resolved, previous=previous,
                        evidence_fingerprint=evidence_fp,
                        input_fingerprint=input_fingerprint(evidence_fp, settings))
    _select_passages(pack, units, settings)
    return pack


def _normalized(text: str) -> str:
    return _WS.sub(" ", text).strip().casefold()


def _select_passages(pack: EvidencePack, units: list[dict], settings: SummarySettings) -> None:
    from .generation import context_chars  # rendering lives with the prompt

    cited_changes = {u for c in pack.comparisons if c.label in ("same_event_update", "same_event_conflict")
                     for u in c.incoming_unit_ids}
    candidates: list[EvidencePassage] = []
    for unit in units:
        source = pack.sources[str(unit["revision_id"])]
        location = dict(unit["location"] or {})
        field_text = unit["analysis_text"] if location.get("source_field") == "analysis_text" else unit["body_text"]
        text = _passage_from_spans(field_text or "", location) or unit["input_text"]
        unit_id = str(unit["unit_id"])
        if source.relation == "origin":
            tier = TIER_ORIGIN
        elif source.relation in ("same_event_update", "same_event_conflict"):
            tier = TIER_CITED_CHANGE if unit_id in cited_changes else TIER_CHANGE
        else:
            tier = TIER_REPEAT
        candidates.append(EvidencePassage("", unit_id, source.revision_id, source.membership_id, source.relation,
                                          text, unit["input_hash"], tier, int(unit["ordinal"])))

    order = {rev: i for i, rev in enumerate(pack.sources)}  # sources are in source-time order
    candidates.sort(key=lambda p: (p.tier, order[p.revision_id], p.ordinal, p.unit_id))
    seen_text: set[str] = set()
    unique: list[EvidencePassage] = []
    duplicates = 0
    for passage in candidates:
        key = _normalized(passage.text)
        if key in seen_text:
            duplicates += 1
            continue
        seen_text.add(key)
        unique.append(passage)

    budget = max(settings.max_input_chars - context_chars(pack), settings.max_input_chars // 3)
    chosen: list[EvidencePassage] = []
    used = 0

    def take(passage: EvidencePassage) -> bool:
        nonlocal used
        cost = len(passage.text) + 200  # passage element + source attributes
        if len(chosen) >= settings.max_passages or used + cost > budget:
            return False
        chosen.append(passage)
        used += cost
        return True

    first_of_member: dict[str, EvidencePassage] = {}
    for passage in unique:
        first_of_member.setdefault(passage.membership_id, passage)
    taken: set[str] = set()
    for passage in sorted(first_of_member.values(), key=lambda p: (p.tier, order[p.revision_id])):
        if take(passage):
            taken.add(passage.unit_id)
    for passage in unique:
        if passage.unit_id not in taken and take(passage):
            taken.add(passage.unit_id)

    # Present passages grouped by source in time order, so the model reads a timeline.
    chosen.sort(key=lambda p: (order[p.revision_id], p.ordinal))
    for number, passage in enumerate(chosen, start=1):
        passage.ref = f"P{number}"
    pack.passages = chosen
    pack.omitted_unit_ids = [p.unit_id for p in unique if p.unit_id not in taken]
    if duplicates:
        pack.limitations.append(f"{duplicates} passage(s) identical to another source's text were given once")
    if pack.omitted_unit_ids:
        missing_sources = sorted({p.revision_id for p in unique} - {p.revision_id for p in chosen})
        note = (f"{len(pack.omitted_unit_ids)} of {len(unique)} passage(s) omitted to fit SUMMARY_MAX_INPUT_CHARS="
                f"{settings.max_input_chars} / SUMMARY_MAX_PASSAGES={settings.max_passages}")
        if missing_sources:
            note += f"; {len(missing_sources)} source(s) had no passage in the input"
        pack.limitations.append(note)
    without_units = sorted(set(pack.sources) - {p.revision_id for p in unique})
    if without_units:
        pack.limitations.append(f"{len(without_units)} member source(s) have no supporting text units")
