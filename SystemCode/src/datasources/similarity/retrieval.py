"""Candidate retrieval for one incoming revision.

Candidate pool
    Only previously COMMITTED rows are searched. A candidate must be in the same
    sample/live partition, have text units under the same embedding profile, be
    the CURRENT revision of a different document, share at least one company
    with the incoming record, and have a source time within
    ``anchor ± SIMILARITY_WINDOW_DAYS`` (default 7 days, an experimental
    starting point). The anchor is the incoming record's own source time (see
    ``AnalysisSubject.anchor``), never the worker's clock; the window is
    symmetric, so an article that arrives late still finds both earlier and later
    coverage of its event.

Bounded fallbacks
    No resolvable company link: no company filter, but the window narrows to
    ``SIMILARITY_NO_COMPANY_WINDOW_HOURS`` (default 48 h) and the limitation is
    recorded. No source publication time: the anchor falls back to the update
    time, the SEC filing date, then the retrieval time, and that is recorded too.

Two independent channels
    semantic  pgvector cosine distance (``<=>``), exact search over the pool;
    lexical   PostgreSQL full-text search (``search_tsv @@ tsquery``, ranked with
              ``ts_rank_cd``) using an OR-query of the incoming unit's lexemes.
    Neither channel gates the other: a paraphrase with no shared words is still
    found semantically. The per-channel ranks are merged with reciprocal rank
    fusion, score = sum over channels of 1 / (k + rank), k = SIMILARITY_RRF_K
    (60), and both channels' ranks and scores are kept for audit.

Scores are not probabilities; a high cosine similarity never means "same
event". Retrieval only builds the shortlist the comparison node judges.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any

from psycopg import Connection

from . import store
from .embeddings import vector_literal
from .models import CandidateEvent, CandidatePassage, RecordWork, RetrievalHit
from .settings import SimilaritySettings

LOGGER = logging.getLogger(__name__)
PREVIOUS_REVISION = "previous_revision"


class RetrievalError(RuntimeError):
    """Retrieval could not run; the analysis stays pending instead of claiming 'no matches'."""


@dataclass
class Scope:
    is_sample: bool
    profile_id: str
    pipeline_version: str
    window_start: Any
    window_end: Any
    company_ids: list[str]
    exclude_document_ids: list[str]

    def params(self) -> dict[str, Any]:
        return {"profile_id": self.profile_id, "pipeline_version": self.pipeline_version,
                "is_sample": self.is_sample, "window_start": self.window_start, "window_end": self.window_end,
                "company_ids": self.company_ids, "any_company": not self.company_ids,
                "exclude_documents": self.exclude_document_ids}


_POOL = f"""
    t.profile_id = %(profile_id)s
    AND d.is_sample = %(is_sample)s
    AND d.current_revision_id = t.revision_id
    AND NOT (d.document_id = ANY(%(exclude_documents)s::uuid[]))
    AND {store.SOURCE_TIME} BETWEEN %(window_start)s AND %(window_end)s
    AND (%(any_company)s OR EXISTS (
        SELECT 1 FROM revision_company rc
         WHERE rc.revision_id = t.revision_id AND rc.company_id = ANY(%(company_ids)s::uuid[])))
"""

SEMANTIC_SQL = f"""
SELECT t.unit_id, t.revision_id, d.document_id, 1 - (e.embedding <=> %(vector)s::vector) AS similarity
  FROM embedding e
  JOIN text_unit t ON t.unit_id = e.unit_id
  JOIN document_revision r ON r.revision_id = t.revision_id
  JOIN document d ON d.document_id = r.document_id
  LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
 WHERE t.embedding_status = 'completed' AND {_POOL}
 ORDER BY e.embedding <=> %(vector)s::vector, t.unit_id
 LIMIT %(limit)s
"""

LEXICAL_SQL = f"""
SELECT t.unit_id, t.revision_id, d.document_id, ts_rank_cd(t.search_tsv, q.query, 32) AS score
  FROM text_unit t
  JOIN document_revision r ON r.revision_id = t.revision_id
  JOIN document d ON d.document_id = r.document_id
  LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
  CROSS JOIN (SELECT %(tsquery)s::tsquery AS query) q
 WHERE t.search_tsv @@ q.query AND {_POOL}
 ORDER BY score DESC, t.unit_id
 LIMIT %(limit)s
"""

COVERAGE_SQL = f"""
WITH pool AS (
    SELECT r.revision_id, a.status,
           EXISTS (SELECT 1 FROM text_unit t JOIN embedding e ON e.unit_id = t.unit_id
                    WHERE t.revision_id = r.revision_id AND t.profile_id = %(profile_id)s
                      AND t.embedding_status = 'completed') AS indexed
      FROM document d
      JOIN document_revision r ON r.revision_id = d.current_revision_id
      LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
      LEFT JOIN revision_analysis a ON a.revision_id = r.revision_id AND a.profile_id = %(profile_id)s
                                   AND a.pipeline_version = %(pipeline_version)s
     WHERE d.is_sample = %(is_sample)s
       AND NOT (d.document_id = ANY(%(exclude_documents)s::uuid[]))
       AND {store.SOURCE_TIME} BETWEEN %(window_start)s AND %(window_end)s
       AND (%(any_company)s OR EXISTS (
            SELECT 1 FROM revision_company rc
             WHERE rc.revision_id = r.revision_id AND rc.company_id = ANY(%(company_ids)s::uuid[])))
)
SELECT count(*) AS pool_revisions,
       count(*) FILTER (WHERE indexed) AS indexed_revisions,
       count(*) FILTER (WHERE NOT indexed AND status = 'skipped') AS unusable_revisions,
       count(*) FILTER (WHERE NOT indexed AND status = 'failed') AS failed_revisions,
       count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM event_membership m JOIN news_event e ON e.event_id = m.event_id
            WHERE m.revision_id = pool.revision_id AND m.state = 'active' AND e.status <> 'superseded'
              AND e.profile_id = %(profile_id)s AND e.pipeline_version = %(pipeline_version)s)) AS grouped_revisions
  FROM pool
"""


def tsquery_literal(lexemes: list[str]) -> str | None:
    """OR-query of already-normalized lexemes, quoted so PostgreSQL does not re-parse them."""
    quoted = ["'" + lex.replace("\\", "\\\\").replace("'", "''") + "'" for lex in lexemes if lex]
    return " | ".join(quoted) or None


def lexical_queries(conn: Connection, texts: list[str], max_terms: int) -> list[str | None]:
    """One tsquery per text from its most frequent (then earliest) English lexemes."""
    if not texts:
        return []
    rows = conn.execute(
        """
        SELECT u.ord, v.lexeme, cardinality(v.positions) AS tf, v.positions[1] AS first_pos
          FROM unnest(%s::text[]) WITH ORDINALITY AS u(txt, ord)
          CROSS JOIN LATERAL unnest(to_tsvector('english', u.txt)) AS v
        """,
        (texts,),
    ).fetchall()
    per_text: dict[int, list[tuple[int, int, str]]] = {}
    for row in rows:
        lexeme = row["lexeme"]
        if len(lexeme) < 2 and not lexeme.isdigit():
            continue
        per_text.setdefault(int(row["ord"]) - 1, []).append((-(row["tf"] or 1), row["first_pos"] or 0, lexeme))
    queries: list[str | None] = []
    for index in range(len(texts)):
        chosen = [lex for _tf, _pos, lex in sorted(per_text.get(index, []))[:max_terms]]
        queries.append(tsquery_literal(chosen))
    return queries


@dataclass
class Fused:
    unit_id: str
    revision_id: str
    document_id: str
    score: float = 0.0
    semantic_rank: int | None = None
    semantic_similarity: float | None = None
    lexical_rank: int | None = None
    lexical_score: float | None = None


def fuse(semantic: list[dict], lexical: list[dict], k: int) -> list[Fused]:
    """Reciprocal rank fusion of two ranked lists; ties broken by unit id for determinism."""
    merged: dict[str, Fused] = {}
    for rank, row in enumerate(semantic, start=1):
        entry = merged.setdefault(str(row["unit_id"]), Fused(str(row["unit_id"]), str(row["revision_id"]),
                                                              str(row["document_id"])))
        entry.semantic_rank, entry.semantic_similarity = rank, float(row["similarity"])
        entry.score += 1.0 / (k + rank)
    for rank, row in enumerate(lexical, start=1):
        entry = merged.setdefault(str(row["unit_id"]), Fused(str(row["unit_id"]), str(row["revision_id"]),
                                                              str(row["document_id"])))
        entry.lexical_rank, entry.lexical_score = rank, float(row["score"])
        entry.score += 1.0 / (k + rank)
    return sorted(merged.values(), key=lambda f: (-f.score, f.unit_id))


def build_scope(work: RecordWork, *, profile_id: str, pipeline_version: str,
                settings: SimilaritySettings) -> Scope:
    anchor, source = work.subject.anchor()
    if anchor is None:
        raise RetrievalError("the record has no usable timestamp to anchor the search window")
    work.anchor_time, work.anchor_source = anchor, source
    if source != "published_at":
        work.limitations.append(f"search window anchored to {source}: no source publication time")
    if work.company_ids:
        half = timedelta(days=settings.window_days)
    else:
        half = timedelta(hours=settings.no_company_window_hours)
        work.limitations.append(
            f"no company link: company filter not applied, window narrowed to +/-{settings.no_company_window_hours:g}h")
    exclude = [work.subject.document_id] if work.subject.document_id else []
    return Scope(work.subject.is_sample, profile_id, pipeline_version, anchor - half, anchor + half,
                 list(work.company_ids), exclude)


def coverage(conn: Connection, scope: Scope) -> dict[str, Any]:
    """How much of the candidate pool is searchable under this profile.

    Revisions with no usable text (``skipped``) and revisions whose analysis was
    given up (``failed``) can never be indexed; they are reported separately, so
    they do not keep every neighbour ``partial`` forever. Anything else without
    embeddings makes coverage incomplete.
    """
    row = conn.execute(COVERAGE_SQL, scope.params()).fetchone()
    pool = int(row["pool_revisions"])
    indexed = int(row["indexed_revisions"])
    unusable = int(row["unusable_revisions"])
    failed = int(row["failed_revisions"])
    unindexed = max(pool - indexed - unusable - failed, 0)
    return {"pool_revisions": pool, "indexed_revisions": indexed, "unusable_revisions": unusable,
            "failed_revisions": failed, "unindexed_revisions": unindexed,
            "grouped_revisions": int(row["grouped_revisions"]), "complete": unindexed == 0,
            "window_start": scope.window_start.isoformat(), "window_end": scope.window_end.isoformat(),
            "company_filter": bool(scope.company_ids)}


def retrieve(conn: Connection, work: RecordWork, *, profile_id: str, pipeline_version: str,
             settings: SimilaritySettings) -> None:
    """Fill ``work.hits``, ``work.candidate_events`` and ``work.coverage`` (read-only queries)."""
    plan = work.plan
    assert plan is not None
    scope = build_scope(work, profile_id=profile_id, pipeline_version=pipeline_version, settings=settings)
    work.coverage = coverage(conn, scope)
    if not work.coverage["complete"]:
        work.limitations.append(
            f"index coverage partial: {work.coverage['unindexed_revisions']} revision(s) in the search window "
            "have no embeddings under this profile, so 'no match' is not proof of uniqueness")

    queried = [u for u in plan.units if u.key in work.vectors]
    tsqueries = lexical_queries(conn, [u.text for u in queried], settings.lexical_max_terms)
    fused_by_unit: dict[str, list[Fused]] = {}
    for unit, tsquery in zip(queried, tsqueries):
        params = scope.params()
        semantic = conn.execute(SEMANTIC_SQL, {**params, "vector": vector_literal(work.vectors[unit.key]),
                                               "limit": settings.semantic_top_k}).fetchall()
        lexical = conn.execute(LEXICAL_SQL, {**params, "tsquery": tsquery, "limit": settings.lexical_top_k}
                               ).fetchall() if tsquery else []
        fused_by_unit[unit.key] = fuse(semantic, lexical, settings.rrf_k)

    unit_ids = sorted({f.unit_id for hits in fused_by_unit.values() for f in hits})
    details = store.passages(conn, unit_ids)
    by_unit, by_revision = store.events_for_candidates(
        conn, unit_ids, sorted({d.revision_id for d in details.values()}), profile_id=profile_id,
        pipeline_version=pipeline_version, is_sample=work.subject.is_sample)
    own_events = {m["event_id"] for m in work.existing.memberships}
    segment_keys = [s.key for s in plan.segments]
    unit_segment = {u.key: u.segment_key for u in plan.units}

    events: dict[str, CandidateEvent] = {}
    for incoming_key, fused in fused_by_unit.items():
        targets = [unit_segment[incoming_key]] if unit_segment[incoming_key] else segment_keys
        for rank, entry in enumerate(fused, start=1):
            base = details.get(entry.unit_id)
            if base is None:
                continue
            candidate = replace(base, semantic_rank=entry.semantic_rank,
                                semantic_similarity=entry.semantic_similarity, lexical_rank=entry.lexical_rank,
                                lexical_score=entry.lexical_score, fused_score=entry.score,
                                incoming_unit_key=incoming_key,
                                event_ids=list(by_unit.get(entry.unit_id) or by_revision.get(base.revision_id, [])))
            hit = RetrievalHit(incoming_key, candidate, rank)
            work.hits.append(hit)
            if not _plausible(candidate, settings):
                hit.not_compared_reason = "below_similarity_floor"
                continue
            if not candidate.event_ids:
                hit.not_compared_reason = "candidate_not_grouped"
                continue
            fresh = [e for e in candidate.event_ids if e not in own_events]
            if not fresh:
                hit.not_compared_reason = "own_event"
                continue
            for event_id in fresh:
                event = events.setdefault(event_id, CandidateEvent(event_id, "", "", {}, 0, None, []))
                event.passages.append(candidate)
                event.best_fused_score = max(event.best_fused_score, candidate.fused_score)
                if candidate.semantic_similarity is not None:
                    event.best_semantic_similarity = max(event.best_semantic_similarity or -1.0,
                                                         candidate.semantic_similarity)
                if candidate.lexical_score is not None:
                    event.best_lexical_score = max(event.best_lexical_score or 0.0, candidate.lexical_score)
                event.segment_keys.update(targets)

    ranked = sorted(events.values(), key=lambda e: (-e.best_fused_score, e.event_id))
    shortlist = {e.event_id for e in ranked[:settings.max_candidate_events]}
    for hit in work.hits:
        if hit.not_compared_reason is None:
            if any(e in shortlist for e in hit.candidate.event_ids):
                hit.shortlisted = True
            else:
                hit.not_compared_reason = "below_shortlist"
    kept = {e.event_id: e for e in ranked if e.event_id in shortlist}

    if work.predecessor_revision_id:
        _add_previous_revision(conn, work, kept, profile_id=profile_id, pipeline_version=pipeline_version,
                               segment_keys=segment_keys, own_events=own_events)

    _attach_details(conn, kept)
    for event in kept.values():
        unique: dict[str, CandidatePassage] = {}
        for passage in sorted(event.passages, key=lambda p: -p.fused_score):
            unique.setdefault(passage.unit_id, passage)
        event.passages = list(unique.values())[:2]
    work.candidate_events = kept


def _plausible(candidate: CandidatePassage, settings: SimilaritySettings) -> bool:
    """Whether a candidate may reach the LLM.

    Semantic: cosine similarity at or above SIMILARITY_MIN_SEMANTIC_SIMILARITY.
    Lexical (off by default): among the top SIMILARITY_LEXICAL_PLAUSIBLE_RANK
    full-text hits for the incoming unit. The rank is relative, so the OR-query
    puts some same-company article in the top N almost every time; with the
    default 0 the full-text channel only orders candidates (fusion) and never
    sends one to the model on its own.
    """
    semantic_ok = (candidate.semantic_similarity is not None
                   and candidate.semantic_similarity >= settings.min_semantic_similarity)
    lexical_ok = (settings.lexical_plausible_rank > 0 and candidate.lexical_rank is not None
                  and candidate.lexical_rank <= settings.lexical_plausible_rank
                  and (candidate.lexical_score or 0.0) >= settings.min_lexical_score)
    return semantic_ok or lexical_ok


def _add_previous_revision(conn: Connection, work: RecordWork, kept: dict[str, CandidateEvent], *,
                           profile_id: str, pipeline_version: str, segment_keys: list[str],
                           own_events: set[str]) -> None:
    """Events of this document's previous revision: compared deliberately, never counted as another source."""
    revision_id = work.predecessor_revision_id
    event_ids = [e for e in store.revision_event_ids(conn, revision_id, profile_id=profile_id,
                                                     pipeline_version=pipeline_version) if e not in own_events]
    if not event_ids:
        return
    units = store.passages(conn, store.revision_unit_ids(conn, revision_id, profile_id))
    by_unit, _ = store.events_for_candidates(conn, list(units), [], profile_id=profile_id,
                                             pipeline_version=pipeline_version, is_sample=work.subject.is_sample)
    for event_id in event_ids:
        supporting = [p for uid, p in units.items() if event_id in by_unit.get(uid, [])] or list(units.values())
        event = kept.setdefault(event_id, CandidateEvent(event_id, "", "", {}, 0, None, []))
        event.source = PREVIOUS_REVISION
        event.segment_keys.update(segment_keys)
        for passage in supporting[:2]:
            event.passages.append(replace(passage, event_ids=[event_id]))


def _attach_details(conn: Connection, events: dict[str, CandidateEvent]) -> None:
    details = store.event_details(conn, list(events))
    origin_ids = sorted({uid for d in details.values() for uid in d["origin_unit_ids"][:1]})
    origin_passages = store.passages(conn, origin_ids)
    for event_id, event in events.items():
        info = details.get(event_id)
        if info is None:
            continue
        event.title = info["title"]
        event.status = info["status"]
        event.facts = dict(info["event_facts"] or {})
        event.member_documents = int(info["documents"] or 0)
        event.first_source_time = info["first_source_time"]
        event.origin_passages = [origin_passages[u] for u in info["origin_unit_ids"][:1] if u in origin_passages]
