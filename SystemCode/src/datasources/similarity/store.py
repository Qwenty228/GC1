"""SQL for semantic analysis: profiles, text units, embeddings, analysis status,
retrieval scores, comparisons, events and memberships.

Reads run in short transactions. ``persist_analysis`` runs inside the one
transaction opened by the graph's ``persist_result`` node, after all model work
is finished, so no transaction is ever held open during embedding or LLM calls.
All SQL is parameterised; replay safety comes from the unique constraints of
migrations 001/002 (``ON CONFLICT DO NOTHING`` + re-select).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from ..db.repository import PersistenceError, companies_for_symbols
from .embeddings import ProfileSpec, parse_vector, vector_literal
from .models import AnalysisSubject, CandidatePassage, Judgment, RecordWork

LOGGER = logging.getLogger(__name__)

SOURCE_TIME = ("COALESCE(r.published_at, s.accepted_at, r.source_updated_at, "
               "(s.filing_date::timestamp AT TIME ZONE 'UTC'), r.retrieved_at)")
REQUIRED_RELATIONS = ("revision_analysis", "news_event", "event_comparison", "event_membership",
                      "event_membership_unit", "retrieval_candidate", "text_unit", "embedding", "embedding_profile")


# ----------------------------------------------------------------------------- schema / profile
def missing_schema(conn: Connection) -> list[str]:
    missing = [name for name in REQUIRED_RELATIONS
               if not conn.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (f"public.{name}",)).fetchone()["ok"]]
    column = conn.execute("SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' "
                          "AND table_name = 'text_unit' AND column_name = 'search_tsv'").fetchone()
    if column is None:
        missing.append("text_unit.search_tsv")
    return missing


def get_or_create_profile(conn: Connection, spec: ProfileSpec) -> str:
    row = conn.execute("SELECT profile_id, dimensions FROM embedding_profile WHERE config_hash = %s",
                       (spec.config_hash,)).fetchone()
    if row is None:
        ident = spec.identity
        row = conn.execute(
            """
            INSERT INTO embedding_profile (config_hash, model_provider, model_name, model_version, dimensions,
                                           distance_metric, chunking_version, configuration)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (config_hash) DO NOTHING
            RETURNING profile_id, dimensions
            """,
            (spec.config_hash, ident.model_provider, ident.model_name, ident.model_version, ident.dimensions,
             ident.distance_metric, spec.chunking_version, Jsonb(spec.configuration())),
        ).fetchone() or conn.execute("SELECT profile_id, dimensions FROM embedding_profile WHERE config_hash = %s",
                                     (spec.config_hash,)).fetchone()
    if int(row["dimensions"]) != spec.identity.dimensions:
        raise PersistenceError("embedding_profile dimensions disagree with the configured model")
    return str(row["profile_id"])


# ----------------------------------------------------------------------------- lookups
def find_persisted_revision(conn: Connection, subject: AnalysisSubject) -> dict[str, Any] | None:
    """The stored revision for this exact payload, and the document's current revision."""
    return conn.execute(
        """
        SELECT d.document_id, r.revision_id, d.current_revision_id
          FROM document d
          JOIN provider p ON p.provider_id = d.provider_id
          JOIN document_revision r ON r.document_id = d.document_id
         WHERE p.code = %s AND d.source_namespace = %s AND d.source_record_id = %s AND d.is_sample = %s
           AND r.source_snapshot_hash = %s AND r.preprocessing_version = %s
        """,
        (subject.provider, subject.source_namespace, subject.source_record_id, subject.is_sample,
         subject.snapshot_hash, subject.preprocessing_version),
    ).fetchone()


def find_document(conn: Connection, subject: AnalysisSubject) -> dict[str, Any] | None:
    return conn.execute(
        """
        SELECT d.document_id, d.current_revision_id
          FROM document d JOIN provider p ON p.provider_id = d.provider_id
         WHERE p.code = %s AND d.source_namespace = %s AND d.source_record_id = %s AND d.is_sample = %s
        """,
        (subject.provider, subject.source_namespace, subject.source_record_id, subject.is_sample),
    ).fetchone()


def resolve_company_ids(conn: Connection, subject: AnalysisSubject) -> list[str]:
    """Company ids for a record that may not be stored yet (read-only: nothing is created)."""
    if subject.company_ids is not None:
        return list(subject.company_ids)
    ids, _unresolved = companies_for_symbols(conn, subject.symbols)
    if subject.record_type == "sec_filing" and subject.cik:
        row = conn.execute("SELECT company_id FROM company WHERE cik = %s", (subject.cik,)).fetchone()
        if row and str(row["company_id"]) not in ids:
            ids.append(str(row["company_id"]))
    return ids


def _evidence_from_db(record_type: str, content_status: str, title: str | None) -> tuple[str | None, str | None]:
    if record_type == "sec_filing":
        return ("full", None) if content_status == "available" else (None, "extraction_failed")
    if content_status == "available":
        return "full", None
    if content_status == "partial":
        return "summary_only", None
    if content_status == "missing" and (title or "").strip():
        return "headline_only", None
    return None, None


def load_subject(conn: Connection, revision_id: str) -> AnalysisSubject | None:
    row = conn.execute(
        """
        SELECT d.document_id, d.record_type, d.is_sample, d.source_namespace, d.source_record_id,
               p.code AS provider, r.revision_id, r.title, r.title_generated, r.summary_text, r.body_text,
               r.analysis_text, r.content_status, r.published_at, r.source_updated_at, r.retrieved_at,
               r.source_snapshot_hash, r.preprocessing_version, pub.name AS publisher,
               s.form, s.filing_date, s.item_codes, s.cik, c.name AS issuer
          FROM document_revision r
          JOIN document d ON d.document_id = r.document_id
          JOIN provider p ON p.provider_id = d.provider_id
          LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
          LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
          LEFT JOIN company c ON c.company_id = s.issuer_company_id
         WHERE r.revision_id = %s
        """,
        (revision_id,),
    ).fetchone()
    if row is None:
        return None
    companies = [str(r["company_id"]) for r in conn.execute(
        "SELECT company_id FROM revision_company WHERE revision_id = %s ORDER BY company_id", (revision_id,))]
    evidence, unusable = _evidence_from_db(row["record_type"], row["content_status"], row["title"])
    filing_date = row["filing_date"]
    return AnalysisSubject(
        record_type=row["record_type"], provider=row["provider"], is_sample=row["is_sample"],
        source_record_id=row["source_record_id"], source_namespace=row["source_namespace"],
        title=row["title"], title_is_generated=row["title_generated"], summary_text=row["summary_text"],
        body_text=row["body_text"], analysis_text=row["analysis_text"], evidence_level=evidence,
        unusable_reason=unusable, publisher=row["publisher"], issuer_name=row["issuer"], cik=row["cik"],
        form=row["form"], filing_date=filing_date.isoformat() if isinstance(filing_date, date) else None,
        items=list(row["item_codes"] or []), published_at=row["published_at"], updated_at=row["source_updated_at"],
        retrieved_at=row["retrieved_at"], snapshot_hash=row["source_snapshot_hash"],
        preprocessing_version=row["preprocessing_version"], document_id=str(row["document_id"]),
        revision_id=str(row["revision_id"]), company_ids=companies,
    )


def load_analysis(conn: Connection, revision_id: str, profile_id: str, pipeline_version: str) -> dict | None:
    return conn.execute(
        "SELECT * FROM revision_analysis WHERE revision_id = %s AND profile_id = %s AND pipeline_version = %s",
        (revision_id, profile_id, pipeline_version),
    ).fetchone()


def load_units(conn: Connection, revision_id: str, profile_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT t.unit_id, t.unit_kind, t.ordinal, t.input_hash, t.embedding_status, t.retry_count,
               e.embedding::text AS vector
          FROM text_unit t LEFT JOIN embedding e ON e.unit_id = t.unit_id
         WHERE t.revision_id = %s AND t.profile_id = %s
         ORDER BY t.unit_kind, t.ordinal
        """,
        (revision_id, profile_id),
    ).fetchall()
    return [{**r, "unit_id": str(r["unit_id"]), "vector": parse_vector(r["vector"]) if r["vector"] else None}
            for r in rows]


def vectors_by_hash(conn: Connection, profile_id: str, hashes: list[str]) -> dict[str, list[float]]:
    """Completed vectors for identical input under the same profile (reused, not recomputed)."""
    if not hashes:
        return {}
    rows = conn.execute(
        """
        SELECT DISTINCT ON (t.input_hash) t.input_hash, e.embedding::text AS vector
          FROM text_unit t JOIN embedding e ON e.unit_id = t.unit_id
         WHERE t.profile_id = %s AND t.input_hash = ANY(%s) AND t.embedding_status = 'completed'
         ORDER BY t.input_hash, t.unit_id
        """,
        (profile_id, list(hashes)),
    ).fetchall()
    return {r["input_hash"]: parse_vector(r["vector"]) for r in rows}


def load_judgments(conn: Connection, analysis_id: str, unit_keys_by_id: dict[str, str]) -> list[Judgment]:
    rows = conn.execute("SELECT * FROM event_comparison WHERE analysis_id = %s ORDER BY created_at", (analysis_id,))
    judgments = []
    for row in rows:
        judgments.append(Judgment(
            segment_key=row["segment_key"], event_id=str(row["candidate_event_id"]), label=row["label"],
            method=row["method"],
            incoming_unit_keys=[unit_keys_by_id[str(u)] for u in row["incoming_unit_ids"] if str(u) in unit_keys_by_id],
            candidate_unit_ids=[str(u) for u in row["candidate_unit_ids"]],
            candidate_revision_ids=[str(r) for r in row["candidate_revision_ids"]],
            matching_facts=list(row["matching_facts"]), differences=list(row["differences"]),
            evidence=dict(row["evidence"]), explanation=row["explanation"],
            insufficient_context=row["insufficient_context"], uncertainty=row["uncertainty"],
            validation_notes=list(row["validation_notes"]), model=row["model"],
            prompt_version=row["prompt_version"], usage=dict(row["usage"]),
            persisted_id=str(row["comparison_id"]),
        ))
    return judgments


def load_memberships(conn: Connection, revision_id: str, profile_id: str, pipeline_version: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT m.membership_id, m.event_id, m.segment_key, m.relation, m.state, m.analysis_id,
               e.status AS event_status, e.origin_revision_id,
               (SELECT count(DISTINCT m2.document_id) FROM event_membership m2
                 WHERE m2.event_id = m.event_id AND m2.state = 'active') AS documents
          FROM event_membership m JOIN news_event e ON e.event_id = m.event_id
         WHERE m.revision_id = %s AND e.profile_id = %s AND e.pipeline_version = %s
         ORDER BY m.created_at
        """,
        (revision_id, profile_id, pipeline_version),
    ).fetchall()
    return [{**r, "membership_id": str(r["membership_id"]), "event_id": str(r["event_id"]),
             "analysis_id": str(r["analysis_id"]), "origin_revision_id": str(r["origin_revision_id"])} for r in rows]


# ----------------------------------------------------------------------------- candidate context
def events_for_candidates(conn: Connection, unit_ids: list[str], revision_ids: list[str], *, profile_id: str,
                          pipeline_version: str, is_sample: bool) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Active, non-superseded events per candidate unit (direct support) and per candidate revision."""
    by_unit: dict[str, list[str]] = {}
    by_revision: dict[str, list[str]] = {}
    if unit_ids:
        for row in conn.execute(
            """
            SELECT mu.unit_id, m.event_id
              FROM event_membership_unit mu
              JOIN event_membership m ON m.membership_id = mu.membership_id
              JOIN news_event e ON e.event_id = m.event_id
             WHERE mu.unit_id = ANY(%s::uuid[]) AND m.state = 'active' AND e.status <> 'superseded'
               AND e.profile_id = %s AND e.pipeline_version = %s AND e.is_sample = %s
             ORDER BY mu.unit_id, m.created_at
            """,
            (unit_ids, profile_id, pipeline_version, is_sample),
        ):
            by_unit.setdefault(str(row["unit_id"]), []).append(str(row["event_id"]))
    if revision_ids:
        for row in conn.execute(
            """
            SELECT m.revision_id, m.event_id
              FROM event_membership m JOIN news_event e ON e.event_id = m.event_id
             WHERE m.revision_id = ANY(%s::uuid[]) AND m.state = 'active' AND e.status <> 'superseded'
               AND e.profile_id = %s AND e.pipeline_version = %s AND e.is_sample = %s
             ORDER BY m.revision_id, m.created_at
            """,
            (revision_ids, profile_id, pipeline_version, is_sample),
        ):
            by_revision.setdefault(str(row["revision_id"]), []).append(str(row["event_id"]))
    return by_unit, by_revision


def passages(conn: Connection, unit_ids: list[str]) -> dict[str, CandidatePassage]:
    """Candidate units with the metadata a comparison needs."""
    if not unit_ids:
        return {}
    rows = conn.execute(
        f"""
        SELECT t.unit_id, t.revision_id, d.document_id, t.input_text, t.location, d.record_type, r.title,
               COALESCE(pub.name, c.name) AS source_name, {SOURCE_TIME} AS source_time, r.analysis_text, r.body_text
          FROM text_unit t
          JOIN document_revision r ON r.revision_id = t.revision_id
          JOIN document d ON d.document_id = r.document_id
          LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
          LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
          LEFT JOIN company c ON c.company_id = s.issuer_company_id
         WHERE t.unit_id = ANY(%s::uuid[])
        """,
        (unit_ids,),
    ).fetchall()
    out: dict[str, CandidatePassage] = {}
    for row in rows:
        location = dict(row["location"])
        source = row["analysis_text"] if location.get("source_field") == "analysis_text" else row["body_text"]
        passage = _passage_from_spans(source or "", location) or row["input_text"]
        out[str(row["unit_id"])] = CandidatePassage(
            unit_id=str(row["unit_id"]), revision_id=str(row["revision_id"]), document_id=str(row["document_id"]),
            text=row["input_text"], passage=passage, record_type=row["record_type"], title=row["title"],
            publisher=row["source_name"], source_time=row["source_time"], event_ids=[], location=location)
    return out


def _passage_from_spans(source: str, location: dict[str, Any]) -> str | None:
    spans = location.get("spans") or []
    if not spans or not source:
        return None
    parts: list[str] = []
    previous = None
    for start, end in spans:
        if previous is not None:
            parts.append("\n\n")
        parts.append(source[start:end])
        previous = end
    return "".join(parts)


def event_details(conn: Connection, event_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not event_ids:
        return {}
    rows = conn.execute(
        """
        SELECT e.event_id, e.title, e.status, e.event_facts, e.first_source_time, e.origin_revision_id,
               (SELECT count(DISTINCT m.document_id) FROM current_event_membership m
                 WHERE m.event_id = e.event_id) AS documents
          FROM news_event e WHERE e.event_id = ANY(%s::uuid[])
        """,
        (event_ids,),
    ).fetchall()
    details = {str(r["event_id"]): {**r, "event_id": str(r["event_id"]), "origin_unit_ids": []} for r in rows}
    for row in conn.execute(
        """
        SELECT m.event_id, mu.unit_id
          FROM event_membership m
          JOIN event_membership_unit mu ON mu.membership_id = m.membership_id
          JOIN text_unit t ON t.unit_id = mu.unit_id
         WHERE m.event_id = ANY(%s::uuid[]) AND m.relation = 'origin'
         ORDER BY m.event_id, t.ordinal
        """,
        (event_ids,),
    ):
        details[str(row["event_id"])]["origin_unit_ids"].append(str(row["unit_id"]))
    return details


def revision_event_ids(conn: Connection, revision_id: str, *, profile_id: str, pipeline_version: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT m.event_id FROM event_membership m JOIN news_event e ON e.event_id = m.event_id
         WHERE m.revision_id = %s AND m.state = 'active' AND e.status <> 'superseded'
           AND e.profile_id = %s AND e.pipeline_version = %s
         ORDER BY m.created_at
        """,
        (revision_id, profile_id, pipeline_version),
    ).fetchall()
    return [str(r["event_id"]) for r in rows]


def revision_unit_ids(conn: Connection, revision_id: str, profile_id: str) -> list[str]:
    return [str(r["unit_id"]) for r in conn.execute(
        "SELECT unit_id FROM text_unit WHERE revision_id = %s AND profile_id = %s ORDER BY ordinal",
        (revision_id, profile_id))]


# ----------------------------------------------------------------------------- persistence
def persist_analysis(conn: Connection, work: RecordWork, *, profile_id: str, pipeline_version: str,
                     comparison_model: str | None, prompt_version: str, max_attempts: int,
                     retry_base_minutes: float, max_embedding_attempts: int | None = None,
                     now: datetime | None = None, count_attempt: bool = True,
                     due_now: bool = False) -> dict[str, Any]:
    """Write everything the analysis produced for one stored revision (inside the caller's transaction).

    ``count_attempt=False`` / ``due_now=True`` are used by the bootstrap's index-only
    pass: indexing is not a failed grouping attempt, and grouping is due at once.
    """
    now = now or datetime.now(timezone.utc)
    subject = work.subject
    assert subject.revision_id and subject.document_id, "persist the revision before its analysis"
    plan = work.plan
    existing = conn.execute(
        "SELECT analysis_id, attempts FROM revision_analysis "
        "WHERE revision_id = %s AND profile_id = %s AND pipeline_version = %s FOR UPDATE",
        (subject.revision_id, profile_id, pipeline_version),
    ).fetchone()
    attempts = (int(existing["attempts"]) if existing else 0) + (1 if count_attempt else 0)
    status = work.status or "pending"
    last_error = "; ".join(f"{e['stage']}: {e['message']}" for e in work.errors)[:1000] or None
    if status == "pending" and count_attempt and work.errors and attempts >= max_attempts:
        status = "failed"
        work.limitations.append(f"gave up after {attempts} attempts")
    if status == "pending":
        next_retry = now if due_now or not work.errors else now + timedelta(
            minutes=retry_base_minutes * 2 ** max(attempts - 1, 0))
    else:
        next_retry = None
    values = {
        "revision_id": subject.revision_id, "document_id": subject.document_id, "profile_id": profile_id,
        "pipeline_version": pipeline_version, "status": status, "stage_status": Jsonb(work.stage_status),
        "skip_reason": plan.skip_reason if plan is not None and status == "skipped" else None,
        "evidence_level": plan.evidence_level if plan is not None else subject.evidence_level,
        "anchor_time": work.anchor_time, "anchor_source": work.anchor_source,
        "index_coverage": Jsonb(work.coverage), "limitations": Jsonb(list(dict.fromkeys(work.limitations))),
        "usage": Jsonb(work.usage), "comparison_model": comparison_model, "prompt_version": prompt_version,
        "attempts": attempts, "last_error": last_error, "next_retry_at": next_retry,
        "completed_at": now if status in ("completed", "skipped") else None,
    }
    if values["skip_reason"] is None and status == "skipped":
        values["skip_reason"] = "no_text"
    if existing is None:
        analysis_id = conn.execute(
            """
            INSERT INTO revision_analysis (revision_id, document_id, profile_id, pipeline_version, status,
                stage_status, skip_reason, evidence_level, anchor_time, anchor_source, index_coverage, limitations,
                usage, comparison_model, prompt_version, attempts, last_error, next_retry_at, completed_at)
            VALUES (%(revision_id)s, %(document_id)s, %(profile_id)s, %(pipeline_version)s, %(status)s,
                %(stage_status)s, %(skip_reason)s, %(evidence_level)s, %(anchor_time)s, %(anchor_source)s,
                %(index_coverage)s, %(limitations)s, %(usage)s, %(comparison_model)s, %(prompt_version)s,
                %(attempts)s, %(last_error)s, %(next_retry_at)s, %(completed_at)s)
            RETURNING analysis_id
            """,
            values,
        ).fetchone()["analysis_id"]
    else:
        analysis_id = existing["analysis_id"]
        conn.execute(
            """
            UPDATE revision_analysis SET status = %(status)s, stage_status = %(stage_status)s,
                   skip_reason = %(skip_reason)s, evidence_level = %(evidence_level)s, anchor_time = %(anchor_time)s,
                   anchor_source = %(anchor_source)s, index_coverage = %(index_coverage)s,
                   limitations = %(limitations)s, usage = %(usage)s, comparison_model = %(comparison_model)s,
                   prompt_version = %(prompt_version)s, attempts = %(attempts)s, last_error = %(last_error)s,
                   next_retry_at = %(next_retry_at)s, completed_at = %(completed_at)s, updated_at = now()
             WHERE analysis_id = %(analysis_id)s
            """,
            {**values, "analysis_id": analysis_id},
        )
    analysis_id = str(analysis_id)
    work.status = status
    summary: dict[str, Any] = {"analysis_id": analysis_id, "status": status, "attempts": attempts,
                               "units_inserted": 0, "embeddings_inserted": 0, "events_created": 0,
                               "comparisons_inserted": 0, "memberships_inserted": 0, "events_superseded": 0}
    if plan is None or not plan.units:
        return summary

    # Text units (immutable input) and their vectors.
    for unit in plan.units:
        if unit.key in work.unit_ids:
            continue
        row = conn.execute(
            """
            INSERT INTO text_unit (revision_id, profile_id, unit_kind, ordinal, input_text, input_hash, location)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (revision_id, profile_id, unit_kind, ordinal) DO NOTHING
            RETURNING unit_id
            """,
            (subject.revision_id, profile_id, unit.kind, unit.ordinal, unit.text, unit.input_hash,
             Jsonb(unit.location)),
        ).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT unit_id, input_hash FROM text_unit WHERE revision_id = %s AND profile_id = %s "
                "AND unit_kind = %s AND ordinal = %s",
                (subject.revision_id, profile_id, unit.kind, unit.ordinal),
            ).fetchone()
            if row["input_hash"] != unit.input_hash:
                raise PersistenceError(f"stored text unit {unit.kind}/{unit.ordinal} differs from the recomputed one")
        else:
            summary["units_inserted"] += 1
        work.unit_ids[unit.key] = str(row["unit_id"])

    for unit in plan.units:
        unit_id = work.unit_ids[unit.key]
        vector = work.vectors.get(unit.key)
        if vector is not None and unit.key not in work.stored_vector_keys:
            inserted = conn.execute(
                "INSERT INTO embedding (unit_id, embedding) VALUES (%s, %s::vector) "
                "ON CONFLICT (unit_id) DO NOTHING RETURNING unit_id",
                (unit_id, vector_literal(vector)),
            ).fetchone()
            summary["embeddings_inserted"] += 1 if inserted else 0
            conn.execute("UPDATE text_unit SET embedding_status = 'completed', last_error = NULL "
                         "WHERE unit_id = %s AND embedding_status <> 'completed'", (unit_id,))
        elif vector is None and work.embedding_error:
            conn.execute(
                """
                UPDATE text_unit SET retry_count = retry_count + 1, last_error = %s,
                       embedding_status = CASE WHEN retry_count + 1 >= %s THEN 'failed' ELSE 'pending' END
                 WHERE unit_id = %s AND embedding_status <> 'completed'
                """,
                (work.embedding_error[:500], max_embedding_attempts or max_attempts, unit_id),
            )

    # Retrieval scores: the latest attempt's shortlist replaces the previous one.
    conn.execute("DELETE FROM retrieval_candidate WHERE analysis_id = %s", (analysis_id,))
    for hit in work.hits:
        incoming_id = work.unit_ids.get(hit.incoming_unit_key)
        cand = hit.candidate
        if incoming_id is None or incoming_id == cand.unit_id:
            continue
        conn.execute(
            """
            INSERT INTO retrieval_candidate (analysis_id, incoming_unit_id, candidate_unit_id, candidate_revision_id,
                semantic_rank, semantic_similarity, lexical_rank, lexical_score, fused_score, fused_rank,
                candidate_event_ids, shortlisted, not_compared_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::uuid[], %s, %s)
            ON CONFLICT (analysis_id, incoming_unit_id, candidate_unit_id) DO NOTHING
            """,
            (analysis_id, incoming_id, cand.unit_id, cand.revision_id, cand.semantic_rank, cand.semantic_similarity,
             cand.lexical_rank, cand.lexical_score, cand.fused_score, hit.fused_rank, cand.event_ids,
             hit.shortlisted, hit.not_compared_reason),
        )

    # New provisional events.
    event_ids: dict[str, str] = {}
    for draft in work.new_events:
        row = conn.execute(
            """
            INSERT INTO news_event (is_sample, profile_id, pipeline_version, title, event_facts, flags,
                                    first_source_time, last_source_time, origin_revision_id, origin_analysis_id,
                                    origin_segment_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (origin_analysis_id, origin_segment_key) DO NOTHING
            RETURNING event_id
            """,
            (subject.is_sample, profile_id, pipeline_version, draft.title[:500], Jsonb(draft.facts),
             Jsonb(draft.flags), draft.source_time, draft.source_time, subject.revision_id, analysis_id,
             draft.segment_key),
        ).fetchone()
        if row is None:
            row = conn.execute("SELECT event_id FROM news_event WHERE origin_analysis_id = %s "
                               "AND origin_segment_key = %s", (analysis_id, draft.segment_key)).fetchone()
        else:
            summary["events_created"] += 1
        event_ids[draft.key] = str(row["event_id"])

    # Comparison decisions (never rewritten; a replay finds the existing row).
    comparison_ids: dict[int, str] = {}
    for judgment in work.judgments:
        row = conn.execute(
            """
            INSERT INTO event_comparison (analysis_id, revision_id, segment_key, candidate_event_id,
                candidate_revision_ids, label, method, incoming_unit_ids, candidate_unit_ids, matching_facts,
                differences, evidence, explanation, insufficient_context, uncertainty, validation_notes, model,
                prompt_version, usage)
            VALUES (%s, %s, %s, %s, %s::uuid[], %s, %s, %s::uuid[], %s::uuid[], %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s)
            ON CONFLICT (analysis_id, segment_key, candidate_event_id) DO NOTHING
            RETURNING comparison_id
            """,
            (analysis_id, subject.revision_id, judgment.segment_key, judgment.event_id,
             judgment.candidate_revision_ids, judgment.label, judgment.method,
             [work.unit_ids[k] for k in judgment.incoming_unit_keys if k in work.unit_ids],
             judgment.candidate_unit_ids, Jsonb(judgment.matching_facts), Jsonb(judgment.differences),
             Jsonb(judgment.evidence), judgment.explanation, judgment.insufficient_context, judgment.uncertainty,
             Jsonb(judgment.validation_notes), judgment.model, judgment.prompt_version, Jsonb(judgment.usage)),
        ).fetchone()
        if row is None:
            row = conn.execute("SELECT comparison_id FROM event_comparison WHERE analysis_id = %s "
                               "AND segment_key = %s AND candidate_event_id = %s",
                               (analysis_id, judgment.segment_key, judgment.event_id)).fetchone()
        else:
            summary["comparisons_inserted"] += 1
        judgment.persisted_id = str(row["comparison_id"])
        comparison_ids[id(judgment)] = judgment.persisted_id

    # Memberships, one per (event, revision), with their supporting units.
    touched: set[str] = set()
    for membership in work.memberships:
        event_id = event_ids.get(membership.event_ref, membership.event_ref)
        comparison_id = None
        if membership.judgment is not None:
            comparison_id = membership.judgment.persisted_id or comparison_ids.get(id(membership.judgment))
        row = conn.execute(
            """
            INSERT INTO event_membership (event_id, revision_id, document_id, analysis_id, segment_key, relation,
                                          state, comparison_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id, revision_id) DO NOTHING
            RETURNING membership_id
            """,
            (event_id, subject.revision_id, subject.document_id, analysis_id, membership.segment_key,
             membership.relation, membership.state, comparison_id),
        ).fetchone()
        if row is None:
            row = conn.execute("SELECT membership_id FROM event_membership WHERE event_id = %s AND revision_id = %s",
                               (event_id, subject.revision_id)).fetchone()
        else:
            summary["memberships_inserted"] += 1
        for key in membership.unit_keys:
            if key in work.unit_ids:
                conn.execute("INSERT INTO event_membership_unit (membership_id, unit_id) VALUES (%s, %s) "
                             "ON CONFLICT DO NOTHING", (row["membership_id"], work.unit_ids[key]))
        touched.add(event_id)

    for old_event_id, replacement in work.supersede:
        new_id = event_ids.get(replacement, replacement)
        updated = conn.execute(
            "UPDATE news_event SET status = 'superseded', superseded_by = %s, updated_at = now() "
            "WHERE event_id = %s AND status <> 'superseded' RETURNING event_id",
            (new_id, old_event_id),
        ).fetchone()
        summary["events_superseded"] += 1 if updated else 0
        touched.add(new_id)

    if touched:
        conn.execute(
            f"""
            UPDATE news_event e
               SET first_source_time = x.first_time, last_source_time = x.last_time, updated_at = now(),
                   status = CASE WHEN e.status = 'superseded' THEN e.status
                                 WHEN x.documents >= 2 THEN 'corroborated' ELSE 'provisional' END
              FROM (SELECT m.event_id, min({SOURCE_TIME}) AS first_time, max({SOURCE_TIME}) AS last_time,
                           count(DISTINCT m.document_id) AS documents
                      FROM event_membership m
                      JOIN document_revision r ON r.revision_id = m.revision_id
                      LEFT JOIN sec_filing_details s ON s.revision_id = m.revision_id
                     WHERE m.event_id = ANY(%s::uuid[]) AND m.state = 'active'
                     GROUP BY m.event_id) x
             WHERE e.event_id = x.event_id
            """,
            (list(touched),),
        )
    summary["event_ids"] = sorted(touched)
    return summary


# ----------------------------------------------------------------------------- maintenance queries
def select_unanalyzed(conn: Connection, *, profile_id: str, pipeline_version: str, is_sample: bool,
                      limit: int) -> list[str]:
    """Current revisions with no analysis in this space, oldest source time first."""
    rows = conn.execute(
        f"""
        SELECT r.revision_id
          FROM document d
          JOIN document_revision r ON r.revision_id = d.current_revision_id
          LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
         WHERE d.is_sample = %s
           AND NOT EXISTS (SELECT 1 FROM revision_analysis a WHERE a.revision_id = r.revision_id
                             AND a.profile_id = %s AND a.pipeline_version = %s)
         ORDER BY {SOURCE_TIME}, r.revision_id
         LIMIT %s
        """,
        (is_sample, profile_id, pipeline_version, limit),
    ).fetchall()
    return [str(r["revision_id"]) for r in rows]


def select_retry(conn: Connection, *, profile_id: str, pipeline_version: str, is_sample: bool, limit: int,
                 now: datetime, include_not_due: bool = False, revision_ids: list[str] | None = None) -> list[str]:
    """Pending and partial analyses that are due, oldest source time first."""
    rows = conn.execute(
        f"""
        SELECT a.revision_id
          FROM revision_analysis a
          JOIN document d ON d.document_id = a.document_id
          JOIN document_revision r ON r.revision_id = a.revision_id
          LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
         WHERE a.profile_id = %s AND a.pipeline_version = %s AND d.is_sample = %s
           AND a.status IN ('pending', 'partial')
           AND (%s OR a.next_retry_at IS NULL OR a.next_retry_at <= %s)
           AND (%s::uuid[] IS NULL OR a.revision_id = ANY(%s::uuid[]))
         ORDER BY {SOURCE_TIME}, a.revision_id
         LIMIT %s
        """,
        (profile_id, pipeline_version, is_sample, include_not_due, now, revision_ids, revision_ids, limit),
    ).fetchall()
    return [str(r["revision_id"]) for r in rows]


def status_summary(conn: Connection, *, profile_id: str, pipeline_version: str, is_sample: bool) -> dict[str, Any]:
    space = (profile_id, pipeline_version)
    analyses = {r["status"]: int(r["n"]) for r in conn.execute(
        """
        SELECT COALESCE(a.status, 'not_analyzed') AS status, count(*) AS n
          FROM document d
          LEFT JOIN revision_analysis a ON a.revision_id = d.current_revision_id
                                       AND a.profile_id = %s AND a.pipeline_version = %s
         WHERE d.is_sample = %s AND d.current_revision_id IS NOT NULL
         GROUP BY 1 ORDER BY 1
        """,
        (*space, is_sample),
    )}
    units = conn.execute(
        """
        SELECT count(*) AS units, count(*) FILTER (WHERE t.embedding_status = 'completed') AS embedded,
               count(*) FILTER (WHERE t.embedding_status IN ('pending', 'failed')) AS embedding_work
          FROM text_unit t JOIN document_revision r ON r.revision_id = t.revision_id
          JOIN document d ON d.document_id = r.document_id
         WHERE t.profile_id = %s AND d.is_sample = %s
        """,
        (profile_id, is_sample),
    ).fetchone()
    events = {r["status"]: int(r["n"]) for r in conn.execute(
        "SELECT status, count(*) AS n FROM news_event WHERE profile_id = %s AND pipeline_version = %s "
        "AND is_sample = %s GROUP BY 1", (*space, is_sample))}
    relations = {f"{r['relation']}:{r['state']}": int(r["n"]) for r in conn.execute(
        """
        SELECT m.relation, m.state, count(*) AS n
          FROM event_membership m JOIN news_event e ON e.event_id = m.event_id
         WHERE e.profile_id = %s AND e.pipeline_version = %s AND e.is_sample = %s GROUP BY 1, 2
        """,
        (*space, is_sample))}
    coverage = conn.execute(
        """
        SELECT count(*) FILTER (WHERE (a.index_coverage->>'complete')::boolean IS FALSE) AS partial_coverage,
               count(*) FILTER (WHERE (a.index_coverage->>'complete')::boolean IS TRUE) AS complete_coverage
          FROM revision_analysis a JOIN document d ON d.document_id = a.document_id
         WHERE a.profile_id = %s AND a.pipeline_version = %s AND d.is_sample = %s
        """,
        (*space, is_sample),
    ).fetchone()
    comparisons = {f"{r['method']}:{r['label']}": int(r["n"]) for r in conn.execute(
        """
        SELECT c.method, c.label, count(*) AS n
          FROM event_comparison c JOIN revision_analysis a ON a.analysis_id = c.analysis_id
          JOIN document d ON d.document_id = a.document_id
         WHERE a.profile_id = %s AND a.pipeline_version = %s AND d.is_sample = %s GROUP BY 1, 2
        """,
        (*space, is_sample))}
    return {"current_revisions_by_analysis_status": analyses, "text_units": dict(units),
            "events_by_status": events, "memberships": relations, "comparisons": comparisons,
            "analyses_with_index_coverage": dict(coverage)}


def inspect_revision(conn: Connection, revision_id: str) -> dict[str, Any]:
    analyses = conn.execute(
        """
        SELECT a.*, p.model_name, p.model_version, p.dimensions
          FROM revision_analysis a JOIN embedding_profile p ON p.profile_id = a.profile_id
         WHERE a.revision_id = %s ORDER BY a.created_at
        """,
        (revision_id,),
    ).fetchall()
    units = conn.execute(
        """
        SELECT t.unit_id, t.profile_id, t.unit_kind, t.ordinal, t.embedding_status, t.retry_count, t.last_error,
               t.location->>'segment_key' AS segment_key, t.location->>'section' AS section,
               (t.location->>'token_count')::int AS tokens, left(t.input_text, 160) AS text_preview,
               e.unit_id IS NOT NULL AS has_vector
          FROM text_unit t LEFT JOIN embedding e ON e.unit_id = t.unit_id
         WHERE t.revision_id = %s ORDER BY t.profile_id, t.ordinal
        """,
        (revision_id,),
    ).fetchall()
    candidates = conn.execute(
        """
        SELECT rc.analysis_id, rc.incoming_unit_id, rc.candidate_unit_id, rc.candidate_revision_id,
               rc.semantic_rank, round(rc.semantic_similarity::numeric, 4) AS semantic_similarity,
               rc.lexical_rank, round(rc.lexical_score::numeric, 4) AS lexical_score,
               round(rc.fused_score::numeric, 5) AS fused_score, rc.fused_rank, rc.shortlisted,
               rc.not_compared_reason, rc.candidate_event_ids, left(r.title, 100) AS candidate_title
          FROM retrieval_candidate rc
          JOIN revision_analysis a ON a.analysis_id = rc.analysis_id
          JOIN document_revision r ON r.revision_id = rc.candidate_revision_id
         WHERE a.revision_id = %s ORDER BY rc.fused_rank, rc.fused_score DESC LIMIT 60
        """,
        (revision_id,),
    ).fetchall()
    comparisons = conn.execute(
        """
        SELECT c.comparison_id, c.segment_key, c.candidate_event_id, c.label, c.method, c.explanation,
               c.matching_facts, c.differences, c.evidence, c.insufficient_context, c.uncertainty,
               c.validation_notes, c.model, c.prompt_version, c.usage
          FROM event_comparison c WHERE c.revision_id = %s ORDER BY c.created_at
        """,
        (revision_id,),
    ).fetchall()
    memberships = conn.execute(
        """
        SELECT m.event_id, m.segment_key, m.relation, m.state, e.status AS event_status, e.title,
               e.superseded_by, (SELECT count(DISTINCT m2.document_id) FROM event_membership m2
                                  WHERE m2.event_id = m.event_id AND m2.state = 'active') AS event_documents,
               ARRAY(SELECT mu.unit_id FROM event_membership_unit mu
                      WHERE mu.membership_id = m.membership_id) AS supporting_units
          FROM event_membership m JOIN news_event e ON e.event_id = m.event_id
         WHERE m.revision_id = %s ORDER BY m.created_at
        """,
        (revision_id,),
    ).fetchall()
    return {"revision_id": revision_id, "analyses": analyses, "text_units": units,
            "retrieval_candidates": candidates, "comparisons": comparisons, "memberships": memberships}
