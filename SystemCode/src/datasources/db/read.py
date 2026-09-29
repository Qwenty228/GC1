"""Read-only queries behind ``/api/articles`` and ``/api/status``. Nothing here writes or calls a model.

An article is a stored source record (``document``) shown as its CURRENT revision:
the original headline, the original publisher (news) or issuer (SEC), the
source's own times, the excerpt the source supplied (Alpaca ``summary``; SEC
records have none) and the original link. Older revisions stay in the database
and are counted, never hidden or rewritten. Event links and grouping status
come from the semantic-grouping tables when they exist (migration 002).
"""

from __future__ import annotations

from typing import Any

from psycopg import Connection

from ..similarity.store import SOURCE_TIME
from ..summary.conflicts import safe_url

RECORD_TYPES = {"news": "news_article", "sec": "sec_filing"}
EXCERPT_CHARS = 600


def _has_relation(conn: Connection, name: str) -> bool:
    return bool(conn.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (f"public.{name}",)).fetchone()["ok"])


def _excerpt(text: str | None) -> tuple[str | None, bool]:
    text = " ".join((text or "").split())
    if not text:
        return None, False
    if len(text) <= EXCERPT_CHARS:
        return text, False
    cut = text[:EXCERPT_CHARS].rsplit(" ", 1)[0]
    return cut.rstrip(" ,;:."), True


_ARTICLE_FILTER = """
     WHERE (%(sample)s OR NOT d.is_sample)
       AND (%(record_type)s::text IS NULL OR d.record_type = %(record_type)s::text)
       AND (%(symbol)s::text IS NULL OR EXISTS (
            SELECT 1 FROM revision_company rc JOIN company_symbol cs ON cs.company_id = rc.company_id
             WHERE rc.revision_id = d.current_revision_id AND cs.symbol = %(symbol)s::text))
"""


def list_articles(conn: Connection, *, include_sample: bool, limit: int, offset: int, source: str = "all",
                  symbol: str | None = None) -> dict[str, Any]:
    """Stored records newest first (by the source's own time), with the total matching the filters."""
    params = {"sample": include_sample, "record_type": RECORD_TYPES.get(source), "symbol": symbol,
              "limit": limit, "offset": offset}
    grouped = _has_relation(conn, "current_revision_analysis")
    analysis_column = "a.analysis_status" if grouped else "NULL::text"
    analysis_join = "LEFT JOIN current_revision_analysis a ON a.document_id = d.document_id" if grouped else ""
    rows = conn.execute(
        f"""
        SELECT d.document_id, d.record_type, d.is_sample, pv.code AS provider, r.revision_id, r.title,
               r.title_generated, r.summary_text, r.original_url, r.published_at, r.source_updated_at,
               r.retrieved_at, r.content_status, r.content_scope, {SOURCE_TIME} AS source_time,
               pub.name AS publisher, co.name AS issuer, s.form, s.accession_number, s.filing_date, s.item_codes,
               s.primary_document_url, s.filing_index_url, {analysis_column} AS analysis_status,
               (SELECT count(*) FROM document_revision x WHERE x.document_id = d.document_id) AS revisions
          FROM document d
          JOIN document_revision r ON r.revision_id = d.current_revision_id
          JOIN provider pv ON pv.provider_id = d.provider_id
          LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
          LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
          LEFT JOIN company co ON co.company_id = s.issuer_company_id
          {analysis_join}
          {_ARTICLE_FILTER}
         ORDER BY source_time DESC NULLS LAST, d.document_id
         LIMIT %(limit)s OFFSET %(offset)s
        """,
        params,
    ).fetchall()
    total = conn.execute(f"SELECT count(*) AS n FROM document d {_ARTICLE_FILTER} AND d.current_revision_id "
                         "IS NOT NULL", params).fetchone()["n"]
    events: dict[str, list[dict[str, str]]] = {}
    if rows and _has_relation(conn, "current_event_membership"):
        for m in conn.execute(
            """
            SELECT m.revision_id::text AS revision_id, m.event_id::text AS event_id, e.title
              FROM current_event_membership m JOIN news_event e ON e.event_id = m.event_id
             WHERE m.revision_id = ANY(%s::uuid[]) ORDER BY e.last_source_time DESC NULLS LAST, m.event_id
            """,
            ([str(r["revision_id"]) for r in rows],),
        ):
            events.setdefault(m["revision_id"], []).append({"event_id": m["event_id"], "title": m["title"]})
    articles = []
    for r in rows:
        excerpt, truncated = _excerpt(r["summary_text"]) if r["record_type"] == "news_article" else (None, False)
        articles.append({
            "document_id": str(r["document_id"]), "revision_id": str(r["revision_id"]),
            "record_type": r["record_type"], "provider": r["provider"], "is_sample": r["is_sample"],
            "title": r["title"], "title_generated": r["title_generated"],
            "publisher": r["publisher"], "issuer": r["issuer"],
            "published_at": r["published_at"], "source_updated_at": r["source_updated_at"],
            "retrieved_at": r["retrieved_at"], "source_time": r["source_time"],
            "excerpt": excerpt, "excerpt_truncated": truncated,
            "url": safe_url(r["original_url"] or r["primary_document_url"]),
            "filing_index_url": safe_url(r["filing_index_url"]),
            "form": r["form"], "accession_number": r["accession_number"], "filing_date": r["filing_date"],
            "item_codes": list(r["item_codes"] or []),
            "content_status": r["content_status"], "content_scope": r["content_scope"],
            "revisions": int(r["revisions"]), "grouping_status": r["analysis_status"],
            "events": events.get(str(r["revision_id"]), []),
        })
    return {"articles": articles, "total": int(total), "limit": limit, "offset": offset}


def stored_counts(conn: Connection, *, include_sample: bool) -> dict[str, Any]:
    """How much is stored, so empty views can say why they are empty (e.g. records stored but not grouped)."""
    row = conn.execute(
        """
        SELECT count(*) FILTER (WHERE record_type = 'news_article') AS news,
               count(*) FILTER (WHERE record_type = 'sec_filing') AS sec
          FROM document WHERE current_revision_id IS NOT NULL AND (%(sample)s OR NOT is_sample)
        """,
        {"sample": include_sample},
    ).fetchone()
    counts: dict[str, Any] = {"news": int(row["news"]), "sec": int(row["sec"]),
                              "articles": int(row["news"]) + int(row["sec"]), "events": None, "grouping": None}
    if _has_relation(conn, "current_event_membership"):
        counts["events"] = int(conn.execute(
            """
            SELECT count(*) AS n FROM news_event e
             WHERE e.status <> 'superseded' AND (%(sample)s OR NOT e.is_sample)
               AND EXISTS (SELECT 1 FROM current_event_membership m WHERE m.event_id = e.event_id)
            """,
            {"sample": include_sample},
        ).fetchone()["n"])
    if _has_relation(conn, "current_revision_analysis"):
        counts["grouping"] = {r["analysis_status"]: int(r["n"]) for r in conn.execute(
            "SELECT analysis_status, count(*) AS n FROM current_revision_analysis "
            "WHERE revision_id IS NOT NULL AND (%(sample)s OR NOT is_sample) GROUP BY 1",
            {"sample": include_sample},
        )}
    return counts
