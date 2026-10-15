"""Stage 2 evaluation: event summaries, judged by people.

A summary has no single right answer, so this runner gives no score of its own. For every event it saves
the summary AND everything the model was given (sources, passages, review decisions), so a person can judge
it with the rubric in evaluation/summarization/README.md. It also runs mechanical checks that point the
reader at likely problems, and writes a score sheet to fill in.

Experiment files (evaluation/summarization/experiments/*.py) call `run_experiment(...)`:

    1. one throwaway database `stock_news_eval_..._test`; ALL test articles go in, in test-number order,
       through the real ingestion service with grouping on and summaries off
    2. grouping: by default `AnswerKeyComparator` answers the compare step from the test file names, so
       every event holds exactly the articles it should and grouping mistakes cannot be blamed on the
       summary; an experiment may plug in a real comparison model instead (end-to-end)
    3. conflict reviews: production holds an event's summary until a person resolves its conflict review;
       the answer key resolves each one (confirmed_conflict / different_events / not_a_conflict)
    4. one summary per event from its final evidence, through the production generate -> validate ->
       publish steps, with up to `attempts` tries (production retries a summary that fails validation)
    5. drop the database (unless keep_db)
"""

from __future__ import annotations

import csv
import html
import json
import re
import tempfile
import time
from dataclasses import replace
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable

from ..db.evaluation import FIXTURE_DIR, Fixture, FixtureType, read_fixtures
from ..db.repository import ALPACA_NAMESPACE
from . import metrics
from .consolidation import EXPECTED_LABEL, PREDICTIONS_SQL, RECORDED_SETTINGS, BatchDatabase, _predicted_label, \
    to_jsonable

RESULTS_DIR = Path(__file__).resolve().parents[3] / "evaluation" / "summarization" / "results"
REVIEWER = "eval-answer-key"
# The human score sheet: one row per event. 0-2 scales are easier for two raters to agree on than 1-5.
RUBRIC = {
    "faithful": "0-2  2 every statement is supported by its quotes; 1 a small overstatement; 0 any wrong or "
                "invented fact",
    "coverage": "0-2  2 the main fact AND every new fact from update articles; 1 main fact only; "
                "0 misses the main fact",
    "conflict": "0-2  only for events with a conflict article: 2 both claims, each attributed, no winner; "
                "1 mentioned but unattributed; 0 the false claim stated as fact. Blank otherwise",
    "clarity": "0-2  2 clear and concise; 1 understandable but clumsy or padded; 0 confusing",
    "acceptable": "y/n  would you publish it on the site as it is?",
}
SCORE_COLUMNS = ["event", "sources", "files", "auto_valid", *RUBRIC, "notes"]

EVENTS_SQL = """
SELECT e.event_id::text AS event_id, e.title
  FROM news_event e
 WHERE e.status <> 'superseded'
   AND EXISTS (SELECT 1 FROM current_event_membership m WHERE m.event_id = e.event_id)
 ORDER BY e.first_source_time NULLS LAST, e.event_id
"""

OPEN_REVIEWS_SQL = """
SELECT v.review_id::text AS review_id, c.candidate_event_id::text AS event_id, d.source_record_id AS article_id
  FROM event_conflict_review v
  JOIN event_comparison c ON c.comparison_id = v.comparison_id
  JOIN document_revision r ON r.revision_id = c.revision_id
  JOIN document d ON d.document_id = r.document_id
 WHERE v.status IN ('open', 'in_review')
 ORDER BY v.created_at, v.review_id
"""

DECISIONS_SQL = """
SELECT c.candidate_event_id::text AS event_id, d.source_record_id AS article_id, v.resolution, v.reviewer_notes
  FROM event_conflict_review v
  JOIN event_comparison c ON c.comparison_id = v.comparison_id
  JOIN document_revision r ON r.revision_id = c.revision_id
  JOIN document d ON d.document_id = r.document_id
 WHERE v.status = 'resolved'
 ORDER BY v.created_at, v.review_id
"""

# An event's member articles, its first (origin, else earliest) first: that one's group is the event's group.
EVENT_MEMBERS_SQL = """
SELECT d.source_record_id AS article_id
  FROM current_event_membership m JOIN document d ON d.document_id = m.document_id
 WHERE m.event_id = %s
 ORDER BY (m.relation = 'origin') DESC, m.source_time NULLS LAST, m.revision_id
"""


# ----------------------------------------------------------------------------- test articles
def article_text(article: dict[str, Any]) -> str:
    """Plain text of an Alpaca test article (headline, summary, body without HTML)."""
    parts = [article.get("headline") or "", article.get("summary") or "", article.get("content") or ""]
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", " ".join(parts)))).strip()


_NUMBER = re.compile(r"\d+(?:[.,]\d+)*")
_SENTENCE = re.compile(r"(?<=[.!?])\s+")


def changed_claims(new: str, earlier: list[str], *, limit: int = 2) -> list[dict[str, str]]:
    """The conflicting claims of a conflict test article, as comparison `differences` (kind conflict).

    A conflict article usually changes a number. Each of its sentences holding a number that no earlier article
    of the group states is paired with the most similar earlier sentence: the claim and what it contradicts, the
    way a good comparison model reports them. [] when the change is not a number (about 10 of the 25)."""
    seen = {n.replace(",", "") for text in earlier for n in _NUMBER.findall(text)}
    old = [s for text in earlier for s in _SENTENCE.split(text) if s.strip()]
    pairs = []
    for sentence in _SENTENCE.split(new):
        if not old or not {n.replace(",", "") for n in _NUMBER.findall(sentence)} - seen:
            continue
        best = max(old, key=lambda o: SequenceMatcher(None, o, sentence).ratio())
        ratio = SequenceMatcher(None, best, sentence).ratio()
        if ratio > 0.45:  # below that the "closest" sentence is about something else
            pairs.append((ratio, sentence, best))
    return [{"aspect": "changed claim", "incoming": s[:300], "candidate": b[:300], "kind": "conflict"}
            for _, s, b in sorted(pairs, key=lambda p: -p[0])[:limit]]


class AnswerKeyComparator:
    """The compare step answered from the answer key, so each event holds exactly its test group.

    The runner calls `before_article(fixture, database)` before feeding each article. A candidate event belongs
    to the group of its first article; the label is the one the incoming file's name expects. Retrieval still
    decides which events are candidates, as in production: an article it misses opens its own event."""

    name = "answer_key"
    model = "answer-key"

    def __init__(self, fixtures: dict[str, Fixture], texts: dict[str, str]):
        self.fixtures, self.texts = fixtures, texts  # by article id
        self.current: Fixture | None = None
        self.current_id: str | None = None
        self.database = None

    def before_article(self, fixture: Fixture, article_id: str, database) -> None:
        self.current, self.current_id, self.database = fixture, article_id, database

    def _event_articles(self, event_id: str) -> list[str]:
        """The event's member article ids, its first article first."""
        with self.database.connection() as conn:
            rows = conn.execute(EVENT_MEMBERS_SQL, (event_id,)).fetchall()
        return [str(r["article_id"]) for r in rows]

    def compare(self, request):
        from ..similarity.comparison import ComparisonCall, build_output

        incoming = self.current

        def decide(segment, event):
            members = self._event_articles(event.event_id)
            origin = self.fixtures.get(members[0]) if members else None
            if incoming is None or origin is None or origin.expected_group != incoming.expected_group:
                return {"label": "different_event", "explanation": "answer key: another test group"}
            label = EXPECTED_LABEL[incoming.fixture_type]
            if incoming.fixture_type in (FixtureType.ORIGINAL, FixtureType.DIFFERENT):
                label = "same_event_repeated"  # same group but not a follow-up: should not happen
            decision: dict[str, Any] = {"label": label,
                                        "explanation": f"answer key: {incoming.fixture_type.value} article of "
                                                       f"group {incoming.expected_group}"}
            if label == "same_event_conflict":
                diffs = changed_claims(self.texts.get(self.current_id or "", ""),
                                       [self.texts.get(a, "") for a in members])
                decision["differences"] = diffs
                decision.update(_evidence_for(segment, event, diffs))
            return decision

        return ComparisonCall(build_output(request, decide), self.model, usage={"calls": 0}, stop_reason="answer_key")


def _evidence_for(segment, event, diffs: list[dict[str, str]]) -> dict[str, Any]:
    """Point the judgment at the unit and passage that hold the changed claim (production's comparator would),
    so the summary's both-sides citation check looks at the right passages."""
    from ..similarity.comparison import _fold

    if not diffs:
        return {}
    unit = next((u for u in segment.units if _fold(diffs[0]["incoming"]) in _fold(u.text)), None)
    passage = next((p for p in event.passages if _fold(diffs[0]["candidate"]) in _fold(p.text)), None)
    out: dict[str, Any] = {}
    if unit is not None:
        out.update(incoming_unit_refs=[unit.ref],
                   incoming_evidence=[{"ref": unit.ref, "quote": " ".join(unit.text.split()[:6])}])
    if passage is not None:
        out.update(candidate_passage_refs=[passage.ref],
                   candidate_evidence=[{"ref": passage.ref, "quote": " ".join(passage.text.split()[:6])}])
    return out


def review_decision(conflicting: Fixture | None, event_group: int | None) -> tuple[str, str]:
    """What a reviewer with the answer key decides for a flagged conflict."""
    if conflicting is None:
        return "not_a_conflict", "answer key: not a test article"
    name = conflicting.filename
    if event_group is not None and conflicting.expected_group != event_group:
        return "different_events", f"answer key: {name} is group {conflicting.expected_group}, not {event_group}"
    if conflicting.fixture_type == FixtureType.CONFLICT:
        return "confirmed_conflict", f"answer key: {name} is a conflict test article"
    return "not_a_conflict", f"answer key: {name} is a {conflicting.fixture_type.value} test article"


# ----------------------------------------------------------------------------- the run
def _resolve_reviews(db, fixtures: dict[str, Fixture], progress: Callable[[str], None]) -> int:
    """Open a review for every flagged conflict and decide it from the answer key, until none is open."""
    from ..summary import review, store

    decided = 0
    for _ in range(5):  # a different_events decision can create a new event: look again
        with db.transaction() as conn:
            for row in conn.execute(EVENTS_SQL).fetchall():
                store.ensure_conflict_reviews(conn, row["event_id"])
            open_reviews = conn.execute(OPEN_REVIEWS_SQL).fetchall()
        if not open_reviews:
            break
        for r in open_reviews:
            with db.connection() as conn:
                origin = conn.execute(EVENT_MEMBERS_SQL, (r["event_id"],)).fetchone()
            group = fixtures[str(origin["article_id"])].expected_group if origin and str(
                origin["article_id"]) in fixtures else None
            decision, why = review_decision(fixtures.get(str(r["article_id"])), group)
            review.decide(db, r["review_id"], decision=decision, reviewer=REVIEWER, explanation=why, language="en")
            decided += 1
    progress(f"conflict reviews decided from the answer key: {decided}")
    return decided


def _statements(output: dict[str, Any] | None, passages: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """The model's statements as written (also when validation rejected them), each quote checked."""
    from ..similarity.comparison import _fold

    if not isinstance(output, dict):
        return []
    raw: list[tuple[str, Any]] = [("headline", output.get("headline"))]
    raw += [("overview", s) for s in output.get("overview") or [] if isinstance(output.get("overview"), list)]
    raw += [("key_point", s) for s in output.get("key_points") or [] if isinstance(output.get("key_points"), list)]
    out = []
    for kind, s in raw:
        if not isinstance(s, dict):
            continue
        cites = []
        for c in s.get("citations") or []:
            if not isinstance(c, dict):
                continue
            ref, quote = str(c.get("passage_ref", "")), str(c.get("quote", ""))
            passage = passages.get(ref)
            folded = _fold(html.unescape(quote.strip()))
            verified = bool(passage) and len(folded) >= 8 and folded in _fold(passage["text"])
            cites.append({"ref": ref, "source": passage["source"] if passage else None,
                          "file": passage["file"] if passage else None, "quote": quote, "verified": verified})
        out.append({"id": s.get("id"), "kind": kind, "text": s.get("text"), "citations": cites})
    return out


def _checks(members: list[dict[str, Any]], statements: list[dict[str, Any]], group: int | None) -> dict[str, Any]:
    """Mechanical hints for the human reader. Not a score: a summary can pass them all and still be wrong."""
    cited = {c["file"] for s in statements for c in s["citations"] if c["verified"] and c["file"]}
    files = [m["file"] for m in members]
    in_group = [m for m in members if m["group"] == group]
    update = [m["file"] for m in in_group if m["type"] == "update"]
    conflict = [m["file"] for m in in_group if m["type"] == "conflict"]
    citations = [c for s in statements for c in s["citations"]]
    return {"sources_cited": sum(f in cited for f in files), "sources": len(files),
            "not_cited": [f for f in files if f and f not in cited],
            "citations_verified": sum(c["verified"] for c in citations), "citations": len(citations),
            "update_cited": (any(f in cited for f in update) if update else None),
            "conflict_cited": (any(f in cited for f in conflict) if conflict else None)}


def _summarize_event(event_id: str, sctx, attempts: int, conn_factory, describe: Callable[[str], dict[str, Any]]
                     ) -> dict[str, Any]:
    from ..summary import evidence
    from ..summary.pipeline import process_event

    tries, task, started = [], None, time.monotonic()
    for _ in range(max(attempts, 1)):
        t0 = time.monotonic()
        task = process_event(event_id, sctx, refresh=True)
        tries.append({"result": task.result, "reason": task.reason, "validation_errors": task.validation_errors,
                      "error": task.error, "seconds": round(time.monotonic() - t0, 1),
                      "tokens": {"input": int(task.usage.get("input_tokens", 0) or 0),
                                 "output": int(task.usage.get("output_tokens", 0) or 0)}})
        if task.result not in ("invalid", "error"):
            break
    with conn_factory() as conn:
        rows = evidence.member_rows(conn, event_id)
    members = []
    for i, m in enumerate(rows, start=1):
        info = describe(str(m["document_id"]))
        members.append({"source": f"S{i}", **info, "relation": m["relation"], "publisher": m["source_name"],
                        "published": m["source_time"].isoformat(timespec="minutes") if m["source_time"] else None,
                        "title": m["title"], "revision_id": str(m["revision_id"])})
    by_revision = {m["revision_id"]: m for m in members}
    pack = task.pack
    passages = [{"ref": p.ref, "source": by_revision.get(p.revision_id, {}).get("source"),
                 "file": by_revision.get(p.revision_id, {}).get("file"), "relation": p.relation, "text": p.text}
                for p in (pack.passages if pack else [])]
    statements = _statements(task.output, {p["ref"]: p for p in passages})
    return {
        "event_id": event_id, "result": task.result, "reason": task.reason, "attempts": tries,
        "members": [{k: v for k, v in m.items() if k != "revision_id"} for m in members],
        "input": {"passages": passages, "chars": sum(len(p["text"]) for p in passages),
                  "limitations": list(pack.limitations) if pack else [],
                  "comparison_notes": [{"source": by_revision.get(c.revision_id, {}).get("source"),
                                        "label": c.label, "explanation": c.explanation,
                                        "differences": c.differences}
                                       for c in (pack.comparisons if pack else [])
                                       if c.label in ("same_event_update", "same_event_conflict")]},
        "summary": {"statements": statements,
                    "evidence_gaps": list((task.output or {}).get("evidence_gaps") or [])
                    if isinstance(task.output, dict) else [],
                    "raw_text": (task.output or {}).get("raw_text") if isinstance(task.output, dict) else None},
        "model": task.model,
        "seconds": round(time.monotonic() - started, 1),
        "tokens": {"input": sum(t["tokens"]["input"] for t in tries),
                   "output": sum(t["tokens"]["output"] for t in tries)},
    }


def run(name: str, summarizer, sim_settings, summary_settings, *, comparator=None, embedder=None,
        description: str = "", source: str | None = None, attempts: int = 2, limit_events: int | None = None,
        keep_database: bool = False, directory: Path | None = None,
        progress: Callable[[str], None] = print) -> dict[str, Any]:
    """Ingest every test article into one database, summarize every event, and return the report."""
    from ..ingest.cli import _process_alpaca_payload
    from ..ingest.service import IngestionService
    from ..ingest.settings import load_settings
    from ..similarity.embeddings import build_embedding_backend
    from ..similarity.pipeline import build_runtime
    from ..summary.pipeline import SummaryBudget, SummaryContext, SummaryRuntime
    from ..summary.settings import PROMPT_VERSION

    directory = directory or FIXTURE_DIR
    items = sorted(read_fixtures(directory), key=lambda f: f.test_number)
    articles = {f.test_number: json.loads((directory / f.filename).read_text(encoding="utf-8")) for f in items}
    by_article = {str(articles[f.test_number]["id"]): f for f in items}
    texts = {str(articles[f.test_number]["id"]): article_text(articles[f.test_number]) for f in items}
    comparator = comparator or AnswerKeyComparator(by_article, texts)
    sim_settings = replace(sim_settings, enabled=True)
    embedder = embedder or build_embedding_backend(sim_settings)
    summary_settings = summary_settings.with_overrides(enabled=True, model=getattr(summarizer, "model", None)
                                                       or summary_settings.model)
    started, clock = datetime.now(timezone.utc), time.monotonic()

    with BatchDatabase(f"{name}_sum", keep=keep_database) as db, tempfile.TemporaryDirectory(prefix="eval_") as root:
        analyzer = build_runtime(db, sim_settings, embedder=embedder, comparator=comparator)
        service = IngestionService(load_settings(demo=False).with_overrides(output_root=Path(root)),
                                   database=db, analyzer=analyzer)  # summarizer=None: nothing summarized inline
        progress(f"ingesting {len(items)} articles into {db.settings.dbname} (grouping: "
                 f"{getattr(comparator, 'model', None) or getattr(comparator, 'name', '?')})")
        for i, f in enumerate(items, start=1):
            article = articles[f.test_number]
            if hasattr(comparator, "before_article"):
                comparator.before_article(f, str(article["id"]), db)
            _process_alpaca_payload(service, article, source=directory / f.filename, restrict_symbols=False)
            if i % 25 == 0 or i == len(items):
                progress(f"  {i}/{len(items)} ingested ({time.monotonic() - clock:.0f}s)")
        decided = _resolve_reviews(db, by_article, progress)

        with db.connection() as conn:
            doc_article = {str(r["document_id"]): str(r["source_record_id"]) for r in conn.execute(
                "SELECT document_id, source_record_id FROM document WHERE source_namespace = %s",
                (ALPACA_NAMESPACE,)).fetchall()}
            prediction_rows = conn.execute(PREDICTIONS_SQL, (ALPACA_NAMESPACE,)).fetchall()
            events = conn.execute(EVENTS_SQL).fetchall()
            decision_rows = conn.execute(DECISIONS_SQL).fetchall()

        def describe(document_id: str) -> dict[str, Any]:
            f = by_article.get(doc_article.get(document_id, ""))
            return {"file": f.filename if f else None, "type": f.fixture_type.value if f else None,
                    "group": f.expected_group if f else None}

        # grouping that the summaries were built on (E1, E2, ... in article order)
        names: dict[str, str] = {}
        per_article: dict[str, list[dict[str, Any]]] = {}
        for row in prediction_rows:
            per_article.setdefault(str(row["article_id"]), []).append(dict(row))
        grouping, predictions = [], []
        for f in items:
            events_of, label = _predicted_label(per_article.get(str(articles[f.test_number]["id"]), []))
            expected = None if f.fixture_type == FixtureType.ORIGINAL else EXPECTED_LABEL[f.fixture_type]
            predictions.append(metrics.Prediction(f.test_number, 1, f.expected_group, expected, events_of, label))
            grouping.append({"file": f.filename, "expected_label": expected, "predicted_label": label,
                             "events": [names.setdefault(e, f"E{len(names) + 1}") for e in sorted(events_of)]})
        for e in events:
            names.setdefault(e["event_id"], f"E{len(names) + 1}")
        events = sorted(events, key=lambda e: int(names[e["event_id"]][1:]))  # E1, E2, ...: test order

        runtime = SummaryRuntime(summary_settings, summarizer)
        sctx = SummaryContext(db, runtime, SummaryBudget(10 ** 6))
        chosen = events[:limit_events] if limit_events else events
        progress(f"\nsummarizing {len(chosen)} of {len(events)} event(s) with "
                 f"{getattr(summarizer, 'model', None) or getattr(summarizer, 'name', '?')}")
        reviews_by_event: dict[str, list[dict[str, Any]]] = {}
        for r in decision_rows:
            f = by_article.get(str(r["article_id"]))
            reviews_by_event.setdefault(r["event_id"], []).append(
                {"file": f.filename if f else None, "decision": r["resolution"], "why": r["reviewer_notes"]})
        results = []
        for e in chosen:
            ev = _summarize_event(e["event_id"], sctx, attempts, db.connection, describe)
            groups = sorted({m["group"] for m in ev["members"] if m["group"] is not None})
            group = next((m["group"] for m in ev["members"] if m["relation"] == "origin"), groups[0] if groups else None)
            expected_files = sorted(f.filename for f in items if f.expected_group == group)
            have = {m["file"] for m in ev["members"]}
            ev = {"event": names[e["event_id"]], "title": e["title"], **ev,
                  "answer_key": {"group": group, "groups": groups, "pure": len(groups) <= 1,
                                 "missing": [f for f in expected_files if f not in have]},
                  "reviews": reviews_by_event.get(e["event_id"], [])}
            ev["checks"] = _checks(ev["members"], ev["summary"]["statements"], group)
            results.append(ev)
            headline = next((s["text"] for s in ev["summary"]["statements"] if s["kind"] == "headline"), "")
            progress(f"  {ev['event']:<4} {ev['result'] or '-':<10} {len(ev['members'])} source(s) "
                     f"{ev['seconds']:>6.1f}s  {str(headline)[:90]}")
            error = (ev["attempts"][-1].get("error") or {}) if ev["attempts"] else {}
            if error.get("kind") in ("connection", "configuration", "authentication", "model_not_found"):
                progress(f"stopping: the summary model is not reachable ({error.get('message')}). Is the server "
                         "running (`ollama serve`) and the model pulled? The partial run is saved as incomplete.")
                break
        database_name = db.settings.dbname

    errored = [g["file"] for g in grouping if str(g["predicted_label"]).startswith("error")]
    errored += [e["event"] for e in results if e["result"] == "error"]
    report = {
        "stage": "summarization",
        "experiment": {"name": name, "description": description, "file": source},
        "model": {"summarizer": getattr(summarizer, "name", type(summarizer).__name__),
                  "summary_model": getattr(summarizer, "model", None), "prompt_version": PROMPT_VERSION,
                  "comparator": getattr(comparator, "name", type(comparator).__name__),
                  "comparison_model": getattr(comparator, "model", None),
                  "embedder": to_jsonable(embedder.identity()) if hasattr(embedder, "identity") else str(embedder)},
        "settings": {**{k: getattr(sim_settings, k) for k in RECORDED_SETTINGS},
                     "summary_max_input_chars": summary_settings.max_input_chars,
                     "summary_max_passages": summary_settings.max_passages, "attempts": attempts},
        "started_at": started.isoformat(timespec="seconds"),
        "articles": len(items),
        "events_total": len(events),
        "events_summarized": len(results),
        "reviews_decided": decided,
        "incomplete": bool(errored) or len(results) < len(events),
        "metrics": summary_metrics(results),
        "grouping": {"pairwise": metrics.pairwise(predictions), "labels": metrics.labels(predictions),
                     "articles": grouping},
        "seconds": round(time.monotonic() - clock, 1),
        "database": database_name if keep_database else None,
        "rubric": RUBRIC,
        "events": results,
    }
    if report["incomplete"]:
        report["incomplete_reason"] = (f"{len(errored)} failure(s), e.g. {errored[0]}" if errored else
                                       f"only {len(results)} of {len(events)} events summarized (limit_events)")
    return report


def summary_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Mechanical totals. They say whether summaries were produced and traceable, not whether they are good."""
    results: dict[str, int] = {}
    for e in events:
        results[e["result"] or "none"] = results.get(e["result"] or "none", 0) + 1
    published = [e for e in events if e["result"] == "published"]
    multi = [e for e in published if len(e["members"]) > 1]
    cites = sum(e["checks"]["citations"] for e in events)

    def applicable(key: str) -> list[bool]:  # the events where the check applies (an update / conflict article)
        return [e["checks"][key] for e in published if e["checks"][key] is not None]

    def rate(values: list[bool]) -> float | None:
        return round(sum(values) / len(values), 3) if values else None

    return {
        "events": len(events), "results": results,
        "published_rate": round(len(published) / len(events), 3) if events else None,
        "first_try_published": sum(e["attempts"][0]["result"] == "published" for e in events if e["attempts"]),
        "statements_avg": round(sum(len(e["summary"]["statements"]) for e in published) / len(published), 2)
        if published else None,
        "citations_verified_rate": round(sum(e["checks"]["citations_verified"] for e in events) / cites, 3)
        if cites else None,
        "source_coverage": round(sum(e["checks"]["sources_cited"] / e["checks"]["sources"] for e in multi)
                                 / len(multi), 3) if multi else None,
        "update_cited_rate": rate(applicable("update_cited")), "update_events": len(applicable("update_cited")),
        "conflict_cited_rate": rate(applicable("conflict_cited")),
        "conflict_events": len(applicable("conflict_cited")),
        "tokens": {"input": sum(e["tokens"]["input"] for e in events),
                   "output": sum(e["tokens"]["output"] for e in events)},
        "seconds": round(sum(e["seconds"] for e in events), 1),
    }


def write_score_sheet(report: dict[str, Any], path: Path) -> None:
    """One row per event for a human rater (utf-8 with BOM so Excel opens it cleanly)."""
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=SCORE_COLUMNS)
        writer.writeheader()
        for e in report["events"]:
            published = e["result"] == "published"
            has_conflict = e["checks"]["conflict_cited"] is not None
            writer.writerow({"event": e["event"], "sources": len(e["members"]),
                             "files": " ".join(m["file"] or "?" for m in e["members"]),
                             "auto_valid": "y" if published else "n",
                             "conflict": "" if has_conflict else "n/a",
                             "acceptable": "" if published else "n",
                             "notes": "" if published else f"not published: {e['result']}"})


def read_scores(result_path: Path) -> dict[str, Any] | None:
    """Averages over every filled score sheet of a run (`<run>_scores*.csv`, one per rater)."""
    rows = []
    for sheet in sorted(result_path.parent.glob(f"{result_path.stem}_scores*.csv")):
        with sheet.open(encoding="utf-8-sig", newline="") as handle:
            sheet_rows = [r for r in csv.DictReader(handle) if (r.get("acceptable") or "").strip()]
        if any((r.get("faithful") or "").strip() for r in sheet_rows):  # else untouched: pre-filled rows only
            rows += sheet_rows
    if not rows:
        return None

    def mean(key: str) -> float | None:
        values = [float(r[key]) for r in rows if (r.get(key) or "").strip().replace(".", "", 1).isdigit()]
        return round(sum(values) / len(values), 2) if values else None

    acceptable = [r["acceptable"].strip().lower().startswith("y") for r in rows]
    return {"rated": len(rows), "acceptable_rate": round(sum(acceptable) / len(acceptable), 3),
            **{k: mean(k) for k in ("faithful", "coverage", "conflict", "clarity")}}


def run_experiment(name: str, build_summarizer: Callable[[Any], Any], *, build_comparator=None,
                   description: str = "", save: bool = True, keep_db: bool = False, attempts: int = 2,
                   limit_events: int | None = None, build_embedder=None, source: str | None = None) -> dict[str, Any]:
    """What a summarization experiment file's `__main__` block calls.

    build_summarizer   settings -> summary backend (`summarize(request)`), the model under test
    build_comparator   None = group by the answer key (recommended); or settings -> a comparison backend
    attempts           tries per event when a summary fails validation or errors (production retries)
    limit_events       summarize only the first N events (a quick check); None = all
    save               write evaluation/summarization/results/<date>_<time>_<name>.json and a score sheet
    """
    from ..similarity.settings import load_similarity_settings
    from ..summary.settings import load_summary_settings

    if not re.fullmatch(r"[A-Za-z0-9._-]+", name):
        raise ValueError(f"NAME {name!r} may use only letters, digits, '.', '_' and '-'")
    sim_settings = replace(load_similarity_settings(), enabled=True)
    summary_settings = load_summary_settings()
    print(f"Experiment {name}: {description}")
    report = run(name, build_summarizer(summary_settings), sim_settings, summary_settings,
                 comparator=build_comparator(sim_settings) if build_comparator else None,
                 embedder=build_embedder(sim_settings) if build_embedder else None, description=description,
                 source=Path(source).name if source else None, attempts=attempts, limit_events=limit_events,
                 keep_database=keep_db)
    print()
    print_metrics(report)
    if save:
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        path = RESULTS_DIR / f"{datetime.now().strftime('%Y-%m-%d_%H%M')}_{name}.json"
        path.write_text(json.dumps(to_jsonable(report), indent=2, default=str) + "\n", encoding="utf-8")
        sheet = path.with_name(f"{path.stem}_scores.csv")
        write_score_sheet(report, sheet)
        print(f"Saved {path}\nScore sheet {sheet}\nRead it: uv run datasources-eval report {path.stem}")
    return report


# ----------------------------------------------------------------------------- printing (live and `report`)
def print_metrics(report: dict[str, Any], progress: Callable[[str], None] = print) -> None:
    m, g = report["metrics"], report["grouping"]
    progress(f"Summaries: {m['results']}  published {m['published_rate']}  "
             f"(first try {m['first_try_published']}/{m['events']})")
    progress(f"Citations verified {m['citations_verified_rate']}  sources cited (multi-source events) "
             f"{m['source_coverage']}  update article cited {m['update_cited_rate']} (of {m['update_events']})  "
             f"conflict article cited {m['conflict_cited_rate']} (of {m['conflict_events']})")
    progress(f"Grouping the summaries were built on: pairwise F1 {g['pairwise']['f1']:.3f}, label macro F1 "
             f"{g['labels']['macro_f1']:.3f} ({report['model']['comparison_model']})")
    progress(f"Model tokens in/out {m['tokens']['input']}/{m['tokens']['output']}, summary time {m['seconds']}s, "
             f"total {report.get('seconds')}s")


def _yes(value: bool | None) -> str:
    return "-" if value is None else ("yes" if value else "NO")


def print_event(e: dict[str, Any], *, passages: bool = False, progress: Callable[[str], None] = print) -> None:
    key = e["answer_key"]
    progress(f"\n=== {e['event']}: {e['title'] or '(untitled)'} ===")
    progress(f"{len(e['members'])} source(s), result {e['result']}, {len(e['attempts'])} attempt(s), "
             f"{e['seconds']}s, tokens {e['tokens']['input']}/{e['tokens']['output']}")
    if key["pure"]:
        progress(f"answer key: group {key['group']}" + (f", missing {', '.join(key['missing'])}"
                                                        if key["missing"] else ", complete"))
    else:
        progress(f"answer key: MIXED groups {key['groups']} (grouping error: judge the summary on what it was given)")
    progress("sources:")
    for m in e["members"]:
        progress(f"  {m['source']:<3} {str(m['file']):<38} {m['relation']:<20} {str(m['publisher'])[:18]:<18} "
                 f"{m['published'] or '?':<16}  {str(m['title'])[:70]}")
    for r in e["reviews"]:
        progress(f"review: {r['file']} -> {r['decision']}  ({r['why']})")
    for n in e["input"]["comparison_notes"]:
        for d in n["differences"]:
            progress(f"note {n['source']} {n['label']}: \"{d.get('incoming')}\" vs earlier \"{d.get('candidate')}\"")
    progress(f"input: {len(e['input']['passages'])} passage(s), {e['input']['chars']} chars"
             + (f"; {'; '.join(e['input']['limitations'])}" if e["input"]["limitations"] else ""))
    errors = [x for a in e["attempts"] for x in (a["validation_errors"] or [])]
    final = e["attempts"][-1] if e["attempts"] else {}
    if e["result"] != "published":
        progress(f"NOT PUBLISHED ({e['result']})" + ("" if e["result"] in ("invalid", "error") else f": {e['reason']}"))
        if final.get("error"):
            progress(f"  error: {final['error'].get('message')}")
        for x in final.get("validation_errors") or []:
            progress(f"  validation: {x}")
    elif errors:
        progress(f"published after {len(e['attempts'])} attempts; earlier rejections: {'; '.join(errors[:3])}")
    if e["summary"]["statements"]:
        progress("summary:" if e["result"] == "published" else "summary as written (rejected):")
    for s in e["summary"]["statements"]:
        progress(f"  {str(s['id']):<3} {s['text']}")
        for c in s["citations"]:
            mark = "ok" if c["verified"] else "NOT FOUND"
            progress(f"        {c['ref']} {c['source'] or '?'} {mark:<9} \"{c['quote']}\"")
    if e["summary"].get("raw_text"):
        progress(f"raw model output (not JSON): {e['summary']['raw_text'][:500]}")
    for gap in e["summary"]["evidence_gaps"]:
        progress(f"  gap: {gap}")
    c = e["checks"]
    progress(f"checks: sources cited {c['sources_cited']}/{c['sources']}"
             + (f" (not cited: {', '.join(c['not_cited'])})" if c["not_cited"] else "")
             + f"; quotes verified {c['citations_verified']}/{c['citations']}"
             + f"; update cited {_yes(c['update_cited'])}; conflict cited {_yes(c['conflict_cited'])}")
    if passages:
        progress("passages given to the model:")
        for p in e["input"]["passages"]:
            text = "\n".join(f"    {line}" for line in p["text"].splitlines() if line.strip())
            progress(f"  [{p['ref']} {p['source']} {p['file']} {p['relation']}]\n{text}")


def print_report(report: dict[str, Any], *, event: str | None = None, passages: bool = False,
                 grouping: bool = False, progress: Callable[[str], None] = print) -> int:
    model = report["model"]
    progress(f"summary model: {model['summary_model']}   grouping: {model['comparison_model']}   "
             f"events {report['events_summarized']}/{report['events_total']}   {report['experiment']['description']}")
    if report.get("incomplete"):
        progress(f"[INCOMPLETE — excluded from the leaderboard] {report.get('incomplete_reason', '')}")
    if grouping:
        progress("\ngrouping:")
        for g in report["grouping"]["articles"]:
            mark = "ok" if g["expected_label"] in (None, g["predicted_label"]) else "MISS"
            progress(f"    {g['file']:<38} expected {str(g['expected_label']):<20} got {g['predicted_label']:<20} "
                     f"{'/'.join(g['events']) or '-':<6} {mark}")
    events = [e for e in report["events"] if event is None or e["event"].lower() == event.lower()]
    if event is not None and not events:
        progress(f"\nno event {event} in this run (have {report['events'][0]['event']}..{report['events'][-1]['event']})"
                 if report["events"] else "\nthis run has no events")
        return 1
    for e in events:
        print_event(e, passages=passages, progress=progress)
    if event is None:
        progress("\n=== TOTAL ===")
        print_metrics(report, progress)
        progress("\nHuman rubric (fill the score sheet <run>_scores.csv, one copy per rater):")
        for k, v in report.get("rubric", RUBRIC).items():
            progress(f"  {k:<10} {v}")
    return 0


def print_leaderboard(directory: Path, progress: Callable[[str], None] = print) -> int:
    rows, skipped = [], 0
    for path in sorted(directory.glob("*.json")):
        r = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(r, dict) or r.get("stage") != "summarization":
            continue
        if r.get("incomplete"):
            skipped += 1
            continue
        m, human = r["metrics"], read_scores(path) or {}
        rows.append((path.stem, r["model"].get("summary_model") or "-", r["model"].get("comparison_model") or "-",
                     m["events"], m["published_rate"], m["citations_verified_rate"], m["source_coverage"],
                     human.get("rated"), human.get("acceptable_rate"), human.get("faithful"),
                     human.get("coverage"), human.get("conflict")))
    if not rows:
        progress(f"No complete runs in {directory}" + (f" ({skipped} incomplete log(s) hidden)" if skipped else ""))
        return 0
    head = (f"{'run':<40} {'summary model':<22} {'grouping':<12} {'events':>6} {'publ':>5} {'quote':>5} {'srcs':>5} "
            f"| {'rated':>5} {'accept':>6} {'faith':>5} {'cover':>5} {'confl':>5}")
    progress(head)

    def f(v, w):
        return f"{'-' if v is None else v:>{w}}"

    for row in sorted(rows, key=lambda r: (-(r[8] or -1), -(r[4] or 0))):
        progress(f"{row[0]:<40} {str(row[1])[:22]:<22} {str(row[2])[:12]:<12} {row[3]:>6} {f(row[4], 5)} "
                 f"{f(row[5], 5)} {f(row[6], 5)} | {f(row[7], 5)} {f(row[8], 6)} {f(row[9], 5)} {f(row[10], 5)} "
                 f"{f(row[11], 5)}")
    progress("\nLeft: mechanical checks (publ = summaries that passed validation, quote = quotes found verbatim, "
             "srcs = share of sources cited). Right: human scores from the filled <run>_scores*.csv sheets. "
             "Rank models by the human columns.")
    if skipped:
        progress(f"{skipped} incomplete run(s) hidden; `report <name>` still shows them as logs.")
    return 0
