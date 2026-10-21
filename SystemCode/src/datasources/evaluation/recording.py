"""Full-detail evaluation runs: grouping (step 3) and summaries with validation (step 4), everything kept for people.

Two inputs, each in its own throwaway database `stock_news_eval_..._test` (never the main database):

    fixtures  the 125 test articles, through `summarization.run` (answer-key or model grouping), then enriched
    real      a slice of real NVDA news + 8-K filings from populate/003_nvda_8k_windows_2026-07-08.sql. There is
              no answer key: the run keeps what a person needs to judge it by hand, plus a sheet to label events.

What a result keeps, beyond the existing summarization report:
    * every LLM call: the full prompt text (the system prompt once, by hash), the raw response including any
      reasoning/thinking field the server returns, the parsed output, tokens, latency, errors (calls/<run>.json)
    * per article: the text it was analysed on, its text units (what was embedded), every retrieval candidate with
      its cosine / lexical / fused scores and why it was or was not compared, the LLM judgments with explanations
      and validation notes, the resulting event memberships, timings
    * per event: members, conflict state (held, and how it was resolved), the summary attempts with validation
      errors, the passages given to the model, citations checked, the published version
    * label-free metrics and review lists (uncertain, singletons, shortlist cuts, near-threshold pairs, conflicts)
Embeddings themselves are not kept: they are vectors, not readable evidence.

The `no_filter` variant answers "does the filter only save cost, or does it change the result?": the same slice
with every event in the time window sent to the LLM (gate, top-k and shortlist opened; window and company scope
kept). `compare_filters` lists the links each variant made that the other did not, and the cost difference.
"""

from __future__ import annotations

import contextvars
import csv
import hashlib
import json
import time
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import psycopg

from ..db import Database, load_db_settings
from ..db.evaluation import FIXTURE_DIR, read_fixtures
from . import summarization as summ
from .consolidation import BatchDatabase, _predicted_label, to_jsonable

ROOT = Path(__file__).resolve().parents[3]
RESULTS_DIR = ROOT / "evaluation" / "summarization" / "results"
POPULATE_FILE = ROOT / "populate" / "003_nvda_8k_windows_2026-07-08.sql"
# Real-data slices: [from, to) in UTC around each NVIDIA 8-K (the Alpaca history was fetched for -12 h .. +48 h).
SLICES = {
    "jul2": ("2026-06-30T00:00:00+00:00", "2026-07-06T00:00:00+00:00", "8-K 0001045810-26-000060 (Item 5.02)"),
    "aug17": ("2026-08-14T00:00:00+00:00", "2026-08-21T00:00:00+00:00", "8-K 0001045810-26-000069 (Items 1.01, 2.03, 7.01)"),
    "aug26": ("2026-08-21T00:00:00+00:00", "2026-08-30T00:00:00+00:00", "8-K 0001045810-26-000073 (Items 2.02, 9.01): Q2 FY27 earnings"),
    "all": ("2026-06-30T00:00:00+00:00", "2026-08-30T00:00:00+00:00", "all three windows"),
}
# Everything in the time window and company scope reaches the LLM; calls are still split like production.
NO_FILTER = {"min_semantic_similarity": -1.0, "lexical_plausible_rank": 0, "semantic_top_k": 500, "lexical_top_k": 500,
             "max_candidate_events": 1000, "llm_max_calls_per_record": 50, "llm_token_budget_per_record": 5_000_000}
VARIANTS = {"production": {}, "no_filter": NO_FILTER}
NEAR_GATE = 0.70  # cosine band [0.70, gate) listed for review: pairs the gate kept from the LLM by a small margin
ASSUMED_REVIEWER = "eval-ASSUMED-unreviewed"
STOP_KINDS = ("connection", "configuration", "authentication", "model_not_found")


# ----------------------------------------------------------------------------- LLM call log
_TAG: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("eval_tag", default={})
_CALL: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar("eval_call", default=None)


def set_tag(**tag: Any) -> None:
    _TAG.set(tag)


class LLMCallLog:
    """Records every model call: the backend-level request/response, and each HTTP attempt underneath it.

    Used as a context manager it also wraps `httpx.post` inside `datasources.llm_openai` (the OpenAI-compatible
    transport Ollama uses), so the raw response, including a `reasoning`/`thinking` field, is kept as returned."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.prompts: dict[str, str] = {}
        self._saved_post = None

    def _prompt_ref(self, text: str) -> str:
        sha = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
        self.prompts.setdefault(sha, text)
        return sha

    # backend level -------------------------------------------------------------------------
    def start(self, phase: str, backend: Any, text: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        call = {"seq": len(self.calls) + 1, "phase": phase, "tag": dict(_TAG.get()), **(extra or {}),
                "backend": getattr(backend, "name", type(backend).__name__), "model": getattr(backend, "model", None),
                "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "input": {"text": text, "chars": len(text)}, "http": []}
        self.calls.append(call)
        _CALL.set(call)
        call["_t0"] = time.monotonic()
        return call

    def finish(self, call: dict[str, Any], *, output: Any = None, usage: dict | None = None, model: str | None = None,
               stop_reason: str | None = None, error: BaseException | None = None) -> None:
        call["seconds"] = round(time.monotonic() - call.pop("_t0"), 2)
        call["output"] = to_jsonable(output)
        call["usage"] = dict(usage or {})
        call["model_reported"] = model
        call["stop_reason"] = stop_reason
        if error is not None:
            call["error"] = {"type": type(error).__name__, "message": str(error)[:2000],
                             "kind": getattr(error, "kind", None), "retryable": getattr(error, "retryable", None),
                             "raw_output": getattr(error, "raw_output", None)}
        _CALL.set(None)

    # HTTP level ----------------------------------------------------------------------------
    def __enter__(self) -> "LLMCallLog":
        from .. import llm_openai

        self._saved_post = original = llm_openai.httpx.post
        log = self

        def recorded_post(url, *args, **kwargs):
            if not str(url).endswith("/chat/completions"):
                return original(url, *args, **kwargs)
            body = kwargs.get("json") or {}
            messages = body.get("messages") or []
            system = next((m.get("content", "") for m in messages if m.get("role") == "system"), "")
            user = next((m.get("content", "") for m in messages if m.get("role") == "user"), "")
            call = _CALL.get()
            same = bool(call) and user == call["input"]["text"]
            attempt = {"request": {"url": str(url), "model": body.get("model"), "max_tokens": body.get("max_tokens"),
                                   "temperature": body.get("temperature"),
                                   "response_format": (body.get("response_format") or {}).get("type"),
                                   "system_prompt": log._prompt_ref(system), "system_chars": len(system),
                                   "user_chars": len(user), "user_same_as_input": same,
                                   **({} if same else {"user": user})}}
            t0 = time.monotonic()
            try:
                response = original(url, *args, **kwargs)
            except Exception as exc:  # noqa: BLE001 - recorded, then re-raised unchanged
                attempt.update(seconds=round(time.monotonic() - t0, 2), error=f"{type(exc).__name__}: {exc}")
                (call["http"] if call else log.calls).append(attempt)
                raise
            attempt["seconds"] = round(time.monotonic() - t0, 2)
            attempt["status"] = response.status_code
            try:
                data = response.json()
                choice = (data.get("choices") or [{}])[0]
                message = choice.get("message") or {}
                attempt["response"] = {"id": data.get("id"), "model": data.get("model"),
                                       "finish_reason": choice.get("finish_reason"), "usage": data.get("usage"),
                                       "content": message.get("content"),
                                       # thinking models: Ollama returns the reasoning beside the answer
                                       "reasoning": message.get("reasoning") or message.get("reasoning_content")
                                       or message.get("thinking"),
                                       "other_message_fields": sorted(set(message) - {"role", "content"})}
            except ValueError:
                attempt["response"] = {"text": response.text[:20000]}
            (call["http"] if call else log.calls).append(attempt)
            return response

        llm_openai.httpx.post = recorded_post
        return self

    def __exit__(self, *exc) -> None:
        from .. import llm_openai

        if self._saved_post is not None:
            llm_openai.httpx.post = self._saved_post

    def save(self, path: Path, header: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(to_jsonable({**header, "system_prompts": self.prompts, "calls": self.calls}),
                                   indent=1, default=str, ensure_ascii=False) + "\n", encoding="utf-8")


class _Forward:
    def __init__(self, inner: Any, log: LLMCallLog):
        self.inner, self.log = inner, log

    def __getattr__(self, item: str) -> Any:  # name, model, before_article, ... as the wrapped backend has them
        return getattr(self.inner, item)


class RecordingComparator(_Forward):
    def before_article(self, fixture, article_id: str, database) -> None:
        """Fixture runs call this before each article: tag the calls, and pass it on (answer-key comparator)."""
        set_tag(phase="grouping", article=fixture.filename, article_id=article_id)
        if hasattr(self.inner, "before_article"):
            self.inner.before_article(fixture, article_id, database)

    def compare(self, request):
        call = self.log.start("grouping", self.inner, request.render(),
                              {"pairs": request.pair_count(), "candidate_events": len(request.events)})
        try:
            result = self.inner.compare(request)
        except Exception as exc:
            self.log.finish(call, error=exc)
            raise
        output = result.output.model_dump() if hasattr(result.output, "model_dump") else result.output
        self.log.finish(call, output=output, usage=result.usage, model=result.model, stop_reason=result.stop_reason)
        return result


class RecordingSummarizer(_Forward):
    def summarize(self, request):
        call = self.log.start("summary", self.inner, request.render(),
                              {"event_id": request.pack.event_id, "passages": len(request.pack.passages)})
        try:
            result = self.inner.summarize(request)
        except Exception as exc:
            self.log.finish(call, error=exc)
            raise
        self.log.finish(call, output=result.output, usage=result.usage, model=result.model,
                        stop_reason=getattr(result, "stop_reason", None))
        return result


# ----------------------------------------------------------------------------- reading a run's database
def _source_time_sql() -> str:
    from ..similarity.store import SOURCE_TIME
    return SOURCE_TIME


def _articles_sql() -> str:
    return f"""
SELECT d.document_id::text AS document_id, d.source_record_id, d.source_namespace, d.record_type,
       r.revision_id::text AS revision_id, r.title, r.original_url, r.content_scope, r.summary_text,
       r.analysis_text, r.body_text, {_source_time_sql()} AS source_time, pub.name AS publisher,
       s.accession_number, s.form, s.item_codes, s.filing_date
  FROM document d
  JOIN document_revision r ON r.revision_id = d.current_revision_id
  LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
  LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
 WHERE EXISTS (SELECT 1 FROM revision_analysis a WHERE a.revision_id = r.revision_id)
 ORDER BY {_source_time_sql()}, r.revision_id
"""


def _iso(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.isoformat(timespec="seconds")
    return v.isoformat() if hasattr(v, "isoformat") else v


def _label(rows: list[dict[str, Any]]) -> str:
    """The article's relation to existing events. `partial` (some candidate not evaluated) is a valid outcome, not an
    error, so unlike the fixture runner it still yields a label; pending/failed analyses do not."""
    if rows and rows[0]["analysis_status"] in ("pending", "failed"):
        return f"error:{rows[0]['analysis_status']}"
    return _predicted_label([{**r, "analysis_status": "completed"} for r in rows])[1]


def step3_detail(db: Database, describe: Callable[[dict[str, Any]], dict[str, Any]],
                 timings: dict[str, float] | None = None, names: dict[str, str] | None = None) -> dict[str, Any]:
    """Everything grouping did, per article and per event, readable (no vectors). `names`: event_id -> E#."""
    names = {} if names is None else names
    with db.connection() as conn:
        arts = [dict(r) for r in conn.execute(_articles_sql()).fetchall()]
        events = [dict(r) for r in conn.execute(
            "SELECT * FROM news_event ORDER BY first_source_time NULLS LAST, created_at").fetchall()]
        analyses = {str(r["revision_id"]): dict(r) for r in conn.execute(
            "SELECT DISTINCT ON (revision_id) * FROM revision_analysis ORDER BY revision_id, updated_at DESC").fetchall()}
        units = conn.execute("SELECT unit_id::text AS unit_id, revision_id::text AS revision_id, unit_kind, ordinal, "
                             "input_text, location, embedding_status FROM text_unit ORDER BY revision_id, ordinal").fetchall()
        cands = conn.execute("""
            SELECT a.revision_id::text AS revision_id, rc.incoming_unit_id::text AS incoming_unit_id,
                   rc.candidate_unit_id::text AS candidate_unit_id, rc.candidate_revision_id::text AS candidate_revision_id,
                   rc.semantic_rank, rc.semantic_similarity, rc.lexical_rank, rc.lexical_score, rc.fused_score,
                   rc.fused_rank, rc.candidate_event_ids::text[] AS candidate_event_ids, rc.shortlisted, rc.not_compared_reason
              FROM retrieval_candidate rc JOIN revision_analysis a ON a.analysis_id = rc.analysis_id
             ORDER BY a.revision_id, rc.fused_rank NULLS LAST, rc.fused_score DESC""").fetchall()
        comps = conn.execute("""
            SELECT revision_id::text AS revision_id, segment_key, candidate_event_id::text AS candidate_event_id,
                   candidate_revision_ids::text[] AS candidate_revision_ids, label, method, explanation, matching_facts,
                   differences, evidence, insufficient_context, uncertainty, validation_notes, model, prompt_version,
                   usage, incoming_unit_ids::text[] AS incoming_unit_ids, candidate_unit_ids::text[] AS candidate_unit_ids,
                   comparison_id::text AS comparison_id
              FROM event_comparison ORDER BY created_at""").fetchall()
        members = conn.execute("""
            SELECT revision_id::text AS revision_id, event_id::text AS event_id, segment_key, relation, state,
                   comparison_id::text AS comparison_id, created_at FROM event_membership ORDER BY created_at""").fetchall()
        preds = conn.execute("""
            SELECT r.revision_id::text AS revision_id, a.status AS analysis_status, a.last_error,
                   m.event_id::text AS event_id, m.relation, m.state, m.created_at
              FROM document d JOIN document_revision r ON r.revision_id = d.current_revision_id
              LEFT JOIN revision_analysis a ON a.revision_id = r.revision_id
              LEFT JOIN event_membership m ON m.revision_id = r.revision_id ORDER BY m.created_at""").fetchall()
    for e in events:
        names.setdefault(str(e["event_id"]), f"E{len(names) + 1}")
    key_of = {a["revision_id"]: describe(a)["key"] for a in arts}

    def ev(eid: Any) -> str | None:
        return names.get(str(eid)) if eid else None

    by_rev: dict[str, dict[str, list]] = {}
    for kind, rows in (("units", units), ("candidates", cands), ("judgments", comps), ("memberships", members),
                       ("pred", preds)):
        for r in rows:
            by_rev.setdefault(r["revision_id"], {}).setdefault(kind, []).append(dict(r))
    articles = []
    for a in arts:
        rv = by_rev.get(a["revision_id"], {})
        an = analyses.get(a["revision_id"], {})
        label = _label(rv.get("pred", []))
        info = describe(a)
        articles.append({
            **info, "type": a["record_type"], "source_record_id": a["source_record_id"], "revision_id": a["revision_id"],
            "title": a["title"],
            "published": _iso(a["source_time"]), "publisher": a["publisher"], "url": a["original_url"],
            "content_scope": a["content_scope"],
            "sec": {"accession": a["accession_number"], "form": a["form"], "items": a["item_codes"],
                    "filing_date": _iso(a["filing_date"])} if a["accession_number"] else None,
            "text": {"analysis_text": a["analysis_text"], "analysis_chars": len(a["analysis_text"] or ""),
                     "summary_text": a["summary_text"], "body_chars": len(a["body_text"] or "")},
            "analysis": {k: _iso(an.get(k)) for k in ("status", "stage_status", "skip_reason", "evidence_level",
                                                      "anchor_time", "anchor_source", "index_coverage", "limitations",
                                                      "usage", "comparison_model", "prompt_version", "attempts",
                                                      "last_error")},
            "seconds": (timings or {}).get(a["revision_id"]),
            "units": [{"ordinal": u["ordinal"], "kind": u["unit_kind"], "chars": len(u["input_text"] or ""),
                       "location": u["location"], "embedding_status": u["embedding_status"], "text": u["input_text"]}
                      for u in rv.get("units", [])],
            "retrieval": [{"candidate": key_of.get(c["candidate_revision_id"], c["candidate_revision_id"]),
                           "candidate_events": [ev(x) for x in (c["candidate_event_ids"] or [])],
                           "semantic_rank": c["semantic_rank"], "semantic_similarity": c["semantic_similarity"],
                           "lexical_rank": c["lexical_rank"], "lexical_score": c["lexical_score"],
                           "fused_score": c["fused_score"], "fused_rank": c["fused_rank"],
                           "shortlisted": c["shortlisted"], "not_compared_reason": c["not_compared_reason"]}
                          for c in rv.get("candidates", [])],
            "judgments": [{"segment": j["segment_key"], "candidate_event": ev(j["candidate_event_id"]),
                           "candidate_articles": [key_of.get(x, x) for x in (j["candidate_revision_ids"] or [])],
                           **{k: j[k] for k in ("label", "method", "explanation", "matching_facts", "differences",
                                                "evidence", "insufficient_context", "uncertainty", "validation_notes",
                                                "model", "prompt_version", "usage")}}
                          for j in rv.get("judgments", [])],
            "memberships": [{"event": ev(m["event_id"]), "segment": m["segment_key"], "relation": m["relation"],
                             "state": m["state"]} for m in rv.get("memberships", [])],
            "predicted_label": label,
        })
    event_rows = []
    for e in events:
        ms = [m for m in members if str(m["event_id"]) == str(e["event_id"])]
        types = {next((a["type"] for a in articles if a["key"] == key_of.get(m["revision_id"])), None) for m in ms}
        event_rows.append({"event": ev(e["event_id"]), "event_id": str(e["event_id"]), "title": e["title"],
                           "status": e["status"], "facts": e["event_facts"], "flags": e["flags"],
                           "first_source_time": _iso(e["first_source_time"]), "superseded_by": ev(e["superseded_by"]),
                           "members": [{"article": key_of.get(m["revision_id"], m["revision_id"]),
                                        "relation": m["relation"], "state": m["state"]} for m in ms],
                           "mixes_sec_and_news": {"sec_filing", "news_article"} <= types})
    return {"articles": articles, "events": event_rows}


# ----------------------------------------------------------------------------- metrics and review lists
def label_free_metrics(step3: dict[str, Any], gate: float, calls: list[dict[str, Any]]) -> dict[str, Any]:
    arts, events = step3["articles"], [e for e in step3["events"] if e["status"] != "superseded"]
    active = [len([m for m in e["members"] if m["state"] == "active"]) for e in events]
    count: Callable[[list], dict] = lambda xs: {k: xs.count(k) for k in sorted(set(xs), key=str)}
    cands = [c for a in arts for c in a["retrieval"]]
    phase = lambda p: [c for c in calls if c.get("phase") == p]
    tok = lambda cs, k: sum(int((c.get("usage") or {}).get(k, 0) or 0) for c in cs)
    return {
        "articles": len(arts), "sec_filings": sum(a["type"] == "sec_filing" for a in arts),
        "events": len(events), "singleton_events": sum(n == 1 for n in active),
        "multi_source_events": sum(n > 1 for n in active), "largest_event": max(active, default=0),
        "events_mixing_sec_and_news": sum(e["mixes_sec_and_news"] for e in events),
        "predicted_labels": count([a["predicted_label"] for a in arts]),
        "analysis_status": count([a["analysis"]["status"] for a in arts]),
        "relations": count([m["relation"] for a in arts for m in a["memberships"]]),
        "judgment_labels": count([j["label"] for a in arts for j in a["judgments"]]),
        "judgments_downgraded": sum(any("downgraded" in str(n) for n in (j["validation_notes"] or []))
                                    for a in arts for j in a["judgments"]),
        "retrieval": {"candidates": len(cands), "above_gate": sum((c["semantic_similarity"] or -2) >= gate for c in cands),
                      "not_compared_reasons": count([c["not_compared_reason"] for c in cands if c["not_compared_reason"]]),
                      "articles_with_a_candidate_event": sum(any(c["candidate_events"] for c in a["retrieval"]) for a in arts)},
        "llm": {p: {"calls": len(phase(p)), "errors": sum("error" in c for c in phase(p)),
                    "input_tokens": tok(phase(p), "input_tokens"), "output_tokens": tok(phase(p), "output_tokens"),
                    "input_chars": sum(c["input"]["chars"] for c in phase(p)),
                    "seconds": round(sum(c.get("seconds") or 0 for c in phase(p)), 1)} for p in ("grouping", "summary")},
        "grouping_seconds": round(sum(a["seconds"] or 0 for a in arts), 1),
    }


def review_aids(step3: dict[str, Any], gate: float) -> dict[str, Any]:
    arts = step3["articles"]
    title = {a["key"]: a["title"] for a in arts}
    near = []
    for a in arts:
        best: dict[str, float] = {}
        for c in a["retrieval"]:
            s = c["semantic_similarity"]
            if s is not None and NEAR_GATE <= s < gate and not c["shortlisted"]:
                best[c["candidate"]] = max(best.get(c["candidate"], -1), s)
        near += [{"article": a["key"], "title": a["title"], "candidate": k, "candidate_title": title.get(k),
                  "cosine": round(v, 4)} for k, v in best.items()]
    return {
        "uncertain": [{"article": a["key"], "title": a["title"],
                       "why": [j["explanation"] for j in a["judgments"] if j["label"] == "uncertain"],
                       "validation_notes": [n for j in a["judgments"] for n in (j["validation_notes"] or [])]}
                      for a in arts if a["predicted_label"] == "uncertain"],
        "singletons": [{"event": e["event"], "article": e["members"][0]["article"], "title": e["title"]}
                       for e in step3["events"] if e["status"] != "superseded"
                       and len([m for m in e["members"] if m["state"] == "active"]) == 1],
        "shortlist_cut": [{"article": a["key"], "title": a["title"],
                           "events_cut": sorted({x for c in a["retrieval"] if c["not_compared_reason"] == "below_shortlist"
                                                 for x in c["candidate_events"] if x})}
                          for a in arts if any(c["not_compared_reason"] == "below_shortlist" for c in a["retrieval"])],
        "near_gate_not_sent": sorted(near, key=lambda r: -r["cosine"]),
        "conflicts": [{"article": a["key"], "title": a["title"], "event": j["candidate_event"],
                       "differences": j["differences"], "explanation": j["explanation"]}
                      for a in arts for j in a["judgments"] if j["label"] == "same_event_conflict"],
        "sec_filings": [{"article": a["key"], "title": a["title"], "items": (a["sec"] or {}).get("items"),
                         "events": [m["event"] for m in a["memberships"]], "label": a["predicted_label"],
                         "best_cosine_to_news": max((c["semantic_similarity"] or -1 for c in a["retrieval"]), default=None)}
                        for a in arts if a["type"] == "sec_filing"],
        "errors": [{"article": a["key"], "status": a["analysis"]["status"], "error": a["analysis"]["last_error"]}
                   for a in arts if a["analysis"]["last_error"]],
    }


def _pairs(step3: dict[str, Any]) -> set[tuple[str, str]]:
    groups: dict[str, list[str]] = {}
    for a in step3["articles"]:
        for m in a["memberships"]:
            if m["state"] == "active" and m["event"]:
                groups.setdefault(m["event"], []).append(a["key"])
    return {tuple(sorted((x, y))) for ks in groups.values() for i, x in enumerate(ks) for y in ks[i + 1:] if x != y}


def compare_filters(prod: dict[str, Any], free: dict[str, Any]) -> dict[str, Any]:
    """What the filter changes: links one variant made and the other did not, label changes, and cost."""
    pa, pb = _pairs(prod["step3"]), _pairs(free["step3"])
    arts_b = {a["key"]: a for a in free["step3"]["articles"]}
    arts_a = {a["key"]: a for a in prod["step3"]["articles"]}
    keys = sorted(set(arts_a) & set(arts_b))

    def evidence(pair: tuple[str, str], arts: dict[str, dict]) -> dict[str, Any]:
        x, y = pair
        later = max(pair, key=lambda k: arts.get(k, {}).get("published") or "")
        other = y if later == x else x
        a = arts.get(later, {})
        cos = max((c["semantic_similarity"] or -1 for c in a.get("retrieval", []) if c["candidate"] == other), default=None)
        js = [j for j in a.get("judgments", []) if other in (j["candidate_articles"] or [])]
        return {"pair": list(pair), "titles": [arts.get(x, {}).get("title"), arts.get(y, {}).get("title")],
                "cosine": cos, "judgments": [{k: j[k] for k in ("label", "explanation", "matching_facts", "differences")}
                                             for j in js]}

    agree = sum(((x, y) in pa) == ((x, y) in pb) for i, x in enumerate(keys) for y in keys[i + 1:])
    total = len(keys) * (len(keys) - 1) // 2
    cost = lambda r: {"llm": r["metrics"]["llm"], "grouping_seconds": r["metrics"]["grouping_seconds"]}
    return {
        "stage": "summarization_real_filter_comparison",
        "runs": {"production": prod["file"], "no_filter": free["file"]},
        "articles_compared": len(keys),
        "same_event_pairs": {"production": len(pa), "no_filter": len(pb), "both": len(pa & pb)},
        "pair_agreement_rand_index": round(agree / total, 4) if total else None,
        "linked_only_without_filter": [evidence(p, arts_b) for p in sorted(pb - pa)],
        "linked_only_with_filter": [evidence(p, arts_a) for p in sorted(pa - pb)],
        "label_changes": [{"article": k, "title": arts_a[k]["title"], "production": arts_a[k]["predicted_label"],
                           "no_filter": arts_b[k]["predicted_label"]}
                          for k in keys if arts_a[k]["predicted_label"] != arts_b[k]["predicted_label"]],
        "cost": {"production": cost(prod), "no_filter": cost(free)},
        "reading": "linked_only_without_filter = what the filter hides from the LLM; check each pair by hand: a real "
                   "same-event link there means the filter changes results, not only cost.",
    }


# ----------------------------------------------------------------------------- summaries with full detail
def _summary_db_detail(db: Database, event_id: str) -> dict[str, Any]:
    with db.connection() as conn:
        attempts = conn.execute("""
            SELECT attempt_number, outcome, generation_reason, model_provider, model_name, prompt_version, output,
                   validation_errors, error_kind, error_message, usage, input_manifest, created_at
              FROM event_summary_attempt WHERE event_id = %s ORDER BY created_at""", (event_id,)).fetchall()
        published = conn.execute("""
            SELECT summary_id::text AS summary_id, version, headline, summary_text, key_points, statements, status,
                   generation_reason, model_name, prompt_version, usage, limitations, input_manifest, published_at
              FROM event_summary WHERE event_id = %s ORDER BY version""", (event_id,)).fetchall()
        cites = conn.execute("""
            SELECT c.summary_id::text AS summary_id, c.statement_key, c.quote_index, c.evidence_quote, t.input_text
              FROM event_summary_citation c JOIN event_summary s ON s.summary_id = c.summary_id
              JOIN text_unit t ON t.unit_id = c.unit_id WHERE s.event_id = %s
             ORDER BY c.summary_id, c.statement_key, c.quote_index""", (event_id,)).fetchall()
        job = conn.execute("SELECT status, reason, attempts, last_error FROM event_summary_job WHERE event_id = %s",
                           (event_id,)).fetchone()
    return {"db_attempts": [{k: _iso(v) for k, v in dict(a).items()} for a in attempts],
            "versions": [{k: _iso(v) for k, v in dict(p).items()} for p in published],
            "citations": [dict(c) for c in cites], "job": dict(job) if job else None}


def _conflicts(db: Database, policy: str, progress: Callable[[str], None]) -> dict[str, dict[str, Any]]:
    """Open a review for each flagged conflict (as production's summary routing does), record it, then apply the
    evaluation policy: 'hold' leaves it open (no summary, like production); 'assume_confirmed' resolves it as a
    confirmed conflict, marked as an assumption, so the summary is generated and must state both claims."""
    from ..summary import review, store

    out: dict[str, dict[str, Any]] = {}
    with db.transaction() as conn:
        for row in conn.execute(summ.EVENTS_SQL).fetchall():
            store.ensure_conflict_reviews(conn, row["event_id"])
        rows = conn.execute("""
            SELECT v.review_id::text AS review_id, c.candidate_event_id::text AS event_id, d.source_record_id,
                   c.differences, c.explanation
              FROM event_conflict_review v JOIN event_comparison c ON c.comparison_id = v.comparison_id
              JOIN document_revision r ON r.revision_id = c.revision_id JOIN document d ON d.document_id = r.document_id
             WHERE v.status IN ('open', 'in_review') ORDER BY v.created_at""").fetchall()
    for r in rows:
        entry = out.setdefault(r["event_id"], {"held": True, "policy": policy, "reviews": []})
        decision = None
        if policy == "assume_confirmed":
            review.decide(db, r["review_id"], decision="confirmed_conflict", reviewer=ASSUMED_REVIEWER,
                          explanation="ASSUMED by the evaluation runner, not a human decision: real data has no "
                                      "answer key, so the conflict is treated as confirmed to evaluate the summary.",
                          language="en")
            decision = "confirmed_conflict (assumed)"
        entry["reviews"].append({"article": r["source_record_id"], "claims": r["differences"],
                                 "explanation": r["explanation"], "decision": decision})
    progress(f"conflict reviews: {len(rows)} flagged; policy {policy}")
    return out


def summarize_events(db: Database, summarizer, summary_settings, events: list[dict[str, Any]], *, attempts: int,
                     describe_doc: Callable[[str], dict[str, Any]], conflicts: dict[str, dict[str, Any]],
                     progress: Callable[[str], None]) -> list[dict[str, Any]]:
    from ..summary.pipeline import SummaryBudget, SummaryContext, SummaryRuntime

    sctx = SummaryContext(db, SummaryRuntime(summary_settings, summarizer), SummaryBudget(10 ** 6))
    out = []
    for e in events:
        set_tag(phase="summary", event=e["event"], event_id=e["event_id"])
        ev = summ._summarize_event(e["event_id"], sctx, attempts, db.connection, describe_doc)
        ev = {"event": e["event"], "title": e["title"], **ev,
              "conflict": conflicts.get(e["event_id"], {"held": False}),
              "checks": summ._checks([{**m, "type": m.get("type"), "group": None} for m in ev["members"]],
                                     ev["summary"]["statements"], None),
              "database": _summary_db_detail(db, e["event_id"])}
        out.append(ev)
        head = next((s["text"] for s in ev["summary"]["statements"] if s["kind"] == "headline"), "")
        progress(f"  {ev['event']:<4} {ev['result'] or '-':<14} {len(ev['members'])} source(s) {ev['seconds']:>6.1f}s  "
                 f"{str(head)[:80]}")
        error = (ev["attempts"][-1].get("error") or {}) if ev["attempts"] else {}
        if error.get("kind") in STOP_KINDS:
            progress(f"stopping: the summary model is not reachable ({error.get('message')}); saved as incomplete")
            break
    return out


# ----------------------------------------------------------------------------- the real-data run
def _save(report: dict[str, Any], stem: str, log: LLMCallLog) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    calls_path = RESULTS_DIR / "calls" / f"{stem}.json"
    report["llm_calls_file"] = str(calls_path.relative_to(ROOT))
    log.save(calls_path, {"run": stem, "note": "full prompt and raw response of every model call in this run"})
    path = RESULTS_DIR / f"{stem}.json"
    path.write_text(json.dumps(to_jsonable(report), indent=1, default=str, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def _label_sheet(report: dict[str, Any], path: Path) -> None:
    """Manual grouping labels: give every article the same `true_event` name as the others it is the same event as."""
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        w = csv.writer(handle)
        w.writerow(["article", "published", "type", "title", "predicted_event", "predicted_label", "true_event",
                    "true_label", "notes"])
        for a in report["step3"]["articles"]:
            w.writerow([a["key"], a["published"], a["type"], a["title"],
                        " ".join(m["event"] or "" for m in a["memberships"] if m["state"] == "active"),
                        a["predicted_label"], "", "", ""])


def run_real(name: str, *, comparator, summarizer, sim_settings, summary_settings, slice_key: str = "aug17",
             variant: str = "production", limit_articles: int | None = None, limit_events: int | None = None,
             attempts: int = 2, conflicts: str = "assume_confirmed", keep_db: bool = False, embedder=None,
             source: str | None = None, extra: dict[str, Any] | None = None, save: bool = True,
             progress: Callable[[str], None] = print) -> dict[str, Any]:
    from ..ingest.graph import run_reanalysis
    from ..similarity.embeddings import build_embedding_backend
    from ..similarity.pipeline import PipelineContext, build_runtime
    from ..summary.settings import PROMPT_VERSION as SUMMARY_PV
    from ..similarity.comparison import PROMPT_VERSION as COMPARE_PV

    lo, hi, about = SLICES[slice_key]
    sim = replace(sim_settings, enabled=True, **VARIANTS[variant])
    embedder = embedder or build_embedding_backend(sim)
    summary_settings = summary_settings.with_overrides(enabled=True, model=getattr(summarizer, "model", None)
                                                       or summary_settings.model)
    log = LLMCallLog()
    rec_cmp, rec_sum = RecordingComparator(comparator, log), RecordingSummarizer(summarizer, log)
    started, clock = datetime.now(timezone.utc), time.monotonic()
    sql = POPULATE_FILE.read_text(encoding="utf-8")
    timings: dict[str, float] = {}
    with log, BatchDatabase(f"{name}_real_{slice_key}_{variant}", keep=keep_db) as db:
        with db.transaction() as conn:
            conn.execute(sql)  # data only; the same statements `make db` runs
            rows = conn.execute(f"""
                SELECT r.revision_id::text AS revision_id, d.record_type FROM document d
                  JOIN document_revision r ON r.revision_id = d.current_revision_id
                  LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id
                 WHERE NOT d.is_sample AND {_source_time_sql()} >= %s AND {_source_time_sql()} < %s
                 ORDER BY {_source_time_sql()}, r.revision_id""", (lo, hi)).fetchall()
        ids = [r["revision_id"] for r in rows]
        if limit_articles:
            news = [r["revision_id"] for r in rows if r["record_type"] != "sec_filing"][:limit_articles]
            ids = [r["revision_id"] for r in rows if r["record_type"] == "sec_filing" or r["revision_id"] in news]
        with db.transaction() as conn:  # nothing has "arrived" yet: hidden as sample rows, revealed one by one below
            conn.execute("UPDATE document SET is_sample = true WHERE NOT is_sample")
        progress(f"[{variant}] {len(ids)} record(s) of slice {slice_key} ({about}) in {db.settings.dbname}")
        runtime = build_runtime(db, sim, embedder=embedder, comparator=rec_cmp)
        # One record at a time, in source-time order, each indexed AND grouped in the same graph run: what live
        # arrival does. Records not yet "arrived" are sample rows, invisible to retrieval and index coverage.
        # (Loading and indexing the whole slice first, as `similarity backfill` does, would let retrieval see later
        # articles: they take top-k places, and every analysis is marked partial for the unindexed future ones.)
        for i, rid in enumerate(ids, start=1):
            with db.transaction() as conn:  # this record arrives: retrieval and coverage now see it, not later ones
                conn.execute("UPDATE document SET is_sample = false WHERE current_revision_id = %s", (rid,))
            set_tag(phase="grouping", revision_id=rid)
            t0 = time.monotonic()
            run_reanalysis([rid], PipelineContext(db, runtime))
            timings[rid] = round(time.monotonic() - t0, 2)
            if i % 10 == 0 or i == len(ids):
                progress(f"  grouped {i}/{len(ids)} ({time.monotonic() - clock:.0f}s)")
        conflict_state = _conflicts(db, conflicts, progress)
        names: dict[str, str] = {}

        def describe(a: dict[str, Any]) -> dict[str, Any]:
            key = f"sec:{a['accession_number']}" if a.get("accession_number") else f"alpaca:{a['source_record_id']}"
            return {"key": key}
        step3 = step3_detail(db, describe, timings, names)
        doc_key = {}
        with db.connection() as conn:
            for r in conn.execute("SELECT d.document_id::text AS document_id, d.source_record_id, s.accession_number "
                                  "FROM document d JOIN document_revision r ON r.revision_id = d.current_revision_id "
                                  "LEFT JOIN sec_filing_details s ON s.revision_id = r.revision_id").fetchall():
                doc_key[r["document_id"]] = describe(dict(r))["key"]
        live = [e for e in step3["events"] if e["status"] != "superseded"
                and any(m["state"] == "active" for m in e["members"])]
        chosen = live[:limit_events] if limit_events else live
        progress(f"summarizing {len(chosen)} of {len(live)} event(s)")
        events = summarize_events(db, rec_sum, summary_settings, chosen, attempts=attempts,
                                  describe_doc=lambda d: {"file": doc_key.get(d), "type": None, "group": None},
                                  conflicts=conflict_state, progress=progress)
        dbname = db.settings.dbname
    for c in log.calls:  # readable tags: the article key / E# for each call
        rid = c["tag"].get("revision_id")
        if rid:
            c["tag"]["article"] = next((a["key"] for a in step3["articles"]
                                        if a.get("revision_id") == rid), None)
    gate = sim.min_semantic_similarity
    stem = f"{datetime.now().strftime('%Y-%m-%d_%H%M')}_{name}_real-{slice_key}_{variant.replace('_', '-')}"
    report = {
        "stage": "summarization_real",
        "experiment": {"name": name, "file": source, "variant": variant, "slice": slice_key,
                       "note": "real data, no answer key: judge by hand (scores and grouping-label sheets)", **(extra or {})},
        "model": {"comparator": getattr(comparator, "name", None), "comparison_model": getattr(comparator, "model", None),
                  "comparison_prompt_version": COMPARE_PV, "summarizer": getattr(summarizer, "name", None),
                  "summary_model": getattr(summarizer, "model", None), "summary_prompt_version": SUMMARY_PV,
                  "embedder": to_jsonable(embedder.identity()) if hasattr(embedder, "identity") else str(embedder)},
        "settings": {"similarity": asdict(sim), "variant_overrides": VARIANTS[variant], "summary": asdict(summary_settings),
                     "attempts": attempts, "conflict_policy": conflicts},
        "data": {"populate_file": POPULATE_FILE.name,
                 "populate_sha256": hashlib.sha256(POPULATE_FILE.read_bytes()).hexdigest(),
                 "slice": {"from": lo, "to": hi, "about": about}, "records": len(ids),
                 "limit_articles": limit_articles, "limit_events": limit_events},
        "database": dbname if keep_db else None,
        "started_at": started.isoformat(timespec="seconds"), "seconds": round(time.monotonic() - clock, 1),
        "events_total": len(live), "events_summarized": len(events),
        "incomplete": len(events) < len(live) or any(e["result"] == "error" for e in events),
        "metrics": {**label_free_metrics(step3, gate, log.calls),
                    "summaries": summ.summary_metrics(events) if events else None},
        "review_aids": review_aids(step3, gate),
        "rubric": summ.RUBRIC,
        "step3": step3,
        "events": events,
    }
    report["file"] = None
    if save:
        path = _save(report, stem, log)
        summ.write_score_sheet({"events": [{**e, "checks": {**e["checks"], "conflict_cited": True if e["conflict"]["held"]
                                                             else None}} for e in events]},
                               path.with_name(f"{stem}_scores.csv"))
        _label_sheet(report, path.with_name(f"{stem}_grouping_labels.csv"))
        report["file"] = path.name
        progress(f"saved {path}")
    return report


# ----------------------------------------------------------------------------- the fixture run, enriched
def _drop(dbname: str) -> None:
    server = load_db_settings()
    with psycopg.connect(replace(server, dbname="postgres").conninfo(), autocommit=True) as admin:
        admin.execute(f"DROP DATABASE IF EXISTS {dbname} WITH (FORCE)")


def run_fixtures(name: str, *, summarizer, sim_settings, summary_settings, comparator=None, variant: str = "production",
                 limit_events: int | None = None, attempts: int = 2, keep_db: bool = False, embedder=None,
                 source: str | None = None, save: bool = True, progress: Callable[[str], None] = print) -> dict[str, Any]:
    """The existing 125-article summarization run (its own throwaway database), with the call log and step-3 detail."""
    sim = replace(sim_settings, enabled=True, **VARIANTS[variant])
    log = LLMCallLog()
    rec_sum = RecordingSummarizer(summarizer, log)
    rec_cmp = RecordingComparator(comparator, log) if comparator is not None else None
    full_name = f"{name}_fixtures-{variant.replace('_', '-')}"
    with log:
        report = summ.run(full_name, rec_sum, sim, summary_settings, comparator=rec_cmp, embedder=embedder,
                          description=f"125 test articles, full detail ({variant})", source=source, attempts=attempts,
                          limit_events=limit_events, keep_database=True, progress=progress)
    dbname = report["database"]
    files = {}
    for f in read_fixtures(FIXTURE_DIR):
        article = json.loads((FIXTURE_DIR / f.filename).read_text(encoding="utf-8"))
        files[str(article["id"])] = f
    db = Database(replace(load_db_settings(), dbname=dbname))
    try:
        names = {e["event_id"]: e["event"] for e in report["events"]}

        def describe(a: dict[str, Any]) -> dict[str, Any]:
            f = files.get(str(a["source_record_id"]))
            return {"key": f.filename if f else f"alpaca:{a['source_record_id']}",
                    "fixture_type": f.fixture_type.value if f else None, "group": f.expected_group if f else None}
        step3 = step3_detail(db, describe, None, names)
        for e in report["events"]:
            e["database"] = _summary_db_detail(db, e["event_id"])
    finally:
        db.close()
        if not keep_db:
            _drop(dbname)
            report["database"] = None
    for c in log.calls:
        if c["tag"].get("event_id") in names:
            c["tag"]["event"] = names[c["tag"]["event_id"]]
        if c.get("event_id") in names:
            c["event"] = names[c["event_id"]]
    report["step3"] = step3
    report["settings"]["variant_overrides"] = VARIANTS[variant]
    report["metrics_step3"] = label_free_metrics(step3, sim.min_semantic_similarity, log.calls)
    report["review_aids"] = review_aids(step3, sim.min_semantic_similarity)
    stem = f"{datetime.now().strftime('%Y-%m-%d_%H%M')}_{full_name}"
    report["file"] = None
    if save:
        path = _save(report, stem, log)
        summ.write_score_sheet(report, path.with_name(f"{stem}_scores.csv"))
        report["file"] = path.name
        progress(f"saved {path}\nread it: uv run datasources-eval report {stem}")
    return report


# ----------------------------------------------------------------------------- what an experiment file calls
def run_experiment(name: str, build_summarizer: Callable[[Any], Any], build_comparator: Callable[[Any], Any], *,
                   mode: str = "real", slice_key: str = "aug17", variants: list[str] | None = None,
                   grouping: str = "answer_key", conflicts: str = "assume_confirmed", limit_articles: int | None = None,
                   limit_events: int | None = None, attempts: int = 2, save: bool = True, keep_db: bool = False,
                   description: str = "", source: str | None = None) -> dict[str, Any]:
    """Run the chosen mode and variants; in real mode with both variants, also write the filter comparison.

    build_summarizer / build_comparator   settings -> backend (the comparator is used for real mode, and for
                                          fixtures only with grouping="model")
    """
    import re

    from ..similarity.embeddings import build_embedding_backend
    from ..similarity.settings import load_similarity_settings
    from ..summary.settings import load_summary_settings

    if not re.fullmatch(r"[A-Za-z0-9._-]+", name):
        raise ValueError(f"NAME {name!r} may use only letters, digits, '.', '_' and '-'")
    variants = variants or ["production"]
    unknown = [v for v in variants if v not in VARIANTS]
    if unknown:
        raise ValueError(f"unknown filter variant(s) {unknown}; use {', '.join(VARIANTS)}")
    sim = replace(load_similarity_settings(), enabled=True)
    summary = load_summary_settings()
    summarizer = build_summarizer(summary)
    embedder = build_embedding_backend(sim)  # loaded once, shared by every variant
    print(f"Experiment {name}: {description} (mode {mode}, model {getattr(summarizer, 'model', '?')})")
    runs: dict[str, Any] = {}
    if mode == "fixtures":
        for v in variants if grouping == "model" else variants[:1]:  # the filter only matters when a model groups
            runs[v] = run_fixtures(name, summarizer=summarizer, sim_settings=sim, summary_settings=summary,
                                   comparator=build_comparator(sim) if grouping == "model" else None, variant=v,
                                   limit_events=limit_events, attempts=attempts, keep_db=keep_db, embedder=embedder,
                                   source=source, save=save)
        return runs
    comparator = build_comparator(sim)
    for v in variants:
        runs[v] = run_real(name, comparator=comparator, summarizer=summarizer, sim_settings=sim,
                           summary_settings=summary, slice_key=slice_key, variant=v, limit_articles=limit_articles,
                           limit_events=limit_events, attempts=attempts, conflicts=conflicts, keep_db=keep_db,
                           embedder=embedder, source=source, save=save, extra={"description": description})
    if {"production", "no_filter"} <= set(runs):
        cmp = compare_filters(runs["production"], runs["no_filter"])
        print(f"filter comparison: {len(cmp['linked_only_without_filter'])} link(s) only without the filter, "
              f"{len(cmp['linked_only_with_filter'])} only with it, pair agreement {cmp['pair_agreement_rand_index']}")
        if save:
            path = RESULTS_DIR / f"{datetime.now().strftime('%Y-%m-%d_%H%M')}_{name}_real-{slice_key}_filter-comparison.json"
            path.write_text(json.dumps(to_jsonable(cmp), indent=1, default=str, ensure_ascii=False) + "\n",
                            encoding="utf-8")
            print(f"saved {path}")
        runs["filter_comparison"] = cmp
    return runs
