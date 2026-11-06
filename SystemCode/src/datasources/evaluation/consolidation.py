"""Stage 1 evaluation: event grouping and relation labels, one independent batch at a time.

Experiment files (evaluation/consolidation/experiments/*.py) call `run_experiment(...)`
from their `if __name__ == "__main__":` block; this module is the single place that
runs the pipeline for them.

For each batch of five test files (`evaluation.fixture_file` / populate/event_grouping/):

    1. create a throwaway database `stock_news_eval_..._test`, migrate it, register the
       coverage companies (as production has them), so no events from other batches exist
    2. feed the five articles in test-number order through the real ingestion service,
       with grouping on and the experiment's comparator plugged in
    3. read back each article's event memberships and relation label
    4. drop the database (unless keep_databases)

The answer key (file name) is never shown to the model: only the article goes in.

The report is saved after every batch, so a stopped run keeps what it finished. `resume=<saved run>` keeps the
batches of that run whose five articles all completed and reruns the others from their first article (a batch is
stateful, so it is never patched article by article); the model, prompt, embedder and retrieval settings must
match the saved run. `verbose=True` logs each stage as it happens (see `enable_verbose`).
"""

from __future__ import annotations

import json
import logging
import re
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import psycopg

from ..db import Database, load_db_settings
from ..db import migrate as migrations
from ..db.companies import coverage_companies, register_companies
from ..db.evaluation import FIXTURE_DIR, Fixture, FixtureType, read_fixtures
from ..db.repository import ALPACA_NAMESPACE
from . import metrics

LOGGER = logging.getLogger(__name__)  # stage logs, shown with verbose=True (DEBUG); never keys or text
BATCH_SIZE = 5
# Retrieval settings (SIMILARITY_* in .env) that change which articles reach the model; saved with every run.
RECORDED_SETTINGS = ("window_days", "min_semantic_similarity", "lexical_plausible_rank", "semantic_top_k",
                     "lexical_top_k", "max_candidate_events", "exact_match_min_chars", "no_company_window_hours")
EXPECTED_LABEL = {
    FixtureType.REPEAT: "same_event_repeated",
    FixtureType.UPDATE: "same_event_update",
    FixtureType.CONFLICT: "same_event_conflict",
    FixtureType.DIFFERENT: "different_event",
    FixtureType.ORIGINAL: "different_event",  # an original that is not first in its batch opens its own event
}

PREDICTIONS_SQL = """
SELECT d.source_record_id AS article_id, a.status AS analysis_status, a.last_error, a.limitations,
       m.event_id::text AS event_id, m.relation, m.state, m.created_at
  FROM document d
  JOIN document_revision r ON r.revision_id = d.current_revision_id
  LEFT JOIN revision_analysis a ON a.revision_id = r.revision_id
  LEFT JOIN event_membership m ON m.revision_id = r.revision_id
 WHERE d.source_namespace = %s
 ORDER BY m.created_at
"""


# The model's reasoning for each article: one row per (segment, candidate event) it judged. Kept in the
# report so every run's explanations travel with its scores, without having to keep the batch database.
COMPARISONS_SQL = """
SELECT d.source_record_id AS article_id, c.segment_key, c.candidate_event_id::text AS candidate_event_id,
       c.label, c.method, c.explanation, c.matching_facts, c.differences, c.evidence,
       c.insufficient_context, c.uncertainty, c.validation_notes
  FROM document d
  JOIN document_revision r ON r.revision_id = d.current_revision_id
  JOIN event_comparison c ON c.revision_id = r.revision_id
 WHERE d.source_namespace = %s
 ORDER BY c.created_at
"""


def _judgment_row(c: dict[str, Any]) -> dict[str, Any]:
    """One comparison row from `event_comparison`, shaped for the report (candidate_event filled in later)."""
    return {"candidate_event": c["candidate_event_id"], "label": c["label"], "method": c["method"],
            "explanation": c["explanation"], "matching_facts": c["matching_facts"],
            "differences": c["differences"], "evidence": c["evidence"],
            "insufficient_context": c["insufficient_context"], "uncertainty": c["uncertainty"],
            "validation_notes": c["validation_notes"]}


def batches(fixtures: list[Fixture]) -> dict[int, list[Fixture]]:
    grouped: dict[int, list[Fixture]] = {}
    for f in sorted(fixtures, key=lambda f: f.test_number):
        grouped.setdefault((f.test_number - 1) // BATCH_SIZE + 1, []).append(f)
    return grouped


NO_JUDGMENT = "no_judgment"


def _predicted_label(rows: list[dict[str, Any]]) -> tuple[frozenset[str], str]:
    """(active events, label) for one article from its membership rows."""
    if rows and rows[0]["analysis_status"] not in (None, "completed"):
        # The model answered but left a requested pair out (an empty judgments list; qwen2.5:7b does this for a few
        # articles per run). Its answer, scored as a miss; not a failed run like a timeout or schema reject.
        if (rows[0]["analysis_status"] == "partial" and not rows[0]["last_error"]
                and any("were not judged" in note for note in rows[0]["limitations"] or [])):
            return frozenset(), NO_JUDGMENT
        return frozenset(), f"error:{rows[0]['analysis_status']}"
    memberships = [r for r in rows if r["event_id"]]
    active = frozenset(r["event_id"] for r in memberships if r["state"] == "active")
    linked = [r["relation"] for r in memberships if r["state"] == "active" and r["relation"] != "origin"]
    if linked:
        return active, linked[0]
    if any(r["relation"] == "uncertain" for r in memberships):
        return active, "uncertain"
    return active, "different_event" if memberships else "no_event"


class BatchDatabase:
    """A fresh, migrated database for one batch, dropped afterwards."""

    def __init__(self, label: str, *, keep: bool = False):
        self.server = load_db_settings()
        self.name = f"stock_news_eval_{re.sub(r'[^a-z0-9]+', '_', label.lower())}_{uuid.uuid4().hex[:6]}_test"
        self.keep = keep
        self.database: Database | None = None

    def _admin(self, sql: str) -> None:
        with psycopg.connect(replace(self.server, dbname="postgres").conninfo(), autocommit=True) as admin:
            admin.execute(sql)

    def __enter__(self) -> Database:
        started = time.monotonic()
        self._admin(f"CREATE DATABASE {self.name}")
        self.database = Database(replace(self.server, dbname=self.name))
        applied = migrations.apply_migrations(self.database)
        with self.database.transaction() as conn:
            register_companies(conn, coverage_companies())
        LOGGER.debug("database %s ready in %.1fs (%d migrations)", self.name, time.monotonic() - started,
                     len(applied or ()))
        return self.database

    def __exit__(self, *exc) -> None:
        if self.database is not None:
            self.database.close()
        if not self.keep:
            self._admin(f"DROP DATABASE IF EXISTS {self.name} WITH (FORCE)")
        LOGGER.debug("database %s %s", self.name, "kept" if self.keep else "dropped")


TRACE_SQL = """
SELECT a.stage_status, c.label, c.method, c.validation_notes
  FROM document d
  JOIN document_revision r ON r.revision_id = d.current_revision_id
  LEFT JOIN revision_analysis a ON a.revision_id = r.revision_id
  LEFT JOIN event_comparison c ON c.revision_id = r.revision_id
 WHERE d.source_namespace = %s AND d.source_record_id = %s
 ORDER BY c.created_at
"""
TRACED_STAGES = ("text_units", "embeddings", "retrieval", "route", "comparison", "resolution")


def _log_article(db, article_id: str, filename: str, expected: str | None, seconds: float) -> None:
    """Verbose: what the pipeline did with one article (stage statuses, judgments, label vs expected)."""
    with db.connection() as conn:
        rows = [dict(r) for r in conn.execute(TRACE_SQL, (ALPACA_NAMESPACE, article_id)).fetchall()]
        memberships = [dict(r) for r in conn.execute(PREDICTIONS_SQL, (ALPACA_NAMESPACE,)).fetchall()
                       if r["article_id"] == article_id]
    stages = (rows[0]["stage_status"] if rows else None) or {}
    LOGGER.debug("%s: stages %s", filename, ", ".join(f"{k} {stages[k]}" for k in TRACED_STAGES if k in stages) or "-")
    for r in rows:
        if r["label"]:
            notes = "; ".join(r["validation_notes"] or []) or "ok"
            LOGGER.debug("%s: judgment %s (%s), validation %s", filename, r["label"], r["method"], notes[:200])
    events, label = _predicted_label(memberships)
    outcome = "-" if expected is None else ("ok" if expected == label else "MISS")
    LOGGER.debug("%s: done in %.1fs, predicted %s, expected %s, %d active event(s), %s", filename, seconds,
                 label, expected or "- (first article)", len(events), outcome)


def run_batch(number: int, items: list[Fixture], name: str, comparator, embedder, sim_settings, *,
              directory: Path, keep_database: bool = False) -> dict[str, Any]:
    from ..ingest.cli import _process_alpaca_payload  # exactly what `datasources-ingest process-file` runs
    from ..ingest.service import IngestionService
    from ..ingest.settings import load_settings
    from ..similarity.pipeline import build_runtime

    articles = {f.test_number: json.loads((directory / f.filename).read_text(encoding="utf-8")) for f in items}
    started = time.monotonic()
    with BatchDatabase(f"{name}_b{number:02d}", keep=keep_database) as db, \
            tempfile.TemporaryDirectory(prefix="eval_") as root:
        analyzer = build_runtime(db, sim_settings, embedder=embedder, comparator=comparator)
        service = IngestionService(load_settings(demo=False).with_overrides(output_root=Path(root)),
                                   database=db, analyzer=analyzer)
        for position, f in enumerate(items):  # test-number order: the original opens the event, the rest follow
            article_started = time.monotonic()
            LOGGER.debug("batch %d article %d/%d %s: started", number, position + 1, len(items), f.filename)
            # the pipeline's file input, as `datasources-ingest process-file <file> --provider alpaca`
            _process_alpaca_payload(service, articles[f.test_number], source=directory / f.filename,
                                    restrict_symbols=False)
            if LOGGER.isEnabledFor(logging.DEBUG):  # verbose only: read-only queries, nothing else changes
                _log_article(db, str(articles[f.test_number]["id"]), f.filename,
                             None if position == 0 else EXPECTED_LABEL[f.fixture_type],
                             time.monotonic() - article_started)
        with db.connection() as conn:
            rows = conn.execute(PREDICTIONS_SQL, (ALPACA_NAMESPACE,)).fetchall()
            comparison_rows = conn.execute(COMPARISONS_SQL, (ALPACA_NAMESPACE,)).fetchall()
            usage = conn.execute("SELECT count(*) AS calls, coalesce(sum((usage->>'input_tokens')::int), 0) AS input, "
                                 "coalesce(sum((usage->>'output_tokens')::int), 0) AS output "
                                 "FROM event_comparison").fetchone()
        database_name = db.settings.dbname
    by_article: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_article.setdefault(row["article_id"], []).append(dict(row))
    judgments_by_article: dict[str, list[dict[str, Any]]] = {}
    for row in comparison_rows:
        judgments_by_article.setdefault(row["article_id"], []).append(_judgment_row(dict(row)))
    predictions, table = [], []
    for position, f in enumerate(items):
        article_rows = by_article.get(str(articles[f.test_number]["id"]), [])
        events, label = _predicted_label(article_rows)
        expected = None if position == 0 else EXPECTED_LABEL[f.fixture_type]
        predictions.append(metrics.Prediction(f.test_number, number, f.expected_group, expected, events, label))
        table.append({"file": f.filename, "type": f.fixture_type.value, "expected_group": f.expected_group,
                      "expected_label": expected, "predicted_label": label, "predicted_events": sorted(events),
                      "error": next((r["last_error"] for r in article_rows if r.get("last_error")), None),
                      "judgments": judgments_by_article.get(str(articles[f.test_number]["id"]), [])})
    # readable event names per batch: E1, E2, ... in order of first appearance
    names: dict[str, str] = {}
    for row in table:
        row["predicted_events"] = [names.setdefault(e, f"E{len(names) + 1}") for e in row["predicted_events"]]
        for j in row["judgments"]:  # same E# labels so the reasons line up with predicted_events
            j["candidate_event"] = names.setdefault(j["candidate_event"], f"E{len(names) + 1}")
    return {"batch": number, "seconds": round(time.monotonic() - started, 1), "articles": table,
            "comparison_calls": usage["calls"], "tokens": {"input": usage["input"], "output": usage["output"]},
            "database": database_name if keep_database else None, "_predictions": predictions}


def format_article_line(row: dict[str, Any]) -> str:
    """One article's result line, shown live and by `datasources-eval report` (identical format)."""
    mark = "ok" if row["expected_label"] in (None, row["predicted_label"]) else "MISS"
    return (f"    {row['file']:<38} expected {str(row['expected_label']):<20} got "
            f"{row['predicted_label']:<20} {'/'.join(row['predicted_events']) or '-':<6} {mark}")


def print_scores(scores: dict[str, Any], usage: dict[str, Any], progress: Callable[[str], None] = print) -> None:
    """The grouping/label score block, shown live and by `datasources-eval report` (identical format)."""
    progress(f"\nGrouping (pairwise): F1 {scores['grouping_pairwise']['f1']:.3f}  "
             f"P {scores['grouping_pairwise']['precision']:.3f}  R {scores['grouping_pairwise']['recall']:.3f}")
    progress(f"Labels: macro F1 {scores['labels']['macro_f1']:.3f}  accuracy {scores['labels']['accuracy']:.3f}  "
             f"({scores['labels']['scored']} scored, {scores['labels']['uncertain']} uncertain)")
    for label, score in scores["labels"]["per_label"].items():
        progress(f"    {label:<22} F1 {score['f1']:.3f}  (tp {score['tp']}, fp {score['fp']}, fn {score['fn']})")
    progress(f"Model calls: {usage['comparison_calls']}, tokens in/out "
             f"{usage['input_tokens']}/{usage['output_tokens']}")


def _errored(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Articles the model did not finish (timeout, HTTP error, schema reject): processing failures, not answers."""
    return [a for a in articles if a.get("error") or str(a["predicted_label"]).startswith("error")]


def _saved_predictions(batch: dict[str, Any]) -> list[metrics.Prediction]:
    """A saved batch's articles as `metrics.Prediction`s, to re-score it next to new batches (resume)."""
    return [metrics.Prediction(int(re.match(r"test_(\d+)", a["file"]).group(1)), batch["batch"], a["expected_group"],
                               a["expected_label"], frozenset(a["predicted_events"]), a["predicted_label"])
            for a in batch["articles"]]


def resume_mismatches(saved: dict[str, Any], model: dict[str, Any], settings: dict[str, Any]) -> list[str]:
    """Why batches of `saved` can't be combined with a new run of this model and these settings ([] = they can)."""
    problems = [f"{key}: saved {saved['model'].get(key)!r}, now {model.get(key)!r}"
                for key in ("comparison_model", "prompt_version", "embedder")
                if saved["model"].get(key) != model.get(key)]
    problems += [f"setting {key}: saved {saved['settings'].get(key)!r}, now {value!r}"
                 for key, value in settings.items() if saved["settings"].get(key) != value]
    return problems


def run(name: str, comparator, sim_settings, *, embedder=None, description: str = "", source: str | None = None,
        batch_numbers: list[int] | None = None, keep_databases: bool = False, directory: Path | None = None,
        progress: Callable[[str], None] = print, resume_from: dict[str, Any] | None = None,
        on_batch: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:
    """Run `comparator` through the real pipeline on the chosen batches and score it. Returns the report.

    resume_from  a saved report: its batches whose articles all completed are kept as saved, the others rerun
    on_batch     called with the report so far after every batch (run_experiment saves it there)
    """
    from ..similarity.comparison import PROMPT_VERSION
    from ..similarity.embeddings import build_embedding_backend

    directory = directory or FIXTURE_DIR
    all_batches = batches(read_fixtures(directory))
    chosen = batch_numbers or sorted(all_batches)
    unknown = [n for n in chosen if n not in all_batches]
    if unknown:
        raise ValueError(f"no such batch: {unknown} (have {min(all_batches)}..{max(all_batches)})")
    sim_settings = replace(sim_settings, enabled=True)
    embedder = embedder or build_embedding_backend(sim_settings)
    model = {"comparator": getattr(comparator, "name", type(comparator).__name__),
             "comparison_model": getattr(comparator, "model", None), "prompt_version": PROMPT_VERSION,
             "embedder": to_jsonable(embedder.identity()) if hasattr(embedder, "identity") else str(embedder)}
    settings = {k: getattr(sim_settings, k) for k in RECORDED_SETTINGS}  # from .env: scores depend on them
    kept: dict[int, dict[str, Any]] = {}
    if resume_from is not None:
        problems = resume_mismatches(resume_from, model, settings)
        if problems:
            raise ValueError("cannot resume: the saved run used a different setup:\n  " + "\n  ".join(problems))
        kept = {b["batch"]: b for b in resume_from["batches"] if b["batch"] in chosen and not _errored(b["articles"])}
        progress(f"resuming {resume_from['experiment']['name']} ({resume_from['started_at']}): keeping batches "
                 f"{sorted(kept) or 'none'}, running {[n for n in chosen if n not in kept] or 'none'}")
    started = datetime.now(timezone.utc)
    results: dict[int, dict[str, Any]] = {}
    predictions: dict[int, list[metrics.Prediction]] = {}

    def report() -> dict[str, Any]:
        done = [results[n] for n in chosen if n in results]
        scored = [p for n in chosen for p in predictions.get(n, [])]
        # A run where the model failed to produce a usable answer for some article (timeout, schema reject -> the
        # pipeline leaves the analysis "pending") is not a trustworthy benchmark, and neither is an unfinished one.
        # Flag it so the leaderboard skips it, while the file stays on disk as a log (`datasources-eval report`).
        errored = [a for r in done for a in _errored(r["articles"])]
        missing = [n for n in chosen if n not in results]
        out = {
            "stage": "consolidation",
            "experiment": {"name": name, "description": description, "file": source},
            "model": model,
            "settings": settings,
            "started_at": started.isoformat(timespec="seconds"),
            "batches_run": chosen,
            "incomplete": bool(errored or missing),
            "metrics": {"grouping_pairwise": metrics.pairwise(scored), "labels": metrics.labels(scored)},
            "usage": {"comparison_calls": sum(r["comparison_calls"] for r in done),
                      "input_tokens": sum(r["tokens"]["input"] for r in done),
                      "output_tokens": sum(r["tokens"]["output"] for r in done)},
            "batches": done,
        }
        if resume_from is not None:
            out["resumed_from"] = {"experiment": resume_from["experiment"]["name"],
                                   "started_at": resume_from["started_at"], "kept_batches": sorted(kept)}
        if errored:
            out["incomplete_reason"] = (f"{len(errored)} article(s) did not complete, e.g. {errored[0]['file']}: "
                                        f"{errored[0]['predicted_label']}")
        elif missing:
            out["incomplete_reason"] = f"stopped before batches {missing}"
        return out

    for n in chosen:
        if n in kept:
            results[n], predictions[n] = kept[n], _saved_predictions(kept[n])
            progress(f"batch {n:>2}: kept from the saved run")
        else:
            progress(f"batch {n:>2}: {', '.join(f.filename for f in all_batches[n])}")
            result = run_batch(n, all_batches[n], name, comparator, embedder, sim_settings, directory=directory,
                               keep_database=keep_databases)
            predictions[n] = result.pop("_predictions")
            results[n] = result
        for row in results[n]["articles"]:
            progress(format_article_line(row))
        if on_batch is not None:
            on_batch(report())
    return report()


RESULTS_DIR = Path(__file__).resolve().parents[3] / "evaluation" / "consolidation" / "results"


def load_run(selector: str) -> dict[str, Any]:
    """A saved report by path, file name or stem in RESULTS_DIR (e.g. "2026-10-24_1709_gemini")."""
    for path in (Path(selector), RESULTS_DIR / selector, RESULTS_DIR / f"{selector}.json"):
        if path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))
    raise FileNotFoundError(f"no saved run {selector!r} in {RESULTS_DIR}")


def enable_verbose(stream=None) -> None:
    """Print the evaluation's stage logs and the OpenAI-compatible HTTP calls as they happen (DEBUG, one line each).

    Only names, counts, labels, statuses and timings are logged: never a key, a header, a prompt or article text.
    """
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
    for logger_name in ("datasources.evaluation", "datasources.llm_openai"):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        if not any(getattr(h, "_eval_verbose", False) for h in logger.handlers):
            handler._eval_verbose = True  # one handler per logger, even if enabled twice
            logger.addHandler(handler)
        logger.propagate = False  # printed once, not again by a root handler


def run_experiment(name: str, build_comparator: Callable[[Any], Any], *, description: str = "",
                   batches: list[int] | None = None, save: bool = True, keep_db: bool = False,
                   build_embedder: Callable[[Any], Any] | None = None, source: str | None = None,
                   resume: str | None = None, verbose: bool = False) -> dict[str, Any]:
    """What an experiment file's `__main__` block calls: build the model, run, print the scores, save the report.

    batches   batch numbers to run (1 = tests 001-005); None or [] = all
    save      write evaluation/consolidation/results/<date>_<time>_<name>.json, updated after every batch
    keep_db   keep each batch's test database for inspection
    resume    a saved run (stem, file name or path): keep its completed batches, rerun the rest
    verbose   log every stage as it happens
    """
    from ..similarity.settings import load_similarity_settings

    if not re.fullmatch(r"[A-Za-z0-9._-]+", name):
        raise ValueError(f"NAME {name!r} may use only letters, digits, '.', '_' and '-'")
    if verbose:
        enable_verbose()
    settings = replace(load_similarity_settings(), enabled=True)
    print(f"Experiment {name}: {description}")
    path = RESULTS_DIR / f"{datetime.now().strftime('%Y-%m-%d_%H%M')}_{name}.json"

    def write(report: dict[str, Any]) -> None:
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")  # a crash mid-write never leaves a half file
        temporary.write_text(json.dumps(to_jsonable(report), indent=2, default=str) + "\n", encoding="utf-8")
        temporary.replace(path)

    report = run(name, build_comparator(settings), settings, description=description,
                 source=Path(source).name if source else None,
                 embedder=build_embedder(settings) if build_embedder else None, batch_numbers=batches or None,
                 keep_databases=keep_db, resume_from=load_run(resume) if resume else None,
                 on_batch=write if save else None)
    print_scores(report["metrics"], report["usage"])
    if save:
        write(report)
        print(f"Saved {path}" + (" (incomplete: resume it with RESUME = "
                                 f"{path.stem!r})" if report["incomplete"] else ""))
    return report


def to_jsonable(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return to_jsonable(asdict(value))
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    return value
