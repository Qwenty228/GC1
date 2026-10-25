"""Resume an interrupted consolidation run batch by batch, keeping every batch that already completed.

The evaluation is stateful inside each five-article batch (an article can only join events that earlier
articles of its batch opened), so the unit of recovery is the whole batch: a batch in which any article
did not complete is rerun from its first article in a fresh test database (`consolidation.run_batch`), and
a batch whose five articles all completed is kept exactly as it was saved.

    plan(...)          which batches of a saved run are complete, which need a rerun, and why
    CheckpointStore    one file per recovered batch plus manifest.json, written atomically right after the
                       batch completes: a crash or a stopped run loses at most the batch in flight
    recover(...)       rerun the pending batches, checkpoint each complete one, stop cleanly when the
                       comparator says the run cannot go on (quota exhausted, no usable project)
    recover_all(...)   `recover` until nothing is pending: on such a stop, `CapacityWaiter` sleeps until the
                       earliest permitted quota reset (or a short outage pause) and the run goes on, the
                       batch in flight restarting from its first article
    combine(...)       original complete batches + recovered ones -> one report, re-scored with metrics.py;
                       "incomplete": false only for 25 unique batches / 125 articles without an error

Only processing failures are recovered (error:pending after an HTTP 429 / 503 / timeout ...). A wrong
label or `uncertain` is the model's answer: it is kept and scored, never retried.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from filelock import FileLock

from ..db.evaluation import Fixture
from ..ingest.store import atomic_write_json
from . import metrics
from .consolidation import EXPECTED_LABEL, RECORDED_SETTINGS, format_article_line, to_jsonable
from .gemini_projects import iso, utcnow

MANIFEST_KIND = "consolidation_recovery_manifest"
# Settings that change what reaches the model but are not in a report's "settings": locked by the manifest
# so every recovered batch of one recovery runs with the same values. The client timeout is not one of them:
# it only decides how long to wait for an answer, never what is asked or answered.
LOCKED_LLM_SETTINGS = ("llm_max_output_tokens", "llm_max_input_chars", "llm_max_events_per_call",
                       "llm_max_calls_per_record", "llm_token_budget_per_record")


class ResumeError(RuntimeError):
    """The saved run, the fixtures, the checkpoints or the configuration do not fit together."""


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# ----------------------------------------------------------------------------- one article / one batch
def article_failed(row: dict[str, Any]) -> bool:
    """The test `consolidation.run` uses to flag a run incomplete: an error, or an error:<status> label."""
    return bool(row.get("error")) or str(row.get("predicted_label")).startswith("error")


def failure_kind(row: dict[str, Any]) -> str | None:
    """Why an article did not complete, from its saved error text; None when it completed."""
    if not article_failed(row):
        return None
    error = str(row.get("error") or "")
    status = re.search(r"\bHTTP (\d{3})\b", error)
    if status:
        return f"http_{status.group(1)}"
    for needle, kind in (("timed out", "timeout"), ("connection failed", "connection"),
                         ("did not match the schema", "invalid_response"), ("truncated", "truncated")):
        if needle in error:
            return kind
    return "other_error" if error else str(row.get("predicted_label"))  # e.g. error:pending, no message


def article_outcome(row: dict[str, Any]) -> str:
    """failed:<kind> | first_article (not scored) | uncertain | correct | incorrect (a genuine model error)."""
    kind = failure_kind(row)
    if kind:
        return f"failed:{kind}"
    if row.get("expected_label") is None:
        return "first_article"
    if row.get("predicted_label") == "uncertain":
        return "uncertain"
    return "correct" if row.get("predicted_label") == row.get("expected_label") else "incorrect"


def answer_key_problems(batch: dict[str, Any], items: list[Fixture]) -> list[str]:
    """Where a saved batch does not match its fixtures (files, order, expected group and label)."""
    rows = batch.get("articles") or []
    files, expected = [r.get("file") for r in rows], [f.filename for f in items]
    if files != expected:
        return [f"batch {batch.get('batch')} holds {files}, the fixtures are {expected}"]
    problems = []
    for position, (row, f) in enumerate(zip(rows, items)):
        label = None if position == 0 else EXPECTED_LABEL[f.fixture_type]
        if row.get("expected_label") != label or row.get("expected_group") != f.expected_group:
            problems.append(f"{f.filename}: expected {row.get('expected_label')}/group {row.get('expected_group')} "
                            f"in the file, {label}/group {f.expected_group} in the fixtures")
    return problems


def failed_articles(batch: dict[str, Any]) -> list[str]:
    return [f"{r.get('file')}: {r.get('predicted_label')}" for r in batch.get("articles") or [] if article_failed(r)]


def predictions_from_batch(batch: dict[str, Any], test_numbers: dict[str, int]) -> list[metrics.Prediction]:
    """The Prediction objects back from a saved batch. Event names (E1, E2 ...) are per batch, like the ids
    they replace, and pairwise grouping only compares articles inside one batch, so scores are unchanged."""
    return [metrics.Prediction(test_numbers[row["file"]], batch["batch"], row["expected_group"],
                               row["expected_label"], frozenset(row["predicted_events"]), row["predicted_label"])
            for row in batch["articles"]]


def score(batches: list[dict[str, Any]], test_numbers: dict[str, int]) -> dict[str, Any]:
    predictions = [p for b in batches for p in predictions_from_batch(b, test_numbers)]
    return {"grouping_pairwise": metrics.pairwise(predictions), "labels": metrics.labels(predictions)}


def usage_of(batches: list[dict[str, Any]]) -> dict[str, int]:
    return {"comparison_calls": sum(b.get("comparison_calls", 0) for b in batches),
            "input_tokens": sum(b.get("tokens", {}).get("input", 0) for b in batches),
            "output_tokens": sum(b.get("tokens", {}).get("output", 0) for b in batches)}


# ----------------------------------------------------------------------------- configuration
def run_configuration(*, model: str, base_url: str, json_mode: str, settings) -> dict[str, Any]:
    """Everything a recovered batch must share with the original run: model, prompt, schema, retrieval."""
    from ..similarity.comparison import PROMPT_VERSION, RESPONSE_SCHEMA, SYSTEM_PROMPT

    return {"comparison_model": model, "base_url": base_url, "json_mode": json_mode,
            "prompt_version": PROMPT_VERSION, "prompt_sha256": sha256_text(SYSTEM_PROMPT),
            "schema_sha256": sha256_text(json.dumps(RESPONSE_SCHEMA, sort_keys=True)),
            "embedding_model": settings.embedding_model, "embedding_revision": settings.embedding_revision,
            "settings": {k: getattr(settings, k) for k in RECORDED_SETTINGS},
            "llm_settings": {k: getattr(settings, k) for k in LOCKED_LLM_SETTINGS}}


def source_mismatches(source: dict[str, Any], configuration: dict[str, Any]) -> list[str]:
    """What the current configuration changes compared with the saved run (must be nothing)."""
    model, problems = source.get("model") or {}, []
    embedder = model.get("embedder") or {}
    for what, saved, now in (("comparison model", model.get("comparison_model"), configuration["comparison_model"]),
                             ("prompt version", model.get("prompt_version"), configuration["prompt_version"]),
                             ("embedding model", embedder.get("model_name"), configuration["embedding_model"]),
                             ("embedding revision", embedder.get("model_version"),
                              configuration["embedding_revision"])):
        if saved != now:
            problems.append(f"{what}: saved run {saved!r}, now {now!r}")
    saved_settings = source.get("settings") or {}
    for key, now in configuration["settings"].items():
        if saved_settings.get(key) != now:
            problems.append(f"SIMILARITY_{key.upper()}: saved run {saved_settings.get(key)!r}, now {now!r}")
    return problems


def embedder_mismatch(source: dict[str, Any], embedder) -> str | None:
    saved = (source.get("model") or {}).get("embedder")
    now = to_jsonable(embedder.identity()) if hasattr(embedder, "identity") else str(embedder)
    return None if saved == now else f"embedder identity: saved run {saved!r}, now {now!r}"


# ----------------------------------------------------------------------------- the plan
@dataclass
class BatchStatus:
    number: int
    files: list[str]
    state: str  # original (complete in the saved run) | recovered (checkpoint) | pending (to rerun)
    failures: dict[str, int] = field(default_factory=dict)  # saved run: failure kind -> articles
    outcomes: dict[str, int] = field(default_factory=dict)  # saved run: article_outcome -> articles


@dataclass
class Plan:
    batches: dict[int, BatchStatus]
    saved: dict[int, dict[str, Any]]  # the saved run's batches by number

    def numbers(self, state: str) -> list[int]:
        return [n for n, b in sorted(self.batches.items()) if b.state == state]

    @property
    def pending(self) -> list[int]:
        return self.numbers("pending")

    @property
    def complete(self) -> bool:
        return not self.pending


def plan(source: dict[str, Any], fixture_batches: dict[int, list[Fixture]],
         recovered: dict[int, dict[str, Any]]) -> Plan:
    """Complete saved batches stay; checkpointed ones stay; every other batch is pending."""
    saved: dict[int, dict[str, Any]] = {}
    for batch in source.get("batches") or []:
        n = batch.get("batch")
        if n in saved:
            raise ResumeError(f"batch {n} appears twice in the saved run")
        if n not in fixture_batches:
            raise ResumeError(f"the saved run has batch {n}, the fixtures have {min(fixture_batches)}.."
                              f"{max(fixture_batches)}")
        problems = answer_key_problems(batch, fixture_batches[n])
        if problems:
            raise ResumeError(f"the saved run does not match the fixtures: {problems[0]}")
        saved[n] = batch
    statuses = {}
    for n, items in sorted(fixture_batches.items()):
        batch = saved.get(n)
        rows = batch["articles"] if batch else []
        failures = (dict(Counter(failure_kind(r) for r in rows if article_failed(r))) if batch
                    else {"not_run": len(items)})
        if batch is not None and not failures:
            state = "original"  # never touched, even if a checkpoint exists
        elif n in recovered:
            state = "recovered"
        else:
            state = "pending"
        statuses[n] = BatchStatus(n, [f.filename for f in items], state, failures,
                                  dict(Counter(article_outcome(r) for r in rows)))
    return Plan(statuses, saved)


# ----------------------------------------------------------------------------- checkpoints
class CheckpointStore:
    """`directory`/manifest.json + batch_NN.json; every write goes to a temp file and is renamed into place."""

    def __init__(self, directory: Path, *, now: Callable = utcnow):
        self.directory = Path(directory)
        self.manifest_path = self.directory / "manifest.json"
        self.now = now

    def batch_path(self, number: int) -> Path:
        return self.directory / f"batch_{number:02d}.json"

    def lock(self) -> FileLock:
        """One recovery at a time per directory (released by the OS if the process dies)."""
        self.directory.mkdir(parents=True, exist_ok=True)
        return FileLock(str(self.directory / ".resume.lock"), timeout=0)

    def load_manifest(self) -> dict[str, Any] | None:
        if not self.manifest_path.is_file():
            return None
        return json.loads(self.manifest_path.read_text(encoding="utf-8"))

    def _write(self, manifest: dict[str, Any]) -> None:
        manifest["updated_at"] = iso(self.now())
        atomic_write_json(self.manifest_path, to_jsonable(manifest))

    def open(self, *, source_path: Path, source_sha256: str, configuration: dict[str, Any],
             fixture_checksums: dict[str, str], plan_: Plan, write: bool = True) -> dict[str, Any]:
        """This recovery's manifest, created on first use. A manifest for another run, another
        configuration or changed fixtures is refused: a recovery is never continued under different terms."""
        manifest = self.load_manifest()
        if manifest is None:
            manifest = {"kind": MANIFEST_KIND, "version": 1, "created_at": iso(self.now()), "updated_at": None,
                        "source": {"file": Path(source_path).name, "sha256": source_sha256},
                        "configuration": configuration, "fixtures": fixture_checksums,
                        "original_complete_batches": plan_.numbers("original"),
                        "batches_to_recover": [n for n, b in sorted(plan_.batches.items()) if b.state != "original"],
                        "recovered": {}, "failed_attempts": [], "stops": [], "final_report": None}
            if write:
                self._write(manifest)
            return manifest
        problems = []
        if manifest.get("kind") != MANIFEST_KIND:
            problems.append(f"{self.manifest_path} is not a recovery manifest")
        if (manifest.get("source") or {}).get("sha256") != source_sha256:
            problems.append(f"it belongs to {manifest.get('source', {}).get('file')!r} (another file or the saved run "
                            "changed)")
        if manifest.get("configuration") != configuration:
            saved = manifest.get("configuration") or {}
            changed = sorted(k for k in set(saved) | set(configuration) if saved.get(k) != configuration.get(k))
            problems.append(f"the configuration changed since the recovery started: {', '.join(changed)}")
        if manifest.get("fixtures") != fixture_checksums:
            saved = manifest.get("fixtures") or {}
            changed = sorted(k for k in set(saved) | set(fixture_checksums) if saved.get(k) != fixture_checksums.get(k))
            problems.append(f"fixture files changed: {', '.join(changed[:5])}{' ...' if len(changed) > 5 else ''}")
        if problems:
            raise ResumeError(f"cannot continue the recovery in {self.directory}: " + "; ".join(problems))
        return manifest

    def recovered(self, fixture_batches: dict[int, list[Fixture]], configuration_sha256: str
                  ) -> tuple[dict[int, dict[str, Any]], dict[str, str]]:
        """Checkpointed batches that are complete for their fixtures and configuration, and the rejected files."""
        good: dict[int, dict[str, Any]] = {}
        rejected: dict[str, str] = {}
        for path in sorted(self.directory.glob("batch_*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                batch, meta = data["batch"], data["recovery"]
                number = int(batch["batch"])
            except (ValueError, KeyError, TypeError, OSError) as error:
                rejected[path.name] = f"unreadable ({error})"
                continue
            if path.name != self.batch_path(number).name or number not in fixture_batches:
                rejected[path.name] = f"holds batch {number}"
            elif meta.get("configuration_sha256") != configuration_sha256:
                rejected[path.name] = "made with another configuration"
            elif problems := answer_key_problems(batch, fixture_batches[number]) + failed_articles(batch):
                rejected[path.name] = problems[0]
            else:
                good[number] = data
        return good, rejected

    def adopt(self, manifest: dict[str, Any], recovered: dict[int, dict[str, Any]]) -> None:
        """Batch files saved just before a crash, before the manifest was updated, are listed now."""
        missing = {str(n): {**data["recovery"], "file": self.batch_path(n).name}
                   for n, data in recovered.items() if str(n) not in manifest["recovered"]}
        if missing:
            manifest["recovered"].update(missing)
            self._write(manifest)

    def save_batch(self, manifest: dict[str, Any], number: int, batch: dict[str, Any], meta: dict[str, Any]) -> None:
        # the batch file first, then the manifest: a crash in between is repaired by `adopt`
        atomic_write_json(self.batch_path(number), to_jsonable({"batch": batch, "recovery": meta}))
        manifest["recovered"][str(number)] = {**meta, "file": self.batch_path(number).name}
        self._write(manifest)

    def record(self, manifest: dict[str, Any], key: str, entry: dict[str, Any]) -> None:
        manifest[key].append(entry)
        self._write(manifest)

    def finalize(self, manifest: dict[str, Any], path: Path) -> None:
        manifest["final_report"] = Path(path).name
        self._write(manifest)


# ----------------------------------------------------------------------------- the recovery loop
@dataclass
class RecoveryResult:
    completed: list[int]
    failed: dict[int, list[str]]  # batch -> failed articles of its last attempt (not recovered this run)
    stop: Any = None  # the comparator's Stop, if the run could not go on


def recover(pending: list[int], fixture_batches: dict[int, list[Fixture]], store: CheckpointStore,
            manifest: dict[str, Any], comparator, run_batch: Callable[[int, list[Fixture]], dict[str, Any]], *,
            model: str, configuration_sha256: str, attempts: int = 2,
            progress: Callable[[str], None] = print) -> RecoveryResult:
    """Rerun each pending batch from scratch (at most `attempts` times); checkpoint it as soon as all five
    articles completed. Stops before the next batch when the comparator has stopped the run."""
    completed: list[int] = []
    failed: dict[int, list[str]] = {}
    for number in pending:
        items = fixture_batches[number]
        for attempt in range(1, attempts + 1):
            if comparator.stop is not None:
                break
            progress(f"batch {number:>2} (attempt {attempt}/{attempts}): {', '.join(f.filename for f in items)}")
            comparator.begin_batch()
            result = run_batch(number, items)
            result.pop("_predictions", None)
            stats = dict(comparator.stats)
            for row in result["articles"]:
                progress(format_article_line(row))
            problems = failed_articles(result)
            entry = {"batch": number, "attempt": attempt, "at": iso(utcnow()), "model": model,
                     "projects": stats["projects"], "comparison_calls": result["comparison_calls"],
                     "tokens": result["tokens"], "http_attempts": stats["http_attempts"],
                     "retried_failures": stats["failures"], "waited_seconds": stats["waited_seconds"],
                     "project_switches": stats["switches"], "seconds": result["seconds"]}
            if not problems:
                meta = {k: v for k, v in entry.items() if k != "at"}
                store.save_batch(manifest, number, result, {**meta, "completed_at": entry["at"],
                                                            "configuration_sha256": configuration_sha256})
                progress(f"    checkpoint {store.batch_path(number).name}: {result['comparison_calls']} call(s), "
                         f"tokens in/out {result['tokens']['input']}/{result['tokens']['output']}, "
                         f"{stats['http_attempts']} HTTP attempt(s) via {', '.join(stats['projects']) or '-'}")
                completed.append(number)
                failed.pop(number, None)
                break
            failed[number] = problems
            store.record(manifest, "failed_attempts", {**entry, "failed_articles": problems,
                                                       "stopped": comparator.stop.kind if comparator.stop else None})
            progress(f"    not complete ({len(problems)} article(s) failed); nothing saved for this batch")
        if comparator.stop is not None:
            break
    return RecoveryResult(completed, failed, comparator.stop)


def recover_all(source: dict[str, Any], fixture_batches: dict[int, list[Fixture]], store: CheckpointStore,
                manifest: dict[str, Any], comparator, run_batch: Callable[[int, list[Fixture]], dict[str, Any]], *,
                model: str, configuration_sha256: str, attempts: int = 2, max_batches: int = 0,
                wait: Callable[[Any, int], bool] | None = None,
                progress: Callable[[str], None] = print) -> RecoveryResult:
    """`recover` until no batch is pending (or `max_batches` were recovered). When the comparator stops for want
    of capacity, `wait(stop, next_batch)` may sleep until capacity is back and return True: the run then goes
    on, the batch in flight restarting from its first article. Without `wait`, or when it returns False, the
    result carries the stop. Every completed batch was checkpointed by `recover` before any wait."""
    completed: list[int] = []
    while True:
        recovered, _ = store.recovered(fixture_batches, configuration_sha256)
        pending = plan(source, fixture_batches, recovered).pending
        if max_batches > 0:
            pending = pending[:max(max_batches - len(completed), 0)]
        if not pending:
            return RecoveryResult(completed, {}, None)
        result = recover(pending, fixture_batches, store, manifest, comparator, run_batch, model=model,
                         configuration_sha256=configuration_sha256, attempts=attempts, progress=progress)
        completed += result.completed
        if result.stop is None:
            return RecoveryResult(completed, result.failed, None)
        waiting = [n for n in pending if n not in result.completed]
        if not waiting:
            return RecoveryResult(completed, result.failed, None)
        if wait is None or not wait(result.stop, waiting[0]):
            return RecoveryResult(completed, result.failed, result.stop)
        comparator.resume()


def _duration(seconds: float) -> str:
    minutes = max(int(round(seconds / 60)), 0)
    return f"{minutes // 60}h{minutes % 60:02d}m" if minutes >= 60 else f"{minutes}m"


class CapacityWaiter:
    """`recover_all`'s `wait`: sleep until capacity is back, saying so, then let the run resume.

    quota_exhausted   until the stop's earliest permitted quota reset (+ `margin`), if that is known and no
                      further away than `max_wait`
    unavailable       every project failed at once (an outage): `outage_wait` seconds, at most `outage_waits`
                      times per run
    anything else     no wait: the run ends (a fatal error, an unknown reset)
    """

    def __init__(self, store: CheckpointStore, manifest: dict[str, Any], *, log: Callable[[str], None] = print,
                 enabled: bool = True, max_wait: float = 26 * 3600, margin: float = 60.0, outage_wait: float = 300.0,
                 outage_waits: int = 6, sleep: Callable[[float], None] = time.sleep, now: Callable = utcnow,
                 chunk: float = 900.0):
        self.store, self.manifest, self.log, self.enabled = store, manifest, log, enabled
        self.max_wait, self.margin, self.outage_wait, self.outage_waits = max_wait, margin, outage_wait, outage_waits
        self.sleep, self.now, self.chunk = sleep, now, chunk
        self.outages = 0
        self.waits: list[dict[str, Any]] = []

    def __call__(self, stop, next_batch: int | None) -> bool:
        if not self.enabled:
            return False
        now = self.now()
        if stop.kind == "quota_exhausted" and stop.until:
            target = datetime.fromisoformat(stop.until) + timedelta(seconds=self.margin)
            headline, until_what = "all projects with permitted capacity are currently unavailable", \
                "next permitted quota reset"
        elif stop.kind == "unavailable" and self.outages < self.outage_waits:
            self.outages += 1
            target = now + timedelta(seconds=self.outage_wait)
            headline, until_what = "every usable project is failing right now", \
                f"the outage pause is over ({self.outages}/{self.outage_waits})"
        else:
            return False
        seconds = (target - now).total_seconds()
        if seconds > self.max_wait:
            self.log(f"the next permitted reset is {_duration(seconds)} away (more than {_duration(self.max_wait)}); "
                     "not waiting")
            return False
        done = sorted(int(n) for n in self.manifest.get("recovered", {}))
        self.log(headline)
        self.log(f"saving progress: {len(done)} recovered batch(es) checkpointed"
                 + (f"; batch {next_batch} restarts from its first article" if next_batch is not None else ""))
        entry = {"at": iso(now), **asdict(stop), "waiting_until": iso(target), "next_batch": next_batch}
        self.store.record(self.manifest, "stops", entry)
        self.waits.append(entry)
        self.log(f"waiting until {until_what}: {iso(target)} ({_duration(seconds)})")
        while (left := (target - self.now()).total_seconds()) > 0:
            self.sleep(min(left, self.chunk))
            left = (target - self.now()).total_seconds()
            if left > 0:
                self.log(f"    still waiting: {_duration(left)} until {iso(target)}")
        self.log(f"automatically resuming batch {next_batch}" if next_batch is not None else "automatically resuming")
        return True


# ----------------------------------------------------------------------------- the final report
def combine(source: dict[str, Any], recovered: dict[int, dict[str, Any]], fixture_batches: dict[int, list[Fixture]],
            *, source_name: str, source_sha256: str, manifest: dict[str, Any] | None = None,
            runner: str | None = None) -> dict[str, Any]:
    """The saved run's complete batches + the recovered ones, in batch order, re-scored with metrics.py."""
    p = plan(source, fixture_batches, recovered)
    test_numbers = {f.filename: f.test_number for items in fixture_batches.values() for f in items}
    batches = []
    for n, status in sorted(p.batches.items()):
        if status.state == "original":
            batches.append({**p.saved[n], "origin": "original_run"})
        elif status.state == "recovered":
            batches.append({**recovered[n]["batch"], "origin": "recovery"})
    rows = [r for b in batches for r in b["articles"]]
    numbers, files = [b["batch"] for b in batches], [r["file"] for r in rows]
    expected_articles = sum(len(items) for items in fixture_batches.values())
    errors = [r["file"] for r in rows if article_failed(r)]
    verification = {"batches": len(set(numbers)), "expected_batches": len(fixture_batches),
                    "articles": len(rows), "unique_articles": len(set(files)), "expected_articles": expected_articles,
                    "processing_errors": len(errors), "pending_batches": p.pending}
    complete = (not p.pending and not errors and len(numbers) == len(set(numbers)) == len(fixture_batches)
                and len(files) == len(set(files)) == expected_articles)
    rerun = [n for n, s in sorted(p.batches.items()) if s.state != "original"]
    discarded = usage_of([p.saved[n] for n in rerun if n in p.saved])
    failed_attempts = (manifest or {}).get("failed_attempts", [])
    report = {
        "stage": "consolidation",
        "experiment": {**(source.get("experiment") or {}), **({"recovered_by": runner} if runner else {})},
        "model": source.get("model"),
        "settings": source.get("settings"),
        "started_at": source.get("started_at"),
        "completed_at": iso(utcnow()),
        "batches_run": sorted(numbers),
        "incomplete": not complete,
        "metrics": score(batches, test_numbers),
        "outcomes": dict(sorted(Counter(article_outcome(r) for r in rows).items())),
        "usage": usage_of(batches),
        "batches": batches,
        "recovery": {
            "source_file": source_name, "source_sha256": source_sha256,
            "original_batches": p.numbers("original"), "recovered_batches": p.numbers("recovered"),
            "recovered": {str(n): {k: v for k, v in recovered[n]["recovery"].items() if k != "configuration_sha256"}
                          for n in p.numbers("recovered")},
            "verification": verification,
            # model calls spent on results that are not in this report: the saved run's incomplete batches
            # and every recovery attempt that did not complete
            "discarded_usage": {
                "saved_run_incomplete_batches": discarded,
                "failed_recovery_attempts": {
                    "attempts": len(failed_attempts),
                    "comparison_calls": sum(a.get("comparison_calls", 0) for a in failed_attempts),
                    "input_tokens": sum(a.get("tokens", {}).get("input", 0) for a in failed_attempts),
                    "output_tokens": sum(a.get("tokens", {}).get("output", 0) for a in failed_attempts)}},
            # what the saved run had for the batches that were rerun (stateful batches are rerun whole)
            "superseded_rows": {str(n): [{"file": r["file"], "predicted_label": r["predicted_label"],
                                          "failure": failure_kind(r)} for r in p.saved[n]["articles"]]
                                for n in rerun if n in p.saved},
        },
    }
    if not complete:
        report["incomplete_reason"] = (f"pending batches {p.pending}" if p.pending
                                       else f"verification failed: {verification}")
    return report
