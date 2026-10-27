"""Finish the 125-article Gemini run without redoing what already completed.

Reads a saved run (default results/2026-10-24_1709_gemini.json, never modified), keeps every batch whose
five articles all completed, reruns every other batch from its first article in a fresh test database
with the SAME model, prompt, JSON schema, embedder and retrieval settings (checked against the saved run;
the comparator is experiments/gemini.py's own `build_comparator`), checkpoints each recovered batch the
moment it completes, and once all 25 batches are complete writes results/gemini_125_completed.json,
re-scored with datasources/evaluation/metrics.py.

    uv run python evaluation/consolidation/resume_gemini.py --dry-run     the plan; no request, no write
    uv run python evaluation/consolidation/resume_gemini.py --check-keys  test each key (model list only)
    uv run python evaluation/consolidation/resume_gemini.py               recover, checkpoint, finalize
    uv run python evaluation/consolidation/resume_gemini.py --project GEMINI_API_KEY_2

Keys: GEMINI_API_KEY_1 ... _7 in SystemCode/.env, one per Google Cloud project (GEMINI_API_KEY is used
only when none of them is set). Every run first checks each key with a model-list request (no generation
quota): an expired, disabled or rejected key is skipped. During the run a timeout / 503 / connection error
moves the request to the next key at once; a 429 rate limit waits as the provider asks. Daily quota comes
from the run's home project (--project) and from the projects listed in GEMINI_QUOTA_FAILOVER_PROJECTS
(independently provisioned capacity, e.g. billing enabled; never a free-tier project): when the home
project's quota is used up the request continues on the next of those, and when none is left the run saves
its progress, waits until the earliest permitted quota reset and resumes on its own (--no-wait: stop
instead). See datasources/evaluation/gemini_projects.py.

Exit codes: 0 finished (or dry run), 3 stopped early with progress saved (run it again to continue),
1 refused (configuration differs from the saved run, another run holds the lock, ...).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from collections import Counter
from dataclasses import asdict, replace
from pathlib import Path

from filelock import Timeout

from datasources import config  # noqa: F401  (loads SystemCode/.env)
from datasources.db.evaluation import FIXTURE_DIR, read_fixtures
from datasources.evaluation import consolidation, resume, verbose
from datasources.evaluation.consolidation import RESULTS_DIR, print_scores
from datasources.evaluation.gemini_projects import (ProjectPool, ProjectUnavailable, RecoveringComparator, Stop,
                                                    iso, utcnow)
from datasources.ingest.store import atomic_write_json
from datasources.similarity.settings import load_similarity_settings

# ----------------------------------------------------------------------------- defaults (each has a command-line flag)
SOURCE = RESULTS_DIR / "2026-10-24_1709_gemini.json"  # the saved run to finish: read, never written
RECOVERY_DIR = RESULTS_DIR / "gemini_recovery"  # checkpoints, manifest, project health; never a key
OUTPUT = RESULTS_DIR / "gemini_125_completed.json"
PROJECT_KEY_ENVS = (
    "GEMINI_API_KEY_1",
    "GEMINI_API_KEY_2",
    "GEMINI_API_KEY_3",
    "GEMINI_API_KEY_4",
    "GEMINI_API_KEY_5",
    "GEMINI_API_KEY_6",
    "GEMINI_API_KEY_7",
)  # variable names, in order
FALLBACK_KEY_ENV = "GEMINI_API_KEY"
# comma-separated key variables of projects with independently provisioned capacity (e.g. billing enabled) that
# may take over when the home project's daily quota is used up; a free-tier project never qualifies
QUOTA_FAILOVER_ENV = "GEMINI_QUOTA_FAILOVER_PROJECTS"
BATCH_ATTEMPTS = 2  # runs of one batch per invocation when it fails for a temporary reason
MAX_WAIT_HOURS = 26.0  # longest single wait for a quota reset (Gemini's per-day quotas reset every 24 h)
TIMEOUT = 600.0  # seconds to wait for one answer: some batch-4+ comparisons take gemini-3.8-flash over 180 s
EXPERIMENT = Path(__file__).resolve().parent / "experiments" / "gemini.py"
EXIT_STOPPED = 3


def load_experiment(path: Path = EXPERIMENT):
    spec = importlib.util.spec_from_file_location("experiment_gemini", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # importing never runs the experiment: that is behind __main__
    return module


def project_names(environ=os.environ) -> list[str]:
    names = list(PROJECT_KEY_ENVS)
    if not any(environ.get(n) for n in names) and environ.get(FALLBACK_KEY_ENV):
        return [FALLBACK_KEY_ENV]
    return names


def quota_failover_names(environ=os.environ) -> list[str]:
    names = [n.strip() for n in (environ.get(QUOTA_FAILOVER_ENV) or "").split(",") if n.strip()]
    unknown = [n for n in names if n not in PROJECT_KEY_ENVS]
    if unknown:
        print(f"{QUOTA_FAILOVER_ENV}: ignoring {', '.join(unknown)} (not one of {', '.join(PROJECT_KEY_ENVS)})")
    return [n for n in names if n in PROJECT_KEY_ENVS]


def backend_builder(experiment, settings):
    """gemini.py's `build_comparator`, pointed at one project's key variable."""
    def build(key_env: str):
        experiment.API_KEY_ENV = key_env
        return experiment.build_comparator(settings)
    return build


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--verbose", action="store_true",
                        help="timestamped stage logs as they happen: database, embeddings, each article, "
                             "retrieval, every Gemini request (key variable name only) and HTTP status, "
                             "judgments, OK/MISS, checkpoints; plus a heartbeat when nothing happened for 15 s")
    parser.add_argument("--dry-run", action="store_true",
                        help="show completed / pending batches and project status; send nothing, write nothing")
    parser.add_argument("--check-keys", action="store_true",
                        help="test every key with a model-list request (no generation quota) and exit")
    parser.add_argument("--project", metavar="VAR", help="start on this key variable, e.g. GEMINI_API_KEY_2")
    parser.add_argument("--max-batches", type=int, default=0, metavar="N",
                        help="recover at most N batches in this run (0 = all pending)")
    parser.add_argument("--batch-attempts", type=int, default=BATCH_ATTEMPTS, metavar="N",
                        help=f"runs of one batch when it fails for a temporary reason (default {BATCH_ATTEMPTS})")
    parser.add_argument("--timeout", type=float, default=TIMEOUT, metavar="SECONDS",
                        help=f"wait for one answer this long before the next key takes the request (default "
                             f"{TIMEOUT:.0f}); does not change what the model is asked")
    parser.add_argument("--no-wait", action="store_true",
                        help="stop (exit 3, progress saved) instead of waiting when no project with permitted "
                             "capacity is left")
    parser.add_argument("--max-wait-hours", type=float, default=MAX_WAIT_HOURS, metavar="H",
                        help=f"longest single wait for a quota reset (default {MAX_WAIT_HOURS:.0f})")
    parser.add_argument("--reset-projects", action="store_true",
                        help="forget invalid / exhausted marks, e.g. after enabling billing (the provider still "
                             "enforces its own limits; a fixed key is picked up by the key check on its own)")
    parser.add_argument("--keep-db", action="store_true", help="keep each recovered batch's test database")
    parser.add_argument("--source", type=Path, default=SOURCE, help="the saved run to finish (read only)")
    parser.add_argument("--recovery-dir", type=Path, default=RECOVERY_DIR)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    return parser.parse_args(argv)


def show_plan(args, p: resume.Plan, source, recovered, rejected, pool: ProjectPool, configuration,
              test_numbers) -> None:
    rows = [r for b in source.get("batches", []) for r in b["articles"]]
    failed = [r for r in rows if resume.article_failed(r)]
    print(f"Saved run: {args.source.name} ({len(rows)} articles in {len(source.get('batches', []))} batches, "
          f"{len(failed)} did not complete)")
    print(f"Model {configuration['comparison_model']} ({configuration['json_mode']}), prompt "
          f"{configuration['prompt_version']}, embedder {configuration['embedding_model']}@"
          f"{configuration['embedding_revision'][:7]}, retrieval settings: same as the saved run")
    if failed:
        kinds = Counter(resume.failure_kind(r) for r in failed)
        print("  processing failures (rerun): " + ", ".join(f"{k} {v}" for k, v in kinds.most_common()))
    answers = Counter(resume.article_outcome(r) for r in rows if not resume.article_failed(r))
    print("  model answers (never retried): " + ", ".join(f"{k} {v}" for k, v in sorted(answers.items())))
    print(f"\nKept as saved, complete:         {', '.join(map(str, p.numbers('original'))) or '-'}")
    print(f"Recovered earlier (checkpoints): {', '.join(map(str, p.numbers('recovered'))) or '-'}")
    for name, why in rejected.items():
        print(f"  ignored checkpoint {name}: {why}")
    complete = [p.saved[n] for n in p.numbers("original")]
    per_batch = sum(b["comparison_calls"] for b in complete) / len(complete) if complete else 0
    print(f"To run: {len(p.pending)} batch(es), {sum(len(p.batches[n].files) for n in p.pending)} articles, each "
          f"batch whole from its first article in a fresh database"
          + (f" (about {round(per_batch * len(p.pending))} model comparisons before retries)" if per_batch else ""))
    for n in p.pending:
        b = p.batches[n]
        print(f"  batch {n:>2}  {b.files[0]} .. {b.files[-1]}   saved run: "
              + ", ".join(f"{k} {v}" for k, v in b.failures.items()))
    print("\nProjects (key variables; values are never shown or saved):")
    for line in pool.describe():
        print(line)
    if p.pending:
        try:
            home = pool.choose_start(args.project)
            print(f"A run now starts on {home}; permitted capacity: {', '.join(pool.capacity_names(home))}.")
        except ProjectUnavailable as error:
            if error.kind == "quota_exhausted" and error.until and not args.no_wait:
                print(f"A run now saves its progress and waits until {error.until} (earliest permitted quota "
                      f"reset), then starts on {error.project}: {error}")
            else:
                print(f"A run now cannot start: {error}")
        if not pool.quota_failover:
            print(f"No {QUOTA_FAILOVER_ENV} declared: when the home project's daily quota is used up, the run "
                  "waits for its reset.")
    done = complete + [recovered[n]["batch"] for n in p.numbers("recovered")]
    if done and p.pending:
        scores = resume.score(done, test_numbers)
        print(f"\nProvisional scores over the {len(done)} complete batch(es), NOT the final result:")
        print_scores(scores, resume.usage_of(done))


def check_keys(pool: ProjectPool, experiment) -> None:
    print("\nChecking the keys (model list only, no generation quota used):")
    for name, result in pool.check(experiment.BASE_URL, experiment.MODEL).items():
        print(f"  {name:<20} {result}")
    for name in pool.names:
        if name not in pool.configured:
            print(f"  {name:<20} {pool.usable(name)[1]}")


def run_pending(args, experiment, settings, source, fixture_batches, store, manifest, pool, p,
                configuration_sha) -> int | None:
    """Recover the pending batches; None when the run went through, else the exit code."""
    from datasources.similarity.embeddings import build_embedding_backend

    check_keys(pool, experiment)
    # --verbose: every line goes through the one logging handler (timestamped, flushed, never interleaved)
    log = verbose.LOGGER.info if args.verbose else print
    waiter = resume.CapacityWaiter(store, manifest, log=log, enabled=not args.no_wait,
                                   max_wait=args.max_wait_hours * 3600)
    while True:  # the home project, or a wait for the earliest permitted reset when nothing may serve yet
        try:
            start = pool.choose_start(args.project)
            break
        except ProjectUnavailable as error:
            stop = Stop(error.kind, str(error), error.project, error.until)
            if error.kind == "quota_exhausted" and waiter(stop, p.pending[0] if p.pending else None):
                continue
            print(f"\nCannot start: {error}")
            if error.kind == "quota_exhausted":
                print("Completed batches are saved. Run again after the reset time above (or without --no-wait).")
            store.record(manifest, "stops", {"at": iso(utcnow()), **asdict(stop)})
            return EXIT_STOPPED if error.kind != "configuration" else 1
    capacity = pool.capacity_names(start)
    log(f"home project {start}; permitted capacity: {', '.join(capacity)}"
        + ("" if pool.quota_failover else f" (no {QUOTA_FAILOVER_ENV} declared: the home project only)"))
    loading = time.monotonic()
    if args.verbose:
        log(f"embedding model {settings.embedding_model}@{settings.embedding_revision[:7]}: loading")
    embedder = build_embedding_backend(settings)
    problem = resume.embedder_mismatch(source, embedder)  # identity() loads the model
    if problem:
        print(f"\nRefusing to resume: {problem}")
        return 1
    if args.verbose:
        identity = embedder.identity()
        log(f"embedding model loaded in {time.monotonic() - loading:.1f}s: {identity.model_name}, "
            f"{identity.dimensions} dims, same identity as the saved run")
        embedder = verbose.LoggedEmbedder(embedder)
    comparator = RecoveringComparator(pool, backend_builder(experiment, replace(settings, llm_timeout_seconds=args.timeout)),
                                      model=experiment.MODEL, start=start, log=log)

    def run_batch(number, items):
        comparator.batch_label = f"batch {number}"
        if not args.verbose:
            return consolidation.run_batch(number, items, experiment.NAME, comparator, embedder, settings,
                                           directory=FIXTURE_DIR, keep_database=args.keep_db)
        started = time.monotonic()
        log(f"batch {number}: started ({len(items)} articles, fresh database, active key {comparator.active})")
        try:
            with verbose.heartbeat(f"batch {number}"):
                return consolidation.run_batch(number, items, experiment.NAME, comparator, embedder, settings,
                                               directory=FIXTURE_DIR, keep_database=args.keep_db)
        finally:
            log(f"batch {number}: pipeline finished in {time.monotonic() - started:.1f}s")

    pending = p.pending[:args.max_batches] if args.max_batches > 0 else p.pending
    print(f"\nRecovering {len(pending)} batch(es), starting on {start}, waiting up to {args.timeout:.0f}s per answer\n")
    try:
        result = resume.recover_all(source, fixture_batches, store, manifest, comparator, run_batch,
                                    model=experiment.MODEL, configuration_sha256=configuration_sha,
                                    attempts=max(args.batch_attempts, 1), max_batches=args.max_batches,
                                    wait=waiter, progress=log)
    except KeyboardInterrupt:
        store.record(manifest, "stops", {"at": iso(utcnow()), "kind": "interrupted",
                                         "message": "stopped with Ctrl+C; the batch in flight was discarded"})
        print("\nInterrupted. Every checkpointed batch is kept; run the same command to continue.")
        return EXIT_STOPPED
    print(f"\nRecovered this run: {', '.join(map(str, result.completed)) or 'none'}")
    for number, problems in result.failed.items():
        print(f"  batch {number} still incomplete: {problems[0]}" + (f" (+{len(problems) - 1})" if problems[1:] else ""))
    if result.stop is not None:
        store.record(manifest, "stops", {"at": iso(utcnow()), **asdict(result.stop)})
        print(f"Stopped: {result.stop.message}")
        if result.stop.kind == "quota_exhausted":
            print("Every completed batch is saved. Run the same command to continue: it waits for the earliest "
                  f"permitted quota reset unless --no-wait. Quota failover only uses {QUOTA_FAILOVER_ENV}.")
        return EXIT_STOPPED
    return None


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.verbose:
        verbose.enable()
    if args.output.resolve() == args.source.resolve():
        print("Refusing: --output is the saved run itself, which is never overwritten")
        return 1
    experiment = load_experiment()
    source = json.loads(args.source.read_text(encoding="utf-8"))
    source_sha = resume.sha256_file(args.source)
    fixtures = read_fixtures(FIXTURE_DIR)
    fixture_batches = consolidation.batches(fixtures)
    test_numbers = {f.filename: f.test_number for f in fixtures}
    settings = replace(load_similarity_settings(), enabled=True)
    configuration = resume.run_configuration(model=experiment.MODEL, base_url=experiment.BASE_URL,
                                             json_mode=experiment.JSON_MODE, settings=settings)
    configuration_sha = resume.sha256_text(json.dumps(configuration, sort_keys=True))
    mismatches = resume.source_mismatches(source, configuration)
    if mismatches:
        print("Refusing to resume: the configuration differs from the saved run\n  " + "\n  ".join(mismatches))
        return 1
    store = resume.CheckpointStore(args.recovery_dir)
    recovered, rejected = store.recovered(fixture_batches, configuration_sha)
    p = resume.plan(source, fixture_batches, recovered)
    try:
        manifest = store.open(source_path=args.source, source_sha256=source_sha, configuration=configuration,
                              fixture_checksums={f.filename: f.file_checksum for f in fixtures}, plan_=p,
                              write=not args.dry_run)
    except resume.ResumeError as error:
        print(f"Refusing: {error}")
        return 1
    pool = ProjectPool(project_names(), state_path=store.directory / "projects.json",
                       quota_failover=quota_failover_names())
    if args.reset_projects and not args.dry_run:
        pool.reset()
    show_plan(args, p, source, recovered, rejected, pool, configuration, test_numbers)
    if args.check_keys:
        check_keys(pool, experiment)
        print("\nKeys checked; no model request was sent.")
        return 0
    if args.dry_run:
        print("\nDry run: no request was sent and nothing was written.")
        return 0

    lock = store.lock()
    try:
        lock.acquire()
    except Timeout:
        print(f"Another resume run holds {lock.lock_file}; wait for it to finish.")
        return 1
    try:
        store.adopt(manifest, recovered)
        if not p.complete:
            code = run_pending(args, experiment, settings, source, fixture_batches, store, manifest, pool, p,
                               configuration_sha)
            if code is not None:
                return code
        recovered, rejected = store.recovered(fixture_batches, configuration_sha)  # the report uses what is on disk
        p = resume.plan(source, fixture_batches, recovered)
        if not p.complete:
            print(f"Still pending: {', '.join(map(str, p.pending))}. Run the same command again to continue.")
            return EXIT_STOPPED
        report = resume.combine(source, recovered, fixture_batches, source_name=args.source.name,
                                source_sha256=source_sha, manifest=manifest, runner=Path(__file__).name)
        if report["incomplete"]:
            print(f"Not saving a final report: {report['incomplete_reason']}")
            return 1
        atomic_write_json(args.output, report)
        store.finalize(manifest, args.output)
        verification = report["recovery"]["verification"]
        print(f"\nAll {verification['batches']} batches / {verification['articles']} articles complete "
              f"({len(report['recovery']['original_batches'])} kept from the saved run, "
              f"{len(report['recovery']['recovered_batches'])} recovered), no processing error.")
        print_scores(report["metrics"], report["usage"])
        print(f"Saved {args.output}")
        return 0
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
