"""`datasources-similarity`: maintenance commands for semantic event grouping.

    uv run datasources-similarity check                         # validate configuration (no writes)
    uv run datasources-similarity download-model                # explicit model download (network)
    uv run datasources-similarity backfill --limit 100          # bootstrap stored revisions: index, then group
    uv run datasources-similarity retry --limit 100             # finish pending / partial analyses
    uv run datasources-similarity status                        # analysis status, events, coverage
    uv run datasources-similarity inspect --revision-id <id>    # units, candidates, comparisons, events
    uv run datasources-similarity evaluate --limit 10 --confirm-paid-calls   # real backends, labelled examples

These do not replace the inline analysis: the ingestion graph compares and groups
every new record before storing it. Add ``--sample`` to work on sample data.
``backfill`` and ``retry`` take the same database writer lock as the ingestion
worker, so stop the worker first.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from .. import config
from ..db import Database, DatabaseError, load_db_settings
from ..ingest.settings import ConfigurationError
from . import store
from .comparison import anthropic_credentials_available
from .embeddings import build_embedding_backend, download_model, profile_spec
from .lock import GraphWriterLock
from .settings import load_similarity_settings

LOGGER = logging.getLogger("datasources.similarity")
EXAMPLES = config.ROOT / "tests" / "fixtures" / "synthetic_similarity_labelled_examples.json"
BATCH = 10


def _print(value) -> None:
    print(json.dumps(value, indent=2, default=str))


def _runtime(database):
    from .pipeline import build_runtime

    settings = load_similarity_settings()
    if not settings.enabled:
        print("note: SIMILARITY_ENABLED is false, so the ingestion worker will not analyse new records "
              "until it is enabled; this command still uses the configured backends.", file=sys.stderr)
    return build_runtime(database, settings)


def _outcome_counts(outcomes, errors) -> dict:
    counts: dict[str, int] = {"revisions": len(outcomes) + len(errors), "errors": len(errors)}
    for outcome in outcomes:
        key = "persist_failed" if outcome.error else f"analysis_{outcome.analysis_status}"
        counts[key] = counts.get(key, 0) + 1
        for field in ("llm_calls", "events_created", "memberships_inserted", "comparisons_inserted"):
            counts[field] = counts.get(field, 0) + int(outcome.analysis.get(field) or 0)
    return counts


def _merge(total: dict, part: dict) -> None:
    for key, value in part.items():
        total[key] = total.get(key, 0) + value


# ----------------------------------------------------------------------------- commands
def cmd_check(database: Database) -> int:
    settings = load_similarity_settings()
    report: dict = {"similarity_enabled": settings.enabled, "problems": []}
    info = database.check()
    report["database"] = info.describe()
    if not info.pgvector_installed:
        report["problems"].append("pgvector is not enabled (CREATE EXTENSION vector;)")
    with database.connection() as conn:
        missing = store.missing_schema(conn)
    if missing:
        report["problems"].append(f"schema missing {missing}: run `uv run datasources-db migrate`")
    try:
        backend = build_embedding_backend(settings)
        spec = profile_spec(backend, settings)
        ident = spec.identity
        report["embedding"] = {"backend": settings.embedding_backend, "model": ident.model_name,
                               "version": ident.model_version, "dimensions": ident.dimensions,
                               "max_input_tokens": ident.max_input_tokens, "distance": ident.distance_metric,
                               "config_hash": spec.config_hash}
        if not missing:
            with database.connection() as conn:
                row = conn.execute("SELECT profile_id FROM embedding_profile WHERE config_hash = %s",
                                   (spec.config_hash,)).fetchone()
            report["embedding"]["profile_id"] = str(row["profile_id"]) if row else "not created yet"
    except ConfigurationError as error:
        report["problems"].append(str(error))
    report["comparison"] = {"backend": settings.comparison_backend, "model": settings.comparison_model,
                            "effort": settings.comparison_effort or "model default",
                            "fallbacks": settings.comparison_fallbacks}
    if settings.comparison_backend == "anthropic":
        available = anthropic_credentials_available()
        report["comparison"]["credentials"] = "available" if available else "missing"
        if not available:
            report["problems"].append("no Anthropic credentials: set ANTHROPIC_API_KEY in .env")
    elif settings.comparison_backend == "mock":
        report["comparison"]["note"] = "mock backend labels every pair 'uncertain' and never links events"
    _print(report)
    return 0 if not report["problems"] else 2


def cmd_download_model() -> int:
    settings = load_similarity_settings()
    print(f"downloading {settings.embedding_model}@{settings.embedding_revision} (network) ...", file=sys.stderr)
    path = download_model(settings)
    backend = build_embedding_backend(settings)
    ident = backend.identity()  # proves it now loads offline
    _print({"path": str(path), "model": ident.model_name, "version": ident.model_version,
            "dimensions": ident.dimensions, "max_input_tokens": ident.max_input_tokens})
    return 0


def cmd_backfill(database: Database, args) -> int:
    from ..ingest.graph import run_reanalysis
    from .pipeline import PipelineContext

    runtime = _runtime(database)
    with GraphWriterLock(database, is_sample=args.sample, purpose="similarity backfill"):
        with database.connection() as conn:
            ids = store.select_unanalyzed(conn, profile_id=runtime.profile_id,
                                          pipeline_version=runtime.pipeline_version, is_sample=args.sample,
                                          limit=args.limit)
        print(f"selected {len(ids)} stored revision(s) without analysis (oldest first)", file=sys.stderr)
        summary: dict = {"selected": len(ids), "index_pass": {}, "group_pass": {}}
        # Pass 1: text units and embeddings for the whole selection before any grouping,
        # so no historical record is declared new because its candidates were not indexed yet.
        indexed: list[str] = []
        for start in range(0, len(ids), BATCH):
            result = run_reanalysis(ids[start:start + BATCH], PipelineContext(database, runtime, index_only=True))
            _merge(summary["index_pass"], _outcome_counts(result.outcomes, result.errors))
            indexed += [o.revision_id for o in result.outcomes
                        if not o.error and o.analysis_status == "pending"]
            print(f"  indexed {min(start + BATCH, len(ids))}/{len(ids)}", file=sys.stderr)
        if not args.index_only:
            # Pass 2: group in source-time order with the same graph the worker uses.
            for start in range(0, len(indexed), BATCH):
                result = run_reanalysis(indexed[start:start + BATCH], PipelineContext(database, runtime))
                _merge(summary["group_pass"], _outcome_counts(result.outcomes, result.errors))
                print(f"  grouped {min(start + BATCH, len(indexed))}/{len(indexed)}", file=sys.stderr)
        with database.connection() as conn:
            summary["status"] = store.status_summary(conn, profile_id=runtime.profile_id,
                                                     pipeline_version=runtime.pipeline_version,
                                                     is_sample=args.sample)
    _print(summary)
    return 0


def cmd_retry(database: Database, args) -> int:
    from ..ingest.graph import run_reanalysis
    from .pipeline import PipelineContext

    runtime = _runtime(database)
    with GraphWriterLock(database, is_sample=args.sample, purpose="similarity retry"):
        with database.connection() as conn:
            ids = store.select_retry(conn, profile_id=runtime.profile_id, pipeline_version=runtime.pipeline_version,
                                     is_sample=args.sample, limit=args.limit, now=datetime.now(timezone.utc),
                                     include_not_due=args.all, revision_ids=args.revision_id or None)
        print(f"retrying {len(ids)} pending/partial analysis(es)", file=sys.stderr)
        summary: dict = {"selected": len(ids)}
        for start in range(0, len(ids), BATCH):
            result = run_reanalysis(ids[start:start + BATCH], PipelineContext(database, runtime))
            _merge(summary, _outcome_counts(result.outcomes, result.errors))
    _print(summary)
    return 0


def cmd_status(database: Database, args) -> int:
    settings = load_similarity_settings()
    backend = build_embedding_backend(settings)
    spec = profile_spec(backend, settings)
    with database.connection() as conn:
        missing = store.missing_schema(conn)
        if missing:
            raise ConfigurationError(f"schema missing {missing}: run `uv run datasources-db migrate`")
        row = conn.execute("SELECT profile_id FROM embedding_profile WHERE config_hash = %s",
                           (spec.config_hash,)).fetchone()
        report: dict = {"similarity_enabled": settings.enabled, "partition": "sample" if args.sample else "live",
                        "embedding_profile": str(row["profile_id"]) if row else None,
                        "embedding_model": f"{spec.identity.model_name}@{spec.identity.model_version[:12]}",
                        "comparison": f"{settings.comparison_backend}:{settings.comparison_model}"}
        if row is None:
            report["note"] = "no analysis exists yet for the configured profile"
        else:
            from .settings import PIPELINE_VERSION

            report.update(store.status_summary(conn, profile_id=str(row["profile_id"]),
                                               pipeline_version=PIPELINE_VERSION, is_sample=args.sample))
        report["profiles_in_database"] = [
            {**r, "profile_id": str(r["profile_id"])} for r in conn.execute(
                "SELECT profile_id, model_name, model_version, dimensions, chunking_version FROM embedding_profile")]
    _print(report)
    return 0


def cmd_inspect(database: Database, args) -> int:
    with database.connection() as conn:
        _print(store.inspect_revision(conn, args.revision_id))
    return 0


def cmd_evaluate(args) -> int:
    from .evaluation import load_examples, run_evaluation

    settings = load_similarity_settings()
    examples = load_examples(Path(args.examples))[: args.limit]
    if settings.comparison_backend == "anthropic" and not args.confirm_paid_calls:
        print(f"This sends {len(examples)} request(s) to {settings.comparison_model} (paid API calls). "
              "Re-run with --confirm-paid-calls to proceed.", file=sys.stderr)
        return 2
    _print(run_evaluation(settings, examples, use_embeddings=not args.no_embeddings))
    return 0


# ----------------------------------------------------------------------------- entry point
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="datasources-similarity", description=__doc__.split("\n\n")[0])
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("check", help="Validate similarity configuration (no writes)")
    sub.add_parser("download-model", help="Download the configured embedding model (explicit network access)")
    p_backfill = sub.add_parser("backfill", help="Index, then group, stored revisions that have no analysis")
    p_backfill.add_argument("--limit", type=int, default=100)
    p_backfill.add_argument("--sample", action="store_true", help="Work on sample (is_sample) data")
    p_backfill.add_argument("--index-only", action="store_true", help="Only create text units and embeddings")
    p_retry = sub.add_parser("retry", help="Retry pending / partial analyses (bounded)")
    p_retry.add_argument("--limit", type=int, default=100)
    p_retry.add_argument("--sample", action="store_true")
    p_retry.add_argument("--all", action="store_true", help="Ignore next_retry_at backoff")
    p_retry.add_argument("--revision-id", action="append", help="Only these revisions (repeatable)")
    p_status = sub.add_parser("status", help="Analysis status, events and index coverage")
    p_status.add_argument("--sample", action="store_true")
    p_inspect = sub.add_parser("inspect", help="Show one revision's units, candidates, comparisons and events")
    p_inspect.add_argument("--revision-id", required=True)
    p_eval = sub.add_parser("evaluate", help="Run the real backends on labelled examples (no database writes)")
    p_eval.add_argument("--limit", type=int, default=20)
    p_eval.add_argument("--examples", default=str(EXAMPLES))
    p_eval.add_argument("--no-embeddings", action="store_true", help="Skip the embedding similarity column")
    p_eval.add_argument("--confirm-paid-calls", action="store_true")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING,
                        format="%(levelname)s %(name)s %(message)s")
    command = args.command or "status"
    database: Database | None = None
    try:
        if command == "download-model":
            return cmd_download_model()
        if command == "evaluate":
            return cmd_evaluate(args)
        if getattr(args, "limit", 1) is not None and getattr(args, "limit", 1) <= 0:
            raise ConfigurationError("--limit must be greater than 0")
        database = Database(load_db_settings())
        if command == "check":
            return cmd_check(database)
        if command == "backfill":
            return cmd_backfill(database, args)
        if command == "retry":
            return cmd_retry(database, args)
        if command == "inspect":
            return cmd_inspect(database, args)
        return cmd_status(database, args)
    except (ConfigurationError, DatabaseError) as error:
        sys.stderr.write(f"Configuration error: {error}\n")
        return 2
    finally:
        if database is not None:
            database.close()


if __name__ == "__main__":
    raise SystemExit(main())
