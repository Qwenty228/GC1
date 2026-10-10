"""`datasources-db`: connection check, migrations and the end-to-end smoke test.

    uv run datasources-db check                     # which database, and is pgvector enabled
    uv run datasources-db migrate --dry-run         # what migrate would do (read-only)
    uv run datasources-db migrate                   # build a new database / apply pending migrations
    uv run datasources-db migrate --adopt-baseline  # existing database: record the verified base schema
    uv run datasources-db verify                    # compare the schema with the migrations (read-only)
    uv run datasources-db register-companies --dry-run   # which coverage companies would be added (read-only)
    uv run datasources-db register-companies        # add them to company/company_symbol (insert-only, repeatable)
    uv run datasources-db relink-companies --dry-run     # links stored articles lack for companies registered later
    uv run datasources-db relink-companies          # add those links (insert-only, repeatable)
    uv run datasources-db inspect                   # row counts per table
    uv run datasources-db populate --dry-run        # which populate/*.sql data scripts would run (read-only)
    uv run datasources-db populate                  # regenerate 001 from the JSON, run new/changed data scripts, log each run
    uv run datasources-db populate --force          # run every data script again
    uv run datasources-db export-evaluation         # only regenerate populate/001_evaluation_fixtures.sql (gitignored)
    uv run datasources-db smoke                     # bounded live fetch -> store -> read back -> replay

`smoke` is the end-to-end test: it fetches a small, bounded sample of real NVIDIA
news from Alpaca and one recent NVIDIA 8-K from SEC EDGAR, stores them through
the same ingestion graph and persistence layer normal ingestion uses, reads them
back, then replays the exact same payloads to prove nothing is duplicated. It
runs with semantic analysis disabled: no embeddings, no LLM calls. Semantic
analysis has its own commands (`datasources-similarity`).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .. import config
from ..alpaca import AlpacaConfigError, auth_from_env, iter_news_pages
from ..ingest.adapters import sec_context_from_submissions, unwrap_payload
from ..ingest.sec_client import SecClient, SecFetchError
from ..ingest.service import IngestionService
from ..ingest.settings import ConfigurationError, load_settings, require_sec_user_agent
from . import migrate as migrations
from . import schema as schema_check
from .companies import CoverageError
from .connection import Database, DatabaseError
from .repository import ALPACA_NAMESPACE, persist_record
from .settings import load_db_settings

LOGGER = logging.getLogger("datasources.db")

TABLES = (
    "provider", "company", "company_symbol", "publisher", "document", "document_revision",
    "document_revision_source", "revision_company", "sec_filing_details", "text_unit",
    "embedding", "embedding_profile", "ingestion_state", "revision_analysis", "news_event",
    "event_membership", "event_membership_unit", "event_comparison", "retrieval_candidate",
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ------------------------------------------------------------------------- commands
def cmd_check(db: Database) -> int:
    info = db.check()
    print(info.describe())
    print(json.dumps({
        "database": info.database,
        "server_version": info.server_version,
        "user": info.user,
        "host": info.host,
        "port": info.port,
        "tables": info.tables,
        "pgvector_enabled": info.pgvector_installed,
        "pgvector_version": info.pgvector_version,
    }, indent=2))
    if not info.pgvector_installed:
        print("\nNote: pgvector is NOT enabled. News storage does not need it; semantic "
              "comparison (SIMILARITY_ENABLED=true) does (CREATE EXTENSION vector;).", file=sys.stderr)
    return 0


def _print_comparison(comparison: schema_check.Comparison, *, indent: str = "  ") -> None:
    counts = comparison.counts()
    print(f"{indent}missing: {counts.get('missing', 0)}, mismatched: {counts.get('mismatch', 0)}, "
          f"legacy: {counts.get('legacy', 0)}, not created by this repository: {counts.get('unexpected', 0)}"
          + (f", created by later migrations: {counts['later']}" if counts.get("later") else ""))
    for kind, title in (("missing", "MISSING"), ("mismatch", "MISMATCHED"),
                        ("unexpected", "not created by this repository (left alone)"),
                        ("legacy", "legacy objects of the reverted 003 branch (unused, left in place)")):
        found = comparison.of(kind)
        if found:
            print(f"{indent}{title}:")
            for finding in found:
                print(f"{indent}  - {finding.describe()}")


def _print_plan(plan: migrations.MigrationPlan, *, dry_run: bool) -> None:
    vector = (f"pgvector {plan.pgvector_installed} installed" if plan.pgvector_installed else
              f"pgvector not installed{' (available on the server)' if plan.pgvector_available else ''}")
    print(f"Migration plan for {plan.database} (PostgreSQL {plan.server_version}, {vector})")
    if dry_run:
        print("READ ONLY: nothing was changed.")
    state = {
        "recorded": "recorded",
        "fresh": "not recorded; no base tables yet, so it will be EXECUTED (new database)",
        "adoptable": "not recorded; the existing base schema MATCHES it, so it can be ADOPTED (recorded, never "
                     "executed)",
        "mismatch": "not recorded; the existing schema does NOT match it (see below)",
    }[plan.baseline_state]
    print(f"\nBaseline {migrations.BASELINE}: {state}")
    if plan.baseline is not None:
        _print_comparison(plan.baseline, indent="  ")
    if plan.other_tables:
        print(f"  other tables in this database (not managed here, left alone): {', '.join(plan.other_tables)}")
    print("\nMigration files:")
    names = {f.name for f in plan.files}
    for name in sorted(names | set(plan.recorded)):
        entry = plan.recorded.get(name)
        if entry is None:
            status = "pending"
        elif name not in names:
            status = f"recorded, NO FILE: {migrations.KNOWN_UNFILED.get(name, 'unknown migration; left alone')}"
        else:
            when = entry.applied_at.date().isoformat() if entry.applied_at else "?"
            how = f", {entry.method}" if entry.method else ""
            tracked = "checksum ok" if entry.checksum else "checksum not tracked (recorded before checksums)"
            if any(m[0] == name for m in plan.checksum_mismatches):
                tracked = "CHECKSUM MISMATCH"
            status = f"recorded {when}{how}; {tracked}"
        print(f"  {name:<46} {status}")
    if plan.actions:
        print("\nWould run:" if dry_run else "\nTo run:")
        for name, action in plan.actions:
            print(f"  {name:<46} {'adopt (record only, not executed)' if action == 'adopt' else 'execute'}")
        if any(e.checksum is None for e in plan.recorded.values()) or not plan.recorded:
            print("  (schema_migration gets two nullable columns, checksum and method, when the first file is "
                  "recorded)")
    else:
        print("\nNothing to run: every migration file is recorded.")
    if plan.problems:
        print("\nProblems (migrate changes nothing until they are resolved):")
        for problem in plan.problems:
            print(f"  X {problem}")


def cmd_migrate(db: Database, args) -> int:
    import psycopg

    plan = migrations.plan_migrations(db, adopt_baseline=args.adopt_baseline)
    if args.json:
        print(json.dumps({"dry_run": args.dry_run, **plan.as_dict()}, indent=2, default=str))
    else:
        _print_plan(plan, dry_run=args.dry_run)
    if args.dry_run:
        if not args.json:
            verdict = ("migrate would refuse (see the problems above)" if plan.problems else
                       "nothing to do" if not plan.actions else "migrate would run the files listed above")
            print(f"\nDry run: {verdict}.")
        return 1 if plan.problems else 0
    if plan.problems:
        print("\nNot migrated: nothing was changed.", file=sys.stderr)
        return 1
    if not plan.actions:
        print("\nUp to date: nothing was changed.")
        return 0
    try:
        results = migrations.apply_migrations(db, adopt_baseline=args.adopt_baseline)
    except migrations.MigrationRefused as refused:
        print(f"\nNot migrated: {refused}", file=sys.stderr)
        return 1
    except psycopg.Error as error:
        print(f"\nMigration failed: {error}. The failing file's transaction was rolled back; files listed as done "
              "above stay recorded. Re-run `uv run datasources-db migrate --dry-run` to see the state.", file=sys.stderr)
        return 1
    print()
    for result in results:
        if result.applied:
            print(f"{result.filename}: {'adopted (verified, not executed)' if result.method == 'adopted' else 'executed'}")
    comparison = migrations.verify_schema(db)
    print(f"\nSchema check after migrating: {'OK' if comparison.ok else 'PROBLEMS FOUND'}")
    _print_comparison(comparison)
    return 0 if comparison.ok else 1


def cmd_populate(db: Database, args) -> int:
    """Run the new or changed `populate/*.sql` data scripts (every one with --force) and log each run."""
    from .evaluation import EXPORT_FILE, FixtureError, write_export
    from .populate import POPULATE_DIR, apply_populate, plan_populate

    try:  # the generated script is rebuilt from populate/event_grouping/ first, so it never goes stale
        generated, count = write_export()
    except FixtureError as error:
        print(f"Cannot generate {EXPORT_FILE.name}: {error}", file=sys.stderr)
        return 1
    database = db.check().database
    report = (plan_populate if args.dry_run else apply_populate)(db, force=args.force)
    if args.json:
        print(json.dumps({"database": database, "directory": str(POPULATE_DIR), **report.as_dict()}, indent=2))
        return 0
    print(f"Populate {database} from {POPULATE_DIR}")
    print(f"  {EXPORT_FILE.name}: {generated} from {count} JSON file(s) in event_grouping/")
    if args.dry_run:
        print("READ ONLY: nothing was changed.")
    words = {"run": "would run (new)" if args.dry_run else "ran (new)",
             "rerun": "would run again" if args.dry_run else "ran again", "skip": "unchanged, skipped"}
    for name, action in report.actions:
        print(f"  {name:<46} {words[action]}")
    if not report.actions:
        print("  no populate/*.sql files")
    return 0


def cmd_export_evaluation(args) -> int:
    """Regenerate populate/001_evaluation_fixtures.sql from populate/event_grouping/*.json (no database)."""
    from .evaluation import EXPORT_FILE, FixtureError, write_export

    try:
        status, count = write_export()
    except FixtureError as error:
        print(f"Cannot export: {error}", file=sys.stderr)
        return 1
    print(f"{EXPORT_FILE.name}: {status} ({count} JSON file(s)). `make db` regenerates it automatically.")
    return 0


def cmd_verify(db: Database, args) -> int:
    comparison = migrations.verify_schema(db)
    with db.connection() as conn, conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        row = conn.execute("SELECT current_database() AS db").fetchone()
        version = schema_check.server_version(conn)
    recorded = migrations.applied_migrations(db)
    files = migrations.load_files()
    pending = [f.name for f in files if f.name not in recorded]
    generated = schema_check.expected()["generated_on"]["server_version"]
    if args.json:
        print(json.dumps({"database": row["db"], "server_version": version, "manifest_server_version": generated,
                          "pending_files": pending, **comparison.as_dict()}, indent=2, default=str))
        return 0 if comparison.ok else 1
    print(f"Schema check for {row['db']} (PostgreSQL {version}; expected structure generated on PostgreSQL "
          f"{generated}). READ ONLY: nothing was changed.")
    if version.split(".")[0] != generated.split(".")[0]:
        print("  note: another PostgreSQL major version may print some definitions differently; review mismatches")
    print(f"Result: {'OK' if comparison.ok else 'PROBLEMS FOUND'}")
    _print_comparison(comparison)
    if pending:
        print(f"Pending migration files: {', '.join(pending)} (`uv run datasources-db migrate --dry-run`)")
    unfiled = sorted(set(recorded) - {f.name for f in files})
    for name in unfiled:
        print(f"Recorded without a file: {name}: {migrations.KNOWN_UNFILED.get(name, 'unknown migration')}")
    return 0 if comparison.ok else 1


def cmd_register_companies(db: Database, args) -> int:
    """Add the coverage list's companies and tickers to `company` / `company_symbol` (insert-only, repeatable)."""
    from .companies import coverage_companies, register_companies

    companies = coverage_companies(parse_symbols(args.symbols) if args.symbols else None)
    with db.connection() as conn, conn.transaction():
        if args.dry_run:
            conn.execute("SET TRANSACTION READ ONLY")
        database = conn.execute("SELECT current_database() AS db").fetchone()["db"]
        report = register_companies(conn, companies, dry_run=args.dry_run)
    if args.json:
        print(json.dumps({"database": database, **report.as_dict()}, indent=2))
        return 1 if report.conflicts else 0
    print(f"Company registration for {database} ({config.TICKER_MAP_FILE.name}, {len(companies)} companies)")
    if args.dry_run:
        print("READ ONLY: nothing was changed.")
    verb = {"insert": "would add" if args.dry_run else "added", "exists": "kept",
            "exists_other_market": "kept", "conflict": "CONFLICT"}
    for a in report.actions:
        company = "CONFLICT" if a.conflict else verb[a.company]
        print(f"  {a.symbol:<6} {a.market:<7} CIK {a.cik}  {a.name:<32} "
              f"company {company:<9} ticker {verb[a.symbol_row]}")
        if a.stored_name:
            print(f"         stored name kept: {a.stored_name!r}")
        if a.note:
            print(f"         {a.note}")
    added = "to add" if args.dry_run else "added"
    print(f"\nResult: {report.companies_added} compan{'y' if report.companies_added == 1 else 'ies'} and "
          f"{report.symbols_added} ticker(s) {added}; {len(report.conflicts)} conflict(s). Existing rows are never "
          "changed. Collectors are not affected: they follow INGEST_SYMBOLS.")
    if report.conflicts:
        print("Conflicting tickers were left alone; resolve them by hand before registering those companies.",
              file=sys.stderr)
    return 1 if report.conflicts else 0


def cmd_relink_companies(db: Database, args) -> int:
    """Link stored articles to companies registered after they were stored (insert-only, repeatable)."""
    from .companies import relink_articles

    with db.connection() as conn, conn.transaction():
        if args.dry_run:
            conn.execute("SET TRANSACTION READ ONLY")
        database = conn.execute("SELECT current_database() AS db").fetchone()["db"]
        report = relink_articles(conn, is_sample=args.sample, dry_run=args.dry_run)
    if args.json:
        print(json.dumps({"database": database, **report.as_dict()}, indent=2))
        return 0
    partition = "sample" if args.sample else "live"
    print(f"Company links for stored {partition} articles in {database}")
    if args.dry_run:
        print("READ ONLY: nothing was changed.")
    for item in report.revisions:
        print(f"  + {', '.join(item['added']):<24} {(item['title'] or 'Untitled')[:90]}")
    verb = "to add" if args.dry_run else "added"
    print(f"\nResult: {report.links_added} link(s) {verb} on {len(report.revisions)} of {report.scanned} article "
          f"revision(s); {report.without_payload} without a stored payload left as they are. Existing links, articles, "
          "events and summaries are never changed.")
    return 0


def parse_symbols(value: str) -> list[str]:
    return [s.strip().upper() for s in value.split(",") if s.strip()]


def cmd_backfill(db: Database, args) -> int:
    """Store already-processed records from `output/processed/` into PostgreSQL.

    Needed once, for records the worker completed before the database existed:
    the file ledger has them marked completed, so a normal run skips them. This
    is idempotent, so running it twice stores nothing the second time.
    """
    from ..ingest.schema import PreparedRecord
    from ..ingest.store import OutputStore

    settings = load_settings(demo=args.demo)
    store = OutputStore(settings.root)
    counts = {"seen": 0, "inserted": 0, "updated": 0, "skipped": 0, "failed": 0}
    for path, doc in store.iter_processed():
        if doc is None:
            counts["failed"] += 1
            continue
        counts["seen"] += 1
        try:
            record = PreparedRecord.model_validate(doc)
        except Exception as error:
            counts["failed"] += 1
            print(f"  ! {path.name}: not a valid record ({error})", file=sys.stderr)
            continue
        raw_payload, raw_document = _raw_from_refs(store, record)
        try:
            with db.transaction() as conn:
                result = persist_record(conn, record, raw_payload=raw_payload, raw_document=raw_document)
        except Exception as error:
            counts["failed"] += 1
            print(f"  ! {record.record_id}: {error}", file=sys.stderr)
            continue
        counts[result.outcome] += 1
    print(json.dumps({"root": str(settings.root), **counts}, indent=2))
    return 0 if counts["failed"] == 0 else 1


def _raw_from_refs(store, record) -> tuple[dict[str, Any], str | None]:
    """Recover the original payload and HTML from the raw files the worker saved."""
    raw_payload: dict[str, Any] = {}
    raw_document: str | None = None
    for ref in record.raw_file_refs:
        path = store.abs(ref)
        if not path.exists():
            continue
        if ref.startswith("raw/alpaca/articles/") and path.suffix == ".json":
            try:
                raw_payload = json.loads(path.read_text(encoding="utf-8")).get("article") or {}
            except (OSError, ValueError):
                pass
        elif ref.startswith("raw/sec/documents/"):
            try:
                raw_document = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                pass
    if not raw_payload and record.record_type == "sec_filing" and record.sec_metadata is not None:
        # The SEC "payload" is the submissions row; reconstruct what the adapter saw.
        raw_payload = {
            "accessionNumber": record.sec_metadata.accession_number,
            "form": record.sec_metadata.form,
            "filingDate": record.sec_metadata.filing_date,
            "reportDate": record.sec_metadata.report_date,
            "acceptanceDateTime": record.sec_metadata.acceptance_datetime_raw,
            "items": ",".join(record.sec_metadata.items),
            "primaryDocument": record.sec_metadata.primary_document,
            "backfilled_from": "output/processed",
        }
    return raw_payload, raw_document


def cmd_inspect(db: Database) -> int:
    rows: dict[str, Any] = {}
    with db.connection() as conn:
        for table in TABLES:
            cur = conn.execute("SELECT to_regclass(%s) IS NOT NULL AS present", (f"public.{table}",))
            row = cur.fetchone()
            if not row or not row["present"]:
                rows[table] = "missing"
                continue
            cur = conn.execute(f"SELECT count(*) AS n FROM {table}")  # table from the fixed TABLES tuple
            count = cur.fetchone()
            rows[table] = int(count["n"]) if count else 0
    print(json.dumps(rows, indent=2))
    return 0


# ------------------------------------------------------------------------- smoke test
class Smoke:
    """Bounded end-to-end test: real fetch -> persist -> read back -> replay."""

    def __init__(self, db: Database, symbol: str, *, max_articles: int, lookback_hours: int,
                 root: Path, live_alpaca: bool = True, live_sec: bool = True):
        self.db = db
        self.symbol = symbol
        self.max_articles = max_articles
        self.lookback_hours = lookback_hours
        self.settings = load_settings(demo=False).with_overrides(symbols=[symbol], output_root=root)
        self.service = IngestionService(self.settings, database=db)
        self.live_alpaca = live_alpaca
        self.live_sec = live_sec
        self.summary: dict[str, Any] = {
            "database": None, "pgvector_enabled": None, "symbol": symbol,
            "alpaca": {"mode": None, "fetched": 0, "inserted": 0, "updated": 0, "skipped": 0, "failed": 0},
            "sec": {"mode": None, "fetched": 0, "inserted": 0, "updated": 0, "skipped": 0, "failed": 0},
            "replay": {}, "readback": {}, "notes": [],
        }
        self.alpaca_payloads: list[dict] = []
        self.sec_jobs: list[dict] = []

    # -- step 1 ------------------------------------------------------------------
    def verify_database(self) -> None:
        info = self.db.require_database(self.db.settings.dbname)
        self.summary["database"] = info.database
        self.summary["pgvector_enabled"] = info.pgvector_installed
        print(f"[1/8] {info.describe()}")
        if not info.pgvector_installed:
            self.summary["notes"].append(
                "pgvector not enabled: news storage verified, vector setup is a separate concern"
            )

    # -- step 2 ------------------------------------------------------------------
    def fetch_alpaca(self) -> None:
        """Bounded historical REST fetch, so the test does not wait for a live event."""
        if not self.live_alpaca:
            self.summary["alpaca"]["mode"] = "skipped"
            return
        try:
            auth = auth_from_env()
        except AlpacaConfigError as error:
            self.summary["alpaca"]["mode"] = "unavailable"
            self.summary["notes"].append(f"Alpaca credentials missing, no live news fetched: {error}")
            print(f"[2/8] Alpaca SKIPPED: {error}")
            return
        end = _utcnow()
        start = end - timedelta(hours=self.lookback_hours)
        try:
            for payload, url in iter_news_pages(auth, [self.symbol], start=start, end=end, max_pages=1,
                                                page_size=min(self.max_articles, 50),
                                                timeout=self.settings.http_timeout_seconds):
                for article in payload.get("news", [])[: self.max_articles]:
                    self.alpaca_payloads.append({"article": article, "endpoint": url})
                break
        except Exception as error:
            self.summary["alpaca"]["mode"] = "unavailable"
            self.summary["notes"].append(f"Alpaca REST fetch failed: {error}")
            print(f"[2/8] Alpaca FETCH FAILED: {error}")
            return
        self.summary["alpaca"]["mode"] = "live"
        self.summary["alpaca"]["fetched"] = len(self.alpaca_payloads)
        print(f"[2/8] Alpaca: fetched {len(self.alpaca_payloads)} real {self.symbol} article(s) "
              f"from the last {self.lookback_hours}h")

    # -- step 3 ------------------------------------------------------------------
    def fetch_sec(self) -> None:
        """One recent 8-K, with its primary document downloaded and extracted."""
        if not self.live_sec:
            self.summary["sec"]["mode"] = "skipped"
            return
        try:
            agent = require_sec_user_agent(self.settings)
        except ConfigurationError as error:
            self.summary["sec"]["mode"] = "unavailable"
            self.summary["notes"].append(f"SEC_USER_AGENT missing: {error}")
            print(f"[3/8] SEC SKIPPED: {error}")
            return
        client = SecClient(agent, requests_per_second=self.settings.sec_requests_per_second,
                           timeout=self.settings.sec_http_timeout_seconds, max_retries=self.settings.sec_max_retries)
        self.service.sec_client = client
        cik10 = config.cik_for(self.symbol)
        try:
            submissions, result = client.fetch_submissions(cik10, self.settings.sec_max_submissions_bytes)
        except SecFetchError as error:
            self.summary["sec"]["mode"] = "unavailable"
            self.summary["notes"].append(f"SEC submissions fetch failed: {error}")
            print(f"[3/8] SEC FETCH FAILED: {error}")
            return
        rows, _info = unwrap_payload(submissions, provider="sec_edgar",
                                     sec_forms=set(self.settings.sec_forms), cik=cik10)
        if not rows:
            self.summary["sec"]["mode"] = "unavailable"
            self.summary["notes"].append("no 8-K filings found in submissions")
            print("[3/8] SEC: no 8-K rows found")
            return
        ctx = sec_context_from_submissions(submissions, cik10)
        newest = sorted(rows, key=lambda pair: pair[0].get("filingDate") or "", reverse=True)[0][0]
        raw_ref = self.service.store.save_raw_submissions(cik10, result.body)
        self.sec_jobs.append({
            "identity": f"sec_edgar:{ctx.cik}:{newest['accessionNumber']}",
            "row": newest, "context": ctx.model_dump(), "symbol": self.symbol,
            "endpoint": result.url, "raw_ref": raw_ref,
            "retrieved_at": result.retrieved_at.isoformat(), "transport": "rest",
        })
        self.summary["sec"]["mode"] = "live"
        self.summary["sec"]["fetched"] = 1
        print(f"[3/8] SEC: selected {newest['form']} {newest['accessionNumber']} "
              f"filed {newest.get('filingDate')} (primary doc {newest.get('primaryDocument')})")

    # -- step 4 ------------------------------------------------------------------
    def persist_all(self) -> None:
        """Store through IngestionService, i.e. the same path normal ingestion uses."""
        self.service.startup()
        for entry in self.alpaca_payloads:
            article = entry["article"]
            before = dict(self.service.stats)
            outcome = self.service.process_alpaca({
                "article": article, "fingerprint": _fingerprint(article), "transport": "rest",
                "endpoint": entry["endpoint"], "retrieved_at": _utcnow().isoformat(), "raw_refs": [],
            })
            self._tally("alpaca", outcome, before)
        for job in self.sec_jobs:
            before = dict(self.service.stats)
            outcome = self.service.process_sec(job)
            self._tally("sec", outcome, before)
        a, s = self.summary["alpaca"], self.summary["sec"]
        print(f"[4/8] stored: alpaca inserted={a['inserted']} updated={a['updated']} "
              f"skipped={a['skipped']} failed={a['failed']} | "
              f"sec inserted={s['inserted']} updated={s['updated']} skipped={s['skipped']} failed={s['failed']}")

    def _tally(self, source: str, outcome: str, before: dict) -> None:
        bucket = self.summary[source]
        if outcome == "failed":
            bucket["failed"] += 1
            return
        if outcome == "skipped":
            bucket["skipped"] += 1
            return
        delta = {k: self.service.stats[k] - before.get(k, 0) for k in ("db_inserted", "db_updated", "db_skipped")}
        for key, label in (("db_inserted", "inserted"), ("db_updated", "updated"), ("db_skipped", "skipped")):
            bucket[label] += delta.get(key, 0)

    # -- step 5 ------------------------------------------------------------------
    def read_back(self) -> None:
        """Read every stored record back out and verify the fields that matter."""
        checks: dict[str, Any] = {"alpaca": [], "sec": [], "problems": []}
        with self.db.connection() as conn:
            for entry in self.alpaca_payloads:
                article_id = str(entry["article"].get("id"))
                cur = conn.execute(
                    """
                    SELECT d.document_id, d.source_record_id, d.source_namespace, d.record_type,
                           p.code AS api_provider, pub.name AS publisher_name,
                           pub.credibility_tier, r.revision_id, r.title, r.original_url,
                           r.published_at, r.source_updated_at, r.content_status, r.content_scope,
                           length(coalesce(r.body_text, '')) AS body_chars,
                           (SELECT count(*) FROM revision_company rc WHERE rc.revision_id = r.revision_id) AS links,
                           (SELECT count(*) FROM document_revision_source s
                             WHERE s.revision_id = r.revision_id) AS raw_rows
                      FROM document d
                      JOIN provider p ON p.provider_id = d.provider_id
                      JOIN document_revision r ON r.revision_id = d.current_revision_id
                      LEFT JOIN publisher pub ON pub.publisher_id = r.publisher_id
                     WHERE p.code = 'alpaca' AND d.source_namespace = %s
                       AND d.source_record_id = %s AND d.is_sample = false
                    """,
                    (ALPACA_NAMESPACE, article_id),
                )
                row = cur.fetchone()
                if row is None:
                    checks["problems"].append(f"alpaca article {article_id} not found in the database")
                    continue
                if row["api_provider"] != "alpaca":
                    checks["problems"].append(f"alpaca article {article_id} has provider {row['api_provider']}")
                if int(row["raw_rows"]) != 1:
                    checks["problems"].append(f"alpaca article {article_id} has no original payload stored")
                checks["alpaca"].append({
                    "document_id": row["document_id"], "revision_id": row["revision_id"],
                    "source_record_id": row["source_record_id"], "publisher": row["publisher_name"],
                    "credibility_tier": row["credibility_tier"], "title": (row["title"] or "")[:70],
                    "original_url": row["original_url"], "body_chars": row["body_chars"],
                    "content_status": row["content_status"], "content_scope": row["content_scope"],
                    "company_links": int(row["links"]),
                    "published_at": row["published_at"].isoformat() if row["published_at"] else None,
                })
            for job in self.sec_jobs:
                accession = job["row"]["accessionNumber"]
                cur = conn.execute(
                    """
                    SELECT d.document_id, d.source_namespace, r.revision_id, r.title,
                           r.content_status, r.content_scope, r.original_url,
                           length(coalesce(r.body_text, '')) AS body_chars,
                           s.accession_number, s.form, s.cik, s.filing_date, s.accepted_at,
                           s.item_codes, s.filing_index_url, s.primary_document_url,
                           jsonb_array_length(s.exhibits) AS exhibits,
                           c.name AS issuer,
                           (SELECT count(*) FROM revision_company rc WHERE rc.revision_id = r.revision_id) AS links,
                           (SELECT count(*) FROM document_revision_source src
                             WHERE src.revision_id = r.revision_id
                               AND src.raw_document IS NOT NULL) AS raw_html_rows
                      FROM document d
                      JOIN provider p ON p.provider_id = d.provider_id
                      JOIN document_revision r ON r.revision_id = d.current_revision_id
                      JOIN sec_filing_details s ON s.revision_id = r.revision_id
                      JOIN company c ON c.company_id = s.issuer_company_id
                     WHERE p.code = 'sec_edgar' AND d.source_record_id = %s AND d.is_sample = false
                    """,
                    (accession,),
                )
                row = cur.fetchone()
                if row is None:
                    checks["problems"].append(f"SEC filing {accession} not found in the database")
                    continue
                if int(row["raw_html_rows"]) != 1:
                    checks["problems"].append(f"SEC filing {accession} has no original HTML stored")
                if int(row["body_chars"]) == 0:
                    checks["problems"].append(f"SEC filing {accession} stored no extracted text")
                checks["sec"].append({
                    "document_id": row["document_id"], "revision_id": row["revision_id"],
                    "accession_number": row["accession_number"], "form": row["form"], "cik": row["cik"],
                    "issuer": row["issuer"], "filing_date": str(row["filing_date"]),
                    "accepted_at": row["accepted_at"].isoformat() if row["accepted_at"] else None,
                    "items": list(row["item_codes"]), "exhibits": int(row["exhibits"]),
                    "extracted_chars": int(row["body_chars"]), "content_status": row["content_status"],
                    "content_scope": row["content_scope"], "company_links": int(row["links"]),
                    "primary_document_url": row["primary_document_url"],
                    "filing_index_url": row["filing_index_url"],
                })
        self.summary["readback"] = checks
        print(f"[5/8] read back {len(checks['alpaca'])} article(s) and {len(checks['sec'])} filing(s); "
              f"{len(checks['problems'])} problem(s)")
        for problem in checks["problems"]:
            print(f"        ! {problem}")

    # -- steps 6 and 7 -----------------------------------------------------------
    def replay(self) -> None:
        """Replay the same payloads twice and prove nothing new is created.

        (a) through IngestionService, which is what a repeated REST page or a
            WebSocket redelivery of the same article looks like;
        (b) straight into `persist_record`, bypassing the file ledger, so the
            guarantee is shown to come from the database's unique constraints.
        """
        before = self._counts()
        for entry in self.alpaca_payloads:
            article = entry["article"]
            self.service.process_alpaca({
                "article": article, "fingerprint": _fingerprint(article), "transport": "rest",
                "endpoint": entry["endpoint"], "retrieved_at": _utcnow().isoformat(), "raw_refs": [],
            })
        for job in self.sec_jobs:
            self.service.process_sec(job)
        after_service = self._counts()

        direct = {"skipped": 0, "inserted": 0}
        for record, raw_payload, raw_document in self._prepared_again():
            with self.db.transaction() as conn:
                result = persist_record(conn, record, raw_payload=raw_payload, raw_document=raw_document)
            direct["skipped" if result.outcome == "skipped" else "inserted"] += 1
        after_direct = self._counts()

        self.summary["replay"] = {
            "counts_before": before,
            "counts_after_service_replay": after_service,
            "counts_after_direct_replay": after_direct,
            "service_replay_created_rows": {k: after_service[k] - before[k] for k in before},
            "direct_replay_created_rows": {k: after_direct[k] - after_service[k] for k in before},
            "direct_replay_outcomes": direct,
            "no_duplicates": after_direct == before,
        }
        verdict = "no new rows" if after_direct == before else "ROWS CHANGED"
        print("[6/8] replayed the same payloads through ingestion")
        print(f"[7/8] replayed the same payloads directly into the persistence layer: {verdict} "
              f"({direct['skipped']} reported as already stored)")

    def _prepared_again(self):
        """Re-prepare the fetched payloads to get identical PreparedRecords."""
        from ..ingest.graph import run_prepare
        from ..ingest.schema import SecContext, SourceEnvelope

        for entry in self.alpaca_payloads:
            records, _errors = run_prepare([{
                "provider": "alpaca", "transport": "rest", "api_endpoint": entry["endpoint"],
                "retrieved_at": _utcnow(), "requested_symbols": [self.symbol], "is_sample": False,
                "raw_record": entry["article"], "raw_file_refs": [],
            }])
            if records:
                yield records[0], entry["article"], None
        for job in self.sec_jobs:
            ctx = SecContext.model_validate(job["context"])
            row = job["row"]
            from .. import sec as sec_module

            url = sec_module.primary_document_url(ctx.cik, row["accessionNumber"], row["primaryDocument"])
            html = self.service.store.abs(
                f"raw/sec/documents/{ctx.cik}/{row['accessionNumber']}/{row['primaryDocument']}"
            )
            if not html.exists():
                continue
            text = html.read_text(encoding="utf-8", errors="replace")
            envelope = SourceEnvelope(
                provider="sec_edgar", transport="rest", api_endpoint=job["endpoint"],
                retrieved_at=datetime.fromisoformat(job["retrieved_at"]), requested_symbols=[self.symbol],
                is_sample=False, raw_record=row, raw_file_refs=[job["raw_ref"]], sec_context=ctx,
                primary_document_html=text, primary_document_url=url,
            )
            records, _errors = run_prepare([envelope])
            if records:
                yield records[0], row, text

    def _counts(self) -> dict[str, int]:
        with self.db.connection() as conn:
            cur = conn.execute(
                """
                SELECT (SELECT count(*) FROM document WHERE is_sample = false) AS documents,
                       (SELECT count(*) FROM document_revision r JOIN document d USING (document_id)
                         WHERE d.is_sample = false) AS revisions,
                       (SELECT count(*) FROM revision_company rc JOIN document_revision r USING (revision_id)
                         JOIN document d USING (document_id) WHERE d.is_sample = false) AS company_links,
                       (SELECT count(*) FROM sec_filing_details) AS sec_details,
                       (SELECT count(*) FROM document_revision_source) AS raw_sources
                """
            )
            row = cur.fetchone()
        assert row is not None
        return {k: int(v) for k, v in row.items()}

    # -- step 8 ------------------------------------------------------------------
    def report(self) -> int:
        print("\n[8/8] summary")
        print(json.dumps(self.summary, indent=2, default=str))
        problems = list(self.summary["readback"].get("problems", []))
        if not self.summary["replay"].get("no_duplicates", False):
            problems.append("replay changed row counts: duplicate protection did not hold")
        stored = len(self.summary["readback"].get("alpaca", [])) + len(self.summary["readback"].get("sec", []))
        if stored == 0:
            problems.append("no records were stored and read back")
        print()
        if problems:
            for problem in problems:
                print(f"FAIL: {problem}", file=sys.stderr)
            return 1
        print(f"PASS: {stored} real record(s) written to and read back from "
              f"{self.summary['database']}; replay created no duplicates.")
        return 0

    def run(self) -> int:
        self.verify_database()
        self.fetch_alpaca()
        self.fetch_sec()
        self.persist_all()
        self.read_back()
        self.replay()
        return self.report()


def _fingerprint(article: dict) -> str:
    from ..ingest.adapters import alpaca_fingerprint

    return alpaca_fingerprint(article)


def cmd_smoke(db: Database, args) -> int:
    root = Path(args.output_root) if args.output_root else Path(tempfile.mkdtemp(prefix="datasources-smoke-"))
    print(f"smoke test output root: {root}\n")
    smoke = Smoke(db, args.symbol, max_articles=args.max_articles, lookback_hours=args.lookback_hours,
                  root=root, live_alpaca=not args.no_alpaca, live_sec=not args.no_sec)
    return smoke.run()


# ------------------------------------------------------------------------- entry point
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="datasources-db", description=__doc__.split("\n\n")[0])
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("check", help="Report the database and whether pgvector is enabled")
    p_migrate = sub.add_parser("migrate", help="Build a new database or apply pending migrations (additive)")
    p_migrate.add_argument("--dry-run", action="store_true",
                           help="Show what migrate would do, in a read-only transaction; change nothing")
    p_migrate.add_argument("--adopt-baseline", action="store_true",
                           help="Existing database: record 000_base_schema.sql as adopted, only if every base "
                                "object matches it (it is never executed on an existing schema)")
    p_migrate.add_argument("--json", action="store_true", help="Print the plan as JSON")
    p_verify = sub.add_parser("verify", help="Compare the schema with what the migrations define (read-only)")
    p_verify.add_argument("--json", action="store_true")
    p_register = sub.add_parser("register-companies",
                                help="Add the companies of data/ticker_cik_map.json to company/company_symbol "
                                     "(insert-only, repeatable; starts no collector)")
    p_register.add_argument("--dry-run", action="store_true",
                            help="Show what would be added, in a read-only transaction; change nothing")
    p_register.add_argument("--symbols", help="Only these tickers (comma-separated); default: the whole list")
    p_register.add_argument("--json", action="store_true")
    p_relink = sub.add_parser("relink-companies",
                              help="Link stored articles to companies registered after they were stored "
                                   "(insert-only, repeatable; no model call)")
    p_relink.add_argument("--dry-run", action="store_true",
                          help="Show the links that would be added, in a read-only transaction; change nothing")
    p_relink.add_argument("--sample", action="store_true", help="The sample partition instead of live articles")
    p_relink.add_argument("--json", action="store_true")
    sub.add_parser("inspect", help="Row counts per table")
    p_populate = sub.add_parser("populate", help="Run new/changed populate/*.sql data scripts; log each run")
    p_populate.add_argument("--dry-run", action="store_true", help="Show what would run; change nothing")
    p_populate.add_argument("--force", action="store_true", help="Run every data script again")
    p_populate.add_argument("--json", action="store_true")
    sub.add_parser("export-evaluation", help="Only regenerate populate/001_evaluation_fixtures.sql from "
                                             "populate/event_grouping/*.json (populate does this itself)")

    p_backfill = sub.add_parser("backfill", help="Store existing output/processed records (idempotent)")
    p_backfill.add_argument("--demo", action="store_true", help="Read output/demo instead")

    p_smoke = sub.add_parser("smoke", help="Bounded live end-to-end test")
    p_smoke.add_argument("--symbol", default="NVDA")
    p_smoke.add_argument("--max-articles", type=int, default=5)
    p_smoke.add_argument("--lookback-hours", type=int, default=72,
                         help="Bounded historical window, so the test needs no live news event")
    p_smoke.add_argument("--output-root", help="Where raw/processed files go (default: a temp directory)")
    p_smoke.add_argument("--no-alpaca", action="store_true")
    p_smoke.add_argument("--no-sec", action="store_true")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING,
                        format="%(levelname)s %(name)s %(message)s")
    command = args.command or "check"
    if command == "export-evaluation":  # files only: no database needed
        return cmd_export_evaluation(args)
    database: Database | None = None
    try:
        database = Database(load_db_settings())
        if command == "check":
            return cmd_check(database)
        if command == "migrate":
            return cmd_migrate(database, args)
        if command == "verify":
            return cmd_verify(database, args)
        if command == "register-companies":
            return cmd_register_companies(database, args)
        if command == "relink-companies":
            return cmd_relink_companies(database, args)
        if command == "inspect":
            return cmd_inspect(database)
        if command == "populate":
            return cmd_populate(database, args)
        if command == "backfill":
            return cmd_backfill(database, args)
        if command == "smoke":
            return cmd_smoke(database, args)
        parser.print_help()
        return 2
    except (ConfigurationError, DatabaseError, AlpacaConfigError, KeyError, CoverageError) as error:
        message = error.args[0] if isinstance(error, KeyError) and error.args else str(error)
        sys.stderr.write(f"Configuration error: {message}\n")
        return 2
    finally:
        if database is not None:
            database.close()


if __name__ == "__main__":
    raise SystemExit(main())
