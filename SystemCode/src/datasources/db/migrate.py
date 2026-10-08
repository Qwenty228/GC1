"""Apply the SQL migrations in `SystemCode/migrate/`, once each, in filename order.

``000_base_schema.sql`` is the baseline: the base tables every later migration
builds on. How it is handled depends on the database:

    fresh       none of the repository's tables exist yet: the baseline is
                executed like any other migration, then 001, 002, ... follow.
    recorded    the baseline is in ``schema_migration``: pending files run.
    adoptable   the base tables already exist (stock_news was created from the
                project ERD before the baseline file existed) and every base
                object matches the baseline exactly. The baseline is never
                executed there: ``migrate --adopt-baseline`` records it with
                method ``adopted``; without that flag, migrate stops and says so.
    mismatch    some base objects are missing or differ: migrate stops and lists
                them; nothing is recorded or applied.

Every file runs in its own transaction and is recorded in ``schema_migration``
with the SHA-256 of its text (line endings normalized) and how it was recorded
(``executed`` / ``adopted``). A recorded file whose text changed afterwards stops
migrate: applied migrations are never edited. Filenames recorded without a file
(``003_event_match_routing.sql`` on stock_news, from a reverted branch) are
reported and otherwise left alone. Nothing here drops tables or deletes data.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from psycopg import Connection

from . import schema
from .connection import Database

LOGGER = logging.getLogger(__name__)
# SystemCode/migrate/, next to compose.yaml: src/datasources/db/migrate.py -> parents[3] is SystemCode/.
MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "migrate"
BASELINE = "000_base_schema.sql"
# pg_advisory_xact_lock key: one migrate at a time per database ("DSGR" namespace, as the graph-writer lock).
MIGRATION_LOCK = (0x44534752, 100)
KNOWN_UNFILED = {
    "003_event_match_routing.sql": "recorded from a reverted branch; there is no file for it in this repository. "
                                   "Its leftover objects are reported as legacy and left in place.",
}


class MigrationRefused(RuntimeError):
    """Migrate stopped before changing anything; ``plan`` says why."""

    def __init__(self, message: str, plan: "MigrationPlan | None" = None):
        super().__init__(message)
        self.plan = plan


@dataclass(frozen=True)
class MigrationResult:
    filename: str
    applied: bool  # False = already recorded, nothing ran
    method: str = "recorded"  # executed | adopted | recorded (already there before this run)


@dataclass(frozen=True)
class MigrationFile:
    name: str
    path: Path
    checksum: str

    def sql(self) -> str:
        return self.path.read_text(encoding="utf-8")


@dataclass(frozen=True)
class RecordedMigration:
    filename: str
    applied_at: datetime | None
    checksum: str | None
    method: str | None


def file_checksum(path: Path) -> str:
    """SHA-256 of the file's text with CRLF normalized, so a Windows checkout matches the repository."""
    text = path.read_bytes().decode("utf-8-sig").replace("\r\n", "\n")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def migration_files(directory: Path | None = None) -> list[Path]:
    return sorted((directory or MIGRATIONS_DIR).glob("*.sql"))


def load_files(directory: Path | None = None) -> list[MigrationFile]:
    return [MigrationFile(p.name, p, file_checksum(p)) for p in migration_files(directory)]


def _recorded(conn: Connection) -> dict[str, RecordedMigration]:
    if not conn.execute("SELECT to_regclass('public.schema_migration') IS NOT NULL AS present").fetchone()["present"]:
        return {}
    rows = conn.execute("SELECT * FROM schema_migration ORDER BY filename").fetchall()
    return {r["filename"]: RecordedMigration(r["filename"], r.get("applied_at"), r.get("checksum"), r.get("method"))
            for r in rows}


def applied_migrations(db: Database) -> set[str]:
    with db.connection() as conn:
        return set(_recorded(conn))


# ----------------------------------------------------------------------------- planning (read-only)
@dataclass
class MigrationPlan:
    database: str
    server_version: str
    files: list[MigrationFile]
    recorded: dict[str, RecordedMigration]
    baseline_state: str  # recorded | fresh | adoptable | mismatch
    baseline: schema.Comparison | None  # the structural check, when the baseline is not recorded
    actions: list[tuple[str, str]] = field(default_factory=list)  # (filename, execute | adopt)
    unfiled: list[str] = field(default_factory=list)  # recorded without a file
    checksum_mismatches: list[tuple[str, str, str]] = field(default_factory=list)  # (file, recorded, current)
    pgvector_installed: str | None = None
    pgvector_available: str | None = None
    other_tables: list[str] = field(default_factory=list)  # fresh database: tables this repository does not manage
    problems: list[str] = field(default_factory=list)  # any problem: migrate changes nothing

    @property
    def needs_adoption(self) -> bool:
        return self.baseline_state == "adoptable"

    @property
    def up_to_date(self) -> bool:
        return not self.actions and not self.problems

    def as_dict(self) -> dict[str, Any]:
        return {
            "database": self.database, "server_version": self.server_version,
            "pgvector": {"installed": self.pgvector_installed, "available_on_server": self.pgvector_available},
            "baseline": {"file": BASELINE, "state": self.baseline_state,
                         "check": self.baseline.as_dict() if self.baseline else None},
            "files": [{"file": f.name, "checksum": f.checksum,
                       "recorded": f.name in self.recorded,
                       "recorded_method": self.recorded[f.name].method if f.name in self.recorded else None}
                      for f in self.files],
            "actions": [{"file": name, "action": action} for name, action in self.actions],
            "recorded_without_file": [{"file": name, "note": KNOWN_UNFILED.get(name, "unknown migration")}
                                      for name in self.unfiled],
            "checksum_mismatches": [{"file": n, "recorded": r, "current": c} for n, r, c in self.checksum_mismatches],
            "other_tables": self.other_tables,
            "problems": self.problems,
        }


def build_plan(conn: Connection, files: list[MigrationFile], *, adopt_baseline: bool = False) -> MigrationPlan:
    """What migrate would do on this connection's database. Only reads the catalog and schema_migration."""
    row = conn.execute("SELECT current_database() AS db, "
                       "(SELECT extversion FROM pg_extension WHERE extname = 'vector') AS installed, "
                       "(SELECT default_version FROM pg_available_extensions WHERE name = 'vector') AS available"
                       ).fetchone()
    recorded = _recorded(conn)
    plan = MigrationPlan(database=row["db"], server_version=schema.server_version(conn), files=files,
                         recorded=recorded, baseline_state="recorded", baseline=None,
                         pgvector_installed=row["installed"], pgvector_available=row["available"])
    names = {f.name for f in files}
    plan.unfiled = sorted(set(recorded) - names)
    for f in files:
        entry = recorded.get(f.name)
        if entry is not None and entry.checksum and entry.checksum != f.checksum:
            plan.checksum_mismatches.append((f.name, entry.checksum, f.checksum))
            plan.problems.append(
                f"{f.name} was recorded with checksum {entry.checksum[:12]}... but the file now hashes to "
                f"{f.checksum[:12]}...: an applied migration was edited. Restore the file from git and add a new "
                "migration for the change instead")

    if BASELINE not in names:
        plan.problems.append(f"{BASELINE} is missing from {MIGRATIONS_DIR}")
    elif BASELINE not in recorded:
        snap = schema.snapshot(conn)
        comparison = schema.compare(snap, scope="baseline")
        comparison.findings.extend(schema.reference_data(conn))
        plan.baseline = comparison
        base_tables = set(schema.baseline_tables())
        present = base_tables & set(snap["tables"])
        managed_present = (schema.managed_tables() - {schema.BOOKKEEPING_TABLE}) & set(snap["tables"])
        if not managed_present and not recorded:
            plan.baseline_state = "fresh"
            plan.baseline = None
            plan.other_tables = sorted(set(snap["tables"]))
            if plan.pgvector_installed is None and plan.pgvector_available is None:
                plan.problems.append("the pgvector extension is not installed on this PostgreSQL server; install "
                                     "pgvector first (see the README's PostgreSQL prerequisites)")
        elif present == base_tables and comparison.ok:
            plan.baseline_state = "adoptable"
            if not adopt_baseline:
                plan.problems.append(
                    f"{plan.database} already has the base schema but {BASELINE} is not recorded. Every base object "
                    f"matches it (see the check above); record it with `uv run datasources-db migrate "
                    f"--adopt-baseline`. The baseline is never executed on an existing schema")
        else:
            plan.baseline_state = "mismatch"
            absent = sorted(base_tables - present)
            detail = (f"{len(present)} of {len(base_tables)} base tables exist"
                      + (f" (missing: {', '.join(absent)})" if absent else ""))
            plan.problems.append(
                f"{plan.database} has objects of this repository's schema but its base schema does not match "
                f"{BASELINE}: {detail}; {len(comparison.blocking)} blocking difference(s) are listed above. "
                "Nothing was recorded or applied; fix the differences by hand (or start from an empty database)")

    for f in files:
        if f.name in recorded:
            continue
        if f.name == BASELINE:
            if plan.baseline_state == "fresh":
                plan.actions.append((f.name, "execute"))
            elif plan.baseline_state == "adoptable":
                plan.actions.append((f.name, "adopt"))
            continue
        plan.actions.append((f.name, "execute"))
    return plan


def plan_migrations(db: Database, directory: Path | None = None, *, adopt_baseline: bool = False,
                    files: list[MigrationFile] | None = None) -> MigrationPlan:
    """`migrate --dry-run`: the plan, read in a READ ONLY transaction, so it cannot write anything."""
    files = load_files(directory) if files is None else files
    with db.connection() as conn, conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        return build_plan(conn, files, adopt_baseline=adopt_baseline)


# ----------------------------------------------------------------------------- applying
def _ensure_bookkeeping(conn: Connection) -> None:
    """Columns this runner records (nullable: rows recorded before them keep NULL). Only run when recording."""
    conn.execute("ALTER TABLE schema_migration ADD COLUMN IF NOT EXISTS checksum text")
    conn.execute("ALTER TABLE schema_migration ADD COLUMN IF NOT EXISTS method text")


def _record(conn: Connection, migration: MigrationFile, method: str) -> None:
    _ensure_bookkeeping(conn)
    conn.execute(
        "INSERT INTO schema_migration (filename, checksum, method) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (migration.name, migration.checksum, method),
    )


def apply_migrations(db: Database, directory: Path | None = None, *,
                     adopt_baseline: bool = False) -> list[MigrationResult]:
    """Run every pending migration (see the module docstring). Returns one result per file, in order.

    Raises MigrationRefused, before changing anything, when the plan has a problem:
    a mismatched base schema, an existing base schema without ``adopt_baseline``, an
    edited applied migration, or no pgvector for a fresh database.
    """
    files = load_files(directory)
    plan = plan_migrations(db, adopt_baseline=adopt_baseline, files=files)
    if plan.problems:
        raise MigrationRefused("; ".join(plan.problems), plan)
    results: list[MigrationResult] = []
    for migration in files:
        if migration.name in plan.recorded:
            results.append(MigrationResult(migration.name, applied=False))
            continue
        with db.transaction() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", MIGRATION_LOCK)
            if migration.name in _recorded(conn):  # a concurrent migrate got there first
                results.append(MigrationResult(migration.name, applied=False))
                continue
            if migration.name == BASELINE and plan.baseline_state == "adoptable":
                # Re-checked under the lock, in the same transaction that records it.
                check = schema.compare(schema.snapshot(conn), scope="baseline")
                check.findings.extend(schema.reference_data(conn))
                if not check.ok:
                    raise MigrationRefused("the base schema changed since it was checked: "
                                           + "; ".join(f.describe() for f in check.blocking), plan)
                _record(conn, migration, "adopted")
                LOGGER.info("recorded %s as adopted (existing base schema verified)", migration.name)
                results.append(MigrationResult(migration.name, applied=True, method="adopted"))
                continue
            conn.execute(migration.sql())
            _record(conn, migration, "executed")
        LOGGER.info("applied migration %s", migration.name)
        results.append(MigrationResult(migration.name, applied=True, method="executed"))
    return results


def verify_schema(db: Database) -> schema.Comparison:
    """`datasources-db verify`: the full structural check, in a READ ONLY transaction."""
    with db.connection() as conn, conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        return schema.verify(conn)
