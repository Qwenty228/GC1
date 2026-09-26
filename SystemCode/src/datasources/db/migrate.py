"""Apply the additive SQL migrations in `migrations/`, once each, in filename order.

Migrations are additive only: they never drop tables or delete data. Each file
runs in its own transaction and is recorded in `schema_migration`, so applying
them twice is a no-op.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from .connection import Database

LOGGER = logging.getLogger(__name__)
MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


@dataclass(frozen=True)
class MigrationResult:
    filename: str
    applied: bool  # False = already recorded, nothing ran


def migration_files(directory: Path | None = None) -> list[Path]:
    return sorted((directory or MIGRATIONS_DIR).glob("*.sql"))


def applied_migrations(db: Database) -> set[str]:
    with db.connection() as conn:
        cur = conn.execute(
            "SELECT to_regclass('public.schema_migration') IS NOT NULL AS present"
        )
        row = cur.fetchone()
        if not row or not row["present"]:
            return set()
        cur = conn.execute("SELECT filename FROM schema_migration")
        return {r["filename"] for r in cur.fetchall()}


def apply_migrations(db: Database, directory: Path | None = None) -> list[MigrationResult]:
    """Run every unapplied migration. Returns one result per file, in order."""
    done = applied_migrations(db)
    results: list[MigrationResult] = []
    for path in migration_files(directory):
        if path.name in done:
            results.append(MigrationResult(path.name, applied=False))
            continue
        sql = path.read_text(encoding="utf-8")
        with db.transaction() as conn:
            conn.execute(sql)
            # The first migration creates schema_migration itself, so record after running.
            conn.execute(
                "INSERT INTO schema_migration (filename) VALUES (%s) ON CONFLICT DO NOTHING",
                (path.name,),
            )
        LOGGER.info("applied migration %s", path.name)
        results.append(MigrationResult(path.name, applied=True))
    return results
