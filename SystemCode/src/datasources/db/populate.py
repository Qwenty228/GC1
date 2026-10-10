"""Run the data scripts in `SystemCode/populate/`, in filename order, logging every run.

`migrate/` defines the structure; `populate/` fills it. Each `populate/*.sql` file is
data only (INSERTs into tables the migrations created) and must be safe to run more
than once (`ON CONFLICT ... DO NOTHING` or `DO UPDATE`). The runner:

    new file                 runs, and is logged
    changed file             runs again (its checksum is new), and is logged again
    unchanged file           skipped: its current checksum is already in the log
    force                    every file runs again, and is logged again

`populate_log` is append-only: one row per run (filename, checksum, time), never
updated or deleted, so it is also the history of what data went in when. Each file
runs in its own transaction. Run `migrate` first: populate needs the tables.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from psycopg import Connection

from .connection import Database
from .migrate import file_checksum

LOGGER = logging.getLogger("datasources.db.populate")

POPULATE_DIR = Path(__file__).resolve().parents[3] / "populate"
POPULATE_LOCK = (0x44534752, 200)  # advisory lock: two concurrent populate runs never interleave


@dataclass(frozen=True)
class PopulateFile:
    name: str
    path: Path
    checksum: str


@dataclass
class PopulateReport:
    actions: list[tuple[str, str]] = field(default_factory=list)  # (filename, run | rerun | skip)

    def count(self, action: str) -> int:
        return sum(1 for _, a in self.actions if a == action)

    def as_dict(self) -> dict[str, Any]:
        return {"run": self.count("run"), "rerun": self.count("rerun"), "skipped": self.count("skip"),
                "actions": [{"file": f, "action": a} for f, a in self.actions]}


def populate_files(directory: Path | None = None) -> list[PopulateFile]:
    """Top-level `*.sql` only, in filename order; subfolders hold source data, not scripts."""
    return [PopulateFile(p.name, p, file_checksum(p)) for p in sorted((directory or POPULATE_DIR).glob("*.sql"))]


def _ensure_log(conn: Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS populate_log (
            log_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            filename   text NOT NULL,
            checksum   text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT now()
        )""")
    conn.execute("COMMENT ON TABLE populate_log IS "
                 "'Append-only: one row per run of a populate/*.sql data script (see datasources-db populate)'")


def _logged(conn: Connection) -> dict[str, set[str]]:
    """filename -> every checksum it has run with."""
    if not conn.execute("SELECT to_regclass('public.populate_log') IS NOT NULL AS present").fetchone()["present"]:
        return {}
    seen: dict[str, set[str]] = {}
    for row in conn.execute("SELECT filename, checksum FROM populate_log"):
        seen.setdefault(row["filename"], set()).add(row["checksum"])
    return seen


def _action(f: PopulateFile, logged: dict[str, set[str]], force: bool) -> str:
    if f.name not in logged:
        return "run"
    return "rerun" if force or f.checksum not in logged[f.name] else "skip"


def plan_populate(db: Database, directory: Path | None = None, *, force: bool = False) -> PopulateReport:
    """What populate would do. Read-only."""
    with db.connection() as conn, conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        logged = _logged(conn)
    return PopulateReport([(f.name, _action(f, logged, force)) for f in populate_files(directory)])


def apply_populate(db: Database, directory: Path | None = None, *, force: bool = False) -> PopulateReport:
    """Run every new or changed script (every script with force), each in its own transaction, and log it."""
    report = PopulateReport()
    for f in populate_files(directory):
        with db.transaction() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", POPULATE_LOCK)
            _ensure_log(conn)
            action = _action(f, _logged(conn), force)
            if action != "skip":
                conn.execute(f.path.read_text(encoding="utf-8"))
                conn.execute("INSERT INTO populate_log (filename, checksum) VALUES (%s, %s)", (f.name, f.checksum))
                LOGGER.info("populate %s: %s", f.name, action)
        report.actions.append((f.name, action))
    return report
