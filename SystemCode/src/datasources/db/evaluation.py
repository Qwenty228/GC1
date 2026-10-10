"""Load the labelled event-grouping test articles into `evaluation.fixture_article`.

The files live in `SystemCode/populate/event_grouping/`, one Alpaca-format article
per file, named by the team convention `test_{number}_{type}_group_{group}.json`.
Loading is repeatable: a new file is inserted, a changed file updates its row, an
unchanged one is left alone, and nothing is ever deleted.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from .migrate import file_checksum

FIXTURE_DIR = Path(__file__).resolve().parents[3] / "populate" / "event_grouping"
FIXTURE_NAME = re.compile(r"^test_(\d+)_(original|repeat|update|conflict|different)_group_(\d+)\.json$")


class FixtureError(ValueError):
    """A fixture file that cannot be loaded (bad name, bad JSON, missing id)."""


@dataclass(frozen=True)
class Fixture:
    filename: str
    test_number: int
    fixture_type: str
    expected_group: int
    article_id: int
    published_at: datetime | None
    payload: dict[str, Any]
    file_checksum: str


@dataclass
class LoadReport:
    actions: list[tuple[str, str]] = field(default_factory=list)  # (filename, insert | update | unchanged)
    missing_files: list[str] = field(default_factory=list)  # rows whose file is no longer in the folder

    def count(self, action: str) -> int:
        return sum(1 for _, a in self.actions if a == action)

    def as_dict(self) -> dict[str, Any]:
        return {"inserted": self.count("insert"), "updated": self.count("update"),
                "unchanged": self.count("unchanged"), "missing_files": self.missing_files,
                "actions": [{"file": f, "action": a} for f, a in self.actions]}


def parse_fixture_name(filename: str) -> tuple[int, str, int]:
    """'test_104_conflict_group_041.json' -> (104, 'conflict', 41)."""
    m = FIXTURE_NAME.match(filename)
    if not m:
        raise FixtureError(f"{filename}: expected test_{{number}}_{{type}}_group_{{group}}.json")
    return int(m.group(1)), m.group(2), int(m.group(3))


def read_fixture(path: Path) -> Fixture:
    number, kind, group = parse_fixture_name(path.name)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise FixtureError(f"{path.name}: invalid JSON ({error})") from error
    if not isinstance(payload, dict) or not isinstance(payload.get("id"), int):
        raise FixtureError(f"{path.name}: not an Alpaca article (needs an integer 'id')")
    created = payload.get("created_at")
    try:
        published = datetime.fromisoformat(created.replace("Z", "+00:00")) if created else None
    except (AttributeError, ValueError) as error:
        raise FixtureError(f"{path.name}: created_at {created!r} is not ISO-8601") from error
    return Fixture(path.name, number, kind, group, payload["id"], published, payload, file_checksum(path))


def read_fixtures(directory: Path | None = None) -> list[Fixture]:
    """Every fixture in the folder, validated together: test numbers and article ids must be unique."""
    fixtures = [read_fixture(p) for p in sorted((directory or FIXTURE_DIR).glob("*.json"))]
    for attr in ("test_number", "article_id"):
        seen: dict[Any, str] = {}
        for f in fixtures:
            key = getattr(f, attr)
            if key in seen:
                raise FixtureError(f"{f.filename} and {seen[key]} share {attr} {key}")
            seen[key] = f.filename
    return fixtures


def load_fixtures(conn: Connection, fixtures: list[Fixture], *, dry_run: bool = False) -> LoadReport:
    """Insert new files, update changed ones; never delete. With dry_run, only report."""
    stored = {r["filename"]: r["file_checksum"]
              for r in conn.execute("SELECT filename, file_checksum FROM evaluation.fixture_article")}
    report = LoadReport()
    for f in fixtures:
        action = "insert" if f.filename not in stored else (
            "unchanged" if stored[f.filename] == f.file_checksum else "update")
        report.actions.append((f.filename, action))
        if dry_run or action == "unchanged":
            continue
        conn.execute(
            """
            INSERT INTO evaluation.fixture_article
                (filename, test_number, fixture_type, expected_group, article_id, published_at, payload, file_checksum)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (filename) DO UPDATE SET
                test_number = EXCLUDED.test_number, fixture_type = EXCLUDED.fixture_type,
                expected_group = EXCLUDED.expected_group, article_id = EXCLUDED.article_id,
                published_at = EXCLUDED.published_at, payload = EXCLUDED.payload,
                file_checksum = EXCLUDED.file_checksum, updated_at = now()
            """,
            (f.filename, f.test_number, f.fixture_type, f.expected_group, f.article_id, f.published_at,
             Jsonb(f.payload), f.file_checksum),
        )
    files = {f.filename for f in fixtures}
    report.missing_files = sorted(name for name in stored if name not in files)
    return report
