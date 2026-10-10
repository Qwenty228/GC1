"""The database structure the migrations define, and a comparison of a live database with it.

`snapshot()` reads a database's structure from the PostgreSQL catalog (tables,
columns, constraints, indexes, triggers, views, functions and extensions in the
``public`` schema; extension members and the ``schema_migration`` bookkeeping
table are left out). `schema_expected.json` holds two snapshots of a database
built from the migration files alone: right after ``000_base_schema.sql``
("baseline") and after every migration ("current"). `tests/test_migrations.py`
rebuilds both on a temporary database and fails if the file is out of date.

`compare()` classifies every difference:

    missing      an expected object does not exist                     (blocking)
    mismatch     it exists with a different definition                  (blocking)
    legacy       a known leftover of the reverted 003 branch            (reported, harmless)
    later        created by a later migration (baseline checks only)   (expected)
    unexpected   not created by this repository                         (reported)

Definitions are compared as PostgreSQL prints them (``pg_get_constraintdef`` and
friends). The manifest records the server version it was generated on; another
major version may print some definitions differently.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from psycopg import Connection

EXPECTED_PATH = Path(__file__).resolve().parent / "schema_expected.json"
BOOKKEEPING_TABLE = "schema_migration"
BOOKKEEPING_TABLES = frozenset({BOOKKEEPING_TABLE, "populate_log"})  # runner-owned logs
BLOCKING = ("missing", "mismatch")
REQUIRED_PROVIDERS = ("sec_edgar", "alpaca")

# Objects the reverted `003_event_match_routing.sql` branch left in databases that recorded it (stock_news
# and copies of it). No code reads or writes them and they hold no rows there. They are reported, never
# changed: the normal upgrade path drops nothing.
LEGACY_NOTES = {
    "table:event_routing_decision": "routing audit table of the reverted local-classifier branch; unused",
    "function:validate_routing_decision()": "trigger function of event_routing_decision; unused",
    "column:event_comparison.cache_key": "comparison cache key of the reverted branch; unused, always NULL",
    "index:event_comparison.event_comparison_cache_idx": "index on event_comparison.cache_key; unused",
    "constraint:event_comparison.event_comparison_label_check":
        "also allows 'same_event_unassessed', which current code never writes",
    "constraint:event_comparison.event_comparison_method_check":
        "also allows 'local_classifier', which current code never writes",
    "constraint:event_membership.event_membership_relation_check":
        "also allows 'same_event_unassessed', which current code never writes",
    "view:current_live_event": "has an extra trailing column unassessed_claims, always 0 with current code",
}


# ----------------------------------------------------------------------------- catalog snapshot
_NOT_EXTENSION_MEMBER = ("NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = '{catalog}'::regclass "
                         "AND d.objid = {oid} AND d.deptype = 'e')")


def _body(text: str | None) -> str | None:
    return None if text is None else text.replace("\r\n", "\n")


def snapshot(conn: Connection) -> dict[str, Any]:
    """The structure of the connected database (read-only catalog queries)."""
    # Transaction-local search path, so type and function names print the same on every database.
    conn.execute("SELECT set_config('search_path', 'public', true)")
    tables: dict[str, dict[str, Any]] = {}
    for row in conn.execute(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND "
        + _NOT_EXTENSION_MEMBER.format(catalog="pg_class", oid="c.oid")
    ):
        if row["relname"] not in BOOKKEEPING_TABLES:
            tables[row["relname"]] = {"columns": {}, "constraints": {}, "indexes": {}, "triggers": {}}

    for row in conn.execute(
        """
        SELECT c.relname AS tbl, a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type,
               a.attnotnull AS not_null, pg_get_expr(ad.adbin, ad.adrelid) AS expr,
               a.attidentity AS identity, a.attgenerated AS generated,
               row_number() OVER (PARTITION BY c.oid ORDER BY a.attnum) AS position
          FROM pg_attribute a
          JOIN pg_class c ON c.oid = a.attrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
          LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
         WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped
        """
    ):
        table = tables.get(row["tbl"])
        if table is None:
            continue
        generated = row["generated"] or ""
        table["columns"][row["name"]] = {
            "type": row["type"], "not_null": bool(row["not_null"]),
            "default": None if generated else row["expr"],
            "generated": row["expr"] if generated else None,
            "identity": row["identity"] or None,
            "position": int(row["position"]),
        }

    # contype 'n' (catalogued NOT NULL constraints, PostgreSQL 18+) is already covered by not_null.
    for row in conn.execute(
        """
        SELECT c.relname AS tbl, con.conname AS name, con.contype AS type, pg_get_constraintdef(con.oid) AS definition
          FROM pg_constraint con
          JOIN pg_class c ON c.oid = con.conrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public' AND con.contype <> 'n'
        """
    ):
        if row["tbl"] in tables:
            tables[row["tbl"]]["constraints"][row["name"]] = {"type": row["type"], "definition": row["definition"]}

    for row in conn.execute(
        """
        SELECT t.relname AS tbl, i.relname AS name, pg_get_indexdef(i.oid) AS definition
          FROM pg_index x
          JOIN pg_class i ON i.oid = x.indexrelid
          JOIN pg_class t ON t.oid = x.indrelid
          JOIN pg_namespace n ON n.oid = t.relnamespace
         WHERE n.nspname = 'public'
           AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = x.indexrelid
                             AND con.conrelid = x.indrelid AND con.contype IN ('p', 'u', 'x'))
        """
    ):
        if row["tbl"] in tables:
            tables[row["tbl"]]["indexes"][row["name"]] = row["definition"]

    for row in conn.execute(
        """
        SELECT c.relname AS tbl, t.tgname AS name, pg_get_triggerdef(t.oid) AS definition, t.tgenabled AS enabled
          FROM pg_trigger t
          JOIN pg_class c ON c.oid = t.tgrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public' AND NOT t.tgisinternal
        """
    ):
        if row["tbl"] in tables:
            enabled = row["enabled"]
            enabled = enabled.decode() if isinstance(enabled, bytes) else str(enabled)
            state = "" if enabled == "O" else f" [enabled={enabled}]"  # 'D' = disabled
            tables[row["tbl"]]["triggers"][row["name"]] = row["definition"] + state

    views = {
        row["relname"]: {"definition": row["definition"], "columns": list(row["columns"])}
        for row in conn.execute(
            "SELECT c.relname, pg_get_viewdef(c.oid) AS definition, "
            "ARRAY(SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 "
            "      AND NOT a.attisdropped ORDER BY a.attnum) AS columns "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm') AND "
            + _NOT_EXTENSION_MEMBER.format(catalog="pg_class", oid="c.oid")
        )
    }
    functions = {
        row["signature"]: {"result": row["result"], "language": row["language"], "kind": row["kind"],
                           "volatility": row["volatility"], "security_definer": bool(row["security_definer"]),
                           "config": list(row["config"] or []), "body": _body(row["body"])}
        for row in conn.execute(
            "SELECT p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' AS signature, "
            "pg_get_function_result(p.oid) AS result, l.lanname AS language, p.prokind::text AS kind, "
            "p.provolatile::text AS volatility, p.prosecdef AS security_definer, p.proconfig AS config, "
            "p.prosrc AS body "
            "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace JOIN pg_language l ON l.oid = p.prolang "
            "WHERE n.nspname = 'public' AND "
            + _NOT_EXTENSION_MEMBER.format(catalog="pg_proc", oid="p.oid")
        )
    }
    extensions = {
        row["extname"]: {"schema": row["schema"], "version": row["extversion"]}
        for row in conn.execute("SELECT e.extname, n.nspname AS schema, e.extversion FROM pg_extension e "
                                "JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname <> 'plpgsql'")
    }
    return {"tables": tables, "views": views, "functions": functions, "extensions": extensions}


def server_version(conn: Connection) -> str:
    return str(conn.execute("SELECT current_setting('server_version') AS v").fetchone()["v"]).split(" ")[0]


# ----------------------------------------------------------------------------- expected structure
@lru_cache(maxsize=1)
def expected() -> dict[str, Any]:
    """The committed manifest: {"generated_on", "baseline", "current", "legacy"}."""
    return json.loads(EXPECTED_PATH.read_text(encoding="utf-8"))


def baseline_tables() -> list[str]:
    return sorted(expected()["baseline"]["tables"])


def managed_tables() -> set[str]:
    return set(expected()["current"]["tables"]) | BOOKKEEPING_TABLES


# ----------------------------------------------------------------------------- comparison
@dataclass(frozen=True)
class Finding:
    kind: str  # missing | mismatch | legacy | later | unexpected
    key: str  # e.g. "column:text_unit.search_tsv", "constraint:document.document_check"
    detail: str = ""
    expected: Any = None
    actual: Any = None

    @property
    def blocking(self) -> bool:
        return self.kind in BLOCKING

    def describe(self) -> str:
        text = self.key
        if self.detail:
            text += f" - {self.detail}"
        if self.kind == "mismatch":
            text += f"\n      expected: {_short(self.expected)}\n      actual:   {_short(self.actual)}"
        return text


def _short(value: Any, limit: int = 400) -> str:
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)
    text = " ".join(text.split())
    return text if len(text) <= limit else text[:limit] + " ..."


@dataclass
class Comparison:
    scope: str  # baseline | current
    findings: list[Finding] = field(default_factory=list)

    def of(self, *kinds: str) -> list[Finding]:
        return [f for f in self.findings if f.kind in kinds]

    @property
    def blocking(self) -> list[Finding]:
        return self.of(*BLOCKING)

    @property
    def ok(self) -> bool:
        return not self.blocking

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for finding in self.findings:
            out[finding.kind] = out.get(finding.kind, 0) + 1
        return out

    def as_dict(self) -> dict[str, Any]:
        return {"scope": self.scope, "ok": self.ok, "counts": self.counts(),
                "findings": [{"kind": f.kind, "key": f.key, "detail": f.detail,
                              **({"expected": f.expected, "actual": f.actual} if f.kind == "mismatch" else {})}
                             for f in self.findings]}


def _column_view(spec: dict[str, Any]) -> dict[str, Any]:
    """What must match for a column; its position is informational only."""
    return {k: v for k, v in spec.items() if k != "position"}


class _Comparer:
    def __init__(self, actual: dict[str, Any], expected_: dict[str, Any], *, scope: str,
                 later: dict[str, Any] | None, legacy: dict[str, Any]):
        self.actual, self.expected, self.scope = actual, expected_, scope
        self.later = later or {"tables": {}, "views": {}, "functions": {}, "extensions": {}}
        self.legacy_variants = legacy.get("variants", {})
        self.legacy_extras = set(legacy.get("extras", []))
        self.findings: list[Finding] = []

    def add(self, kind: str, key: str, detail: str = "", expected_: Any = None, actual: Any = None) -> None:
        self.findings.append(Finding(kind, key, detail, expected_, actual))

    def extra(self, key: str, in_later: bool) -> None:
        if key in self.legacy_extras:
            self.add("legacy", key, LEGACY_NOTES.get(key, "left by the reverted 003 branch"))
        elif in_later:
            self.add("later", key, "created by a later migration")
        else:
            self.add("unexpected", key, "not created by this repository's migrations")

    def differ(self, key: str, want: Any, got: Any) -> None:
        if got == want:
            return
        if key in self.legacy_variants and self.legacy_variants[key] == got:
            self.add("legacy", key, LEGACY_NOTES.get(key, "legacy definition from the reverted 003 branch"))
        else:
            self.add("mismatch", key, "definition differs", want, got)

    def run(self) -> list[Finding]:
        exp, act, later = self.expected, self.actual, self.later
        for name, spec in exp["extensions"].items():
            got = act["extensions"].get(name)
            if got is None:
                self.add("missing", f"extension:{name}")
            elif got["schema"] != spec["schema"]:
                self.add("mismatch", f"extension:{name}", "installed in another schema", spec["schema"], got["schema"])
        for table, spec in exp["tables"].items():
            got = act["tables"].get(table)
            if got is None:
                self.add("missing", f"table:{table}")
                continue
            self.table(table, spec, got, later["tables"].get(table))
        for table in sorted(set(act["tables"]) - set(exp["tables"])):
            self.extra(f"table:{table}", table in later["tables"])
        for name, spec in exp["views"].items():
            got = act["views"].get(name)
            if got is None:
                self.add("missing", f"view:{name}")
            else:
                self.differ(f"view:{name}", spec, got)
        for name in sorted(set(act["views"]) - set(exp["views"])):
            self.extra(f"view:{name}", name in later["views"])
        for name, spec in exp["functions"].items():
            got = act["functions"].get(name)
            if got is None:
                self.add("missing", f"function:{name}")
            else:
                self.differ(f"function:{name}", spec, got)
        for name in sorted(set(act["functions"]) - set(exp["functions"])):
            self.extra(f"function:{name}", name in later["functions"])
        return self.findings

    def table(self, table: str, spec: dict[str, Any], got: dict[str, Any], later_spec: dict[str, Any] | None) -> None:
        later_spec = later_spec or {"columns": {}, "constraints": {}, "indexes": {}, "triggers": {}}
        for column, cspec in spec["columns"].items():
            key = f"column:{table}.{column}"
            if column not in got["columns"]:
                self.add("missing", key)
            else:
                self.differ(key, _column_view(cspec), _column_view(got["columns"][column]))
        for column in sorted(set(got["columns"]) - set(spec["columns"])):
            self.extra(f"column:{table}.{column}", column in later_spec["columns"])
        for kind, label in (("constraints", "constraint"), ("indexes", "index"), ("triggers", "trigger")):
            for name, definition in spec[kind].items():
                key = f"{label}:{table}.{name}"
                if name not in got[kind]:
                    self.add("missing", key)
                else:
                    self.differ(key, definition, got[kind][name])
            for name in sorted(set(got[kind]) - set(spec[kind])):
                self.extra(f"{label}:{table}.{name}", name in later_spec[kind])


def compare(actual: dict[str, Any], *, scope: str = "current", manifest: dict[str, Any] | None = None) -> Comparison:
    """Compare a snapshot with the baseline (only the base objects must match) or the current schema."""
    manifest = manifest or expected()
    if scope not in ("baseline", "current"):
        raise ValueError(f"unknown scope {scope!r}")
    later = manifest["current"] if scope == "baseline" else None
    findings = _Comparer(actual, manifest[scope], scope=scope, later=later,
                         legacy=manifest.get("legacy", {})).run()
    return Comparison(scope, findings)


def reference_data(conn: Connection) -> list[Finding]:
    """Rows the ingestion code needs but never creates (checked read-only)."""
    findings: list[Finding] = []
    if conn.execute("SELECT to_regclass('public.provider') IS NULL AS absent").fetchone()["absent"]:
        return findings  # the missing table is reported by the structure comparison
    codes = {r["code"] for r in conn.execute("SELECT code FROM provider")}
    for code in REQUIRED_PROVIDERS:
        if code not in codes:
            findings.append(Finding("missing", f"row:provider.{code}",
                                    "ingestion looks providers up by code and never creates them"))
    return findings


def verify(conn: Connection) -> Comparison:
    """The full check behind `datasources-db verify`: current structure plus required rows."""
    comparison = compare(snapshot(conn), scope="current")
    comparison.findings.extend(reference_data(conn))
    return comparison
