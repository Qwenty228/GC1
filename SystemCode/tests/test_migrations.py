"""Migrations: a new database from the repository files alone, baseline adoption, 005, and idempotency.

Every test builds its own temporary databases (``stock_news_migtest_<random>_test``, dropped afterwards) on
the server of the configured isolated test database; that database itself is not touched. Skipped unless
PGDATABASE names an isolated test database (as for the other database tests) and the role may CREATE DATABASE.
Records written here are sample rows made through the real graph with scripted models; no model is called.

``DATASOURCES_UPDATE_SCHEMA_MANIFEST=1`` rewrites the baseline/current snapshots in
``datasources/db/schema_expected.json`` from the migration files instead of comparing them.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from conftest import FIXTURES, isolated_test_database

psycopg = pytest.importorskip("psycopg")

from psycopg.rows import dict_row  # noqa: E402

from datasources.db import Database, load_db_settings  # noqa: E402
from datasources.db import migrate as migrations  # noqa: E402
from datasources.db import schema  # noqa: E402

LEGACY_DRIFT = FIXTURES / "legacy_003_drift.sql"
# The committed migration files, as applied to existing databases. Applied migrations are never edited: a change
# goes into a new file. (SHA-256 of the text with line endings normalized; see migrate.file_checksum.)
PINNED = {
    "000_base_schema.sql": "c9b211cdd439",
    "001_document_revision_source.sql": "8a5a2e7fe772",
    "002_semantic_event_grouping.sql": "0b2c052442d6",
    "004_event_summary.sql": "e61b09734927",
    "005_event_summary_citation_quote_index.sql": "37e2b04327df",
}
DATA_TABLES = ("provider", "company", "company_symbol", "publisher", "document", "document_revision",
               "document_revision_source", "revision_company", "embedding_profile", "text_unit", "embedding",
               "revision_analysis", "news_event", "event_comparison", "event_membership", "event_membership_unit",
               "retrieval_candidate", "event_summary", "event_summary_citation", "event_conflict_review",
               "event_summary_job", "event_summary_attempt", "event_review_decision")


# ----------------------------------------------------------------------------- temporary databases
@pytest.fixture(scope="module")
def server():
    database = isolated_test_database()  # the same gate as every database test: never stock_news
    try:
        with database.connection() as conn:
            allowed = conn.execute("SELECT rolcreatedb OR rolsuper AS ok FROM pg_roles "
                                   "WHERE rolname = current_user").fetchone()["ok"]
    finally:
        database.close()
    if not allowed:
        pytest.skip("the database role may not CREATE DATABASE; migration tests need temporary databases")
    return load_db_settings()


@pytest.fixture
def temp_db(server):
    created: list[tuple[str, Database]] = []

    def make() -> Database:
        name = f"stock_news_migtest_{uuid.uuid4().hex[:10]}_test"
        with psycopg.connect(replace(server, dbname="postgres").conninfo(), autocommit=True) as admin:
            admin.execute(f"CREATE DATABASE {name}")
        database = Database(replace(server, dbname=name))
        created.append((name, database))
        return database

    yield make
    for name, database in created:
        database.close()
        with psycopg.connect(replace(server, dbname="postgres").conninfo(), autocommit=True) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def execute(db: Database, sql: str, *params) -> list[dict]:
    with db.transaction() as conn:
        cur = conn.execute(sql, params or None)
        return cur.fetchall() if cur.description else []


def files_upto(tmp_path: Path, last: str) -> Path:
    directory = tmp_path / f"migrations_upto_{last[:3]}"
    directory.mkdir()
    for path in migrations.migration_files():
        if path.name <= last:
            shutil.copy(path, directory / path.name)
    return directory


def snapshot(db: Database) -> dict:
    with db.connection() as conn, conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        snap = json.loads(json.dumps(schema.snapshot(conn), default=str))
    for extension in snap["extensions"].values():
        extension.pop("version", None)
    return snap


def bookkeeping(db: Database) -> list[dict]:
    return execute(db, "SELECT * FROM schema_migration ORDER BY filename")


def row_digests(db: Database, tables=DATA_TABLES, *, columns: dict[str, list[str]] | None = None) -> dict:
    out = {}
    with db.connection() as conn:
        for table in tables:
            cols = (columns or {}).get(table) or [r["attname"] for r in conn.execute(
                "SELECT attname FROM pg_attribute WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped "
                "AND attgenerated = '' ORDER BY attnum", (f"public.{table}",))]
            select = ", ".join(f'"{c}"' for c in cols)
            row = conn.execute(f"SELECT count(*) AS n, md5(string_agg(h, '' ORDER BY h)) AS digest FROM "
                               f"(SELECT md5(row_to_json(x)::text) AS h FROM (SELECT {select} FROM {table}) x) y"
                               ).fetchone()
            out[table] = (cols, int(row["n"]), row["digest"])
    return out


def build_like_stock_news(db: Database, *, with_005: bool = True) -> None:
    """The state stock_news reached before this baseline existed: the base tables from the ERD (never recorded),
    001 and 002 recorded, the reverted 003 branch's leftovers and its record, 004 recorded, and (with_005) 005's
    structure applied by hand without a record."""
    files = {f.name: f for f in migrations.load_files()}
    with db.transaction() as conn:
        conn.execute(files[migrations.BASELINE].sql())  # executed like the ERD script: NOT recorded
    for name in ("001_document_revision_source.sql", "002_semantic_event_grouping.sql"):
        with db.transaction() as conn:
            conn.execute(files[name].sql())
            conn.execute("INSERT INTO schema_migration (filename) VALUES (%s)", (name,))
    with db.transaction() as conn:
        conn.execute(LEGACY_DRIFT.read_text(encoding="utf-8"))
        # Mirrors stock_news's recorded history in this throwaway database; no 003 file exists anywhere.
        conn.execute("INSERT INTO schema_migration (filename) VALUES ('003_event_match_routing.sql')")
    with db.transaction() as conn:
        conn.execute(files["004_event_summary.sql"].sql())
        conn.execute("INSERT INTO schema_migration (filename) VALUES ('004_event_summary.sql')")
    if with_005:
        with db.transaction() as conn:
            conn.execute(files["005_event_summary_citation_quote_index.sql"].sql())  # "by hand": not recorded


def revert_005(db: Database) -> None:
    """Turn event_summary_citation back into its migration-004 structure (one quote per unit only)."""
    with db.transaction() as conn:
        conn.execute("ALTER TABLE event_summary_citation DROP CONSTRAINT event_summary_citation_quote_position_key")
        conn.execute("ALTER TABLE event_summary_citation DROP CONSTRAINT event_summary_citation_quote_index_check")
        conn.execute("ALTER TABLE event_summary_citation DROP COLUMN quote_index")
        conn.execute("ALTER TABLE event_summary_citation ADD CONSTRAINT "
                     "event_summary_citation_summary_id_statement_key_unit_id_key "
                     "UNIQUE (summary_id, statement_key, unit_id)")
        conn.execute("DELETE FROM schema_migration WHERE filename = '005_event_summary_citation_quote_index.sql'")


def populate(db: Database, *, multi_quote: bool) -> dict:
    """Representative sample records through the real graph: an event with an origin, a repeat, an update and a
    conflict (review opened, then an insufficient_evidence decision), a second event, published summaries."""
    import test_event_summary as tes
    from test_similarity_pipeline import ACQ, ACQ_PARAPHRASE, OTHER, article

    from datasources.similarity.lock import GraphWriterLock
    from datasources.summary import review
    from datasources.summary.generation import extractive_output

    label = {"next": "different_event"}

    def script(request):
        out = extractive_output(request)
        words = request.pack.passages[0].text.split()
        if multi_quote and len(words) > 20:  # the headline cites the same passage with two distinct quotes
            out["headline"]["citations"].append({"passage_ref": request.pack.passages[0].ref,
                                                 "quote": " ".join(words[12:20])})
        return out

    at = datetime(2036, 5, 1, 12, tzinfo=timezone.utc)
    with GraphWriterLock(db, is_sample=True, purpose="pytest migrations"):
        grt = tes.grouping_runtime(db, tes.scripted(lambda s, e: label["next"]))
        srt = tes.summarizer(db, script=script)
        first = tes.ingest(db, grt, srt, article(*ACQ, at))
        label["next"] = "same_event_repeated"
        tes.ingest(db, grt, srt, article(*ACQ_PARAPHRASE, at + timedelta(hours=2)))
        label["next"] = "same_event_update"
        tes.ingest(db, grt, srt, article(*tes.UPDATE, at + timedelta(days=1)))
        conflict = tes.ingest(db, tes.grouping_runtime(db, tes.scripted(tes.conflict_label)), srt,
                              article(*tes.CONFLICT_TEXT, at + timedelta(days=1, hours=3)))
        label["next"] = "different_event"
        tes.ingest(db, grt, srt, article(*OTHER, at + timedelta(days=2)))
    (review_row,) = execute(db, "SELECT v.review_id::text AS id FROM event_conflict_review v "
                                "JOIN event_comparison c USING (comparison_id) WHERE c.revision_id = %s",
                            conflict.revision_id)
    review.decide(db, review_row["id"], decision="insufficient_evidence", reviewer="pytest",
                  explanation="kept open across the upgrade", language="en")
    (event,) = execute(db, "SELECT event_id::text AS id FROM event_membership WHERE revision_id = %s",
                       first.revision_id)
    return {"event_id": event["id"], "review_id": review_row["id"]}


# ----------------------------------------------------------------------------- tests without a database
def test_checksums_ignore_line_endings_and_a_bom(tmp_path):
    lf, crlf, bom = tmp_path / "lf.sql", tmp_path / "crlf.sql", tmp_path / "bom.sql"
    lf.write_bytes(b"SELECT 1;\nSELECT 2;\n")
    crlf.write_bytes(b"SELECT 1;\r\nSELECT 2;\r\n")
    bom.write_bytes(b"\xef\xbb\xbfSELECT 1;\nSELECT 2;\n")
    assert migrations.file_checksum(lf) == migrations.file_checksum(crlf) == migrations.file_checksum(bom)
    assert migrations.file_checksum(lf) == hashlib.sha256(b"SELECT 1;\nSELECT 2;\n").hexdigest()


def test_applied_migration_files_are_not_edited():
    files = {f.name: f.checksum for f in migrations.load_files()}
    assert sorted(files) == sorted(PINNED), "a migration file was added or removed; update PINNED deliberately"
    for name, prefix in PINNED.items():
        assert files[name].startswith(prefix), f"{name} changed after it was applied; add a new migration instead"
    assert "003_event_match_routing.sql" not in files  # recorded on stock_news from a reverted branch; never re-created


def test_legacy_notes_cover_exactly_the_manifest_legacy_objects():
    legacy = schema.expected()["legacy"]
    assert set(legacy["extras"]) | set(legacy["variants"]) == set(schema.LEGACY_NOTES)


# ----------------------------------------------------------------------------- a new database
def test_a_new_database_is_built_from_the_repository_files_alone(temp_db):
    db = temp_db()
    plan = migrations.plan_migrations(db)
    assert plan.baseline_state == "fresh" and not plan.problems
    assert [a for _, a in plan.actions] == ["execute"] * 5 and plan.actions[0][0] == migrations.BASELINE
    assert execute(db, "SELECT to_regclass('public.document') IS NULL AS absent")[0]["absent"]  # dry run wrote nothing

    results = migrations.apply_migrations(db)
    assert [(r.filename, r.method) for r in results] == [(f.name, "executed") for f in migrations.load_files()]
    comparison = migrations.verify_schema(db)
    assert comparison.ok and comparison.findings == []
    assert {r["code"] for r in execute(db, "SELECT code FROM provider")} == {"sec_edgar", "alpaca"}
    assert execute(db, "SELECT c.cik, s.symbol, s.market FROM company_symbol s JOIN company c USING (company_id)") == [
        {"cik": "0001045810", "symbol": "NVDA", "market": "NASDAQ"}]
    checksums = {f.name: f.checksum for f in migrations.load_files()}
    assert [(r["filename"], r["checksum"], r["method"]) for r in bookkeeping(db)] == [
        (name, checksums[name], "executed") for name in sorted(checksums)]


def test_the_expected_schema_manifest_matches_the_migration_files(temp_db):
    db = temp_db()
    files = migrations.load_files()
    snaps = {}
    for migration in files:  # executed directly (not planned), so this also works while the manifest is stale
        with db.transaction() as conn:
            conn.execute(migration.sql())
        if migration.name == migrations.BASELINE:
            snaps["baseline"] = snapshot(db)
    snaps["current"] = snapshot(db)
    if os.environ.get("DATASOURCES_UPDATE_SCHEMA_MANIFEST") == "1":
        manifest = json.loads(schema.EXPECTED_PATH.read_text(encoding="utf-8"))
        manifest.update(snaps)
        with db.connection() as conn:
            manifest["generated_on"]["server_version"] = schema.server_version(conn)
        schema.EXPECTED_PATH.write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        schema.expected.cache_clear()
        pytest.skip("schema_expected.json regenerated from the migration files")
    expected = schema.expected()
    assert snaps["baseline"] == expected["baseline"], "000_base_schema.sql and schema_expected.json disagree"
    assert snaps["current"] == expected["current"], "the migration files and schema_expected.json disagree"


def test_migrating_again_changes_nothing(temp_db):
    db = temp_db()
    migrations.apply_migrations(db)
    before = (snapshot(db), bookkeeping(db), row_digests(db))
    plan = migrations.plan_migrations(db)
    assert plan.up_to_date and plan.baseline_state == "recorded"
    results = migrations.apply_migrations(db)
    assert all(not r.applied for r in results)
    assert (snapshot(db), bookkeeping(db), row_digests(db)) == before


# ----------------------------------------------------------------------------- existing databases
def test_an_existing_base_schema_is_adopted_only_on_request_and_keeps_every_record(temp_db):
    db = temp_db()
    build_like_stock_news(db)
    ids = populate(db, multi_quote=True)
    assert execute(db, "SELECT count(*) AS n FROM event_summary_citation WHERE quote_index > 1")[0]["n"] >= 1
    before = (snapshot(db), bookkeeping(db), row_digests(db))

    plan = migrations.plan_migrations(db)
    assert plan.baseline_state == "adoptable" and plan.baseline.ok
    assert {f.key for f in plan.baseline.of("legacy")} == {"table:event_routing_decision",
                                                           "function:validate_routing_decision()"}
    assert plan.actions == [(migrations.BASELINE, "adopt"), ("005_event_summary_citation_quote_index.sql", "execute")]
    assert plan.unfiled == ["003_event_match_routing.sql"]
    assert any("--adopt-baseline" in p for p in plan.problems)
    with pytest.raises(migrations.MigrationRefused):
        migrations.apply_migrations(db)  # never silently: without the flag nothing changes
    assert (snapshot(db), bookkeeping(db), row_digests(db)) == before

    results = migrations.apply_migrations(db, adopt_baseline=True)
    assert [(r.filename, r.method) for r in results if r.applied] == [
        (migrations.BASELINE, "adopted"), ("005_event_summary_citation_quote_index.sql", "executed")]
    assert row_digests(db) == before[2]  # every record kept, byte for byte
    rows = {r["filename"]: r for r in bookkeeping(db)}
    assert rows["003_event_match_routing.sql"]["method"] is None  # reported, never rewritten
    assert rows["001_document_revision_source.sql"]["checksum"] is None  # recorded before checksums
    assert rows[migrations.BASELINE]["method"] == "adopted"
    comparison = migrations.verify_schema(db)
    assert comparison.ok and not comparison.of("unexpected")
    assert {f.key for f in comparison.of("legacy")} == set(schema.LEGACY_NOTES)
    assert migrations.plan_migrations(db).up_to_date

    # The workflows still hold after the upgrade (triggers and keys are unchanged).
    (summary,) = execute(db, "SELECT summary_id::text AS id FROM event_summary WHERE event_id = %s "
                             "AND status = 'published'", ids["event_id"])
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        execute(db, "UPDATE event_summary SET headline = 'rewritten' WHERE summary_id = %s", summary["id"])
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        execute(db, "UPDATE event_summary_citation SET evidence_quote = 'x' WHERE summary_id = %s", summary["id"])
    assert execute(db, "SELECT status FROM event_conflict_review WHERE review_id = %s",
                   ids["review_id"])[0]["status"] == "in_review"


def test_005_upgrades_a_migration_004_database_and_keeps_its_citations(temp_db, tmp_path):
    reference = temp_db()
    migrations.apply_migrations(reference, files_upto(tmp_path, "004_event_summary.sql"))
    db = temp_db()
    migrations.apply_migrations(db)
    ids = populate(db, multi_quote=False)
    revert_005(db)
    assert snapshot(db) == snapshot(reference)  # the same structure as a database that really stopped at 004
    old_columns = [c for c in row_digests(db)["event_summary_citation"][0]]
    before = row_digests(db)

    plan = migrations.plan_migrations(db)
    assert plan.baseline_state == "recorded" and plan.actions == [("005_event_summary_citation_quote_index.sql",
                                                                   "execute")]
    migrations.apply_migrations(db)
    after = row_digests(db, columns={"event_summary_citation": old_columns})
    assert after == before  # existing citations unchanged on their original columns
    assert execute(db, "SELECT DISTINCT quote_index FROM event_summary_citation") == [{"quote_index": 1}]
    keys = {r["def"] for r in execute(db, "SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint "
                                          "WHERE conrelid = 'event_summary_citation'::regclass AND contype = 'u'")}
    assert keys == {"UNIQUE (summary_id, statement_key, unit_id, quote_index)"}
    assert migrations.verify_schema(db).ok

    # Several quotes per (summary, statement, unit) are now possible; a repeated quote_index is not.
    (citation,) = execute(db, "SELECT c.summary_id::text AS s, c.unit_id::text AS u, c.statement_key AS k "
                              "FROM event_summary_citation c JOIN event_summary e USING (summary_id) "
                              "WHERE e.event_id = %s AND c.statement_key = 'H1' AND e.version = 1", ids["event_id"])
    execute(db, "INSERT INTO event_summary_citation (summary_id, unit_id, statement_key, quote_index, evidence_quote) "
                "VALUES (%s, %s, %s, 2, 'a second verified quote')", citation["s"], citation["u"], citation["k"])
    with pytest.raises(psycopg.errors.UniqueViolation):
        execute(db, "INSERT INTO event_summary_citation (summary_id, unit_id, statement_key, quote_index, "
                    "evidence_quote) VALUES (%s, %s, %s, 2, 'again')", citation["s"], citation["u"], citation["k"])


def test_005_is_a_no_op_where_its_structure_was_applied_by_hand(temp_db):
    db = temp_db()
    build_like_stock_news(db, with_005=True)
    before = snapshot(db)["tables"]["event_summary_citation"]
    migrations.apply_migrations(db, adopt_baseline=True)
    assert snapshot(db)["tables"]["event_summary_citation"] == before


def test_a_mismatched_base_schema_is_refused_with_the_differences(temp_db):
    db = temp_db()
    build_like_stock_news(db)
    execute(db, "ALTER TABLE document DROP CONSTRAINT document_record_type_check")
    execute(db, "ALTER TABLE document_revision DISABLE TRIGGER revision_immutable")
    execute(db, "ALTER TABLE publisher ALTER COLUMN name DROP NOT NULL")
    execute(db, "DELETE FROM provider WHERE code = 'alpaca'")
    before = (snapshot(db), bookkeeping(db))
    plan = migrations.plan_migrations(db, adopt_baseline=True)
    assert plan.baseline_state == "mismatch" and plan.problems
    blocking = {f.key: f.kind for f in plan.baseline.blocking}
    assert blocking == {"constraint:document.document_record_type_check": "missing",
                        "trigger:document_revision.revision_immutable": "mismatch",
                        "column:publisher.name": "mismatch",
                        "row:provider.alpaca": "missing"}
    with pytest.raises(migrations.MigrationRefused):
        migrations.apply_migrations(db, adopt_baseline=True)  # the flag never overrides a mismatch
    assert (snapshot(db), bookkeeping(db)) == before


def test_a_partial_base_schema_is_refused(temp_db):
    db = temp_db()
    execute(db, "CREATE TABLE provider (provider_id smallint PRIMARY KEY, code text NOT NULL, name text NOT NULL)")
    execute(db, "CREATE TABLE unrelated_notes (id int)")
    plan = migrations.plan_migrations(db, adopt_baseline=True)
    assert plan.baseline_state == "mismatch"
    assert any("1 of 12 base tables exist" in p for p in plan.problems)
    with pytest.raises(migrations.MigrationRefused):
        migrations.apply_migrations(db, adopt_baseline=True)
    assert execute(db, "SELECT to_regclass('public.schema_migration') IS NULL AS absent")[0]["absent"]


def test_unrelated_tables_do_not_stop_a_new_database(temp_db):
    db = temp_db()
    execute(db, "CREATE TABLE unrelated_notes (id int)")
    plan = migrations.plan_migrations(db)
    assert plan.baseline_state == "fresh" and plan.other_tables == ["unrelated_notes"] and not plan.problems
    migrations.apply_migrations(db)
    assert migrations.verify_schema(db).of("unexpected")[0].key == "table:unrelated_notes"


def test_an_edited_applied_migration_stops_migrate(temp_db, tmp_path):
    directory = tmp_path / "migrations"
    shutil.copytree(migrations.MIGRATIONS_DIR, directory)
    db = temp_db()
    migrations.apply_migrations(db, directory)
    target = directory / "002_semantic_event_grouping.sql"
    lf = target.read_bytes().replace(b"\r\n", b"\n")
    for variant in (lf, lf.replace(b"\n", b"\r\n")):  # a checkout's line endings alone are not an edit
        target.write_bytes(variant)
        assert migrations.plan_migrations(db, directory).up_to_date
    target.write_text(target.read_text(encoding="utf-8") + "\n-- edited after it was applied\n", encoding="utf-8")
    plan = migrations.plan_migrations(db, directory)
    assert [m[0] for m in plan.checksum_mismatches] == ["002_semantic_event_grouping.sql"]
    with pytest.raises(migrations.MigrationRefused, match="an applied migration was edited"):
        migrations.apply_migrations(db, directory)
