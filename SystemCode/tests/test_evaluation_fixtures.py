"""The labelled event-grouping fixtures: file names, contents and folder-level uniqueness (no database)."""

import json

import pytest

from conftest import temporary_database
from datasources.db import migrate as migrations
from datasources.db.evaluation import (EXPORT_FILE, FIXTURE_DIR, FixtureError, export_sql, parse_fixture_name,
                                       read_fixture, read_fixtures)
from datasources.db.populate import apply_populate, plan_populate


def test_the_file_name_is_split_into_number_type_and_group():
    assert parse_fixture_name("test_104_conflict_group_041.json") == (104, "conflict", 41)
    assert parse_fixture_name("test_001_original_group_001.json") == (1, "original", 1)


@pytest.mark.parametrize("name", ["test_104_conflicts_group_041.json", "test_104_conflict_041.json",
                                  "104_conflict_group_041.json", "test_104_conflict_group_041.txt"])
def test_other_names_are_refused(name):
    with pytest.raises(FixtureError):
        parse_fixture_name(name)


def test_a_fixture_keeps_its_payload_and_reads_id_and_time(tmp_path):
    article = {"id": 990010004, "created_at": "2026-09-17T13:05:11Z", "headline": "h"}
    path = tmp_path / "test_104_conflict_group_041.json"
    path.write_text(json.dumps(article))
    fixture = read_fixture(path)
    assert (fixture.test_number, fixture.fixture_type, fixture.expected_group) == (104, "conflict", 41)
    assert fixture.article_id == 990010004 and fixture.payload == article
    assert fixture.published_at.isoformat() == "2026-09-17T13:05:11+00:00"


def test_duplicate_article_ids_in_one_folder_are_refused(tmp_path):
    for name in ("test_101_original_group_041.json", "test_102_repeat_group_041.json"):
        (tmp_path / name).write_text(json.dumps({"id": 1, "created_at": "2026-09-17T11:05:11Z"}))
    with pytest.raises(FixtureError, match="article_id"):
        read_fixtures(tmp_path)


def test_the_committed_fixtures_load():
    if not any(FIXTURE_DIR.glob("*.json")):
        pytest.skip(f"no fixtures in {FIXTURE_DIR}")
    fixtures = read_fixtures(FIXTURE_DIR)
    assert all(f.payload.get("headline") and f.payload.get("content") for f in fixtures)


# ----------------------------------------------------------------------------- export + populate (temporary database)
def test_the_committed_sql_matches_the_json_files():
    if not EXPORT_FILE.exists():
        pytest.skip(f"no {EXPORT_FILE.name}")
    assert EXPORT_FILE.read_text(encoding="utf-8") == export_sql(read_fixtures(FIXTURE_DIR)), \
        "populate/event_grouping changed: run `uv run datasources-db export-evaluation`"


def test_exported_sql_loads_every_fixture_exactly_and_is_safe_to_run_again(tmp_path):
    source = tmp_path / "json"
    source.mkdir()
    tricky = {"id": 990010004, "created_at": "2026-09-17T13:05:11Z", "headline": "Nebius' B300 \\ $10.50 -- 34%",
              "content": "<p>It's \"quoted\"; café</p>", "symbols": ["NBIS"]}
    (source / "test_104_conflict_group_041.json").write_text(json.dumps(tricky))
    scripts = tmp_path / "populate"
    scripts.mkdir()
    (scripts / "001_evaluation_fixtures.sql").write_text(export_sql(read_fixtures(source)))
    with temporary_database("export") as db:
        apply_populate(db, scripts)
        apply_populate(db, scripts, force=True)  # running it again changes nothing
        with db.connection() as conn:
            rows = conn.execute("SELECT payload, batch, fixture_type FROM evaluation.fixture_article").fetchall()
        assert len(rows) == 1 and rows[0]["payload"] == tricky
        assert (rows[0]["batch"], rows[0]["fixture_type"]) == (21, "conflict")


def test_populate_runs_new_and_changed_scripts_once_and_logs_every_run(tmp_path):
    a, b = tmp_path / "001_a.sql", tmp_path / "002_b.sql"
    a.write_text("CREATE TEMP TABLE IF NOT EXISTS t (x int); INSERT INTO evaluation.fixture_article "
                 "(filename, test_number, fixture_type, expected_group, article_id, payload, file_checksum) "
                 "VALUES ('test_001_original_group_001.json', 1, 'original', 1, 1, '{}', 'c1') "
                 "ON CONFLICT (filename) DO NOTHING;")
    b.write_text("SELECT 1;")
    (tmp_path / "source").mkdir()
    (tmp_path / "source" / "ignored.sql").write_text("SELECT 1/0;")  # subfolders are source data, never run
    with temporary_database("populate") as db:
        assert [x for _, x in plan_populate(db, tmp_path).actions] == ["run", "run"]
        assert [x for _, x in apply_populate(db, tmp_path).actions] == ["run", "run"]
        assert [x for _, x in apply_populate(db, tmp_path).actions] == ["skip", "skip"]  # idempotent
        b.write_text("SELECT 2;")
        assert [x for _, x in apply_populate(db, tmp_path).actions] == ["skip", "rerun"]  # changed file runs again
        assert [x for _, x in apply_populate(db, tmp_path, force=True).actions] == ["rerun", "rerun"]
        with db.connection() as conn:
            log = [r["filename"] for r in conn.execute("SELECT filename FROM populate_log ORDER BY log_id")]
        assert log == ["001_a.sql", "002_b.sql", "002_b.sql", "001_a.sql", "002_b.sql"]  # appended, never rewritten
        assert migrations.verify_schema(db).ok  # populate_log is bookkeeping, not schema drift
