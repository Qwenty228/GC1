"""The labelled event-grouping answer key: file names, the generated populate script, and loading it."""

import json

import pytest

from conftest import temporary_database
from datasources.db import migrate as migrations
from datasources.db.evaluation import (EXPORT_FILE, FIXTURE_DIR, FixtureError, FixtureType, export_sql,
                                       parse_fixture_name, read_fixture, read_fixtures, write_export)
from datasources.db.populate import apply_populate, plan_populate


def _article(folder, name, article_id, **extra):
    (folder / name).write_text(json.dumps({"id": article_id, "created_at": "2026-09-17T11:05:11Z", **extra}))


def test_the_file_name_is_split_into_number_type_and_group():
    assert parse_fixture_name("test_104_conflict_group_041.json") == (104, FixtureType.CONFLICT, 41)
    assert parse_fixture_name("test_001_original_group_001.json") == (1, FixtureType.ORIGINAL, 1)


@pytest.mark.parametrize("name", ["test_104_conflicts_group_041.json", "test_104_conflict_041.json",
                                  "104_conflict_group_041.json", "test_104_conflict_group_041.txt",
                                  "test_066_original_group27.json"])
def test_other_names_are_refused(name):
    with pytest.raises(FixtureError):
        parse_fixture_name(name)


def test_a_fixture_records_the_answer_and_checksum_not_the_content(tmp_path):
    _article(tmp_path, "test_104_conflict_group_041.json", 990010004, headline="h")
    fixture = read_fixture(tmp_path / "test_104_conflict_group_041.json")
    assert (fixture.test_number, fixture.fixture_type, fixture.expected_group) == (104, FixtureType.CONFLICT, 41)
    assert len(fixture.file_checksum) == 64 and not hasattr(fixture, "payload")


@pytest.mark.parametrize("content", ["not json", "[1, 2]", '{"headline": "no id"}'])
def test_a_file_that_is_not_one_alpaca_article_is_refused(tmp_path, content):
    (tmp_path / "test_101_original_group_041.json").write_text(content)
    with pytest.raises(FixtureError):
        read_fixtures(tmp_path)


def test_duplicate_article_ids_or_test_numbers_are_refused(tmp_path):
    _article(tmp_path, "test_101_original_group_041.json", 1)
    _article(tmp_path, "test_102_repeat_group_041.json", 1)
    with pytest.raises(FixtureError, match="article id"):
        read_fixtures(tmp_path)
    (tmp_path / "test_102_repeat_group_041.json").rename(tmp_path / "test_101_repeat_group_041.json")
    with pytest.raises(FixtureError, match="test number"):
        read_fixtures(tmp_path)


def test_the_committed_sql_matches_the_json_files():
    if not any(FIXTURE_DIR.glob("*.json")):
        pytest.skip(f"no fixtures in {FIXTURE_DIR}")
    assert EXPORT_FILE.exists() and EXPORT_FILE.read_text(encoding="utf-8") == export_sql(read_fixtures()), \
        "populate/event_grouping changed: run `uv run datasources-db export-evaluation` and commit the SQL"


# ----------------------------------------------------------------------------- loading (temporary database)
def test_the_generated_sql_loads_the_answer_key_and_follows_edited_files(tmp_path):
    source, scripts = tmp_path / "json", tmp_path / "populate"
    source.mkdir()
    scripts.mkdir()
    _article(source, "test_101_original_group_041.json", 1)
    _article(source, "test_105_different_group_042.json", 5)
    assert write_export(source, scripts / "001_evaluation_fixtures.sql") == 2
    with temporary_database("evaluation") as db:
        apply_populate(db, scripts)
        apply_populate(db, scripts, force=True)  # running it again changes nothing
        with db.connection() as conn:
            rows = conn.execute("SELECT filename, batch, fixture_type, expected_group FROM evaluation.fixture_file "
                                "ORDER BY test_number").fetchall()
        assert [(r["batch"], r["fixture_type"], r["expected_group"]) for r in rows] == [(21, "original", 41),
                                                                                         (21, "different", 42)]
        _article(source, "test_101_original_group_041.json", 1, headline="edited")
        write_export(source, scripts / "001_evaluation_fixtures.sql")
        assert [a for _, a in apply_populate(db, scripts).actions] == ["rerun"]
        with db.connection() as conn:
            checksum = conn.execute("SELECT file_checksum FROM evaluation.fixture_file WHERE test_number = 101"
                                    ).fetchone()["file_checksum"]
        assert checksum == read_fixture(source / "test_101_original_group_041.json").file_checksum


def test_populate_runs_new_and_changed_scripts_once_and_logs_every_run(tmp_path):
    a, b = tmp_path / "001_a.sql", tmp_path / "002_b.sql"
    a.write_text("INSERT INTO evaluation.fixture_file (filename, test_number, fixture_type, expected_group, "
                 "file_checksum) VALUES ('test_001_original_group_001.json', 1, 'original', 1, 'c1') "
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
