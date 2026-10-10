"""The labelled event-grouping fixtures: file names, contents and folder-level uniqueness (no database)."""

import json

import psycopg
import pytest

from conftest import temporary_database
from datasources.db.evaluation import (FIXTURE_DIR, FixtureError, load_fixtures, parse_fixture_name, read_fixture,
                                       read_fixtures)


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


# ----------------------------------------------------------------------------- loading (temporary database)
def _write(folder, name, article):
    (folder / name).write_text(json.dumps(article))


def _load(db, folder, **kwargs):
    with db.connection() as conn, conn.transaction():
        return load_fixtures(conn, read_fixtures(folder), **kwargs)


def _log(db):
    with db.connection() as conn:
        return [(r["filename"], r["action"]) for r in
                conn.execute("SELECT filename, action FROM evaluation.load_log ORDER BY log_id")]


def test_loading_is_idempotent_logs_each_change_and_force_reloads_everything(tmp_path):
    a = "test_101_original_group_041.json"
    b = "test_102_repeat_group_041.json"
    _write(tmp_path, a, {"id": 1, "created_at": "2026-09-17T11:05:11Z", "headline": "first"})
    _write(tmp_path, b, {"id": 2, "created_at": "2026-09-17T14:48:00Z", "headline": "second"})
    with temporary_database("evaluation") as db:
        assert _load(db, tmp_path, dry_run=True).count("insert") == 2 and _log(db) == []  # dry run writes nothing
        assert _load(db, tmp_path).count("insert") == 2
        assert _load(db, tmp_path).count("unchanged") == 2  # running again changes nothing
        assert _log(db) == [(a, "insert"), (b, "insert")]

        _write(tmp_path, b, {"id": 2, "created_at": "2026-09-17T14:48:00Z", "headline": "edited"})
        report = _load(db, tmp_path)
        assert (report.count("update"), report.count("unchanged")) == (1, 1)
        with db.connection() as conn:
            row = conn.execute("SELECT payload->>'headline' AS h, batch FROM evaluation.fixture_article "
                               "WHERE filename = %s", (b,)).fetchone()
        assert (row["h"], row["batch"]) == ("edited", 21)  # tests 101-105 are batch 21

        assert _load(db, tmp_path, force=True).count("reload") == 2
        assert _log(db)[-3:] == [(b, "update"), (a, "reload"), (b, "reload")]

        (tmp_path / a).unlink()
        assert _load(db, tmp_path).missing_files == [a]  # a deleted file keeps its row
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"), db.connection() as conn:
            conn.execute("DELETE FROM evaluation.load_log")
