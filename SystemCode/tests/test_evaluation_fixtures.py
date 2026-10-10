"""The labelled event-grouping fixtures: file names, contents and folder-level uniqueness (no database)."""

import json

import pytest

from datasources.db.evaluation import FIXTURE_DIR, FixtureError, parse_fixture_name, read_fixture, read_fixtures


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
