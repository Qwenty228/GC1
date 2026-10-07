"""Live processing activity: the tracker's file, how readers judge it, and the read-only endpoint.

No database, network or model: the tracker writes to a temporary output folder and the endpoint reads it back.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from datasources import activity
from datasources.activity import ActivityTracker, load_activity

T0 = datetime(2030, 1, 1, 12, tzinfo=timezone.utc)


class Clock:
    def __init__(self):
        self.now = T0

    def __call__(self) -> datetime:
        return self.now

    def tick(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@pytest.fixture
def clock():
    return Clock()


def tracker(root, clock, process="worker") -> ActivityTracker:
    return ActivityTracker(root, process, heartbeat=0, clock=clock)


def test_an_article_is_one_item_with_its_steps_in_order(tmp_path, clock):
    track = tracker(tmp_path, clock)
    track.start_item("article", "NVIDIA wins an example contract", symbols=["NVDA"])
    track.step("similar", "Looking for related stories")
    clock.tick(1)
    track.end_step("done", "1 possibly related story", key="similar")
    track.step("compare", "Comparing with 1 story", model="example-model")
    clock.tick(12)
    view = load_activity(tmp_path, "worker", clock.now)
    assert view["state"] == "running" and view["current"]["title"] == "NVIDIA wins an example contract"
    assert [(s["key"], s["state"]) for s in view["current"]["steps"]] == [("similar", "done"), ("compare", "running")]
    assert view["current"]["steps"][1]["model"] == "example-model"

    track.step("save", "Saving and grouping")  # the comparison is closed as done
    track.end_step("done", "saved; added to an existing story")
    track.note("summary", "Summary not needed", "skipped", "repeated information")
    track.update(revision_id="rev-1")
    track.finish_item()
    view = load_activity(tmp_path, "worker", clock.now)
    (item,) = view["recent"]
    assert view["current"] is None and item["state"] == "done" and item["revision_id"] == "rev-1"
    assert [s["state"] for s in item["steps"]] == ["done", "done", "done", "skipped"]
    assert item["result"] == "repeated information"  # the last step that said something


def test_a_failed_step_marks_the_item_and_stopping_interrupts_it(tmp_path, clock):
    track = tracker(tmp_path, clock)
    track.start_item("event", "Example story", event_id="e-1")
    track.step("summary", "Writing the summary", model="example-model")
    track.end_step("failed", "rejected by the citation checks")
    track.finish_item()
    track.start_item("article", "Second")
    track.step("compare", "Comparing with 2 stories")
    track.end_step("done", "ignored: not the running step", key="similar")  # a different key changes nothing
    track.close("stopped")
    view = load_activity(tmp_path, "worker", clock.now)
    assert view["state"] == "stopped" and view["current"] is None
    second, first = view["recent"]
    assert first["state"] == "failed" and first["result"] == "rejected by the citation checks"
    assert second["state"] == "interrupted" and second["steps"][0]["state"] == "interrupted"


def test_an_item_where_nothing_needed_doing_is_skipped_and_postponements_read_plainly(tmp_path, clock):
    from datasources.summary.pipeline import _postponed

    track = tracker(tmp_path, clock, "summary")
    track.start_item("event", "Example story")
    track.note("summary", "Summary postponed", "skipped", _postponed(
        "the event has no published summary; retry backoff until 2026-10-08T11:10:16.569318+08:00"))
    track.finish_item()
    (item,) = load_activity(tmp_path, "summary", clock.now)["recent"]
    assert item["state"] == "skipped" and item["result"] == "waiting before the next try (until 03:10 UTC)"
    assert _postponed("this run's cap of 5 summary request(s) reached (SUMMARY_MAX_REQUESTS_PER_RUN / --limit)") == \
        "this run's limit on AI requests is used up; it stays queued"


def test_a_silent_running_process_is_stale_and_old_items_drop_off(tmp_path, clock):
    track = tracker(tmp_path, clock)
    for n in range(activity.RECENT_ITEMS + 5):
        track.start_item("article", f"item {n}")
    track.finish_item()
    view = load_activity(tmp_path, "worker", clock.now)
    assert len(view["recent"]) == activity.RECENT_ITEMS and view["recent"][0]["title"] == f"item {activity.RECENT_ITEMS + 4}"
    later = clock.now + timedelta(seconds=activity.STALE_AFTER_SECONDS + 1)
    assert load_activity(tmp_path, "worker", later)["state"] == "stale"


def test_missing_unreadable_and_foreign_files_read_as_not_running(tmp_path):
    assert load_activity(tmp_path, "summary")["state"] == "not_running"
    path = activity.activity_path(tmp_path, "summary")
    path.parent.mkdir(parents=True)
    for content in ("{not json", json.dumps({"version": 99, "state": "running"})):
        path.write_text(content, encoding="utf-8")
        assert load_activity(tmp_path, "summary")["state"] == "not_running"


def test_the_file_holds_titles_and_outcomes_only_and_long_text_is_clipped(tmp_path, clock):
    track = tracker(tmp_path, clock)
    track.start_item("article", "x" * 1000)
    track.step("save", "Saving")
    track.end_step("failed", "y" * 1000)
    raw = json.loads(activity.activity_path(tmp_path, "worker").read_text(encoding="utf-8"))
    assert set(raw) == {"version", "process", "pid", "state", "started_at", "updated_at", "heartbeat_seconds",
                        "current", "recent", "progress", "note"}
    assert len(raw["current"]["title"]) == activity.MAX_TEXT and raw["current"]["title"].endswith("…")
    assert len(raw["current"]["steps"][0]["result"]) == activity.MAX_TEXT


def test_the_heartbeat_keeps_a_long_step_from_looking_dead(tmp_path):
    track = ActivityTracker(tmp_path, "summary", heartbeat=0.05)
    try:
        track.start_item("event", "Example")
        track.step("summary", "Writing the summary")
        first = load_activity(tmp_path, "summary")["updated_at"]
        deadline = datetime.now(timezone.utc) + timedelta(seconds=5)
        while load_activity(tmp_path, "summary")["updated_at"] == first:
            assert datetime.now(timezone.utc) < deadline, "the heartbeat did not rewrite the file"
    finally:
        track.close("finished", note="done")
    assert load_activity(tmp_path, "summary")["state"] == "finished"


def test_the_activity_endpoint_reads_both_files_and_starts_nothing(tmp_path, monkeypatch, clock):
    from fastapi.testclient import TestClient

    from datasources import server

    monkeypatch.setenv("INGEST_OUTPUT_DIR", str(tmp_path))
    client = TestClient(server.app)
    body = client.get("/api/activity").json()
    assert body["worker"]["state"] == "not_running" and body["summary"]["state"] == "not_running"

    track = ActivityTracker(tmp_path, "worker", heartbeat=0)  # real clock: the endpoint judges staleness by now
    track.start_item("filing", "EXAMPLE CORP 8-K", symbols=["NVDA"])
    track.step("compare", "Comparing with 2 stories", model="example-model")
    response = client.get("/api/activity")
    assert response.headers["cache-control"] == "no-store"
    worker = response.json()["worker"]
    assert worker["state"] == "running" and worker["current"]["steps"][0]["label"] == "Comparing with 2 stories"
    track.close("stopped")
    assert not any(p.name.startswith(".") for p in (tmp_path / "state").iterdir())  # no temp files left behind
