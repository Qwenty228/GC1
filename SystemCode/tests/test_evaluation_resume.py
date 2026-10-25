"""Resuming the Gemini evaluation: which batches rerun, quota / 503 / timeout handling, checkpoints, final scores."""

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from datasources.db.evaluation import FIXTURE_DIR, read_fixtures
from datasources.evaluation import consolidation, metrics, resume
from datasources.evaluation.consolidation import EXPECTED_LABEL
from datasources.evaluation.gemini_projects import (ProjectPool, ProjectUnavailable, RecoveringComparator, RetryPolicy,
                                                    classify, next_quota_reset)
from datasources.llm_openai import OpenAICompatibleComparisonBackend
from datasources.similarity.comparison import ComparisonCall, ComparisonError, build_output
from datasources.similarity.evaluation import example_request

ROOT = Path(__file__).resolve().parents[1]
SAVED_RUN = ROOT / "evaluation" / "consolidation" / "results" / "2026-10-24_1709_gemini.json"
NOW = datetime(2026, 10, 24, 10, 0, tzinfo=timezone.utc)  # 03:00 in California (PDT)
FIXTURES = read_fixtures(FIXTURE_DIR)
BATCHES = consolidation.batches(FIXTURES)
TEST_NUMBERS = {f.filename: f.test_number for f in FIXTURES}
CONFIG_SHA = "c" * 64

DAILY_429 = json.dumps([{"error": {
    "code": 429, "status": "RESOURCE_EXHAUSTED",
    "message": "You exceeded your current quota, please check your plan and billing details. * Quota exceeded for "
               "metric: generativelanguage.googleapis.com/generate_content_free_tier_requests, limit: 20",
    "details": [{"@type": "type.googleapis.com/google.rpc.QuotaFailure", "violations": [{
                    "quotaMetric": "generativelanguage.googleapis.com/generate_content_free_tier_requests",
                    "quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier"}]},
                {"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "41s"}]}}])
MINUTE_429 = DAILY_429.replace("PerDay", "PerMinute").replace("41s", "7s")


# ----------------------------------------------------------------------------- helpers
def _row(f, position, *, label=None, error=None):
    expected = None if position == 0 else EXPECTED_LABEL[f.fixture_type]
    return {"file": f.filename, "type": f.fixture_type.value, "expected_group": f.expected_group,
            "expected_label": expected,
            "predicted_label": "error:pending" if error else (label or expected or "different_event"),
            "predicted_events": [] if error else [f"E{f.expected_group}"], "error": error,
            "judgments": [{"candidate_event": "E1", "label": label or expected, "explanation": f"why {f.filename}",
                           "evidence": {"incoming": [], "candidate": []}, "matching_facts": ["a fact"]}]}


def _batch(number, *, failed=(), labels=None, calls=3):
    """A saved batch like `run_batch` writes it: every article right unless `labels` / `failed` say otherwise."""
    rows = [_row(f, i, label=(labels or {}).get(i),
                 error="comparison: gemini: HTTP 503: unavailable" if i in failed else None)
            for i, f in enumerate(BATCHES[number])]
    return {"batch": number, "seconds": 1.0, "articles": rows, "comparison_calls": calls,
            "tokens": {"input": 1000 * calls, "output": 100 * calls}, "database": None}


def _source(complete=(1, 2, 3, 6), **overrides):
    """A saved 25-batch run: `complete` batches done, every other batch with two failed articles."""
    batches = [_batch(n) if n in complete else _batch(n, failed=(1, 3), calls=1) for n in sorted(BATCHES)]
    for n, batch in overrides.items():
        batches[int(n[1:]) - 1] = batch
    return {"stage": "consolidation", "experiment": {"name": "gemini", "file": "gemini.py"},
            "model": {"comparator": "openai_compatible", "comparison_model": "gemini-test"},
            "settings": {"window_days": 7.0}, "started_at": "2026-10-24T09:30:39+00:00",
            "batches_run": sorted(BATCHES), "incomplete": True, "batches": batches}


def _error(status, body="", retry_after=None, kind="http_error"):
    error = ComparisonError(f"m: HTTP {status}: {body[:300]}", kind=kind, retryable=True)
    error.status_code, error.body, error.retry_after = status, body, retry_after
    return error


def _call():
    return ComparisonCall(build_output(_request(), lambda s, e: "same_event_update"), "gemini-test",
                          {"input_tokens": 10, "output_tokens": 5})


def _request(text="NVIDIA buys X for $1 billion."):
    return example_request({"id": "t", "incoming": {"title": None, "text": text},
                            "candidate": {"title": None, "text": "X will be acquired by NVIDIA for $1 billion."}})


class _Backend:
    """One project's backend: raises the queued errors in order, then answers."""

    def __init__(self, *outcomes):
        self.outcomes, self.calls = list(outcomes), 0

    def compare(self, request):
        self.calls += 1
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if isinstance(outcome, Exception):
            raise outcome
        return _call()


def _comparator(tmp_path, backends, *, start="KEY_1", now=NOW, policy=None, failover=(), logs=None):
    environ = {name: f"secret-value-{name}" for name in backends}
    clock = now if callable(now) else (lambda: now)
    pool = ProjectPool(list(backends), state_path=tmp_path / "projects.json", environ=environ, now=clock,
                       quota_failover=failover)
    sleeps = []
    comparator = RecoveringComparator(pool, lambda name: backends[name], model="gemini-test", start=start,
                                      policy=policy or RetryPolicy(), sleep=sleeps.append, rng=random.Random(0),
                                      log=logs.append if logs is not None else lambda _m: None)
    return comparator, pool, sleeps


class _Clock:
    """A fake wall clock: `sleep` moves it forward."""

    def __init__(self, start=NOW):
        self.t, self.slept = start, []

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.t += timedelta(seconds=seconds)


# ----------------------------------------------------------------------------- resume selection
@pytest.mark.skipif(not SAVED_RUN.is_file(), reason="the saved 125-article Gemini run is not in this checkout")
def test_the_saved_gemini_run_keeps_batches_1_2_3_6_and_reruns_the_other_21():
    source = json.loads(SAVED_RUN.read_text(encoding="utf-8"))
    p = resume.plan(source, BATCHES, {})
    assert p.numbers("original") == [1, 2, 3, 6]
    assert p.pending == [n for n in range(1, 26) if n not in (1, 2, 3, 6)]
    rows = [r for b in source["batches"] for r in b["articles"]]
    assert len(rows) == 125 and sum(map(resume.article_failed, rows)) == 57
    kinds = {}
    for n in p.pending:
        for kind, count in p.batches[n].failures.items():
            kinds[kind] = kinds.get(kind, 0) + count
    assert kinds == {"http_429": 51, "timeout": 4, "http_503": 2}
    assert p.batches[2].outcomes["uncertain"] == 1  # batch 2 holds an `uncertain` and still counts as complete


def test_wrong_labels_and_uncertain_are_answers_not_failures():
    wrong = _batch(4, labels={1: "same_event_update", 2: "uncertain"})
    p = resume.plan(_source(B4=wrong), BATCHES, {})
    assert p.batches[4].state == "original" and 4 not in p.pending
    assert p.batches[4].outcomes == {"first_article": 1, "incorrect": 1, "uncertain": 1, "correct": 2}
    assert [resume.failure_kind(r) for r in _batch(5, failed=(2,))["articles"]] == [None, None, "http_503", None,
                                                                                   None]


@pytest.mark.parametrize("error, kind", [("comparison: m: HTTP 429: quota", "http_429"),
                                         ("comparison: m: timed out after 180.0s", "timeout"),
                                         ("comparison: m: HTTP 503: busy", "http_503"), (None, "error:pending")])
def test_failure_kinds_are_read_from_the_saved_error(error, kind):
    row = {"predicted_label": "error:pending", "error": error}
    assert resume.failure_kind(row) == kind and resume.article_outcome(row) == f"failed:{kind}"


def test_a_complete_saved_batch_is_never_replaced_by_a_checkpoint():
    p = resume.plan(_source(), BATCHES, {1: {"batch": _batch(1)}, 4: {"batch": _batch(4)}})
    assert p.batches[1].state == "original" and p.batches[4].state == "recovered" and 4 not in p.pending


def test_a_saved_run_that_does_not_match_the_fixtures_is_refused():
    shifted = _batch(4)
    shifted["articles"][2]["expected_label"] = "same_event_conflict"
    with pytest.raises(resume.ResumeError, match="does not match the fixtures"):
        resume.plan(_source(B4=shifted), BATCHES, {})


# ----------------------------------------------------------------------------- classifying provider errors
def test_a_daily_quota_429_is_exhausted_until_midnight_pacific():
    failure = classify(_error(429, DAILY_429), now=NOW)
    assert failure.kind == "quota_exhausted" and failure.retry_after == 41.0
    assert failure.quota_id == "GenerateRequestsPerDayPerProjectPerModel-FreeTier"
    assert failure.reset_at == datetime(2026, 10, 25, 7, 0, tzinfo=timezone.utc)  # midnight PDT
    assert next_quota_reset(datetime(2026, 12, 1, 9, 0, tzinfo=timezone.utc)) == datetime(2026, 12, 2, 8, 0,
                                                                                         tzinfo=timezone.utc)  # PST


def test_a_per_minute_429_is_a_wait_and_retry_after_wins():
    assert classify(_error(429, MINUTE_429), now=NOW).kind == "rate_limited"
    assert classify(_error(429, MINUTE_429), now=NOW).retry_after == 7.0
    assert classify(_error(429, MINUTE_429, retry_after="12"), now=NOW).retry_after == 12.0
    http_date = "Sat, 24 Oct 2026 10:00:30 GMT"
    assert classify(_error(429, "{}", retry_after=http_date), now=NOW).retry_after == 30.0


@pytest.mark.parametrize("error, kind", [
    (_error(503, '[{"error": {"code": 503, "message": "high demand", "status": "UNAVAILABLE"}}]'), "unavailable"),
    (_error(500, "{}"), "unavailable"),
    (ComparisonError("m: timed out after 180.0s", kind="timeout", retryable=True), "timeout"),
    (ComparisonError("m: connection failed", kind="connection", retryable=True), "connection"),
    (_error(401, "{}"), "auth"),
    (_error(400, '{"error": {"message": "API key not valid.", "status": "INVALID_ARGUMENT"}}'), "auth"),
    (_error(404, "{}"), "fatal"),
    (ComparisonError("m: response did not match the schema", kind="invalid_response", retryable=True), "output"),
])
def test_errors_map_to_what_the_retry_loop_does(error, kind):
    assert classify(error, now=NOW).kind == kind


def test_the_openai_backend_keeps_status_retry_after_and_the_full_body(monkeypatch):
    def post(url, json=None, headers=None, timeout=None):
        return httpx.Response(429, text=DAILY_429, headers={"Retry-After": "41"}, request=httpx.Request("POST", url))
    monkeypatch.setattr(httpx, "post", post)
    monkeypatch.setenv("FAKE_KEY", "sk-test")
    backend = OpenAICompatibleComparisonBackend(base_url="http://local/v1", model="m", api_key_env="FAKE_KEY")
    with pytest.raises(ComparisonError) as info:
        backend.compare(_request())
    assert info.value.status_code == 429 and info.value.retry_after == "41" and "PerDay" in info.value.body
    assert "PerDay" not in str(info.value)  # the message keeps 300 characters, as before
    assert classify(info.value, now=NOW).kind == "quota_exhausted"


# ----------------------------------------------------------------------------- retries, fail-over, quota
def test_a_503_moves_the_request_to_the_next_key_at_once(tmp_path):
    first, second = _Backend(_error(503, "{}")), _Backend()
    comparator, pool, sleeps = _comparator(tmp_path, {"KEY_1": first, "KEY_2": second, "KEY_3": _Backend()})
    assert comparator.compare(_request()).model == "gemini-test"
    assert (first.calls, second.calls) == (1, 1) and sleeps == []
    assert comparator.stats["switches"] == [{"from": "KEY_1", "to": "KEY_2", "after": "unavailable"}]
    # KEY_2 stood in for that request only: the run's own project takes the next one, so an outage never moves
    # the run's daily usage onto another project
    assert comparator.active == "KEY_1"
    comparator.compare(_request())
    assert (first.calls, second.calls) == (2, 1)


def test_a_declared_capacity_project_that_answers_keeps_the_next_requests(tmp_path):
    first, second = _Backend(_error(503, "{}")), _Backend()
    comparator, _, _ = _comparator(tmp_path, {"KEY_1": first, "KEY_2": second}, failover=["KEY_2"])
    comparator.compare(_request())
    comparator.compare(_request())
    assert comparator.active == "KEY_2" and (first.calls, second.calls) == (1, 2)


def test_key_1_off_then_key_2_off_ends_on_key_3_and_is_tracked(tmp_path):
    timeout = ComparisonError("m: timed out after 180.0s", kind="timeout", retryable=True)
    first, second, third = _Backend(timeout), _Backend(_error(503, "{}")), _Backend()
    comparator, pool, sleeps = _comparator(tmp_path, {"KEY_1": first, "KEY_2": second, "KEY_3": third})
    comparator.compare(_request())
    assert (first.calls, second.calls, third.calls) == (1, 1, 1) and sleeps == [] and comparator.active == "KEY_1"
    assert [s["to"] for s in comparator.stats["switches"]] == ["KEY_2", "KEY_3"]
    assert pool.states["KEY_1"].counts == {"attempts": 1, "timeout": 1}
    assert pool.states["KEY_2"].counts == {"attempts": 1, "unavailable": 1}
    assert pool.states["KEY_3"].counts == {"attempts": 1, "successes": 1}
    assert all(pool.states[k].status == "available" for k in ("KEY_1", "KEY_2"))  # an outage is not a bad key
    saved = (tmp_path / "projects.json").read_text(encoding="utf-8")
    assert "secret-value" not in saved and '"timeout": 1' in saved  # names and health only, never a key


def test_the_rotation_wraps_around_from_the_active_key(tmp_path):
    third, first = _Backend(_error(503, "{}")), _Backend()
    comparator, _, _ = _comparator(tmp_path, {"KEY_1": first, "KEY_2": _Backend(), "KEY_3": third},
                                   start="KEY_3")
    comparator.compare(_request())
    assert (third.calls, first.calls) == (1, 1) and comparator.stats["switches"][0]["to"] == "KEY_1"


def test_when_every_key_fails_it_waits_once_and_makes_another_pass(tmp_path):
    busy = _error(503, "{}")
    first, second = _Backend(busy), _Backend(busy)
    comparator, _, sleeps = _comparator(tmp_path, {"KEY_1": first, "KEY_2": second})
    comparator.compare(_request())
    assert len(sleeps) == 1 and 5 <= sleeps[0] <= 10 and (first.calls, second.calls) == (2, 1)
    assert comparator.active == "KEY_1"


def test_retrying_one_key_before_switching_is_still_available(tmp_path):
    first = _Backend(_error(503, "{}"), _error(503, "{}"))
    comparator, _, sleeps = _comparator(tmp_path, {"KEY_1": first, "KEY_2": _Backend()},
                                        policy=RetryPolicy(attempts_per_project=3))
    comparator.compare(_request())
    assert first.calls == 3 and len(sleeps) == 2 and comparator.active == "KEY_1"


def test_a_rejected_key_marks_its_project_invalid_and_hands_over(tmp_path):
    comparator, pool, _ = _comparator(tmp_path, {"KEY_1": _Backend(_error(403, "{}")), "KEY_2": _Backend()})
    comparator.compare(_request())
    assert pool.states["KEY_1"].status == "invalid" and comparator.active == "KEY_2"


def test_a_rate_limit_waits_what_the_provider_asks_on_the_same_project(tmp_path):
    first = _Backend(_error(429, MINUTE_429), _error(429, MINUTE_429, retry_after="20"))
    second = _Backend()
    comparator, pool, sleeps = _comparator(tmp_path, {"KEY_1": first, "KEY_2": second})
    comparator.compare(_request())
    assert sleeps == [7.0, 20.0] and first.calls == 3 and second.calls == 0


def test_an_exhausted_quota_stops_the_run_and_never_moves_to_another_project(tmp_path):
    first, second = _Backend(_error(429, DAILY_429)), _Backend()
    comparator, pool, sleeps = _comparator(tmp_path, {"KEY_1": first, "KEY_2": second})
    with pytest.raises(ComparisonError) as info:
        comparator.compare(_request())
    assert info.value.kind == "quota_exhausted" and not info.value.retryable
    assert second.calls == 0 and sleeps == []
    assert comparator.stop.kind == "quota_exhausted" and comparator.stop.until == "2026-10-25T07:00:00+00:00"
    assert pool.states["KEY_1"].status == "exhausted"
    with pytest.raises(ComparisonError, match="not sent"):  # every later request fails without being sent
        comparator.compare(_request())
    assert (first.calls, second.calls) == (1, 0)


def test_a_daily_quota_continues_the_same_request_on_a_project_with_permitted_capacity(tmp_path):
    home, free, paid = _Backend(_error(429, DAILY_429)), _Backend(), _Backend()
    logs = []
    comparator, pool, sleeps = _comparator(tmp_path, {"KEY_1": home, "KEY_2": free, "KEY_3": paid},
                                           failover=["KEY_3"], logs=logs)
    comparator.batch_label = "batch 14"
    assert comparator.compare(_request()).model == "gemini-test" and comparator.stop is None
    assert (home.calls, free.calls, paid.calls) == (1, 0, 1)  # KEY_2 is not declared capacity: never used
    assert pool.states["KEY_1"].status == "exhausted" and pool.states["KEY_1"].tier == "free"
    assert pool.states["KEY_1"].until == "2026-10-25T07:00:00+00:00" and comparator.active == "KEY_3"
    text = "\n".join(logs)
    for line in ("KEY_1: HTTP 429, daily quota exhausted", "KEY_1: project unavailable until quota reset",
                 "selecting another authorized project with permitted capacity: KEY_3",
                 "continuing batch 14 on KEY_3"):
        assert line in text
    assert "secret-value" not in text and sleeps == []


def test_a_declared_project_that_reports_a_free_tier_quota_is_not_failover_capacity(tmp_path):
    home, declared, other = _Backend(_error(429, DAILY_429)), _Backend(_error(429, DAILY_429)), _Backend()
    logs = []
    comparator, pool, _ = _comparator(tmp_path, {"KEY_1": home, "KEY_2": declared, "KEY_3": other},
                                      failover=["KEY_2"], logs=logs)
    with pytest.raises(ComparisonError) as info:
        comparator.compare(_request())
    assert info.value.kind == "quota_exhausted" and (home.calls, declared.calls, other.calls) == (1, 1, 0)
    assert pool.states["KEY_2"].tier == "free" and not pool.permitted("KEY_2", "KEY_1")
    assert "reports a free-tier quota" in "\n".join(logs)
    assert comparator.stop.until == "2026-10-25T07:00:00+00:00"  # the earliest permitted reset: the home's
    assert pool.capacity_names("KEY_1") == ["KEY_1"]  # even once KEY_2's quota is back, it is not capacity
    reloaded = ProjectPool(["KEY_1", "KEY_2", "KEY_3"], state_path=tmp_path / "projects.json",
                           environ={"KEY_1": "a", "KEY_2": "b", "KEY_3": "c"}, quota_failover=["KEY_2"])
    assert reloaded.states["KEY_2"].tier == "free" and reloaded.capacity_names("KEY_1") == ["KEY_1"]


def test_after_the_quota_reset_the_run_continues_on_its_home_project(tmp_path):
    clock = _Clock()
    home, other = _Backend(_error(429, DAILY_429)), _Backend()
    comparator, pool, _ = _comparator(tmp_path, {"KEY_1": home, "KEY_2": other}, now=clock.now)
    with pytest.raises(ComparisonError):
        comparator.compare(_request())
    with pytest.raises(ProjectUnavailable) as info:  # a later start sees the same: wait for the reset
        pool.choose_start("KEY_1")
    assert info.value.kind == "quota_exhausted" and info.value.until == "2026-10-25T07:00:00+00:00"
    clock.t = datetime(2026, 10, 25, 7, 1, tzinfo=timezone.utc)  # past midnight Pacific
    comparator.resume()
    assert comparator.compare(_request()).model == "gemini-test"
    assert (home.calls, other.calls) == (2, 0) and pool.choose_start("KEY_1") == "KEY_1"


def test_a_429_that_asks_for_too_long_a_wait_counts_as_exhausted(tmp_path):
    long_wait = MINUTE_429.replace('"7s"', '"3600s"')
    comparator, pool, sleeps = _comparator(tmp_path, {"KEY_1": _Backend(_error(429, long_wait)), "KEY_2": _Backend()})
    with pytest.raises(ComparisonError):
        comparator.compare(_request())
    assert sleeps == [] and pool.states["KEY_1"].until == "2026-10-24T11:00:00+00:00"


def test_a_later_run_does_not_step_past_an_exhausted_project_unless_told_to(tmp_path):
    environ = {"KEY_1": "a", "KEY_2": "b", "KEY_3": "a"}  # KEY_3 is KEY_1's key again: one project, not two
    pool = ProjectPool(["KEY_1", "KEY_2", "KEY_3"], state_path=tmp_path / "p.json", environ=environ, now=lambda: NOW)
    assert pool.configured == ["KEY_1", "KEY_2"] and pool.duplicates == {"KEY_3": "KEY_1"}
    pool.mark("KEY_1", "exhausted", until=NOW + timedelta(hours=2))
    later = ProjectPool(["KEY_1", "KEY_2", "KEY_3"], state_path=tmp_path / "p.json", environ=environ,
                        now=lambda: NOW + timedelta(hours=1))
    with pytest.raises(ProjectUnavailable) as info:
        later.choose_start()
    assert info.value.kind == "quota_exhausted" and info.value.project == "KEY_1"
    assert later.choose_start("KEY_2") == "KEY_2"  # the user's explicit choice
    with pytest.raises(ProjectUnavailable):
        later.choose_start("KEY_1")
    after_reset = ProjectPool(["KEY_1", "KEY_2"], state_path=tmp_path / "p.json", environ=environ,
                              now=lambda: NOW + timedelta(hours=3))
    assert after_reset.choose_start() == "KEY_1"


def test_an_invalid_key_is_skipped_at_start(tmp_path):
    pool = ProjectPool(["KEY_1", "KEY_2"], environ={"KEY_1": "a", "KEY_2": "b"}, now=lambda: NOW)
    pool.mark("KEY_1", "invalid", reason="API key expired")
    assert pool.choose_start() == "KEY_2" and pool.rotation("KEY_1") == ["KEY_2"]


def test_all_projects_failing_every_pass_stops_the_run_as_temporary(tmp_path):
    busy = [_error(503, "{}")] * 3
    first, second = _Backend(*busy), _Backend(*busy)
    comparator, pool, sleeps = _comparator(tmp_path, {"KEY_1": first, "KEY_2": second})
    with pytest.raises(ComparisonError) as info:
        comparator.compare(_request())
    assert info.value.kind == "unavailable" and comparator.stop.kind == "unavailable"
    assert (first.calls, second.calls) == (3, 3) and len(sleeps) == 2  # 3 passes, a short wait between them


def _models_server(responses):
    """httpx.get stand-in for `ProjectPool.check`: one queued (status, body) per key, by the key value."""
    seen = []

    def get(url, headers=None, timeout=None):
        seen.append((url, headers["Authorization"]))
        status, body = responses[headers["Authorization"].removeprefix("Bearer ")]
        return httpx.Response(status, text=body, request=httpx.Request("GET", url))
    return get, seen


def test_the_key_check_skips_an_expired_key_without_using_quota(tmp_path):
    expired = json.dumps([{"error": {"code": 400, "message": "API key expired. Please renew the API key.",
                                     "status": "INVALID_ARGUMENT", "details": [{"reason": "API_KEY_INVALID"}]}}])
    listing = json.dumps({"object": "list", "data": [{"id": "models/gemini-test"}, {"id": "models/other"}]})
    get, seen = _models_server({"k1": (400, expired), "k2": (200, listing), "k3": (403, '{"error": {}}')})
    pool = ProjectPool(["KEY_1", "KEY_2", "KEY_3"], state_path=tmp_path / "p.json",
                       environ={"KEY_1": "k1", "KEY_2": "k2", "KEY_3": "k3"}, now=lambda: NOW)
    results = pool.check("http://gemini/v1beta/openai/", "gemini-test", get=get)
    assert results["KEY_1"].startswith("KEY REJECTED") and "expired" in results["KEY_1"]
    assert results["KEY_2"] == "key ok" and results["KEY_3"].startswith("KEY REJECTED")
    assert seen[0][0] == "http://gemini/v1beta/openai/models"  # the model list: no generation quota
    assert pool.choose_start() == "KEY_2"
    assert "k1" not in (tmp_path / "p.json").read_text(encoding="utf-8")
    # the key is renewed in .env: the next check clears the mark by itself
    get, _ = _models_server({"k1": (200, listing), "k2": (200, listing), "k3": (200, listing)})
    pool.check("http://gemini/v1beta/openai/", "gemini-test", get=get)
    assert pool.choose_start() == "KEY_1"


def test_the_key_check_warns_when_the_model_is_not_listed_and_never_clears_an_exhausted_quota(tmp_path):
    listing = json.dumps({"data": [{"id": "models/other"}]})
    get, _ = _models_server({"k1": (200, listing)})
    pool = ProjectPool(["KEY_1"], environ={"KEY_1": "k1"}, now=lambda: NOW)
    pool.mark("KEY_1", "exhausted", until=NOW + timedelta(hours=1))
    result = pool.check("http://gemini", "gemini-test", get=get)["KEY_1"]
    assert "not in its model list" in result and "quota exhausted until" in result
    assert pool.states["KEY_1"].status == "exhausted"


def test_a_prompt_holding_a_fixture_file_name_is_never_sent(tmp_path):
    backend = _Backend()
    comparator, _, _ = _comparator(tmp_path, {"KEY_1": backend})
    with pytest.raises(ComparisonError):
        comparator.compare(_request("see test_017_repeat_group_007.json"))
    assert backend.calls == 0 and comparator.stop.kind == "fatal"


# ----------------------------------------------------------------------------- checkpoints and crash recovery
class _FakeComparator:
    def __init__(self):
        self.stop = None
        self.begin_batch()

    def begin_batch(self):
        self.stats = {"http_attempts": 3, "projects": ["KEY_1"], "failures": {}, "waited_seconds": 0.0,
                      "switches": []}

    def resume(self):
        self.stop = None


def _store(tmp_path, source):
    store = resume.CheckpointStore(tmp_path / "recovery")
    p = resume.plan(source, BATCHES, {})
    manifest = store.open(source_path=tmp_path / "run.json", source_sha256="s" * 64, configuration={"model": "m"},
                          fixture_checksums={f.filename: f.file_checksum for f in FIXTURES}, plan_=p)
    return store, manifest, p


def test_each_completed_batch_is_checkpointed_and_a_quota_stop_keeps_it(tmp_path):
    source = _source()
    store, manifest, p = _store(tmp_path, source)
    comparator, ran = _FakeComparator(), []

    def run_batch(number, items):
        ran.append(number)
        if number == 5:  # the quota runs out inside batch 5
            from datasources.evaluation.gemini_projects import Stop
            comparator.stop = Stop("quota_exhausted", "KEY_1 quota exhausted", "KEY_1", "2026-10-25T07:00:00+00:00")
            return {**_batch(5, failed=(2, 3, 4)), "_predictions": []}
        return {**_batch(number), "_predictions": []}

    result = resume.recover(p.pending, BATCHES, store, manifest, comparator, run_batch, model="gemini-test",
                            configuration_sha256=CONFIG_SHA, progress=lambda _m: None)
    assert ran == [4, 5] and result.completed == [4] and result.stop.kind == "quota_exhausted"
    assert sorted(p.name for p in store.directory.glob("batch_*.json")) == ["batch_04.json"]
    saved = store.load_manifest()
    assert saved["recovered"]["4"]["model"] == "gemini-test" and saved["recovered"]["4"]["tokens"] == {
        "input": 3000, "output": 300}
    assert saved["recovered"]["4"]["completed_at"] and saved["recovered"]["4"]["projects"] == ["KEY_1"]
    assert [a["batch"] for a in saved["failed_attempts"]] == [5] and saved["failed_attempts"][0]["stopped"]
    # the next run picks up exactly where this one stopped
    recovered, rejected = store.recovered(BATCHES, CONFIG_SHA)
    assert list(recovered) == [4] and not rejected
    assert resume.plan(source, BATCHES, recovered).pending[0] == 5


def test_a_crash_mid_batch_loses_only_that_batch(tmp_path):
    source = _source()
    store, manifest, p = _store(tmp_path, source)

    def run_batch(number, items):
        if number == 7:
            raise KeyboardInterrupt  # Ctrl+C / a crash while batch 7 is in flight
        return _batch(number)

    with pytest.raises(KeyboardInterrupt):
        resume.recover(p.pending, BATCHES, store, manifest, _FakeComparator(), run_batch, model="m",
                       configuration_sha256=CONFIG_SHA, progress=lambda _m: None)
    recovered, _ = store.recovered(BATCHES, CONFIG_SHA)
    assert sorted(recovered) == [4, 5] and resume.plan(source, BATCHES, recovered).pending[0] == 7


def _quota_stop(until="2026-10-25T07:00:00+00:00"):
    from datasources.evaluation.gemini_projects import Stop
    return Stop("quota_exhausted", "all projects with permitted capacity are unavailable", "KEY_2", until)


def test_when_no_capacity_is_left_it_saves_progress_and_waits_for_the_earliest_permitted_reset(tmp_path):
    store, manifest, _ = _store(tmp_path, _source())
    clock, logs = _Clock(), []
    waiter = resume.CapacityWaiter(store, manifest, log=logs.append, sleep=clock.sleep, now=clock.now)
    assert waiter(_quota_stop(), 14) is True
    assert clock.t == datetime(2026, 10, 25, 7, 1, tzinfo=timezone.utc)  # the reset plus one minute
    assert max(clock.slept) <= 900 and sum(clock.slept) == 21 * 3600 + 60  # in chunks, so Ctrl+C stays prompt
    text = "\n".join(logs)
    for line in ("all projects with permitted capacity are currently unavailable", "saving progress",
                 "batch 14 restarts from its first article", "waiting until next permitted quota reset",
                 "automatically resuming batch 14"):
        assert line in text
    saved = store.load_manifest()["stops"][-1]
    assert saved["kind"] == "quota_exhausted" and saved["waiting_until"] == "2026-10-25T07:01:00+00:00"
    assert saved["next_batch"] == 14


def test_the_waiter_does_not_wait_when_told_not_to_or_for_an_unknown_or_too_distant_reset(tmp_path):
    store, manifest, _ = _store(tmp_path, _source())
    clock = _Clock()
    quiet = dict(log=lambda _m: None, sleep=clock.sleep, now=clock.now)
    assert resume.CapacityWaiter(store, manifest, enabled=False, **quiet)(_quota_stop(), 14) is False
    assert resume.CapacityWaiter(store, manifest, **quiet)(_quota_stop(until=None), 14) is False
    assert resume.CapacityWaiter(store, manifest, max_wait=3600, **quiet)(_quota_stop(), 14) is False
    from datasources.evaluation.gemini_projects import Stop
    assert resume.CapacityWaiter(store, manifest, **quiet)(Stop("fatal", "answer key in prompt"), 14) is False
    assert clock.slept == []
    outage = resume.CapacityWaiter(store, manifest, outage_waits=2, **quiet)
    assert [outage(Stop("unavailable", "503 everywhere"), 14) for _ in range(3)] == [True, True, False]
    assert clock.slept == [300.0, 300.0]


def test_the_run_waits_for_the_reset_and_continues_to_all_125_articles(tmp_path):
    source = _source()
    store, manifest, p = _store(tmp_path, source)
    comparator, clock, logs, calls = _FakeComparator(), _Clock(), [], []
    waiter = resume.CapacityWaiter(store, manifest, log=logs.append, sleep=clock.sleep, now=clock.now)
    seen = {}

    def run_batch(number, items):
        calls.append((number, items[0].filename))
        if number == 5 and "quota" not in seen:  # the last permitted quota runs out inside batch 5
            seen["quota"] = True
            seen["batch_04"] = store.batch_path(4).read_bytes()
            comparator.stop = _quota_stop(until="2026-10-24T13:00:00+00:00")
            return _batch(5, failed=(2, 3, 4))
        return _batch(number, labels={2: "same_event_repeated"} if number == 7 else None)  # a genuine MISS

    result = resume.recover_all(source, BATCHES, store, manifest, comparator, run_batch, model="gemini-test",
                                configuration_sha256=CONFIG_SHA, wait=waiter, progress=lambda _m: None)
    assert result.stop is None and result.completed == p.pending
    assert [n for n, _ in calls][:3] == [4, 5, 5]  # batch 5 again after the wait ...
    assert calls[2][1] == BATCHES[5][0].filename  # ... from its first article
    assert len(waiter.waits) == 1 and clock.t >= datetime(2026, 10, 24, 13, 0, tzinfo=timezone.utc)
    assert store.batch_path(4).read_bytes() == seen["batch_04"]  # the checkpoint saved before the wait is intact
    assert [a["batch"] for a in store.load_manifest()["failed_attempts"]] == [5]
    recovered, rejected = store.recovered(BATCHES, CONFIG_SHA)
    assert sorted(recovered) == p.pending and not rejected
    report = resume.combine(source, recovered, BATCHES, source_name="run.json", source_sha256="s" * 64,
                            manifest=store.load_manifest())
    verification = report["recovery"]["verification"]
    assert report["incomplete"] is False and verification["articles"] == verification["unique_articles"] == 125
    assert verification["batches"] == 25 and verification["processing_errors"] == 0
    batch_7 = next(b for b in report["batches"] if b["batch"] == 7)
    assert batch_7["articles"][2]["predicted_label"] == "same_event_repeated"  # the MISS is kept and scored
    assert report["outcomes"]["incorrect"] == 1 and report["metrics"]["labels"]["accuracy"] == 0.99


def test_without_a_wait_the_run_stops_and_max_batches_is_respected(tmp_path):
    source = _source()
    store, manifest, p = _store(tmp_path, source)
    comparator = _FakeComparator()

    def run_batch(number, items):
        if number == 7:
            comparator.stop = _quota_stop()
            return _batch(7, failed=(1,))
        return _batch(number)

    result = resume.recover_all(source, BATCHES, store, manifest, comparator, run_batch, model="m",
                                configuration_sha256=CONFIG_SHA, progress=lambda _m: None)
    assert result.completed == [4, 5] and result.stop.kind == "quota_exhausted"
    comparator.resume()
    limited = resume.recover_all(source, BATCHES, store, manifest, comparator, lambda n, items: _batch(n),
                                 model="m", configuration_sha256=CONFIG_SHA, max_batches=2,
                                 progress=lambda _m: None)
    assert limited.completed == [7, 8] and limited.stop is None


def test_a_batch_file_saved_just_before_a_crash_is_adopted(tmp_path):
    source = _source()
    store, manifest, p = _store(tmp_path, source)
    resume.atomic_write_json(store.batch_path(9), {"batch": _batch(9), "recovery": {
        "batch": 9, "model": "m", "configuration_sha256": CONFIG_SHA}})  # the manifest was never updated
    recovered, _ = store.recovered(BATCHES, CONFIG_SHA)
    store.adopt(manifest, recovered)
    assert store.load_manifest()["recovered"]["9"]["file"] == "batch_09.json"


def test_unusable_checkpoints_are_ignored_and_their_batch_reruns(tmp_path):
    store, manifest, p = _store(tmp_path, _source())
    store.directory.joinpath("batch_04.json").write_text("{not json", encoding="utf-8")
    resume.atomic_write_json(store.batch_path(5), {"batch": _batch(5, failed=(1,)), "recovery": {
        "configuration_sha256": CONFIG_SHA}})
    resume.atomic_write_json(store.batch_path(7), {"batch": _batch(7), "recovery": {"configuration_sha256": "x"}})
    recovered, rejected = store.recovered(BATCHES, CONFIG_SHA)
    assert recovered == {} and set(rejected) == {"batch_04.json", "batch_05.json", "batch_07.json"}
    assert "another configuration" in rejected["batch_07.json"]


def test_a_recovery_is_never_continued_under_different_terms(tmp_path):
    store, manifest, p = _store(tmp_path, _source())
    checksums = {f.filename: f.file_checksum for f in FIXTURES}
    with pytest.raises(resume.ResumeError, match="configuration changed"):
        store.open(source_path=tmp_path / "run.json", source_sha256="s" * 64, configuration={"model": "other"},
                   fixture_checksums=checksums, plan_=p)
    with pytest.raises(resume.ResumeError, match="another file"):
        store.open(source_path=tmp_path / "run.json", source_sha256="t" * 64, configuration={"model": "m"},
                   fixture_checksums=checksums, plan_=p)
    with pytest.raises(resume.ResumeError, match="fixture files changed"):
        store.open(source_path=tmp_path / "run.json", source_sha256="s" * 64, configuration={"model": "m"},
                   fixture_checksums={**checksums, FIXTURES[0].filename: "edited"}, plan_=p)


def test_the_configuration_must_match_the_saved_run():
    from datasources.similarity.settings import load_similarity_settings

    settings = load_similarity_settings()
    configuration = resume.run_configuration(model="gemini-x", base_url="u", json_mode="json_schema",
                                             settings=settings)
    source = {"model": {"comparison_model": "gemini-x", "prompt_version": configuration["prompt_version"],
                        "embedder": {"model_name": settings.embedding_model,
                                     "model_version": settings.embedding_revision}},
              "settings": dict(configuration["settings"])}
    assert resume.source_mismatches(source, configuration) == []
    source["settings"]["max_candidate_events"] = 99
    source["model"]["comparison_model"] = "gemini-y"
    problems = resume.source_mismatches(source, configuration)
    assert len(problems) == 2 and any("SIMILARITY_MAX_CANDIDATE_EVENTS" in p for p in problems)


# ----------------------------------------------------------------------------- the final report
def _all_recovered(source):
    p = resume.plan(source, BATCHES, {})
    return {n: {"batch": _batch(n, labels={2: "uncertain"} if n == 4 else None),
                "recovery": {"batch": n, "model": "gemini-test", "tokens": {"input": 3000, "output": 300},
                             "configuration_sha256": CONFIG_SHA}} for n in p.pending}


def test_the_final_report_has_25_batches_125_articles_and_scores_from_metrics_py():
    source = _source()
    recovered = _all_recovered(source)
    report = resume.combine(source, recovered, BATCHES, source_name="run.json", source_sha256="s" * 64,
                            manifest={"failed_attempts": [{"comparison_calls": 2, "tokens": {"input": 7,
                                                                                              "output": 1}}]})
    assert report["incomplete"] is False and "incomplete_reason" not in report
    assert report["recovery"]["verification"] == {"batches": 25, "expected_batches": 25, "articles": 125,
                                                  "unique_articles": 125, "expected_articles": 125,
                                                  "processing_errors": 0, "pending_batches": []}
    assert report["batches_run"] == list(range(1, 26))
    assert [b["origin"] for b in report["batches"]][:6] == ["original_run"] * 3 + ["recovery"] * 2 + ["original_run"]
    # scores: exactly metrics.py over Prediction objects built straight from the fixtures
    expected = []
    for n, items in BATCHES.items():
        for i, f in enumerate(items):
            label = None if i == 0 else EXPECTED_LABEL[f.fixture_type]
            predicted = "uncertain" if (n == 4 and i == 2) else (label or "different_event")
            expected.append(metrics.Prediction(f.test_number, n, f.expected_group, label,
                                               frozenset({f"batch{n}-group{f.expected_group}"}), predicted))
    assert report["metrics"] == {"grouping_pairwise": metrics.pairwise(expected), "labels": metrics.labels(expected)}
    assert report["metrics"]["grouping_pairwise"]["f1"] == 1.0
    assert report["metrics"]["labels"]["uncertain"] == 1 and report["outcomes"]["uncertain"] == 1
    assert set(report["metrics"]["labels"]["per_label"]) == set(metrics.LABELS)
    # usage counts the 25 kept batches; what the discarded attempts spent is reported separately
    assert report["usage"] == {"comparison_calls": 75, "input_tokens": 75000, "output_tokens": 7500}
    assert report["recovery"]["discarded_usage"]["saved_run_incomplete_batches"]["comparison_calls"] == 21
    assert report["recovery"]["discarded_usage"]["failed_recovery_attempts"]["comparison_calls"] == 2


def test_original_batches_and_the_models_reasons_are_kept_verbatim():
    source = _source()
    report = resume.combine(source, _all_recovered(source), BATCHES, source_name="r", source_sha256="s")
    kept = {k: v for k, v in report["batches"][0].items() if k != "origin"}
    assert kept == source["batches"][0]
    assert report["batches"][0]["articles"][1]["judgments"][0]["explanation"].startswith("why test_002")
    assert report["recovery"]["superseded_rows"]["4"][1]["failure"] == "http_503"


def test_a_report_with_a_pending_batch_is_incomplete():
    source = _source()
    recovered = _all_recovered(source)
    del recovered[25]
    report = resume.combine(source, recovered, BATCHES, source_name="r", source_sha256="s")
    assert report["incomplete"] is True and report["recovery"]["verification"]["pending_batches"] == [25]


def test_rebuilt_predictions_score_like_the_pipelines_own():
    """run_batch renames event ids to E1, E2 ... per batch; rebuilding Predictions from the names changes nothing."""
    ids = {1: "6f1c", 2: "aa02", 3: "77b9"}
    raw = [metrics.Prediction(n, 1, g, lab, frozenset(ids[e] for e in evs), lab or "different_event")
           for n, g, lab, evs in [(1, 1, None, [1]), (2, 1, "same_event_repeated", [1]), (3, 1, "same_event_update", [2]),
                                  (4, 1, "same_event_conflict", [1, 2]), (5, 2, "different_event", [3])]]
    names: dict[str, str] = {}
    rows = [{"file": f"f{p.test_number}", "expected_group": p.expected_group, "expected_label": p.expected_label,
             "predicted_label": p.predicted_label,
             "predicted_events": [names.setdefault(e, f"E{len(names) + 1}") for e in sorted(p.predicted_events)]}
            for p in raw]
    rebuilt = resume.predictions_from_batch({"batch": 1, "articles": rows}, {f"f{i}": i for i in range(1, 6)})
    assert metrics.pairwise(rebuilt) == metrics.pairwise(raw) and metrics.labels(rebuilt) == metrics.labels(raw)


def test_the_final_report_reads_with_datasources_eval_report(tmp_path, capsys):
    from datasources.evaluation import cli

    source = _source()
    path = tmp_path / "gemini_125_completed.json"
    resume.atomic_write_json(path, resume.combine(source, _all_recovered(source), BATCHES, source_name="r",
                                                  source_sha256="s"))
    assert cli.main(["report", str(path)]) == 0
    assert "TOTAL over 25 batch(es)" in capsys.readouterr().out


# ----------------------------------------------------------------------------- one recovery through the real pipeline
def test_a_flaky_batch_is_recovered_through_the_real_pipeline_and_checkpointed(tmp_path):
    from dataclasses import replace

    from conftest import isolated_test_database
    from datasources.similarity.embeddings import build_embedding_backend
    from datasources.similarity.settings import load_similarity_settings
    from test_evaluation_harness import EXPERIMENTS, _module

    gate = isolated_test_database()  # skips without a test database server
    gate.close()
    fixture_dir = tmp_path / "fixtures"
    fixture_dir.mkdir()
    for f in sorted(FIXTURE_DIR.glob("test_00[1-9]_*.json")) + sorted(FIXTURE_DIR.glob("test_010_*.json")):
        (fixture_dir / f.name).write_text(f.read_text(encoding="utf-8"), encoding="utf-8")
    batches = consolidation.batches(read_fixtures(fixture_dir))
    # hashing vectors score low: a low similarity floor makes retrieval shortlist candidates, so the model is called
    settings = replace(load_similarity_settings(), enabled=True, embedding_backend="hashing",
                       min_semantic_similarity=0.05)
    embedder = build_embedding_backend(settings)
    rule = _module(EXPERIMENTS / "baseline_rule.py").build_comparator(settings)

    class Flaky:  # project 1 answers 503, then times out: project 2 stands in for those requests only
        def __init__(self, failures):
            self.failures = list(failures)

        def compare(self, request):
            if self.failures:
                raise self.failures.pop(0)
            return rule.compare(request)

    timeout = ComparisonError("m: timed out after 180.0s", kind="timeout", retryable=True)
    backends = {"KEY_1": Flaky([_error(503, "{}"), timeout]), "KEY_2": Flaky([])}
    comparator, pool, _ = _comparator(tmp_path, backends, now=datetime.now(timezone.utc))
    store = resume.CheckpointStore(tmp_path / "recovery")
    source = {"batches": []}
    p = resume.plan(source, batches, {})
    manifest = store.open(source_path=tmp_path / "run.json", source_sha256="s", configuration={},
                          fixture_checksums={}, plan_=p)
    result = resume.recover(p.pending, batches, store, manifest, comparator,
                            lambda n, items: consolidation.run_batch(n, items, "resume", comparator, embedder,
                                                                     settings, directory=fixture_dir),
                            model="rule", configuration_sha256=CONFIG_SHA, progress=lambda _m: None)
    assert result.completed == [1, 2] and result.stop is None
    recovered, rejected = store.recovered(batches, CONFIG_SHA)
    assert sorted(recovered) == [1, 2] and not rejected
    one, two = pool.states["KEY_1"].counts, pool.states["KEY_2"].counts
    assert (one["unavailable"], one["timeout"], two["successes"]) == (1, 1, 2)  # each failure handed over at once
    assert one["successes"] == one["attempts"] - 2 >= 1  # ... and the home project took the rest back
    assert pool.states["KEY_1"].status == "available" and comparator.active == "KEY_1"
    report = resume.combine(source, recovered, batches, source_name="r", source_sha256="s")
    assert report["incomplete"] is False and report["recovery"]["verification"]["articles"] == 10
