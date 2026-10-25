"""`--verbose`: real-time stage logs that are printed once, stay whole across threads and never leak a secret."""

import io
import logging
import re
import threading
import time
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from datasources.db.evaluation import FIXTURE_DIR, read_fixtures
from datasources.evaluation import consolidation, verbose
from datasources.evaluation.gemini_projects import ProjectPool, RecoveringComparator
from datasources.llm_openai import OpenAICompatibleComparisonBackend
from datasources.similarity.comparison import build_output
from datasources.similarity.evaluation import example_request

SECRET = "sk-SECRET-never-printed-123"
ARTICLE_TEXT = "NVIDIA buys Xylo Robotics for $1 billion."
LINE = re.compile(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} \S+\s+.+$")


@pytest.fixture
def log():
    stream = io.StringIO()
    verbose.enable(stream)
    yield stream
    verbose.disable()


def _request():
    return example_request({"id": "t", "incoming": {"title": None, "text": ARTICLE_TEXT},
                            "candidate": {"title": None, "text": "Xylo Robotics will be acquired by NVIDIA."}})


def _gemini(monkeypatch, *, status=200, raise_timeout=False):
    """A fake Gemini endpoint and a comparator on GEMINI_API_KEY_5 whose key value is SECRET."""
    request = _request()
    answer = build_output(request, lambda s, e: "same_event_update").model_dump_json()

    def post(url, json=None, headers=None, timeout=None):
        if raise_timeout:
            raise httpx.ReadTimeout("slow", request=httpx.Request("POST", url))
        body = {"id": "r", "model": "gemini-test", "usage": {"prompt_tokens": 120, "completion_tokens": 40},
                "choices": [{"finish_reason": "stop", "message": {"content": answer}}]}
        return httpx.Response(status, json=body if status < 400 else {"error": {"message": "busy"}},
                              request=httpx.Request("POST", url))
    monkeypatch.setattr(httpx, "post", post)
    monkeypatch.setenv("GEMINI_API_KEY_5", SECRET)
    pool = ProjectPool(["GEMINI_API_KEY_5"], environ={"GEMINI_API_KEY_5": SECRET})
    backend = lambda name: OpenAICompatibleComparisonBackend(base_url="http://gemini/v1", model="gemini-test",
                                                             api_key_env=name, timeout=600)
    comparator = RecoveringComparator(pool, backend, model="gemini-test", start="GEMINI_API_KEY_5",
                                      sleep=lambda _s: None, log=verbose.LOGGER.info)
    return comparator, request


# ----------------------------------------------------------------------------- the handler
def test_each_message_is_printed_once_even_with_a_root_handler(log):
    root_stream = io.StringIO()
    root = logging.StreamHandler(root_stream)
    logging.getLogger().addHandler(root)  # e.g. something called logging.basicConfig()
    try:
        verbose.enable(io.StringIO())  # a second enable() adds nothing
        consolidation.LOGGER.info("database stock_news_eval_x_test: creating")
    finally:
        logging.getLogger().removeHandler(root)
    assert log.getvalue().count("creating") == 1 and root_stream.getvalue() == ""
    assert LINE.match(log.getvalue().splitlines()[0])


def test_without_verbose_the_stage_loggers_are_silent():
    verbose.disable()
    assert not consolidation.LOGGER.isEnabledFor(logging.INFO)
    assert not logging.getLogger("datasources.llm_openai").isEnabledFor(logging.INFO)


def test_lines_from_many_threads_stay_whole_and_unduplicated(log):
    def worker(n):
        for i in range(200):
            consolidation.LOGGER.info("thread %d line %d %s", n, i, "x" * 80)

    threads = [threading.Thread(target=worker, args=(n,), name=f"w{n}") for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    lines = log.getvalue().splitlines()
    assert len(lines) == 1600 and len(set(lines)) == 1600 and all(LINE.match(line) for line in lines)


def test_the_heartbeat_names_the_last_step_while_nothing_else_happens(log):
    verbose.LOGGER.info("    Gemini request -> GEMINI_API_KEY_5 (gemini-test): 1 pair(s)")
    with verbose.heartbeat("batch 9", every=0.05):
        time.sleep(0.3)
    beats = [line for line in log.getvalue().splitlines() if "still running" in line]
    assert beats and all("batch 9 still running" in b and "since:" in b
                         and "Gemini request -> GEMINI_API_KEY_5" in b for b in beats)
    assert all(" heartbeat " in b for b in beats)  # its own thread, named in the line


def test_the_logged_embedder_changes_nothing():
    class Embedder:
        dimensions = 3

        def identity(self):
            return "bge-identity"

        def embed(self, texts):
            return [[float(len(t)), 0.0, 1.0] for t in texts]

    stream = io.StringIO()
    verbose.enable(stream)
    try:
        wrapped = verbose.LoggedEmbedder(Embedder())
        assert wrapped.embed(["ab", "abc"]) == Embedder().embed(["ab", "abc"])
        assert wrapped.identity() == "bge-identity" and wrapped.dimensions == 3
    finally:
        verbose.disable()
    assert "embeddings: 2 text unit(s) encoded" in stream.getvalue() and "abc" not in stream.getvalue()


# ----------------------------------------------------------------------------- Gemini requests
def test_a_request_logs_project_name_http_status_and_parsed_labels_but_no_secret(log, monkeypatch):
    comparator, request = _gemini(monkeypatch)
    comparator.compare(request)
    out = log.getvalue()
    assert "Gemini request -> GEMINI_API_KEY_5 (gemini-test, attempt 1 this batch): 1 segment(s) x 1 candidate" in out
    assert "timeout 600s" in out and re.search(r"HTTP 200 from gemini-test in \d+\.\ds \(timeout 600s\)", out)
    assert "finish_reason stop, tokens in/out 120/40" in out
    assert "schema valid: S1->E1 same_event_update" in out
    for leak in (SECRET, "Bearer", "Authorization", ARTICLE_TEXT, "Xylo"):
        assert leak not in out  # no key, no header, no prompt or article text


def test_a_503_and_a_timeout_are_logged_with_their_timing(log, monkeypatch):
    comparator, request = _gemini(monkeypatch, status=503)
    with pytest.raises(Exception):
        comparator.compare(request)
    assert re.search(r"HTTP 503 from gemini-test in \d+\.\ds", log.getvalue())
    assert "GEMINI_API_KEY_5 failed after" in log.getvalue() and "unavailable" in log.getvalue()
    comparator, request = _gemini(monkeypatch, raise_timeout=True)
    with pytest.raises(Exception):
        comparator.compare(request)
    assert re.search(r"timed out after \d+\.\ds \(timeout 600s\)", log.getvalue())
    assert SECRET not in log.getvalue()


# ----------------------------------------------------------------------------- one batch through the real pipeline
def test_a_verbose_batch_logs_every_stage_in_order(tmp_path, log):
    from conftest import isolated_test_database
    from datasources.similarity.embeddings import build_embedding_backend
    from datasources.similarity.settings import load_similarity_settings
    from test_evaluation_harness import EXPERIMENTS, _module

    gate = isolated_test_database()  # skips without a test database server
    gate.close()
    for f in sorted(FIXTURE_DIR.glob("test_00[1-5]_*.json")):
        (tmp_path / f.name).write_text(f.read_text(encoding="utf-8"), encoding="utf-8")
    items = consolidation.batches(read_fixtures(tmp_path))[1]
    settings = replace(load_similarity_settings(), enabled=True, embedding_backend="hashing",
                       min_semantic_similarity=0.05)
    embedder = verbose.LoggedEmbedder(build_embedding_backend(settings))
    rule = _module(EXPERIMENTS / "baseline_rule.py").build_comparator(settings)
    with verbose.heartbeat("batch 1"):
        result = consolidation.run_batch(1, items, "verbose", rule, embedder, settings, directory=tmp_path)
    out = log.getvalue()
    order = ["database stock_news_eval_verbose_b01_", ": creating", "migration(s) applied", "ready in",
             "article 1/5 test_001_original_group_001.json: ingestion started", "embeddings:",
             "article 1/5 test_001_original_group_001.json: ingestion finished in",
             "retrieval   ok:", "result      predicted different_event, expected - (first article)",
             "article 5/5 test_005_different_group_002.json: ingestion started", ": dropped"]
    positions = [out.find(marker) for marker in order]
    assert all(p >= 0 for p in positions), [m for m, p in zip(order, positions) if p < 0]
    assert positions == sorted(positions)
    marks = re.findall(r"-> (OK|MISS)$", out, re.M)
    assert len(marks) == 5 and marks.count("OK") == sum(
        r["expected_label"] in (None, r["predicted_label"]) for r in result["articles"])
    assert "judgment" in out and all(LINE.match(line) for line in out.splitlines())
