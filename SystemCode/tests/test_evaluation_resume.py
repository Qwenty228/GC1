"""Consolidation runs: saving after every batch, resuming a stopped run, and verbose logs. Offline: `run_batch`
is replaced by a fake, so no database, embedding model or API is needed."""

import io
import json
import logging
from dataclasses import replace

import httpx
import pytest

from datasources.evaluation import consolidation, metrics
from datasources.llm_openai import OpenAICompatibleComparisonBackend
from datasources.similarity.evaluation import example_request
from datasources.similarity.settings import load_similarity_settings


class FakeComparator:
    name, model = "fake", "fake-model-1"


class FakeEmbedder:
    def identity(self):
        return {"model_name": "fake-embedder", "dimensions": 3}


def fake_run_batch(fail: set[tuple[int, int]] | None = None, calls: list[int] | None = None):
    """A stand-in for run_batch: every article gets its expected label, except (batch, position) pairs in `fail`,
    which end as a processing error (what an HTTP 429 or a timeout leaves behind)."""
    fail = fail or set()

    def run_batch(number, items, name, comparator, embedder, sim_settings, *, directory, keep_database=False):
        if calls is not None:
            calls.append(number)
        rows, predictions = [], []
        for position, f in enumerate(items):
            expected = None if position == 0 else consolidation.EXPECTED_LABEL[f.fixture_type]
            failed = (number, position) in fail
            label = "error:pending" if failed else (expected or "different_event")
            events = [] if failed else [f"E{f.expected_group}"]
            rows.append({"file": f.filename, "type": f.fixture_type.value, "expected_group": f.expected_group,
                         "expected_label": expected, "predicted_label": label, "predicted_events": events,
                         "error": "HTTP 429" if failed else None, "judgments": []})
            predictions.append(metrics.Prediction(f.test_number, number, f.expected_group, expected,
                                                  frozenset(events), label))
        return {"batch": number, "seconds": 0.0, "articles": rows, "comparison_calls": 4,
                "tokens": {"input": 100, "output": 10}, "database": None, "_predictions": predictions}
    return run_batch


@pytest.fixture
def settings():
    return replace(load_similarity_settings(), enabled=True)


def _run(settings, **kwargs):
    return consolidation.run("fake", kwargs.pop("comparator", FakeComparator()), settings, embedder=FakeEmbedder(),
                             batch_numbers=kwargs.pop("batches", [1, 2]), progress=lambda line: None, **kwargs)


def test_a_failed_article_marks_the_run_incomplete(monkeypatch, settings):
    monkeypatch.setattr(consolidation, "run_batch", fake_run_batch(fail={(2, 3)}))
    report = _run(settings)
    assert report["incomplete"] and "test_009" in report["incomplete_reason"]


def test_resume_keeps_complete_batches_and_reruns_only_the_others(monkeypatch, settings):
    monkeypatch.setattr(consolidation, "run_batch", fake_run_batch(fail={(2, 3)}))
    saved = json.loads(json.dumps(_run(settings), default=str))  # as it is read back from the file
    calls = []
    monkeypatch.setattr(consolidation, "run_batch", fake_run_batch(calls=calls))
    report = _run(settings, resume_from=saved)
    assert calls == [2]  # batch 1 kept, batch 2 (one failed article) rerun whole
    assert not report["incomplete"]
    assert report["resumed_from"]["kept_batches"] == [1]
    assert [b["batch"] for b in report["batches"]] == [1, 2]
    assert report["metrics"]["labels"]["accuracy"] == 1.0  # kept batch 1 is re-scored with the new batch 2
    assert report["usage"]["comparison_calls"] == 8  # both batches counted


def test_resume_refuses_a_different_model_or_settings(monkeypatch, settings):
    monkeypatch.setattr(consolidation, "run_batch", fake_run_batch())
    saved = json.loads(json.dumps(_run(settings), default=str))
    other = FakeComparator()
    other.model = "another-model"
    with pytest.raises(ValueError, match="comparison_model"):
        _run(settings, comparator=other, resume_from=saved)
    with pytest.raises(ValueError, match="max_candidate_events"):
        _run(replace(settings, max_candidate_events=settings.max_candidate_events + 1), resume_from=saved)


def test_the_report_is_saved_after_every_batch_so_a_stopped_run_keeps_its_work(monkeypatch, settings, tmp_path):
    monkeypatch.setattr(consolidation, "RESULTS_DIR", tmp_path)
    done = fake_run_batch()

    def stops_at_batch_2(number, *args, **kwargs):
        if number == 2:
            raise KeyboardInterrupt
        return done(number, *args, **kwargs)
    monkeypatch.setattr(consolidation, "run_batch", stops_at_batch_2)
    with pytest.raises(KeyboardInterrupt):
        consolidation.run_experiment("fake", lambda s: FakeComparator(), batches=[1, 2],
                                     build_embedder=lambda s: FakeEmbedder())
    [path] = tmp_path.glob("*_fake.json")
    saved = json.loads(path.read_text())
    assert [b["batch"] for b in saved["batches"]] == [1]
    assert saved["incomplete"] and saved["incomplete_reason"] == "stopped before batches [2]"
    assert not list(tmp_path.glob("*.tmp"))  # written atomically

    calls = []
    monkeypatch.setattr(consolidation, "run_batch", fake_run_batch(calls=calls))
    report = consolidation.run_experiment("fake", lambda s: FakeComparator(), batches=[1, 2], resume=path.stem,
                                          build_embedder=lambda s: FakeEmbedder())
    assert calls == [2] and not report["incomplete"]


def test_an_unknown_resume_file_is_reported(monkeypatch, tmp_path):
    monkeypatch.setattr(consolidation, "RESULTS_DIR", tmp_path)
    with pytest.raises(FileNotFoundError, match="no-such-run"):
        consolidation.load_run("no-such-run")


# ----------------------------------------------------------------------------- verbose
@pytest.fixture
def verbose_stream():
    stream = io.StringIO()
    consolidation.enable_verbose(stream)
    consolidation.enable_verbose(stream)  # twice: still one handler, each line printed once
    yield stream
    for name in ("datasources.evaluation", "datasources.llm_openai"):
        logger = logging.getLogger(name)
        logger.handlers = [h for h in logger.handlers if not getattr(h, "_eval_verbose", False)]
        logger.setLevel(logging.NOTSET)
        logger.propagate = True


def test_verbose_logs_each_http_call_once_without_keys_or_prompt_text(monkeypatch, verbose_stream):
    request = example_request({"id": "t", "incoming": {"title": None, "text": "NVIDIA buys SecretCo for $1 billion."},
                               "candidate": {"title": None, "text": "SecretCo will be acquired by NVIDIA."}})

    def post(url, json=None, headers=None, timeout=None):
        payload = {"id": "r", "model": "m", "usage": {"prompt_tokens": 11, "completion_tokens": 7},
                   "choices": [{"finish_reason": "stop", "message": {"content": "{}"}}]}
        return httpx.Response(200, json=payload, request=httpx.Request("POST", url))
    monkeypatch.setattr(httpx, "post", post)
    monkeypatch.setenv("FAKE_KEY", "sk-very-secret")
    backend = OpenAICompatibleComparisonBackend(base_url="http://local/v1", model="m", api_key_env="FAKE_KEY")
    with pytest.raises(Exception):  # "{}" is not a valid comparison: the HTTP lines are what is tested
        backend.compare(request)
    out = verbose_stream.getvalue()
    assert out.count("m: request sent") == 1 and out.count("m: HTTP 200 in") == 1
    assert "tokens in/out 11/7" in out
    assert "sk-very-secret" not in out and "SecretCo" not in out and "Authorization" not in out


def test_without_verbose_nothing_is_printed(monkeypatch, capsys, settings):
    monkeypatch.setattr(consolidation, "run_batch", fake_run_batch())
    consolidation.LOGGER.debug("hidden unless verbose")
    assert "hidden" not in capsys.readouterr().out
