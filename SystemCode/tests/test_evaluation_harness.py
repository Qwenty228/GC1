"""The evaluation harness: metrics, experiment files, the OpenAI-compatible backend, and a full batch run."""

import json
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from datasources.evaluation import metrics
from datasources.evaluation.experiment import ExperimentError, load_experiment
from datasources.llm_openai import OpenAICompatibleComparisonBackend
from datasources.similarity.comparison import ComparisonError, build_output
from datasources.similarity.evaluation import example_request

EXPERIMENTS = Path(__file__).resolve().parents[1] / "evaluation" / "consolidation" / "experiments"


def _p(n, group, expected, events, label, batch=1):
    return metrics.Prediction(n, batch, group, expected, frozenset(events), label)


# ----------------------------------------------------------------------------- metrics
def test_pairwise_counts_membership_within_batches_only():
    perfect = [_p(1, 1, None, {"a"}, "x"), _p(2, 1, "r", {"a"}, "x"), _p(3, 2, "d", {"b"}, "x"),
               _p(4, 9, None, {"a"}, "x", batch=2)]  # same event id in another batch: never a pair
    assert metrics.pairwise(perfect)["f1"] == 1.0
    merged = [_p(1, 1, None, {"a"}, "x"), _p(2, 1, "r", {"a"}, "x"), _p(3, 2, "d", {"a"}, "x")]
    score = metrics.pairwise(merged)
    assert (score["tp"], score["fp"], score["fn"]) == (1, 2, 0)


def test_macro_f1_weights_every_present_label_equally_and_uncertain_is_wrong():
    preds = [_p(1, 1, None, {"a"}, "different_event"),  # first of batch: not scored
             _p(2, 1, "same_event_repeated", {"a"}, "same_event_repeated"),
             _p(3, 1, "same_event_update", {"a"}, "uncertain"),
             _p(4, 2, "different_event", {"b"}, "different_event")]
    score = metrics.labels(preds)
    assert score["scored"] == 3 and score["uncertain"] == 1
    assert score["per_label"]["same_event_update"]["fn"] == 1
    assert score["macro_f1"] == round((1 + 0 + 1) / 3, 4)  # conflict absent from the answers: not averaged


# ----------------------------------------------------------------------------- experiment files
def test_the_template_and_bundled_experiments_load():
    for path in sorted(EXPERIMENTS.glob("*.py")):
        experiment = load_experiment(path)
        assert experiment.hook("build_comparator") and experiment.name
    assert load_experiment(EXPERIMENTS / "baseline_rule.py").paid_calls is False


def test_the_template_comparator_labels_every_pair_and_passes_validation():
    comparator = load_experiment(EXPERIMENTS / "baseline_rule.py").hook("build_comparator")(None)
    call = comparator.compare(example_request({"id": "t", "incoming": {"title": None, "text": "NVIDIA buys X."},
                                               "candidate": {"title": None, "text": "X is bought by NVIDIA."}}))
    labels = [j.label for s in call.output.segments for j in s.judgments]
    assert labels == ["same_event_repeated"]


@pytest.mark.parametrize("source, message", [
    ("NAME = 'x'\n", "must define build_comparator"),
    ("NAME = 'bad name!'\ndef build_comparator(s): return None\n", "NAME"),
    ("import nonexistent_module_xyz\n", "failed to import"),
])
def test_broken_experiment_files_are_refused_with_a_reason(tmp_path, source, message):
    path = tmp_path / "exp.py"
    path.write_text(source)
    with pytest.raises(ExperimentError, match=message):
        load_experiment(path)


# ----------------------------------------------------------------------------- OpenAI-compatible backend
def _fake_server(monkeypatch, content, *, status=200, finish="stop", seen=None):
    def post(url, json=None, headers=None, timeout=None):
        if seen is not None:
            seen.update(url=url, body=json, headers=headers)
        payload = {"id": "req-1", "model": "fake-model", "usage": {"prompt_tokens": 11, "completion_tokens": 7},
                   "choices": [{"finish_reason": finish, "message": {"content": content}}]}
        return httpx.Response(status, json=payload if status < 400 else {"error": "nope"},
                              request=httpx.Request("POST", url))
    monkeypatch.setattr(httpx, "post", post)


def _request():
    return example_request({"id": "t", "incoming": {"title": None, "text": "NVIDIA buys X for $1 billion."},
                            "candidate": {"title": None, "text": "X will be acquired by NVIDIA for $1 billion."}})


@pytest.mark.parametrize("mode", ["json_schema", "json_object", "prompt"])
def test_openai_compatible_backend_sends_the_production_prompt_and_parses_the_answer(monkeypatch, mode):
    request, seen = _request(), {}
    answer = build_output(request, lambda s, e: "same_event_update").model_dump_json()
    _fake_server(monkeypatch, f"```json\n{answer}\n```" if mode == "prompt" else answer, seen=seen)
    monkeypatch.setenv("FAKE_KEY", "sk-test")
    backend = OpenAICompatibleComparisonBackend(base_url="http://local/v1/", model="m", api_key_env="FAKE_KEY",
                                                json_mode=mode)
    call = backend.compare(request)
    assert call.output.segments[0].judgments[0].label == "same_event_update"
    assert call.usage == {"input_tokens": 11, "output_tokens": 7} and call.model == "fake-model"
    assert seen["url"] == "http://local/v1/chat/completions" and seen["headers"]["Authorization"] == "Bearer sk-test"
    assert seen["body"]["messages"][1]["content"] == request.render()
    assert ("response_format" in seen["body"]) == (mode != "prompt")


@pytest.mark.parametrize("kwargs, kind", [({"status": 429}, "http_error"), ({"finish": "length"}, "truncated"),
                                          ({"content": "not json"}, "invalid_response")])
def test_openai_compatible_backend_failures_are_comparison_errors(monkeypatch, kwargs, kind):
    _fake_server(monkeypatch, kwargs.pop("content", "{}"), **kwargs)
    backend = OpenAICompatibleComparisonBackend(base_url="http://local/v1", model="m")
    with pytest.raises(ComparisonError) as info:
        backend.compare(_request())
    assert info.value.kind == kind


def test_a_missing_api_key_is_reported_by_name(monkeypatch):
    monkeypatch.delenv("NO_SUCH_KEY", raising=False)
    with pytest.raises(ComparisonError, match="NO_SUCH_KEY"):
        OpenAICompatibleComparisonBackend(base_url="http://x/v1", model="m", api_key_env="NO_SUCH_KEY")


# ----------------------------------------------------------------------------- a full batch (temporary databases)
def test_a_batch_runs_through_the_real_pipeline_in_its_own_database(tmp_path):
    from conftest import isolated_test_database
    from datasources.evaluation.consolidation import run
    from datasources.similarity.settings import load_similarity_settings

    gate = isolated_test_database()  # skips without a test database server
    gate.close()
    source = Path(__file__).resolve().parents[1] / "populate" / "event_grouping"
    for f in sorted(source.glob("test_00[1-5]_*.json")):
        (tmp_path / f.name).write_text(f.read_text())
    experiment_file = tmp_path / "exp.py"
    experiment_file.write_text((EXPERIMENTS / "baseline_rule.py").read_text() + (
        "\n\ndef build_embedder(settings):\n"
        "    from dataclasses import replace\n"
        "    from datasources.similarity.embeddings import build_embedding_backend\n"
        "    return build_embedding_backend(replace(settings, embedding_backend='hashing'))\n"))
    report = run(load_experiment(experiment_file), replace(load_similarity_settings(), embedding_backend="hashing"),
                 directory=tmp_path, progress=lambda _m: None)
    rows = report["batches"][0]["articles"]
    assert [r["file"] for r in rows] == sorted(p.name for p in tmp_path.glob("test_*.json"))
    assert rows[0]["predicted_label"] == "different_event" and rows[0]["expected_label"] is None
    assert all(r["predicted_label"] in metrics.LABELS + ("uncertain",) for r in rows)
    assert 0.0 <= report["metrics"]["grouping_pairwise"]["f1"] <= 1.0
    json.dumps(report, default=str)  # serialisable for the results file
