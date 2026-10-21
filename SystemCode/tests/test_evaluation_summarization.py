"""Stage 2 evaluation: answer-key grouping and reviews, the summary run, its report and the human score sheet."""

import csv
import importlib.util
import json
from dataclasses import replace
from pathlib import Path

from datasources.db.evaluation import Fixture, FixtureType
from datasources.evaluation import summarization

EXPERIMENTS = Path(__file__).resolve().parents[1] / "evaluation" / "summarization" / "experiments"
SOURCE = Path(__file__).resolve().parents[1] / "populate" / "event_grouping"


def _fixture(kind: FixtureType, group: int) -> Fixture:
    return Fixture(f"test_001_{kind.value}_group_{group:03d}.json", 1, kind, group, "x")


def test_a_changed_number_is_paired_with_the_sentence_it_contradicts():
    earlier = ["Nvidia's commitments under the new model totaled $36 billion as of July 26. Shares rose."]
    new = "A competing account says Nvidia's commitments under the new model totaled $18 billion as of July 26."
    [diff] = summarization.changed_claims(new, earlier)
    assert diff["kind"] == "conflict" and "$18 billion" in diff["incoming"] and "$36 billion" in diff["candidate"]
    assert summarization.changed_claims("Shares fell sharply on the news.", earlier) == []  # no new number


def test_the_answer_key_reviewer_decides_like_a_person_with_the_key():
    assert summarization.review_decision(_fixture(FixtureType.CONFLICT, 1), 1)[0] == "confirmed_conflict"
    assert summarization.review_decision(_fixture(FixtureType.UPDATE, 1), 1)[0] == "not_a_conflict"
    assert summarization.review_decision(_fixture(FixtureType.CONFLICT, 2), 1)[0] == "different_events"


def test_every_summarization_experiment_file_has_the_same_runnable_shape():
    for path in sorted(EXPERIMENTS.glob("*.py")):
        spec = importlib.util.spec_from_file_location(f"summary_experiment_{path.stem}", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # importing never runs the experiment
        assert callable(module.build_summarizer) and module.NAME, path.name
        assert {"SAVE", "KEEP_DB", "DESCRIPTION", "ATTEMPTS", "LIMIT_EVENTS"} <= set(vars(module)), path.name
        assert "run_experiment(" in path.read_text(), path.name


def test_all_articles_go_into_one_database_and_every_event_is_summarized(tmp_path):
    from conftest import isolated_test_database
    from datasources.similarity.embeddings import build_embedding_backend
    from datasources.similarity.settings import load_similarity_settings
    from datasources.summary.generation import ScriptedSummaryBackend, extractive_output
    from datasources.summary.settings import load_summary_settings

    gate = isolated_test_database()  # skips without a test database server
    gate.close()
    for f in sorted(SOURCE.glob("test_0[01][0-9]_*.json"))[:10]:  # two batches: groups 1-4
        (tmp_path / f.name).write_text(f.read_text())
    settings = replace(load_similarity_settings(), enabled=True, embedding_backend="hashing")
    lines: list[str] = []
    report = summarization.run("unit", ScriptedSummaryBackend(extractive_output), settings, load_summary_settings(),
                               embedder=build_embedding_backend(settings), directory=tmp_path, attempts=1,
                               progress=lines.append)

    assert report["stage"] == "summarization" and report["articles"] == 10
    assert report["events_summarized"] == report["events_total"] >= 4
    assert [e["event"] for e in report["events"]] == [f"E{i}" for i in range(1, len(report["events"]) + 1)]
    first = report["events"][0]
    assert first["members"][0]["file"].startswith("test_001_original") and first["answer_key"]["pure"]
    assert all(e["result"] in ("published", "invalid") for e in report["events"])
    assert {r["decision"] for e in report["events"] for r in e["reviews"]} <= {"confirmed_conflict"}
    for e in report["events"]:  # every statement's quotes are checked against the passages it was given
        assert all(c["verified"] in (True, False) for s in e["summary"]["statements"] for c in s["citations"])
    json.dumps(report, default=str)  # serialisable for the results file

    (tmp_path / "results").mkdir()
    path = tmp_path / "results" / "2026-01-01_0000_unit.json"
    path.write_text(json.dumps(summarization.to_jsonable(report), default=str))
    sheet = path.with_name(f"{path.stem}_scores.csv")
    summarization.write_score_sheet(report, sheet)
    assert summarization.read_scores(path) is None  # nothing rated yet
    rows = list(csv.DictReader(sheet.open(encoding="utf-8-sig")))
    assert [r["event"] for r in rows] == [e["event"] for e in report["events"]]
    for r in rows:
        if r["auto_valid"] == "y":
            r.update(faithful="2", coverage="1", clarity="2", acceptable="y")
    with sheet.with_name(f"{path.stem}_scores_rater2.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    scores = summarization.read_scores(path)
    assert scores["rated"] == len(rows) and scores["faithful"] == 2.0

    out: list[str] = []
    assert summarization.print_report(report, event="E1", passages=True, progress=out.append) == 0
    assert any("passages given to the model" in line for line in out)
    assert summarization.print_report(report, event="E99", progress=out.append) == 1
    assert summarization.print_leaderboard(path.parent, progress=out.append) == 0
    assert any(line.startswith("2026-01-01_0000_unit") for line in out)
