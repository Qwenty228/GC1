"""`datasources-eval`: inspect saved evaluation runs.

    compare   leaderboard of every saved run in a stage (consolidation, or summarization)
    report    one run in full (the same output you see live): per-batch grouping with the model's
              reasoning (--reasons), or per-event summaries with what the model was given (--passages)

Runs themselves are started from the experiment files:
    uv run python evaluation/consolidation/experiments/baseline_rule.py
    uv run python evaluation/summarization/experiments/local.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .. import config  # noqa: F401  (loads .env)
from . import metrics, summarization
from .consolidation import format_article_line, print_scores

EVAL_ROOT = Path(__file__).resolve().parents[3] / "evaluation"
STAGES = ("consolidation", "summarization")


def results_dir(stage: str) -> Path:
    return EVAL_ROOT / stage / "results"


def cmd_compare(args) -> int:
    if args.stage == "summarization":
        return summarization.print_leaderboard(results_dir(args.stage))
    rows, skipped = [], 0
    for path in sorted(results_dir(args.stage).glob("*.json")):
        r = json.loads(path.read_text(encoding="utf-8"))
        if r.get("incomplete"):  # errored/aborted runs stay on disk as logs but never rank
            skipped += 1
            continue
        m = r["metrics"]
        settings = r.get("settings", {})
        rows.append((path.stem, r["model"].get("comparison_model") or "-", len(r["batches_run"]),
                     m["grouping_pairwise"]["f1"], m["labels"]["macro_f1"], m["labels"]["accuracy"],
                     m["labels"]["uncertain"], settings.get("max_candidate_events", "?"),
                     settings.get("lexical_plausible_rank", "?")))
    if not rows:
        print(f"No complete runs in {results_dir(args.stage)}"
              + (f" ({skipped} incomplete log(s) hidden)" if skipped else ""))
        return 0
    print(f"{'run':<44} {'model':<26} {'batches':>7} {'group F1':>8} {'label F1':>8} {'acc':>6} {'unc':>4} "
          f"{'cands':>5} {'lex':>3}")
    for row in sorted(rows, key=lambda r: (-r[4], -r[3])):
        print(f"{row[0]:<44} {str(row[1])[:26]:<26} {row[2]:>7} {row[3]:>8.3f} {row[4]:>8.3f} {row[5]:>6.3f} "
              f"{row[6]:>4} {row[7]!s:>5} {row[8]!s:>3}")
    print("\nOnly compare runs over the same batches AND the same retrieval settings (cands = "
          "SIMILARITY_MAX_CANDIDATE_EVENTS, lex = SIMILARITY_LEXICAL_PLAUSIBLE_RANK; all are in each file's "
          "\"settings\").")
    if skipped:
        print(f"{skipped} incomplete run(s) hidden; `report <name>` still shows them as logs.")
    return 0


def _is_incomplete(path: Path) -> bool:
    try:
        return bool(json.loads(path.read_text(encoding="utf-8")).get("incomplete"))
    except (ValueError, OSError):
        return False


def resolve_run(stage: str | None, selector: str) -> Path | None:
    """Find a saved run by path, exact stem, or model-name substring (stage None = look in every stage).

    An exact path/stem is returned even if incomplete (you asked for that log by name); a model-name
    substring skips incomplete runs and returns the newest complete match (filenames sort chronologically).
    """
    direct = Path(selector)
    if direct.is_file():
        return direct
    dirs = [results_dir(s) for s in ((stage,) if stage else STAGES)]
    for d in dirs:
        exact = d / (selector if selector.endswith(".json") else f"{selector}.json")
        if exact.is_file():
            return exact
    for d in dirs:
        matches = [p for p in sorted(d.glob("*.json")) if selector in p.stem and not _is_incomplete(p)]
        if matches:
            return matches[-1]
    return None


def _batch_predictions(batch: dict[str, Any]) -> list[metrics.Prediction]:
    """Rebuild this batch's predictions from the saved rows, to re-score it on its own."""
    return [metrics.Prediction(test_number=0, batch=batch["batch"], expected_group=a["expected_group"],
                               expected_label=a["expected_label"],
                               predicted_events=frozenset(a["predicted_events"]),
                               predicted_label=a["predicted_label"])
            for a in batch["articles"]]


def _print_reasons(row: dict[str, Any]) -> None:
    for j in row.get("judgments", []):
        head = f"        -> {j['candidate_event']}: {j['label']} ({j['method']})"
        if j.get("explanation"):
            head += f"  {j['explanation']}"
        print(head)
        evidence = j.get("evidence") or {}
        for side in ("incoming", "candidate"):
            for e in evidence.get(side) or []:
                print(f"             {side}[{e['ref']}]: \"{e['quote']}\"")
        for d in j.get("differences") or []:
            print(f"             diff {d.get('aspect', '')}: {d.get('incoming', '')} vs "
                  f"{d.get('candidate', '')} [{d.get('kind', '')}]")
        if j.get("uncertainty"):
            print(f"             uncertain: {j['uncertainty']}")
        for note in j.get("validation_notes") or []:
            print(f"             note: {note}")


def cmd_report(args) -> int:
    path = resolve_run(args.stage, args.run)
    if path is None:
        allp = [p for s in ((args.stage,) if args.stage else STAGES) for p in sorted(results_dir(s).glob("*.json"))]
        hidden = [p.stem for p in allp if args.run in p.stem and _is_incomplete(p)]
        print(f"No complete run matching {args.run!r} in {', '.join(args.stage and [args.stage] or STAGES)}")
        if hidden:
            print("Incomplete log(s) matching (name one exactly to view):\n  " + "\n  ".join(hidden))
        elif allp:
            print("Available:\n  " + "\n  ".join(p.stem for p in allp))
        return 1
    r = json.loads(path.read_text(encoding="utf-8"))
    if r.get("stage") == "summarization":
        print(path.stem)
        return summarization.print_report(r, event=args.event, passages=args.passages, grouping=args.grouping)
    model = r["model"].get("comparison_model") or r["model"].get("comparator") or "-"
    print(f"{path.stem}")
    print(f"model: {model}   batches_run: {r['batches_run']}   {r['experiment'].get('description', '')}".rstrip())
    if r.get("incomplete"):
        print(f"[INCOMPLETE — excluded from the leaderboard] {r.get('incomplete_reason', '')}".rstrip())
    batches = r.get("batches", [])
    if args.batch is not None:
        batches = [b for b in batches if b["batch"] == args.batch]
        if not batches:
            print(f"\nbatch {args.batch} not in this run (ran {r['batches_run']})")
            return 1
    for batch in batches:
        print(f"\nbatch {batch['batch']:>2}: {', '.join(a['file'] for a in batch['articles'])}")
        for row in batch["articles"]:
            print(format_article_line(row))
            if args.reasons:
                _print_reasons(row)
        preds = _batch_predictions(batch)
        scores = {"grouping_pairwise": metrics.pairwise(preds), "labels": metrics.labels(preds)}
        usage = {"comparison_calls": batch["comparison_calls"], "input_tokens": batch["tokens"]["input"],
                 "output_tokens": batch["tokens"]["output"]}
        print_scores(scores, usage)
    if args.batch is None and len(batches) > 1:
        print(f"\n=== TOTAL over {len(r['batches_run'])} batch(es) ===")
        print_scores(r["metrics"], r["usage"])
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="datasources-eval", description=(__doc__ or "").split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p_cmp = sub.add_parser("compare", help="Leaderboard of saved runs")
    p_cmp.add_argument("stage", nargs="?", default="consolidation", choices=STAGES)

    p_rep = sub.add_parser("report", help="Full per-batch breakdown of one saved run")
    p_rep.add_argument("run", help="model name (newest match) or a result file / stem, e.g. local-qwen3-8b")
    p_rep.add_argument("--stage", choices=STAGES, help="where to look (default: every stage)")
    p_rep.add_argument("--reasons", action="store_true", help="consolidation: the model's reasoning per judgment")
    p_rep.add_argument("--batch", type=int, help="consolidation: only this batch number")
    p_rep.add_argument("--event", help="summarization: only this event, e.g. E3")
    p_rep.add_argument("--passages", action="store_true",
                       help="summarization: also print the full passages the model was given")
    p_rep.add_argument("--grouping", action="store_true",
                       help="summarization: also print how every article was grouped")

    args = parser.parse_args(argv)
    if args.command == "report":
        return cmd_report(args)
    return cmd_compare(args)


if __name__ == "__main__":
    sys.exit(main())
