"""`datasources-eval`: run an experiment file through a pipeline stage, and compare runs.

    uv run datasources-eval consolidation evaluation/consolidation/experiments/baseline_rule.py --batch 1
    uv run datasources-eval consolidation evaluation/consolidation/experiments/claude.py --all --yes
    uv run datasources-eval compare consolidation          # leaderboard of every saved run

Each run writes evaluation/<stage>/results/<date>_<time>_<NAME>.json (commit it, so the
team can compare models). Batches run in throwaway `stock_news_eval_..._test` databases
on the configured PostgreSQL server; stock_news is never touched.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from .. import config  # noqa: F401  (loads .env)

EVAL_ROOT = Path(__file__).resolve().parents[3] / "evaluation"


def results_dir(stage: str) -> Path:
    return EVAL_ROOT / stage / "results"


def cmd_consolidation(args) -> int:
    from ..similarity.settings import load_similarity_settings
    from .consolidation import run, to_jsonable
    from .experiment import ExperimentError, load_experiment

    try:
        experiment = load_experiment(args.experiment)
    except ExperimentError as error:
        print(f"Experiment: {error}", file=sys.stderr)
        return 2
    if not args.all and not args.batch:
        print("Choose batches: --batch N (repeatable) or --all", file=sys.stderr)
        return 2
    if experiment.paid_calls and not args.yes:
        print(f"{experiment.path.name} sets PAID_CALLS = True: it calls a model that may cost money. "
              "Re-run with --yes. Start with one batch (--batch 1).", file=sys.stderr)
        return 2
    print(f"Experiment {experiment.name}: {experiment.description}")
    report = run(experiment, load_similarity_settings(), batch_numbers=None if args.all else args.batch,
                 keep_databases=args.keep_db)
    m = report["metrics"]
    print(f"\nGrouping (pairwise): F1 {m['grouping_pairwise']['f1']:.3f}  "
          f"P {m['grouping_pairwise']['precision']:.3f}  R {m['grouping_pairwise']['recall']:.3f}")
    print(f"Labels: macro F1 {m['labels']['macro_f1']:.3f}  accuracy {m['labels']['accuracy']:.3f}  "
          f"({m['labels']['scored']} scored, {m['labels']['uncertain']} uncertain)")
    for label, score in m["labels"]["per_label"].items():
        print(f"    {label:<22} F1 {score['f1']:.3f}  (tp {score['tp']}, fp {score['fp']}, fn {score['fn']})")
    print(f"Model calls: {report['usage']['comparison_calls']}, tokens in/out "
          f"{report['usage']['input_tokens']}/{report['usage']['output_tokens']}")
    if not args.no_save:
        out = results_dir("consolidation")
        out.mkdir(parents=True, exist_ok=True)
        path = out / f"{datetime.now().strftime('%Y-%m-%d_%H%M')}_{experiment.name}.json"
        path.write_text(json.dumps(to_jsonable(report), indent=2, default=str) + "\n", encoding="utf-8")
        print(f"Saved {path.relative_to(EVAL_ROOT.parent)}")
    return 0


def cmd_compare(args) -> int:
    rows = []
    for path in sorted(results_dir(args.stage).glob("*.json")):
        r = json.loads(path.read_text(encoding="utf-8"))
        m = r["metrics"]
        rows.append((path.stem, r["model"].get("comparison_model") or "-", len(r["batches_run"]),
                     m["grouping_pairwise"]["f1"], m["labels"]["macro_f1"], m["labels"]["accuracy"],
                     m["labels"]["uncertain"]))
    if not rows:
        print(f"No saved runs in {results_dir(args.stage)}")
        return 0
    print(f"{'run':<44} {'model':<26} {'batches':>7} {'group F1':>8} {'label F1':>8} {'acc':>6} {'unc':>4}")
    for row in sorted(rows, key=lambda r: (-r[4], -r[3])):
        print(f"{row[0]:<44} {str(row[1])[:26]:<26} {row[2]:>7} {row[3]:>8.3f} {row[4]:>8.3f} {row[5]:>6.3f} "
              f"{row[6]:>4}")
    print("\nCompare runs over the same batches: scores on different batches are not comparable.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="datasources-eval", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p_cons = sub.add_parser("consolidation", help="Event grouping + relation labels on the labelled batches")
    p_cons.add_argument("experiment", help="Path to an experiment .py file")
    p_cons.add_argument("--batch", type=int, action="append", help="Batch number (repeatable); 1 = tests 001-005")
    p_cons.add_argument("--all", action="store_true", help="Every batch")
    p_cons.add_argument("--yes", action="store_true", help="Allow an experiment with PAID_CALLS = True")
    p_cons.add_argument("--keep-db", action="store_true", help="Keep each batch's test database for inspection")
    p_cons.add_argument("--no-save", action="store_true", help="Print only; do not write a results file")
    p_cmp = sub.add_parser("compare", help="Leaderboard of saved runs")
    p_cmp.add_argument("stage", nargs="?", default="consolidation")
    args = parser.parse_args(argv)
    return {"consolidation": cmd_consolidation, "compare": cmd_compare}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
