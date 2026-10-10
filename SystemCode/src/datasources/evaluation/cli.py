"""`datasources-eval compare`: leaderboard of saved evaluation runs.

Runs themselves are started from the experiment files:
    uv run python evaluation/consolidation/experiments/baseline_rule.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .. import config  # noqa: F401  (loads .env)

EVAL_ROOT = Path(__file__).resolve().parents[3] / "evaluation"


def results_dir(stage: str) -> Path:
    return EVAL_ROOT / stage / "results"


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
    p_cmp = sub.add_parser("compare", help="Leaderboard of saved runs")
    p_cmp.add_argument("stage", nargs="?", default="consolidation")
    args = parser.parse_args(argv)
    return cmd_compare(args)


if __name__ == "__main__":
    sys.exit(main())
