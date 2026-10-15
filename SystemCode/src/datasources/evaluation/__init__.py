"""Evaluation harness: run the real pipeline with a swappable model and score it.

    consolidation.py  stage 1: event grouping + relation labels (`run_experiment`)
    summarization.py  stage 2: one summary per event, for human rating (its own `run_experiment`)
    metrics.py        pairwise F1 (grouping) and macro F1 (labels)
    cli.py            `datasources-eval compare` / `report`: saved runs of either stage

Experiment files live in `SystemCode/evaluation/` and are run directly:
`uv run python evaluation/consolidation/experiments/<file>.py`.
"""

from .consolidation import run_experiment

__all__ = ["run_experiment"]
