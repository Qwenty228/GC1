"""Evaluation harness: run an experiment file through a pipeline stage and score it.

    experiment.py     load a user's experiment file (the plug-in point for a model)
    consolidation.py  stage 1: event grouping + relation labels, batch by batch
    metrics.py        pairwise F1 (grouping) and macro F1 (labels)
    cli.py            `datasources-eval`

Experiment files and results live in `SystemCode/evaluation/` (see its README).
"""
