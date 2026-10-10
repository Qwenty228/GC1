"""Scores for the consolidation stage, as the team agreed: grouping and labels separately.

Grouping, pairwise F1: every pair of articles inside one batch is "same event" or not,
predicted vs expected. Only membership counts, never event ids. Pooled over batches.

Labels, macro F1: over the four relation labels, for every article except the first of
its batch (nothing to compare it with). Each label's F1 counts equally, so a weak class
is not hidden by an easy one. `uncertain` (or anything else) is never correct.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Any, Iterable

LABELS = ("same_event_repeated", "same_event_update", "same_event_conflict", "different_event")


def _f1(tp: int, fp: int, fn: int) -> dict[str, float]:
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4),
            "tp": tp, "fp": fp, "fn": fn}


@dataclass(frozen=True)
class Prediction:
    test_number: int
    batch: int
    expected_group: int
    expected_label: str | None  # None for the first article of a batch
    predicted_events: frozenset[str]  # active event memberships
    predicted_label: str


def pairwise(predictions: Iterable[Prediction]) -> dict[str, float]:
    tp = fp = fn = 0
    by_batch: dict[int, list[Prediction]] = {}
    for p in predictions:
        by_batch.setdefault(p.batch, []).append(p)
    for items in by_batch.values():
        for a, b in combinations(items, 2):
            expected = a.expected_group == b.expected_group
            predicted = bool(a.predicted_events & b.predicted_events)
            tp += expected and predicted
            fp += predicted and not expected
            fn += expected and not predicted
    return _f1(tp, fp, fn)


def labels(predictions: Iterable[Prediction]) -> dict[str, Any]:
    scored = [p for p in predictions if p.expected_label is not None]
    per_label = {}
    for label in LABELS:
        tp = sum(p.expected_label == label and p.predicted_label == label for p in scored)
        fp = sum(p.expected_label != label and p.predicted_label == label for p in scored)
        fn = sum(p.expected_label == label and p.predicted_label != label for p in scored)
        per_label[label] = _f1(tp, fp, fn)
    present = [label for label in LABELS if any(p.expected_label == label for p in scored)]
    macro = sum(per_label[label]["f1"] for label in present) / len(present) if present else 0.0
    accuracy = sum(p.expected_label == p.predicted_label for p in scored) / len(scored) if scored else 0.0
    return {"macro_f1": round(macro, 4), "accuracy": round(accuracy, 4), "scored": len(scored),
            "uncertain": sum(p.predicted_label == "uncertain" for p in scored), "per_label": per_label}
