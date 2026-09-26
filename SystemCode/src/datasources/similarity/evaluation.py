"""Bounded evaluation of the REAL comparison backend on labelled examples.

The offline tests use scripted model responses: they prove the orchestration
(routing, validation, resolution, persistence), not that a model labels news
correctly. This module runs the configured backends on a small labelled set
and reports agreement separately. It never touches the database. With the
Anthropic backend it makes one paid API call per example, so the CLI requires
``--confirm-paid-calls``.

The bundled examples are synthetic (fictional companies) and few; their
accuracy is a smoke signal, not a benchmark.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from .comparison import (
    ComparisonError,
    ComparisonRequest,
    PromptEvent,
    PromptPassage,
    PromptSegment,
    PromptUnit,
    build_comparison_backend,
    validate_output,
)
from .embeddings import build_embedding_backend, cosine_similarity
from .models import LABELS
from .settings import SimilaritySettings


def load_examples(path: Path) -> list[dict[str, Any]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    examples = data["examples"] if isinstance(data, dict) else data
    for example in examples:
        if example.get("expected_label") not in LABELS:
            raise ValueError(f"example {example.get('id')!r} has an unknown expected_label")
    return examples


def example_request(example: dict[str, Any]) -> ComparisonRequest:
    incoming, candidate = example["incoming"], example["candidate"]
    incoming_text = "\n\n".join(p for p in (incoming.get("title"), incoming.get("text")) if p)
    candidate_text = "\n\n".join(p for p in (candidate.get("title"), candidate.get("text")) if p)
    segment = PromptSegment("S1", "S1", "article", [PromptUnit("U1", "unit:0", incoming_text)])
    event = PromptEvent("E1", f"example:{example['id']}", candidate.get("title") or "candidate event",
                        "provisional", {}, 1, None, "independent source",
                        [PromptPassage("C1", "example-unit", "example-revision", candidate_text,
                                       f"news article, {candidate.get('source', 'example source')}, "
                                       f"{candidate.get('published', 'unknown time')}")])
    evidence = incoming.get("evidence", "full text")
    return ComparisonRequest({"record_type": "news_article", "source": incoming.get("source", "example source"),
                              "published": incoming.get("published", "unknown"), "evidence": evidence,
                              "title": incoming.get("title")}, [segment], [event], {"S1": ["E1"]})


def run_evaluation(settings: SimilaritySettings, examples: list[dict[str, Any]], *, comparator=None,
                   embedder=None, use_embeddings: bool = True) -> dict[str, Any]:
    comparator = comparator or build_comparison_backend(settings)
    if use_embeddings and embedder is None:
        embedder = build_embedding_backend(settings)
    results: list[dict[str, Any]] = []
    confusion: Counter[tuple[str, str]] = Counter()
    usage: Counter[str] = Counter()
    for example in examples:
        request = example_request(example)
        row: dict[str, Any] = {"id": example["id"], "category": example.get("category"),
                               "expected": example["expected_label"]}
        if embedder is not None:
            vectors = embedder.embed([request.segments[0].units[0].text, request.events[0].passages[0].text])
            row["cosine_similarity"] = round(cosine_similarity(vectors[0], vectors[1]), 4)
        try:
            call = comparator.compare(request)
        except ComparisonError as error:
            row.update(predicted="error", error=f"{error.kind}: {error}")
            confusion[(example["expected_label"], "error")] += 1
            results.append(row)
            continue
        for key, value in call.usage.items():
            if isinstance(value, int):
                usage[key] += value
        validated = validate_output(request, call.output, model=call.model, usage=call.usage)
        judgment = validated.judgments[0] if validated.judgments else None
        predicted = judgment.label if judgment else "not_judged"
        row.update(predicted=predicted, correct=predicted == example["expected_label"], model=call.model,
                   explanation=judgment.explanation if judgment else None,
                   validation_notes=judgment.validation_notes if judgment else validated.notes)
        confusion[(example["expected_label"], predicted)] += 1
        results.append(row)
    total = len(results)
    correct = sum(1 for r in results if r.get("correct"))
    per_label: dict[str, dict[str, int]] = {}
    for r in results:
        bucket = per_label.setdefault(r["expected"], {"examples": 0, "correct": 0})
        bucket["examples"] += 1
        bucket["correct"] += 1 if r.get("correct") else 0
    return {
        "backend": f"{getattr(comparator, 'name', '?')}:{getattr(comparator, 'model', '?')}",
        "examples": total,
        "accuracy": round(correct / total, 3) if total else None,
        "per_expected_label": per_label,
        "confusion": [{"expected": e, "predicted": p, "count": n} for (e, p), n in sorted(confusion.items())],
        "usage": dict(usage),
        "results": results,
        "caveat": "synthetic examples; a smoke signal for the real backend, not a benchmark",
    }
