"""Validation of a model's summary before anything is published.

These checks establish TRACEABILITY: every statement points at passages the
model was actually given, from this event's active members in its own
sample/live partition, with quotes that really occur in those passages. They
do not prove that a statement is entailed by its quotes; that remains a matter
for evaluation and review.

Checks (any error rejects the output; it is kept as an ``invalid`` attempt)
    schema       the JSON matches ``SummaryOutput`` exactly (no extra fields)
    ids          headline "H1"; overview "O1".."O3" (1 to 3, in order); key points
                 "K1".."K3" (0 to 3, in order); text non-empty and bounded
    citations    every statement cites at least one passage; every ref was
                 provided; every quote has at least 8 characters and occurs in the
                 cited passage after normalization (below)
    figures      every number in a statement occurs in one of its cited passages
                 (so amounts, dates and periods cannot be invented or altered)
    advice       no investment-recommendation phrasing
    conflicts    for a reviewer-confirmed conflict whose two sides were both in the
                 input, the summary cites at least one passage from each side

Quote normalization (same as the comparison validator): HTML entities
unescaped; curly quotes, en/em dashes and non-breaking spaces folded to ASCII;
runs of whitespace collapsed to one space; case-folded. Nothing else: no
stemming, no punctuation removal, no fuzzy matching.

Kept citations: every distinct verified quote of a statement, each stored
separately and never joined into one quotation. Quotes of the same unit that
are identical after the normalization above are kept once (the first as
written). ``quote_index`` numbers a statement's distinct quotes from each unit
from 1 in the order the model gave them, so the same output always gets the
same numbering.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError

from ..similarity.comparison import _fold as fold_quote
from .generation import MAX_QUOTE_CHARS, SummaryOutput
from .models import Citation, EvidencePack, Statement, SummaryDraft

MIN_QUOTE_CHARS = 8
MAX_HEADLINE_CHARS = 160
MAX_STATEMENT_CHARS = 600
MAX_GAPS = 5
_NUMBER = re.compile(r"\d+(?:[.,]\d+)*")
_ADVICE = re.compile(
    r"\b(?:investors|you|readers|shareholders) should (?:buy|sell|hold|consider|avoid)\b"
    r"|\bwe (?:recommend|advise|suggest)\b"
    r"|\b(?:a|an) (?:good|great|bad|attractive) (?:time|opportunity) to (?:buy|sell|invest)\b"
    r"|\b(?:strong|good) (?:buy|investment)\b",
    re.I,
)


@dataclass
class ValidationResult:
    draft: SummaryDraft | None
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.draft is not None and not self.errors


def _numbers(text: str) -> set[str]:
    return {m.group(0).replace(",", "") for m in _NUMBER.finditer(text)}


def validate_summary(pack: EvidencePack, output: dict[str, Any] | None) -> ValidationResult:
    if not isinstance(output, dict):
        return ValidationResult(None, ["schema: output is not a JSON object"])
    try:
        parsed = SummaryOutput.model_validate(output)
    except ValidationError as error:
        messages = [f"schema: {'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in error.errors()[:8]]
        return ValidationResult(None, messages)

    errors: list[str] = []
    passages = {p.ref: p for p in pack.passages}
    groups = [("headline", [parsed.headline], "H", 1, 1), ("overview", parsed.overview, "O", 1, 3),
              ("key_point", parsed.key_points, "K", 0, 3)]
    statements: list[Statement] = []
    for kind, items, prefix, least, most in groups:
        if not least <= len(items) <= most:
            errors.append(f"ids: {kind} needs {least} to {most} statement(s), got {len(items)}")
        for position, raw in enumerate(items, start=1):
            expected = f"{prefix}{position}"
            if raw.id != expected:
                errors.append(f"ids: {kind} statement {position} must have id {expected!r}, got {raw.id!r}")
            text = raw.text.strip()
            limit = MAX_HEADLINE_CHARS if kind == "headline" else MAX_STATEMENT_CHARS
            if not text:
                errors.append(f"{expected}: empty text")
            elif len(text) > limit:
                errors.append(f"{expected}: text longer than {limit} characters")
            if _ADVICE.search(text):
                errors.append(f"{expected}: reads as investment advice")
            citations, verified_refs = _check_citations(expected, raw.citations, passages, errors)
            cited_text = " ".join(passages[ref].text for ref in verified_refs)
            missing = sorted(n for n in _numbers(text) if n not in _numbers(cited_text))
            if citations and missing:
                errors.append(f"{expected}: figure(s) {', '.join(missing)} not found in the cited passage(s)")
            statements.append(Statement(expected, kind, text, citations))

    cited_units = {c.unit_id for s in statements for c in s.citations}
    provided = {p.unit_id for p in pack.passages}
    for review in pack.reviews:
        if review.resolution != "confirmed_conflict":
            continue
        this_side = set(review.incoming_unit_ids) & provided
        other_side = set(review.candidate_unit_ids) & provided
        if this_side and other_side and not (cited_units & this_side and cited_units & other_side):
            errors.append(f"conflicts: the confirmed conflict (review {review.review_id[:8]}) must cite passages "
                          "from both sides")

    gaps = [g.strip()[:200] for g in parsed.evidence_gaps if g.strip()][:MAX_GAPS]
    if errors:
        return ValidationResult(None, errors)
    return ValidationResult(SummaryDraft(statements, gaps), [])


def _check_citations(statement_id: str, raw_citations, passages,
                     errors: list[str]) -> tuple[list[Citation], list[str]]:
    """Verified citations (every distinct quote, numbered per unit) and the refs that verified."""
    kept: list[Citation] = []
    verified: list[str] = []
    seen: set[tuple[str, str]] = set()  # (unit_id, normalized quote)
    if not raw_citations:
        errors.append(f"{statement_id}: no citation")
    for citation in raw_citations:
        passage = passages.get(citation.passage_ref)
        quote = html.unescape(citation.quote.strip())
        if passage is None:
            errors.append(f"{statement_id}: cites {citation.passage_ref!r}, which was not provided")
            continue
        if len(quote) > MAX_QUOTE_CHARS:
            errors.append(f"{statement_id}: quote from {citation.passage_ref} is longer than {MAX_QUOTE_CHARS} "
                          "characters")
            continue
        folded = fold_quote(quote)
        if len(folded) < MIN_QUOTE_CHARS:
            errors.append(f"{statement_id}: quote from {citation.passage_ref} is too short to verify")
            continue
        if folded not in fold_quote(passage.text):
            errors.append(f"{statement_id}: quote not found in {citation.passage_ref}")
            continue
        verified.append(citation.passage_ref)
        if (passage.unit_id, folded) not in seen:
            seen.add((passage.unit_id, folded))
            index = sum(c.unit_id == passage.unit_id for c in kept) + 1
            kept.append(Citation(passage.unit_id, quote, index))
    return kept, verified
