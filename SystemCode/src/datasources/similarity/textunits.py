"""Deterministic text units for embedding and comparison (text preparation ``textunits-1``).

Rules
-----
* Articles use the pipeline's existing ``analysis_text``: headline, then the
  summary only when the body does not already contain it, then the body. A text
  that fits the model's input limit becomes ONE ``document`` unit; a longer one
  becomes ordered ``chunk`` units.
* SEC filings use the extracted primary-document ``body_text`` (never the
  generated metadata header). The text is split at ``Item x.xx`` headings. The
  cover page and the signature block are boilerplate and are not embedded; they
  are recorded in ``skipped_passages`` so nothing disappears silently.
  Known boilerplate paragraphs ("furnished and shall not be deemed filed",
  forward-looking-statement safe harbours, "does not purport to be complete")
  are skipped the same way.
* Every section is split into chunks that fit the embedding model's tokenizer
  limit: paragraphs first, then sentences, then words. Consecutive chunks of
  the same section share a modest overlap. Nothing is truncated: every kept
  character belongs to at least one unit.
* Each unit records its exact source spans (``location.spans``), so the passage
  is always traceable to ``analysis_text`` / ``body_text``.
* Units are grouped into *segments*: the parts of a revision that can describe
  one event (a news article; one substantive SEC Item). Overlapping chunks of a
  segment support the same event, never several copies of it.

Headline-only and summary-only articles are kept as one unit with a limited
evidence level. A revision with no usable text gets a ``skip_reason``.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass, field

from .models import AnalysisSubject, SegmentDraft, UnitDraft, UnitPlan, sha256_text
from .settings import TEXT_PREP_VERSION

TokenCounter = Callable[[str], int]  # tokens for text as a full model input (special tokens included)

MIN_WORDS = 3
COMPANION_ITEMS = frozenset({"7.01", "9.01"})  # usually accompany a primary disclosure
CROSS_REFERENCE_MAX_WORDS = 60

_ITEM_HEADING = re.compile(r"^## (?:ITEM|Item)\s+(\d{1,2}\.\d{2})\.?\s*(.*?)\s*$")
_SIGNATURE_HEADING = re.compile(r"^## (?:SIGNATURES?|Signatures?)\s*$")
_EXHIBIT_INDEX_HEADING = re.compile(r"^## (?:EXHIBIT INDEX|Exhibit Index)\s*$")
_CROSS_REFERENCE = re.compile(
    r"information (?:set forth|contained|included|described|reported|provided) (?:in|under)\s+Item\s+(\d{1,2}\.\d{2})",
    re.I,
)
_FORWARD_LOOKING_HEADING = re.compile(
    r"^(?:cautionary (?:note|statement)[^\n]{0,80}|forward[- ]looking statements?)\.?$", re.I)
_SEC_BOILERPLATE = (
    ("furnished_not_filed", re.compile(r"shall not be deemed\s+[\"“”']?filed", re.I)),
    ("not_incorporated_by_reference", re.compile(r"shall not be incorporated by reference in(?:to)? any", re.I)),
    ("forward_looking_safe_harbor",
     re.compile(r"forward-looking statements? within the meaning of|\bsafe harbor\b", re.I)),
    ("forward_looking_factors",
     re.compile(r"important factors that could cause (?:actual )?results to differ materially|disclaims any "
                r"obligation to update (?:these|any) forward-looking statements", re.I)),
    ("qualified_by_reference", re.compile(r"does not purport to be complete and is qualified in its entirety", re.I)),
)
# Exhibit rows that only describe the filing's own XBRL cover page / interactive data.
_EXHIBIT_ROW = re.compile(r"^\s*(\d{1,3}(?:\.\d{1,3})?)\s*\|", re.M)
_XBRL_ONLY_EXHIBITS = frozenset({"101", "104"})
_ARTICLE_BOILERPLATE = (
    ("promo_link", re.compile(r"^(?:read (?:also|next|more)|also read|see also|don'?t miss|trending)\s*:", re.I)),
    ("image_credit", re.compile(r"^(?:photo|image|picture)(?: courtesy| via| by| credit)?\s*:", re.I)),
)
_BLANK_LINES = re.compile(r"\n[ \t]*\n+")
# Breaks consume whitespace only, so closing quotes/brackets stay with their sentence.
_SENTENCE_BREAK = re.compile(r"(?:(?<=[.!?])|(?<=[.!?][\"'”’)\]]))\s+(?=[\"'“‘(\[]?[A-Z0-9$])|\n")
_WORD = re.compile(r"\S+")
_WS = re.compile(r"\s+")


def normalize_for_match(text: str) -> str:
    """Whitespace-insensitive, otherwise exact form used by the exact-content shortcut."""
    return _WS.sub(" ", unicodedata.normalize("NFC", text)).strip()


def word_count(text: str) -> int:
    return len(_WORD.findall(text))


# ----------------------------------------------------------------------------- spans
@dataclass
class _Section:
    kind: str  # article | item | exhibit_index | document | cover | signature
    start: int
    end: int
    heading: str | None = None
    item_code: str | None = None
    paragraphs: list[tuple[int, int]] = field(default_factory=list)  # kept paragraphs
    references: str | None = None  # item code this cross-reference-only item points to


def _paragraph_spans(text: str, start: int, end: int) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    pos = start
    for match in _BLANK_LINES.finditer(text, start, end):
        _add_trimmed(text, pos, match.start(), spans)
        pos = match.end()
    _add_trimmed(text, pos, end, spans)
    return spans


def _add_trimmed(text: str, start: int, end: int, spans: list[tuple[int, int]]) -> None:
    while start < end and text[start].isspace():
        start += 1
    while end > start and text[end - 1].isspace():
        end -= 1
    if end > start:
        spans.append((start, end))


def _split_spans(text: str, span: tuple[int, int], pattern: re.Pattern[str]) -> list[tuple[int, int]]:
    start, end = span
    pieces: list[tuple[int, int]] = []
    pos = start
    for match in pattern.finditer(text, start, end):
        _add_trimmed(text, pos, match.start(), pieces)
        pos = match.end()
    _add_trimmed(text, pos, end, pieces)
    return pieces or [span]


# ----------------------------------------------------------------------------- builder
class TextUnitBuilder:
    """Builds the same units for the same text and configuration, every time."""

    def __init__(self, count_tokens: TokenCounter, *, max_input_tokens: int, chunk_tokens: int,
                 overlap_tokens: int):
        self.count_tokens = count_tokens
        self.max_input_tokens = max_input_tokens
        self.chunk_tokens = min(chunk_tokens, max_input_tokens)
        self.overlap_tokens = overlap_tokens
        self._special = count_tokens("")
        self._cache: dict[str, int] = {}

    # -- token helpers -------------------------------------------------------
    def _piece_tokens(self, piece: str) -> int:
        cached = self._cache.get(piece)
        if cached is None:
            cached = max(self.count_tokens(piece) - self._special, 0)
            if len(self._cache) < 20000:
                self._cache[piece] = cached
        return cached

    def fits(self, text: str) -> bool:
        return self.count_tokens(text) <= self.max_input_tokens

    # -- public --------------------------------------------------------------
    def build(self, subject: AnalysisSubject) -> UnitPlan:
        if subject.unusable_reason:
            return UnitPlan([], [], None, skip_reason=subject.unusable_reason)
        if subject.evidence_level is None:
            return UnitPlan([], [], None, skip_reason="no_text")
        if subject.record_type == "sec_filing":
            return self._build_sec(subject)
        return self._build_article(subject)

    # -- articles ------------------------------------------------------------
    def _build_article(self, subject: AnalysisSubject) -> UnitPlan:
        text = subject.analysis_text or "\n\n".join(
            part for part in (subject.title, subject.summary_text, subject.body_text) if part)
        evidence = subject.evidence_level
        if not text.strip():
            return UnitPlan([], [], evidence, skip_reason="no_text")
        if evidence != "full" and word_count(text) < MIN_WORDS:
            return UnitPlan([], [], evidence, skip_reason="insufficient_text", source_field="analysis_text")

        skipped: list[dict] = []
        section = _Section("article", 0, len(text), heading=None)
        for span in _paragraph_spans(text, 0, len(text)):
            reason = self._boilerplate(text[span[0]:span[1]], _ARTICLE_BOILERPLATE)
            if reason and span[0] > 0:  # never drop the headline itself
                skipped.append({"section": "article", "reason": reason, "spans": [list(span)],
                                "chars": span[1] - span[0]})
            else:
                section.paragraphs.append(span)
        if not section.paragraphs or word_count(" ".join(text[s:e] for s, e in section.paragraphs)) < MIN_WORDS:
            return UnitPlan([], [], evidence, skip_reason="insufficient_text", skipped_passages=skipped,
                            source_field="analysis_text")

        title = (subject.title or "").strip()
        whole = self._render(text, section.paragraphs)
        units: list[UnitDraft] = []
        if self.count_tokens(whole) <= min(self.max_input_tokens, self.chunk_tokens * 4 // 3):
            units.append(self._unit(text, "document", 0, section.paragraphs, 0, "", "S1", {
                "section": "article", "section_kind": "article", "item_code": None, "section_index": 0,
                "chunk_in_section": 0, "evidence_level": evidence}))
        else:
            for index, (spans, overlap_spans) in enumerate(self._chunk(text, section.paragraphs,
                                                                       lambda i: title if i > 0 else "")):
                prefix = title if index > 0 and title and not text[spans[0][0]:].startswith(title) else ""
                units.append(self._unit(text, "chunk", index, spans, overlap_spans, prefix, "S1", {
                    "section": "article", "section_kind": "article", "item_code": None, "section_index": 0,
                    "chunk_in_section": index, "evidence_level": evidence}))
        segment = SegmentDraft("S1", "article", "article", [], [u.key for u in units])
        plan = UnitPlan(units, [segment], evidence, skipped_passages=skipped, source_field="analysis_text")
        self._stamp_segments(plan)
        return plan

    # -- SEC filings ---------------------------------------------------------
    def _build_sec(self, subject: AnalysisSubject) -> UnitPlan:
        text = subject.body_text or ""
        if not text.strip():
            return UnitPlan([], [], None, skip_reason="extraction_failed", source_field="body_text")
        sections, skipped = self._sec_sections(text)
        content = [s for s in sections if s.kind in ("item", "exhibit_index", "document") and s.paragraphs]
        if not content:
            return UnitPlan([], [], "full", skip_reason="boilerplate_only", skipped_passages=skipped,
                            source_field="body_text")

        issuer = (subject.issuer_name or (f"CIK {subject.cik}" if subject.cik else "SEC registrant")).strip()
        base_prefix = f"{issuer} Form {subject.form or '8-K'} filed {subject.filing_date or 'unknown date'}"
        units: list[UnitDraft] = []
        section_units: dict[int, list[str]] = {}
        ordinal = 0
        for index, section in enumerate(content):
            heading = (section.heading or "").removeprefix("## ").strip()
            keys: list[str] = []
            for chunk_index, (spans, overlap_spans) in enumerate(self._chunk(
                    text, section.paragraphs, lambda i, h=heading: f"{base_prefix} | {h}" if i > 0 and h else base_prefix)):
                prefix = f"{base_prefix} | {heading}" if chunk_index > 0 and heading else base_prefix
                unit = self._unit(text, "chunk", ordinal, spans, overlap_spans, prefix, None, {
                    "section": heading or "document", "section_kind": section.kind, "item_code": section.item_code,
                    "section_index": index, "chunk_in_section": chunk_index, "evidence_level": "full"})
                units.append(unit)
                keys.append(unit.key)
                ordinal += 1
            section_units[index] = keys

        segments = self._sec_segments(content, section_units)
        by_key = {u.key: u for u in units}
        for segment in segments:
            for key in segment.unit_keys:
                by_key[key].segment_key = segment.key
                by_key[key].location["segment_key"] = segment.key
        plan = UnitPlan(units, segments, "full", skipped_passages=skipped, source_field="body_text")
        self._stamp_segments(plan)
        return plan

    def _sec_sections(self, text: str) -> tuple[list[_Section], list[dict]]:
        headings: list[tuple[int, int, re.Match[str] | None, str]] = []
        pos = 0
        for line in text.split("\n"):
            line_start, pos = pos, pos + len(line) + 1
            item = _ITEM_HEADING.match(line)
            if item:
                headings.append((line_start, line_start + len(line), item, "item"))
            elif _SIGNATURE_HEADING.match(line):
                headings.append((line_start, line_start + len(line), None, "signature"))
            elif _EXHIBIT_INDEX_HEADING.match(line):
                headings.append((line_start, line_start + len(line), None, "exhibit_index"))

        sections: list[_Section] = []
        skipped: list[dict] = []
        has_items = any(kind == "item" for *_, kind in headings)
        first = headings[0][0] if headings else len(text)
        if first > 0:
            if has_items:
                self._skip(skipped, text, "cover_page", "boilerplate_cover_page", 0, first)
            else:
                preamble = _Section("document", 0, first)
                preamble.paragraphs = self._keep_paragraphs(text, preamble, skipped)
                sections.append(preamble)
        for index, (start, line_end, match, kind) in enumerate(headings):
            end = headings[index + 1][0] if index + 1 < len(headings) else len(text)
            heading = text[start:line_end].strip()
            if kind == "signature":
                self._skip(skipped, text, heading, "signature_block", start, end)
                continue
            section = _Section(kind, start, end, heading=heading,
                               item_code=match.group(1) if match else None)
            section.paragraphs = self._keep_paragraphs(text, section, skipped)
            body_words = sum(word_count(text[s:e]) for s, e in section.paragraphs[1:])
            if body_words == 0:
                self._skip(skipped, text, heading, "empty_section", start, end)
                section.paragraphs = []
            elif self._xbrl_only_exhibits(text, section):
                self._skip(skipped, text, heading, "exhibit_list_xbrl_cover_only", start, end)
                section.paragraphs = []
            elif kind == "item" and body_words <= CROSS_REFERENCE_MAX_WORDS:
                reference = _CROSS_REFERENCE.search(" ".join(text[s:e] for s, e in section.paragraphs[1:]))
                if reference and reference.group(1) != section.item_code:
                    section.references = reference.group(1)
            sections.append(section)
        return sections, skipped

    def _keep_paragraphs(self, text: str, section: _Section, skipped: list[dict]) -> list[tuple[int, int]]:
        """Drop known boilerplate paragraphs, and everything after a forward-looking-statements
        heading (the safe-harbour block closes an Item and may span page breaks)."""
        kept: list[tuple[int, int]] = []
        in_safe_harbor = False
        name = (section.heading or section.kind).removeprefix("## ")
        for span in _paragraph_spans(text, section.start, section.end):
            paragraph = text[span[0]:span[1]]
            if _FORWARD_LOOKING_HEADING.match(paragraph.strip()):
                in_safe_harbor = True
            reason = "forward_looking_block" if in_safe_harbor else self._boilerplate(paragraph, _SEC_BOILERPLATE)
            if reason:
                skipped.append({"section": name, "reason": reason, "spans": [list(span)],
                                "chars": span[1] - span[0]})
            else:
                kept.append(span)
        return kept

    @staticmethod
    def _xbrl_only_exhibits(text: str, section: _Section) -> bool:
        """An exhibit list whose only exhibits are the XBRL cover page carries no event information."""
        if not (section.kind == "exhibit_index" or section.item_code == "9.01"):
            return False
        body = "\n".join(text[s:e] for s, e in section.paragraphs[1:])
        numbers = {m.group(1) for m in _EXHIBIT_ROW.finditer(body)}
        return bool(numbers) and numbers <= _XBRL_ONLY_EXHIBITS

    @staticmethod
    def _skip(skipped: list[dict], text: str, section: str, reason: str, start: int, end: int) -> None:
        while end > start and text[end - 1].isspace():
            end -= 1
        if end > start:
            skipped.append({"section": section.removeprefix("## "), "reason": reason,
                            "spans": [[start, end]], "chars": end - start})

    @staticmethod
    def _sec_segments(content: list[_Section], section_units: dict[int, list[str]]) -> list[SegmentDraft]:
        """One segment per substantive Item; companion Items attach to a single primary Item.

        Items 7.01 (Regulation FD) and 9.01 (exhibits) usually accompany another
        disclosure. With exactly one primary Item they support its event. With
        several primary Items, 7.01 stands alone and 9.01 / the exhibit index stay
        context units that support no event by themselves. An Item that only says
        "the information in Item X is incorporated here" joins Item X.
        """
        present = {s.item_code for s in content if s.item_code}
        primary = [i for i, s in enumerate(content)
                   if s.kind == "item" and s.item_code not in COMPANION_ITEMS
                   and not (s.references and s.references in present)]
        if not primary:
            keys = [k for i in range(len(content)) for k in section_units[i]]
            items = [s.item_code for s in content if s.item_code]
            label = "; ".join((s.heading or "").removeprefix("## ") for s in content if s.heading) or "document"
            kind = "sec_item" if items else "document"
            return [SegmentDraft("S1", label, kind, items, keys)] if keys else []

        segments: dict[int, SegmentDraft] = {}
        order: list[int] = []
        for number, index in enumerate(primary, start=1):
            section = content[index]
            segments[index] = SegmentDraft(f"S{number}", (section.heading or "").removeprefix("## "), "sec_item",
                                           [section.item_code or ""], list(section_units[index]))
            order.append(index)
        by_code = {content[i].item_code: i for i in primary}
        next_number = len(primary) + 1
        for index, section in enumerate(content):
            if index in segments:
                continue
            if section.references and section.references in by_code:
                target = segments[by_code[section.references]]
            elif len(primary) == 1:
                target = segments[primary[0]]
            elif section.item_code == "7.01":
                target = SegmentDraft(f"S{next_number}", (section.heading or "").removeprefix("## "), "sec_item",
                                      ["7.01"], [])
                segments[index] = target
                order.append(index)
                next_number += 1
            else:
                continue  # 9.01 / exhibit index with several primary Items: context units only
            if section.item_code and section.item_code not in target.item_codes:
                target.item_codes.append(section.item_code)
            target.unit_keys.extend(section_units[index])
        return [segments[i] for i in order]

    # -- chunking ------------------------------------------------------------
    def _pieces(self, text: str, paragraphs: list[tuple[int, int]], budget: int) -> list[tuple[int, int]]:
        """Sentence-sized pieces (table rows count as sentences), so chunks break and
        overlap at sentence boundaries; words only as a last resort."""
        pieces: list[tuple[int, int]] = []
        for paragraph in paragraphs:
            for sentence in _split_spans(text, paragraph, _SENTENCE_BREAK):
                if self._piece_tokens(text[sentence[0]:sentence[1]]) <= budget:
                    pieces.append(sentence)
                else:
                    pieces.extend(self._word_windows(text, sentence, budget))
        return pieces

    def _word_windows(self, text: str, span: tuple[int, int], budget: int) -> list[tuple[int, int]]:
        """Last resort for a run of text with no usable sentence breaks (e.g. a huge table row)."""
        words = [m.span() for m in _WORD.finditer(text, span[0], span[1])]
        costs = [max(self._piece_tokens(text[s:e]), 1) for s, e in words]
        windows: list[tuple[int, int]] = []
        start = 0
        while start < len(words):
            end, total = start, 0
            while end < len(words) and (end == start or total + costs[end] <= budget):
                total += costs[end]
                end += 1
            # Summed word costs approximate the real count; confirm and shrink if needed.
            while end - start > 1 and self._piece_tokens(text[words[start][0]:words[end - 1][1]]) > budget:
                end -= 1
            windows.append((words[start][0], words[end - 1][1]))
            start = end
        return windows

    def _chunk(self, text: str, paragraphs: list[tuple[int, int]],
               prefix_for: Callable[[int], str]) -> list[tuple[list[tuple[int, int]], list[tuple[int, int]]]]:
        """Pack pieces into chunks of at most ``chunk_tokens``; returns (spans, overlap spans)."""
        prefix_room = max((self._piece_tokens(prefix_for(i)) for i in (0, 1)), default=0) + 2
        budget = max(8, min(self.chunk_tokens, self.max_input_tokens - self._special - prefix_room))
        pieces = self._pieces(text, paragraphs, budget)
        costs = [self._piece_tokens(text[s:e]) for s, e in pieces]
        chunks: list[tuple[list[tuple[int, int]], list[tuple[int, int]]]] = []
        index = 0
        previous_first = 0
        while index < len(pieces):
            overlap_first = index
            if chunks and self.overlap_tokens > 0:
                used = 0
                candidate = index - 1
                while candidate > previous_first and used + costs[candidate] <= self.overlap_tokens:
                    used += costs[candidate]
                    candidate -= 1
                overlap_first = candidate + 1
            total = sum(costs[overlap_first:index])
            end = index
            while end < len(pieces) and (end == index or total + costs[end] <= budget):
                total += costs[end]
                end += 1
            # Verify against the real tokenizer (joins can add tokens); shrink if needed.
            while True:
                spans = pieces[overlap_first:end]
                rendered = self._render(text, spans)
                prefix = prefix_for(len(chunks))
                if self.fits(f"{prefix}\n\n{rendered}" if prefix else rendered) or end - index <= 1:
                    break
                if overlap_first < index:
                    overlap_first += 1
                else:
                    end -= 1
            chunks.append((pieces[overlap_first:end], pieces[overlap_first:index]))
            previous_first = index
            index = end
        return chunks

    # -- units ---------------------------------------------------------------
    @staticmethod
    def _render(text: str, spans: list[tuple[int, int]]) -> str:
        """Join spans with their original separators, or a paragraph break across a skipped gap."""
        parts: list[str] = []
        previous: int | None = None
        for start, end in spans:
            if previous is not None:
                gap = text[previous:start]
                parts.append(gap if not gap.strip() else "\n\n")
            parts.append(text[start:end])
            previous = end
        return "".join(parts)

    @staticmethod
    def _merge(text: str, spans: list[tuple[int, int]]) -> list[list[int]]:
        merged: list[list[int]] = []
        for start, end in spans:
            if merged and not text[merged[-1][1]:start].strip():
                merged[-1][1] = end
            else:
                merged.append([start, end])
        return merged

    def _unit(self, text: str, kind: str, ordinal: int, spans: list[tuple[int, int]],
              overlap_spans: list[tuple[int, int]] | int, prefix: str, segment_key: str | None,
              extra: dict) -> UnitDraft:
        passage = self._render(text, spans)
        overlap_chars = 0
        if isinstance(overlap_spans, list) and overlap_spans:
            overlap_chars = len(self._render(text, spans[:len(overlap_spans) + 1])) - (spans[len(overlap_spans)][1]
                                                                                    - spans[len(overlap_spans)][0])
        input_text = f"{prefix}\n\n{passage}" if prefix else passage
        tokens = self.count_tokens(input_text)
        location = {
            "spans": self._merge(text, spans),
            "char_start": spans[0][0],
            "char_end": spans[-1][1],
            "overlap_prev_chars": overlap_chars,
            "context_prefix": prefix or None,
            "segment_key": segment_key,
            "token_count": tokens,
            "text_prep_version": TEXT_PREP_VERSION,
            **extra,
        }
        return UnitDraft(key=f"unit:{ordinal}", kind=kind, ordinal=ordinal, text=input_text, passage=passage,
                         segment_key=segment_key, location=location, token_count=tokens)

    @staticmethod
    def _stamp_segments(plan: UnitPlan) -> None:
        """Record each segment's normalized hash and length on its units (exact-content shortcut)."""
        for segment in plan.segments:
            normalized = normalize_for_match(plan.segment_text(segment))
            digest = sha256_text(normalized)
            for key in segment.unit_keys:
                unit = plan.unit(key)
                unit.location["segment_hash"] = digest
                unit.location["segment_chars"] = len(normalized)
        for unit in plan.units:
            unit.location["source_field"] = plan.source_field
            unit.location.setdefault("segment_hash", None)
            unit.location.setdefault("segment_chars", None)

    @staticmethod
    def _boilerplate(paragraph: str, rules) -> str | None:
        for reason, pattern in rules:
            if pattern.search(paragraph):
                return reason
        return None
