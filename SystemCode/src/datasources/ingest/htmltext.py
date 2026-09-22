"""Deterministic HTML -> readable text for SEC filings and Alpaca article content.

Design rules (no NLP, nothing lossy for analysis):
    * parse with a real HTML parser (BeautifulSoup + lxml);
    * drop scripts, styles, comments, processing instructions, the inline-XBRL
      ``ix:header`` and elements hidden with ``display:none`` / ``hidden``;
    * keep visible inline-XBRL facts as the text a reader sees (signs,
      parentheses, currency symbols and percentages are untouched);
    * keep block structure: one line per block, blank lines around paragraphs,
      headings, lists and tables; tables become ``cell | cell`` rows;
    * Unicode NFC (not NFKC, which would turn footnote superscripts into digits),
      whitespace normalised; case, punctuation, numbers and negation preserved;
    * never truncate.
"""

from __future__ import annotations

import html as html_lib
import re
import unicodedata
import warnings
from dataclasses import dataclass, field
from urllib.parse import urldefrag, urljoin, urlparse

from bs4 import BeautifulSoup, Comment, Declaration, Doctype, NavigableString, ProcessingInstruction, Tag
from bs4 import XMLParsedAsHTMLWarning
from bs4.element import CData

PARSER = "lxml"


def _soup(markup: str) -> BeautifulSoup:
    # Inline-XBRL filings are XHTML that browsers render as HTML, so the HTML
    # parser is the right tool and the XML-prolog warning is expected.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", XMLParsedAsHTMLWarning)
        return BeautifulSoup(markup, PARSER)

_DROP_TAGS = {
    "script", "style", "noscript", "template", "head", "iframe", "object",
    "embed", "applet", "canvas", "map", "button", "input", "select", "textarea",
}
_BLOCK_TAGS = {
    "address", "article", "aside", "blockquote", "center", "dd", "div", "dl",
    "dt", "figcaption", "figure", "footer", "form", "header", "main", "nav",
    "section", "tbody", "thead", "tfoot", "caption", "body", "html",
}
_PARAGRAPH_TAGS = {"p", "pre", "hr"}
_HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
_LIST_TAGS = {"ul", "ol"}
_IX_FACT_TAGS = {"ix:nonfraction", "ix:nonnumeric"}
_HIDDEN_STYLE = re.compile(r"(display\s*:\s*none|visibility\s*:\s*hidden)", re.I)
_SEC_ITEM_HEADING = re.compile(r"^(ITEM|Item)\s+\d{1,2}\.\d{2}\b")
_SEC_SECTION_WORDS = re.compile(r"^(SIGNATURES?|EXHIBIT INDEX|Exhibit Index)$")
_CURRENCY_ONLY = re.compile(r"^(\$|US\$|€|£|¥|C\$|A\$)$")
_TRAILING_ONLY = re.compile(r"^(\)|%|\)%|%\)|\)\s*%)$")
_SPACES = re.compile(r"[ \t\f\v]+")
_ZERO_WIDTH = dict.fromkeys((0x200B, 0x200C, 0x200D, 0x2060, 0xFEFF, 0x00AD), None)
_SPACE_LIKE = dict.fromkeys(
    (0x00A0, *range(0x2002, 0x200B), 0x202F, 0x205F, 0x3000), " "
)

SEC_HOSTS = frozenset({"www.sec.gov", "sec.gov"})

_ERROR_PAGE_MARKERS = (
    "request rate threshold exceeded",
    "undeclared automated tool",
    "your request originates from an undeclared",
    "access denied",
    "the page you requested could not be found",
)


@dataclass
class ExtractedText:
    text: str
    table_count: int = 0
    inline_xbrl_fact_count: int = 0
    section_headings: list[str] = field(default_factory=list)
    links: list[tuple[str, str]] = field(default_factory=list)  # (absolute url, link text)
    warnings: list[str] = field(default_factory=list)
    title: str | None = None


def normalize_unicode(text: str) -> str:
    """NFC-normalise, map exotic spaces to ASCII space, remove zero-width chars."""
    return unicodedata.normalize("NFC", text).translate(_ZERO_WIDTH).translate(_SPACE_LIKE)


def normalize_whitespace(text: str) -> str:
    """Trim every line, collapse runs of spaces and of blank lines."""
    lines = [_SPACES.sub(" ", line).strip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    out: list[str] = []
    for line in lines:
        if not line and (not out or not out[-1]):
            continue
        out.append(line)
    while out and not out[-1]:
        out.pop()
    return "\n".join(out)


def clean_inline(value: str | None) -> str | None:
    """Clean a single-line field (headline, summary): strip tags, decode entities."""
    if value is None:
        return None
    text = str(value)
    if "<" in text and ">" in text:
        text = _soup(text).get_text(" ")
    text = html_lib.unescape(text)
    text = normalize_unicode(text)
    text = " ".join(text.split())
    return text or None


class _Writer:
    """Accumulates text with explicit line and paragraph breaks."""

    def __init__(self) -> None:
        self.parts: list[str] = []

    def text(self, value: str) -> None:
        if value:
            self.parts.append(value)

    def newline(self) -> None:
        self.parts.append("\n")

    def paragraph(self) -> None:
        self.parts.append("\n\n")

    def value(self) -> str:
        return "".join(self.parts)


def _is_hidden(tag: Tag) -> bool:
    if tag.has_attr("hidden"):
        return True
    style = tag.get("style")
    return bool(style and _HIDDEN_STYLE.search(style))


def _inline_text(node: Tag) -> str:
    writer = _Writer()
    _walk(node, writer, stats=None, list_depth=0)
    return " ".join(normalize_unicode(writer.value()).split())


def _merge_cells(cells: list[str]) -> list[str]:
    """Join split financial cells: ``$`` + ``1,234`` and ``(5`` + ``)`` / ``%``."""
    merged: list[str] = []
    pending_prefix = ""
    for cell in cells:
        if not cell:
            continue
        if _CURRENCY_ONLY.match(cell):
            pending_prefix += cell
            continue
        if _TRAILING_ONLY.match(cell) and merged and not pending_prefix:
            merged[-1] = merged[-1] + cell
            continue
        merged.append(pending_prefix + cell)
        pending_prefix = ""
    if pending_prefix:
        merged.append(pending_prefix)
    return merged


def _render_table(table: Tag, writer: _Writer, stats: dict | None) -> None:
    if stats is not None:
        stats["tables"] += 1
    writer.paragraph()
    for row in table.find_all("tr"):
        if row.find_parent("table") is not table:
            continue  # nested tables are rendered through their own cell
        cells = [
            _inline_text(cell)
            for cell in row.find_all(["td", "th"], recursive=False)
        ]
        merged = _merge_cells(cells)
        if merged:
            writer.text(" | ".join(merged))
            writer.newline()
    writer.paragraph()


def _walk(node: Tag, writer: _Writer, stats: dict | None, list_depth: int) -> None:
    for child in node.children:
        if isinstance(child, (Comment, Declaration, Doctype, ProcessingInstruction, CData)):
            continue
        if isinstance(child, NavigableString):
            writer.text(re.sub(r"\s+", " ", str(child)))
            continue
        if not isinstance(child, Tag):
            continue
        name = child.name
        if name == "br":
            writer.newline()
        elif name == "table":
            _render_table(child, writer, stats)
        elif name in _HEADING_TAGS:
            writer.paragraph()
            heading = _inline_text(child)
            if heading:
                writer.text(f"## {heading}")
                if stats is not None:
                    stats["headings"].append(heading)
            writer.paragraph()
        elif name in _LIST_TAGS:
            # Top-level lists are their own paragraph; nested lists stay attached.
            if list_depth == 0:
                writer.paragraph()
            ordered = name == "ol"
            number = 0
            for item in child.find_all("li", recursive=False):
                number += 1
                marker = f"{number}." if ordered else "-"
                writer.newline()
                writer.text("  " * list_depth + marker + " ")
                _walk(item, writer, stats, list_depth + 1)
            if list_depth == 0:
                writer.paragraph()
        elif name == "li":  # stray <li> outside a list
            writer.newline()
            writer.text("- ")
            _walk(child, writer, stats, list_depth + 1)
            writer.newline()
        elif list_depth > 0 and (name in _PARAGRAPH_TAGS or name in _BLOCK_TAGS):
            # Blocks inside a list item keep the item marker on the same line.
            _walk(child, writer, stats, list_depth)
            writer.text(" ")
        elif name in _PARAGRAPH_TAGS:
            writer.paragraph()
            _walk(child, writer, stats, list_depth)
            writer.paragraph()
        elif name in _BLOCK_TAGS or name in {"tr", "td", "th"}:
            writer.newline()
            _walk(child, writer, stats, list_depth)
            writer.newline()
        else:
            _walk(child, writer, stats, list_depth)


def _mark_sec_sections(text: str, headings: list[str]) -> str:
    """Put SEC ``Item x.xx`` / ``SIGNATURES`` lines on their own paragraph as headings."""
    out: list[str] = []
    for line in text.split("\n"):
        if (_SEC_ITEM_HEADING.match(line) or _SEC_SECTION_WORDS.match(line)) and not line.startswith("## "):
            headings.append(line)
            out.extend(["", f"## {line}", ""])
        else:
            out.append(line)
    return "\n".join(out)


def html_to_text(markup: str, *, base_url: str | None = None, sec_document: bool = False) -> ExtractedText:
    """Convert an HTML document or fragment to structured readable text."""
    soup = _soup(markup)
    notes: list[str] = []
    title_tag = soup.find("title")
    title = " ".join(title_tag.get_text(" ").split()) if title_tag else None

    for node in soup.find_all(string=lambda s: isinstance(s, (Comment, Declaration, Doctype, ProcessingInstruction, CData))):
        node.extract()
    for tag in soup.find_all(_DROP_TAGS):
        tag.decompose()
    ix_header_count = 0
    for tag in soup.find_all("ix:header"):
        ix_header_count += 1
        tag.decompose()
    hidden = [tag for tag in soup.find_all(True) if _is_hidden(tag)]
    hidden_count = 0
    for tag in hidden:
        if not tag.decomposed:
            hidden_count += 1
            tag.decompose()
    if hidden_count:
        notes.append(f"removed {hidden_count} hidden element(s)")

    ix_facts = sum(1 for tag in soup.find_all(True) if tag.name in _IX_FACT_TAGS)

    links: list[tuple[str, str]] = []
    if base_url:
        # Filings often split one visible link over consecutive <a> tags (even
        # mid-word), so concatenate the raw text of the first run per URL.
        texts: dict[str, str] = {}
        previous: str | None = None
        for anchor in soup.find_all("a", href=True):
            absolute, _ = urldefrag(urljoin(base_url, anchor["href"].strip()))
            if not absolute:
                continue
            if absolute not in texts:
                texts[absolute] = anchor.get_text()
            elif absolute == previous:
                texts[absolute] += anchor.get_text()
            previous = absolute
        links = [(url, " ".join(normalize_unicode(text).split())) for url, text in texts.items()]

    stats = {"tables": 0, "headings": []}
    writer = _Writer()
    root = soup.body or soup
    _walk(root, writer, stats, list_depth=0)
    text = normalize_whitespace(normalize_unicode(writer.value()))
    headings: list[str] = list(stats["headings"])
    if sec_document:
        text = normalize_whitespace(_mark_sec_sections(text, headings))
    return ExtractedText(
        text=text,
        table_count=stats["tables"],
        inline_xbrl_fact_count=ix_facts,
        section_headings=headings,
        links=links,
        warnings=notes,
        title=title,
    )


def sec_archive_links(links: list[tuple[str, str]], primary_url: str | None) -> list[tuple[str, str]]:
    """Keep only SEC-hosted EDGAR archive links (exhibits); drop the document itself."""
    kept: list[tuple[str, str]] = []
    for url, text in links:
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname not in SEC_HOSTS:
            continue
        if not parsed.path.startswith("/Archives/edgar/data/"):
            continue
        if primary_url and url == primary_url:
            continue
        kept.append((url, text))
    return kept


def looks_like_error_page(text: str) -> bool:
    """Detect SEC throttling / access-denied / not-found pages served as HTML."""
    head = text[:2000].lower()
    return any(marker in head for marker in _ERROR_PAGE_MARKERS) and len(text) < 20_000
