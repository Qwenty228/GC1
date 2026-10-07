"""Company coverage: which companies can be collected and linked, and their registration in PostgreSQL.

The coverage list is ``data/ticker_cik_map.json`` (ticker -> CIK, name, market), the file the SEC pollers already
resolve CIKs from. ``register_companies`` copies it into ``company`` / ``company_symbol``, the rows ingestion links
articles and filings through (``repository.companies_for_symbols``: an article is linked to every registered
company among its tickers).

Registration only inserts. A company that already exists (matched by CIK) and a symbol row that already exists are
kept exactly as they are, so running it again changes nothing and no stored document, revision or company link is
touched. A ticker already registered for a different company is reported as a conflict and left alone: linking it to
a second company would make every article carrying that ticker ambiguous.

Links are made when a record is stored, so articles stored before a company was registered are not linked to it;
``relink_articles`` adds those links afterwards from each article's stored tickers (insert-only, repeatable).

Three things stay separate: the companies registered here (what can be linked), ``INGEST_SYMBOLS`` (what the
collectors fetch) and a reader's watchlist on the web page (what is shown). Registering a company starts no
collector, and following one on the page neither starts nor stops anything.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field

from psycopg import Connection

from .. import config

CIK_RE = re.compile(r"^[0-9]{10}$")
SYMBOL_RE = re.compile(r"^[A-Z0-9.\-]{1,12}$")


class CoverageError(ValueError):
    """The coverage list is malformed, or a requested ticker is not on it."""


@dataclass(frozen=True)
class CoverageCompany:
    symbol: str
    cik: str
    name: str
    market: str


def coverage_companies(symbols: Iterable[str] | None = None, mapping: dict | None = None) -> list[CoverageCompany]:
    """The coverage list in file order, validated; only ``symbols`` when given (each must be on the list)."""
    mapping = config.load_ticker_map() if mapping is None else mapping
    companies: list[CoverageCompany] = []
    problems: list[str] = []
    for ticker, info in mapping.items():
        info = info if isinstance(info, dict) else {}
        company = CoverageCompany(symbol=str(ticker).strip().upper(), cik=str(info.get("cik") or "").strip(),
                                  name=str(info.get("name") or "").strip(),
                                  market=str(info.get("market") or "").strip().upper())
        missing = [label for label, ok in (("a ticker", SYMBOL_RE.match(company.symbol)),
                                           ("a 10-digit CIK", CIK_RE.match(company.cik)),
                                           ("a name", company.name), ("a market", company.market)) if not ok]
        if missing:
            problems.append(f"{ticker!r} needs {', '.join(missing)}")
        companies.append(company)
    if problems:
        raise CoverageError(f"{config.TICKER_MAP_FILE.name}: " + "; ".join(problems))
    if symbols is None:
        return companies
    wanted = list(dict.fromkeys(s.strip().upper() for s in symbols if s and s.strip()))
    known = {c.symbol: c for c in companies}
    unknown = [s for s in wanted if s not in known]
    if unknown:
        raise CoverageError(f"not on the coverage list ({config.TICKER_MAP_FILE.name}): {', '.join(unknown)}; "
                            f"known: {', '.join(known)}")
    return [known[s] for s in wanted]


@dataclass
class CompanyAction:
    """What registration does (or did) for one coverage entry."""

    symbol: str
    cik: str
    name: str
    market: str
    company: str = "exists"          # insert | exists
    company_id: str | None = None
    stored_name: str | None = None   # the existing row's name when it differs (kept unchanged)
    symbol_row: str = "exists"       # insert | exists | exists_other_market | conflict
    note: str | None = None

    @property
    def conflict(self) -> bool:
        return self.symbol_row == "conflict"


@dataclass
class RegistrationReport:
    actions: list[CompanyAction]
    dry_run: bool

    @property
    def conflicts(self) -> list[CompanyAction]:
        return [a for a in self.actions if a.conflict]

    @property
    def companies_added(self) -> int:
        return sum(a.company == "insert" and not a.conflict for a in self.actions)

    @property
    def symbols_added(self) -> int:
        return sum(a.symbol_row == "insert" for a in self.actions)

    def as_dict(self) -> dict:
        return {"dry_run": self.dry_run, "companies_added": self.companies_added, "symbols_added": self.symbols_added,
                "conflicts": len(self.conflicts), "companies": [asdict(a) for a in self.actions]}


def _symbol_rows(conn: Connection, symbol: str) -> list[dict]:
    return conn.execute(
        "SELECT s.market, s.company_id::text AS company_id, c.cik, c.name FROM company_symbol s "
        "JOIN company c ON c.company_id = s.company_id WHERE s.symbol = %s ORDER BY s.market",
        (symbol,),
    ).fetchall()


def plan_registration(conn: Connection, companies: list[CoverageCompany]) -> list[CompanyAction]:
    """What registration would do, from reads only."""
    actions = []
    for c in companies:
        action = CompanyAction(c.symbol, c.cik, c.name, c.market)
        row = conn.execute("SELECT company_id::text AS company_id, name FROM company WHERE cik = %s",
                           (c.cik,)).fetchone()
        if row is None:
            action.company = "insert"
        else:
            action.company_id = row["company_id"]
            if row["name"] != c.name:
                action.stored_name = row["name"]
        bound = _symbol_rows(conn, c.symbol)
        others = [b for b in bound if b["cik"] != c.cik]
        if others:
            action.symbol_row = "conflict"
            action.note = "; ".join(f"{c.symbol} is already registered on {b['market']} for {b['name']} "
                                    f"(CIK {b['cik'] or 'none'})" for b in others)
        elif any(b["market"] == c.market for b in bound):
            action.symbol_row = "exists"
        elif bound:
            action.symbol_row = "exists_other_market"
            action.note = f"{c.symbol} is already registered for this company on {bound[0]['market']}; kept"
        else:
            action.symbol_row = "insert"
        actions.append(action)
    return actions


def register_companies(conn: Connection, companies: list[CoverageCompany], *, dry_run: bool = False
                       ) -> RegistrationReport:
    """Insert missing companies and symbols (inside the caller's transaction); never updates or deletes.

    An entry whose ticker belongs to another company is skipped entirely. With ``dry_run`` nothing is written.
    """
    actions = plan_registration(conn, companies)
    if dry_run:
        return RegistrationReport(actions, dry_run=True)
    for action in actions:
        if action.conflict:
            continue
        if action.company == "insert":
            row = conn.execute("INSERT INTO company (name, cik) VALUES (%s, %s) ON CONFLICT (cik) DO NOTHING "
                               "RETURNING company_id::text AS company_id", (action.name, action.cik)).fetchone()
            if row is None:  # created meanwhile, e.g. by an SEC filing of that issuer: use it as it is
                row = conn.execute("SELECT company_id::text AS company_id FROM company WHERE cik = %s",
                                   (action.cik,)).fetchone()
                action.company = "exists"
            action.company_id = row["company_id"]
        if action.symbol_row != "insert":
            continue
        inserted = conn.execute(
            "INSERT INTO company_symbol (company_id, symbol, market) VALUES (%s, %s, %s) "
            "ON CONFLICT (market, symbol) DO NOTHING RETURNING symbol_id",
            (action.company_id, action.symbol, action.market),
        ).fetchone()
        if inserted is None:  # registered meanwhile: report what is there now
            rebound = [b for b in _symbol_rows(conn, action.symbol) if b["cik"] != action.cik]
            action.symbol_row = "conflict" if rebound else "exists"
            if rebound:
                action.note = f"{action.symbol} was registered for {rebound[0]['name']} meanwhile"
    return RegistrationReport(actions, dry_run=False)


@dataclass
class RelinkReport:
    """What ``relink_articles`` found (and, unless a dry run, added)."""

    dry_run: bool
    scanned: int = 0                 # news article revisions in the partition
    without_payload: int = 0         # no stored original payload to read tickers from (left as they are)
    links_added: int = 0
    revisions: list[dict] = field(default_factory=list)  # each changed revision: id, title, added tickers

    def as_dict(self) -> dict:
        return {"dry_run": self.dry_run, "scanned": self.scanned, "without_payload": self.without_payload,
                "revisions_changed": len(self.revisions), "links_added": self.links_added,
                "revisions": self.revisions}


def relink_articles(conn: Connection, *, is_sample: bool = False, dry_run: bool = False) -> RelinkReport:
    """Link stored news articles to companies registered after they were stored (inside the caller's transaction).

    Ingestion links an article to the registered companies among its tickers when it is stored, so an article
    stored before a company was registered stays unlinked from it. This applies the same resolution
    (``repository.companies_for_symbols`` over the tickers in the article's stored original payload, normalized as
    ingestion normalizes them) to every stored revision. It only adds missing ``revision_company`` rows: no link is
    removed, and documents, revisions, events, summaries and reviews are not touched. SEC filings need nothing:
    they are linked to their issuer by CIK, which registration matches. Repeatable: a second run adds nothing.
    """
    from ..ingest.adapters import normalize_symbols
    from .repository import companies_for_symbols

    report = RelinkReport(dry_run=dry_run)
    rows = conn.execute(
        """
        SELECT r.revision_id::text AS revision_id, r.title, s.raw_payload -> 'symbols' AS symbols,
               s.revision_id IS NOT NULL AS has_payload,
               ARRAY(SELECT rc.company_id::text FROM revision_company rc WHERE rc.revision_id = r.revision_id) AS linked
          FROM document d
          JOIN document_revision r ON r.document_id = d.document_id
          LEFT JOIN document_revision_source s ON s.revision_id = r.revision_id
         WHERE d.record_type = 'news_article' AND d.is_sample = %s
         ORDER BY r.retrieved_at, r.revision_id
        """,
        (is_sample,),
    ).fetchall()
    tickers_of = {r["company_id"]: r["symbol"] for r in conn.execute(
        "SELECT company_id::text AS company_id, min(symbol) AS symbol FROM company_symbol GROUP BY 1")}
    for row in rows:
        report.scanned += 1
        if not row["has_payload"]:
            report.without_payload += 1
            continue
        symbols, _warnings = normalize_symbols(row["symbols"])
        company_ids, _unresolved = companies_for_symbols(conn, symbols)
        missing = [c for c in company_ids if c not in set(row["linked"])]
        if not missing:
            continue
        if not dry_run:
            for company_id in missing:
                conn.execute("INSERT INTO revision_company (revision_id, company_id) VALUES (%s, %s) "
                             "ON CONFLICT (revision_id, company_id) DO NOTHING", (row["revision_id"], company_id))
        report.links_added += len(missing)
        report.revisions.append({"revision_id": row["revision_id"], "title": row["title"],
                                 "added": sorted(tickers_of.get(c, c) for c in missing)})
    return report


def unregistered_symbols(conn: Connection, symbols: Iterable[str]) -> list[str]:
    """The tickers among ``symbols`` with no ``company_symbol`` row: their articles would be stored unlinked."""
    wanted = [s.strip().upper() for s in symbols if s and s.strip() and s.strip() != "*"]
    if not wanted:
        return []
    found = {r["symbol"] for r in conn.execute("SELECT DISTINCT symbol FROM company_symbol WHERE symbol = ANY(%s)",
                                               (wanted,))}
    return [s for s in wanted if s not in found]
