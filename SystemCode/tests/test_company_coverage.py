"""Company coverage: the 11 companies, their registration, and collecting and linking several companies.

Offline: the coverage list against the SEC identifiers recorded in ``fixtures/sec_company_identifiers.json``;
SEC polling and recovery per company with one shared rate limit; Alpaca subscription and REST recovery for every
configured symbol (fake network, nothing leaves the machine).

Database: each test builds its own temporary database from the repository's migration files
(``conftest.temporary_database``, dropped afterwards; never stock_news, and the configured test database itself
is not touched). Registration twice, with pre-existing rows and a conflicting ticker; a multi-company article
stored once and linked to each registered company.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from conftest import (
    FIXTURES,
    SYNTHETIC_8K,
    FakeSession,
    html_response,
    json_response,
    synthetic_submissions,
    temporary_database,
)
from datasources import config, sec
from datasources.alpaca import AlpacaAuth
from datasources.db.companies import CoverageError, coverage_companies, register_companies, unregistered_symbols
from datasources.ingest import cli
from datasources.ingest.sec_client import RateLimiter, SecClient
from datasources.ingest.service import IngestionService
from datasources.ingest.settings import IngestSettings

PROPOSAL = ["NVDA", "MSFT", "AAPL", "GOOGL", "AMZN", "META", "NFLX", "DIS", "JPM", "V", "GS"]
SEC_RECORD = json.loads((FIXTURES / "sec_company_identifiers.json").read_text(encoding="utf-8"))
NVDA_CIK, MSFT_CIK, AAPL_CIK = "0001045810", "0000789019", "0000320193"


# ----------------------------------------------------------------------------- the coverage list (offline)
def test_coverage_list_is_the_eleven_companies_with_sec_verified_identifiers():
    companies = coverage_companies()
    assert [c.symbol for c in companies] == PROPOSAL
    recorded = SEC_RECORD["companies"]
    for c in companies:
        sec_row = recorded[c.symbol]
        assert c.cik == sec_row["cik"] == sec_row["submissions"]["cik"], c.symbol
        assert sec_row["company_tickers_exchange"]["ticker"] == c.symbol
        assert c.symbol in sec_row["submissions"]["tickers"]
        assert c.market == sec_row["company_tickers_exchange"]["exchange"].upper()
        assert sec_row["submissions"]["url"] == f"https://data.sec.gov/submissions/CIK{c.cik}.json"
    assert len({c.cik for c in companies}) == len(companies)  # one company per ticker
    assert config.cik_for("v") == "0001403161" and config.cik_for("GS") == "0000886982"  # the SEC pollers' lookup


def test_coverage_list_rejects_malformed_entries_and_unknown_tickers():
    with pytest.raises(CoverageError, match="'BAD' needs a 10-digit CIK, a market"):
        coverage_companies(mapping={"BAD": {"cik": "12345", "name": "Bad Corp"}})
    with pytest.raises(CoverageError, match="not on the coverage list.*TSLA"):
        coverage_companies(["NVDA", "TSLA"])
    assert [c.symbol for c in coverage_companies(["v", " nvda", "V"])] == ["V", "NVDA"]


# ----------------------------------------------------------------------------- SEC: per company, one rate limit
COMPANIES = {"SYNA": "0009999991", "SYNB": "0009999992", "SYNC": "0009999993"}


@pytest.fixture
def synthetic_companies(monkeypatch, tmp_path):
    mapping = {**config.load_ticker_map(), **{t: {"cik": c, "name": f"{t} Example Corp", "market": "TEST"}
                                              for t, c in COMPANIES.items()}}
    monkeypatch.setattr(config, "load_ticker_map", lambda: mapping)
    monkeypatch.setattr(sec, "CACHE_DIR", tmp_path / "cache")


def submissions_url(cik: str) -> str:
    return f"https://data.sec.gov/submissions/CIK{cik}.json"


def doc_url(cik: str, accession: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/synthetic-8k.htm"


def filing(cik: str, n: int, days_ago: int = 1) -> dict:
    date = (datetime.now(timezone.utc).date() - timedelta(days=days_ago)).isoformat()
    return {"accessionNumber": f"{cik}-26-{n:06d}", "form": "8-K", "filingDate": date,
            "primaryDocument": "synthetic-8k.htm"}


def sec_settings(tmp_path, symbols) -> IngestSettings:
    return IngestSettings(symbols=list(symbols), output_root=tmp_path / "output", sec_user_agent="Tests tests@example.test",
                          sec_initial_lookback_days=30, sec_recovery_max_days=365)


def test_sec_pollers_for_several_companies_share_one_rate_limit(synthetic_companies, tmp_path):
    clock = [0.0]
    waits: list[float] = []

    def sleep(seconds: float) -> None:
        waits.append(seconds)
        clock[0] += seconds

    routes = {}
    for cik in COMPANIES.values():
        routes[submissions_url(cik)] = json_response(synthetic_submissions([filing(cik, 1)], cik=cik))
        routes[doc_url(cik, filing(cik, 1)["accessionNumber"])] = html_response(SYNTHETIC_8K.read_bytes())
    session = FakeSession(routes)
    limiter = RateLimiter(2.0, clock=lambda: clock[0], sleep=sleep)
    client = SecClient("Tests tests@example.test", session=session, limiter=limiter, sleep=sleep)
    service = IngestionService(sec_settings(tmp_path, COMPANIES), sec_client=client)
    service.startup()
    for symbol in COMPANIES:  # what the per-company pollers do, sharing the service's one client
        assert [service.process_item(item) for item in service.sec_poll_once(symbol)] == ["saved"]
    requests = len(session.calls)
    assert requests == 6  # a submissions request and a document per company
    # The clock only moves when the limiter waits: every request after the first waited a full interval,
    # whichever company it was for, so all companies together stay within SEC_REQUESTS_PER_SECOND.
    assert waits == [pytest.approx(0.5)] * (requests - 1)
    assert [c["url"] for c in session.calls][::2] == [submissions_url(c) for c in COMPANIES.values()]


def test_sec_polling_and_recovery_are_independent_per_company(synthetic_companies, tmp_path):
    """A failing company neither stops nor delays another; each keeps its own scope floor and retry state."""
    a_cik, b_cik = COMPANIES["SYNA"], COMPANIES["SYNB"]
    a_filing, b_filing = filing(a_cik, 1), filing(b_cik, 1)
    settings = sec_settings(tmp_path, ["SYNA", "SYNB"])

    def run_once(session: FakeSession) -> IngestionService:
        client = SecClient("Tests tests@example.test", session=session, limiter=RateLimiter(1000.0, sleep=lambda s: None),
                           sleep=lambda s: None, max_retries=0)
        service = IngestionService(settings, sec_client=client)  # a new process each run; the ledger is on disk
        asyncio.run(cli.run_service(service, enable_sec=True, enable_alpaca=False, once=True))
        return service

    # Run 1: SYNA's submissions fail; SYNB is collected anyway.
    first = run_once(FakeSession({submissions_url(a_cik): json_response({"error": "busy"}, 503),
                                  submissions_url(b_cik): json_response(synthetic_submissions([b_filing], cik=b_cik)),
                                  doc_url(b_cik, b_filing["accessionNumber"]): html_response(SYNTHETIC_8K.read_bytes())}))
    root = settings.root
    assert (root / f"processed/sec/{b_cik}/{b_filing['accessionNumber']}.json").exists()
    assert not (root / f"processed/sec/{a_cik}").exists()
    assert first.ledger.get_value(f"sec_scope_floor:{b_cik}") and not first.ledger.get_value(f"sec_scope_floor:{a_cik}")

    # Run 2: SYNA's document download fails (retryable); SYNB's filing is not fetched again.
    second_session = FakeSession({
        submissions_url(a_cik): json_response(synthetic_submissions([a_filing], cik=a_cik)),
        submissions_url(b_cik): json_response(synthetic_submissions([b_filing], cik=b_cik)),
        doc_url(a_cik, a_filing["accessionNumber"]): html_response("busy", 503)})
    second = run_once(second_session)
    a_identity = f"sec_edgar:{a_cik}:{a_filing['accessionNumber']}"
    assert second.ledger.sec_entry(a_identity)["status"] == "failed"
    assert second.ledger.sec_entry(a_identity)["retryable"] is True
    assert doc_url(b_cik, b_filing["accessionNumber"]) not in second_session.urls()
    floor_a = second.ledger.get_value(f"sec_scope_floor:{a_cik}")
    assert floor_a and floor_a == second.ledger.get_value(f"sec_scope_floor:{b_cik}")  # each set by its own first poll

    # Run 3 (after SYNA's backoff): SYNA recovers its filing; SYNB is untouched.
    second.ledger.sec_entry(a_identity)["next_retry_at"] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    second.ledger.save()
    third_session = FakeSession({
        submissions_url(a_cik): json_response(synthetic_submissions([a_filing], cik=a_cik)),
        submissions_url(b_cik): json_response(synthetic_submissions([b_filing], cik=b_cik)),
        doc_url(a_cik, a_filing["accessionNumber"]): html_response(SYNTHETIC_8K.read_bytes())})
    third = run_once(third_session)
    assert third.ledger.sec_entry(a_identity)["status"] == "completed"
    assert (root / f"processed/sec/{a_cik}/{a_filing['accessionNumber']}.json").exists()
    assert doc_url(b_cik, b_filing["accessionNumber"]) not in third_session.urls()


# ----------------------------------------------------------------------------- Alpaca: every configured symbol
class RestPages:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        from conftest import FakeResponse

        self.calls.append({"url": url, "params": dict(params or {})})
        return FakeResponse(200, self.pages.pop(0), {"Content-Type": "application/json"})


def test_alpaca_stream_and_rest_recovery_cover_every_configured_symbol(tmp_path, monkeypatch):
    from alpaca_news import client

    settings = IngestSettings(symbols=PROPOSAL, output_root=tmp_path / "output")
    auth = AlpacaAuth(mode="trading", api_key="k" * 8, secret_key="s" * 8)
    stream = cli._news_settings(settings, auth)
    assert stream.symbols == PROPOSAL
    assert client.subscription_message(stream.symbols) == {"action": "subscribe", "news": PROPOSAL}

    # The WebSocket client sends that subscription for all 11 symbols.
    sent = []

    class FakeSocket:
        async def send(self, message):
            sent.append(json.loads(message))

        def __aiter__(self):
            async def frames():
                for frame in ([{"T": "success", "msg": "connected"}], [{"T": "success", "msg": "authenticated"}],
                              [{"T": "subscription", "news": PROPOSAL}]):
                    yield json.dumps(frame)
            return frames()

    class FakeConnect:
        def __init__(self, url, additional_headers):
            pass

        async def __aenter__(self):
            return FakeSocket()

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(client.websockets, "connect", FakeConnect)
    asyncio.run(client.stream_once(stream, set(), on_articles=lambda articles: asyncio.sleep(0),
                                   on_frame=lambda frame: None))  # never the real archive file
    assert sent == [{"action": "subscribe", "news": PROPOSAL}]

    # REST recovery asks for the same symbols in one window.
    service = IngestionService(settings, alpaca_auth=auth)
    service.startup()
    service.alpaca_session = RestPages([{"news": [], "next_page_token": None}])

    async def recover():
        writer = asyncio.create_task(service.writer())
        await service.alpaca_backfill("startup")
        writer.cancel()

    asyncio.run(recover())
    (call,) = service.alpaca_session.calls
    assert call["params"]["symbols"] == ",".join(PROPOSAL)


# ----------------------------------------------------------------------------- registration (temporary databases)
def rows(db, sql: str, *params) -> list[dict]:
    with db.connection() as conn:
        return conn.execute(sql, params or None).fetchall()


def register(db, symbols=None, *, dry_run=False):
    with db.transaction() as conn:
        return register_companies(conn, coverage_companies(symbols), dry_run=dry_run)


def sample_article(article_id: int, symbols: list[str], at: datetime) -> dict:
    stamp = at.isoformat().replace("+00:00", "Z")
    return {"id": article_id, "headline": f"Example coverage story {article_id}", "summary": "",
            "content": f"<p>Synthetic article {article_id} about {', '.join(symbols)}.</p>", "author": "Pytest",
            "created_at": stamp, "updated_at": stamp, "url": f"https://news.example.test/{article_id}",
            "symbols": symbols, "source": "pytest-synthetic-wire"}


def store_article(db, raw: dict):
    from datasources.ingest.graph import run_pipeline
    from datasources.similarity.pipeline import PipelineContext

    result = run_pipeline([{"provider": "alpaca", "transport": "rest", "api_endpoint": "https://example.test/news",
                            "retrieved_at": datetime.now(timezone.utc), "requested_symbols": ["NVDA"],
                            "is_sample": True, "raw_record": raw}], PipelineContext(database=db))
    assert not result.errors, result.errors
    (outcome,) = result.outcomes
    assert outcome.error is None, outcome.error
    return outcome


def test_registration_is_repeatable_and_keeps_existing_rows():
    from datasources.db.repository import company_by_cik

    with temporary_database("coverage") as db:
        # Before: the baseline's NVIDIA row with an NVDA-linked article, and Microsoft created the way an SEC
        # filing creates its issuer (SEC name, no ticker).
        nvda_story = store_article(db, sample_article(81_000_001, ["NVDA"], datetime(2034, 6, 1, tzinfo=timezone.utc)))
        with db.transaction() as conn:
            msft_id = company_by_cik(conn, MSFT_CIK, "MICROSOFT CORP")
        before = {t: rows(db, f"SELECT * FROM {t} ORDER BY 1, 2") for t in ("company", "company_symbol",
                                                                           "revision_company", "document",
                                                                           "document_revision")}
        assert unregistered_symbols_in(db, PROPOSAL) == PROPOSAL[1:]

        preview = register(db, dry_run=True)
        assert (preview.companies_added, preview.symbols_added, len(preview.conflicts)) == (9, 10, 0)
        assert rows(db, "SELECT * FROM company ORDER BY 1, 2") == before["company"]  # a dry run writes nothing

        first = register(db)
        assert (first.companies_added, first.symbols_added, len(first.conflicts)) == (9, 10, 0)
        actions = {a.symbol: a for a in first.actions}
        assert (actions["NVDA"].company, actions["NVDA"].symbol_row) == ("exists", "exists")
        assert (actions["MSFT"].company, actions["MSFT"].symbol_row) == ("exists", "insert")
        assert actions["MSFT"].company_id == msft_id and actions["MSFT"].stored_name == "MICROSOFT CORP"
        assert unregistered_symbols_in(db, PROPOSAL) == []

        second = register(db)
        assert (second.companies_added, second.symbols_added, len(second.conflicts)) == (0, 0, 0)
        assert {a.symbol: (a.company, a.symbol_row) for a in second.actions} == {s: ("exists", "exists") for s in PROPOSAL}

        # Nothing that existed changed: same ids, names, links, documents and revisions.
        after = {t: rows(db, f"SELECT * FROM {t} ORDER BY 1, 2") for t in before}
        for table in ("revision_company", "document", "document_revision"):
            assert after[table] == before[table], table
        assert all(row in after["company"] for row in before["company"])
        assert all(row in after["company_symbol"] for row in before["company_symbol"])
        assert len(after["company"]) == 11 and len(after["company_symbol"]) == 11
        (msft,) = [r for r in after["company"] if r["cik"] == MSFT_CIK]
        assert str(msft["company_id"]) == msft_id and msft["name"] == "MICROSOFT CORP"  # kept, not renamed
        assert rows(db, "SELECT count(*) AS n FROM revision_company WHERE revision_id = %s",
                    nvda_story.revision_id)[0]["n"] == 1


def unregistered_symbols_in(db, symbols) -> list[str]:
    with db.connection() as conn:
        return unregistered_symbols(conn, symbols)


def test_a_ticker_held_by_another_company_is_reported_and_left_alone(monkeypatch, capsys):
    from datasources.db import cli as db_cli

    with temporary_database("coverage") as db:
        with db.transaction() as conn:
            other = conn.execute("INSERT INTO company (name, cik) VALUES ('Not Visa Example', '0000000042') "
                                 "RETURNING company_id").fetchone()["company_id"]
            conn.execute("INSERT INTO company_symbol (company_id, symbol, market) VALUES (%s, 'V', 'NASDAQ')", (other,))
        monkeypatch.setattr(db_cli, "load_db_settings", lambda: db.settings)

        assert db_cli.main(["register-companies", "--dry-run"]) == 1
        out = capsys.readouterr()
        assert "READ ONLY: nothing was changed." in out.out and "V is already registered on NASDAQ for Not Visa" in out.out
        assert rows(db, "SELECT count(*) AS n FROM company")[0]["n"] == 2

        assert db_cli.main(["register-companies"]) == 1  # the conflict is reported; the others are registered
        assert "CONFLICT" in capsys.readouterr().out
        assert rows(db, "SELECT count(*) AS n FROM company WHERE cik = '0001403161'")[0]["n"] == 0
        assert rows(db, "SELECT c.name FROM company_symbol s JOIN company c USING (company_id) WHERE s.symbol = 'V'") \
            == [{"name": "Not Visa Example"}]
        assert unregistered_symbols_in(db, PROPOSAL) == []  # V still resolves, to the company it already had
        assert rows(db, "SELECT count(*) AS n FROM company")[0]["n"] == 2 + 9

        assert db_cli.main(["register-companies", "--symbols", "NVDA,MSFT", "--json"]) == 0
        report = json.loads(capsys.readouterr().out)
        assert report["companies_added"] == 0 and [c["symbol"] for c in report["companies"]] == ["NVDA", "MSFT"]


def test_a_multi_company_article_is_stored_once_and_linked_to_each_company(tmp_path):
    with temporary_database("coverage") as db:
        register(db)
        settings = IngestSettings(symbols=PROPOSAL, output_root=tmp_path / "output", demo=True)  # sample rows
        service = IngestionService(settings, alpaca_auth=AlpacaAuth(mode="trading", api_key="k" * 8,
                                                                    secret_key="s" * 8), database=db)
        service.startup()
        article = sample_article(82_000_001, ["NVDA", "MSFT", "AAPL", "TSLA"], datetime(2034, 7, 1, tzinfo=timezone.utc))
        now = datetime.now(timezone.utc)
        for transport in ("rest", "websocket", "rest"):  # REST recovery and the stream deliver the same article
            for item in service.alpaca_items([article], transport=transport, endpoint="https://example.test", retrieved_at=now,
                                             raw_refs=[]):
                assert service.process_item(item) == "saved"
        assert service.stats["alpaca_saved"] == 1 and service.stats["alpaca_skip_unchanged_delivery"] == 2
        assert service.stats["db_unresolved_symbols"] == 1  # TSLA is reported, never invented
        (document,) = rows(db, "SELECT document_id FROM document WHERE source_record_id = '82000001'")
        linked = rows(db, """SELECT c.cik FROM document d JOIN document_revision r ON r.document_id = d.document_id
                               JOIN revision_company rc ON rc.revision_id = r.revision_id
                               JOIN company c ON c.company_id = rc.company_id
                              WHERE d.document_id = %s ORDER BY c.cik""", document["document_id"])
        assert [r["cik"] for r in linked] == sorted([NVDA_CIK, MSFT_CIK, AAPL_CIK])
        assert rows(db, "SELECT count(*) AS n FROM document_revision WHERE document_id = %s",
                    document["document_id"])[0]["n"] == 1


def test_the_launcher_and_the_worker_warn_about_collected_tickers_without_a_company_row(monkeypatch, caplog):
    import datasources.db as db_package
    from datasources import launcher

    with temporary_database("coverage") as db:
        monkeypatch.setattr(db_package, "load_db_settings", lambda: db.settings)
        monkeypatch.setenv("DATASOURCES_DB_ENABLED", "true")

        def preflight_warnings() -> list[str]:
            report = launcher.Preflight()
            launcher._check_database(report, collectors=True, similarity=None, summary=None,
                                     symbols=["NVDA", "MSFT", "GS"])
            assert not report.problems, report.problems
            return [w for w in report.warnings if "INGEST_SYMBOLS" in w]

        (warning,) = preflight_warnings()
        assert "MSFT, GS" in warning and "register-companies --dry-run" in warning
        with caplog.at_level("WARNING", logger="datasources.ingest"):
            assert cli.warn_unregistered_symbols(db, ["NVDA", "MSFT"]) == ["MSFT"]
        assert "register-companies" in caplog.text

        register(db)
        assert preflight_warnings() == []
        assert cli.warn_unregistered_symbols(db, PROPOSAL) == []


def test_relinking_adds_links_for_companies_registered_after_articles_were_stored(monkeypatch, capsys):
    """Articles stored before a company was registered get the missing links; nothing is removed or rewritten."""
    from datasources.db import cli as db_cli
    from datasources.db.read import list_articles
    from datasources.ingest.graph import run_pipeline
    from datasources.similarity.pipeline import PipelineContext
    from test_similarity_pipeline import sec_envelope

    with temporary_database("coverage") as db:
        at = datetime(2034, 8, 1, tzinfo=timezone.utc)
        early = store_article(db, sample_article(83_000_001, ["NVDA", "MSFT", "TSLA"], at))  # before MSFT exists
        (filing,) = run_pipeline([sec_envelope(at, "0001045810-34-000777")], PipelineContext(database=db)).outcomes
        register(db)
        with db.transaction() as conn:  # a link the tickers do not explain must survive relinking
            conn.execute("INSERT INTO revision_company (revision_id, company_id) SELECT %s, company_id FROM company "
                         "WHERE cik = '0000886982'", (early.revision_id,))
        later = store_article(db, sample_article(83_000_002, ["MSFT", "AAPL"], at + timedelta(hours=1)))
        kept = {t: rows(db, f"SELECT * FROM {t} ORDER BY 1, 2") for t in ("document", "document_revision",
                                                                         "sec_filing_details")}
        before = rows(db, "SELECT revision_id, company_id FROM revision_company ORDER BY 1, 2")
        linked = lambda revision: [r["symbol"] for r in rows(  # noqa: E731
            db, "SELECT cs.symbol FROM revision_company rc JOIN company_symbol cs USING (company_id) "
                "WHERE rc.revision_id = %s ORDER BY 1", revision)]
        assert linked(early.revision_id) == ["GS", "NVDA"] and linked(later.revision_id) == ["AAPL", "MSFT"]
        monkeypatch.setattr(db_cli, "load_db_settings", lambda: db.settings)

        assert db_cli.main(["relink-companies", "--sample", "--dry-run"]) == 0
        out = capsys.readouterr().out
        assert "READ ONLY" in out and "+ MSFT" in out and "1 link(s) to add on 1 of 2" in out
        assert rows(db, "SELECT revision_id, company_id FROM revision_company ORDER BY 1, 2") == before

        assert db_cli.main(["relink-companies", "--sample"]) == 0
        assert "1 link(s) added on 1 of 2" in capsys.readouterr().out
        assert linked(early.revision_id) == ["GS", "MSFT", "NVDA"]  # MSFT added, the extra GS link kept; TSLA unknown
        assert linked(later.revision_id) == ["AAPL", "MSFT"] and linked(filing.revision_id) == ["NVDA"]
        after = rows(db, "SELECT revision_id, company_id FROM revision_company ORDER BY 1, 2")
        assert all(link in after for link in before) and len(after) == len(before) + 1  # nothing removed
        assert {t: rows(db, f"SELECT * FROM {t} ORDER BY 1, 2") for t in kept} == kept

        assert db_cli.main(["relink-companies", "--sample", "--json"]) == 0
        assert json.loads(capsys.readouterr().out)["links_added"] == 0  # repeatable: nothing left to add
        assert db_cli.main(["relink-companies"]) == 0  # the live partition is separate (empty here)
        assert "0 link(s) added on 0 of 0" in capsys.readouterr().out
        with db.connection() as conn:
            msft = list_articles(conn, include_sample=True, limit=50, offset=0, source="news", symbol="MSFT")
        assert {a["revision_id"] for a in msft["articles"]} == {early.revision_id, later.revision_id}
