"""Live-only collection (INGEST_LIVE_ONLY=true), offline with mocked SEC and Alpaca APIs.

Every worker startup takes a fresh UTC cutoff; nothing published before it is collected; there is no SEC catch-up
and no Alpaca REST backfill (at startup or after a reconnect); after a reconnect only news published from then on
is collected; the saved recovery state (Alpaca cursor, SEC scope floors, retry entries) is left untouched, so
recovery resumes from it when the mode is switched off. Polling, duplicate prevention and processing are unchanged.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone

import pytest

from conftest import SYNTHETIC_8K, FakeSession, html_response, json_response, synthetic_submissions
from datasources import config, sec
from datasources.alpaca import AlpacaAuth
from datasources.ingest import cli
from datasources.ingest.sec_client import RateLimiter, SecClient
from datasources.ingest.service import ALPACA_CURSOR, IngestionService, sec_before_cutoff
from datasources.ingest.settings import ConfigurationError, IngestSettings, load_settings
from datasources.launcher import recovery_summary

OLD, NEW = "0009999991", "0009999992"  # a company with saved progress, and one never collected


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@pytest.fixture(autouse=True)
def synthetic_companies(monkeypatch, tmp_path):
    mapping = {**config.load_ticker_map(), "SYNA": {"cik": OLD, "name": "SYNA Example", "market": "TEST"},
               "SYNB": {"cik": NEW, "name": "SYNB Example", "market": "TEST"}}
    monkeypatch.setattr(config, "load_ticker_map", lambda: mapping)
    monkeypatch.setattr(sec, "CACHE_DIR", tmp_path / "cache")


def live_settings(tmp_path, **overrides) -> IngestSettings:
    return IngestSettings(**{"symbols": ["SYNA", "SYNB"], "output_root": tmp_path / "output",
                             "sec_user_agent": "Tests tests@example.test", "live_only": True, **overrides})


def test_the_mode_is_configured_explicitly(monkeypatch):
    monkeypatch.delenv("INGEST_LIVE_ONLY", raising=False)
    assert load_settings().live_only is False  # off unless asked for
    monkeypatch.setenv("INGEST_LIVE_ONLY", "true")
    assert load_settings().live_only is True
    monkeypatch.setenv("INGEST_LIVE_ONLY", "maybe")
    with pytest.raises(ConfigurationError, match="INGEST_LIVE_ONLY"):
        load_settings()


def test_sec_before_cutoff_uses_the_acceptance_time_and_never_guesses_a_time_of_day():
    cutoff = datetime(2034, 5, 2, 14, 0, tzinfo=timezone.utc)
    assert sec_before_cutoff({"acceptanceDateTime": "2034-05-02T13:59:59.000Z", "filingDate": "2034-05-02"}, cutoff)
    assert not sec_before_cutoff({"acceptanceDateTime": "2034-05-02T14:00:00.000Z", "filingDate": "2034-05-02"}, cutoff)
    assert sec_before_cutoff({"acceptanceDateTime": "", "filingDate": "2034-05-01"}, cutoff)
    assert not sec_before_cutoff({"acceptanceDateTime": "", "filingDate": "2034-05-02"}, cutoff)  # same day: kept


# ----------------------------------------------------------------------------- SEC
def filing(cik: str, n: int, accepted: datetime | None, filed: datetime | None = None) -> dict:
    return {"accessionNumber": f"{cik}-34-{n:06d}", "form": "8-K", "filingDate": (filed or accepted).date().isoformat(),
            "acceptanceDateTime": stamp(accepted) if accepted else "", "primaryDocument": "synthetic-8k.htm"}


def doc_url(cik: str, accession: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/synthetic-8k.htm"


def test_sec_polling_collects_only_filings_after_the_cutoff_and_keeps_saved_progress(tmp_path):
    cutoff = utcnow().replace(microsecond=0)
    old_filings = [filing(OLD, 1, cutoff - timedelta(days=3)),             # before the cutoff (earlier day)
                   filing(OLD, 2, cutoff - timedelta(minutes=30)),          # before the cutoff (same day)
                   filing(OLD, 3, cutoff - timedelta(hours=2)),             # failed earlier: its retry waits too
                   filing(OLD, 4, cutoff + timedelta(minutes=1)),           # after the cutoff
                   filing(OLD, 5, None, filed=cutoff)]                       # no acceptance time, cutoff's day
    submissions = synthetic_submissions(old_filings, cik=OLD)
    submissions["filings"]["files"] = [{"name": f"CIK{OLD}-submissions-001.json", "filingCount": 9,
                                        "filingFrom": "2015-01-01", "filingTo": "2020-01-01"}]  # must not be fetched
    new_filings = [filing(NEW, 1, cutoff - timedelta(hours=1)), filing(NEW, 2, cutoff + timedelta(minutes=2))]
    routes = {f"https://data.sec.gov/submissions/CIK{OLD}.json": json_response(submissions),
              f"https://data.sec.gov/submissions/CIK{NEW}.json": json_response(
                  synthetic_submissions(new_filings, cik=NEW))}
    for f in (old_filings[3], old_filings[4], new_filings[1]):
        routes[doc_url(f["accessionNumber"][:10], f["accessionNumber"])] = html_response(SYNTHETIC_8K.read_bytes())
    session = FakeSession(routes)
    client = SecClient("Tests tests@example.test", session=session, limiter=RateLimiter(1000.0, sleep=lambda s: None),
                       sleep=lambda s: None)
    service = IngestionService(live_settings(tmp_path), sec_client=client)
    service.startup()
    # Saved recovery state from earlier runs: a scope floor for SYNA and a pending retry of one of its filings.
    service.ledger.set_value(f"sec_scope_floor:{OLD}", "2020-01-01")
    failed = f"sec_edgar:{OLD}:{old_filings[2]['accessionNumber']}"
    service.ledger.mark_sec_failed(failed, kind="http_503", message="busy", retryable=True)
    service.ledger.data["sec"][failed]["next_retry_at"] = stamp(cutoff - timedelta(hours=1))  # would be due now
    saved = json.loads(json.dumps(service.ledger.data))

    assert service.begin_live_only(now=cutoff) == cutoff
    old_items = service.sec_poll_once("SYNA")
    assert [i.payload["row"]["accessionNumber"] for i in old_items] == [old_filings[3]["accessionNumber"],
                                                                         old_filings[4]["accessionNumber"]]
    new_items = service.sec_poll_once("SYNB")
    assert [i.payload["row"]["accessionNumber"] for i in new_items] == [new_filings[1]["accessionNumber"]]
    assert service.stats["before_live_cutoff_sec"] == 3  # SYNA's same-day filing and old retry, SYNB's earlier one
    assert [service.process_item(i) for i in old_items + new_items] == ["saved"] * 3
    assert all("submissions-001" not in url for url in session.urls())  # no historical catch-up

    # Recovery state is exactly as it was: no floor for the new company, SYNA's floor and pending retry untouched.
    assert service.ledger.data["cursors"] == saved["cursors"]
    assert service.ledger.data["sec"][failed] == saved["sec"][failed]
    assert service.ledger.get_value(f"sec_scope_floor:{NEW}") is None

    # Polling again (normal polling while running) finds nothing new: duplicate prevention is unchanged.
    assert service.sec_poll_once("SYNA") == [] and service.sec_poll_once("SYNB") == []


# ----------------------------------------------------------------------------- Alpaca, through the real worker loop
class NoRest:
    """Any REST request fails the test: live-only collection makes none."""

    def __init__(self):
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append(url)
        raise AssertionError(f"unexpected Alpaca REST request {url}")


class StopScenario(Exception):
    pass


def news(article_id: int, published: datetime, updated: datetime | None = None, symbols=("SYNA",)) -> dict:
    return {"T": "n", "id": article_id, "headline": f"Example headline {article_id}", "summary": "",
            "content": f"<p>Example body {article_id}.</p>", "author": "Pytest", "created_at": stamp(published),
            "updated_at": stamp(updated or published), "url": f"https://news.example.test/{article_id}",
            "symbols": list(symbols), "source": "pytest-synthetic-wire"}


def test_the_worker_streams_new_news_only_with_no_backfill_and_resumes_after_a_reconnect_without_recovery(
        tmp_path, monkeypatch):
    from alpaca_news import client

    settings = live_settings(tmp_path)
    service = IngestionService(settings, alpaca_auth=AlpacaAuth(mode="trading", api_key="k" * 8, secret_key="s" * 8))
    service.alpaca_session = NoRest()
    service.startup()
    old_cursor = utcnow() - timedelta(hours=5)
    service.ledger.set_cursor(ALPACA_CURSOR, old_cursor)  # saved recovery state from an earlier run
    seen: dict[str, datetime] = {}

    async def scripted_stream(_settings, *, on_articles, on_subscribed, on_frame, on_disconnected):
        await on_subscribed()
        now = utcnow()
        await on_articles([news(1, now - timedelta(hours=1), updated=now),   # an update to an older article
                           news(2, now + timedelta(seconds=1)),               # newly published
                           news(2, now + timedelta(seconds=1)),               # delivered twice
                           news(5, now + timedelta(seconds=1), symbols=("SYNA", "SYNB"))])
        await service.queue.join()
        on_disconnected()
        seen["during_outage"] = utcnow()
        await asyncio.sleep(0.05)
        await on_subscribed()  # reconnected: no recovery, the news cutoff moves to now
        seen["reconnected"] = service.alpaca_live_cutoff
        await on_articles([news(3, seen["during_outage"]),                   # published while disconnected
                           news(4, utcnow() + timedelta(seconds=1))])        # newly published
        await service.queue.join()
        raise StopScenario

    monkeypatch.setattr(client, "run_forever", scripted_stream)
    started = utcnow()
    with pytest.raises(StopScenario):
        asyncio.run(cli.run_service(service, enable_sec=False, enable_alpaca=True, once=False))

    saved = sorted(p.parent.name for p in (settings.root / "processed" / "alpaca").glob("*/*.json"))
    assert saved == ["2", "4", "5"]  # new articles only, each once
    assert service.stats["before_live_cutoff_alpaca"] == 2  # the old article's update, the one from the outage
    assert service.stats["skipped_in_flight"] + service.stats["alpaca_skip_unchanged_delivery"] == 1
    assert service.alpaca_session.calls == []  # no REST backfill at startup or after the reconnect
    assert started <= service.live_cutoff < seen["reconnected"] and seen["during_outage"] < seen["reconnected"]
    assert service.ledger.get_cursor(ALPACA_CURSOR) == old_cursor  # saved cursor untouched
    status = json.loads((settings.root / "state" / "worker_status.json").read_text(encoding="utf-8"))
    assert status["collection_mode"] == "live_only" and status["state"] == "stopped"
    assert status["live_cutoff"]["alpaca"] == seen["reconnected"].isoformat()


def test_every_startup_takes_a_fresh_cutoff_and_switching_the_mode_off_resumes_saved_recovery(tmp_path):
    settings = live_settings(tmp_path)
    first = IngestionService(settings)
    first.startup()
    cursor = utcnow() - timedelta(hours=2)
    first.ledger.set_cursor(ALPACA_CURSOR, cursor)
    first.ledger.set_value(f"sec_scope_floor:{OLD}", "2026-01-01")
    before = json.loads(json.dumps(first.ledger.data["cursors"]))
    one = first.begin_live_only()

    time.sleep(0.01)
    second = IngestionService(settings)  # a restart
    second.startup()
    two = second.begin_live_only()
    assert one < two  # a fresh cutoff each time, never saved
    assert second.ledger.data["cursors"] == before  # nothing about the cutoff is written

    recovering = IngestionService(live_settings(tmp_path, live_only=False))  # live-only switched off later
    recovering.startup()
    assert recovering.begin_live_only() is None and recovering.live_cutoff is None
    start, _end = recovering.backfill_window()
    assert start == cursor - timedelta(minutes=settings.alpaca_reconnect_overlap_minutes)  # resumes from the cursor
    assert recovering.ledger.get_value(f"sec_scope_floor:{OLD}") == "2026-01-01"


def test_the_startup_summary_describes_the_mode_and_the_kept_recovery_state(tmp_path):
    settings = live_settings(tmp_path)
    service = IngestionService(settings)
    service.startup()
    service.ledger.set_cursor(ALPACA_CURSOR, datetime(2034, 1, 1, tzinfo=timezone.utc))
    service.ledger.set_value(f"sec_scope_floor:{OLD}", "2033-12-01")
    text = recovery_summary(settings)
    assert text.startswith("OFF (INGEST_LIVE_ONLY=true)") and "no Alpaca REST backfill" in text
    assert "Alpaca cursor 2034-01-01T00:00:00+00:00, SEC scope floor(s) for 1 company" in text
    assert recovery_summary(live_settings(tmp_path, live_only=False)).startswith("at startup: Alpaca news since")
