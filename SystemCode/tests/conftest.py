"""Shared offline test helpers: fake HTTP sessions, settings and SEC rows."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from datasources.ingest.sec_client import RateLimiter, SecClient
from datasources.ingest.settings import IngestSettings

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
REAL_SUBMISSIONS = ROOT / "data" / "submissions_CIK0001045810.json"
REAL_8K_DIR = ROOT / "data" / "8k"
SYNTHETIC_8K = FIXTURES / "synthetic_8k_financial.htm"


def pytest_configure(config):
    config.addinivalue_line("markers", "live: opt-in tests that call real SEC/Alpaca APIs (RUN_LIVE_TESTS=1)")


class FakeResponse:
    def __init__(self, status: int = 200, body: bytes | str | dict | list = b"", headers: dict | None = None):
        if isinstance(body, (dict, list)):
            body = json.dumps(body)
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.status_code = status
        self.body = body
        self.headers = headers or {}
        self.closed = False

    def iter_content(self, chunk_size: int = 65536):
        for start in range(0, len(self.body), chunk_size):
            yield self.body[start:start + chunk_size]

    def close(self):
        self.closed = True

    def json(self):
        return json.loads(self.body)

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    """Routes GET requests by URL (prefix match) to queued FakeResponses."""

    def __init__(self, routes: dict[str, list[FakeResponse] | FakeResponse] | None = None):
        self.routes: dict[str, list[FakeResponse]] = {}
        for url, responses in (routes or {}).items():
            self.add(url, responses)
        self.calls: list[dict[str, Any]] = []

    def add(self, url: str, responses):
        self.routes.setdefault(url, []).extend(responses if isinstance(responses, list) else [responses])

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        for prefix, queue in self.routes.items():
            if url.startswith(prefix):
                if not queue:
                    raise AssertionError(f"no more fake responses for {url}")
                return queue.pop(0) if len(queue) > 1 else queue[0]
        raise AssertionError(f"unexpected request to {url}")

    def urls(self) -> list[str]:
        return [call["url"] for call in self.calls]


def html_response(body: bytes | str, status: int = 200) -> FakeResponse:
    return FakeResponse(status, body, {"Content-Type": "text/html; charset=utf-8"})


def json_response(body, status: int = 200) -> FakeResponse:
    return FakeResponse(status, body, {"Content-Type": "application/json"})


def make_sec_client(session: FakeSession, sleeps: list[float] | None = None, **kwargs) -> SecClient:
    record = sleeps if sleeps is not None else []
    limiter = RateLimiter(1000.0, sleep=record.append)
    return SecClient("TestSuite tests@example.test", session=session, limiter=limiter,
                     sleep=record.append, **kwargs)


@pytest.fixture
def settings(tmp_path) -> IngestSettings:
    return IngestSettings(symbols=["NVDA"], output_root=tmp_path / "output", sec_user_agent="TestSuite tests@example.test",
                          sec_poll_interval_seconds=60, sec_initial_lookback_days=100000,
                          sec_recovery_max_days=100000)


TEST_PUBLISHER = "pytest-synthetic-wire"


def cleanup_sample_rows(database) -> None:
    """Delete every sample (is_sample) row the database tests may have written, and nothing else.

    Live rows are never touched: every statement is restricted to sample
    documents, their revisions and sample-partition events. Hashing-backend test
    profiles are removed only when nothing references them any more.
    """
    with database.transaction() as conn:
        present = conn.execute("SELECT to_regclass('public.news_event') IS NOT NULL AS ok").fetchone()["ok"]
        conn.execute("CREATE TEMP TABLE sample_rev ON COMMIT DROP AS SELECT r.revision_id FROM document_revision r "
                     "JOIN document d ON d.document_id = r.document_id WHERE d.is_sample")
        if present:
            conn.execute("CREATE TEMP TABLE sample_event ON COMMIT DROP AS "
                         "SELECT event_id FROM news_event WHERE is_sample")
            conn.execute("DELETE FROM event_membership_unit WHERE membership_id IN (SELECT membership_id FROM "
                         "event_membership WHERE revision_id IN (SELECT revision_id FROM sample_rev) "
                         "OR event_id IN (SELECT event_id FROM sample_event))")
            conn.execute("DELETE FROM event_membership WHERE revision_id IN (SELECT revision_id FROM sample_rev) "
                         "OR event_id IN (SELECT event_id FROM sample_event)")
            conn.execute("DELETE FROM retrieval_candidate WHERE candidate_revision_id IN (SELECT revision_id FROM "
                         "sample_rev) OR analysis_id IN (SELECT analysis_id FROM revision_analysis WHERE "
                         "revision_id IN (SELECT revision_id FROM sample_rev))")
            conn.execute("DELETE FROM event_comparison WHERE revision_id IN (SELECT revision_id FROM sample_rev) "
                         "OR candidate_event_id IN (SELECT event_id FROM sample_event)")
            conn.execute("UPDATE news_event SET superseded_by = NULL, status = 'provisional' "
                         "WHERE is_sample AND superseded_by IS NOT NULL")
            conn.execute("DELETE FROM news_event WHERE is_sample")
            conn.execute("DELETE FROM revision_analysis WHERE revision_id IN (SELECT revision_id FROM sample_rev)")
        conn.execute("DELETE FROM embedding WHERE unit_id IN (SELECT unit_id FROM text_unit "
                     "WHERE revision_id IN (SELECT revision_id FROM sample_rev))")
        conn.execute("DELETE FROM text_unit WHERE revision_id IN (SELECT revision_id FROM sample_rev)")
        conn.execute("DELETE FROM document_revision_source WHERE revision_id IN (SELECT revision_id FROM sample_rev)")
        conn.execute("DELETE FROM revision_company WHERE revision_id IN (SELECT revision_id FROM sample_rev)")
        conn.execute("DELETE FROM sec_filing_details WHERE revision_id IN (SELECT revision_id FROM sample_rev)")
        conn.execute("UPDATE document SET current_revision_id = NULL WHERE is_sample = true")
        conn.execute("DELETE FROM document_revision WHERE revision_id IN (SELECT revision_id FROM sample_rev)")
        conn.execute("DELETE FROM document WHERE is_sample = true")
        # Test publishers are named "pytest-..." (never a real outlet); only unreferenced ones go.
        conn.execute("DELETE FROM publisher p WHERE p.canonical_key LIKE 'pytest-%%' AND NOT EXISTS "
                     "(SELECT 1 FROM document_revision r WHERE r.publisher_id = p.publisher_id)")
        referenced = ["SELECT 1 FROM text_unit t WHERE t.profile_id = p.profile_id"]
        if present:
            referenced += ["SELECT 1 FROM revision_analysis a WHERE a.profile_id = p.profile_id",
                           "SELECT 1 FROM news_event e WHERE e.profile_id = p.profile_id"]
        conn.execute("DELETE FROM embedding_profile p WHERE p.model_provider = 'datasources' AND "
                     + " AND ".join(f"NOT EXISTS ({q})" for q in referenced))


def synthetic_submissions(rows: list[dict], cik: str = "9999999", name: str = "EXAMPLE SYNTHETIC CORP") -> dict:
    """SYNTHETIC submissions JSON with aligned filings.recent arrays."""
    keys = ["accessionNumber", "filingDate", "reportDate", "acceptanceDateTime", "act", "form", "fileNumber",
            "filmNumber", "items", "core_type", "size", "isXBRL", "isInlineXBRL", "isXBRLNumeric",
            "primaryDocument", "primaryDocDescription"]
    defaults = {"act": "34", "fileNumber": "000-00000", "filmNumber": "1", "core_type": "XBRL", "size": 1000,
                "isXBRL": 1, "isInlineXBRL": 1, "isXBRLNumeric": 0, "primaryDocDescription": "8-K",
                "reportDate": "", "items": "8.01", "acceptanceDateTime": "2026-09-01T12:00:00.000Z"}
    recent = {key: [row.get(key, defaults.get(key, "")) for row in rows] for key in keys}
    return {"cik": cik.zfill(10), "name": name, "tickers": ["SYNX"], "filings": {"recent": recent, "files": []}}
