"""SEC submissions parsing, archive URLs, the HTTP client and text extraction (offline)."""

from __future__ import annotations

import json

import pytest

from conftest import (
    REAL_8K_DIR,
    REAL_SUBMISSIONS,
    SYNTHETIC_8K,
    FakeResponse,
    FakeSession,
    html_response,
    json_response,
    make_sec_client,
    synthetic_submissions,
)
from datasources import sec
from datasources.ingest.cli import live_collectors
from datasources.ingest.htmltext import html_to_text, sec_archive_links
from datasources.ingest.sec_client import RateLimiter, SecFetchError
from datasources.ingest.settings import IngestSettings, load_settings

BASE = "https://www.sec.gov/Archives/edgar/data/9999999/000999999926000001/synthetic-8k.htm"


# ---------------------------------------------------------------- aligned arrays
def test_filing_rows_reads_real_submissions_as_aligned_arrays():
    data = json.loads(REAL_SUBMISSIONS.read_text())
    rows = sec.filing_rows(sec.filing_columns(data), forms={"8-K"})
    assert rows and all(r["form"] == "8-K" for r in rows)
    first = next(r for r in rows if r["accessionNumber"] == "0001045810-26-000073")
    assert first["primaryDocument"] == "nvda-20260826.htm"
    assert first["items"] == "2.02,9.01"
    assert first["acceptanceDateTime"] == "2026-08-26T20:21:19.000Z"
    assert first["reportDate"] == "2026-08-26"


def test_misaligned_required_array_is_rejected_not_truncated():
    data = synthetic_submissions([{"accessionNumber": "0009999999-26-000001", "form": "8-K", "filingDate": "2026-09-01",
                                   "primaryDocument": "a.htm"},
                                  {"accessionNumber": "0009999999-26-000002", "form": "8-K", "filingDate": "2026-09-02",
                                   "primaryDocument": "b.htm"}])
    data["filings"]["recent"]["primaryDocument"].pop()
    with pytest.raises(sec.SubmissionsFormatError, match="primaryDocument"):
        sec.filing_rows(sec.filing_columns(data))


def test_misaligned_optional_array_becomes_null_with_warning():
    data = synthetic_submissions([{"accessionNumber": "0009999999-26-000001", "form": "8-K", "filingDate": "2026-09-01",
                                   "primaryDocument": "a.htm"},
                                  {"accessionNumber": "0009999999-26-000002", "form": "8-K", "filingDate": "2026-09-02",
                                   "primaryDocument": "b.htm"}])
    data["filings"]["recent"]["items"].pop()
    del data["filings"]["recent"]["reportDate"]
    warnings: list[str] = []
    rows = sec.filing_rows(sec.filing_columns(data), warnings=warnings)
    assert len(rows) == 2
    assert all(r["items"] is None for r in rows)
    assert any("items" in w for w in warnings)


def test_amendments_are_included_when_configured_and_kept_separate():
    data = synthetic_submissions([
        {"accessionNumber": "0009999999-26-000001", "form": "8-K", "filingDate": "2026-09-01", "primaryDocument": "a.htm"},
        {"accessionNumber": "0009999999-26-000002", "form": "8-K/A", "filingDate": "2026-09-02", "primaryDocument": "a.htm"},
        {"accessionNumber": "0009999999-26-000003", "form": "10-Q", "filingDate": "2026-09-03", "primaryDocument": "q.htm"},
    ])
    rows = sec.filing_rows(sec.filing_columns(data), forms={"8-K", "8-K/A"})
    assert [r["accessionNumber"] for r in rows] == ["0009999999-26-000001", "0009999999-26-000002"]
    assert [r["form"] for r in rows] == ["8-K", "8-K/A"]


def test_historical_submissions_file_shape_is_supported():
    recent = synthetic_submissions([{"accessionNumber": "0009999999-19-000001", "form": "8-K",
                                     "filingDate": "2019-01-01", "primaryDocument": "old.htm"}])["filings"]["recent"]
    rows = sec.filing_rows(sec.filing_columns(recent))
    assert rows[0]["accessionNumber"] == "0009999999-19-000001"


# ---------------------------------------------------------------- URLs
def test_archive_urls_follow_edgar_conventions():
    cik, accession = "0001045810", "0001045810-26-000073"
    assert sec.SUBMISSIONS_URL.format(cik=cik) == "https://data.sec.gov/submissions/CIK0001045810.json"
    assert sec.archive_directory_url(cik, accession) == \
        "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073"
    assert sec.filing_index_url(cik, accession) == \
        "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/0001045810-26-000073-index.htm"
    assert sec.primary_document_url(cik, accession, "nvda-20260826.htm") == \
        "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/nvda-20260826.htm"


@pytest.mark.parametrize("name", ["../secret.htm", "/etc/passwd", "https://evil.test/x.htm", "", "a b.htm"])
def test_invalid_primary_document_names_are_rejected(name):
    with pytest.raises(ValueError):
        sec.primary_document_url("0001045810", "0001045810-26-000073", name)


def test_parse_8k_keeps_ui_wrapper_shape():
    data = json.loads(REAL_SUBMISSIONS.read_text())
    record = sec.parse_8k(data, "0001045810", ticker="NVDA", limit=1)[0]
    assert {"cik", "ticker", "filing_index_url", "primary_doc_url", "accessionNumber"} <= set(record)


# ---------------------------------------------------------------- client
def test_rate_limiter_spaces_requests():
    now = [0.0]
    sleeps: list[float] = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    limiter = RateLimiter(2.0, clock=lambda: now[0], sleep=sleep)
    for _ in range(3):
        limiter.acquire()
    assert sleeps == [0.5, 0.5]


def test_retry_after_is_honoured_then_succeeds():
    session = FakeSession({"https://data.sec.gov/submissions/": [
        FakeResponse(429, b"slow down", {"Retry-After": "7"}),
        json_response({"cik": "0001045810", "filings": {"recent": {}}}),
    ]})
    sleeps: list[float] = []
    client = make_sec_client(session, sleeps)
    data, result = client.fetch_submissions("1045810", 10_000)
    assert data["cik"] == "0001045810"
    assert 7 in sleeps
    assert result.url == "https://data.sec.gov/submissions/CIK0001045810.json"
    assert session.calls[0]["headers"]["User-Agent"] == "TestSuite tests@example.test"


def test_sec_http_timeout_is_configurable_and_used_by_live_client(monkeypatch):
    monkeypatch.setenv("SEC_HTTP_TIMEOUT_SECONDS", "75")
    settings = load_settings()
    assert settings.sec_http_timeout_seconds == 75

    client, _ = live_collectors(
        IngestSettings(sec_user_agent="TestSuite tests@example.test", sec_http_timeout_seconds=75),
        enable_sec=True,
        enable_alpaca=False,
    )
    assert client is not None
    assert client.timeout == 75


def test_server_errors_use_bounded_retries():
    session = FakeSession({"https://www.sec.gov/": [html_response("oops", 503)]})
    client = make_sec_client(session, max_retries=2)
    with pytest.raises(SecFetchError) as info:
        client.fetch_document(BASE, 10_000)
    assert info.value.retryable is True
    assert len(session.calls) == 3


def test_403_is_not_retried():
    session = FakeSession({"https://www.sec.gov/": [html_response("Forbidden", 403)]})
    client = make_sec_client(session)
    with pytest.raises(SecFetchError) as info:
        client.fetch_document(BASE, 10_000)
    assert info.value.kind == "access_denied"
    assert len(session.calls) == 1


def test_error_page_with_200_is_not_treated_as_content():
    page = "<html><body><h1>Request Rate Threshold Exceeded</h1><p>Your Request Originates from an Undeclared Automated Tool</p></body></html>"
    client = make_sec_client(FakeSession({"https://www.sec.gov/": [html_response(page)]}))
    with pytest.raises(SecFetchError) as info:
        client.fetch_document(BASE, 10_000)
    assert info.value.kind == "error_page"


def test_redirect_off_sec_hosts_is_rejected():
    session = FakeSession({"https://www.sec.gov/": [FakeResponse(302, b"", {"Location": "https://evil.example.test/x.htm"})]})
    with pytest.raises(SecFetchError) as info:
        make_sec_client(session).fetch_document(BASE, 10_000)
    assert info.value.kind == "invalid_url"


def test_redirect_within_sec_is_followed():
    target = "https://www.sec.gov/Archives/edgar/data/9999999/000999999926000001/moved.htm"
    session = FakeSession({
        BASE: [FakeResponse(301, b"", {"Location": "/Archives/edgar/data/9999999/000999999926000001/moved.htm"})],
        target: [html_response(SYNTHETIC_8K.read_bytes())],
    })
    result = make_sec_client(session).fetch_document(BASE, 1_000_000)
    assert result.url == target


def test_oversized_and_wrong_type_responses_are_rejected():
    big = FakeSession({"https://www.sec.gov/": [html_response(b"x" * 5000)]})
    with pytest.raises(SecFetchError, match="exceeded"):
        make_sec_client(big).fetch_document(BASE, 1000)
    pdf = FakeSession({"https://www.sec.gov/": [FakeResponse(200, b"%PDF", {"Content-Type": "application/pdf"})]})
    with pytest.raises(SecFetchError) as info:
        make_sec_client(pdf).fetch_document(BASE, 1000)
    assert info.value.kind == "bad_content_type"


def test_non_sec_document_url_is_refused_before_any_request():
    session = FakeSession()
    with pytest.raises(SecFetchError):
        make_sec_client(session).fetch_document("https://example.test/doc.htm", 1000)
    assert session.calls == []


# ---------------------------------------------------------------- extraction
def test_synthetic_8k_extraction_preserves_financial_tables_and_visible_ixbrl():
    result = html_to_text(SYNTHETIC_8K.read_text(encoding="utf-8"), base_url=BASE, sec_document=True)
    text = result.text
    for gone in ("STYLE_SHOULD_NOT_APPEAR", "SCRIPT_SHOULD_NOT_APPEAR", "HIDDEN_XBRL_SHOULD_NOT_APPEAR",
                 "HIDDEN_SPAN_SHOULD_NOT_APPEAR", "COMMENT_SHOULD_NOT_APPEAR"):
        assert gone not in text
    assert "FORM 8-K" in text  # visible ix:nonNumeric
    assert "revenue of $46,743 million, up 56.2% year over year" in text  # visible ix:nonFraction
    assert "did not record an impairment" in text  # negation kept
    assert "€1,020 million" in text  # entity decoded, currency kept
    assert "August 26, 2026" in text  # nbsp normalised
    assert "Revenue | $46,743 | $30,040 | 56%" in text
    assert "Other income (expense) | $(1,234) | $512 | (341)%" in text
    assert "- First highlight with 1,500 units\n- Second highlight" in text
    assert "## Item 2.02 Results of Operations and Financial Condition." in text
    assert result.section_headings[:2] == ["Item 2.02 Results of Operations and Financial Condition.",
                                           "Item 9.01 Financial Statements and Exhibits."]
    assert result.table_count == 2
    assert result.inline_xbrl_fact_count == 5  # hidden header facts not counted


def test_exhibit_links_are_resolved_and_limited_to_sec_archives():
    result = html_to_text(SYNTHETIC_8K.read_text(encoding="utf-8"), base_url=BASE, sec_document=True)
    links = [url for url, _ in sec_archive_links(result.links, BASE)]
    assert links == [
        "https://www.sec.gov/Archives/edgar/data/9999999/000999999926000001/ex991.htm",
        "https://www.sec.gov/Archives/edgar/data/9999999/000999999926000001/ex992.htm",
    ]


def test_real_nvidia_8k_extraction():
    html = (REAL_8K_DIR / "0001045810-26-000073-nvda-20260826.htm").read_text(encoding="utf-8")
    url = "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/nvda-20260826.htm"
    result = html_to_text(html, base_url=url, sec_document=True)
    assert "Item 2.02 Results of Operations and Financial Condition." in result.section_headings
    assert "Common Stock, $0.001 par value per share | NVDA | The Nasdaq Global Select Market" in result.text
    assert "99.1 | Press Release, dated August 26, 2026" in result.text
    assert "0001045810" not in result.text  # hidden dei:EntityCentralIndexKey removed
    texts = dict(sec_archive_links(result.links, url))
    assert texts["https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/q2fy27pr.htm"] ==         'Press Release, dated August 26, 2026, entitled "NVIDIA Announces Financial Results for Second Quarter Fiscal 2027"'
    exhibits = [u for u, _ in sec_archive_links(result.links, url)]
    assert exhibits == [
        "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/q2fy27pr.htm",
        "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/q2fy27cfocommentary.htm",
    ]


def test_extraction_is_never_truncated():
    paragraphs = "".join(f"<p>Paragraph {i} with value ${i},000.</p>" for i in range(20000))
    text = html_to_text(f"<html><body>{paragraphs}</body></html>").text
    assert text.endswith("Paragraph 19999 with value $19999,000.")
    assert text.count("Paragraph ") == 20000
