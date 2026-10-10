"""LangGraph prepare_data node, adapters and payload unwrapping (offline)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from conftest import FIXTURES, REAL_8K_DIR, REAL_SUBMISSIONS, ROOT, SYNTHETIC_8K
from datasources.ingest.adapters import normalize_symbols, parse_timestamp, unwrap_payload
from datasources.ingest.graph import build_graph, get_graph, run_prepare
from datasources.ingest.schema import SCHEMA_VERSION, SecContext

RETRIEVED = datetime(2026, 9, 22, 1, 2, 3, tzinfo=timezone.utc)
WS_BATCH = json.loads((FIXTURES / "synthetic_alpaca_ws_batch.json").read_text(encoding="utf-8"))


def sec_envelope(row: dict | None = None, html: str | None = None, **overrides) -> dict:
    row = row or {"accessionNumber": "0009999999-26-000001", "form": "8-K", "filingDate": "2026-08-26",
                  "reportDate": "2026-08-26", "acceptanceDateTime": "2026-08-26T20:21:19.000Z",
                  "items": "2.02,9.01", "primaryDocument": "synthetic-8k.htm", "primaryDocDescription": "8-K"}
    envelope = {
        "provider": "sec_edgar", "transport": "rest",
        "api_endpoint": "https://data.sec.gov/submissions/CIK0009999999.json",
        "retrieved_at": RETRIEVED, "requested_symbols": ["SYNX"], "is_sample": False,
        "raw_record": row, "raw_file_refs": ["raw/sec/submissions/CIK0009999999/abc.json"],
        "sec_context": {"cik": "9999999", "company_name": "EXAMPLE SYNTHETIC CORP", "tickers": ["synx"]},
        "primary_document_html": SYNTHETIC_8K.read_text(encoding="utf-8") if html is None else html,
    }
    envelope.update(overrides)
    return envelope


def alpaca_envelope(article: dict, transport: str = "websocket", **overrides) -> dict:
    envelope = {
        "provider": "alpaca", "transport": transport,
        "api_endpoint": "wss://stream.data.alpaca.markets/v1beta1/news",
        "retrieved_at": RETRIEVED, "requested_symbols": ["NVDA"], "is_sample": False,
        "raw_record": article, "raw_file_refs": ["raw/alpaca/websocket/20260922.jsonl"],
    }
    envelope.update(overrides)
    return envelope


def test_graph_is_compiled_once_and_has_the_analysis_nodes():
    assert get_graph() is get_graph()
    drawn = build_graph().get_graph()
    assert set(drawn.nodes) == {"__start__", "prepare_data", "create_text_units", "generate_embeddings",
                                "retrieve_candidates", "route_candidates", "propose_new_event", "compare_with_llm",
                                "resolve_event_memberships", "persist_result", "route_event_summary",
                                "generate_event_summary", "validate_event_summary", "persist_event_summary",
                                "flag_conflict_review", "skip_summary", "__end__"}
    edges = {(e.source, e.target, e.conditional) for e in drawn.edges}
    assert edges == {
        ("__start__", "prepare_data", False),
        ("prepare_data", "create_text_units", True),
        ("prepare_data", "__end__", True),  # a batch with no valid record
        ("create_text_units", "generate_embeddings", False),
        ("generate_embeddings", "retrieve_candidates", False),
        ("retrieve_candidates", "route_candidates", False),
        ("route_candidates", "propose_new_event", True),  # no plausible candidates
        ("route_candidates", "compare_with_llm", True),  # plausible candidates
        ("route_candidates", "resolve_event_memberships", True),  # nothing to decide
        ("propose_new_event", "resolve_event_memberships", False),
        ("compare_with_llm", "resolve_event_memberships", False),
        ("resolve_event_memberships", "persist_result", False),
        ("persist_result", "route_event_summary", False),  # after the record's commit
        ("route_event_summary", "generate_event_summary", True),  # first summary / new information / retry
        ("route_event_summary", "flag_conflict_review", True),  # open conflict: keep the published summary
        ("route_event_summary", "skip_summary", True),  # repeated / uncertain: nothing to summarize
        ("route_event_summary", "create_text_units", True),  # every affected event done: next record
        ("route_event_summary", "__end__", True),
        ("generate_event_summary", "validate_event_summary", False),
        ("validate_event_summary", "persist_event_summary", False),
        ("persist_event_summary", "route_event_summary", False),  # next affected event (multi-Item filings)
        ("flag_conflict_review", "route_event_summary", False),
        ("skip_summary", "route_event_summary", False),
    }


def test_sec_record_common_schema_and_provenance():
    (record,), errors = run_prepare([sec_envelope()])
    assert errors == []
    assert record.schema_version == SCHEMA_VERSION
    assert record.record_type == "sec_filing" and record.provider == "sec_edgar"
    assert record.record_id == "sec_edgar:0009999999:0009999999-26-000001"
    assert record.issuer_name == "EXAMPLE SYNTHETIC CORP" and record.publisher is None
    assert record.symbols == ["SYNX"]
    assert record.title_is_generated is True
    assert record.title == "EXAMPLE SYNTHETIC CORP Form 8-K filed 2026-08-26 (Items 2.02, 9.01)"
    assert record.content_scope == "primary_document_only"
    assert record.published_at == datetime(2026, 8, 26, 20, 21, 19, tzinfo=timezone.utc)
    assert record.sec_metadata.filing_date == "2026-08-26" and record.sec_metadata.report_date == "2026-08-26"
    assert record.primary_document_url == "https://www.sec.gov/Archives/edgar/data/9999999/000999999926000001/synthetic-8k.htm"
    assert record.original_url == record.primary_document_url
    assert record.filing_index_url.endswith("/000999999926000001/0009999999-26-000001-index.htm")
    assert record.exhibit_urls[0].endswith("/ex991.htm")
    assert record.sec_metadata.exhibits_extracted is False
    assert record.provenance.api_endpoint == "https://data.sec.gov/submissions/CIK0009999999.json"
    assert record.raw_file_refs == ["raw/sec/submissions/CIK0009999999/abc.json"]
    assert record.analysis_text.startswith("[SEC EDGAR filing metadata]\nIssuer: EXAMPLE SYNTHETIC CORP (CIK 0009999999)")
    assert "[Primary document text]\nFORM 8-K" in record.analysis_text
    assert record.body_text in record.analysis_text


def test_sec_date_only_acceptance_is_not_turned_into_timestamp():
    row = {"accessionNumber": "0009999999-26-000001", "form": "8-K", "filingDate": "2026-08-26",
           "acceptanceDateTime": "2026-08-26", "primaryDocument": "synthetic-8k.htm"}
    (record,), _ = run_prepare([sec_envelope(row)])
    assert record.published_at is None
    assert any("date-only" in w for w in record.warnings)


def test_sec_missing_or_empty_document_is_a_retryable_error():
    records, errors = run_prepare([sec_envelope(html=""), sec_envelope(html="<html><body><div style='display:none'>x</div></body></html>")])
    assert records == []
    assert [e.stage for e in errors] == ["download", "extract"]
    assert all(e.retryable for e in errors)


def test_sec_mismatched_primary_url_is_rejected():
    _, errors = run_prepare([sec_envelope(primary_document_url="https://www.sec.gov/Archives/edgar/data/1/2/other.htm")])
    assert errors[0].stage == "validate"


def test_malformed_record_does_not_discard_batch():
    good = alpaca_envelope(WS_BATCH[2])
    malformed_article = alpaca_envelope({"T": "n", "headline": "no id"})
    not_an_envelope = {"provider": "unknown"}
    records, errors = run_prepare([malformed_article, good, not_an_envelope, sec_envelope()])
    assert [r.source_record_id for r in records] == ["91000001", "0009999999-26-000001"]
    assert sorted(e.index for e in errors) == [0, 2]


def test_alpaca_ws_record_cleaning_and_attribution():
    (record,), _ = run_prepare([alpaca_envelope(WS_BATCH[2])])
    assert record.publisher == "benzinga"  # article source, never "alpaca_live"
    assert record.provider == "alpaca"
    assert record.title == "Synthetic Co & NVIDIA announce test partnership"
    assert record.title_is_generated is False
    assert record.summary_text == "Synthetic summary: revenue won't decline, per the test fixture."
    assert record.body_text == ("First paragraph with revenue of $1.2 billion, up 12%.\n\n"
                                "- Point one\n- Point two is not negative\n\nQ1 | $(3.4)")
    assert record.symbols == ["NVDA", "BRK.B"]
    assert record.published_at == datetime(2026, 9, 21, 14, 0, tzinfo=timezone.utc)
    assert record.updated_at == datetime(2026, 9, 21, 14, 5, tzinfo=timezone.utc)
    assert record.original_url == "https://news.example.test/synthetic-nvda-partnership"
    assert record.provenance.transport == "websocket"
    assert record.content_status == "full_text"
    assert record.analysis_text.startswith("Synthetic Co & NVIDIA announce test partnership\n\nSynthetic summary")


def test_alpaca_missing_content_and_invalid_timestamp_stay_visible():
    (record,), _ = run_prepare([alpaca_envelope(WS_BATCH[3])])
    assert record.body_text is None and record.summary_text is None
    assert record.content_status == "headline_only"
    assert record.updated_at is None
    assert any("invalid timestamp 'not-a-timestamp'" in w for w in record.warnings)
    assert record.published_at == datetime(2026, 9, 21, 19, 0, tzinfo=timezone.utc)  # -04:00 -> UTC
    assert record.author is None


def test_api_wrapper_publisher_is_article_source_not_provenance_tag():
    wrapper = {"source": "alpaca_news", "live": True, "records": [{**WS_BATCH[2], "T": None, "_source": "alpaca_live"}]}
    rows, info = unwrap_payload(wrapper, provider="alpaca")
    assert info["shape"] == "api_alpaca_wrapper"
    article = {k: v for k, v in rows[0][0].items() if k != "T"}
    (record,), _ = run_prepare([alpaca_envelope(article, transport="rest")])
    assert record.publisher == "benzinga"


def test_unwrap_ws_batch_ignores_operational_messages():
    rows, info = unwrap_payload(WS_BATCH, provider="alpaca")
    assert [r["id"] for r, _ in rows] == [91000001, 91000002]
    assert info == {"shape": "alpaca_websocket_batch", "operational": 2}


def test_unwrap_a_single_article_object_as_a_saved_article_file():
    article = {"id": 61837011, "headline": "h", "summary": "s", "content": "<p>c</p>", "symbols": ["NVDA"]}
    assert unwrap_payload(article, provider="alpaca") == ([(article, None)], {"shape": "alpaca_article"})
    message = {**article, "T": "n"}  # one WebSocket news message on its own
    assert unwrap_payload(message, provider="alpaca")[1] == {"shape": "alpaca_article"}
    for not_an_article in ({"T": "success", "msg": "authenticated"}, {"id": 1}, {"headline": "no id"}):
        with pytest.raises(ValueError, match="not a recognised Alpaca shape"):
            unwrap_payload(not_an_article, provider="alpaca")


def test_unwrap_rest_page_and_data_dictionary():
    page = json.loads((FIXTURES / "synthetic_alpaca_rest_page1.json").read_text())
    rows, info = unwrap_payload(page, provider="alpaca")
    assert info["shape"] == "alpaca_rest" and rows[0][0]["id"] == 92000001
    datadict = json.loads((ROOT / "data" / "submissions_CIK0001045810_datadict.json").read_text())
    assert unwrap_payload(datadict, provider="sec_edgar") == ([], {"shape": "data_dictionary", "ignored": True})


def test_unwrap_raw_submissions_and_api_edgar_wrapper():
    submissions = json.loads(REAL_SUBMISSIONS.read_text())
    rows, info = unwrap_payload(submissions, provider="sec_edgar", sec_forms={"8-K"})
    assert info["shape"] == "sec_submissions"
    row, ctx = rows[0]
    assert ctx == SecContext(cik="0001045810", company_name="NVIDIA CORP", tickers=["NVDA"])
    wrapper = {"source": "sec_edgar", "cik": "0001045810",
               "records": [{**row, "cik": "0001045810", "ticker": "NVDA", "filing_index_url": "x", "primary_doc_url": "y"}]}
    wrapped_rows, info = unwrap_payload(wrapper, provider="sec_edgar")
    assert info["shape"] == "api_edgar_wrapper"
    assert wrapped_rows[0][0] == row and wrapped_rows[0][1].tickers == ["NVDA"]


def test_real_nvidia_filing_through_graph():
    submissions = json.loads(REAL_SUBMISSIONS.read_text())
    rows, _ = unwrap_payload(submissions, provider="sec_edgar", sec_forms={"8-K"})
    row, ctx = next((r, c) for r, c in rows if r["accessionNumber"] == "0001045810-26-000073")
    html = (REAL_8K_DIR / "0001045810-26-000073-nvda-20260826.htm").read_text(encoding="utf-8")
    envelope = sec_envelope(row, html, sec_context=ctx.model_dump(),
                            api_endpoint="https://data.sec.gov/submissions/CIK0001045810.json")
    (record,), errors = run_prepare([envelope])
    assert not errors
    assert record.sec_metadata.item_descriptions["2.02"] == "Results of Operations and Financial Condition"
    assert record.published_at.isoformat() == "2026-08-26T20:21:19+00:00"
    assert len(record.exhibit_urls) == 2


def test_identical_text_different_ids_are_separate_records():
    a = {**WS_BATCH[2], "id": 1}
    b = {**WS_BATCH[2], "id": 2}
    records, _ = run_prepare([alpaca_envelope(a), alpaca_envelope(b)])
    assert [r.source_record_id for r in records] == ["1", "2"]
    assert records[0].body_text == records[1].body_text
    assert records[0].record_id != records[1].record_id


def test_helpers_do_not_guess():
    assert parse_timestamp("2026-09-21T10:00:00", "x")[0] is None  # no timezone
    assert parse_timestamp("2026-09-21T10:00:00+02:00", "x")[0] == datetime(2026, 9, 21, 8, tzinfo=timezone.utc)
    symbols, warnings = normalize_symbols(["brk.b", "BTC/USD", "bad symbol", "", 5])
    assert symbols == ["BRK.B", "BTC/USD"] and len(warnings) == 2
