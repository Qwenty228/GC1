"""Offline tests for the Alpaca real-time news client."""

from __future__ import annotations

import json

import pytest

from alpaca_news.client import (
    MissingCredentialsError,
    handle_messages,
    load_settings,
    parse_message_batch,
    parse_symbols,
    subscription_message,
)


ARTICLE = {
    "T": "n",
    "id": 123,
    "headline": "NVIDIA news",
    "summary": "A summary",
    "author": "Reporter",
    "created_at": "2026-09-21T10:00:00Z",
    "updated_at": "2026-09-21T10:01:00Z",
    "content": "Article content",
    "url": "https://example.test/article",
    "symbols": ["NVDA"],
    "source": "example",
}


def test_parses_valid_alpaca_news_message():
    assert parse_message_batch(json.dumps([ARTICLE])) == [ARTICLE]


def test_handles_multiple_messages_in_one_json_array():
    received: list[dict] = []
    raw = json.dumps([{"T": "success", "msg": "connected"}, ARTICLE])

    assert handle_messages(raw, set(), received.append) == 1
    assert received[0]["headline"] == "NVIDIA news"


def test_unknown_message_type_is_ignored_safely(caplog):
    assert handle_messages('[{"T":"unexpected"}]', set()) == 0
    assert "unknown Alpaca message type" in caplog.text


def test_missing_credentials_are_rejected(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    monkeypatch.setattr("alpaca_news.client.load_dotenv", lambda *args, **kwargs: False)
    with pytest.raises(MissingCredentialsError):
        load_settings()


def test_duplicate_article_ids_are_not_processed_twice():
    received: list[dict] = []
    seen: set[str] = set()
    raw = json.dumps([ARTICLE])

    assert handle_messages(raw, seen, received.append) == 1
    assert handle_messages(raw, seen, received.append) == 0
    assert len(received) == 1


def test_comma_separated_symbols_are_parsed():
    assert parse_symbols("NVDA,AAPL") == ["NVDA", "AAPL"]


def test_inline_comment_is_not_part_of_a_symbol():
    assert parse_symbols("NVDA# default symbol") == ["NVDA"]


def test_all_news_subscription_is_supported():
    assert parse_symbols("*") == ["*"]


def test_subscription_message_has_expected_shape():
    assert subscription_message(["NVDA"]) == {"action": "subscribe", "news": ["NVDA"]}