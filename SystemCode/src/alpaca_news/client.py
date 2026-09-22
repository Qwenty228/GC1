"""Alpaca Trading API WebSocket client for real-time news."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import websockets
from dotenv import load_dotenv

NEWS_STREAM_URL = "wss://stream.data.alpaca.markets/v1beta1/news"
LOGGER = logging.getLogger(__name__)
ARTICLE_FIELDS = (
    "id",
    "headline",
    "summary",
    "author",
    "created_at",
    "updated_at",
    "content",
    "url",
    "symbols",
    "source",
)
DEFAULT_MESSAGE_ARCHIVE = Path(__file__).resolve().parents[2] / "output" / "alpaca_news_stream.jsonl"


class MissingCredentialsError(ValueError):
    """Raised when the Trading API credentials are not configured."""


@dataclass(frozen=True)
class NewsSettings:
    api_key: str
    secret_key: str
    symbols: list[str]


def parse_symbols(value: str | None) -> list[str]:
    """Parse comma-separated subscriptions, defaulting to NVIDIA."""
    configured_value = (value or "NVDA").split("#", maxsplit=1)[0]
    symbols = [item.strip().upper() for item in configured_value.split(",")]
    parsed = [symbol for symbol in symbols if symbol]
    return parsed or ["NVDA"]


def load_settings() -> NewsSettings:
    """Load Trading API credentials and subscriptions from the environment."""
    root = Path(__file__).resolve().parents[2]
    load_dotenv(root / ".env")
    api_key = os.getenv("ALPACA_API_KEY", "").strip()
    secret_key = os.getenv("ALPACA_SECRET_KEY", "").strip()
    if not api_key or not secret_key:
        raise MissingCredentialsError(
            "ALPACA_API_KEY and ALPACA_SECRET_KEY must be set in .env or the environment."
        )
    return NewsSettings(
        api_key=api_key,
        secret_key=secret_key,
        symbols=parse_symbols(os.getenv("ALPACA_NEWS_SYMBOLS")),
    )


def subscription_message(symbols: list[str]) -> dict[str, object]:
    """Build the Alpaca news subscription message."""
    return {"action": "subscribe", "news": symbols}


def parse_message_batch(raw_message: str | bytes) -> list[dict[str, Any]]:
    """Decode one Alpaca JSON message batch, which is always an array."""
    decoded = json.loads(raw_message)
    if not isinstance(decoded, list):
        raise ValueError("Alpaca message must be a JSON array")
    return [message for message in decoded if isinstance(message, dict)]


def persist_raw_message(raw_message: str | bytes, archive_path: Path = DEFAULT_MESSAGE_ARCHIVE) -> None:
    """Append one received JSON websocket frame to the message archive."""
    decoded = json.loads(raw_message)
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with archive_path.open("a", encoding="utf-8") as archive:
        archive.write(json.dumps(decoded, ensure_ascii=True) + "\n")


def extract_article(message: Mapping[str, Any]) -> dict[str, Any]:
    """Extract the stable article shape for future downstream processing."""
    return {field: message.get(field) for field in ARTICLE_FIELDS}


def process_article(article: Mapping[str, Any]) -> None:
    """Print a single article; replace this boundary with an LLM pipeline later."""
    print(
        "\n".join(
            (
                "\n--- Alpaca news article ---",
                f"ID: {article['id']}",
                f"Headline: {article['headline'] or ''}",
                f"Summary: {article['summary'] or ''}",
                f"Author: {article['author'] or ''}",
                f"Created: {article['created_at'] or ''}",
                f"Updated: {article['updated_at'] or ''}",
                f"Source: {article['source'] or ''}",
                f"Symbols: {', '.join(article['symbols'] or [])}",
                f"URL: {article['url'] or ''}",
                f"Content: {article['content'] or ''}",
                "---------------------------",
            )
        )
    )


def handle_messages(
    raw_message: str | bytes,
    seen_article_ids: set[str],
    article_processor: Callable[[Mapping[str, Any]], None] = process_article,
) -> int:
    """Handle an Alpaca batch and return its count of newly processed articles."""
    processed = 0
    for message in parse_message_batch(raw_message):
        message_type = message.get("T")
        if message_type == "success":
            LOGGER.info("Alpaca news stream status: %s", message.get("msg", "success"))
        elif message_type == "subscription":
            LOGGER.info("Active Alpaca news subscriptions: %s", message.get("news", []))
        elif message_type == "error":
            LOGGER.error("Alpaca news stream error: %s", message.get("msg", "unknown error"))
        elif message_type == "n":
            article = extract_article(message)
            article_id = article["id"]
            if article_id is None:
                LOGGER.warning("Ignoring Alpaca news article without an id")
            elif str(article_id) in seen_article_ids:
                LOGGER.debug("Ignoring duplicate Alpaca news article id %s", article_id)
            else:
                seen_article_ids.add(str(article_id))
                article_processor(article)
                processed += 1
        else:
            LOGGER.warning("Ignoring unknown Alpaca message type: %r", message_type)
    return processed


async def stream_once(
    settings: NewsSettings,
    seen_article_ids: set[str],
    message_archive: Path = DEFAULT_MESSAGE_ARCHIVE,
) -> None:
    """Connect, subscribe, and process news until the connection closes."""
    headers = {
        "APCA-API-KEY-ID": settings.api_key,
        "APCA-API-SECRET-KEY": settings.secret_key,
    }
    async with websockets.connect(
        NEWS_STREAM_URL,
        additional_headers=headers,
    ) as websocket:
        await websocket.send(json.dumps(subscription_message(settings.symbols)))
        LOGGER.info("Connected to Alpaca news stream; subscribed to %s", settings.symbols)
        async for raw_message in websocket:
            try:
                persist_raw_message(raw_message, message_archive)
                handle_messages(raw_message, seen_article_ids)
            except (ValueError, json.JSONDecodeError) as error:
                LOGGER.warning("Ignoring invalid Alpaca message: %s", error)


async def run_forever(settings: NewsSettings) -> None:
    """Reconnect after unexpected disconnects with capped exponential backoff."""
    seen_article_ids: set[str] = set()
    delay_seconds = 1
    while True:
        try:
            await stream_once(settings, seen_article_ids)
            LOGGER.warning("Alpaca news stream closed; reconnecting")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            LOGGER.warning("Alpaca news stream disconnected: %s", error)

        LOGGER.info("Reconnecting to Alpaca news stream in %s seconds", delay_seconds)
        await asyncio.sleep(delay_seconds)
        delay_seconds = min(delay_seconds * 2, 30)