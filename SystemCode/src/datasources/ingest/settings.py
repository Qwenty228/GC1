"""Environment-driven configuration for the ingestion worker.

Every setting has a documented default (see `.env.example`). Semantic comparison
has its own settings (and its only AI credential) in ``datasources.similarity.settings``.
Live and demo runs use different output roots so their records and tracking
state can never mix: live -> ``INGEST_OUTPUT_DIR`` (default ``output/``),
demo -> ``<INGEST_OUTPUT_DIR>/demo``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from pathlib import Path

from .. import config


class ConfigurationError(ValueError):
    """Raised when required live configuration (credentials, user agent) is missing."""


def _env(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    # Tolerate inline comments the way the existing symbol parser does.
    value = raw.split(" #", maxsplit=1)[0].strip()
    return value or default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number") from exc


def _env_bool(name: str, default: bool) -> bool:
    value = _env(name, "true" if default else "false").lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name} must be true or false")


def parse_symbol_list(value: str | None, default: str = "NVDA") -> list[str]:
    """Parse ``NVDA,AAPL`` into upper-case symbols, preserving ticker punctuation."""
    items = [item.strip().upper() for item in (value or default).split("#")[0].split(",")]
    parsed = list(dict.fromkeys(item for item in items if item))
    return parsed or [default]


@dataclass(frozen=True)
class IngestSettings:
    symbols: list[str] = field(default_factory=lambda: ["NVDA"])
    output_root: Path = config.ROOT / "output"
    demo: bool = False

    # SEC EDGAR
    sec_user_agent: str = config.SEC_USER_AGENT
    sec_forms: tuple[str, ...] = ("8-K", "8-K/A")
    sec_poll_interval_seconds: float = 60.0
    sec_requests_per_second: float = 2.0
    sec_initial_lookback_days: int = 30
    sec_recovery_max_days: int = 365
    sec_max_historical_files: int = 2
    sec_max_document_bytes: int = 15 * 1024 * 1024
    sec_max_submissions_bytes: int = 60 * 1024 * 1024
    sec_max_retries: int = 3

    # Alpaca
    alpaca_initial_lookback_hours: int = 24
    alpaca_reconnect_overlap_minutes: int = 15
    alpaca_recovery_max_hours: int = 72
    alpaca_backfill_max_pages: int = 40
    alpaca_page_size: int = 50  # documented maximum for /v1beta1/news

    # Worker
    queue_size: int = 200
    http_timeout_seconds: float = 30.0
    sec_http_timeout_seconds: float = 60.0
    # Live-only collection (INGEST_LIVE_ONLY): every worker startup takes a fresh UTC cutoff and nothing published
    # before it is collected; no SEC catch-up, no Alpaca REST backfill at startup or after reconnects, and the saved
    # recovery state (Alpaca cursor, SEC scope floors) is left as it is. See IngestionService.begin_live_only.
    live_only: bool = False

    @property
    def root(self) -> Path:
        """Effective output root: demo data lives in its own subtree."""
        return self.output_root / "demo" if self.demo else self.output_root

    def with_overrides(self, **changes) -> "IngestSettings":
        return replace(self, **changes)


def load_settings(demo: bool | None = None) -> IngestSettings:
    """Build settings from the environment (``.env`` is loaded by ``config``)."""
    output_dir = Path(_env("INGEST_OUTPUT_DIR", "output"))
    if not output_dir.is_absolute():
        output_dir = config.ROOT / output_dir
    symbols_raw = os.getenv("INGEST_SYMBOLS") or os.getenv("ALPACA_NEWS_SYMBOLS") or "NVDA"
    forms = tuple(parse_symbol_list(_env("SEC_FORMS", "8-K,8-K/A"), default="8-K"))
    settings = IngestSettings(
        symbols=parse_symbol_list(symbols_raw),
        output_root=output_dir,
        demo=_env_bool("INGEST_DEMO_MODE", False) if demo is None else demo,
        sec_user_agent=os.getenv("SEC_USER_AGENT", "").strip(),
        sec_forms=forms,
        sec_poll_interval_seconds=_env_float("SEC_POLL_INTERVAL_SECONDS", 60.0),
        sec_requests_per_second=_env_float("SEC_REQUESTS_PER_SECOND", 2.0),
        sec_initial_lookback_days=_env_int("SEC_INITIAL_LOOKBACK_DAYS", 30),
        sec_recovery_max_days=_env_int("SEC_RECOVERY_MAX_DAYS", 365),
        sec_max_historical_files=_env_int("SEC_MAX_HISTORICAL_FILES", 2),
        sec_max_document_bytes=_env_int("SEC_MAX_DOCUMENT_BYTES", 15 * 1024 * 1024),
        sec_max_submissions_bytes=_env_int("SEC_MAX_SUBMISSIONS_BYTES", 60 * 1024 * 1024),
        sec_max_retries=_env_int("SEC_MAX_RETRIES", 3),
        alpaca_initial_lookback_hours=_env_int("ALPACA_INITIAL_LOOKBACK_HOURS", 24),
        alpaca_reconnect_overlap_minutes=_env_int("ALPACA_RECONNECT_OVERLAP_MINUTES", 15),
        alpaca_recovery_max_hours=_env_int("ALPACA_RECOVERY_MAX_HOURS", 72),
        alpaca_backfill_max_pages=_env_int("ALPACA_BACKFILL_MAX_PAGES", 40),
        alpaca_page_size=min(max(_env_int("ALPACA_PAGE_SIZE", 50), 1), 50),
        queue_size=_env_int("INGEST_QUEUE_SIZE", 200),
        http_timeout_seconds=_env_float("HTTP_TIMEOUT_SECONDS", 30.0),
        sec_http_timeout_seconds=_env_float("SEC_HTTP_TIMEOUT_SECONDS", 60.0),
        live_only=_env_bool("INGEST_LIVE_ONLY", False),
    )
    if settings.sec_requests_per_second <= 0 or settings.sec_requests_per_second > 5:
        # SEC's published aggregate limit is 10 req/s; stay well below it.
        raise ConfigurationError("SEC_REQUESTS_PER_SECOND must be > 0 and <= 5")
    if settings.sec_http_timeout_seconds <= 0:
        raise ConfigurationError("SEC_HTTP_TIMEOUT_SECONDS must be greater than 0")
    return settings


def require_sec_user_agent(settings: IngestSettings) -> str:
    """Live SEC access needs an identifying User-Agent ("Name contact@example.com")."""
    agent = settings.sec_user_agent.strip()
    if not agent or "@" not in agent:
        raise ConfigurationError(
            "SEC_USER_AGENT must identify you, e.g. 'MyResearchApp you@example.com' "
            "(SEC fair-access policy)."
        )
    return agent
