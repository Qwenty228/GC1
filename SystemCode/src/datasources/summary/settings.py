"""Environment-driven configuration for event summarization.

Summarization is OFF unless ``SUMMARY_ENABLED=true``; it also needs event
grouping (``SIMILARITY_ENABLED=true``), because it summarizes grouped events.
Its model, limits and retries are configured separately from the comparison
model so either can change without touching the other. Secrets
(``ANTHROPIC_API_KEY``) are read by the SDK from the environment and never
stored or printed here.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from ..ingest.settings import ConfigurationError, _env, _env_bool, _env_float, _env_int
from ..similarity.settings import EFFORT_LEVELS

PROMPT_VERSION = "summary-1"
DEFAULT_SUMMARY_MODEL = "claude-opus-5"
SUMMARY_BACKENDS = ("anthropic",)
LANGUAGES = ("en",)  # storage is per language; only English is generated for now


@dataclass(frozen=True)
class SummarySettings:
    enabled: bool = False
    backend: str = "anthropic"
    model: str = DEFAULT_SUMMARY_MODEL
    effort: str | None = None  # None = the model's default effort
    fallbacks: str = "default"  # server-side refusal fallback: "default" | "none"
    language: str = "en"

    # One request per summary; the SDK retries 408/409/429/5xx and connection errors.
    # max_output_tokens is a ceiling (billing is per token used) and also covers the
    # model's adaptive thinking, so it is set well above the few hundred tokens a summary needs.
    llm_timeout_seconds: float = 120.0
    llm_max_retries: int = 2
    max_output_tokens: int = 16000

    # Bounded evidence: whole passages only, never cut mid-passage.
    max_input_chars: int = 24000
    max_passages: int = 40

    # Cost: at most this many summary requests per graph run (one ingested record, or
    # one retry/backfill batch) and per CLI command.
    max_requests_per_run: int = 5

    # Durable retries of failed summary work (independent of the ingestion ledger).
    max_attempts: int = 4
    retry_base_minutes: float = 10.0
    lease_minutes: float = 10.0

    def with_overrides(self, **changes) -> "SummarySettings":
        return replace(self, **changes)

    def validate(self) -> "SummarySettings":
        problems: list[str] = []
        if self.backend not in SUMMARY_BACKENDS:
            problems.append(f"SUMMARY_BACKEND must be one of {', '.join(SUMMARY_BACKENDS)}")
        if not self.model.strip():
            problems.append("SUMMARY_MODEL must name a Claude model")
        if self.effort is not None and self.effort not in EFFORT_LEVELS:
            problems.append(f"SUMMARY_EFFORT must be empty or one of {', '.join(EFFORT_LEVELS)}")
        if self.fallbacks not in ("default", "none"):
            problems.append("SUMMARY_FALLBACKS must be 'default' or 'none'")
        if self.language not in LANGUAGES:
            problems.append(f"SUMMARY_LANGUAGE must be one of {', '.join(LANGUAGES)} (no translation yet)")
        positive = {
            "SUMMARY_LLM_TIMEOUT_SECONDS": self.llm_timeout_seconds,
            "SUMMARY_MAX_OUTPUT_TOKENS": self.max_output_tokens,
            "SUMMARY_MAX_INPUT_CHARS": self.max_input_chars,
            "SUMMARY_MAX_PASSAGES": self.max_passages,
            "SUMMARY_MAX_REQUESTS_PER_RUN": self.max_requests_per_run,
            "SUMMARY_MAX_ATTEMPTS": self.max_attempts,
            "SUMMARY_RETRY_BASE_MINUTES": self.retry_base_minutes,
            "SUMMARY_LEASE_MINUTES": self.lease_minutes,
        }
        problems += [f"{name} must be greater than 0" for name, value in positive.items() if value <= 0]
        if self.llm_max_retries < 0:
            problems.append("SUMMARY_LLM_MAX_RETRIES must be >= 0")
        if self.max_input_chars < 2000:
            problems.append("SUMMARY_MAX_INPUT_CHARS must be at least 2000")
        if problems:
            raise ConfigurationError("; ".join(problems))
        return self


def _optional(name: str) -> str | None:
    value = _env(name, "")
    return value or None


def load_summary_settings() -> SummarySettings:
    """Build settings from the environment (``.env`` is loaded by ``datasources.config``)."""
    effort = _optional("SUMMARY_EFFORT")
    return SummarySettings(
        enabled=_env_bool("SUMMARY_ENABLED", False),
        backend=_env("SUMMARY_BACKEND", "anthropic").lower(),
        model=_env("SUMMARY_MODEL", DEFAULT_SUMMARY_MODEL),
        effort=effort.lower() if effort else None,
        fallbacks=_env("SUMMARY_FALLBACKS", "default").lower(),
        language=_env("SUMMARY_LANGUAGE", "en").lower(),
        llm_timeout_seconds=_env_float("SUMMARY_LLM_TIMEOUT_SECONDS", 120.0),
        llm_max_retries=_env_int("SUMMARY_LLM_MAX_RETRIES", 2),
        max_output_tokens=_env_int("SUMMARY_MAX_OUTPUT_TOKENS", 16000),
        max_input_chars=_env_int("SUMMARY_MAX_INPUT_CHARS", 24000),
        max_passages=_env_int("SUMMARY_MAX_PASSAGES", 40),
        max_requests_per_run=_env_int("SUMMARY_MAX_REQUESTS_PER_RUN", 5),
        max_attempts=_env_int("SUMMARY_MAX_ATTEMPTS", 4),
        retry_base_minutes=_env_float("SUMMARY_RETRY_BASE_MINUTES", 10.0),
        lease_minutes=_env_float("SUMMARY_LEASE_MINUTES", 10.0),
    ).validate()
