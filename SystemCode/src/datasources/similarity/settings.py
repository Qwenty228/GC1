"""Environment-driven configuration for semantic comparison and event grouping.

Similarity is OFF unless ``SIMILARITY_ENABLED=true``. When it is off, ingestion
behaves as before: records are stored (or written to files only) and their
analysis is reported as ``disabled``. Every setting has a documented default in
``.env.example``; secrets (``ANTHROPIC_API_KEY``) are read by the SDK from the
environment and never stored here.

The retrieval defaults (7-day window, candidate limits, similarity floors) are
experimental starting points, not established optimal values.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace

from ..ingest.settings import ConfigurationError, _env, _env_bool, _env_float, _env_int

# Versions recorded with every analysis. Changing text preparation changes the
# embedding profile (and therefore creates new, versioned work); changing the
# grouping logic changes the pipeline version.
PIPELINE_VERSION = "similarity-1"
TEXT_PREP_VERSION = "textunits-1"

DEFAULT_EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"
# Pinned Hugging Face commit, so the profile identifies exact weights.
DEFAULT_EMBEDDING_REVISION = "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a"
DEFAULT_COMPARISON_MODEL = "claude-opus-5"

EMBEDDING_BACKENDS = ("sentence-transformers", "hashing")
COMPARISON_BACKENDS = ("anthropic", "mock")
EFFORT_LEVELS = ("low", "medium", "high", "xhigh", "max")


@dataclass(frozen=True)
class SimilaritySettings:
    enabled: bool = False

    # Embeddings
    embedding_backend: str = "sentence-transformers"
    embedding_model: str = DEFAULT_EMBEDDING_MODEL
    embedding_revision: str = DEFAULT_EMBEDDING_REVISION
    embedding_device: str = "cpu"
    embedding_batch_size: int = 16
    embedding_cache_dir: str | None = None
    hashing_dimensions: int = 256  # deterministic test/demo backend only

    # Text units (part of the embedding profile: changing them re-embeds)
    chunk_tokens: int = 384
    chunk_overlap_tokens: int = 48

    # LLM comparison
    comparison_backend: str = "anthropic"
    comparison_model: str = DEFAULT_COMPARISON_MODEL
    comparison_effort: str | None = None  # None = the model's default effort
    comparison_fallbacks: str = "default"  # server-side refusal fallback: "default" | "none"
    llm_timeout_seconds: float = 180.0
    llm_max_retries: int = 2
    llm_max_output_tokens: int = 16000
    llm_max_input_chars: int = 30000
    llm_max_calls_per_record: int = 3
    llm_max_events_per_call: int = 6
    llm_token_budget_per_record: int = 150000

    # Retrieval
    window_days: float = 7.0
    no_company_window_hours: float = 48.0
    semantic_top_k: int = 20
    lexical_top_k: int = 20
    lexical_max_terms: int = 48
    rrf_k: int = 60
    max_candidate_events: int = 6
    # Plausibility floors decide what reaches the LLM, never what is "the same event".
    # 0.78 sits near the 90th percentile of unrelated NVIDIA-news pairs measured with
    # bge-small-en-v1.5 on the stored data; it is a starting point to re-measure.
    min_semantic_similarity: float = 0.78
    lexical_plausible_rank: int = 3
    min_lexical_score: float = 0.0
    exact_match_min_chars: int = 400

    # Retry of persisted pending analysis
    max_analysis_attempts: int = 5
    retry_base_minutes: float = 10.0

    def with_overrides(self, **changes) -> "SimilaritySettings":
        return replace(self, **changes)

    def validate(self) -> "SimilaritySettings":
        problems: list[str] = []
        if self.embedding_backend not in EMBEDDING_BACKENDS:
            problems.append(f"SIMILARITY_EMBEDDING_BACKEND must be one of {', '.join(EMBEDDING_BACKENDS)}")
        if self.comparison_backend not in COMPARISON_BACKENDS:
            problems.append(f"SIMILARITY_COMPARISON_BACKEND must be one of {', '.join(COMPARISON_BACKENDS)}")
        if self.comparison_effort is not None and self.comparison_effort not in EFFORT_LEVELS:
            problems.append(f"SIMILARITY_COMPARISON_EFFORT must be empty or one of {', '.join(EFFORT_LEVELS)}")
        if self.comparison_fallbacks not in ("default", "none"):
            problems.append("SIMILARITY_COMPARISON_FALLBACKS must be 'default' or 'none'")
        if self.embedding_backend == "sentence-transformers" and not self.embedding_model.strip():
            problems.append("SIMILARITY_EMBEDDING_MODEL must name a sentence-transformers model")
        if self.comparison_backend == "anthropic" and not self.comparison_model.strip():
            problems.append("SIMILARITY_COMPARISON_MODEL must name a Claude model")
        positive = {
            "SIMILARITY_EMBEDDING_BATCH_SIZE": self.embedding_batch_size,
            "SIMILARITY_HASHING_DIMENSIONS": self.hashing_dimensions,
            "SIMILARITY_CHUNK_TOKENS": self.chunk_tokens,
            "SIMILARITY_LLM_TIMEOUT_SECONDS": self.llm_timeout_seconds,
            "SIMILARITY_LLM_MAX_OUTPUT_TOKENS": self.llm_max_output_tokens,
            "SIMILARITY_LLM_MAX_INPUT_CHARS": self.llm_max_input_chars,
            "SIMILARITY_LLM_MAX_CALLS_PER_RECORD": self.llm_max_calls_per_record,
            "SIMILARITY_LLM_MAX_EVENTS_PER_CALL": self.llm_max_events_per_call,
            "SIMILARITY_LLM_TOKEN_BUDGET_PER_RECORD": self.llm_token_budget_per_record,
            "SIMILARITY_WINDOW_DAYS": self.window_days,
            "SIMILARITY_NO_COMPANY_WINDOW_HOURS": self.no_company_window_hours,
            "SIMILARITY_SEMANTIC_TOP_K": self.semantic_top_k,
            "SIMILARITY_LEXICAL_TOP_K": self.lexical_top_k,
            "SIMILARITY_LEXICAL_MAX_TERMS": self.lexical_max_terms,
            "SIMILARITY_RRF_K": self.rrf_k,
            "SIMILARITY_MAX_CANDIDATE_EVENTS": self.max_candidate_events,
            "SIMILARITY_LEXICAL_PLAUSIBLE_RANK": self.lexical_plausible_rank,
            "SIMILARITY_EXACT_MATCH_MIN_CHARS": self.exact_match_min_chars,
            "SIMILARITY_MAX_ANALYSIS_ATTEMPTS": self.max_analysis_attempts,
            "SIMILARITY_RETRY_BASE_MINUTES": self.retry_base_minutes,
        }
        problems += [f"{name} must be greater than 0" for name, value in positive.items() if value <= 0]
        if self.chunk_overlap_tokens < 0 or self.chunk_overlap_tokens * 2 > self.chunk_tokens:
            problems.append("SIMILARITY_CHUNK_OVERLAP_TOKENS must be >= 0 and at most half of SIMILARITY_CHUNK_TOKENS")
        if self.llm_max_retries < 0:
            problems.append("SIMILARITY_LLM_MAX_RETRIES must be >= 0")
        if not -1.0 <= self.min_semantic_similarity <= 1.0:
            problems.append("SIMILARITY_MIN_SEMANTIC_SIMILARITY must be between -1 and 1")
        if self.min_lexical_score < 0:
            problems.append("SIMILARITY_MIN_LEXICAL_SCORE must be >= 0")
        if problems:
            raise ConfigurationError("; ".join(problems))
        return self


def _optional(name: str) -> str | None:
    value = _env(name, "")
    return value or None


def load_similarity_settings() -> SimilaritySettings:
    """Build settings from the environment (``.env`` is loaded by ``datasources.config``)."""
    effort = _optional("SIMILARITY_COMPARISON_EFFORT")
    return SimilaritySettings(
        enabled=_env_bool("SIMILARITY_ENABLED", False),
        embedding_backend=_env("SIMILARITY_EMBEDDING_BACKEND", "sentence-transformers").lower(),
        embedding_model=_env("SIMILARITY_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL),
        embedding_revision=_env("SIMILARITY_EMBEDDING_MODEL_REVISION", DEFAULT_EMBEDDING_REVISION),
        embedding_device=_env("SIMILARITY_EMBEDDING_DEVICE", "cpu"),
        embedding_batch_size=_env_int("SIMILARITY_EMBEDDING_BATCH_SIZE", 16),
        embedding_cache_dir=_optional("SIMILARITY_EMBEDDING_CACHE_DIR") or os.getenv("SENTENCE_TRANSFORMERS_HOME"),
        hashing_dimensions=_env_int("SIMILARITY_HASHING_DIMENSIONS", 256),
        chunk_tokens=_env_int("SIMILARITY_CHUNK_TOKENS", 384),
        chunk_overlap_tokens=_env_int("SIMILARITY_CHUNK_OVERLAP_TOKENS", 48),
        comparison_backend=_env("SIMILARITY_COMPARISON_BACKEND", "anthropic").lower(),
        comparison_model=_env("SIMILARITY_COMPARISON_MODEL", DEFAULT_COMPARISON_MODEL),
        comparison_effort=effort.lower() if effort else None,
        comparison_fallbacks=_env("SIMILARITY_COMPARISON_FALLBACKS", "default").lower(),
        llm_timeout_seconds=_env_float("SIMILARITY_LLM_TIMEOUT_SECONDS", 180.0),
        llm_max_retries=_env_int("SIMILARITY_LLM_MAX_RETRIES", 2),
        llm_max_output_tokens=_env_int("SIMILARITY_LLM_MAX_OUTPUT_TOKENS", 16000),
        llm_max_input_chars=_env_int("SIMILARITY_LLM_MAX_INPUT_CHARS", 30000),
        llm_max_calls_per_record=_env_int("SIMILARITY_LLM_MAX_CALLS_PER_RECORD", 3),
        llm_max_events_per_call=_env_int("SIMILARITY_LLM_MAX_EVENTS_PER_CALL", 6),
        llm_token_budget_per_record=_env_int("SIMILARITY_LLM_TOKEN_BUDGET_PER_RECORD", 150000),
        window_days=_env_float("SIMILARITY_WINDOW_DAYS", 7.0),
        no_company_window_hours=_env_float("SIMILARITY_NO_COMPANY_WINDOW_HOURS", 48.0),
        semantic_top_k=_env_int("SIMILARITY_SEMANTIC_TOP_K", 20),
        lexical_top_k=_env_int("SIMILARITY_LEXICAL_TOP_K", 20),
        lexical_max_terms=_env_int("SIMILARITY_LEXICAL_MAX_TERMS", 48),
        rrf_k=_env_int("SIMILARITY_RRF_K", 60),
        max_candidate_events=_env_int("SIMILARITY_MAX_CANDIDATE_EVENTS", 6),
        min_semantic_similarity=_env_float("SIMILARITY_MIN_SEMANTIC_SIMILARITY", 0.78),
        lexical_plausible_rank=_env_int("SIMILARITY_LEXICAL_PLAUSIBLE_RANK", 3),
        min_lexical_score=_env_float("SIMILARITY_MIN_LEXICAL_SCORE", 0.0),
        exact_match_min_chars=_env_int("SIMILARITY_EXACT_MATCH_MIN_CHARS", 400),
        max_analysis_attempts=_env_int("SIMILARITY_MAX_ANALYSIS_ATTEMPTS", 5),
        retry_base_minutes=_env_float("SIMILARITY_RETRY_BASE_MINUTES", 10.0),
    ).validate()
