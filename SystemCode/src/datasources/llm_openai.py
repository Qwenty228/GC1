"""Model backends for any OpenAI-compatible chat API: OpenAI, Gemini, Ollama, LM Studio, vLLM, ...

The pipeline's two LLM steps each take a backend object:

    event comparison  backend.compare(request)   -> similarity.comparison.ComparisonCall
    event summary     backend.summarize(request) -> summary.generation.SummaryCall

The Claude backends use the Anthropic SDK. These two speak the OpenAI chat-completions
API instead (`POST {base_url}/chat/completions`), which most providers and local
servers expose, so one class covers them all. Same prompts, same JSON schema, same
validation afterwards: only the transport differs.

    base_url examples
      OpenAI      https://api.openai.com/v1
      Gemini      https://generativelanguage.googleapis.com/v1beta/openai/
      Ollama      http://localhost:11434/v1
      LM Studio   http://localhost:1234/v1

`json_mode` controls how the JSON schema is requested:
    "json_schema"  response_format json_schema (OpenAI, Gemini, recent Ollama/vLLM)
    "json_object"  response_format json_object + the schema in the system prompt
    "prompt"       no response_format; the schema only in the system prompt (any server)
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

import httpx

from .similarity.comparison import (
    RESPONSE_SCHEMA as COMPARISON_SCHEMA,
    SYSTEM_PROMPT as COMPARISON_PROMPT,
    ComparisonCall,
    ComparisonError,
    ComparisonOutput,
    JsonModelResult,
)

JSON_MODES = ("json_schema", "json_object", "prompt")
_FENCE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.S)
# one line per HTTP outcome (status, time, timeout); never a header, a key, the prompt or the answer text
LOGGER = logging.getLogger(__name__)


def call_openai_json(*, base_url: str, model: str, api_key: str | None, system: str, user: str,
                     schema: dict[str, Any], schema_name: str, max_tokens: int, timeout: float,
                     json_mode: str = "json_schema", temperature: float | None = 0.0,
                     error: type[Exception] = ComparisonError) -> JsonModelResult:
    """One chat-completions call that must return a JSON object. Raises `error` on any failure."""
    if json_mode not in JSON_MODES:
        raise ValueError(f"json_mode must be one of {', '.join(JSON_MODES)}")
    if json_mode != "json_schema":
        system = (f"{system}\n\nReply with ONE JSON object only, no prose, matching this JSON schema:\n"
                  f"{json.dumps(schema)}")
    body: dict[str, Any] = {"model": model, "max_tokens": max_tokens,
                            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]}
    if temperature is not None:
        body["temperature"] = temperature
    if json_mode == "json_schema":
        body["response_format"] = {"type": "json_schema",
                                   "json_schema": {"name": schema_name, "schema": schema, "strict": False}}
    elif json_mode == "json_object":
        body["response_format"] = {"type": "json_object"}
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    started = time.monotonic()
    try:
        response = httpx.post(f"{base_url.rstrip('/')}/chat/completions", json=body, headers=headers,
                              timeout=timeout)
    except httpx.TimeoutException as exc:
        LOGGER.info("    HTTP: no answer from %s, timed out after %.1fs (timeout %.0fs)", model,
                    time.monotonic() - started, timeout)
        raise error(f"{model}: timed out after {timeout}s", kind="timeout", retryable=True) from exc
    except httpx.HTTPError as exc:
        LOGGER.info("    HTTP: connection to %s failed after %.1fs (%s)", model, time.monotonic() - started,
                    type(exc).__name__)
        raise error(f"{model}: connection failed ({exc})", kind="connection", retryable=True) from exc
    LOGGER.info("    HTTP %d from %s in %.1fs (timeout %.0fs)", response.status_code, model,
                time.monotonic() - started, timeout)
    if response.status_code >= 400:
        retryable = response.status_code in (408, 409, 429) or response.status_code >= 500
        failure = error(f"{model}: HTTP {response.status_code}: {response.text[:300]}",
                        kind="http_error", retryable=retryable)
        # for callers that retry (evaluation.gemini_projects): the status, the server's Retry-After and the full
        # error body (Gemini puts the quota id and RetryInfo past the 300 characters kept in the message)
        failure.status_code = response.status_code
        failure.retry_after = response.headers.get("retry-after")
        failure.body = response.text[:20000]
        raise failure
    data = response.json()
    choice = (data.get("choices") or [{}])[0]
    finish = choice.get("finish_reason")
    if finish == "length":
        raise error(f"{model}: answer truncated at max_tokens={max_tokens}", kind="truncated", retryable=False)
    text = (choice.get("message") or {}).get("content") or ""
    fenced = _FENCE.match(text)  # some local models wrap JSON in ```json fences
    usage = data.get("usage") or {}
    LOGGER.info("    answer: finish_reason %s, tokens in/out %s/%s, %d chars of JSON", finish,
                usage.get("prompt_tokens", "?"), usage.get("completion_tokens", "?"), len(text))
    return JsonModelResult(
        text=fenced.group(1) if fenced else text.strip(),
        model=data.get("model") or model,
        usage={"input_tokens": usage.get("prompt_tokens", 0), "output_tokens": usage.get("completion_tokens", 0)},
        request_id=data.get("id"),
        latency_seconds=time.monotonic() - started,
        stop_reason=finish,
    )


class OpenAICompatibleComparisonBackend:
    """Event comparison through an OpenAI-compatible API. Drop-in for `AnthropicComparisonBackend`."""

    name = "openai_compatible"

    def __init__(self, *, base_url: str, model: str, api_key_env: str | None = None, json_mode: str = "json_schema",
                 max_tokens: int = 16000, timeout: float = 180.0, temperature: float | None = 0.0):
        self.base_url, self.model, self.json_mode = base_url, model, json_mode
        self.api_key = os.environ.get(api_key_env) if api_key_env else None
        if api_key_env and not self.api_key:
            raise ComparisonError(f"{api_key_env} is not set (put it in SystemCode/.env)", kind="configuration",
                                  retryable=False)
        self.max_tokens, self.timeout, self.temperature = max_tokens, timeout, temperature

    def compare(self, request) -> ComparisonCall:
        result = call_openai_json(base_url=self.base_url, model=self.model, api_key=self.api_key,
                                  system=COMPARISON_PROMPT, user=request.render(), schema=COMPARISON_SCHEMA,
                                  schema_name="event_comparison", max_tokens=self.max_tokens, timeout=self.timeout,
                                  json_mode=self.json_mode, temperature=self.temperature)
        try:
            output = ComparisonOutput.model_validate_json(result.text)
        except ValueError as exc:
            raise ComparisonError(f"{self.model}: response did not match the schema ({str(exc)[:200]})",
                                  kind="invalid_response", retryable=True) from exc
        return ComparisonCall(output, result.model, result.usage, request_id=result.request_id,
                              latency_seconds=result.latency_seconds, stop_reason=result.stop_reason)


class OpenAICompatibleSummaryBackend:
    """Event summaries through an OpenAI-compatible API. Drop-in for `AnthropicSummaryBackend`."""

    name = "openai_compatible"

    def __init__(self, *, base_url: str, model: str, api_key_env: str | None = None, json_mode: str = "json_schema",
                 max_tokens: int = 16000, timeout: float = 180.0, temperature: float | None = 0.0):
        from .summary.generation import SummaryError

        self.base_url, self.model, self.json_mode = base_url, model, json_mode
        self.api_key = os.environ.get(api_key_env) if api_key_env else None
        if api_key_env and not self.api_key:
            raise SummaryError(f"{api_key_env} is not set (put it in SystemCode/.env)", kind="configuration",
                               retryable=False)
        self.max_tokens, self.timeout, self.temperature = max_tokens, timeout, temperature

    def summarize(self, request):
        from .summary.generation import RESPONSE_SCHEMA, SYSTEM_PROMPT, SummaryCall, SummaryError

        result = call_openai_json(base_url=self.base_url, model=self.model, api_key=self.api_key,
                                  system=SYSTEM_PROMPT, user=request.render(), schema=RESPONSE_SCHEMA,
                                  schema_name="event_summary", max_tokens=self.max_tokens, timeout=self.timeout,
                                  json_mode=self.json_mode, temperature=self.temperature, error=SummaryError)
        try:
            output = json.loads(result.text)
        except json.JSONDecodeError as exc:
            raise SummaryError("summary response was not JSON", kind="invalid_response", retryable=True,
                               raw_output=result.text[:20000]) from exc
        if not isinstance(output, dict):
            raise SummaryError("summary response was not a JSON object", kind="invalid_response", retryable=True,
                               raw_output=result.text[:20000])
        return SummaryCall(output, result.model, result.usage, result.request_id, result.latency_seconds,
                           result.stop_reason)
