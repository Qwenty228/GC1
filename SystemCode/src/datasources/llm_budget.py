"""A run-wide cap on actual model HTTP attempts: comparison and summary calls, SDK retries included.

One logical model call (``messages.create``) can become several HTTP requests: the Anthropic SDK retries
408/409/429/5xx responses, timeouts and connection errors up to SIMILARITY_LLM_MAX_RETRIES /
SUMMARY_LLM_MAX_RETRIES times, and a retried request the server processed is billed like any other. So the
cap counts attempts, not calls. `CountingTransport` sits between the SDK and the network (the provider-call
boundary): it reserves one attempt in the `AttemptLedger` BEFORE each HTTP request is sent and refuses to
send once the cap is reached, raising `AttemptBudgetExhausted`, an ``anthropic.AnthropicError`` that the
SDK neither wraps nor retries. The ledger file is rewritten before every attempt, so a crash or a kill never
loses one.

This caps attempts, not money. Token usage is read from each response and reported; an attempt that timed
out or lost its connection reports no usage although the server may have processed (and billed) it.
Nothing here logs or stores request content, response text or headers other than the request id.
"""

from __future__ import annotations

import json
import logging
import socket
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anthropic
import httpx2

from .ingest.store import atomic_write_json

LOGGER = logging.getLogger(__name__)
USAGE_KEYS = ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class AttemptBudgetExhausted(anthropic.AnthropicError):
    """No request was sent: the run's attempt cap is reached. Never retried by the SDK."""

    def __init__(self, message: str, *, sdk_retry: int | None = None):
        super().__init__(message)
        self.sdk_retry = sdk_retry  # >0: an earlier attempt of the same logical call was sent and failed


class AttemptLedger:
    """Counts, caps and records the HTTP attempts of one run (thread-safe, persisted before each send)."""

    def __init__(self, limit: int, path: Path | None = None):
        if limit < 1:
            raise ValueError("the attempt limit must be at least 1")
        self.limit = limit
        self.path = path
        self.attempts: list[dict[str, Any]] = []
        self.blocked: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._save()

    @property
    def used(self) -> int:
        return len(self.attempts)

    @property
    def remaining(self) -> int:
        return max(self.limit - self.used, 0)

    def exhausted(self) -> bool:
        return self.used >= self.limit

    def persist_to(self, path: Path) -> None:
        """Start writing the ledger file (e.g. once the run's locks are held)."""
        with self._lock:
            self.path = path
            self._save()

    def reserve(self, entry: dict[str, Any]) -> dict[str, Any]:
        """Count one attempt before it is sent, or raise AttemptBudgetExhausted (nothing is sent)."""
        with self._lock:
            if self.used >= self.limit:
                self.blocked.append({**entry, "at": _now(), "reason": f"cap of {self.limit} attempt(s) reached"})
                self._save()
                raise AttemptBudgetExhausted(
                    f"model request not sent: the run's cap of {self.limit} HTTP attempt(s) is used up",
                    sdk_retry=entry.get("sdk_retry"))
            entry = {"n": self.used + 1, "at": _now(), **entry, "outcome": "sending"}
            self.attempts.append(entry)
            self._save()
            return entry

    def finish(self, entry: dict[str, Any], **fields: Any) -> None:
        with self._lock:
            entry.update(fields)
            self._save()

    def _save(self) -> None:
        if self.path is not None:
            atomic_write_json(self.path, {"limit": self.limit, "used": self.used, "remaining": self.remaining,
                                          "updated_at": _now(), "attempts": self.attempts, "blocked": self.blocked})

    def summary(self) -> dict[str, Any]:
        """What was actually sent, and the usage the responses reported (no dollar amounts)."""
        with self._lock:
            attempts = list(self.attempts)
            blocked = len(self.blocked)
        tokens = {key: 0 for key in USAGE_KEYS}
        with_usage = 0
        unknown = 0
        models: list[str] = []
        for attempt in attempts:
            usage = attempt.get("usage")
            if usage:
                with_usage += 1
                for key in USAGE_KEYS:
                    tokens[key] += int(usage.get(key) or 0)
            elif attempt.get("outcome") in ("timeout", "connection_error", "sending", "unreadable_response"):
                unknown += 1
            model = attempt.get("response_model")
            if model and model not in models:
                models.append(model)
        return {
            "limit": self.limit, "used": len(attempts), "remaining": max(self.limit - len(attempts), 0),
            "not_sent_because_of_the_cap": blocked,
            "by_purpose": dict(Counter(a.get("purpose") for a in attempts)),
            # A logical call's first attempt carries SDK retry count 0; its retries carry 1, 2, ...
            "logical_calls_sent": dict(Counter(a.get("purpose") for a in attempts if a.get("sdk_retry") == 0)),
            "sdk_retries": sum(1 for a in attempts if (a.get("sdk_retry") or 0) > 0),
            "by_outcome": dict(Counter(a.get("outcome") for a in attempts)),
            "tokens_reported": {**tokens, "attempts_with_usage": with_usage},
            "attempts_without_usage_that_may_be_billed": unknown,
            "served_models": models,
        }


class CountingTransport(httpx2.BaseTransport):
    """Wraps the transport that sends model requests: reserve an attempt, then send (or refuse)."""

    def __init__(self, ledger: AttemptLedger, purpose: str, inner: httpx2.BaseTransport):
        self.ledger = ledger
        self.purpose = purpose
        self.inner = inner

    def handle_request(self, request: httpx2.Request) -> httpx2.Response:
        retry = request.headers.get("x-stainless-retry-count")
        entry: dict[str, Any] = {"purpose": self.purpose, "method": request.method, "path": request.url.path,
                                 "sdk_retry": int(retry) if retry and retry.isdigit() else None}
        try:
            body = json.loads(request.content or b"{}")
            entry["model"] = body.get("model") if isinstance(body, dict) else None
        except (ValueError, TypeError):
            entry["model"] = None
        entry = self.ledger.reserve(entry)  # raises before anything is sent
        started = time.monotonic()
        try:
            response = self.inner.handle_request(request)
            response.read()  # buffered here; the SDK reads the same bytes again
        except httpx2.TimeoutException as error:
            self.ledger.finish(entry, outcome="timeout", error_type=type(error).__name__,
                               latency_seconds=round(time.monotonic() - started, 3))
            raise
        except BaseException as error:  # connection errors, and Ctrl+C while waiting for the response
            self.ledger.finish(entry, outcome="connection_error" if isinstance(error, Exception) else "interrupted",
                               error_type=type(error).__name__, latency_seconds=round(time.monotonic() - started, 3))
            raise
        self.ledger.finish(entry, **_describe(response), latency_seconds=round(time.monotonic() - started, 3))
        return response

    def close(self) -> None:
        self.inner.close()


def _describe(response: httpx2.Response) -> dict[str, Any]:
    """Status, request id, stop reason, served model and usage: never the content."""
    out: dict[str, Any] = {"outcome": f"http_{response.status_code}", "status": response.status_code,
                           "request_id": response.headers.get("request-id")}
    try:
        body = json.loads(response.content)
    except ValueError:
        out["outcome"] = "unreadable_response" if response.status_code == 200 else out["outcome"]
        return out
    if not isinstance(body, dict):
        return out
    if response.status_code != 200:
        error = body.get("error") if isinstance(body.get("error"), dict) else {}
        out["error_type"] = error.get("type")
        return out
    usage = body.get("usage") if isinstance(body.get("usage"), dict) else {}
    out["usage"] = {k: usage[k] for k in USAGE_KEYS if isinstance(usage.get(k), int)}
    if isinstance(usage.get("iterations"), list):  # server-side fallback: usage per model that ran
        out["usage"]["iterations"] = [{k: i.get(k) for k in ("type", "model", "input_tokens", "output_tokens")}
                                      for i in usage["iterations"] if isinstance(i, dict)]
    out["stop_reason"] = body.get("stop_reason")
    out["response_model"] = body.get("model")
    return out


def _socket_options() -> list[tuple[int, int, int | bool]]:
    """The keep-alive options the SDK's default client uses (long model calls keep the connection open)."""
    options: list[tuple[int, int, int | bool]] = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)]
    for name, value in (("TCP_KEEPINTVL", 60), ("TCP_KEEPCNT", 5), ("TCP_KEEPIDLE", 60)):
        option = getattr(socket, name, None)
        if option is not None:
            options.append((socket.IPPROTO_TCP, option, value))
    if getattr(socket, "TCP_KEEPINTVL", None) is None and sys.platform == "darwin":
        options.append((socket.IPPROTO_TCP, getattr(socket, "TCP_KEEPALIVE", 0x10), 60))
    return options


def counting_http_client(ledger: AttemptLedger, purpose: str, *, transport_factory=None) -> httpx2.Client:
    """An httpx client like the SDK's default one, with EVERY transport wrapped in a CountingTransport.

    httpx ignores proxy environment variables once a transport is given, so the SDK's own proxy handling is
    reproduced here with counting transports: a proxy configured in the environment is still used, and no
    request can bypass the count. ``transport_factory(proxy_url_or_None)`` replaces the real transports (tests).
    """
    def make(proxy_url: str | None) -> httpx2.BaseTransport:
        if transport_factory is not None:
            return transport_factory(proxy_url)
        kwargs: dict[str, Any] = {"limits": anthropic.DEFAULT_CONNECTION_LIMITS, "socket_options": _socket_options()}
        if proxy_url:
            kwargs["proxy"] = httpx2.Proxy(url=proxy_url)
        return httpx2.HTTPTransport(**kwargs)

    try:
        from anthropic._utils._httpx import get_environment_proxies

        proxies = get_environment_proxies()
    except ImportError:  # SDK internals moved: fall back to direct connections (still counted)
        LOGGER.warning("could not read proxy settings the way the Anthropic SDK does; model requests connect directly")
        proxies = {}
    mounts = {key: None if url is None else CountingTransport(ledger, purpose, make(url)) for key, url in proxies.items()}
    return anthropic.DefaultHttpxClient(transport=CountingTransport(ledger, purpose, make(None)), mounts=mounts)


def counting_anthropic(ledger: AttemptLedger, purpose: str, *, timeout: float, max_retries: int,
                       transport_factory=None) -> anthropic.Anthropic:
    """The client the comparison / summary adapters would build (same timeout and SDK retries), counted."""
    return anthropic.Anthropic(timeout=timeout, max_retries=max_retries,
                               http_client=counting_http_client(ledger, purpose, transport_factory=transport_factory))
