"""SEC HTTP client for the ingestion worker.

* One shared `RateLimiter` covers every SEC request (submissions JSON, historical
  submissions files, primary documents). Default 2 req/s, well under SEC's
  published 10 req/s aggregate fair-access limit.
* Identifying User-Agent from configuration.
* Bounded retries with exponential backoff + jitter; ``Retry-After`` honoured.
  403/404 and other 4xx are not retried (no retry storms on access errors).
* Redirects are followed manually and every hop must stay on an SEC host.
* Responses are streamed and capped in size; content types are checked.
"""

from __future__ import annotations

import email.utils
import json
import logging
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import requests

from .htmltext import looks_like_error_page

LOGGER = logging.getLogger(__name__)

SUBMISSIONS_HOSTS = frozenset({"data.sec.gov"})
ARCHIVE_HOSTS = frozenset({"www.sec.gov", "sec.gov"})
ALL_SEC_HOSTS = SUBMISSIONS_HOSTS | ARCHIVE_HOSTS
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
HISTORICAL_URL = "https://data.sec.gov/submissions/{name}"
HTML_TYPES = ("text/html", "application/xhtml+xml", "application/xml", "text/xml")
TEXT_TYPES = ("text/plain",)
JSON_TYPES = ("application/json", "text/json")
MAX_REDIRECTS = 3
MAX_RETRY_AFTER_SECONDS = 120.0


class SecFetchError(RuntimeError):
    """A failed SEC request. `retryable` says whether a later attempt may succeed."""

    def __init__(self, message: str, *, kind: str, retryable: bool, status: int | None = None):
        super().__init__(message)
        self.kind = kind
        self.retryable = retryable
        self.status = status


class RateLimiter:
    """Thread-safe minimum-interval limiter shared by all SEC requests."""

    def __init__(self, per_second: float, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self.interval = 1.0 / per_second
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            wait = self._next - now
            if wait > 0:
                self._sleep(wait)
                now = self._clock()
            self._next = max(now, self._next) + self.interval


@dataclass
class FetchResult:
    url: str  # final URL after validated redirects
    status: int
    content_type: str
    body: bytes
    retrieved_at: datetime

    def text(self) -> str:
        charset = "utf-8"
        if "charset=" in self.content_type:
            charset = self.content_type.split("charset=", 1)[1].split(";")[0].strip() or "utf-8"
        try:
            return self.body.decode(charset, errors="replace")
        except LookupError:
            return self.body.decode("utf-8", errors="replace")


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def validate_sec_url(url: str, allowed_hosts: frozenset[str] = ALL_SEC_HOSTS) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in allowed_hosts:
        raise SecFetchError(f"refusing non-SEC URL {url!r}", kind="invalid_url", retryable=False)
    if parsed.username or parsed.password or (parsed.port not in (None, 443)):
        raise SecFetchError(f"refusing unexpected URL form {url!r}", kind="invalid_url", retryable=False)
    return url


class SecClient:
    def __init__(
        self,
        user_agent: str,
        *,
        requests_per_second: float = 2.0,
        timeout: float = 30.0,
        max_retries: int = 3,
        session: requests.Session | None = None,
        limiter: RateLimiter | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.user_agent = user_agent
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.limiter = limiter or RateLimiter(requests_per_second)
        self._sleep = sleep
        self.request_count = 0

    # -- low level -------------------------------------------------------------
    def _get_once(self, url: str, allowed_hosts: frozenset[str], max_bytes: int,
                  accept_types: tuple[str, ...]) -> FetchResult:
        current = validate_sec_url(url, allowed_hosts)
        for _hop in range(MAX_REDIRECTS + 1):
            self.limiter.acquire()
            self.request_count += 1
            response = self.session.get(
                current,
                headers={"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"},
                timeout=self.timeout,
                allow_redirects=False,
                stream=True,
            )
            try:
                status = response.status_code
                if status in (301, 302, 303, 307, 308):
                    location = response.headers.get("Location")
                    if not location:
                        raise SecFetchError("redirect without Location", kind="bad_redirect", retryable=False, status=status)
                    current = validate_sec_url(urljoin(current, location), allowed_hosts)
                    continue
                if status != 200:
                    retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
                    retryable = status == 429 or status >= 500
                    kind = "access_denied" if status == 403 else ("not_found" if status == 404 else f"http_{status}")
                    error = SecFetchError(f"SEC returned HTTP {status} for {current}", kind=kind,
                                          retryable=retryable, status=status)
                    error.retry_after = retry_after  # type: ignore[attr-defined]
                    raise error
                content_type = (response.headers.get("Content-Type") or "").lower()
                if accept_types and not any(t in content_type for t in accept_types):
                    raise SecFetchError(f"unexpected content type {content_type!r} for {current}",
                                        kind="bad_content_type", retryable=False, status=status)
                declared = response.headers.get("Content-Length")
                if declared and declared.isdigit() and int(declared) > max_bytes:
                    raise SecFetchError(f"response too large ({declared} bytes > {max_bytes})",
                                        kind="too_large", retryable=False, status=status)
                chunks: list[bytes] = []
                size = 0
                for chunk in response.iter_content(chunk_size=65536):
                    size += len(chunk)
                    if size > max_bytes:
                        raise SecFetchError(f"response exceeded {max_bytes} bytes",
                                            kind="too_large", retryable=False, status=status)
                    chunks.append(chunk)
                return FetchResult(current, status, content_type, b"".join(chunks), datetime.now(timezone.utc))
            finally:
                response.close()
        raise SecFetchError("too many redirects", kind="bad_redirect", retryable=False)

    def get(self, url: str, *, allowed_hosts: frozenset[str], max_bytes: int,
            accept_types: tuple[str, ...]) -> FetchResult:
        attempt = 0
        while True:
            try:
                return self._get_once(url, allowed_hosts, max_bytes, accept_types)
            except SecFetchError as error:
                if not error.retryable or attempt >= self.max_retries:
                    raise
                delay = getattr(error, "retry_after", None)
                attempt += 1
                if delay is None:
                    delay = min(2 ** attempt, 30) + random.uniform(0, 1)
                delay = min(delay, MAX_RETRY_AFTER_SECONDS)
                LOGGER.warning("SEC request failed (%s); retry %d/%d in %.1fs", error, attempt, self.max_retries, delay)
                self._sleep(delay)
            except requests.RequestException as error:
                if attempt >= self.max_retries:
                    raise SecFetchError(f"network error: {error}", kind="network", retryable=True) from error
                attempt += 1
                delay = min(2 ** attempt, 30) + random.uniform(0, 1)
                LOGGER.warning("SEC network error (%s); retry %d/%d in %.1fs", error, attempt, self.max_retries, delay)
                self._sleep(delay)

    # -- high level --------------------------------------------------------------
    def _json(self, url: str, max_bytes: int) -> tuple[dict, FetchResult]:
        result = self.get(url, allowed_hosts=SUBMISSIONS_HOSTS, max_bytes=max_bytes, accept_types=JSON_TYPES)
        try:
            data = json.loads(result.body)
        except ValueError as error:
            raise SecFetchError(f"invalid JSON from {url}", kind="bad_json", retryable=True) from error
        if not isinstance(data, dict):
            raise SecFetchError(f"unexpected JSON shape from {url}", kind="bad_json", retryable=False)
        return data, result

    def fetch_submissions(self, cik10: str, max_bytes: int) -> tuple[dict, FetchResult]:
        """Always a fresh request (the 12-hour UI cache is not consulted)."""
        return self._json(SUBMISSIONS_URL.format(cik=cik10.zfill(10)), max_bytes)

    def fetch_historical(self, name: str, max_bytes: int) -> tuple[dict, FetchResult]:
        if "/" in name or ".." in name or not name.endswith(".json"):
            raise SecFetchError(f"invalid historical submissions filename {name!r}", kind="invalid_url", retryable=False)
        return self._json(HISTORICAL_URL.format(name=name), max_bytes)

    def fetch_document(self, url: str, max_bytes: int) -> FetchResult:
        """Download a filing document; reject error pages and empty bodies."""
        accept = HTML_TYPES + TEXT_TYPES
        result = self.get(url, allowed_hosts=ARCHIVE_HOSTS, max_bytes=max_bytes, accept_types=accept)
        if not result.body.strip():
            raise SecFetchError(f"empty document body from {url}", kind="empty_document", retryable=True)
        if looks_like_error_page(result.text()):
            raise SecFetchError(f"SEC served an error page for {url}", kind="error_page", retryable=True)
        return result
