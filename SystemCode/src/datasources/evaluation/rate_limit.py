"""Stay under a hosted model's per-minute limits during an evaluation run (free tiers: Groq, Gemini, ...).

Wrap the experiment's comparator (used by experiments/groq.py):

    return RateLimited(OpenAICompatibleComparisonBackend(...), requests_per_minute=30, tokens_per_minute=8000)

Before each call it waits until the last 60 seconds hold fewer than `requests_per_minute` calls and leave room for
another call of the size of the largest recent one (input + output tokens, from the API's usage report). If the
API still answers HTTP 429, it waits a minute and retries, up to `retries` times. `None` turns a limit off.
"""

from __future__ import annotations

import time
from collections import deque

from datasources.similarity.comparison import ComparisonError

WINDOW_SECONDS = 60.0


class RateLimited:
    def __init__(self, inner, *, requests_per_minute: int | None = None, tokens_per_minute: int | None = None,
                 retries: int = 3):
        self.inner, self.rpm, self.tpm, self.retries = inner, requests_per_minute, tokens_per_minute, retries
        self.calls: deque[tuple[float, int]] = deque()  # (start time, tokens used) inside the last minute
        self.largest_call = 0  # tokens of the biggest call so far: the estimate for the next one

    def __getattr__(self, attr):  # name, model, ... come from the wrapped backend
        return getattr(self.inner, attr)

    def _wait_for_room(self) -> None:
        while True:
            now = time.monotonic()
            while self.calls and now - self.calls[0][0] >= WINDOW_SECONDS:
                self.calls.popleft()
            used = sum(tokens for _, tokens in self.calls)
            over_rpm = self.rpm is not None and len(self.calls) >= self.rpm
            over_tpm = self.tpm is not None and self.calls and used + self.largest_call > self.tpm
            if not (over_rpm or over_tpm):
                return
            wait = self.calls[0][0] + WINDOW_SECONDS - now  # until the oldest call leaves the window
            print(f"rate limit: waiting {wait:.0f}s ({len(self.calls)} calls, {used} tokens in the last minute)")
            time.sleep(max(wait, 0.1))

    def compare(self, request):
        for attempt in range(self.retries + 1):
            self._wait_for_room()
            started = time.monotonic()
            try:
                result = self.inner.compare(request)
            except ComparisonError as exc:
                self.calls.append((started, self.largest_call))  # a refused call still counts as a request
                if "HTTP 429" not in str(exc) or attempt == self.retries:
                    raise
                print(f"HTTP 429 from the API, retrying in {WINDOW_SECONDS:.0f}s ({attempt + 1}/{self.retries})")
                time.sleep(WINDOW_SECONDS)
                continue
            tokens = int(result.usage.get("input_tokens", 0)) + int(result.usage.get("output_tokens", 0))
            self.largest_call = max(self.largest_call, tokens)
            self.calls.append((started, tokens))
            return result
