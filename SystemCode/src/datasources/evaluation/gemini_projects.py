"""Gemini projects for long evaluation runs: switch project on errors, wait as the provider asks on rate
limits, and draw daily quota only from projects with permitted capacity.

Each project is one API-key variable in SystemCode/.env (GEMINI_API_KEY_1 ... _7). Only the variable NAME
is ever printed or saved; the key itself stays in the environment and in the request header.

Permitted capacity: a run draws daily quota from its HOME project (`--project`, else the first configured
one) and from the projects listed in GEMINI_QUOTA_FAILOVER_PROJECTS, which the user declares to have
independently provisioned capacity (e.g. billing enabled). A project that reports a free-tier quota
(`...-FreeTier` in a 429) is never failover capacity, whatever the list says: moving from one free-tier
project to the next to get around the daily limit is exactly what the provider's terms exclude.

    before a run (`ProjectPool.check`)  each key lists the models (uses no generation quota): an expired,
                                     disabled or rejected key is marked invalid and skipped from the start
    timeout, connection, HTTP 5xx    the next project takes that request at once (capacity projects
                                     first, then any usable one as a stand-in for this request only); when
                                     every project failed, a short backoff and another pass, at most `rounds`
    HTTP 401 / 403 / expired key     the project is marked invalid; the next usable project takes over
    HTTP 429, rate limit             wait what the provider asks (Retry-After header, RetryInfo), same project
    HTTP 429, quota exhausted        a per-day quota, a "limit: 0" quota, or a wait longer than
                                     `max_rate_limit_wait`: the project is unavailable until its quota resets;
                                     the request continues on the next project with permitted capacity, and
                                     when there is none the run stops with the earliest permitted reset time
                                     (the runner waits for it and resumes)
    HTTP 404, other 4xx              the run stops: the same request fails on every project
    invalid / truncated answer       asked again on the same project when the error says it is retryable

Marks, tiers and counters persist in a small JSON state file, so `--dry-run` shows them and a later run
never calls a project that is known to be exhausted before its reset time.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

import httpx

from ..ingest.store import atomic_write_json
from ..similarity.comparison import ComparisonError

LOGGER = logging.getLogger(__name__)  # --verbose request logs: variable names, sizes, labels; never a key or text
PACIFIC = ZoneInfo("America/Los_Angeles")  # Gemini API per-day quotas reset at midnight Pacific time
TRANSIENT = ("timeout", "connection", "unavailable")
AUTH_MARKERS = re.compile(r"API_KEY_INVALID|API key not valid|API key expired|API_KEY_EXPIRED|SERVICE_DISABLED")
COUNTERS = ("attempts", "successes") + TRANSIENT + ("rate_limited", "quota_exhausted", "auth", "output", "fatal")
# Fixture file names carry the answer (type and group); none may ever appear in what the model is sent.
ANSWER_KEY = re.compile(r"test_\d{3}_(?:original|repeat|update|conflict|different)_group_\d{3}", re.I)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds") if value else None


def _parse_iso(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def next_quota_reset(now: datetime) -> datetime:
    """The next midnight Pacific time (when Gemini's per-day quotas reset), in UTC."""
    local = now.astimezone(PACIFIC)
    midnight = datetime.combine(local.date() + timedelta(days=1), datetime.min.time(), tzinfo=PACIFIC)
    return midnight.astimezone(timezone.utc)


# ----------------------------------------------------------------------------- what went wrong
@dataclass(frozen=True)
class Failure:
    kind: str  # timeout | connection | unavailable | rate_limited | quota_exhausted | auth | output | fatal
    message: str  # the provider's message, shortened; never a header or a key
    status: int | None = None
    retry_after: float | None = None  # seconds the provider asked to wait
    quota_id: str | None = None
    reset_at: datetime | None = None  # when an exhausted quota comes back


def _short(text: str, limit: int = 300) -> str:
    return re.sub(r"\s+", " ", text or "").strip()[:limit]


def _seconds(value: Any, now: datetime) -> float | None:
    """'33s' / '33.5s' (google.rpc.RetryInfo), '33' or an HTTP date (Retry-After) -> seconds."""
    if value in (None, ""):
        return None
    text = str(value).strip()
    plain = re.fullmatch(r"(\d+(?:\.\d+)?)\s*s?", text)
    if plain:
        return float(plain.group(1))
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max((when - now).total_seconds(), 0.0)


def _google_error(body: str | None) -> dict[str, Any]:
    """The `error` object of a Google API error body (the OpenAI-compatible endpoint wraps it in a list)."""
    try:
        data = json.loads(body or "")
    except ValueError:
        return {}
    if isinstance(data, list):
        data = data[0] if data else {}
    error = data.get("error") if isinstance(data, dict) else None
    return error if isinstance(error, dict) else {}


def classify(error: ComparisonError, *, now: datetime | None = None) -> Failure:
    """Map a backend `ComparisonError` (llm_openai.call_openai_json) onto what the retry loop should do."""
    now = now or utcnow()
    status = getattr(error, "status_code", None)
    if status is None:
        match = re.search(r"\bHTTP (\d{3})\b", str(error))
        status = int(match.group(1)) if match else None
    body = getattr(error, "body", None) or ""
    details = _google_error(body)
    message = _short(details.get("message") or str(error))
    if error.kind == "timeout" or status == 408:
        return Failure("timeout", message, status)
    if error.kind == "connection":
        return Failure("connection", message, status)
    if status is None:  # no HTTP error: the answer itself was unusable (schema, truncated) or a setup problem
        return Failure("fatal" if error.kind == "configuration" else "output", message)
    if status == 429:
        return _quota_failure(error, details, body, message, now)
    if status >= 500 or status == 409:
        return Failure("unavailable", message, status)
    if status in (401, 403) or AUTH_MARKERS.search(body):
        return Failure("auth", message, status)
    return Failure("fatal", message, status)


def _quota_failure(error, details: dict[str, Any], body: str, message: str, now: datetime) -> Failure:
    retry = _seconds(getattr(error, "retry_after", None), now)
    quota_ids: list[str] = []
    for detail in details.get("details") or []:
        if not isinstance(detail, dict):
            continue
        if str(detail.get("@type", "")).endswith("RetryInfo") and retry is None:
            retry = _seconds(detail.get("retryDelay"), now)
        quota_ids += [str(v["quotaId"]) for v in detail.get("violations") or [] if isinstance(v, dict)
                      and v.get("quotaId")]
    text = f"{message} {body}"
    if retry is None:
        hint = re.search(r"retry in (\d+(?:\.\d+)?)\s*s", text, re.I)
        retry = float(hint.group(1)) if hint else None
    quota_id = ", ".join(dict.fromkeys(quota_ids)) or None
    daily = any("perday" in q.lower() for q in quota_ids) or bool(re.search(r"per[ _-]?day", text, re.I))
    no_quota = bool(re.search(r"\blimit:\s*0\b", text))  # this model has no quota at all on this project's tier
    if daily or no_quota:
        return Failure("quota_exhausted", message, 429, retry, quota_id, next_quota_reset(now))
    return Failure("rate_limited", message, 429, retry, quota_id)


# ----------------------------------------------------------------------------- the projects and their health
@dataclass
class ProjectState:
    status: str = "available"  # available | exhausted | invalid
    until: str | None = None  # ISO UTC: when an exhausted quota comes back
    reason: str | None = None
    quota_id: str | None = None
    counts: dict[str, int] = field(default_factory=dict)
    tokens: dict[str, int] = field(default_factory=lambda: {"input": 0, "output": 0})
    last_error: str | None = None
    last_error_at: str | None = None
    last_success_at: str | None = None
    tier: str | None = None  # "free" once a 429 named a free-tier quota; None = not known


class ProjectUnavailable(RuntimeError):
    """No project may take the run now (not configured, invalid, or exhausted)."""

    def __init__(self, message: str, *, kind: str = "unavailable", project: str | None = None,
                 until: str | None = None):
        super().__init__(message)
        self.kind, self.project, self.until = kind, project, until


class ProjectPool:
    """The configured projects in a fixed order, what happened to each, and which may be used now."""

    def __init__(self, names: Iterable[str], *, state_path: Path | None = None,
                 environ: Mapping[str, str] | None = None, now: Callable[[], datetime] = utcnow,
                 quota_failover: Iterable[str] = ()):
        environ = os.environ if environ is None else environ
        self._environ = environ  # read only by `check`, for the request header
        self.names = list(dict.fromkeys(names))
        # declared by the user: projects with independently provisioned capacity (GEMINI_QUOTA_FAILOVER_PROJECTS)
        self.quota_failover = [n for n in dict.fromkeys(quota_failover) if n in self.names]
        self.configured: list[str] = []
        self.duplicates: dict[str, str] = {}  # name -> earlier name holding the same key (one project, not two)
        first: dict[str, str] = {}
        for name in self.names:
            value = (environ.get(name) or "").strip()
            if not value:
                continue
            if value in first:
                self.duplicates[name] = first[value]
            else:
                first[value] = name
                self.configured.append(name)
        self.state_path, self.now = state_path, now
        self.states = {name: ProjectState() for name in self.names}
        if state_path is not None and state_path.is_file():
            saved = json.loads(state_path.read_text(encoding="utf-8")).get("projects") or {}
            for name, raw in saved.items():
                if name in self.states:
                    self.states[name] = ProjectState(**{k: v for k, v in raw.items()
                                                        if k in ProjectState.__dataclass_fields__})

    def save(self) -> None:
        if self.state_path is not None:
            atomic_write_json(self.state_path, {
                "updated_at": iso(self.now()), "note": "API-key variable names only; no key is ever stored",
                "projects": {name: asdict(state) for name, state in self.states.items()}})

    def usable(self, name: str) -> tuple[bool, str]:
        if name not in self.configured:
            return False, f"same key as {self.duplicates[name]}" if name in self.duplicates else "not set in .env"
        state = self.states[name]
        if state.status == "invalid":
            return False, f"key rejected ({state.reason})"
        if state.status == "exhausted":
            until = _parse_iso(state.until)
            if until is not None and until > self.now():
                return False, f"quota exhausted until {state.until}" + (f" ({state.quota_id})" if state.quota_id
                                                                        else "")
        if state.status != "available":  # an exhausted quota that has reset, or a mark this version no longer sets
            state.status, state.until, state.reason, state.quota_id = "available", None, None, None
        return True, "available"

    def rotation(self, start: str | None) -> list[str]:
        """The usable projects in configuration order, beginning at `start` and wrapping around (2 -> 3 -> 1)."""
        order = self.configured
        if start in order:
            order = order[order.index(start):] + order[:order.index(start)]
        return [name for name in order if self.usable(name)[0]]

    def check(self, base_url: str, model: str, *, get: Callable[..., Any] | None = None,
              timeout: float = 20.0) -> dict[str, str]:
        """Ask every configured project for its model list, which uses no generation quota. A rejected,
        expired or disabled key is marked invalid (skipped from the start); a key that answers again loses an
        old invalid mark. Says nothing about quota: that is only known from a real request."""
        get = get or httpx.get
        wanted = model.split("/")[-1]
        results: dict[str, str] = {}
        for name in self.configured:
            try:
                response = get(f"{base_url.rstrip('/')}/models", timeout=timeout,
                               headers={"Authorization": f"Bearer {self._environ[name].strip()}"})
            except httpx.HTTPError as error:
                results[name] = f"not checked ({type(error).__name__})"
                continue
            body = response.text[:20000]
            if response.status_code == 200:
                try:
                    listed = {str(m.get("id", "")).split("/")[-1] for m in response.json().get("data") or []}
                except (ValueError, AttributeError):
                    listed = set()
                if self.states[name].status == "invalid":
                    self.mark(name, "available")
                results[name] = "key ok" + ("" if not listed or wanted in listed else f", but {model} is not "
                                                                                    "in its model list")
            elif response.status_code in (401, 403) or AUTH_MARKERS.search(body):
                reason = _short(_google_error(body).get("message") or f"HTTP {response.status_code}")
                self.mark(name, "invalid", reason=reason)
                results[name] = f"KEY REJECTED, skipped: {reason[:160]}"
            elif response.status_code == 429:
                results[name] = "key ok (rate limited right now)"
            else:
                results[name] = f"not checked (HTTP {response.status_code})"
            if self.states[name].status == "exhausted" and not self.usable(name)[0]:
                results[name] += f"; {self.usable(name)[1]}"
        return results

    # ------------------------------------------------------------------------- permitted capacity
    def permitted(self, name: str, home: str) -> bool:
        """May a run with this home draw daily quota from `name`? The home project itself, or a declared
        quota-failover project that never reported a free-tier quota."""
        return name == home or (name in self.quota_failover and self.states[name].tier != "free")

    def capacity_names(self, home: str) -> list[str]:
        """Home first, then the declared projects that are permitted (usable or not)."""
        return [n for n in [home] + [m for m in self.quota_failover if m != home]
                if n in self.configured and self.permitted(n, home)]

    def capacity(self, home: str) -> list[str]:
        """The projects with permitted capacity that may take a request now, home first."""
        return [n for n in self.capacity_names(home) if self.usable(n)[0]]

    def earliest_reset(self, names: Iterable[str]) -> str | None:
        """The first time one of `names` gets its exhausted quota back; None when none is waiting for a reset."""
        now = self.now()
        resets = [_parse_iso(self.states[n].until) for n in names if self.states[n].status == "exhausted"]
        resets = [t for t in resets if t is not None and t > now]
        return iso(min(resets)) if resets else None

    def choose_start(self, requested: str | None = None) -> str:
        """The run's home project: `requested`, else the first configured project whose key was not rejected.

        Never steps past an exhausted home project to an arbitrary one: when the home project has used up its
        quota, only a declared project with permitted capacity may stand in. When none can, this raises with the
        earliest permitted reset time, which the runner waits for."""
        if requested:
            if requested not in self.names:
                raise ProjectUnavailable(f"{requested} is not a configured project variable "
                                         f"({', '.join(self.names)})", kind="configuration")
            if requested not in self.configured or self.states[requested].status == "invalid":
                raise ProjectUnavailable(f"{requested}: {self.usable(requested)[1]}", kind="unavailable",
                                         project=requested)
            home = requested
        else:
            home = next((n for n in self.configured if self.states[n].status != "invalid"), None)
            if home is None:
                raise ProjectUnavailable("no usable project: " + "; ".join(f"{n}: {self.usable(n)[1]}"
                                                                          for n in self.names)
                                         if self.names else "no project variable configured")
        if self.capacity(home):
            return home
        raise ProjectUnavailable(f"{home}: {self.usable(home)[1]}; no project with permitted capacity is usable",
                                 kind="quota_exhausted", project=home,
                                 until=self.earliest_reset(self.capacity_names(home)))

    def _count(self, name: str, counter: str) -> None:
        counts = self.states[name].counts
        counts[counter] = counts.get(counter, 0) + 1

    def record_attempt(self, name: str) -> None:
        self._count(name, "attempts")
        self.save()

    def record_success(self, name: str, usage: Mapping[str, Any]) -> None:
        state = self.states[name]
        self._count(name, "successes")
        state.tokens["input"] = state.tokens.get("input", 0) + int(usage.get("input_tokens", 0) or 0)
        state.tokens["output"] = state.tokens.get("output", 0) + int(usage.get("output_tokens", 0) or 0)
        state.last_success_at = iso(self.now())
        self.save()

    def record_failure(self, name: str, failure: Failure) -> None:
        state = self.states[name]
        self._count(name, failure.kind)
        if failure.quota_id and "freetier" in failure.quota_id.lower():
            state.tier = "free"  # the provider says so; a declaration in .env never overrides it
        state.last_error = _short(f"{failure.kind}" + (f" HTTP {failure.status}" if failure.status else "")
                                  + f": {failure.message}")
        state.last_error_at = iso(self.now())
        self.save()

    def mark(self, name: str, status: str, *, until: datetime | None = None, reason: str | None = None,
             quota_id: str | None = None) -> None:
        state = self.states[name]
        state.status, state.until, state.reason, state.quota_id = status, iso(until), _short(reason or ""), quota_id
        self.save()

    def reset(self, names: Iterable[str] | None = None) -> None:
        """Forget invalid / exhausted marks (counters stay), e.g. after fixing a key or enabling billing."""
        for name in names or self.names:
            state = self.states[name]
            state.status, state.until, state.reason, state.quota_id = "available", None, None, None
        self.save()

    def describe(self) -> list[str]:
        lines = []
        for name in self.names:
            ok, why = self.usable(name)
            state = self.states[name]
            counts = ", ".join(f"{k} {state.counts[k]}" for k in COUNTERS if state.counts.get(k))
            line = f"  {name:<20} {'usable' if ok else 'NOT usable'}: {why}"
            if state.tier:
                line += f" [{state.tier} tier]"
            if name in self.quota_failover:
                line += (" [declared quota failover, ignored: free tier]" if state.tier == "free"
                         else " [declared quota failover]")
            if counts:
                line += f"\n  {'':<20} {counts}; tokens in/out {state.tokens.get('input', 0)}/" \
                        f"{state.tokens.get('output', 0)}"
            if state.last_error and not ok:
                line += f"\n  {'':<20} last error {state.last_error_at}: {state.last_error[:160]}"
            lines.append(line)
        return lines


# ----------------------------------------------------------------------------- the comparator the pipeline calls
@dataclass(frozen=True)
class RetryPolicy:
    attempts_per_project: int = 1  # timeout / connection / 5xx tries on one project before the next takes over
    rounds: int = 3  # passes over all usable projects for one request before the run stops as unavailable
    base_delay: float = 10.0  # backoff between passes (and repeated tries): 10, 20 ... s, half fixed, half random
    max_delay: float = 120.0
    rate_limit_retries: int = 4  # 429 waits for one request on one project
    max_rate_limit_wait: float = 180.0  # a 429 asking for a longer wait counts as an exhausted quota
    unknown_reset: float = 900.0  # an exhausted quota without reset information blocks its project this long
    output_retries: int = 1  # invalid / truncated answers asked again (only if the error is retryable)

    def backoff(self, failures: int, rng: random.Random) -> float:
        delay = min(self.max_delay, self.base_delay * 2 ** max(failures - 1, 0))
        return delay / 2 + rng.uniform(0, delay / 2)


@dataclass
class Stop:
    """Why the run cannot go on; every later `compare()` fails at once without sending anything."""
    kind: str  # quota_exhausted | unavailable | fatal
    message: str
    project: str | None = None
    until: str | None = None


class RecoveringComparator:
    """The pipeline's comparator (`compare(request)`) with bounded retries and project fail-over.

    Holds no key: each project's `OpenAICompatibleComparisonBackend` reads its own variable."""

    name = "openai_compatible"  # what the original run recorded as its comparator

    def __init__(self, pool: ProjectPool, build_backend: Callable[[str], Any], *, model: str, start: str,
                 policy: RetryPolicy | None = None, sleep: Callable[[float], None] = time.sleep,
                 rng: random.Random | None = None, log: Callable[[str], None] = print):
        self.pool, self.build_backend, self.model = pool, build_backend, model
        self.home = self.active = start  # home: the run's own project; active: the capacity project in use
        self.policy, self.sleep, self.rng, self.log = policy or RetryPolicy(), sleep, rng or random.Random(), log
        self.stop: Stop | None = None
        self.batch_label = "the batch"  # set by the runner, for "continuing batch 14 on ..."
        self._backends: dict[str, Any] = {}
        self.begin_batch()

    def resume(self) -> None:
        """Forget a stop once the runner waited for capacity to come back (a quota reset, an outage)."""
        self.stop = None

    def _order(self) -> list[str]:
        """The projects for one request: the active capacity project, the other capacity projects, then every
        other usable project as an error-only stand-in. [] when no project with permitted capacity is usable."""
        if self.pool.states[self.home].status == "invalid":  # a rejected key is no quota matter: new home
            try:
                home = self.pool.choose_start()
            except ProjectUnavailable as error:
                home = error.project
            if home and home != self.home:
                self.log(f"    {self.home}: key rejected; the run's home project is now {home}")
                self.home = self.active = home
        capacity = self.pool.capacity(self.home)
        if not capacity:
            return []
        primary = self.active if self.active in capacity else capacity[0]
        stand_ins = [n for n in self.pool.rotation(primary) if n not in capacity]
        return [primary] + [n for n in capacity if n != primary] + stand_ins

    def begin_batch(self) -> None:
        self.stats: dict[str, Any] = {"http_attempts": 0, "projects": [], "failures": {}, "waited_seconds": 0.0,
                                      "switches": []}

    def _halt(self, kind: str, message: str, *, project: str | None = None, until: str | None = None) -> None:
        self.stop = Stop(kind, message, project, until)
        self.log(f"    STOP ({kind}): {message}")

    def _wait(self, seconds: float, why: str) -> None:
        self.log(f"    {why}; waiting {seconds:.0f}s")
        self.stats["waited_seconds"] = round(self.stats["waited_seconds"] + seconds, 1)
        self.sleep(seconds)

    def compare(self, request):
        if self.stop is not None:
            raise ComparisonError(f"not sent: {self.stop.message}", kind=self.stop.kind, retryable=False)
        if ANSWER_KEY.search(request.render()):
            self._halt("fatal", "a fixture file name (the answer key) is in the prompt; nothing was sent")
            raise ComparisonError(self.stop.message, kind="fatal", retryable=False)
        failure = error = None
        for round_ in range(1, self.policy.rounds + 1):
            tried: list[str] = []
            while True:
                order = self._order()
                if not order:
                    self._no_capacity(error)  # raises: the runner waits for the earliest permitted reset
                remaining = [n for n in order if n not in tried]
                if not remaining:
                    break
                name = remaining[0]
                call, failure, error = self._on_project(name, request)
                if call is not None:
                    if name in self.pool.capacity(self.home):
                        self.active = name  # a stand-in serves this request only, never becomes the run's project
                    return call
                tried.append(name)
                following = [n for n in self._order() if n not in tried]
                if failure.kind == "quota_exhausted" and self.pool.permitted(name, self.home):
                    self._quota_handover(name)
                elif following:  # switch at once: the next project takes the same request
                    self.log(f"    switching {name} -> {following[0]} after {failure.kind}")
                    self.stats["switches"].append({"from": name, "to": following[0], "after": failure.kind})
            if round_ < self.policy.rounds and self._order():
                self._wait(self.policy.backoff(round_, self.rng),
                           f"every usable project failed ({failure.kind}); pass {round_ + 1}/{self.policy.rounds}")
        what = failure.kind if failure else "no usable project"
        self._halt("unavailable", f"{what} on every usable project after {self.policy.rounds} pass(es)")
        raise ComparisonError(f"{self.model}: {self.stop.message}", kind="unavailable", retryable=True) from error

    def _quota_handover(self, name: str) -> None:
        state = self.pool.states[name]
        self.log(f"    {name}: HTTP 429, daily quota exhausted ({state.quota_id or 'quota'})")
        self.log(f"    {name}: project unavailable until quota reset {state.until}")
        capacity = self.pool.capacity(self.home)
        if capacity:
            self.log(f"    selecting another authorized project with permitted capacity: {capacity[0]}")
            self.log(f"    continuing {self.batch_label} on {capacity[0]}")
            self.stats["switches"].append({"from": name, "to": capacity[0], "after": "quota_exhausted"})

    def _no_capacity(self, error) -> None:
        names = self.pool.capacity_names(self.home)
        until = self.pool.earliest_reset(names)
        self._halt("quota_exhausted", f"all projects with permitted capacity are unavailable ({', '.join(names)}); "
                   + (f"earliest permitted quota reset {until}" if until else "no reset time is known"),
                   project=self.home, until=until)
        raise ComparisonError(f"{self.model}: {self.stop.message}", kind="quota_exhausted", retryable=False) from error

    def _backend(self, name: str):
        if name not in self._backends:
            self._backends[name] = self.build_backend(name)
        return self._backends[name]

    def _on_project(self, name: str, request):
        """(call, None, None) on success; (None, failure, error) when this project should hand over."""
        policy, transient, waits, output_retries = self.policy, 0, 0, 0
        if name not in self.stats["projects"]:
            self.stats["projects"].append(name)
        while True:
            self.pool.record_attempt(name)
            self.stats["http_attempts"] += 1
            backend, started = self._backend(name), time.monotonic()
            if LOGGER.isEnabledFor(logging.INFO):
                timeout = getattr(backend, "timeout", None)
                LOGGER.info("    Gemini request -> %s (%s, attempt %d this batch): %d segment(s) x %d candidate "
                            "event(s), %d pair(s), prompt %d chars, timeout %s", name, self.model,
                            self.stats["http_attempts"], len(request.segments), len(request.events),
                            request.pair_count(), len(request.render()),
                            f"{timeout:.0f}s" if isinstance(timeout, (int, float)) else "n/a")
            try:
                call = backend.compare(request)
            except ComparisonError as error:
                now = self.pool.now()
                failure = classify(error, now=now)
                detail = ""
                if error.kind == "invalid_response":
                    detail = ": the answer did not pass schema validation"
                elif failure.kind == "output":
                    detail = f": {error.kind}"
                LOGGER.info("    Gemini %s failed after %.1fs: %s%s", name, time.monotonic() - started,
                            failure.kind, detail)
                was_free = self.pool.states[name].tier == "free"
                self.pool.record_failure(name, failure)
                if name in self.pool.quota_failover and not was_free and self.pool.states[name].tier == "free":
                    self.log(f"    {name} reports a free-tier quota ({failure.quota_id}): declared for quota failover, "
                             "but it is not independently provisioned capacity and is not used as such")
                self.stats["failures"][failure.kind] = self.stats["failures"].get(failure.kind, 0) + 1
                label = f"[{name}] {failure.kind}" + (f" (HTTP {failure.status})" if failure.status else "")
                if failure.kind in TRANSIENT:
                    transient += 1
                    if transient < policy.attempts_per_project:
                        self._wait(policy.backoff(transient, self.rng),
                                   f"{label}, retry {transient}/{policy.attempts_per_project - 1}")
                        continue
                    self.log(f"    {label}: {failure.message[:120]}")
                    return None, failure, error
                if failure.kind == "rate_limited":
                    waits += 1
                    wait = failure.retry_after if failure.retry_after is not None else policy.backoff(waits, self.rng)
                    if waits <= policy.rate_limit_retries and wait <= policy.max_rate_limit_wait:
                        self._wait(wait, f"{label}: the provider asks to wait")
                        continue
                    failure = replace(failure, kind="quota_exhausted",
                                      reset_at=now + timedelta(seconds=wait if wait > policy.max_rate_limit_wait
                                                               else policy.unknown_reset))
                if failure.kind == "quota_exhausted":  # unavailable until the reset; `compare` decides what next
                    until = failure.reset_at or now + timedelta(seconds=policy.unknown_reset)
                    self.pool.mark(name, "exhausted", until=until, reason=failure.message, quota_id=failure.quota_id)
                    return None, failure, error
                if failure.kind == "auth":
                    self.pool.mark(name, "invalid", reason=failure.message)
                    return None, failure, error
                if failure.kind == "output" and error.retryable and output_retries < policy.output_retries:
                    output_retries += 1
                    self.log(f"    {label}: {failure.message[:120]}; asking again")
                    continue
                if failure.kind == "fatal":
                    self._halt("fatal", f"[{name}] {failure.message}")
                raise  # an unusable answer: this article fails and its batch is rerun later
            if LOGGER.isEnabledFor(logging.INFO):
                pairs = [f"{s.segment_ref}->{j.candidate_event_ref} {j.label}" for s in call.output.segments
                         for j in s.judgments]
                LOGGER.info("    Gemini %s answered in %.1fs, schema valid: %s", name, time.monotonic() - started,
                            "; ".join(pairs) or "no judgment")
            self.pool.record_success(name, call.usage)
            return call, None, None
