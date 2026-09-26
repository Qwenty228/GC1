"""Collection services around the preprocessing graph.

One asyncio process per output directory (enforced by a file lock) runs:

    * one SEC poller per configured company (fresh submissions every
      ~SEC_POLL_INTERVAL_SECONDS, independent of the 12-hour UI cache, which is
      refreshed after each successful poll);
    * the existing Alpaca WebSocket client (`alpaca_news.client.run_forever`)
      with ingestion callbacks;
    * Alpaca REST backfill at startup and after every (re)subscription, from the
      last synced cursor minus an overlap window, following pagination;
    * a single writer task that consumes a bounded queue in FIFO order, makes the
      authoritative duplicate decision against the ledger, downloads SEC
      documents, runs the graph and writes outputs.

Because exactly one writer makes every decision sequentially, a REST backfill
and a WebSocket delivery of the same article revision can never be processed
concurrently; an in-flight key set also keeps duplicates out of the queue.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from filelock import FileLock, Timeout

from .. import config, sec
from ..alpaca import AlpacaAuth, iter_news_pages
from .adapters import (
    alpaca_content_level,
    alpaca_fingerprint,
    parse_timestamp,
    sec_context_from_submissions,
    unwrap_payload,
)
from .graph import run_prepare
from .ledger import Ledger
from .schema import SecContext, SourceEnvelope
from .sec_client import FetchResult, SecClient, SecFetchError
from .settings import ConfigurationError, IngestSettings
from .store import OutputStore

LOGGER = logging.getLogger(__name__)
ALPACA_CURSOR = "alpaca_synced_through"
WS_ENDPOINT = "wss://stream.data.alpaca.markets/v1beta1/news"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class WorkItem:
    kind: str  # "sec" | "alpaca"
    key: str
    payload: dict[str, Any]
    done: asyncio.Future | None = field(default=None, repr=False)


DocumentFetcher = Callable[[str, dict, SecContext], FetchResult]


class ProcessLock:
    """One ingestion worker per output directory (cross-platform file lock)."""

    def __init__(self, root: Path):
        self.path = root / "state" / "ingest.lock"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = FileLock(str(self.path))

    def __enter__(self) -> "ProcessLock":
        try:
            self._lock.acquire(timeout=0)
        except Timeout as error:
            raise ConfigurationError(
                f"another ingestion worker already holds {self.path}; run one worker per output directory"
            ) from error
        return self

    def __exit__(self, *exc) -> None:
        self._lock.release()


class IngestionService:
    def __init__(
        self,
        settings: IngestSettings,
        *,
        sec_client: SecClient | None = None,
        document_fetcher: DocumentFetcher | None = None,
        alpaca_auth: AlpacaAuth | None = None,
        alpaca_session=None,
        update_shared_cache: bool = True,
        database=None,
    ):
        self.settings = settings
        self.store = OutputStore(settings.root)
        self.ledger = Ledger(self.store)
        # Optional `datasources.db.Database`. When None, ingestion behaves exactly
        # as before and writes files only.
        self.database = database
        self.sec_client = sec_client
        self.document_fetcher = document_fetcher or self._fetch_document_live
        self.alpaca_auth = alpaca_auth
        self.alpaca_session = alpaca_session
        self.update_shared_cache = update_shared_cache and not settings.demo
        self.stats: Counter[str] = Counter()
        self.queue: asyncio.Queue[WorkItem] | None = None
        self.in_flight: set[str] = set()
        self._historical_cache: dict[tuple[str, str], dict] = {}
        self._backfill_lock: asyncio.Lock | None = None
        self._backfill_again = False
        self._current: asyncio.Future | None = None
        self.stream_subscribed = False
        self._last_ws_ref: str | None = None

    # ------------------------------------------------------------------ lifecycle
    def startup(self) -> dict[str, int]:
        self.store.ensure_dirs()
        self.ledger.load()
        summary = self.ledger.reconcile()
        LOGGER.info("ledger reconciled: %s; totals %s", summary, self.ledger.counts())
        return summary

    def _ensure_async_state(self) -> None:
        if self.queue is None:
            self.queue = asyncio.Queue(maxsize=self.settings.queue_size)
            self._backfill_lock = asyncio.Lock()

    async def enqueue(self, item: WorkItem) -> bool:
        self._ensure_async_state()
        assert self.queue is not None
        if item.key in self.in_flight:
            self.stats["skipped_in_flight"] += 1
            if item.done is not None and not item.done.done():
                item.done.set_result("in_flight")
            return False
        self.in_flight.add(item.key)
        await self.queue.put(item)  # bounded: back-pressure on producers
        return True

    async def writer(self) -> None:
        """Single coordinated writer: processes queue items one at a time, FIFO."""
        self._ensure_async_state()
        assert self.queue is not None
        while True:
            item = await self.queue.get()
            outcome = "error"
            try:
                self._current = asyncio.ensure_future(asyncio.to_thread(self.process_item, item))
                try:
                    outcome = await asyncio.shield(self._current)
                except asyncio.CancelledError:
                    # Finish the record being written, then stop; queued items stay unmarked.
                    outcome = await self._current
                    raise
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("unexpected failure processing %s", item.key)
            finally:
                self._current = None
                self.in_flight.discard(item.key)
                self.queue.task_done()
                if item.done is not None and not item.done.done():
                    item.done.set_result(outcome)

    def process_item(self, item: WorkItem) -> str:
        if item.kind == "sec":
            return self.process_sec(item.payload)
        return self.process_alpaca(item.payload)

    # ------------------------------------------------------------------ persistence
    def persist(self, record, *, raw_payload: dict | None = None, raw_document: str | None = None):
        """Store one record in PostgreSQL, or return None when no database is configured.

        Called from `process_sec` / `process_alpaca`, which the writer already runs
        off the event loop via `asyncio.to_thread`, so this synchronous driver call
        never blocks asyncio. Everything for one record goes in one transaction, so
        a failed related insert leaves no partial article behind.

        Raises on failure: the caller then leaves the record unmarked in the ledger
        so a later poll or delivery retries it, rather than silently losing it.
        """
        if self.database is None:
            return None
        from ..db import persist_record

        with self.database.transaction() as conn:
            result = persist_record(conn, record, raw_payload=raw_payload, raw_document=raw_document)
        self.stats[f"db_{result.outcome}"] += 1
        if result.unresolved_symbols:
            self.stats["db_unresolved_symbols"] += len(result.unresolved_symbols)
        return result

    # ------------------------------------------------------------------ SEC collection
    def _sec_floor(self, cik10: str, today: date | None = None) -> str:
        """Oldest filingDate inspected. Fixed at first run, clamped by the recovery limit.

        It never moves forward, so filings that failed or were still queued at
        shutdown are re-inspected on the next poll (already-completed ones are
        skipped by the ledger in memory, without any request).
        """
        today = today or utcnow().date()
        key = f"sec_scope_floor:{cik10}"
        floor = self.ledger.get_value(key)
        if not floor:
            floor = (today - timedelta(days=self.settings.sec_initial_lookback_days)).isoformat()
            self.ledger.set_value(key, floor)
        recovery_floor = (today - timedelta(days=self.settings.sec_recovery_max_days)).isoformat()
        return max(floor, recovery_floor)

    def _historical_rows(self, cik10: str, submissions: dict, floor: str, ctx: SecContext) -> list[dict]:
        """Rows from older submissions files when the recovery range needs them."""
        recent = sec.filing_columns(submissions)
        dates = [d for d in recent.get("filingDate", []) if d]
        if not dates or min(dates) <= floor:
            return []
        files = [f for f in (submissions.get("filings") or {}).get("files", []) if (f.get("filingTo") or "") >= floor]
        files.sort(key=lambda f: f.get("filingTo") or "", reverse=True)
        if len(files) > self.settings.sec_max_historical_files:
            LOGGER.warning("recovery range needs %d historical files for CIK %s; limited to %d",
                           len(files), cik10, self.settings.sec_max_historical_files)
            files = files[: self.settings.sec_max_historical_files]
        rows: list[dict] = []
        for meta in files:
            name = meta["name"]
            cached = self._historical_cache.get((cik10, name))
            if cached is None:
                body = self.store.load_raw_historical(cik10, name)
                if body is None:
                    assert self.sec_client is not None
                    cached, result = self.sec_client.fetch_historical(name, self.settings.sec_max_submissions_bytes)
                    self.store.save_raw_historical(cik10, name, result.body)
                    self.stats["sec_historical_fetched"] += 1
                else:
                    cached = json.loads(body)
                self._historical_cache[(cik10, name)] = cached
            unwrapped, _ = unwrap_payload(cached, provider="sec_edgar", sec_forms=set(self.settings.sec_forms), cik=cik10)
            rows.extend(row for row, _ctx in unwrapped)
        return rows

    def sec_collect(self, symbol: str, submissions: dict, *, raw_ref: str, endpoint: str,
                    retrieved_at: datetime, transport: str = "rest") -> list[WorkItem]:
        """Select filings to process from raw submissions JSON or the /api/edgar wrapper."""
        cik10 = str(submissions.get("cik") or config.cik_for(symbol)).zfill(10)
        rows, info = unwrap_payload(submissions, provider="sec_edgar", sec_forms=set(self.settings.sec_forms), cik=cik10)
        floor = self._sec_floor(cik10) if not self.settings.demo else "0000-00-00"
        candidates = list(rows)
        if not self.settings.demo and "filings" in submissions:
            ctx = sec_context_from_submissions(submissions, cik10)
            candidates += [(row, ctx) for row in self._historical_rows(cik10, submissions, floor, ctx)]
        items: list[WorkItem] = []
        seen: set[str] = set()
        for row, ctx in candidates:
            accession = row.get("accessionNumber")
            filing_date = row.get("filingDate") or ""
            if not accession or accession in seen or filing_date < floor:
                continue
            seen.add(accession)
            identity = f"sec_edgar:{ctx.cik}:{accession}"
            decision = self.ledger.sec_decide(identity)
            if not decision.process:
                self.stats[f"sec_skip_{decision.reason}"] += 1
                continue
            items.append(WorkItem("sec", identity, {
                "identity": identity, "row": row, "context": ctx.model_dump(), "symbol": symbol,
                "endpoint": endpoint, "raw_ref": raw_ref, "retrieved_at": retrieved_at.isoformat(),
                "transport": transport,
            }))
        if info.get("warnings"):
            self.stats["sec_submissions_warnings"] += len(info["warnings"])
        return items

    def sec_poll_once(self, symbol: str) -> list[WorkItem]:
        """Fetch fresh submissions for one company and return filings to process."""
        assert self.sec_client is not None
        cik10 = config.cik_for(symbol)
        submissions, result = self.sec_client.fetch_submissions(cik10, self.settings.sec_max_submissions_bytes)
        self.stats["sec_polls"] += 1
        if self.update_shared_cache:
            sec.save_submissions_cache(cik10, submissions)  # keep the UI's 12h cache current
        raw_ref = self.store.save_raw_submissions(cik10, result.body)
        return self.sec_collect(symbol, submissions, raw_ref=raw_ref, endpoint=result.url, retrieved_at=result.retrieved_at)

    # ------------------------------------------------------------------ SEC processing
    def _fetch_document_live(self, url: str, row: dict, ctx: SecContext) -> FetchResult:
        assert self.sec_client is not None
        return self.sec_client.fetch_document(url, self.settings.sec_max_document_bytes)

    def _sec_failure(self, identity: str, *, stage: str, kind: str, message: str, retryable: bool,
                     payload: dict) -> str:
        error_ref = self.store.write_error("sec_edgar", identity, {
            "identity": identity, "stage": stage, "kind": kind, "message": message,
            "retryable": retryable, "context": payload,
        })
        entry = self.ledger.mark_sec_failed(identity, kind=kind, message=message, retryable=retryable,
                                            base_delay=self.settings.sec_poll_interval_seconds, error_ref=error_ref)
        LOGGER.warning("SEC %s failed at %s (%s): %s; next retry %s", identity, stage, kind, message,
                       entry.get("next_retry_at"))
        self.stats[f"sec_failed_{stage}"] += 1
        return "failed"

    def process_sec(self, payload: dict) -> str:
        identity = payload["identity"]
        decision = self.ledger.sec_decide(identity)
        if not decision.process:
            self.stats[f"sec_skip_{decision.reason}"] += 1
            return "skipped"
        row, ctx = payload["row"], SecContext.model_validate(payload["context"])
        context = {"accession": row.get("accessionNumber"), "primary_document": row.get("primaryDocument")}
        try:
            url = sec.primary_document_url(ctx.cik, row["accessionNumber"], row.get("primaryDocument") or "")
        except (ValueError, KeyError) as error:
            return self._sec_failure(identity, stage="validate", kind="invalid_row", message=str(error),
                                     retryable=False, payload=context)
        context["url"] = url
        try:
            document = self.document_fetcher(url, row, ctx)
        except SecFetchError as error:
            return self._sec_failure(identity, stage="download", kind=error.kind, message=str(error),
                                     retryable=error.retryable, payload={**context, "status": error.status})
        doc_ref = self.store.save_raw_document(ctx.cik, row["accessionNumber"], row["primaryDocument"], document.body)
        envelope = SourceEnvelope(
            provider="sec_edgar",
            transport=payload.get("transport", "rest"),
            api_endpoint=payload["endpoint"],
            retrieved_at=document.retrieved_at,
            requested_symbols=[payload["symbol"]],
            is_sample=self.settings.demo,
            raw_record=row,
            raw_file_refs=[payload["raw_ref"], doc_ref],
            sec_context=ctx,
            primary_document_html=document.text(),
            primary_document_url=url,
        )
        records, errors = run_prepare([envelope])
        if errors:
            error = errors[0]
            return self._sec_failure(identity, stage=error.stage, kind=error.error_type, message=error.message,
                                     retryable=error.retryable, payload={**context, "raw_document": doc_ref})
        record = records[0]
        # Persist before writing the file and marking the ledger: a database failure
        # then leaves the filing pending for the next poll instead of marking it
        # completed with nothing stored. The insert is idempotent, so a crash between
        # the commit and the file write cannot produce a duplicate on the retry.
        try:
            self.persist(record, raw_payload=row, raw_document=document.text())
        except Exception as error:
            return self._sec_failure(identity, stage="persist", kind="database_error", message=str(error),
                                     retryable=True, payload={**context, "raw_document": doc_ref})
        output_ref = self.store.write_processed(record)
        self.ledger.mark_sec_completed(identity, output_ref, record.provenance.dedup.content_fingerprint)
        self.stats["sec_saved"] += 1
        LOGGER.info("saved SEC %s %s -> %s", record.sec_metadata.form if record.sec_metadata else "", identity, output_ref)
        return "saved"

    # ------------------------------------------------------------------ Alpaca
    def alpaca_items(self, articles: list[Any], *, transport: str, endpoint: str, retrieved_at: datetime,
                     raw_refs: list[str]) -> list[WorkItem]:
        items: list[WorkItem] = []
        for article in articles:
            if not isinstance(article, dict):
                self.stats["alpaca_invalid_payload"] += 1
                continue
            fingerprint = alpaca_fingerprint(article)
            identity = f"alpaca:{article.get('id')}"
            updated_at, _ = parse_timestamp(article.get("updated_at"), "updated_at")
            if article.get("id") is not None:
                decision = self.ledger.alpaca_decide(identity, updated_at, fingerprint, alpaca_content_level(article))
                if not decision.process:
                    self.stats[f"alpaca_skip_{decision.reason}"] += 1
                    continue
            items.append(WorkItem("alpaca", f"{identity}:{fingerprint}", {
                "article": article, "fingerprint": fingerprint, "transport": transport, "endpoint": endpoint,
                "retrieved_at": retrieved_at.isoformat(), "raw_refs": raw_refs,
            }))
        return items

    def process_alpaca(self, payload: dict) -> str:
        article = payload["article"]
        fingerprint = payload["fingerprint"]
        identity = f"alpaca:{article.get('id')}"
        retrieved_at = datetime.fromisoformat(payload["retrieved_at"])
        updated_at, _ = parse_timestamp(article.get("updated_at"), "updated_at")
        level = alpaca_content_level(article)
        if article.get("id") is not None:
            decision = self.ledger.alpaca_decide(identity, updated_at, fingerprint, level)
            if not decision.process:
                self.stats[f"alpaca_skip_{decision.reason}"] += 1
                return "skipped"
        else:
            decision = None
        raw_ref = self.store.save_raw_alpaca_article(article, fingerprint, transport=payload["transport"],
                                                     api_endpoint=payload["endpoint"], retrieved_at=retrieved_at)
        envelope = {
            "provider": "alpaca",
            "transport": payload["transport"],
            "api_endpoint": payload["endpoint"],
            "retrieved_at": retrieved_at,
            "requested_symbols": list(self.settings.symbols),
            "is_sample": self.settings.demo,
            "raw_record": article,
            "raw_file_refs": [*payload["raw_refs"], raw_ref],
        }
        records, errors = run_prepare([envelope])
        if errors:
            error = errors[0]
            self.store.write_error("alpaca", identity, {"identity": identity, **error.model_dump(), "raw_article": raw_ref})
            self.stats["alpaca_failed"] += 1
            LOGGER.warning("Alpaca %s rejected at %s: %s", identity, error.stage, error.message)
            return "failed"
        record = records[0]
        try:
            self.persist(record, raw_payload=article)
        except Exception as error:
            self.store.write_error("alpaca", identity, {"identity": identity, "stage": "persist",
                                                        "error_type": "database_error", "message": str(error),
                                                        "retryable": True, "raw_article": raw_ref})
            self.stats["alpaca_failed"] += 1
            LOGGER.warning("Alpaca %s not stored in PostgreSQL: %s", identity, error)
            return "failed"
        output_ref = self.store.write_processed(record)
        self.ledger.mark_alpaca_saved(identity, updated_at=updated_at, fingerprint=fingerprint, level=level,
                                      output_ref=output_ref)
        self.stats["alpaca_saved"] += 1
        LOGGER.info("saved Alpaca %s (%s, %s) -> %s", identity, payload["transport"],
                    decision.reason if decision else "", output_ref)
        if payload["transport"] == "websocket":
            self._advance_cursor(retrieved_at)
        return "saved"

    def _advance_cursor(self, value: datetime) -> None:
        current = self.ledger.get_cursor(ALPACA_CURSOR)
        if current is None or value > current:
            self.ledger.set_cursor(ALPACA_CURSOR, value)

    def backfill_window(self, now: datetime | None = None) -> tuple[datetime, datetime]:
        now = now or utcnow()
        cursor = self.ledger.get_cursor(ALPACA_CURSOR)
        if cursor is None:
            start = now - timedelta(hours=self.settings.alpaca_initial_lookback_hours)
        else:
            start = cursor - timedelta(minutes=self.settings.alpaca_reconnect_overlap_minutes)
        floor = now - timedelta(hours=self.settings.alpaca_recovery_max_hours)
        if start < floor:
            LOGGER.warning("Alpaca recovery window clamped to ALPACA_RECOVERY_MAX_HOURS=%s",
                           self.settings.alpaca_recovery_max_hours)
            start = floor
        return start, now

    async def alpaca_backfill(self, reason: str) -> None:
        """REST backfill/recovery for [cursor - overlap, now]; advances the cursor when done."""
        self._ensure_async_state()
        assert self._backfill_lock is not None
        if self._backfill_lock.locked():
            self._backfill_again = True
            return
        async with self._backfill_lock:
            while True:
                self._backfill_again = False
                await self._backfill_window(reason)
                if not self._backfill_again:
                    break
                reason = "resubscribed_during_backfill"

    async def _backfill_window(self, reason: str) -> None:
        assert self.alpaca_auth is not None
        start, end = self.backfill_window()
        LOGGER.info("Alpaca REST backfill (%s): %s -> %s", reason, start.isoformat(), end.isoformat())
        pages = iter_news_pages(self.alpaca_auth, self.settings.symbols, start=start, end=end,
                                max_pages=self.settings.alpaca_backfill_max_pages,
                                page_size=self.settings.alpaca_page_size, session=self.alpaca_session,
                                timeout=self.settings.http_timeout_seconds)
        futures: list[asyncio.Future] = []
        page_count = 0
        loop = asyncio.get_running_loop()
        while True:
            try:
                page = await asyncio.to_thread(next, pages, None)
            except Exception as error:
                LOGGER.warning("Alpaca REST backfill failed after %d page(s): %s", page_count, error)
                self.stats["alpaca_backfill_errors"] += 1
                return  # cursor not advanced; next backfill retries the window
            if page is None:
                break
            payload, request_url = page
            page_count += 1
            retrieved_at = utcnow()
            page_ref = self.store.save_raw_alpaca_page(payload, request_url, retrieved_at)
            for item in self.alpaca_items(payload.get("news", []), transport="rest", endpoint=request_url,
                                          retrieved_at=retrieved_at, raw_refs=[page_ref]):
                item.done = loop.create_future()
                if await self.enqueue(item):
                    futures.append(item.done)
            if page_count >= self.settings.alpaca_backfill_max_pages and payload.get("next_page_token"):
                LOGGER.warning("Alpaca backfill stopped at ALPACA_BACKFILL_MAX_PAGES=%d; older pages not fetched",
                               self.settings.alpaca_backfill_max_pages)
        if futures:
            await asyncio.gather(*futures)
        self.stats["alpaca_backfill_pages"] += page_count
        self._advance_cursor(end)

    async def on_ws_articles(self, articles: list[dict]) -> None:
        received_at = utcnow()
        refs = [self._last_ws_ref] if self._last_ws_ref else []
        for item in self.alpaca_items(articles, transport="websocket", endpoint=WS_ENDPOINT,
                                      retrieved_at=received_at, raw_refs=refs):
            await self.enqueue(item)

    async def on_ws_subscribed(self) -> None:
        self.stream_subscribed = True
        # Recovery runs as its own task so frame reading is never blocked by it.
        asyncio.create_task(self.alpaca_backfill("websocket_subscribed"))

    def on_ws_frame(self, raw_message: str | bytes) -> None:
        self._last_ws_ref = self.store.append_ws_frame(raw_message, utcnow())

    async def stream_heartbeat(self, interval: float = 60.0) -> None:
        """While subscribed and idle, everything up to now has been received and saved."""
        self._ensure_async_state()
        assert self.queue is not None and self._backfill_lock is not None
        while True:
            await asyncio.sleep(interval)
            if (self.stream_subscribed and self.queue.empty() and self._current is None
                    and not self._backfill_lock.locked()):
                self._advance_cursor(utcnow())

    # ------------------------------------------------------------------ SEC poller
    async def sec_poller(self, symbol: str, initial_delay: float = 0.0) -> None:
        await asyncio.sleep(initial_delay)
        while True:
            try:
                items = await asyncio.to_thread(self.sec_poll_once, symbol)
                for item in items:
                    await self.enqueue(item)
            except KeyError as error:
                LOGGER.error("SEC poller for %s stopped: %s", symbol, error)
                return
            except SecFetchError as error:
                LOGGER.warning("SEC poll for %s failed (%s): %s", symbol, error.kind, error)
                self.stats["sec_poll_errors"] += 1
                if error.kind == "access_denied":
                    await asyncio.sleep(max(self.settings.sec_poll_interval_seconds, 600))
                    continue
            except Exception:
                LOGGER.exception("SEC poll for %s failed", symbol)
                self.stats["sec_poll_errors"] += 1
            await asyncio.sleep(self.settings.sec_poll_interval_seconds)

    async def stats_reporter(self, interval: float = 300.0) -> None:
        while True:
            await asyncio.sleep(interval)
            LOGGER.info("ingestion stats: %s | ledger %s", dict(self.stats), self.ledger.counts())
