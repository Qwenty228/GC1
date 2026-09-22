"""`datasources-ingest`: automatic ingestion worker and offline tools.

    uv run datasources-ingest run            # EDGAR polling + Alpaca WebSocket + REST backfill
    uv run datasources-ingest run --once     # one SEC poll per company + one REST backfill, then exit
    uv run datasources-ingest demo           # offline, bundled fixtures -> output/demo/
    uv run datasources-ingest process-file PATH --provider alpaca|sec_edgar [--sec-html-dir DIR] [--demo]
    uv run datasources-ingest status [--demo]

Run exactly one worker per output directory (a file lock enforces it). Do not
also run `datasources-news-stream`: the worker already owns the WebSocket.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from .. import config
from ..alpaca import AlpacaAuth, AlpacaConfigError, auth_from_env
from .adapters import unwrap_payload
from .sec_client import FetchResult, SecClient, SecFetchError
from .service import IngestionService, ProcessLock
from .settings import ConfigurationError, IngestSettings, load_settings, parse_symbol_list, require_sec_user_agent

LOGGER = logging.getLogger("datasources.ingest")
FIXTURE_SUBMISSIONS = config.DATA_DIR / "submissions_CIK0001045810.json"
FIXTURE_8K_DIR = config.DATA_DIR / "8k"


class RedactSecrets(logging.Filter):
    """Mask credential values if they ever appear in a log message."""

    def __init__(self, secrets: list[str]):
        super().__init__()
        self.secrets = [s for s in secrets if s and len(s) >= 6]

    def filter(self, record: logging.LogRecord) -> bool:
        if self.secrets:
            message = record.getMessage()
            for secret in self.secrets:
                message = message.replace(secret, "***")
            record.msg, record.args = message, ()
        return True


def configure_logging(root: Path, verbose: bool = False) -> None:
    (root / "logs").mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = [
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(root / "logs" / "ingest.log", maxBytes=5_000_000,
                                             backupCount=5, encoding="utf-8"),
    ]
    redactor = RedactSecrets([os.getenv(name, "") for name in (
        "ALPACA_API_KEY", "ALPACA_SECRET_KEY", "ALPACA_BROKER_CLIENT_ID", "ALPACA_BROKER_CLIENT_SECRET")])
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    for handler in handlers:
        handler.addFilter(redactor)
        handler.setFormatter(formatter)
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, handlers=handlers, force=True)
    for noisy in ("websockets", "urllib3", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ---------------------------------------------------------------------- live run
def build_live_service(settings: IngestSettings, *, enable_sec: bool, enable_alpaca: bool) -> IngestionService:
    if settings.demo:
        raise ConfigurationError("`run` is live-only; use `datasources-ingest demo` for sample data")
    sec_client = None
    if enable_sec:
        sec_client = SecClient(require_sec_user_agent(settings),
                               requests_per_second=settings.sec_requests_per_second,
                               timeout=settings.http_timeout_seconds, max_retries=settings.sec_max_retries)
        for symbol in settings.symbols:
            config.cik_for(symbol)  # fail fast on an unknown ticker (KeyError)
    auth: AlpacaAuth | None = None
    if enable_alpaca:
        auth = auth_from_env()  # AlpacaConfigError when credentials are missing
    return IngestionService(settings, sec_client=sec_client, alpaca_auth=auth)


def _news_settings(settings: IngestSettings, auth: AlpacaAuth):
    from alpaca_news.client import NewsSettings

    if auth.mode == "broker":
        return NewsSettings(api_key="", secret_key="", symbols=settings.symbols, headers_provider=auth.headers)
    return NewsSettings(api_key=auth.api_key or "", secret_key=auth.secret_key or "", symbols=settings.symbols)


async def run_service(service: IngestionService, *, enable_sec: bool, enable_alpaca: bool, once: bool) -> None:
    from alpaca_news.client import run_forever

    settings = service.settings
    service.startup()
    service._ensure_async_state()
    writer = asyncio.create_task(service.writer(), name="writer")
    producers: list[asyncio.Task] = []
    try:
        if once:
            if enable_sec:
                for symbol in settings.symbols:
                    try:
                        for item in await asyncio.to_thread(service.sec_poll_once, symbol):
                            await service.enqueue(item)
                    except SecFetchError as error:
                        LOGGER.error("SEC poll for %s failed: %s", symbol, error)
            if enable_alpaca:
                await service.alpaca_backfill("once")
            assert service.queue is not None
            await service.queue.join()
            return

        if enable_sec:
            # Stagger companies across the interval; each company is polled ~every interval.
            spacing = settings.sec_poll_interval_seconds / max(len(settings.symbols), 1)
            for index, symbol in enumerate(settings.symbols):
                producers.append(asyncio.create_task(service.sec_poller(symbol, index * spacing), name=f"sec:{symbol}"))
        if enable_alpaca:
            assert service.alpaca_auth is not None
            producers.append(asyncio.create_task(service.alpaca_backfill("startup"), name="alpaca:startup-backfill"))
            producers.append(asyncio.create_task(run_forever(
                _news_settings(settings, service.alpaca_auth),
                on_articles=service.on_ws_articles,
                on_subscribed=service.on_ws_subscribed,
                on_frame=service.on_ws_frame,
            ), name="alpaca:websocket"))
            producers.append(asyncio.create_task(service.stream_heartbeat(), name="alpaca:heartbeat"))
        producers.append(asyncio.create_task(service.stats_reporter(), name="stats"))
        await asyncio.gather(*producers)
    finally:
        LOGGER.info("shutting down: stopping collectors, finishing the current record")
        for task in producers:
            task.cancel()
        await asyncio.gather(*producers, return_exceptions=True)
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)
        pending = service.queue.qsize() if service.queue else 0
        LOGGER.info("stopped; %d queued item(s) left unprocessed and retryable; stats %s", pending, dict(service.stats))


# ---------------------------------------------------------------------- demo / file
def file_ref(path: Path) -> str:
    """``file:`` reference, relative to the project root when possible (portable, no user paths)."""
    resolved = path.resolve()
    try:
        return f"file:{resolved.relative_to(config.ROOT).as_posix()}"
    except ValueError:
        return f"file:{resolved.as_posix()}"


def fixture_document_fetcher(html_dir: Path):
    """Serve SEC primary documents from `<accession>-<primaryDocument>` files."""

    def fetch(url: str, row: dict, ctx) -> FetchResult:
        name = f"{row['accessionNumber']}-{row['primaryDocument'].replace('/', '__')}"
        path = html_dir / name
        if not path.exists():
            raise SecFetchError(f"no local document {name} in {html_dir}", kind="missing_fixture", retryable=True)
        return FetchResult(url=url, status=200, content_type="text/html; charset=utf-8",
                           body=path.read_bytes(), retrieved_at=datetime.now(timezone.utc))

    return fetch


def _process_sec_payload(service: IngestionService, payload, *, source: Path, symbol: str,
                         only_available: Path | None) -> None:
    items = service.sec_collect(symbol, payload, raw_ref=file_ref(source), endpoint=file_ref(source),
                                retrieved_at=datetime.fromtimestamp(source.stat().st_mtime, timezone.utc),
                                transport="file")
    for item in items:
        row = item.payload["row"]
        if only_available is not None and not (only_available / f"{row['accessionNumber']}-{row['primaryDocument']}").exists():
            service.stats["sec_demo_no_fixture_html"] += 1
            continue
        service.process_item(item)


def _process_alpaca_payload(service: IngestionService, payload, *, source: Path, restrict_symbols: bool) -> None:
    articles, info = unwrap_payload(payload, provider="alpaca")
    if info.get("ignored"):
        LOGGER.info("%s is a data dictionary; ignored as document content", source)
        return
    records = [article for article, _ in articles]
    if restrict_symbols and "*" not in service.settings.symbols:
        wanted = set(service.settings.symbols)
        records = [a for a in records if wanted & {str(s).upper() for s in (a.get("symbols") or [])}]
    retrieved_at = datetime.fromtimestamp(source.stat().st_mtime, timezone.utc)
    for item in service.alpaca_items(records, transport="file", endpoint=file_ref(source),
                                     retrieved_at=retrieved_at, raw_refs=[file_ref(source)]):
        service.process_item(item)


def _load_payloads(path: Path) -> list:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return [json.loads(text)]


def run_demo(settings: IngestSettings) -> IngestionService:
    """Offline end-to-end run over bundled fixtures into output/demo (is_sample=true)."""
    settings = settings.with_overrides(demo=True)
    service = IngestionService(settings, document_fetcher=fixture_document_fetcher(FIXTURE_8K_DIR))
    service.startup()
    submissions = json.loads(FIXTURE_SUBMISSIONS.read_text(encoding="utf-8"))
    _process_sec_payload(service, submissions, source=FIXTURE_SUBMISSIONS, symbol="NVDA", only_available=FIXTURE_8K_DIR)
    _process_alpaca_payload(service, json.loads(config.SAMPLE_ALPACA_FILE.read_text(encoding="utf-8")),
                            source=config.SAMPLE_ALPACA_FILE, restrict_symbols=True)
    return service


def run_process_file(settings: IngestSettings, path: Path, provider: str, html_dir: Path | None,
                     symbol: str | None) -> IngestionService:
    fetcher = fixture_document_fetcher(html_dir) if html_dir else None
    if provider == "sec_edgar" and fetcher is None and settings.demo:
        raise ConfigurationError("--sec-html-dir is required for SEC files in demo mode")
    sec_client = None  # without --sec-html-dir, primary documents are downloaded live
    if provider == "sec_edgar" and fetcher is None:
        sec_client = SecClient(require_sec_user_agent(settings), requests_per_second=settings.sec_requests_per_second,
                               timeout=settings.http_timeout_seconds, max_retries=settings.sec_max_retries)
    service = IngestionService(settings, sec_client=sec_client, document_fetcher=fetcher, update_shared_cache=False)
    service.startup()
    for payload in _load_payloads(path):
        if provider == "sec_edgar":
            _, info = unwrap_payload(payload, provider="sec_edgar")  # shape check
            if info.get("ignored"):
                LOGGER.info("%s is a data dictionary; ignored as document content", path)
                continue
            _process_sec_payload(service, payload, source=path, symbol=symbol or settings.symbols[0], only_available=None)
        else:
            _process_alpaca_payload(service, payload, source=path, restrict_symbols=False)
    return service


# ---------------------------------------------------------------------- entry point
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="datasources-ingest", description=__doc__.split("\n\n")[0])
    parser.add_argument("--symbols", help="Comma-separated symbols (default INGEST_SYMBOLS or NVDA)")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command")

    p_run = sub.add_parser("run", help="Start automatic ingestion (default)")
    p_run.add_argument("--once", action="store_true", help="One SEC poll + one REST backfill, then exit (no WebSocket)")
    p_run.add_argument("--no-sec", action="store_true")
    p_run.add_argument("--no-alpaca", action="store_true")

    sub.add_parser("demo", help="Offline run over bundled fixtures into output/demo")

    p_file = sub.add_parser("process-file", help="Ingest a saved JSON/JSONL payload (transport=file)")
    p_file.add_argument("path", type=Path)
    p_file.add_argument("--provider", required=True, choices=["sec_edgar", "alpaca"])
    p_file.add_argument("--sec-html-dir", type=Path, help="Directory of <accession>-<primaryDocument> files")
    p_file.add_argument("--symbol", help="Ticker for SEC submissions files")
    p_file.add_argument("--demo", action="store_true", help="Treat as sample data (output/demo)")

    p_status = sub.add_parser("status", help="Print ledger counts")
    p_status.add_argument("--demo", action="store_true")

    args = parser.parse_args(argv)
    command = args.command or "run"
    try:
        demo = command == "demo" or bool(getattr(args, "demo", False))
        settings = load_settings(demo=demo)
        if args.symbols:
            settings = settings.with_overrides(symbols=parse_symbol_list(args.symbols))
        settings.root.mkdir(parents=True, exist_ok=True)
        configure_logging(settings.root, args.verbose)

        if command == "status":
            service = IngestionService(settings)
            service.ledger.load()
            print(json.dumps({"root": str(settings.root), **service.ledger.counts(),
                              "cursors": service.ledger.data["cursors"]}, indent=2))
            return 0

        with ProcessLock(settings.root):
            if command == "run":
                enable_sec = not args.no_sec
                enable_alpaca = not args.no_alpaca
                service = build_live_service(settings, enable_sec=enable_sec, enable_alpaca=enable_alpaca)
                LOGGER.info("starting ingestion: symbols=%s root=%s sec=%s alpaca=%s", settings.symbols,
                            settings.root, enable_sec, enable_alpaca)
                try:
                    asyncio.run(run_service(service, enable_sec=enable_sec, enable_alpaca=enable_alpaca, once=args.once))
                except KeyboardInterrupt:
                    LOGGER.info("interrupted")
            elif command == "demo":
                service = run_demo(settings)
            else:
                service = run_process_file(settings, args.path, args.provider, args.sec_html_dir, args.symbol)
            print(json.dumps({"root": str(settings.root), "stats": dict(service.stats),
                              "ledger": service.ledger.counts()}, indent=2))
        return 0
    except (ConfigurationError, AlpacaConfigError, KeyError) as error:
        message = error.args[0] if isinstance(error, KeyError) and error.args else str(error)
        sys.stderr.write(f"Configuration error: {message}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
