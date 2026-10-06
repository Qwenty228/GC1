"""Detachable web layer: a thin FastAPI wrapper over the backend core.

The EDGAR and Alpaca endpoints only call the pure functions in `sec` and
`alpaca`. The event and article endpoints only READ persisted records, summaries
and sources from PostgreSQL (`summary.read`, `db.read`): they never write, never
call a model, and never summarize on a page view. `/api/status` reads the
ingestion worker's status file; it never starts or contacts a collector.
Review decisions are recorded with the `datasources-summary review` CLI, not over
HTTP. The frontend can be replaced or removed without touching the backend modules.

Run:  uv run datasources-server   ->  http://127.0.0.1:8000
      (or `uv run datasources-dev` for the server plus the ingestion worker)
"""

from __future__ import annotations

import hashlib
import logging
import re
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import alpaca, config, sec

LOGGER = logging.getLogger(__name__)
_database = None
_database_lock = threading.Lock()


def get_database():
    """The shared read pool, opened on the first event request (not at import or startup)."""
    global _database
    with _database_lock:
        if _database is None:
            from .db import Database, db_enabled, load_db_settings

            if not db_enabled():
                raise LookupError("DATASOURCES_DB_ENABLED=false: event summaries need PostgreSQL")
            try:
                _database = Database(load_db_settings())  # lazy: connects on first use
            except Exception as exc:  # configuration messages name settings, never their values
                raise LookupError(f"database not configured: {exc}") from exc
        return _database


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    global _database
    if _database is not None:
        _database.close()
        _database = None


app = FastAPI(title="Node 1 - Data Sources", version="0.1.0", lifespan=lifespan)
SYMBOL_RE = re.compile(r"^[A-Z0-9.\-]{1,12}$")


@app.middleware("http")
async def _cache_headers(request: Request, call_next):
    """The page polls the API, so API responses are never cached; static files are revalidated."""
    response = await call_next(request)
    response.headers.setdefault("Cache-Control", "no-store" if request.url.path.startswith("/api/") else "no-cache")
    return response


def _symbol(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    symbol = value.strip().upper()
    if not SYMBOL_RE.match(symbol):
        raise ValueError("symbol must be a ticker such as NVDA")
    return symbol


@app.get("/api/companies")
def api_companies() -> dict:
    """Ticker -> {cik, name} mapping used to populate the dropdown."""
    return config.load_ticker_map()


@app.get("/api/edgar")
def api_edgar(ticker: str, limit: int = 10, refresh: bool = False) -> JSONResponse:
    """Most-recent 8-K filings for `ticker` (records + data dictionary).

    Served from the per-CIK cache unless `refresh=true` forces a fresh request.
    """
    try:
        return JSONResponse(sec.recent_8k(ticker, limit=limit, refresh=refresh))
    except KeyError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)
    except Exception as exc:  # network / parsing failure: report, don't crash
        return JSONResponse({"error": f"{type(exc).__name__}: {exc}"}, status_code=502)


@app.get("/api/alpaca")
def api_alpaca(ticker: str, limit: int = 10, demo: bool | None = None) -> JSONResponse:
    """Most-recent Alpaca news for `ticker` (records + data dictionary).

    Sample data is returned only in explicit demo mode (`demo=true` or
    DATASOURCES_DEMO_MODE=true); missing credentials are a configuration error.
    """
    try:
        return JSONResponse(alpaca.recent_news(ticker, limit=limit, demo=demo))
    except alpaca.AlpacaConfigError as exc:
        return JSONResponse({"error": f"Configuration error: {exc}"}, status_code=503)
    except Exception as exc:
        return JSONResponse({"error": f"{type(exc).__name__}: {exc}"}, status_code=502)


# --------------------------------------------------------------------------- event summaries (read-only)
def _summary_language() -> str:
    from .summary.settings import load_summary_settings

    return load_summary_settings().language


def _database_unavailable(exc: Exception) -> JSONResponse:
    # DatabaseError messages already hide the password; never echo connection strings.
    LOGGER.warning("event endpoint database error: %s", exc)
    return JSONResponse({"error": f"database unavailable: {type(exc).__name__}"}, status_code=503)


@app.get("/api/events")
def api_events(limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0),
               state: str | None = Query(None, description="Filter by summary_state"),
               symbol: str | None = Query(None, description="Only events with a source linked to this ticker"),
               include_sample: bool = False, database=Depends(get_database)) -> JSONResponse:
    """Grouped events, newest first, each with its current summary (or pending / review state), a
    compact `conflict` status and count from the stored conflict reviews, the company `symbols` of its
    sources and its `latest_source` (title, publisher, time, http(s) link) for cards without a summary.

    Sample (demo/test) events are excluded unless `include_sample=true`.
    """
    from fastapi.encoders import jsonable_encoder

    from .summary.read import SUMMARY_STATES, list_events

    if state is not None and state not in SUMMARY_STATES:
        return JSONResponse({"error": f"state must be one of {', '.join(SUMMARY_STATES)}"}, status_code=422)
    try:
        ticker = _symbol(symbol)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=422)
    try:
        with database.connection() as conn:
            events = list_events(conn, include_sample=include_sample, language=_summary_language(), limit=limit,
                                 offset=offset, state=state, symbol=ticker)
    except Exception as exc:
        return _database_unavailable(exc)
    return JSONResponse(jsonable_encoder({"events": events, "limit": limit, "offset": offset}))


@app.get("/api/articles")
def api_articles(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0),
                 source: Literal["all", "news", "sec"] = "all",
                 symbol: str | None = Query(None, description="Only records linked to this ticker"),
                 include_sample: bool = False, database=Depends(get_database)) -> JSONResponse:
    """Stored news articles and SEC filings (current revision of each), newest first by source time.

    Each item has the original headline, publisher (news) or issuer (SEC), the source's own
    timestamps, the excerpt supplied with the article (never a generated summary), the original
    link (http/https only), revision count, grouping status and the events it belongs to.
    Sample records are excluded unless `include_sample=true`.
    """
    from fastapi.encoders import jsonable_encoder

    from .db.read import list_articles

    try:
        ticker = _symbol(symbol)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=422)
    try:
        with database.connection() as conn:
            page = list_articles(conn, include_sample=include_sample, limit=limit, offset=offset, source=source,
                                 symbol=ticker)
    except Exception as exc:
        return _database_unavailable(exc)
    return JSONResponse(jsonable_encoder(page))


def optional_database() -> tuple[object | None, str | None]:
    """(pool, None), or (None, reason) when there is no database: /api/status reports it instead of failing."""
    try:
        return get_database(), None
    except LookupError as exc:
        return None, str(exc)


def _configured_flag(loader) -> bool | None:
    try:
        return bool(loader().enabled)
    except Exception:  # invalid settings are reported by the worker and the launcher, not here
        return None


@app.get("/api/status")
def api_status(include_sample: bool = False, pool=Depends(optional_database)) -> JSONResponse:
    """What the page header shows: collector state from the ingestion worker's own status file
    (never inferred from this request succeeding), the configured feature flags, and stored counts."""
    from fastapi.encoders import jsonable_encoder

    from .ingest.settings import load_settings
    from .ingest.status import collector_status, load_status_file
    from .similarity.settings import load_similarity_settings
    from .summary.settings import load_summary_settings

    body: dict = {"server_time": datetime.now(timezone.utc), "symbols": [],
                  "collector": {"state": "unknown", "updated_at": None},
                  "features": {"similarity_enabled": _configured_flag(load_similarity_settings),
                               "summary_enabled": _configured_flag(load_summary_settings)},
                  "database": {"available": False, "error": None}, "counts": None}
    try:
        settings = load_settings()
        body["symbols"] = list(settings.symbols)
        body["collector"] = collector_status(load_status_file(settings.root))
    except Exception as exc:
        LOGGER.warning("status endpoint could not read the ingestion settings: %s", type(exc).__name__)
    database, reason = pool
    if database is None:
        body["database"]["error"] = reason
        return JSONResponse(jsonable_encoder(body))
    try:
        from .db.read import stored_counts

        with database.connection() as conn:
            body["counts"] = stored_counts(conn, include_sample=include_sample)
        body["database"] = {"available": True, "error": None}
    except Exception as exc:
        LOGGER.warning("status endpoint database error: %s", exc)
        body["database"]["error"] = f"database unavailable: {type(exc).__name__}"
    return JSONResponse(jsonable_encoder(body))


@app.get("/api/events/{event_id}")
def api_event(event_id: uuid.UUID, include_sample: bool = False, database=Depends(get_database)) -> JSONResponse:
    """One event: current summary with statement-level citations and source links, its version and update
    time, under-review / pending state, version history, every original source, and `conflict`: each
    dispute's claims, verbatim quotes and sources from the stored review and comparison records."""
    from fastapi.encoders import jsonable_encoder

    from .summary.read import event_detail

    try:
        with database.connection() as conn:
            detail = event_detail(conn, str(event_id), include_sample=include_sample,
                                  language=_summary_language())
    except Exception as exc:
        return _database_unavailable(exc)
    if detail is None:
        return JSONResponse({"error": "event not found"}, status_code=404)
    return JSONResponse(jsonable_encoder(detail))


@app.exception_handler(LookupError)
def _lookup_error(_request, exc: LookupError) -> JSONResponse:
    return JSONResponse({"error": str(exc)}, status_code=503)


_ASSET_REF = re.compile(r'(?P<attr>src|href)="(?P<name>[\w.-]+\.(?:js|css))"')


def _page_html(name: str) -> str:
    """A page with every local script and stylesheet URL versioned by its content (app.js?v=<hash>).

    Earlier versions of this server sent no Cache-Control header, so browsers may still hold an
    old app.js they consider fresh and reuse without asking. Mixed with a newer page it throws
    during startup and leaves the page unresponsive. A new URL per content version rules that out.
    """
    html = (config.WEB_DIR / name).read_text(encoding="utf-8")

    def versioned(match: re.Match) -> str:
        try:
            digest = hashlib.sha256((config.WEB_DIR / match["name"]).read_bytes()).hexdigest()[:12]
        except OSError:
            return match[0]
        return f'{match["attr"]}="{match["name"]}?v={digest}"'

    return _ASSET_REF.sub(versioned, html)


@app.get("/", include_in_schema=False)
@app.get("/index.html", include_in_schema=False)
def index_page() -> HTMLResponse:
    return HTMLResponse(_page_html("index.html"), headers={"Cache-Control": "no-cache"})


@app.get("/tools.html", include_in_schema=False)
def tools_page() -> HTMLResponse:
    """Stored articles, collector status and the source explorer, kept off the news summaries page."""
    return HTMLResponse(_page_html("tools.html"), headers={"Cache-Control": "no-cache"})


# Serve the static frontend at the root. Mounted last so /api/* wins.
app.mount("/", StaticFiles(directory=str(config.WEB_DIR), html=True), name="web")


def main(host: str = "127.0.0.1", port: int = 8000) -> None:
    import uvicorn

    # reload stays off: a reloader would start extra processes, and collectors never run in this server anyway.
    uvicorn.run("datasources.server:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
