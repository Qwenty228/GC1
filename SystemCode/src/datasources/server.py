"""Detachable web layer: a thin FastAPI wrapper over the backend core.

The EDGAR and Alpaca endpoints only call the pure functions in `sec` and
`alpaca`. The event endpoints only READ persisted summaries and sources from
PostgreSQL (`summary.read`): they never write, never call a model, and never
summarize on a page view. Review decisions are recorded with the
`datasources-summary review` CLI, not over HTTP. The frontend can be replaced
or removed without touching the backend modules.

Run:  uv run datasources-server   ->  http://127.0.0.1:8000
"""

from __future__ import annotations

import logging
import threading
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Query
from fastapi.responses import JSONResponse
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
               include_sample: bool = False, database=Depends(get_database)) -> JSONResponse:
    """Grouped events, newest first, each with its current summary (or pending / review state) and a
    compact `conflict` status and count from the stored conflict reviews.

    Sample (demo/test) events are excluded unless `include_sample=true`.
    """
    from fastapi.encoders import jsonable_encoder

    from .summary.read import SUMMARY_STATES, list_events

    if state is not None and state not in SUMMARY_STATES:
        return JSONResponse({"error": f"state must be one of {', '.join(SUMMARY_STATES)}"}, status_code=422)
    try:
        with database.connection() as conn:
            events = list_events(conn, include_sample=include_sample, language=_summary_language(), limit=limit,
                                 offset=offset, state=state)
    except Exception as exc:
        return _database_unavailable(exc)
    return JSONResponse(jsonable_encoder({"events": events, "limit": limit, "offset": offset}))


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


# Serve the static frontend at the root. Mounted last so /api/* wins.
app.mount("/", StaticFiles(directory=str(config.WEB_DIR), html=True), name="web")


def main() -> None:
    import uvicorn

    uvicorn.run("datasources.server:app", host="127.0.0.1", port=8000, reload=False)


if __name__ == "__main__":
    main()
