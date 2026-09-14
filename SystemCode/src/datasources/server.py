"""Detachable web layer: a thin FastAPI wrapper over the backend core.

This module ONLY calls the pure functions in `sec` and `alpaca`. It holds no
data logic of its own, so the frontend can be replaced or removed without
touching the backend module.

Run:  uv run datasources-server   ->  http://127.0.0.1:8000
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from . import sec
from . import alpaca
from . import config

app = FastAPI(title="Node 1 - Data Sources", version="0.1.0")


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
def api_alpaca(ticker: str, limit: int = 10) -> JSONResponse:
    """Most-recent Alpaca news for `ticker` (records + data dictionary)."""
    try:
        return JSONResponse(alpaca.recent_news(ticker, limit=limit))
    except Exception as exc:
        return JSONResponse({"error": f"{type(exc).__name__}: {exc}"}, status_code=502)


# Serve the static frontend at the root. Mounted last so /api/* wins.
app.mount("/", StaticFiles(directory=str(config.WEB_DIR), html=True), name="web")


def main() -> None:
    import uvicorn

    uvicorn.run("datasources.server:app", host="127.0.0.1", port=8000, reload=False)


if __name__ == "__main__":
    main()
