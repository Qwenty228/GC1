# Data Sources

This repository includes a real-time Alpaca news WebSocket client that uses
Alpaca Trading API credentials and subscribes to NVIDIA news by default.

## Setup

Install [uv](https://docs.astral.sh/uv/) and synchronize the pinned Python 3.13.7
environment and dependencies:

```powershell
uv sync
```

Create your local credentials file from the template:

```powershell
Copy-Item .env.example .env
```

Set these variables in `.env`:

```text
ALPACA_API_KEY=
ALPACA_SECRET_KEY=
ALPACA_NEWS_SYMBOLS=NVDA
```

`ALPACA_API_KEY` and `ALPACA_SECRET_KEY` are required Trading API credentials.
`ALPACA_NEWS_SYMBOLS` is optional and defaults to `NVDA`; use `NVDA` to receive
only NVIDIA news. Set `ALPACA_NEWS_SYMBOLS=*` temporarily to confirm that the
stream is working with all available news.

Never commit `.env` or expose API secrets in logs, source code, screenshots, or
support requests.

## Run

```powershell
uv run python -m alpaca_news.main
```

The stream only delivers newly published articles. It can remain quiet when no
new NVIDIA news is released; this does not necessarily indicate a connection
problem.

Run offline tests without credentials or a network connection:

```powershell
uv run pytest
```

## Postman WebSocket Check

1. Create a new WebSocket request.
2. Connect to `wss://stream.data.alpaca.markets/v1beta1/news`.
3. Add the Trading API headers `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY`.
4. Connect.
5. Send `{"action":"subscribe","news":["NVDA"]}`.