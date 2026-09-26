# Node 1 — Data Sources. Everything runs through `uv`; no manual venv activation.
#
#   make sync              install dependencies into .venv
#   make run               serve the UI + API on http://127.0.0.1:8000
#   make test              run the offline test suite
#   make edgar T=NVDA      backend-only CLI (no web layer)
#   make ingest            start automatic ingestion (EDGAR + Alpaca WebSocket/REST)
#   make ingest-demo       offline ingestion over bundled fixtures -> output/demo
#   make db-check          report the PostgreSQL connection and pgvector
#   make db-smoke          bounded live end-to-end test into PostgreSQL

UV := uv
T  ?= NVDA
LIMIT ?= 10

.DEFAULT_GOAL := help
.PHONY: help sync run test companies edgar alpaca ingest ingest-once ingest-demo \n	db-check db-migrate db-inspect db-backfill db-smoke clean clean-cache

help: ## Show this help
	@echo "Node 1 — Data Sources"
	@echo
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-13s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  Variables: T=<ticker> (default $(T)), LIMIT=<n> (default $(LIMIT))"

sync: ## Create/refresh .venv from pyproject.toml + uv.lock
	$(UV) sync

run: sync ## Serve the web UI and API at http://127.0.0.1:8000
	@echo "Open http://127.0.0.1:8000 — Ctrl-C to stop"
	$(UV) run datasources-server

test: sync ## Run the offline test suite (no network needed)
	$(UV) run pytest -q

companies: sync ## Print the ticker -> CIK mapping
	$(UV) run datasources companies

edgar: sync ## Fetch recent 8-K filings, e.g. make edgar T=NVDA LIMIT=5
	$(UV) run datasources edgar $(T) --limit $(LIMIT)

alpaca: sync ## Fetch recent news, e.g. make alpaca T=AAPL LIMIT=5
	$(UV) run datasources alpaca $(T) --limit $(LIMIT)

ingest: sync ## Start automatic ingestion (one worker per output dir)
	$(UV) run datasources-ingest run

ingest-once: sync ## One SEC poll + one Alpaca REST backfill, then exit
	$(UV) run datasources-ingest run --once

ingest-demo: sync ## Offline ingestion of bundled fixtures into output/demo
	$(UV) run datasources-ingest demo

db-check: sync ## Report the database and whether pgvector is enabled
	$(UV) run datasources-db check

db-migrate: sync ## Apply the additive SQL migrations (idempotent)
	$(UV) run datasources-db migrate

db-inspect: sync ## Row counts per table
	$(UV) run datasources-db inspect

db-backfill: sync ## Store existing output/processed records (idempotent)
	$(UV) run datasources-db backfill

db-smoke: sync ## Bounded live end-to-end test into PostgreSQL
	$(UV) run datasources-db smoke --symbol $(T) --max-articles 5 --lookback-hours 72

clean: ## Remove Python and test caches
	find . -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true
	find . -name '.DS_Store' -delete 2>/dev/null || true
	rm -rf .pytest_cache

clean-cache: ## Drop the cached EDGAR submissions (they are re-fetched on demand)
	rm -rf data/cache
