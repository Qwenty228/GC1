# Node 1 — Data Sources. Everything runs through `uv`; no manual venv activation.
#
#   make sync              install dependencies into .venv
#   make dev               web app + API + collectors in one command (datasources-dev)
#   make dev-check         the launcher's startup checks only; starts nothing
#   make run               serve the UI + API on http://127.0.0.1:8000 (no collectors)
#   make test              run the offline test suite
#   make edgar T=NVDA      backend-only CLI (no web layer)
#   make ingest            start automatic ingestion (EDGAR + Alpaca WebSocket/REST)
#   make ingest-demo       offline ingestion over bundled fixtures -> output/demo
#   make db                one-shot, idempotent: start Docker Postgres if needed, apply pending migrate/*.sql,
#                          run new/changed populate/*.sql, show both logs
#   make db FORCE=1        DESTRUCTIVE: drop and recreate the database, then migrate + populate everything
#   make db-up             start the shared PostgreSQL + pgvector container (compose.yaml)
#   make db-down           stop it (data volume kept)
#   make db-check          report the PostgreSQL connection and pgvector
#   make db-populate       run new/changed populate/*.sql data scripts (logged in populate_log)
#   make eval EXP=<file>   run an experiment file on the labelled batches (see evaluation/README.md)
#   make eval-compare      leaderboard of saved evaluation runs
#   make db-plan           what `migrate` would do (read-only)
#   make db-verify         compare the schema with the migrations (read-only)
#   make db-companies-plan which coverage companies registration would add (read-only)
#   make db-companies      register the coverage companies (insert-only, repeatable)
#   make db-smoke          bounded live end-to-end test into PostgreSQL
#   make graph             print the compiled ingestion graph
#   make similarity-status semantic analysis / event grouping status
#   make summary-status    event summary jobs, versions and reviews
#   make summary-pending   summary work that backfill/retry would do (no model calls)
#   make summary-reviews   open conflict reviews

UV := uv
T  ?= NVDA
LIMIT ?= 10

.DEFAULT_GOAL := help
# The database the app uses: PGDATABASE from the shell, else from .env (same precedence as the app).
DBNAME := $(or $(PGDATABASE),$(shell sed -n 's/^PGDATABASE=//p' .env 2>/dev/null | tail -1),stock_news)
.PHONY: help sync sync-local-embeddings dev dev-check run test companies edgar alpaca ingest ingest-once ingest-demo graph \
	db db-log db-reset db-rerun db-populate eval eval-compare db-up db-down db-psql db-logs db-check db-plan db-migrate db-verify db-companies-plan db-companies db-inspect db-backfill db-smoke similarity-check similarity-download-model \
	similarity-backfill similarity-retry similarity-status summary-check summary-status summary-pending \
	summary-reviews clean clean-cache

help: ## Show this help
	@echo "Node 1 — Data Sources"
	@echo
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-13s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  Variables: T=<ticker> (default $(T)), LIMIT=<n> (default $(LIMIT))"

sync: ## Create/refresh .venv from pyproject.toml + uv.lock
	$(UV) sync

sync-local-embeddings: ## Also install the local CPU embedding backend (torch + sentence-transformers)
	$(UV) sync --locked --extra local-embeddings

dev: ## Start the web app, API and collectors (Ctrl-C stops everything)
	$(UV) run datasources-dev

dev-check: ## Run the launcher's startup checks without starting anything
	$(UV) run datasources-dev --check

run: sync ## Serve the web UI and API at http://127.0.0.1:8000 (no collectors)
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

graph: ## Print the compiled LangGraph ingestion graph (Mermaid + edges)
	$(UV) run datasources-ingest graph

# make db is one-way: it asks the logs "has this file run?" (schema_migration, populate_log), never "does the
# table still exist?". A table dropped by hand stays missing, because the log still says done. To recover:
# `make db-rerun FILE=<the migrate/ file that created it>` (re-runs it, then repopulates), or rebuild
# everything with `make db FORCE=1` (deletes all data in the database).
db: ## One-shot, idempotent: start Postgres if needed, apply pending migrations, run new/changed populate scripts, show logs (FORCE=1 rebuilds)
	@docker info >/dev/null 2>&1 || { echo "Docker is not running. Start Docker Desktop and retry."; exit 1; }
	docker compose up -d --wait postgres
ifeq ($(FORCE),1)
	@$(MAKE) -s db-reset
endif
	$(UV) run datasources-db migrate
	$(UV) run datasources-db populate
	@$(MAKE) -s db-log

db-reset: ## DESTRUCTIVE: drop and recreate the database (asks for its name; CONFIRM=<db> skips the prompt)
	@echo "make db FORCE=1 will DROP database '$(DBNAME)' with ALL its data (news, events, summaries) and rebuild it."; \
	if [ "$(CONFIRM)" != "$(DBNAME)" ]; then printf "Type the database name to continue: "; read answer; \
	  [ "$$answer" = "$(DBNAME)" ] || { echo "Not confirmed; nothing was changed."; exit 1; }; fi
	docker compose exec -T postgres sh -c 'dropdb -U "$$POSTGRES_USER" --if-exists --force "$(DBNAME)" && createdb -U "$$POSTGRES_USER" "$(DBNAME)"'
	@echo "Recreated empty database '$(DBNAME)'."

db-log: ## Migration log (schema_migration) and data log (populate_log)
	@docker compose exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$(DBNAME)" -c "SELECT filename, method, applied_at::timestamp(0) AS applied_at, left(checksum, 12) AS checksum FROM schema_migration ORDER BY filename" -c "SELECT count(*) AS migrations_applied FROM schema_migration"'
	@docker compose exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$(DBNAME)" -c "SELECT filename, count(*) AS runs, max(applied_at)::timestamp(0) AS last_run, left((array_agg(checksum ORDER BY log_id DESC))[1], 12) AS checksum FROM populate_log GROUP BY filename ORDER BY filename"' 2>/dev/null || echo "(no populate_log yet)"

db-rerun: ## Recreate objects dropped by hand: re-run one applied migrate/ file in one transaction, then repopulate (FILE=006_evaluation_fixture.sql)
	@test -n "$(FILE)" && test -f migrate/$(FILE) || { echo "Usage: make db-rerun FILE=<a file in migrate/>, e.g. FILE=006_evaluation_fixture.sql"; exit 2; }
	docker compose exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$(DBNAME)" -v ON_ERROR_STOP=1 -1 -q' < migrate/$(FILE)
	@echo "Re-ran migrate/$(FILE) on $(DBNAME) (files written with IF NOT EXISTS only recreate what is missing)."
	$(UV) run datasources-db populate --force

db-populate: ## Run new/changed populate/*.sql data scripts, each logged in populate_log (`datasources-db populate --force` reruns all)
	$(UV) run datasources-db populate

eval: ## Run an experiment file (its own BATCHES/SAVE/KEEP_DB config): make eval EXP=evaluation/consolidation/experiments/groq.py
	@test -n "$(EXP)" || { echo "Usage: make eval EXP=evaluation/consolidation/experiments/<file>.py"; exit 2; }
	$(UV) run python $(EXP)

eval-compare: ## Leaderboard of saved evaluation runs (evaluation/consolidation/results/)
	$(UV) run datasources-eval compare

db-up: ## Start PostgreSQL 18 + pgvector in Docker (reads PG* from .env), wait until healthy
	docker compose up -d --wait postgres

db-down: ## Stop the Docker database (data volume kept; `docker compose down -v` wipes it)
	docker compose down

db-psql: ## psql shell inside the Docker database
	docker compose exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

db-logs: ## Follow the Docker database logs
	docker compose logs -f postgres

db-check: sync ## Report the database and whether pgvector is enabled
	$(UV) run datasources-db check

db-plan: ## What `migrate` would do, read in a READ ONLY transaction
	$(UV) run datasources-db migrate --dry-run

db-migrate: sync ## New database: the whole schema; existing one: pending files (see README before adopting)
	$(UV) run datasources-db migrate

db-verify: ## Compare the schema with the migration files (read-only)
	$(UV) run datasources-db verify

db-companies-plan: ## Which coverage companies registration would add (read-only)
	$(UV) run datasources-db register-companies --dry-run

db-companies: ## Register the coverage companies in company/company_symbol (insert-only; starts no collector)
	$(UV) run datasources-db register-companies

db-inspect: sync ## Row counts per table
	$(UV) run datasources-db inspect

db-backfill: sync ## Store existing output/processed records (idempotent)
	$(UV) run datasources-db backfill

db-smoke: sync ## Bounded live end-to-end test into PostgreSQL
	$(UV) run datasources-db smoke --symbol $(T) --max-articles 5 --lookback-hours 72

similarity-check: ## Validate the semantic-analysis configuration (no writes)
	$(UV) run datasources-similarity check

similarity-download-model: ## Download the configured embedding model (explicit network access)
	$(UV) run datasources-similarity download-model

similarity-backfill: ## Index, then group, stored revisions without analysis (LIMIT=<n>)
	$(UV) run datasources-similarity backfill --limit $(LIMIT)

similarity-retry: ## Retry pending / partial analyses (LIMIT=<n>)
	$(UV) run datasources-similarity retry --limit $(LIMIT)

similarity-status: ## Analysis status, events and index coverage
	$(UV) run datasources-similarity status

summary-check: ## Validate the event-summary configuration (no writes, no model calls)
	$(UV) run datasources-summary check

summary-status: ## Summary jobs, versions, attempts and reviews
	$(UV) run datasources-summary status

summary-pending: ## Summary work backfill/retry would do (no model calls)
	$(UV) run datasources-summary pending --limit $(LIMIT)

summary-reviews: ## Open conflict reviews
	$(UV) run datasources-summary review list

clean: ## Remove Python and test caches
	find . -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true
	find . -name '.DS_Store' -delete 2>/dev/null || true
	rm -rf .pytest_cache

clean-cache: ## Drop the cached EDGAR submissions (they are re-fetched on demand)
	rm -rf data/cache
