"""Connection pool and health check for the `stock_news` database.

The ingestion worker makes every database call from `IngestionService.process_*`,
which the writer already runs off the event loop via `asyncio.to_thread`, so a
synchronous driver never blocks asyncio. `Database.transaction()` is the only
way records are written: one transaction per source record, so a failed related
insert rolls the whole record back instead of leaving a partial article.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ..ingest.settings import ConfigurationError
from .settings import DbSettings, load_db_settings

LOGGER = logging.getLogger(__name__)


class DatabaseError(RuntimeError):
    """A database problem that ingestion should surface without leaking credentials."""


@dataclass(frozen=True)
class ConnectionInfo:
    """What a connection check reports. Deliberately carries no credentials."""

    database: str
    server_version: str
    user: str
    host: str
    port: int
    pgvector_installed: bool
    pgvector_version: str | None
    tables: int

    def describe(self) -> str:
        vector = f"pgvector {self.pgvector_version} enabled" if self.pgvector_installed else "pgvector NOT enabled"
        return (
            f"connected to {self.database} as {self.user} at {self.host}:{self.port} "
            f"(PostgreSQL {self.server_version}, {self.tables} tables, {vector})"
        )


class Database:
    """Lazily-opened pool of connections to one PostgreSQL database."""

    def __init__(self, settings: DbSettings | None = None, *, open_now: bool = False):
        self.settings = settings or load_db_settings()
        self._pool: ConnectionPool | None = None
        if open_now:
            self.pool()

    # ------------------------------------------------------------------ lifecycle
    def pool(self) -> ConnectionPool:
        if self._pool is None:
            # Probe once directly first. The pool retries and then reports only
            # "pool initialization incomplete", which hides whether the real
            # problem was a bad password, a missing database or a dead server.
            self._probe()
            try:
                pool = ConnectionPool(
                    self.settings.conninfo(),
                    min_size=self.settings.pool_min_size,
                    max_size=self.settings.pool_max_size,
                    kwargs={"row_factory": dict_row},
                    open=False,
                    name="datasources",
                )
                pool.open(wait=True, timeout=float(self.settings.connect_timeout))
            except Exception as error:  # pool failures wrap the libpq error
                raise self._connection_error(error) from error
            self._pool = pool
        return self._pool

    def _probe(self) -> None:
        """One direct connection, so a misconfiguration is reported accurately and fast."""
        try:
            psycopg.connect(self.settings.conninfo()).close()
        except psycopg.Error as error:
            raise self._connection_error(error) from error

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    def __enter__(self) -> "Database":
        self.pool()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _connection_error(self, error: Exception) -> DatabaseError:
        """Turn a driver error into an actionable message that hides the password."""
        text = str(error).replace(self.settings.password, "***") if self.settings.password else str(error)
        hint = ""
        lowered = text.lower()
        if "password authentication failed" in lowered or "no password supplied" in lowered:
            hint = " Check PGUSER / PGPASSWORD in .env."
        elif "does not exist" in lowered and self.settings.dbname in text:
            hint = f" Database {self.settings.dbname!r} does not exist on that server."
        elif "could not connect" in lowered or "connection refused" in lowered or "timeout" in lowered:
            hint = " Is PostgreSQL running and reachable on PGHOST/PGPORT?"
        return DatabaseError(f"cannot connect to {self.settings.safe_target()}: {text}{hint}")

    # ------------------------------------------------------------------ connections
    @contextmanager
    def connection(self) -> Iterator[Connection]:
        """A pooled connection. Any error is re-raised with credentials stripped."""
        try:
            with self.pool().connection() as conn:
                yield conn
        except DatabaseError:
            raise
        except psycopg.OperationalError as error:
            raise self._connection_error(error) from error

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """One explicit transaction: it commits on success and rolls back on any error."""
        with self.connection() as conn:
            with conn.transaction():
                yield conn

    # ------------------------------------------------------------------ health check
    def check(self) -> ConnectionInfo:
        """Report the database, server version and whether pgvector is enabled."""
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT current_database() AS database,
                       current_user       AS "user",
                       split_part(current_setting('server_version'), ' ', 1) AS server_version,
                       (SELECT extversion FROM pg_extension WHERE extname = 'vector') AS pgvector_version,
                       (SELECT count(*) FROM information_schema.tables
                         WHERE table_schema = 'public' AND table_type = 'BASE TABLE') AS tables
                """
            )
            row = cur.fetchone()
        assert row is not None
        return ConnectionInfo(
            database=row["database"],
            server_version=row["server_version"],
            user=row["user"],
            host=self.settings.host,
            port=self.settings.port,
            pgvector_installed=row["pgvector_version"] is not None,
            pgvector_version=row["pgvector_version"],
            tables=int(row["tables"]),
        )

    def require_database(self, expected: str) -> ConnectionInfo:
        """Fail loudly when pointed at the wrong database (used by the smoke test)."""
        info = self.check()
        if info.database != expected:
            raise ConfigurationError(
                f"connected to {info.database!r} but expected {expected!r}; check PGDATABASE in .env"
            )
        return info
