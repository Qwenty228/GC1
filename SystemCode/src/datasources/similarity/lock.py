"""Database-level single-writer lock for graph processing.

The file lock in ``output/state/ingest.lock`` only protects one output
directory. Two workers with different output directories, or a worker and a
``datasources-similarity backfill`` / ``retry`` run, could still write events for
the same database concurrently and miss each other's uncommitted records.

This lock is a PostgreSQL session-level advisory lock held on a dedicated
connection for the life of the process. PostgreSQL releases it automatically
if the process dies. Live and sample data are separate partitions, so a demo
or test run never blocks the live worker.
"""

from __future__ import annotations

import logging

import psycopg

from ..ingest.settings import ConfigurationError

LOGGER = logging.getLogger(__name__)
LOCK_NAMESPACE = 0x44534752  # "DSGR"
PARTITIONS = {False: 0, True: 1}


class WriterLockError(ConfigurationError):
    """Another process already writes graph results for this partition."""


class GraphWriterLock:
    def __init__(self, database, *, is_sample: bool, purpose: str):
        self.database = database
        self.is_sample = is_sample
        self.purpose = purpose
        self._conn: psycopg.Connection | None = None

    @property
    def partition(self) -> str:
        return "sample" if self.is_sample else "live"

    def acquire(self) -> "GraphWriterLock":
        if self._conn is not None:
            return self
        try:
            conn = psycopg.connect(self.database.settings.conninfo(), autocommit=True)
        except psycopg.Error as error:
            raise self.database._connection_error(error) from error
        row = conn.execute("SELECT pg_try_advisory_lock(%s, %s)", (LOCK_NAMESPACE, PARTITIONS[self.is_sample])).fetchone()
        if not row or not row[0]:
            try:
                holder = conn.execute(
                    """
                    SELECT a.application_name, a.pid
                      FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
                     WHERE l.locktype = 'advisory' AND l.classid = %s::oid AND l.objid = %s::oid AND l.granted
                    """,
                    (LOCK_NAMESPACE, PARTITIONS[self.is_sample]),
                ).fetchone()
            except psycopg.Error:
                holder = None
            conn.close()
            who = f" (held by {holder[0] or 'unknown application'}, pid {holder[1]})" if holder else ""
            raise WriterLockError(
                f"another process is already writing graph results for the {self.partition} data in "
                f"{self.database.settings.dbname}{who}. Run one ingestion worker, backfill or retry at a time."
            )
        self._conn = conn
        LOGGER.info("acquired %s graph-writer lock for %s", self.partition, self.purpose)
        return self

    def release(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.execute("SELECT pg_advisory_unlock(%s, %s)", (LOCK_NAMESPACE, PARTITIONS[self.is_sample]))
        except psycopg.Error:
            pass  # closing the session releases it anyway
        finally:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "GraphWriterLock":
        return self.acquire()

    def __exit__(self, *exc) -> None:
        self.release()
