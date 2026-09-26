"""PostgreSQL persistence for ingested news and filings (`stock_news` database).

    from datasources.db import Database, persist_record

    with Database() as db, db.transaction() as conn:
        result = persist_record(conn, record, raw_payload=article)

Embeddings are deliberately out of scope here: see `repository` for what is left
to the later embedding stage.
"""

from .connection import ConnectionInfo, Database, DatabaseError
from .migrate import MigrationResult, apply_migrations, migration_files
from .repository import (
    ALPACA_NAMESPACE,
    PersistenceError,
    PersistResult,
    map_content,
    persist_record,
    publisher_key,
)
from .settings import DbSettings, db_enabled, load_db_settings

__all__ = [
    "ALPACA_NAMESPACE",
    "ConnectionInfo",
    "Database",
    "DatabaseError",
    "DbSettings",
    "MigrationResult",
    "PersistResult",
    "PersistenceError",
    "apply_migrations",
    "db_enabled",
    "load_db_settings",
    "map_content",
    "migration_files",
    "persist_record",
    "publisher_key",
]
