"""PostgreSQL connection settings, read from the environment like the rest of the app.

Uses the standard libpq variable names so `psql`, pgAdmin and this project can
share one `.env`:

    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

`DATASOURCES_DB_ENABLED=false` turns persistence off without removing settings.
Nothing here ever logs or repr's the password.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from .. import config  # noqa: F401  (imported for its .env loading side effect)
from ..ingest.settings import ConfigurationError, _env, _env_bool, _env_int


@dataclass(frozen=True)
class DbSettings:
    host: str = "localhost"
    port: int = 5432
    dbname: str = "stock_news"
    user: str = "postgres"
    password: str = field(default="", repr=False)
    connect_timeout: int = 10
    pool_min_size: int = 1
    pool_max_size: int = 4
    application_name: str = "datasources-ingest"

    def conninfo(self) -> str:
        """libpq connection string. Never log this: it carries the password."""
        parts = {
            "host": self.host,
            "port": str(self.port),
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "connect_timeout": str(self.connect_timeout),
            "application_name": self.application_name,
        }
        return " ".join(f"{key}={_quote(value)}" for key, value in parts.items() if value != "")

    def safe_target(self) -> str:
        """Human-readable target with no credentials, for logs and error messages."""
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.dbname}"


def _quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'" if (" " in escaped or escaped == "") else escaped


def db_enabled() -> bool:
    """Whether ingestion should persist to PostgreSQL (default: yes)."""
    return _env_bool("DATASOURCES_DB_ENABLED", True)


def load_db_settings() -> DbSettings:
    """Build settings from the environment, validating what libpq cannot default."""
    settings = DbSettings(
        host=_env("PGHOST", "localhost"),
        port=_env_int("PGPORT", 5432),
        dbname=_env("PGDATABASE", "stock_news"),
        user=_env("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        connect_timeout=_env_int("DB_CONNECT_TIMEOUT_SECONDS", 10),
        pool_min_size=_env_int("DB_POOL_MIN_SIZE", 1),
        pool_max_size=_env_int("DB_POOL_MAX_SIZE", 4),
        application_name=_env("DB_APPLICATION_NAME", "datasources-ingest"),
    )
    missing = [name for name, value in (
        ("PGHOST", settings.host), ("PGDATABASE", settings.dbname), ("PGUSER", settings.user),
    ) if not value.strip()]
    if missing:
        raise ConfigurationError(
            f"{', '.join(missing)} must be set for PostgreSQL persistence. "
            "Copy .env.example to .env and fill in the PG* values."
        )
    if not settings.password:
        raise ConfigurationError(
            "PGPASSWORD is not set. Add it to .env (it is git-ignored) — for example "
            f"PGPASSWORD=<your password> for {settings.safe_target()}. "
            "Set DATASOURCES_DB_ENABLED=false to run ingestion without a database."
        )
    if not 1 <= settings.port <= 65535:
        raise ConfigurationError("PGPORT must be between 1 and 65535")
    if settings.pool_min_size < 1 or settings.pool_max_size < settings.pool_min_size:
        raise ConfigurationError("DB_POOL_MIN_SIZE must be >= 1 and <= DB_POOL_MAX_SIZE")
    return settings
