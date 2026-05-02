"""Database helpers for Supabase Postgres and PostGIS."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Sequence

import geopandas as gpd
from sqlalchemy import Engine, create_engine, text

from config.settings import Settings


class Database:
    """Thin database wrapper around SQLAlchemy."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.engine: Engine = create_engine(
            settings.sqlalchemy_database_url,
            future=True,
            pool_pre_ping=True,
        )

    def run_migrations(self) -> None:
        """Execute ordered SQL migration files."""

        sql_files = sorted(self.settings.sql_dir.glob("*.sql"))
        lock_sql = "select pg_advisory_lock(hashtext('landintel.run_migrations')::bigint)"
        unlock_sql = "select pg_advisory_unlock(hashtext('landintel.run_migrations')::bigint)"

        with self.engine.connect() as connection:
            connection.exec_driver_sql(lock_sql)
            connection.commit()
            try:
                for path in sql_files:
                    sql = path.read_text(encoding="utf-8")
                    with connection.begin():
                        connection.exec_driver_sql(sql)
            finally:
                if connection.in_transaction():
                    connection.rollback()
                connection.exec_driver_sql(unlock_sql)
                connection.commit()

    def execute_script(self, path: Path) -> None:
        """Execute a single SQL file inside a transaction."""

        sql = path.read_text(encoding="utf-8")
        with self.engine.begin() as connection:
            connection.exec_driver_sql(sql)

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        """Execute a single SQL statement."""

        with self.engine.begin() as connection:
            connection.execute(text(sql), params or {})

    def execute_many(self, sql: str, params_list: Sequence[dict[str, Any]]) -> None:
        """Execute a SQL statement against many parameter mappings."""

        if not params_list:
            return
        with self.engine.begin() as connection:
            connection.execute(text(sql), list(params_list))

    def scalar(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        """Return a scalar value."""

        with self.engine.begin() as connection:
            return connection.execute(text(sql), params or {}).scalar_one()

    def fetch_one(self, sql: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Fetch a single row as a mapping."""

        with self.engine.begin() as connection:
            row = connection.execute(text(sql), params or {}).mappings().first()
        return dict(row) if row else None

    def fetch_all(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Fetch all rows as mappings."""

        with self.engine.begin() as connection:
            rows = connection.execute(text(sql), params or {}).mappings().all()
        return [dict(row) for row in rows]

    def read_geodataframe(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        geom_col: str = "geometry",
    ) -> gpd.GeoDataFrame:
        """Read a GeoDataFrame from PostGIS."""

        return gpd.read_postgis(
            text(sql),
            self.engine,
            geom_col=geom_col,
            params=params,
        )

    def dispose(self) -> None:
        """Dispose of the underlying engine."""

        self.engine.dispose()


def chunked(items: Sequence[dict[str, Any]], batch_size: int) -> Iterable[Sequence[dict[str, Any]]]:
    """Yield batched slices from a list of dictionaries."""

    for offset in range(0, len(items), batch_size):
        yield items[offset : offset + batch_size]
