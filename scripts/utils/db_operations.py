"""Database operation helpers built on top of DatabaseConnector."""

from __future__ import annotations

from typing import Iterable, Sequence

from psycopg2 import sql
from psycopg2.extras import execute_values

from .db_connector import DatabaseConnector
from .logger import setup_logger

logger = setup_logger(__name__)


class DatabaseOperations:
    """Common SQL execution and table write/read helpers."""

    def __init__(self, connector: DatabaseConnector) -> None:
        self.connector = connector

    def execute_query(
        self,
        query: str,
        params: Sequence[object] | None = None,
        fetch: bool = False,
    ):
        """Execute one SQL query with optional result fetching."""
        with self.connector.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchall() if fetch else None
                connection.commit()
                return result

    def bulk_insert(
        self,
        table: str,
        columns: Sequence[str],
        data: Iterable[Sequence[object]],
        schema: str = "raw",
        page_size: int = 10000,
    ) -> int:
        """Insert records efficiently using execute_values."""
        rows = list(data)
        if not rows:
            logger.warning("No rows to insert into %s.%s", schema, table)
            return 0

        insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        )

        with self.connector.get_connection() as connection:
            with connection.cursor() as cursor:
                execute_values(cursor, insert_query.as_string(
                    cursor), rows, page_size=page_size)
                connection.commit()

        logger.info("Inserted %s rows into %s.%s",
                    f"{len(rows):,}", schema, table)
        return len(rows)

    def truncate_table(self, table: str, schema: str = "raw") -> None:
        """Truncate target table before full reload."""
        truncate_query = sql.SQL("TRUNCATE TABLE {}.{};").format(
            sql.Identifier(schema),
            sql.Identifier(table),
        )
        with self.connector.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(truncate_query)
                connection.commit()

        logger.info("Truncated table %s.%s", schema, table)

    def get_table_count(self, table: str, schema: str = "raw") -> int:
        """Return row count for a table."""
        count_query = sql.SQL("SELECT COUNT(*) FROM {}.{};").format(
            sql.Identifier(schema),
            sql.Identifier(table),
        )
        with self.connector.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(count_query)
                result = cursor.fetchone()
                return int(result[0]) if result else 0
