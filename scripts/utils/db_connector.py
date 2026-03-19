"""PostgreSQL connection and write helpers for ingestion."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterable, Optional, Sequence

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from .logger import setup_logger

logger = setup_logger(__name__)


class DatabaseConnector:
    """Thin wrapper around psycopg2 for common ingestion operations."""

    def __init__(self) -> None:
        """Connection defined in .env file"""
        self.host = os.getenv("DB_HOST", "postgres")
        self.port = int(os.getenv("DB_PORT", "5432"))
        self.database = os.getenv(
            "DB_NAME", os.getenv("POSTGRES_DB", "movie_db"))
        self.user = os.getenv("ETL_DB_USER", "etl")
        self.password = os.getenv("ETL_DB_PASSWORD", "")

    @contextmanager
    def get_connection(self):
        """Yield a database connection and guarantee close."""
        connection = None
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            yield connection  # returns active connection
        except Exception:
            logger.exception("Database connection failure")
            raise
        finally:
            if connection is not None:
                connection.close()

    def execute_query(
        self,
        query: str,
        params: Optional[Sequence[object]] = None,
        fetch: bool = False,
    ):
        """Execute one SQL query with optional result fetching."""
        with self.get_connection() as connection:  # database connection object
            with connection.cursor() as cursor:  # cursor object created
                cursor.execute(query, params)  # sql query sent to database
                # if true, retrieve all rows of the result
                result = cursor.fetchall() if fetch else None
                connection.commit()  # save query changes
                return result

    def bulk_insert(
        self,
        table: str,
        columns: Sequence[str],
        data: Iterable[Sequence[object]],
        schema: str = "raw",
        page_size: int = 10000,  # num of rows to insert in a single SQL statement
    ) -> int:
        """Insert records efficiently using execute_values."""
        rows = list(data)  # ensure data iterable is turned into a list
        if not rows:
            logger.warning("No rows to insert into %s.%s", schema, table)
            return 0

        # construct sql query securely
        insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        )

        with self.get_connection() as connection:
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
        with self.get_connection() as connection:
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
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(count_query)
                result = cursor.fetchone()
                return int(result[0]) if result else 0
