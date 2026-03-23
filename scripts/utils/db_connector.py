"""PostgreSQL connection utility for ingestion and staging scripts."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg2

from .logger import setup_logger

logger = setup_logger(__name__)


class DatabaseConnector:
    """Thin wrapper around psycopg2 for connection lifecycle management."""

    def __init__(self) -> None:
        """Connection defined in .env file"""
        self.host = os.getenv("DB_HOST", "postgres")
        self.port = int(os.getenv("DB_PORT", "5432"))
        self.database = os.getenv(
            "DB_NAME", os.getenv("POSTGRES_DB", "movie_db"))
        self.user = os.getenv("ETL_DB_USER", "etl")
        self.password = os.getenv("ETL_DB_PASSWORD", "")

    @contextmanager
    def get_connection(self) -> Iterator[psycopg2.extensions.connection]:
        """Yield a database connection and guarantee close."""
        connection: psycopg2.extensions.connection | None = None
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
