"""Ingest raw movie datasets into PostgreSQL raw schema."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from utils.db_connector import DatabaseConnector
from utils.db_operations import DatabaseOperations
from utils.logger import setup_logger
from utils.pipeline_utils import (
    complete_pipeline_run,
    complete_task_log,
    prepare_rows,
    start_pipeline_run,
    start_task_log,
)

logger = setup_logger(__name__)


class MovieDataIngestion:
    """Orchestrates as-is file ingestion for raw layer tables."""

    def __init__(
        self,
        data_dir: str = "/app/datasets",
    ) -> None:
        self.data_dir = Path(data_dir)
        self.db = DatabaseOperations(DatabaseConnector())
        self.run_id: int | None = None

    @staticmethod
    def _read_json_records(json_path: Path) -> list[dict[str, Any]]:
        with json_path.open("r", encoding="utf-8") as handle:
            # translates text inside file to python object
            payload = json.load(handle)

        if isinstance(payload, list):  # if already a list, return as-is
            return payload
        if isinstance(payload, dict):  # if it's a dictionary, turn it into a list using brackets
            return [payload]
        raise ValueError(f"Unsupported JSON structure in {json_path}")

    def load_csv_to_postgres(self, csv_path: Path, table_name: str, schema: str = "raw") -> int:
        """Load one CSV file as-is into a Postgres table (no transformations)."""
        if not csv_path.exists():  # check if file exists
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        logger.info("Loading CSV: %s -> %s.%s",
                    csv_path.name, schema, table_name)

        frame = pd.read_csv(csv_path, dtype=str)  # read csv file

        # convert to dataframe for bulk insert
        columns, rows = prepare_rows(frame)
        return self.db.bulk_insert(table_name, columns, rows, schema=schema)

    @staticmethod
    def _flatten_ratings(records: Iterable[dict[str, Any]]) -> tuple[list[str], list[tuple[Any, ...]]]:
        columns = ["movie_id", "avg_rating",
                   "total_ratings", "stg_dev", "loaded_at"]  # headers
        loaded_at = datetime.now(timezone.utc).isoformat()
        flattened: list[tuple[Any, ...]] = []

        for item in records:  # for each item in the records json string create a tuple and append to flattened
            # look for the nested ratings_summary
            summary = item.get("ratings_summary") or {}
            # creates a tuple for this row
            flattened.append(
                (
                    item.get("movie_id"),
                    summary.get("avg_rating"),
                    summary.get("total_ratings"),
                    summary.get("std_dev"),
                    loaded_at,
                )
            )

        return columns, flattened

    def load_json_to_postgres(self, json_path: Path, table_name: str, schema: str = "raw") -> int:
        """Load one JSON file into a Postgres table."""
        if not json_path.exists():
            raise FileNotFoundError(f"JSON file not found: {json_path}")

        logger.info("Loading JSON: %s -> %s.%s",
                    json_path.name, schema, table_name)
        # turn json into list of dictionaries using load()
        records = self._read_json_records(json_path)
        if not records:
            logger.warning("No records found in %s", json_path.name)
            return 0

        if table_name == "ratings":
            columns, rows = self._flatten_ratings(records)
        else:
            columns = list(records[0].keys())  # list columns
            rows = [tuple(record.get(column) for column in columns)  # create a row (tuple) using the record.get(column) where column is the key and record is a dictionary with key-value pairs
                    for record in records]

        return self.db.bulk_insert(table_name, columns, rows, schema=schema)

    def ingest_movies_main(self, truncate: bool = True) -> int:
        if truncate:
            self.db.truncate_table("movies_main", schema="raw")
        return self.load_csv_to_postgres(self.data_dir / "movies_main.csv", "movies_main", schema="raw")

    def ingest_movie_extended(self, truncate: bool = True) -> int:
        if truncate:
            self.db.truncate_table("movie_extended", schema="raw")
        return self.load_csv_to_postgres(self.data_dir / "movie_extended.csv", "movie_extended", schema="raw")

    def ingest_ratings(self, truncate: bool = True) -> int:
        if truncate:
            self.db.truncate_table("ratings", schema="raw")
        return self.load_json_to_postgres(self.data_dir / "ratings.json", "ratings", schema="raw")

    def ingest_all(self, truncate: bool = True) -> dict[str, int]:
        """Run the complete ingestion sequence."""
        logger.info("Starting ingestion from %s", self.data_dir)

        # create a log entry for starting the pipeline run
        self.run_id = start_pipeline_run(
            self.db,
            logger,
            "movie_mart_ingestion",
            trigger_type="manual",
        )
        # for storing inserted rows per table like movies_main -> count
        results: dict[str, int] = {}

        tasks = [
            (
                "movies_main",                                  # table name
                self.ingest_movies_main,                        # function call
                str(self.data_dir / "movies_main.csv"),         # file path
                "raw.movies_main",                              # database target
            ),
            (
                "movie_extended",
                self.ingest_movie_extended,
                str(self.data_dir / "movie_extended.csv"),
                "raw.movie_extended",
            ),
            (
                "ratings",
                self.ingest_ratings,
                str(self.data_dir / "ratings.json"),
                "raw.ratings",
            ),
        ]

        try:
            for table_name, load_func, source_file, target_relation in tasks:
                # start a task log for each task in tuple list above
                task_id = start_task_log(
                    self.db,
                    logger,
                    self.run_id,
                    stage_name="ingestion",
                    task_name=f"load_{table_name}",
                    source_file=source_file,
                    target_relation=target_relation,
                )
                try:
                    inserted = load_func(truncate=truncate)
                    results[table_name] = inserted  # count result
                    complete_task_log(  # marks task completed
                        self.db,
                        logger,
                        task_id,
                        status="completed",
                        rows_in=inserted,
                        rows_out=inserted,
                        rows_rejected=0,
                    )
                except Exception as exc:
                    complete_task_log(  # if failed
                        self.db,
                        logger,
                        task_id,
                        status="failed",
                        rows_rejected=0,
                        error_message=str(exc),
                    )
                    raise

            for table in results:
                count = self.db.get_table_count(table, schema="raw")
                logger.info("raw.%s row count: %s", table, f"{count:,}")

            complete_pipeline_run(
                self.db,
                logger,
                self.run_id,
                run_status="completed",
            )
            logger.info("Ingestion completed successfully")
            return results
        except Exception as exc:
            complete_pipeline_run(
                self.db,
                logger,
                self.run_id,
                run_status="failed",
                error_message=str(exc),
            )
            raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Movie data raw ingestion (as-is)")
    # set path for data directory
    parser.add_argument(
        "--data-dir",
        default="/app/datasets",
        help="Directory containing movies_main.csv, movie_extended.csv, ratings.json",
    )
    # if user wants to append instead of truncate
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Append data instead of truncating target tables first",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ingestion = MovieDataIngestion(
        data_dir=args.data_dir,
    )

    try:
        ingestion.ingest_all(truncate=not args.no_truncate)
        return 0
    except Exception:
        logger.exception("Ingestion failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
