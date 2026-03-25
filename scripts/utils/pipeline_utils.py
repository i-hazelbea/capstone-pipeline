"""Shared pipeline metadata logging and dataframe helpers."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from .db_operations import DatabaseOperations


def start_pipeline_run(
    db: DatabaseOperations,
    logger: logging.Logger,
    pipeline_name: str,
    trigger_type: str = "manual",
) -> int | None:
    query = """
        INSERT INTO metadata.pipeline_run_log (
            pipeline_name,
            trigger_type,
            run_status,
            started_at
        )
        VALUES (%s, %s, 'started', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')
        RETURNING run_id;
    """
    try:
        result = db.execute_query(
            query, (pipeline_name, trigger_type), fetch=True)
        if result:
            return int(result[0][0])
    except Exception:
        logger.warning(
            "Metadata logging unavailable: unable to start pipeline run", exc_info=True)
    return None


def complete_pipeline_run(
    db: DatabaseOperations,
    logger: logging.Logger,
    run_id: int | None,
    run_status: str,
    error_message: str | None = None,
) -> None:
    if run_id is None:
        return

    query = """
        UPDATE metadata.pipeline_run_log
        SET
            run_status = %s,
            error_message = %s,
            finished_at = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila',
            duration_seconds = EXTRACT(
                EPOCH FROM ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila') - started_at)
            )::INT
        WHERE run_id = %s;
    """
    try:
        db.execute_query(query, (run_status, error_message, run_id))
    except Exception:
        logger.warning(
            "Metadata logging unavailable: unable to complete pipeline run", exc_info=True)


def start_task_log(
    db: DatabaseOperations,
    logger: logging.Logger,
    run_id: int | None,
    stage_name: str,
    task_name: str,
    source_file: str | None = None,
    source_relation: str | None = None,
    target_relation: str | None = None,
) -> int | None:
    if run_id is None:
        return None

    query = """
        INSERT INTO metadata.pipeline_task_log (
            run_id,
            stage_name,
            task_name,
            source_file,
            source_relation,
            target_relation,
            status,
            started_at
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            'started',
            CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila'
        )
        RETURNING task_id;
    """
    try:
        result = db.execute_query(
            query,
            (run_id, stage_name, task_name, source_file, source_relation, target_relation),
            fetch=True,
        )
        if result:
            return int(result[0][0])
    except Exception:
        logger.warning(
            "Metadata logging unavailable: unable to start task log", exc_info=True)
    return None


def complete_task_log(
    db: DatabaseOperations,
    logger: logging.Logger,
    task_id: int | None,
    status: str,
    rows_in: int | None = None,
    rows_out: int | None = None,
    rows_rejected: int | None = None,
    error_message: str | None = None,
) -> None:
    if task_id is None:
        return

    if rows_rejected is None:
        query = """
            UPDATE metadata.pipeline_task_log
            SET
                status = %s,
                rows_in = %s,
                rows_out = %s,
                error_message = %s,
                finished_at = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila',
                duration_seconds = EXTRACT(
                    EPOCH FROM ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila') - started_at)
                )::INT
            WHERE task_id = %s;
        """
        params = (status, rows_in, rows_out, error_message, task_id)
    else:
        query = """
            UPDATE metadata.pipeline_task_log
            SET
                status = %s,
                rows_in = %s,
                rows_out = %s,
                rows_rejected = %s,
                error_message = %s,
                finished_at = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila',
                duration_seconds = EXTRACT(
                    EPOCH FROM ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila') - started_at)
                )::INT
            WHERE task_id = %s;
        """
        params = (status, rows_in, rows_out,
                  rows_rejected, error_message, task_id)

    try:
        db.execute_query(query, params)
    except Exception:
        logger.warning(
            "Metadata logging unavailable: unable to complete task log", exc_info=True)


def nullify(value: Any) -> Any:
    """Normalize null-like values to None for PostgreSQL writes."""
    if pd.isna(value):  # checks for pandas specific nulls, and returns None if it finds any
        return None
    if isinstance(value, str):  # if is instance of a string, remove leading and trailing whitespace
        cleaned = value.strip()
        return cleaned if cleaned else None
    return value


def prepare_rows(frame: pd.DataFrame) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Convert DataFrame into column names and row tuples for bulk insert."""
    frame = frame.astype(object).where(pd.notna(frame), None)  # type: ignore
    columns = frame.columns.tolist()
    rows = [tuple(nullify(value) for value in row)
            for row in frame.itertuples(index=False, name=None)]
    return columns, rows


def normalize_mixed_dates(series: pd.Series) -> pd.Series:
    """Parse mixed date formats and render them as YYYY-MM-DD strings."""
    parsed = pd.to_datetime(
        series, errors="coerce")  # attempts to turn rows into dates
    still_null = parsed.isna()  # list which row succeeded/failed
    if still_null.any():  # if there are any NaT values
        parsed2 = pd.to_datetime(
            series[still_null], errors="coerce", dayfirst=True)
        # place new values that were previously marked as still_null
        parsed.loc[still_null] = parsed2
    return parsed.dt.strftime("%Y-%m-%d")
