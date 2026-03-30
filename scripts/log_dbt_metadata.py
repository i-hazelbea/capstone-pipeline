"""Log dbt model build results into metadata tables."""

from __future__ import annotations
from utils.logger import setup_logger
from utils.db_operations import DatabaseOperations
from utils.db_connector import DatabaseConnector

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))


logger = setup_logger(__name__)
PH_TZ = timezone(timedelta(hours=8))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Log dbt model creation details into metadata.pipeline_task_log"
    )
    parser.add_argument(
        "--run-results",
        required=True,
        help="Path to dbt target/run_results.json",
    )
    parser.add_argument(
        "--manifest",
        required=True,
        help="Path to dbt target/manifest.json",
    )
    parser.add_argument(
        "--stage-name",
        required=True,
        choices=["staging", "intermediate", "marts", "analytics"],
        help="Schema stage to log (matches dbt schema)",
    )
    parser.add_argument(
        "--pipeline-name",
        default="movie_mart_dbt",
        help="Pipeline name to store in metadata.pipeline_run_log",
    )
    return parser.parse_args()


def load_json(path: str) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed
    # Store PH-local wall-clock time in TIMESTAMP columns (no timezone type).
    return parsed.astimezone(PH_TZ).replace(tzinfo=None)


def normalize_relation_name(relation: str | None) -> str | None:
    if not relation:
        return None
    return relation.replace('"', "")


def relation_from_entry(entry: dict[str, Any] | None) -> str | None:
    if not entry:
        return None

    relation_name = normalize_relation_name(entry.get("relation_name"))
    if relation_name:
        return relation_name

    database = entry.get("database")
    schema = entry.get("schema")
    identifier = entry.get("alias") or entry.get(
        "identifier") or entry.get("name")
    parts = [part for part in [database, schema, identifier] if part]
    return ".".join(parts) if parts else None


def resolve_stage_name(db: DatabaseOperations, desired_stage: str) -> str:
    """Return a stage_name value compatible with current DB CHECK constraints."""
    try:
        query = """
            SELECT cc.check_clause
            FROM information_schema.table_constraints tc
            JOIN information_schema.check_constraints cc
              ON tc.constraint_schema = cc.constraint_schema
             AND tc.constraint_name = cc.constraint_name
            WHERE tc.table_schema = 'metadata'
              AND tc.table_name = 'pipeline_task_log'
              AND tc.constraint_type = 'CHECK'
              AND cc.check_clause ILIKE '%stage_name%';
        """
        result = db.execute_query(query, fetch=True)
        if not result:
            return desired_stage

        allowed: set[str] = set()
        for row in result:
            check_clause = str(row[0])
            allowed.update(re.findall(r"'([^']+)'", check_clause))
        if not allowed:
            return desired_stage
        if desired_stage in allowed:
            return desired_stage

        fallback_order = ["dbt", "staging", "marts", "analytics", "ingestion"]
        for candidate in fallback_order:
            if candidate in allowed:
                logger.warning(
                    "Stage '%s' is not allowed by current pipeline_task_log constraint; "
                    "falling back to '%s'.",
                    desired_stage,
                    candidate,
                )
                return candidate
    except Exception:
        logger.warning(
            "Unable to inspect stage_name constraint; continuing with requested stage '%s'.",
            desired_stage,
            exc_info=True,
        )
    return desired_stage


def start_run(
    db: DatabaseOperations,
    pipeline_name: str,
    stage_name: str,
    orchestrator_run_id: str | None,
) -> int:
    query = """
        INSERT INTO metadata.pipeline_run_log (
            pipeline_name,
            orchestrator_run_id,
            trigger_type,
            run_status,
            started_at
        )
        VALUES (%s, %s, %s, 'started', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')
        RETURNING run_id;
    """
    result = db.execute_query(
        query,
        (pipeline_name, orchestrator_run_id, f"dbt_{stage_name}"),
        fetch=True,
    )
    if not result:
        raise RuntimeError("Unable to create metadata.pipeline_run_log entry")
    return int(result[0][0])


def complete_run(db: DatabaseOperations, run_id: int, run_status: str, error_message: str | None = None) -> None:
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
    db.execute_query(query, (run_status, error_message, run_id))


def insert_task_log(
    db: DatabaseOperations,
    run_id: int,
    stage_name: str,
    task_name: str,
    source_file: str | None,
    source_relation: str | None,
    target_relation: str | None,
    status: str,
    rows_out: int | None,
    error_message: str | None,
    started_at: datetime | None,
    finished_at: datetime | None,
    duration_seconds: int | None,
) -> None:
    query = """
        INSERT INTO metadata.pipeline_task_log (
            run_id,
            stage_name,
            task_name,
            source_file,
            source_relation,
            target_relation,
            status,
            rows_out,
            error_message,
            started_at,
            finished_at,
            duration_seconds
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    db.execute_query(
        query,
        (
            run_id,
            stage_name,
            task_name,
            source_file,
            source_relation,
            target_relation,
            status,
            rows_out,
            error_message,
            started_at,
            finished_at,
            duration_seconds,
        ),
    )


def main() -> int:
    args = parse_args()
    db = DatabaseOperations(DatabaseConnector())

    run_results = load_json(args.run_results)
    manifest = load_json(args.manifest)
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    try:
        log_stage_name = resolve_stage_name(db, args.stage_name)

        orchestrator_run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID")
        run_id = start_run(db, args.pipeline_name,
                           log_stage_name, orchestrator_run_id)
    except Exception:
        logger.warning(
            "Metadata logging is unavailable (run start failed). Continuing pipeline without metadata.",
            exc_info=True,
        )
        return 0

    total_models = 0
    failed_models = 0
    try:
        for result in run_results.get("results", []):
            unique_id = result.get("unique_id")
            node = nodes.get(unique_id)
            if not node:
                continue
            if node.get("resource_type") != "model":
                continue
            if node.get("schema") != args.stage_name:
                continue

            total_models += 1
            raw_status = result.get("status")
            if raw_status == "success":
                status = "completed"
            elif raw_status == "skipped":
                status = "skipped"
            else:
                status = "failed"
                failed_models += 1

            timing = result.get("timing", [])
            started_at = parse_iso(timing[0].get(
                "started_at")) if timing else None
            finished_at = parse_iso(
                timing[-1].get("completed_at")) if timing else None
            execution_time = result.get("execution_time")
            duration_seconds = int(round(execution_time)) if isinstance(
                execution_time, (int, float)) else None

            adapter_response = result.get("adapter_response") or {}
            rows_affected = adapter_response.get("rows_affected")
            rows_out = rows_affected if isinstance(
                rows_affected, int) and rows_affected >= 0 else None

            parent_relations: list[str] = []
            for parent_unique_id in node.get("depends_on", {}).get("nodes", []):
                parent_entry = nodes.get(parent_unique_id)
                if parent_entry is None:
                    parent_entry = sources.get(parent_unique_id)
                relation = relation_from_entry(parent_entry)
                if relation:
                    parent_relations.append(relation)

            source_relation = ", ".join(
                sorted(set(parent_relations))) if parent_relations else None
            target_relation = normalize_relation_name(
                result.get("relation_name")) or relation_from_entry(node)

            try:
                insert_task_log(
                    db=db,
                    run_id=run_id,
                    stage_name=log_stage_name,
                    task_name=node.get("name", str(unique_id)),
                    source_file=node.get("original_file_path"),
                    source_relation=source_relation,
                    target_relation=target_relation,
                    status=status,
                    rows_out=rows_out,
                    error_message=result.get(
                        "message") if status == "failed" else None,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_seconds=duration_seconds,
                )
            except Exception:
                logger.warning(
                    "Failed to insert metadata for model '%s'; continuing.",
                    node.get("name", str(unique_id)),
                    exc_info=True,
                )

        run_status = "completed" if failed_models == 0 else "partial"
        try:
            complete_run(db, run_id, run_status)
        except Exception:
            logger.warning(
                "Failed to finalize metadata run log; continuing.", exc_info=True)
        logger.info(
            "Logged dbt metadata for stage=%s models=%s failed=%s",
            args.stage_name,
            total_models,
            failed_models,
        )
        return 0
    except Exception as exc:
        try:
            complete_run(db, run_id, "failed", str(exc))
        except Exception:
            logger.warning(
                "Failed to finalize failed metadata run log.", exc_info=True)
        logger.exception(
            "Failed to log dbt metadata; continuing pipeline without blocking")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
