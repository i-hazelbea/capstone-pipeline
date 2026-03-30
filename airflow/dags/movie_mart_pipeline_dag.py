"""Airflow DAG orchestrating the full Movie Mart pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "movie-mart",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="movie_mart_full_pipeline",
    description="Run ingestion, staging, and schema-scoped dbt builds/tests",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["movie-mart", "pipeline", "etl", "dbt"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command="""
        set -euo pipefail
        docker exec mm-python-scripts python /scripts/ingest_data.py
        """,
    )

    stage_data = BashOperator(
        task_id="stage_data",
        bash_command="""
        set -euo pipefail
        docker exec mm-python-scripts python /scripts/stage_data.py
        """,
    )

    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command="""
        set -euo pipefail
        echo "[$(date -Iseconds)] dbt build staging start"
        docker exec mm-dbt dbt build --select path:models/staging --fail-fast
                python /opt/airflow/scripts/log_dbt_metadata.py \
                    --run-results /opt/airflow/dbt/target/run_results.json \
                    --manifest /opt/airflow/dbt/target/manifest.json \
                    --stage-name staging \
                    --pipeline-name movie_mart_dbt
        echo "[$(date -Iseconds)] dbt build staging end"
        """,
    )

    dbt_build_intermediate = BashOperator(
        task_id="dbt_build_intermediate",
        bash_command="""
        set -euo pipefail
        echo "[$(date -Iseconds)] dbt build intermediate start"
        docker exec mm-dbt dbt build --select path:models/intermediate --fail-fast
                python /opt/airflow/scripts/log_dbt_metadata.py \
                    --run-results /opt/airflow/dbt/target/run_results.json \
                    --manifest /opt/airflow/dbt/target/manifest.json \
                    --stage-name intermediate \
                    --pipeline-name movie_mart_dbt
        echo "[$(date -Iseconds)] dbt build intermediate end"
        """,
    )

    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command="""
        set -euo pipefail
        echo "[$(date -Iseconds)] dbt build marts start"
        docker exec mm-dbt dbt build --select path:models/marts --fail-fast
                python /opt/airflow/scripts/log_dbt_metadata.py \
                    --run-results /opt/airflow/dbt/target/run_results.json \
                    --manifest /opt/airflow/dbt/target/manifest.json \
                    --stage-name marts \
                    --pipeline-name movie_mart_dbt
        echo "[$(date -Iseconds)] dbt build marts end"
        """,
    )

    dbt_build_analytics = BashOperator(
        task_id="dbt_build_analytics",
        bash_command="""
        set -euo pipefail
        echo "[$(date -Iseconds)] dbt build analytics start"
        docker exec mm-dbt dbt build --select path:models/analytics --fail-fast
                python /opt/airflow/scripts/log_dbt_metadata.py \
                    --run-results /opt/airflow/dbt/target/run_results.json \
                    --manifest /opt/airflow/dbt/target/manifest.json \
                    --stage-name analytics \
                    --pipeline-name movie_mart_dbt
        echo "[$(date -Iseconds)] dbt build analytics end"
        """,
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> ingest_raw
        >> stage_data
        >> dbt_build_staging
        >> dbt_build_intermediate
        >> dbt_build_marts
        >> dbt_build_analytics
        >> end
    )
