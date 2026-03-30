# Movie Analytics Capstone Pipeline

## Project Summary

This repository implements an end-to-end movie analytics data pipeline using Docker, PostgreSQL, Python scripts, Airflow orchestration, and dbt transformations.

The pipeline ingests source datasets, enriches missing fields, applies layered transformations, and publishes analytics views for BI reporting.

## Project Details

- Author: Hazel Mae C. Bea
- Repository: capstone-pipeline
- Primary database: movie_db
- Project directory (local)

## Table of Contents

1. [Architecture and Data Flow](#architecture-and-data-flow)
2. [Source Datasets](#source-datasets)
3. [Project Directory Tree](#project-directory-tree)
4. [Repository Structure](#repository-structure)
5. [Runtime Services and Ports](#runtime-services-and-ports)
6. [Database Bootstrap and Access Model](#database-bootstrap-and-access-model)
7. [Ingestion and Staging Modules](#ingestion-and-staging-modules)
8. [dbt Layers and Models](#dbt-layers-and-models)
9. [Transformation Logic Highlights](#transformation-logic-highlights)
10. [Setup and Run Commands](#setup-and-run-commands)
11. [Documentation Index](#documentation-index)

## Project Directory Tree

```text
capstone-pipeline/
├── .env.example
├── docker-compose.yml
├── Makefile
├── README.md
├── QUICK_REFERENCE.md
├── airflow/
│   ├── dags/
│   │   └── movie_mart_pipeline_dag.py
│   ├── datasets/
│   │   ├── movies_main.csv
│   │   ├── movie_extended.csv
│   │   └── ratings.json
│   ├── logs/
│   ├── Dockerfile
│   └── requirements.txt
├── dbt/
│   ├── analyses/
│   ├── macros/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   ├── marts/
│   │   └── analytics/
│   ├── tests/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml
│   └── package-lock.yml
├── postgres/
│   └── init/
│       ├── 01_roles.sh
│       ├── 02_schemas.sql
│       ├── 03_raw_tables.sql
│       ├── 04_metadata_tables.sql
│       └── 05_permissions.sql
├── scripts/
│   ├── ingest_data.py
│   ├── stage_data.py
│   ├── log_dbt_metadata.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── utils/
│       ├── db_connector.py
│       ├── db_operations.py
│       ├── pipeline_utils.py
│       └── logger.py
└── documentation/
    ├── codebase_guide.md
    ├── ingestion_staging_detailed.md
    ├── data_handling.md
    └── data_audit_summary.md
```

## Architecture and Data Flow

### Data Layers

- raw: immutable landing tables from source files
- staging: cleaned and typed source-level outputs
- intermediate: reusable transformation and metric logic
- marts: conformed dimensions and fact tables
- analytics: BI-ready reporting views
- metadata: pipeline run and task logging tables

### Orchestration Sequence

Main DAG file: `airflow/dags/movie_mart_pipeline_dag.py`  
DAG ID: `movie_mart_full_pipeline`

Execution order:

1. `scripts/ingest_data.py`
2. `scripts/stage_data.py`
3. `dbt build --select path:models/staging --fail-fast`
4. `dbt build --select path:models/intermediate --fail-fast`
5. `dbt build --select path:models/marts --fail-fast`
6. `dbt build --select path:models/analytics --fail-fast`

## Source Datasets

Location: `airflow/datasets`

- `movies_main.csv`: base movie attributes including dates and financial columns
- `movie_extended.csv`: genres, countries, production companies, and language attributes
- `ratings.json`: ratings and voting-related attributes

## Repository Structure

Top-level folders:

- `airflow`: DAGs, dataset mount, and scheduler/webserver runtime assets
- `dbt`: dbt project with models, tests, macros, analyses, and profiles
- `postgres`: initialization scripts for roles, schemas, tables, and grants
- `scripts`: ingestion and staging scripts plus shared utilities
- `documentation`: supporting technical documentation

## Runtime Services and Ports

| Service           | Container Name         | Purpose                      | Port Mapping |
| :---------------- | :--------------------- | :--------------------------- | :----------- |
| postgres          | `mm-postgres`          | database engine              | `5430:5432`  |
| airflow-webserver | `mm-airflow-webserver` | Airflow UI/API               | `8082:8080`  |
| airflow-scheduler | `mm-airflow-scheduler` | DAG scheduling and execution | none         |
| python-app        | `mm-python-scripts`    | ingestion/staging runtime    | none         |
| dbt               | `mm-dbt`               | dbt runtime                  | none         |

Supporting runtime assets:

- Docker network name: `mm-pipeline-network`
- Postgres volume name: `mm-postgres-data`

## Database Bootstrap and Access Model

### Initialization Scripts

Path: `postgres/init`

- `01_roles.sh`: role creation
- `02_schemas.sql`: schema creation
- `03_raw_tables.sql`: raw table DDL
- `04_metadata_tables.sql`: metadata table DDL
- `05_permissions.sql`: grants and role-based access

### Schema Responsibilities

- `raw`: source landing zone from files
- `staging`: cleaned/typed source models
- `intermediate`: business and enrichment transformations
- `marts`: dimensions and facts for analysis
- `analytics`: final reporting views
- `metadata`: ingestion and orchestration logs

### Verify Database Initialization

```bash
docker exec -it mm-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dn"
docker exec -it mm-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt raw.*"
docker exec -it mm-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt metadata.*"
docker exec -it mm-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\du"
```

### Environment Role Mapping

Defined in `.env` and templated in `.env.example`:

| Variable               | Default Role Name  | Responsibility                  |
| :--------------------- | :----------------- | :------------------------------ |
| `POSTGRES_USER`        | user-defined admin | superadmin/bootstrap            |
| `ETL_DB_USER`          | `etl`              | ingestion and staging writes    |
| `DBT_DB_USER`          | `dbt`              | transformation and model builds |
| `ORCHESTRATOR_DB_USER` | `airflow`          | orchestration logging           |
| `ANALYTICS_DB_USER`    | `analytics_reader` | BI read-only access             |

### Permission Model Summary

Grants are applied via `postgres/init/05_permissions.sql`.

| Role               | Key Access Summary                                                          |
| :----------------- | :-------------------------------------------------------------------------- |
| `etl`              | raw and staging read/write; metadata logging                                |
| `dbt`              | read source tables; create/use staging/intermediate/marts/analytics schemas |
| `airflow`          | metadata read/write for orchestration logs                                  |
| `analytics_reader` | read-only access to marts and analytics outputs                             |

## Ingestion and Staging Modules

### Shared Utilities (`scripts/utils`)

- `db_connector.py`
    - `get_connection(...)`: context-managed database connection
- `db_operations.py`
    - `execute_query(...)`: generic SQL execution
    - `bulk_insert(...)`: efficient row inserts via psycopg2 extras
    - `truncate_table(...)`: table truncation helper
    - `get_table_count(...)`: row-count helper
- `pipeline_utils.py`
    - `start_pipeline_run(...)`, `complete_pipeline_run(...)`
    - `start_task_log(...)`, `complete_task_log(...)`
    - `nullify(...)`, `prepare_rows(...)`, `normalize_mixed_dates(...)`
- `logger.py`
    - `setup_logger(...)`: standard structured logger initialization

### Raw Ingestion (`scripts/ingest_data.py`)

Main class: `MovieDataIngestion`

Core methods:

- `load_csv_to_postgres(...)`
- `load_json_to_postgres(...)`
- `ingest_movies_main(...)`
- `ingest_movie_extended(...)`
- `ingest_ratings(...)`
- `ingest_all(...)`

Operational behavior:

- rerun-safe ingestion with optional truncate control
- raw-layer writes to `raw.movies_main`, `raw.movie_extended`, `raw.ratings`
- metadata logging at pipeline and task level

### Staging Enrichment (`scripts/stage_data.py`)

Main classes: `KaggleMovieEnricher`, `MovieDataStaging`

Core methods:

- `KaggleMovieEnricher.load(...)`
- `stage_movies_main(...)`
- `stage_movie_extended(...)`
- `stage_ratings(...)`
- `stage_all(...)`

Operational behavior:

- fills selected missing fields using Kaggle TMDB overlap
- duplicate diagnostics and safe collapse strategy per key
- writes to `staging.movies_main`, `staging.movie_extended`, `staging.ratings`

### dbt Metadata Logging Helper

Script: `scripts/log_dbt_metadata.py`

This script is invoked by DAG dbt stages to capture dbt run metadata from `target/run_results.json` and `target/manifest.json` into metadata tables.

## dbt Layers and Models

### Staging Models (`dbt/models/staging`)

- `stg_movies_main.sql`
- `stg_movie_extended.sql`
- `stg_ratings.sql`

Typical processing:

- type casting and field cleanup
- date normalization
- deterministic deduplication

### Intermediate Models (`dbt/models/intermediate`)

- `int_movie_company_bridge.sql`
- `int_company_year_metrics.sql`

### Marts Models (`dbt/models/marts`)

- `dim_date.sql`
- `dim_movies.sql`
- `dim_production_companies.sql`
- `fct_movie_ratings.sql`
- `fct_company_year_performance.sql`

### Analytics Models (`dbt/models/analytics`)

- `vw_company_year_trends.sql`
- `vw_high_value_studio_performance.sql`

## Transformation Logic Highlights

### Budget and Revenue Correction

Model: `dbt/models/intermediate/int_movie_company_bridge.sql`

Rules:

1. If budget and revenue are both in 1-999, scale both by 1,000,000.
2. If budget is 1-999 and revenue is above 1,000,000, scale budget by 1,000,000.
3. If budget is 1,000-999,999 and revenue has an extreme ratio, scale budget by 1,000.

Audit trace column: `budget_correction_flag`

### Studio Recommendation Logic

Model: `dbt/models/analytics/vw_high_value_studio_performance.sql`

Implemented behavior:

- weighted decision score using latest band, dominant band, years observed, and recency
- inactivity override for studios with `latest_year <= 2013`
- recommendation labels: `Inactive`, `Incentivise`, `Review`, `Renegotiate`

Current thresholds:

- `Incentivise`: `consistency_ratio > 0.15` and `weighted_decision_score >= 0.55`
- `Review`: `consistency_ratio > 0.15` and `weighted_decision_score >= 0.30`
- `Renegotiate`: active studios that do not meet the thresholds above

## Setup and Run Commands

### Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- GNU Make
- A configured `.env` file copied from `.env.example`

### Quick Start

1. Copy `.env.example` to `.env` and set all required values.
2. Run environment validation.
3. Start all services.
4. Run end-to-end pipeline.

```bash
make check-env
make up
make pipeline-run
```

### Access Points

- Airflow UI: `http://localhost:8082`
- PostgreSQL host port: `localhost:5430`

### Useful Make Targets

| Task                              | Command                                             |
| :-------------------------------- | :-------------------------------------------------- |
| Start services                    | `make up`                                           |
| Stop services                     | `make down`                                         |
| Show service status and DB checks | `make status`                                       |
| Ingest raw files                  | `make ingest`                                       |
| Run staging script                | `make stage`                                        |
| Install dbt packages              | `make dbt-deps`                                     |
| Run all dbt models                | `make dbt-run`                                      |
| Run dbt tests                     | `make dbt-test`                                     |
| Validate dbt connection           | `make dbt-debug`                                    |
| Execute manual audit SQL          | `make audit`                                        |
| Trigger Airflow DAG               | `make airflow-trigger DAG=movie_mart_full_pipeline` |

## Documentation Index

- `documentation/codebase_guide.md`
- `documentation/ingestion_staging_detailed.md`
- `documentation/data_handling.md`
- `documentation/data_audit_summary.md`
- `QUICK_REFERENCE.md`

## Notes on dbt Analyses

Files under `dbt/analyses` are manual audit and validation SQL assets. They are not automatically executed by the scheduled Airflow DAG unless explicitly invoked.
