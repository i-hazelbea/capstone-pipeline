# Movie Analytics Capstone Project - Implementation Documentation

## Project Details

> - **Author**: Hazel Mae C. Bea
> - **Last Update**: March 11, 2026 Tues
> - **Project Repository**: `capstone-pipeline`
> - **Database**: `movie-db`

## Table of Contents

1. [Raw Data Source](#1-raw-data-source)
    - [Data Observations](#data-observations)
2. [Project Set up](#2-project-set-up)
    - [Project Directory Structure](#project-directory-structure)
    - [Docker Services](#docker-services)
    - [PostgreSQL Config](#postgresql-config)
        - [DB Schema](#db-schema)
        - [DB & Airflow Connection Details](#db--airflow-connection-details)
        - [DB User Config](#db-user-config-defined-in-env)
        - [DB User Permissions](#db-user-permissions)
3. [Ingestion](#3-ingestion)
4. [Cleaning and Transformation](#4-cleaning-and-transformation)

---

## 1. Raw Data Source

> `./airflow/datasets`

- `movies_main.csv` - main movie information including titles, release dates, and financial data
- `movie_extended.csv` - extended movie metadata including genres, production details, and languages
- `ratings.json` - movie ratings aggregated data, processed by PySpark, documented here for reference

### Data Observations

## 2. Project Set up

### Project Directory Structure

```bash
capstone-pipeline/
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
├── Makefile
├── README.md
|
├── airflow /
│   ├── Dockerfile
│   ├── requirements
│   ├── dags/
│   │   ├── movie_etl_pipeline.py
│   │   └── monitoring_dag.py
│   ├── datasets/
│   │   ├── movies_main.csv
│   │   ├── movie_extended.csv
│   │   └── ratings.json
│   └── logs/
|
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   ├── stg_movies_main.sql
│   │   │   ├── stg_movie_extended.sql
│   │   │   └── stg_ratings.sql
│   │   ├── intermediate/
│   │   │   ├── schema.yml
│   │   │   ├── int_movies_enriched.sql
│   │   │   └── int_movies_with_metrics.sql
│   │   └── marts/
│   │   │   ├── schema.yml
│   │   │   ├── dim_movies.sql
│   │   │   ├── dim_genres.sql
│   │   │   ├── dim_production_companies.sql
│   │   │   ├── dim_dates.sql
│   │   │   ├── fact_movie_performance.sql
│   │   └── analytics/
│   │   │   └── vm_movie_performance_summary.sql
│   │   │
│   ├── tests/
|   |   └── generic/
|   └── macros/
├── postgres/
|   └── init
│       ├── 01_roles.sh
│       ├── 02_schema.sql
│       ├── 03_raw_tables.sql
│       ├── 04_metadata_tables.sql
│       └── 05_permissions.sql
|
└── scripts/
    ├── Dockerfile
    ├── requirements.txt
    ├── ingest_data.py
    ├── stage_data.py
    ├── validate_data.py
    └── utils/
        ├── __init__.py
        ├── db_connector.py
        ├── db_operations.py
        ├── pipeline_utils.py
        └── logger.py

```

### Docker Services:

- **container network**: `mm-network`
- **global network**:`mm-pipeline-network`

| Service Name          | Container              | Image                                    | Port        |
| :-------------------- | :--------------------- | :--------------------------------------- | :---------- |
| **postgres**          | `mm-postgres`          | postgres:15-alpine                       | `5430:5432` |
| **airflow-webserver** | `mm-airflow-webserver` | apache/airflow:2.8.1                     | `8082:8080` |
| **airflow-scheduler** | `mm-airflow-scheduler` | apache/airflow:2.8.1                     | `None`      |
| **python-app**        | `mm-python-scripts`    | python:3.11-slim                         | `None`      |
| **dbt**               | `mm-dbt`               | ghcr.io/dbt-labs/dbt-postgres:1.8.latest | `None`      |

### PostgreSQL Config

#### DB Schema

```sql
raw               -- source data landing zone, exact copy from csv/json files
staging           -- cleaned, typed, validated data (dbt views)
intermediate      -- business logic transformationss (dbt views)
marts             -- star schema dimension/fact tables (dbt tables)
analytics         -- final views for Power BI (dbt views)
metadata          -- -- metadata table for logs, audit trails, and monitoring
```

#### DB & Airflow Connection Details

| PostgreSQL Config |          | Airflow Config         |                               |
| :---------------- | :------- | :--------------------- | :---------------------------- |
| POSTGRES_USER     | admin    | AIRFLOW_ADMIN_USER     | admin                         |
| POSTGRES_PASSWORD | `*****`  | AIRFLOW_ADMIN_PASSWORD | `*****`                       |
| POSTGRES_DB       | movie_db | AIRFLOW_ADMIN_EMAIL    | i_hazelmae.bea@stratpoint.com |
| POSTGRES_PORT     | `5430`   | AIRFLOW_WEBSERVER_PORT | `8082`                        |

#### DB User Config (Defined in .env)

| .env variable        | user    | role           |
| :------------------- | :------ | :------------- |
| POSTGRES_USER        | admin   | superadmin     |
| ETL_DB_USER          | etl     | ingestion      |
| DBT_DB_USER          | dbt     | transformation |
| ORCHESTRATOR_DB_USER | airflow | orchestration  |

#### DB User Permissions

| User             | Layer               | Permissions              | Role                            |
| :--------------- | :------------------ | :----------------------- | :------------------------------ |
| etl              | raw schema          | USAGE                    | access raw namespace            |
| etl              | raw tables          | SELECT, INSERT, TRUNCATE | load/replace source data        |
| etl              | metadata schema     | USAGE                    | access pipeline logs            |
| etl              | metadata tables     | SELECT, INSERT, UPDATE   | record ingestion runs           |
| dbt              | raw schema          | USAGE                    | access source schema            |
| dbt              | raw tables          | SELECT                   | read source data                |
| dbt              | staging schema      | USAGE, CREATE            | build staging models            |
| dbt              | intermediate schema | USAGE, CREATE            | build transformation models     |
| dbt              | marts schema        | USAGE, CREATE            | build business tables           |
| dbt              | analytics schema    | USAGE, CREATE            | build analytics datasets        |
| dbt              | metadata schema     | USAGE                    | access pipeline metadata        |
| dbt              | metadata tables     | SELECT                   | read ingestion logs             |
| airflow          | metadata schema     | USAGE                    | orchestrate pipelines           |
| airflow          | metadata tables     | SELECT, INSERT, UPDATE   | record pipeline logs            |
| analytics_reader | marts schema        | USAGE                    | read tables in marts schema     |
| analytics_reader | analytics schema    | USAGE                    | read tables in analytics schema |

### Verify Database Initialization

After starting the PostgreSQL container, verify all objecs were created:

```bash
# Check all schemas
docker exec -it mm-postgres psql -U admin -d movie_db -c "\dn"

# Check raw tables
docker exec -it mm-postgres psql -U admin -d movie_db -c "\dt raw.*"

# Check metadata tables
docker exec -it mm-postgres psql -U admin -d movie_db -c "\dt metadata.*"

# Check roles -- expected roles: etl, dbt, airlow, analyics_reader
docker exec -it mm-postgres psql -U admin -d movie_db -c "\du"
```

## 3. Ingestion

### `logger.py` | Key Functions

```bash
scripts/utils/logger.py
```

- Logging configuration for ingestion scripts

| Function Name       | Description                                     |
| :------------------ | :---------------------------------------------- |
| `setup_logger(...)` | Configure logger with timestamp and formatting. |

### `db_connector.py` | Key Functions

```bash
scripts/utils/db_connector.py
```

- PostgreSQL connection lifecycle utility

| Function Name         | Description                               |
| :-------------------- | :---------------------------------------- |
| `get_connection(...)` | Context manager for database connections. |

### `db_operations.py` | Key Functions

```bash
scripts/utils/db_operations.py
```

- Shared SQL and table operations built on top of `DatabaseConnector`

| Function Name                 | Description                                            |
| :---------------------------- | :----------------------------------------------------- |
| `execute_query(...)`          | Execute a SQL query.                                   |
| `bulk_insert(...)`            | Bulk insert data using execute_values for performance. |
| `truncate_table(...)`         | Truncate a table (remove all rows).                    |
| `get_table_count(...) -> int` | Get row count for a table.                             |

### `pipeline_utils.py` | Key Functions

```bash
scripts/utils/pipeline_utils.py
```

- Shared metadata logging and dataframe helpers used by both ingestion scripts

| Function Name                | Description                                             |
| :--------------------------- | :------------------------------------------------------ |
| `start_pipeline_run(...)`    | Start metadata row for pipeline execution.              |
| `complete_pipeline_run(...)` | Complete metadata row for pipeline execution.           |
| `start_task_log(...)`        | Start metadata row for task execution.                  |
| `complete_task_log(...)`     | Complete metadata row for task execution.               |
| `nullify(...)`               | Convert NA/blank values to `None` before write.         |
| `prepare_rows(...)`          | Convert DataFrame to `(columns, rows)` for bulk insert. |
| `normalize_mixed_dates(...)` | Parse mixed date formats to `YYYY-MM-DD`.               |

### `ingest_data.py` | Key

```bash
scripts/ingest_data.py
```

- Main data ingestion script for movie analytics pipeline
- This script loads raw csv and json data into PostgreSQL raw schema.
- Designed to be idempotent and can be run multiple times safely

| Function Name                       | Description                                                     |
| :---------------------------------- | :-------------------------------------------------------------- |
| `load_csv_to_postgres(...) -> int`  | Load CSV file into PostgreSQL table.                            |
| `load_json_to_postgres(...) -> int` | Load JSON file (line-delimited or array) into PostgreSQL table. |
| `ingest_movies_main(...) -> int`    | Ingest movies_main.csv into raw.movies_main table.              |
| `ingest_movie_extended(...) -> int` | Ingest movie_extended.csv into raw.movie_extended table.        |
| `ingest_ratings(...) -> int`        | Ingest ratings.json into raw.ratings table.                     |
| `ingest_all(...) -> Dict[str, int]` | Ingest all datasets in sequence.                                |

### `stage_data.py` | Key

```bash
scripts/stage_data.py
```

- Staging enrichment script that reads `raw.*`, enriches from Kaggle TMDB, and writes `staging.*`

| Function Name                                   | Description                                                                   |
| :---------------------------------------------- | :---------------------------------------------------------------------------- |
| `KaggleMovieEnricher.load(...)`                 | Loads/caches Kaggle data and prepares helper columns.                         |
| `stage_movies_main(...) -> tuple[int, dict]`    | Fills missing core movie attributes and writes `staging.movies_main`.         |
| `stage_movie_extended(...) -> tuple[int, dict]` | Fills missing extended attributes and writes `staging.movie_extended`.        |
| `stage_ratings(...) -> tuple[int, dict]`        | Fallback-fills ratings with vote quality filter and writes `staging.ratings`. |
| `stage_all(...) -> Dict[str, int]`              | Runs complete staging flow with metadata task logs.                           |

## 4. Cleaning and Transformation

### Staging Layer Models

| stg_model                | materialization | tags             | transformations                                           |
| :----------------------- | :-------------- | :--------------- | :-------------------------------------------------------- |
| `stg_movies_main.sql`    | view            | staging, movies  | 1. Type Casting: budget, revenue from TEXT to BIGINT      |
|                          |                 |                  | 2. Date Parsing: release_date from TEXT to DATE           |
|                          |                 |                  | 3. Data Validation: filter invalid dates, negative values |
|                          |                 |                  | 4. Null Handling: replace empty strings with NULL         |
|                          |                 |                  | 5. Add surrogate key using dbt_utils                      |
| `stg_movie_extended.sql` | view            | staging, movies  |                                                           |
| `stg_ratings.sql`        | view            | staging, ratings |                                                           |

## 5.
