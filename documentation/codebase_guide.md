# Codebase Guide

## Purpose

This guide explains the current responsibilities of key folders and models in the pipeline.

## End-To-End Pipeline

1. Source files are ingested into raw tables.
2. Staging scripts enrich and clean data using Kaggle overlap.
3. dbt builds are executed schema-by-schema: staging, intermediate, marts, analytics.
4. Analytics views are consumed by Power BI.

## PostgreSQL Initialization

Folder: postgres/init

- 01_roles.sh: creates service roles
- 02_schemas.sql: creates raw, staging, intermediate, marts, analytics, metadata schemas
- 03_raw_tables.sql: creates raw landing tables
- 04_metadata_tables.sql: creates run and task logging tables
- 05_permissions.sql: grants role-based schema/table access

## Orchestration

File: airflow/dags/movie_mart_pipeline_dag.py

Scheduled DAG runs:

- scripts/ingest_data.py
- scripts/stage_data.py
- dbt build --select path:models/staging
- dbt build --select path:models/intermediate
- dbt build --select path:models/marts
- dbt build --select path:models/analytics

## Python Scripts

Folder: scripts

- ingest_data.py: loads source files into raw schema
- stage_data.py: enriches and writes staging tables

Shared utilities in scripts/utils:

- db_connector.py: connection lifecycle
- db_operations.py: execute, truncate, bulk insert helpers
- pipeline_utils.py: metadata logging and dataframe normalization helpers
- logger.py: standard logging setup

## dbt Layers

### Staging Models

Folder: dbt/models/staging

- stg_movies_main.sql: type casting, date parsing, deduplication
- stg_movie_extended.sql: text cleaning and deduplication
- stg_ratings.sql: ratings type casting and deduplication

### Intermediate Models

Folder: dbt/models/intermediate

- int_movie_company_bridge.sql:
    - explodes movie-to-company relationships
    - applies budget and revenue scaling correction logic
    - standardizes selected company names
- int_company_year_metrics.sql:
    - aggregates company-year metrics
    - computes prior-year references and YoY deltas

### Marts Models

Folder: dbt/models/marts

- dim_date.sql: calendar dimension
- dim_movies.sql: movie dimension
- dim_production_companies.sql: conformed company dimension with first/latest release year
- fct_movie_ratings.sql: movie-level ratings fact-like table
- fct_company_year_performance.sql: company-year performance fact table

### Analytics Views

Folder: dbt/models/analytics

- vw_company_year_trends.sql:
    - yearly trend labels per company-year
- vw_high_value_studio_performance.sql:
    - studio-level consistency, trend labels, weighted decision score
    - inactive override for studios with latest year <= 2013
    - recommendation output: Inactive, Incentivise, Review, Renegotiate

## dbt Analyses

Folder: dbt/analyses

Files in this folder are manual audit SQL assets. They are not automatically run by the Airflow DAG unless explicitly invoked.

## Documentation Map

- documentation/ingestion_staging_detailed.md
- documentation/data_handling.md
- documentation/data_audit_summary.md
