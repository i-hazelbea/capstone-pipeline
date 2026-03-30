# Ingestion And Staging Detailed Guide

## Scope
This document covers operational details for the ingestion and staging segments of the pipeline.

## Input Datasets
Expected files in airflow/datasets:
- movies_main.csv
- movie_extended.csv
- ratings.json

## Raw Ingestion

Script: scripts/ingest_data.py

Writes to:
- raw.movies_main
- raw.movie_extended
- raw.ratings

Behavior:
- loads source files into raw schema
- preserves source shape as much as possible
- supports rerun-safe loading workflow
- writes pipeline and task metadata logs

## Staging Enrichment

Script: scripts/stage_data.py

Writes to:
- staging.movies_main
- staging.movie_extended
- staging.ratings

Behavior:
- fills selected missing values using Kaggle overlap by movie id
- applies quality filters for rating fallback
- deduplicates before write
- logs rows touched and enrichment counts

## Shared Utility Modules
Folder: scripts/utils

- db_connector.py: database connection lifecycle
- db_operations.py: execute, truncate, bulk insert, count helpers
- pipeline_utils.py: metadata logging and dataframe utilities
- logger.py: shared logger configuration

## dbt Staging Models
Folder: dbt/models/staging

- stg_movies_main.sql
- stg_movie_extended.sql
- stg_ratings.sql

Role of dbt staging models:
- apply type-safe casts
- normalize and parse date values
- clean text fields
- enforce deterministic deduplication logic

## Run Commands

Basic lifecycle:
1. make check-env
2. make up
3. make ingest
4. make stage
5. make dbt-run
6. make dbt-test

One-command pipeline path:
- make pipeline-run

Validation helpers:
- make ingest-check
- make audit

## Notes
- dbt analyses SQL files are manual audit assets and are not scheduled by Airflow unless explicitly invoked.
- For full orchestration, use the Airflow DAG movie_mart_full_pipeline.