# Data Handling

## Objective
This document summarizes current handling rules for common data issues across ingestion, staging, and dbt transformation layers.

## Source Data Challenges
- missing or blank text fields
- mixed date formats
- numeric values stored as text
- duplicate rows across reruns
- sparse or low-quality rating coverage
- inconsistent production company strings

## Current Handling Strategy By Layer

### Raw Layer
- preserves source payloads for traceability
- loads into raw.movies_main, raw.movie_extended, raw.ratings

### Python Staging Layer
- enrichment from Kaggle overlap by movie id when available
- deduplication before write
- null normalization helpers
- metadata run logging

### dbt Staging Layer
- type casting and string cleanup
- date parsing and normalization
- deterministic deduplication per key using recency ordering

## Budget And Revenue Correction Rules
Implemented in dbt/models/intermediate/int_movie_company_bridge.sql.

Rules:
1. If budget and revenue are both 1-999, scale both by 1,000,000.
2. If budget is 1-999 and revenue is above 1,000,000, scale budget by 1,000,000.
3. If budget is 1,000-999,999 and revenue is very high with extreme ratio, scale budget by 1,000.

Audit trace:
- budget_correction_flag preserves correction type per row.

## Deduplication Controls
- Python staging deduplicates by movie id.
- dbt models apply window-based row selection using latest loaded_at where appropriate.

## Null And Optional Field Policy
- core identifiers are required and validated with dbt tests.
- optional descriptive fields remain nullable if no trusted value is available.

## Replayability
- Full reruns are supported.
- pipeline-run path is designed to be repeatable without multiplying final model rows.

## Operational Checks
- make check-env validates required runtime configuration.
- make ingest-check verifies raw row counts.
- make audit executes manual audit SQL from dbt/analyses.

## Known Constraints
- analyses SQL is not scheduled by DAG by default.
- outlier YoY percentages are mathematically valid when prior-year revenue is very small.
