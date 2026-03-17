-- ==========================================================
-- DATABASE SCHEMA INITIALIZATION
-- ==========================================================
    -- This SQL script initializes the database structure for
    -- the Movie Mart data platform.

-- -----------------------------------------------------------
--  SCHEMA CREATION
-- -----------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS raw;                    -- source data landing zone, exact copy from csv/json files
CREATE SCHEMA IF NOT EXISTS staging;                -- cleaned, typed, validated data (dbt views)
CREATE SCHEMA IF NOT EXISTS intermediate;           -- business logic transformationss (dbt views)
CREATE SCHEMA IF NOT EXISTS marts;                  -- star schema dimension/fact tables (dbt tables)
CREATE SCHEMA IF NOT EXISTS analytics;              -- final views for Power BI (dbt views)
CREATE SCHEMA IF NOT EXISTS metadata;               -- metadata table for logs, audit trails, and monitoring
