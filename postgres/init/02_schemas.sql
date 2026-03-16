-- ==========================================================
-- DATABASE SCHEMA AND PERMISSIONS INITIALIZATION
-- ==========================================================
    -- This SQL Script initializes the database structure and
    -- role-based permissions for the Movie Mart data platform 

-- -----------------------------------------------------------
--  SCHEMA CREATION
-- -----------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS raw;                    -- source data landing zone, exact copy from csv/json files
CREATE SCHEMA IF NOT EXISTS staging;                -- cleaned, typed, validated data (dbt views)
CREATE SCHEMA IF NOT EXISTS intermediate;           -- business logic transformationss (dbt views)
CREATE SCHEMA IF NOT EXISTS marts;                  -- star schema dimension/fact tables (dbt tables)
CREATE SCHEMA IF NOT EXISTS analytics;              -- final views for Power BI (dbt views)
CREATE SCHEMA IF NOT EXISTS metadata;               -- metadata table for logs, audit trails, and monitoring

-- -----------------------------------------------------------
--  RAW SCHEMA PERMISSIONS
-- -----------------------------------------------------------

-- schema-level access; table-level grants are applied in 03_raw_tables.sql
GRANT USAGE ON SCHEMA raw TO etl;

-- dbt user can read raw data
GRANT USAGE ON SCHEMA raw to dbt;

-- -----------------------------------------------------------
--  STAGING SCHEMA PERMISSIONS
-- -----------------------------------------------------------
GRANT USAGE, CREATE ON SCHEMA staging TO dbt;

-- -----------------------------------------------------------
--  INTERMEDIATE SCHEMA PERMISSIONS
-- -----------------------------------------------------------
GRANT USAGE, CREATE ON SCHEMA intermediate TO dbt;

-- -----------------------------------------------------------
--  MARTS SCHEMA PERMISSIONS
-- -----------------------------------------------------------
GRANT USAGE, CREATE ON SCHEMA marts TO dbt;

-- -----------------------------------------------------------
--  ANALYTICS SCHEMA PERMISSIONS
-- -----------------------------------------------------------
GRANT USAGE, CREATE ON SCHEMA analytics TO dbt;

-- -----------------------------------------------------------
--  ANALYTICS CONSUMER PERMISSIONS
-- -----------------------------------------------------------
GRANT USAGE ON SCHEMA marts TO analytics_reader;
GRANT USAGE ON SCHEMA analytics TO analytics_reader;

GRANT SELECT ON ALL TABLES IN SCHEMA marts TO analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analytics_reader;

-- -----------------------------------------------------------
--  METADATA SCHEMA PERMISSIONS
-- -----------------------------------------------------------
GRANT USAGE ON SCHEMA metadata TO etl;
GRANT INSERT ON ALL TABLES IN SCHEMA metadata TO etl;

GRANT USAGE ON SCHEMA metadata TO dbt;
GRANT SELECT ON ALL TABLES IN SCHEMA metadata TO dbt;

GRANT USAGE ON SCHEMA metadata TO airflow;

-- -----------------------------------------------------------
--  DEFAULT PRIVILEGES FOR FUTURE TABLES
-- -----------------------------------------------------------

-- raw tables are created by the init user (admin)
ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA raw
GRANT SELECT, INSERT, TRUNCATE ON TABLES TO etl;

ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA raw
GRANT SELECT ON TABLES TO dbt;

-- dbt-created transformation tables
ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA staging
GRANT SELECT ON TABLES TO dbt;

ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA intermediate
GRANT SELECT ON TABLES TO dbt;

ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA marts
GRANT SELECT ON TABLES TO dbt;


ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA analytics
GRANT SELECT ON TABLES TO dbt;

-- consumable tables access to analytics_reader
ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA marts
GRANT SELECT ON TABLES TO analytics_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA analytics
GRANT SELECT ON TABLES TO analytics_reader;
