-- ==========================================================
-- DATABASE PERMISSIONS INITIALIZATION
-- ==========================================================

-- -----------------------------------------------------------
--  DATABASE-LEVEL PRIVILEGES
-- -----------------------------------------------------------

GRANT CREATE, TEMP ON DATABASE movie_db TO dbt;

-- -----------------------------------------------------------
--  SCHEMA-LEVEL PRIVILEGES
-- -----------------------------------------------------------

GRANT USAGE ON SCHEMA raw TO etl;
GRANT USAGE ON SCHEMA raw TO dbt;

GRANT USAGE, CREATE ON SCHEMA staging TO etl;
GRANT USAGE, CREATE ON SCHEMA staging TO dbt;
GRANT USAGE, CREATE ON SCHEMA intermediate TO dbt;
GRANT USAGE, CREATE ON SCHEMA marts TO dbt;
GRANT USAGE, CREATE ON SCHEMA analytics TO dbt;

GRANT USAGE ON SCHEMA marts TO analytics_reader;
GRANT USAGE ON SCHEMA analytics TO analytics_reader;

GRANT USAGE ON SCHEMA metadata TO etl;
GRANT USAGE ON SCHEMA metadata TO dbt;
GRANT USAGE ON SCHEMA metadata TO airflow;

-- -----------------------------------------------------------
--  EXISTING OBJECT PRIVILEGES
-- -----------------------------------------------------------

GRANT SELECT, INSERT, TRUNCATE ON ALL TABLES IN SCHEMA raw TO etl;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO dbt;

GRANT SELECT, INSERT, TRUNCATE ON ALL TABLES IN SCHEMA staging TO etl;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO dbt;

GRANT SELECT ON ALL TABLES IN SCHEMA marts TO analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analytics_reader;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA metadata TO etl;
GRANT SELECT ON ALL TABLES IN SCHEMA metadata TO dbt;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA metadata TO airflow;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA metadata TO etl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA metadata TO airflow;

-- -----------------------------------------------------------
--  DEFAULT PRIVILEGES FOR FUTURE OBJECTS
-- -----------------------------------------------------------

-- raw tables are created by the init user (admin)
ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA raw
GRANT SELECT, INSERT, TRUNCATE ON TABLES TO etl;

ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA raw
GRANT SELECT ON TABLES TO dbt;

ALTER DEFAULT PRIVILEGES FOR ROLE etl IN SCHEMA staging
GRANT SELECT, INSERT, TRUNCATE ON TABLES TO etl;

ALTER DEFAULT PRIVILEGES FOR ROLE etl IN SCHEMA staging
GRANT SELECT ON TABLES TO dbt;

ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA metadata
GRANT SELECT, INSERT, UPDATE ON TABLES TO etl;

ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA metadata
GRANT SELECT ON TABLES TO dbt;

ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA metadata
GRANT SELECT, INSERT, UPDATE ON TABLES TO airflow;

ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA metadata
GRANT USAGE, SELECT ON SEQUENCES TO etl;

ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA metadata
GRANT USAGE, SELECT ON SEQUENCES TO airflow;

-- dbt-created transformation tables
ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA staging
GRANT SELECT ON TABLES TO dbt;

ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA intermediate
GRANT SELECT ON TABLES TO dbt;

ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA marts
GRANT SELECT ON TABLES TO dbt;

ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA analytics
GRANT SELECT ON TABLES TO dbt;

ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA marts
GRANT SELECT ON TABLES TO analytics_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA analytics
GRANT SELECT ON TABLES TO analytics_reader;