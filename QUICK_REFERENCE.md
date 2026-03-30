# 🚀 Movie Analytics Pipeline - Quick Reference Cheat Sheet

**Essential Commands for Daily Operations**

---

## 🎯 Getting Started (First Time Setup)

```bash
# 1. Create environment file
cp .env.example .env
# Edit .env with your credentials

# 2. Start all services
docker-compose up -d

# 3. Wait for services to be healthy (~60 seconds)
watch docker ps  # Press Ctrl+C when all healthy

# 4. Verify PostgreSQL schemas
docker exec -it mm-postgres psql -U airflow -d movie_db -c "\dn"

# 5. Run initial data ingestion
docker exec -it mm-python-scripts python /scripts/ingest_data.py

# 6. Install dbt packages
docker exec -it mm-dbt dbt deps

# 7. Run dbt transformations
docker exec -it mm-dbt dbt run

# 8. Run dbt tests
docker exec -it mm-dbt dbt test

# 9. Access Airflow UI
# Open browser: http://localhost:8082
# Username: admin, Password: admin_pass_12345
```

---

## 🐳 Docker Commands

### Service Management

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# Restart specific service
docker-compose restart [service-name]

# View logs (follow mode)
docker-compose logs -f [service-name]

# Rebuild after code changes
docker-compose up -d --build

# Remove all containers and volumes (⚠️ DELETES DATA)
docker-compose down -v
```

### Service Status

```bash
# Check running containers
docker ps

# Check resource usage
docker stats

# View specific service logs
docker-compose logs postgres
docker-compose logs airflow-scheduler
```

---

## 🗄️ PostgreSQL Commands

### Connect to Database

```bash
# Interactive shell
docker exec -it mm-postgres psql -U airflow -d movie_db

# Run single query
docker exec -it mm-postgres psql -U airflow -d movie_db -c "SELECT COUNT(*) FROM raw.movies_main;"
```

### Common Queries

```sql
-- List all schemas
\dn

-- List tables in schema
\dt raw.*
\dt staging.*
\dt marts.*

-- Describe table structure
\d+ raw.movies_main
\d+ marts.fact_movie_performance

-- Row counts by schema
SELECT 'raw.movies_main' as table, COUNT(*) as rows FROM raw.movies_main
UNION ALL SELECT 'raw.movie_extended', COUNT(*) FROM raw.movie_extended
UNION ALL SELECT 'raw.ratings', COUNT(*) FROM raw.ratings
UNION ALL SELECT 'staging.stg_movies_main', COUNT(*) FROM staging.stg_movies_main
UNION ALL SELECT 'marts.dim_movies', COUNT(*) FROM marts.dim_movies
UNION ALL SELECT 'marts.fact_movie_performance', COUNT(*) FROM marts.fact_movie_performance
UNION ALL SELECT 'analytics.vw_movie_performance_summary', COUNT(*) FROM analytics.vw_movie_performance_summary;

-- Check data freshness
SELECT
    'raw.movies_main' as table_name,
    MAX(loaded_at) as last_loaded,
    NOW() - MAX(loaded_at) as time_since_load
FROM raw.movies_main;

-- Top 10 highest revenue movies
SELECT title, revenue, release_year
FROM analytics.vw_movie_performance_summary
ORDER BY revenue_millions DESC
LIMIT 10;

-- Genre performance summary
SELECT genre_name, total_movies, avg_rating, total_revenue_millions
FROM analytics.vw_genre_performance
ORDER BY total_revenue_millions DESC;

-- Exit psql
\q
```

### Maintenance

```sql
-- Analyze tables (update statistics)
ANALYZE raw.movies_main;
ANALYZE marts.fact_movie_performance;

-- Vacuum (reclaim space)
VACUUM ANALYZE marts.fact_movie_performance;

-- Find large tables
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 10;
```

---

## ⚙️ dbt Commands

### Basic Operations

```bash
# Install packages (dbt_utils, etc.)
docker exec -it mm-dbt dbt deps

# Run all models
docker exec -it mm-dbt dbt run

# Run specific model
docker exec -it mm-dbt dbt run --models stg_movies_main
docker exec -it mm-dbt dbt run --models dim_movies

# Run models by tag
docker exec -it mm-dbt dbt run --models tag:staging
docker exec -it mm-dbt dbt run --models tag:marts

# Run models by folder
docker exec -it mm-dbt dbt run --models staging
docker exec -it mm-dbt dbt run --models marts

# Run downstream models (current + children)
docker exec -it mm-dbt dbt run --models stg_movies_main+

# Run upstream models (parents + current)
docker exec -it mm-dbt dbt run --models +dim_movies
```

### Testing

```bash
# Run all tests
docker exec -it mm-dbt dbt test

# Test specific model
docker exec -it mm-dbt dbt test --models stg_movies_main

# Store test failures for debugging
docker exec -it mm-dbt dbt test --store-failures

# Run tests by tag
docker exec -it mm-dbt dbt test --models tag:staging
```

### Documentation

```bash
# Generate dbt docs
docker exec -it mm-dbt dbt docs generate

# Serve docs (view in browser)
docker exec -it mm-dbt dbt docs serve --port 8080
# Open: http://localhost:8080

# Compile model (see compiled SQL)
docker exec -it mm-dbt dbt compile --models dim_movies

# Show compiled SQL
docker exec -it mm-dbt dbt show --models dim_movies
```

### Debugging

```bash
# Debug mode (verbose output)
docker exec -it mm-dbt dbt run --models dim_movies --debug

# Check source freshness
docker exec -it mm-dbt dbt source freshness

# List models
docker exec -it mm-dbt dbt list

# List models by selector
docker exec -it mm-dbt dbt list --models tag:staging
docker exec -it mm-dbt dbt list --models marts
```

---

## ✈️ Apache Airflow Commands

### DAG Management

```bash
# List all DAGs
docker exec -it mm-airflow-webserver airflow dags list

# Show DAG details
docker exec -it mm-airflow-webserver airflow dags show movie_analytics_etl

# Trigger DAG manually
docker exec -it mm-airflow-webserver airflow dags trigger movie_analytics_etl

# Pause/Unpause DAG
docker exec -it mm-airflow-webserver airflow dags pause movie_analytics_etl
docker exec -it mm-airflow-webserver airflow dags unpause movie_analytics_etl

# Delete DAG run
docker exec -it mm-airflow-webserver airflow dags delete movie_analytics_etl
```

### Task Management

```bash
# List tasks in DAG
docker exec -it mm-airflow-webserver airflow tasks list movie_analytics_etl

# Test specific task (dry run, no state saved)
docker exec -it mm-airflow-webserver airflow tasks test movie_analytics_etl ingest_raw_data 2026-03-11

# View task logs
docker exec -it mm-airflow-webserver airflow tasks logs movie_analytics_etl ingest_raw_data 2026-03-11

# Run task (saves state)
docker exec -it mm-airflow-webserver airflow tasks run movie_analytics_etl ingest_raw_data 2026-03-11
```

### Database

```bash
# Initialize Airflow database
docker exec -it mm-airflow-webserver airflow db init

# Upgrade Airflow database
docker exec -it mm-airflow-webserver airflow db upgrade

# Reset Airflow database (⚠️ DELETES ALL DATA)
docker exec -it mm-airflow-webserver airflow db reset
```

### Users

```bash
# Create admin user
docker exec -it mm-airflow-webserver airflow users create \
    --username admin \
    --password admin_pass \
    --firstname Hazel \
    --lastname Bea \
    --role Admin \
    --email hazel@example.com

# List users
docker exec -it mm-airflow-webserver airflow users list
```

### Connections

```bash
# List connections
docker exec -it mm-airflow-webserver airflow connections list

# Add PostgreSQL connection
docker exec -it mm-airflow-webserver airflow connections add \
    'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-schema 'movie_db' \
    --conn-login 'airflow' \
    --conn-password 'airflow_pass_12345' \
    --conn-port '5432'
```

---

## 🐍 Python Scripts

### Data Ingestion

```bash
# Run main ingestion script
docker exec -it mm-python-scripts python /scripts/ingest_data.py

# Run with Python debugger
docker exec -it mm-python-scripts python -m pdb /scripts/ingest_data.py

# Interactive Python shell (for debugging)
docker exec -it mm-python-scripts python

>>> from ingest_data import MovieDataIngestion
>>> ing = MovieDataIngestion('/app/datasets')
>>> ing.ingest_movies_main()
```

### Testing

```bash
# Run all Python tests
docker exec -it mm-python-scripts pytest /scripts/tests/ -v

# Run specific test file
docker exec -it mm-python-scripts pytest /scripts/tests/test_ingest_data.py -v

# Run with coverage
docker exec -it mm-python-scripts pytest /scripts/tests/ --cov=scripts --cov-report=html

# Run single test
docker exec -it mm-python-scripts pytest /scripts/tests/test_ingest_data.py::TestMovieDataIngestion::test_load_csv_to_postgres -v
```

### PySpark (Optional)

```bash
# Run PySpark ratings aggregation
docker exec -it mm-python-scripts python /scripts/pyspark_ratings_processor.py

# PySpark shell
docker exec -it mm-python-scripts pyspark
```

---

## 🔍 Monitoring & Troubleshooting

### Check Service Health

```bash
# Overall health
docker ps | grep mm-

# Check container logs
docker-compose logs --tail=100 -f postgres
docker-compose logs --tail=100 -f airflow-scheduler

# Resource usage
docker stats mm-postgres mm-airflow-webserver mm-dbt
```

### Database Health

```bash
# Connection test
docker exec mm-postgres pg_isready -U airflow -d movie_db

# Active connections
docker exec -it mm-postgres psql -U airflow -d movie_db -c "
SELECT COUNT(*) as active_connections
FROM pg_stat_activity
WHERE datname = 'movie_db';"

# Table sizes
docker exec -it mm-postgres psql -U airflow -d movie_db -c "
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname IN ('raw', 'staging', 'intermediate', 'marts', 'analytics')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"
```

### Airflow Health

```bash
# Check scheduler status
docker exec -it mm-airflow-scheduler airflow jobs check --job-type SchedulerJob

# View Airflow configuration
docker exec -it mm-airflow-webserver airflow config list

# Check for DAG import errors
docker exec -it mm-airflow-webserver python -c "
import sys
sys.path.insert(0, '/opt/airflow/dags')
import movie_etl_pipeline
print('DAG imported successfully!')
"
```

### dbt Health

```bash
# Debug dbt connection
docker exec -it mm-dbt dbt debug

# Check dbt version
docker exec -it mm-dbt dbt --version

# Validate dbt project
docker exec -it mm-dbt dbt parse
```

---

## 🔧 Common Fixes

### Reset Everything (Nuclear Option)

```bash
# ⚠️ WARNING: This deletes ALL data
docker-compose down -v
docker system prune -a
docker volume prune
docker-compose up -d --build
```

### Reset Just the Database

```bash
# ⚠️ WARNING: This deletes all data and recreates the database
docker-compose down
docker volume rm capstone-pipeline_postgres-data
docker-compose up -d postgres

# Wait for postgres to be healthy (check with: docker ps)
# Init scripts in postgres/init/ run automatically on first startup:
# - 01_roles.sh creates database roles
# - 02_schemas.sql creates schemas and permissions
# - 03_raw_tables.sql creates raw tables
# - 04_metadata_tables.sql creates metadata tables
```

### Fix Airflow Import Errors

```bash
# Check DAG syntax
docker exec -it mm-airflow-webserver python /opt/airflow/dags/movie_etl_pipeline.py

# Restart scheduler
docker-compose restart airflow-scheduler

# Clear DAG cache
docker exec -it mm-airflow-webserver rm -rf /opt/airflow/logs/*
```

### Fix dbt Errors

```bash
# Clean dbt artifacts
docker exec -it mm-dbt rm -rf /usr/app/target
docker exec -it mm-dbt rm -rf /usr/app/dbt_packages

# Reinstall packages
docker exec -it mm-dbt dbt clean
docker exec -it mm-dbt dbt deps

# Rerun with fresh start
docker exec -it mm-dbt dbt run --full-refresh
```

---

## 📊 Power BI Connection

### Connection String

```
Server: localhost:5430
Database: movie_db
Username: airflow
Password: airflow_pass_12345
```

### Import These Views

```
✅ analytics.vw_movie_performance_summary
✅ analytics.vw_genre_performance
✅ analytics.vw_yearly_trends
✅ marts.dim_movies
✅ marts.dim_genres
✅ marts.fact_movie_performance
```

---

## 🎯 Daily Operations Workflow

### Morning Routine (Check Pipeline Health)

```bash
# 1. Check services
docker ps

# 2. Check Airflow DAG runs
# Visit: http://localhost:8082

# 3. Check data freshness
docker exec -it mm-postgres psql -U airflow -d movie_db -c "
SELECT MAX(loaded_at) as last_load FROM raw.movies_main;"

# 4. Check row counts
docker exec -it mm-postgres psql -U airflow -d movie_db -c "
SELECT COUNT(*) FROM marts.fact_movie_performance;"
```

### Make Code Changes

```bash
# 1. Edit files locally

# 2. For Python changes: Rebuild container
docker-compose up -d --build python-app

# 3. For dbt changes: Just re-run
docker exec -it mm-dbt dbt run --models [changed_model]

# 4. For Airflow DAG changes: Wait ~30s for auto-reload
# Or restart scheduler:
docker-compose restart airflow-scheduler
```

### Run Full Pipeline Manually

```bash
# 1. Ingest data
docker exec -it mm-python-scripts python /scripts/ingest_data.py

# 2. Run dbt
docker exec -it mm-dbt dbt run

# 3. Test dbt
docker exec -it mm-dbt dbt test

# 4. Verify analytics views
docker exec -it mm-postgres psql -U airflow -d movie_db -c "
SELECT COUNT(*) FROM analytics.vw_movie_performance_summary;"
```

---

## 🆘 Emergency Contacts

| Issue                     | Command                                       | Expected Fix Time |
| ------------------------- | --------------------------------------------- | ----------------- |
| Services won't start      | `docker-compose down && docker-compose up -d` | 2 min             |
| Database connection fails | `docker-compose restart postgres`             | 30 sec            |
| Airflow DAG errors        | `docker-compose restart airflow-scheduler`    | 1 min             |
| dbt models fail           | `docker exec -it mm-dbt dbt debug`            | 5 min             |
| Out of disk space         | `docker system prune -a`                      | 10 min            |

---

**Created by**: Hazel Mae C. Bea  
**Last Updated**: March 11, 2026  
**Version**: 1.0.0

**Quick Links**:

- [Full Documentation](PROJECT_PLAN.md)
- [Architecture Guide](README.md)
- [Troubleshooting](README.md)
