# ============================================
# MOVIE MART DATA PIPELINE - MAKEFILE
# Commands for Docker, Postgres, Airflow, and dbt
# ============================================

.DEFAULT_GOAL := help

# Check .env file
ifneq (,$(wildcard .env))
include .env
export
endif

# Variables
POSTGRES_CONTAINER := mm-postgres
PYTHON_CONTAINER := mm-python-scripts
DBT_CONTAINER := mm-dbt
AIRFLOW_WEBSERVER_CONTAINER := mm-airflow-webserver
AIRFLOW_SCHEDULER_CONTAINER := mm-airflow-scheduler

POSTGRES_VOLUME := mm-postgres-data
POSTGRES_INIT_DIR := /docker-entrypoint-initdb.d

.PHONY: help\
	check-env up down restart build rebuild ps logs logs-postgres logs-airflow logs-dbt \
	bootstrap reset-db reinit-grants \
	db-shell-admin db-shell-etl db-shell-dbt db-shell-airflow \
	verify-roles verify-raw-grants verify-schemas \
	ingest ingest-check \
	dbt-deps dbt-run dbt-test dbt-debug \
	airflow-dags airflow-trigger \
	status

help:
	@echo "Movie Mart Pipeline - available targets"
	@echo ""
	@echo "Lifecycle"
	@echo "  make up                Start all services"
	@echo "  make down              Stop all services"
	@echo "  make restart           Restart all services"
	@echo "  make build             Build python/airflow images"
	@echo "  make rebuild           Rebuild and start all services"
	@echo "  make ps                Show running containers"
	@echo "  make logs              Tail all compose logs"
	@echo ""
	@echo "Database"
	@echo "  make bootstrap         Start services and verify core DB objects"
	@echo "  make reset-db          Recreate Postgres volume (DELETES ALL DATA)"
	@echo "  make reinit-grants     Re-apply grants from 03_raw_tables.sql"
	@echo "  make verify-roles      Show Postgres roles"
	@echo "  make verify-schemas    Show schemas"
	@echo "  make verify-raw-grants Show raw.movies_main privileges"
	@echo ""
	@echo "Database Shells"
	@echo "  make db-shell-admin    psql shell as admin"
	@echo "  make db-shell-etl      psql shell as etl"
	@echo "  make db-shell-dbt      psql shell as dbt"
	@echo "  make db-shell-airflow  psql shell as airflow"
	@echo ""
	@echo "Data + Transform"
	@echo "  make ingest            Run ingestion script in python container"
	@echo "  make ingest-check      Verify row counts in raw tables"
	@echo "  make dbt-deps          Install dbt packages"
	@echo "  make dbt-run           Run dbt models"
	@echo "  make dbt-test          Run dbt tests"
	@echo "  make dbt-debug         Validate dbt connection"
	@echo ""
	@echo "Airflow"
	@echo "  make airflow-dags      List DAGs"
	@echo "  make airflow-trigger DAG=<dag_id>  Trigger DAG"
	@echo ""
	@echo "Diagnostics"
	@echo "  make check-env         Validate required env vars"
	@echo "  make status            One-shot health summary"

check-env:
	@test -n "$(POSTGRES_USER)" || (echo "POSTGRES_USER is missing in .env" && exit 1)
	@test -n "$(POSTGRES_PASSWORD)" || (echo "POSTGRES_PASSWORD is missing in .env" && exit 1)
	@test -n "$(POSTGRES_DB)" || (echo "POSTGRES_DB is missing in .env" && exit 1)
	@test -n "$(ETL_DB_USER)" || (echo "ETL_DB_USER is missing in .env" && exit 1)
	@test -n "$(ETL_DB_PASSWORD)" || (echo "ETL_DB_PASSWORD is missing in .env" && exit 1)
	@test -n "$(DBT_DB_USER)" || (echo "DBT_DB_USER is missing in .env" && exit 1)
	@test -n "$(DBT_DB_PASSWORD)" || (echo "DBT_DB_PASSWORD is missing in .env" && exit 1)
	@test -n "$(ORCHESTRATOR_DB_USER)" || (echo "ORCHESTRATOR_DB_USER is missing in .env" && exit 1)
	@test -n "$(ORCHESTRATOR_DB_PASSWORD)" || (echo "ORCHESTRATOR_DB_PASSWORD is missing in .env" && exit 1)
	@echo "Environment looks good"

up: check-env
	docker-compose up -d

down:
	docker-compose down

restart:
	docker-compose restart

build:
	docker-compose build airflow-webserver airflow-scheduler python-app

rebuild:
	docker-compose up -d --build

ps:
	docker-compose ps

logs:
	docker-compose logs -f

logs-postgres:
	docker-compose logs -f postgres

logs-airflow:
	docker-compose logs -f airflow-webserver airflow-scheduler

logs-dbt:
	docker-compose logs -f dbt

bootstrap: up verify-roles verify-schemas
	@echo "Bootstrap checks complete"

reset-db:
	@echo "WARNING: this deletes all database data in volume $(POSTGRES_VOLUME)"
	docker-compose down
	-docker volume rm $(POSTGRES_VOLUME)
	docker-compose up -d postgres
	@echo "Postgres restarted; init scripts in postgres/init run automatically"

reinit-grants:
	MSYS_NO_PATHCONV=1 docker exec -i $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -f $(POSTGRES_INIT_DIR)/03_raw_tables.sql

db-shell-admin:
	docker exec -it $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) 

db-shell-etl:
	docker exec -it $(POSTGRES_CONTAINER) psql -U $(ETL_DB_USER) -d $(POSTGRES_DB)

db-shell-dbt:
	docker exec -it $(POSTGRES_CONTAINER) psql -U $(DBT_DB_USER) -d $(POSTGRES_DB)

db-shell-airflow:
	docker exec -it $(POSTGRES_CONTAINER) psql -U $(ORCHESTRATOR_DB_USER) -d $(POSTGRES_DB)

verify-roles:
	docker exec -i $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "\du"

verify-schemas:
	docker exec -i $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "\dn"

verify-raw-grants:
	docker exec -i $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "SELECT grantee, privilege_type FROM information_schema.table_privileges WHERE table_schema='raw' AND table_name='movies_main' ORDER BY grantee, privilege_type;"

ingest:
	@if [ ! -f scripts/ingest_data.py ]; then \
	echo "scripts/ingest_data.py not found"; \
	echo "Create ingestion script first, then run: make ingest"; \
	exit 1; \
	fi
	docker exec -it $(PYTHON_CONTAINER) python /scripts/ingest_data.py

ingest-check:
	docker exec -i $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "SELECT 'movies_main' AS table_name, COUNT(*) AS row_count FROM raw.movies_main UNION ALL SELECT 'movie_extended', COUNT(*) FROM raw.movie_extended UNION ALL SELECT 'ratings', COUNT(*) FROM raw.ratings;"

dbt-deps:
	docker exec -it $(DBT_CONTAINER) dbt deps

dbt-run:
	docker exec -it $(DBT_CONTAINER) dbt run

dbt-test:
	docker exec -it $(DBT_CONTAINER) dbt test

dbt-debug:
	docker exec -it $(DBT_CONTAINER) dbt debug

airflow-dags:
	docker exec -it $(AIRFLOW_WEBSERVER_CONTAINER) airflow dags list

airflow-trigger:
	@test -n "$(DAG)" || (echo "Usage: make airflow-trigger DAG=<dag_id>" && exit 1)
	docker exec -it $(AIRFLOW_WEBSERVER_CONTAINER) airflow dags trigger $(DAG)

status: ps
	@echo ""
	@echo "Postgres roles"
	@$(MAKE) --no-print-directory verify-roles
	@echo ""
	@echo "Raw grants"
	@$(MAKE) --no-print-directory verify-raw-grants