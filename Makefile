SHELL := /bin/bash
ENV_FILE := .env
HOST_WORKSPACE ?= $(PWD)
export HOST_WORKSPACE
# TODO(option-3): remove sudo once devcontainer user has direct Docker socket access.
COMPOSE := sudo --preserve-env=HOST_WORKSPACE docker compose --env-file $(ENV_FILE)
PROFILES_SUPERSET := --profile superset
PROFILES_AIRFLOW := --profile airflow
ETL_VERBOSE_FLAG := $(if $(filter 1 true yes,$(VERBOSE)),--verbose,)

.PHONY: help init check-host-workspace up-superset up-airflow up-all down logs ps reset-volumes reset-all \
	etl-bootstrap etl-dry-run etl-backfill-2020-2025 etl-backfill-2020-today \
	devcontainer-join-course-network

help:
	@echo "Targets:"
	@echo "  make init           Copy .env.example to .env (if missing), set secret key, set AIRFLOW_UID"
	@echo "  make up-superset    Start Superset stack (profile: superset)"
	@echo "  make up-airflow     Start Airflow stack (profile: airflow)"
	@echo "  make up-all         Start Superset + Airflow"
	@echo "  make down           Stop and remove containers"
	@echo "  make ps             Show container status"
	@echo "  make logs SERVICE=<name>  Follow logs for one service"
	@echo "  make reset-volumes  Remove containers and named volumes"
	@echo "  make reset-all      Remove containers, volumes, and local images"
	@echo "  make etl-bootstrap  Create/update ETL warehouse schema objects"
	@echo "  make etl-dry-run    Run ETL extraction + validation without database writes"
	@echo "  make etl-backfill-2020-2025  Load Airviro data for 2020-2025"
	@echo "  make etl-backfill-2020-today Load Airviro data from 2020-01-01 to today"
	@echo "    Optional: add VERBOSE=1 to ETL targets for progress logs"
	@echo "  make devcontainer-join-course-network  Attach devcontainer to compose network"

init:
	@if [ ! -f "$(ENV_FILE)" ]; then cp .env.example "$(ENV_FILE)"; echo "Created $(ENV_FILE) from .env.example"; fi
	@if grep -q '^SUPERSET_SECRET_KEY=__CHANGE_ME__' "$(ENV_FILE)"; then \
		key="$$(openssl rand -hex 32)"; \
		sed -i "s/^SUPERSET_SECRET_KEY=.*/SUPERSET_SECRET_KEY=$$key/" "$(ENV_FILE)"; \
		echo "Generated SUPERSET_SECRET_KEY"; \
	fi
	@uid="$$(id -u)"; \
	if grep -q '^AIRFLOW_UID=' "$(ENV_FILE)"; then \
		sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$uid/" "$(ENV_FILE)"; \
	else \
		echo "AIRFLOW_UID=$$uid" >> "$(ENV_FILE)"; \
	fi
	@mkdir -p airflow/dags

check-host-workspace:
	@if [ -z "$$HOST_WORKSPACE" ]; then echo "HOST_WORKSPACE is not set."; exit 1; fi

up-superset: init check-host-workspace
	@$(COMPOSE) $(PROFILES_SUPERSET) up -d

up-airflow: init check-host-workspace
	@$(COMPOSE) $(PROFILES_AIRFLOW) up -d

up-all: init check-host-workspace
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) up -d

down: check-host-workspace
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) down --remove-orphans

ps: check-host-workspace
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) ps

logs: check-host-workspace
	@if [ -z "$(SERVICE)" ]; then echo "Usage: make logs SERVICE=<service-name>"; exit 1; fi
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) logs -f --tail=200 $(SERVICE)

reset-volumes: check-host-workspace
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) down -v --remove-orphans

reset-all: check-host-workspace
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) down -v --rmi local --remove-orphans

etl-bootstrap: init
	@.venv/bin/python -m etl.airviro.cli bootstrap-db

etl-dry-run: init
	@.venv/bin/python -m etl.airviro.cli run --from 2025-01-01 --to 2025-01-31 --dry-run $(ETL_VERBOSE_FLAG)

etl-backfill-2020-2025: init
	@.venv/bin/python -m etl.airviro.cli run --from 2020-01-01 --to 2025-12-31 $(ETL_VERBOSE_FLAG)

etl-backfill-2020-today: init
	@.venv/bin/python -m etl.airviro.cli backfill --from 2020-01-01 $(ETL_VERBOSE_FLAG)

devcontainer-join-course-network: init
	@project_name="$$(grep -E '^COMPOSE_PROJECT_NAME=' "$(ENV_FILE)" | cut -d '=' -f2-)"; \
	if [ -z "$$project_name" ]; then project_name="course"; fi; \
	network_name="$${project_name}_default"; \
	container_id="$$(hostname)"; \
	if ! sudo docker network inspect "$$network_name" >/dev/null 2>&1; then \
		echo "Compose network '$$network_name' was not found. Run 'make up-superset' or 'make up-all' first."; \
		exit 1; \
	fi; \
	connect_output="$$(sudo docker network connect "$$network_name" "$$container_id" 2>&1 || true)"; \
	if [ -z "$$connect_output" ]; then \
		echo "Connected devcontainer '$$container_id' to '$$network_name'."; \
	elif echo "$$connect_output" | grep -qi 'already exists'; then \
		echo "Devcontainer '$$container_id' is already attached to '$$network_name'."; \
	else \
		echo "$$connect_output"; \
		exit 1; \
	fi
