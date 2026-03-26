SHELL := /bin/bash
ENV_FILE := .env
WORKSPACE_DIR := $(realpath $(dir $(lastword $(MAKEFILE_LIST))))
HOST_WORKSPACE ?= $(WORKSPACE_DIR)
# Docker commands run inside the devcontainer against the host daemon through the
# mounted socket, so bind mounts must use the daemon-visible workspace path.
RESOLVE_HOST_WORKSPACE = \
	requested_path="$(HOST_WORKSPACE)"; \
	if [ -z "$$requested_path" ]; then \
		requested_path="$(WORKSPACE_DIR)"; \
	fi; \
	path=""; \
	inspected_path=""; \
	if printf "%s" "$$requested_path" | grep -Eq "^/.*[A-Za-z]:[\\\\/]"; then \
		echo "HOST_WORKSPACE contains an invalid mixed path: $$requested_path" >&2; \
		exit 1; \
	elif printf "%s" "$$requested_path" | grep -Eq "^[A-Za-z]:[\\\\/]"; then \
		drive="$$(printf "%s" "$$requested_path" | cut -c1 | tr "[:upper:]" "[:lower:]")"; \
		rest="$$(printf "%s" "$$requested_path" | sed -E "s@^[A-Za-z]:[\\\\/]?@@; s@\\\\@/@g")"; \
		path="/run/desktop/mnt/host/$$drive/$$rest"; \
	elif [ "$$requested_path" != "$(WORKSPACE_DIR)" ]; then \
		path="$$requested_path"; \
	fi; \
	if [ -z "$$path" ]; then \
	inspected_path="$$(sudo docker inspect "$$(hostname)" --format '{{range .Mounts}}{{if eq .Destination "$(WORKSPACE_DIR)"}}{{println .Source}}{{end}}{{end}}' 2>/dev/null | head -n 1)"; \
	if [ -n "$$inspected_path" ]; then \
		path="$$inspected_path"; \
	else \
		path="$$requested_path"; \
	fi; \
	fi; \
	if [ -z "$$path" ]; then \
		echo "HOST_WORKSPACE is not set." >&2; \
		exit 1; \
	elif ! printf "%s" "$$path" | grep -Eq '^/'; then \
		echo "HOST_WORKSPACE must be an absolute path. Current value: $$path" >&2; \
		exit 1; \
	fi;
# TODO(option-3): remove sudo once devcontainer user has direct Docker socket access.
COMPOSE := $(RESOLVE_HOST_WORKSPACE) sudo env HOST_WORKSPACE="$$path" docker compose --env-file $(ENV_FILE)
PROFILES_SUPERSET := --profile superset
PROFILES_AIRFLOW := --profile airflow
ETL_VERBOSE_FLAG := $(if $(filter 1 true yes,$(VERBOSE)),--verbose,)
DBT_PROJECT_DIR := /opt/airflow/dbt
LECTURE4_SOURCE_KEYS := --source-key air_quality_station_8 --source-key pollen_station_25
DAG_ID ?= airviro_incremental
BACKFILL_START ?= 2020-01-01
BACKFILL_END ?=
BACKFILL_CHUNK_DAYS ?= 31
BACKFILL_SOURCE_KEYS ?=
BACKFILL_ADVANCE_WATERMARK ?= true
STATUS_INDICATOR_LIMIT ?= 500
STATUS_AUDIT_LIMIT ?= 10

.PHONY: help init check-host-workspace print-host-workspace up-superset up-airflow up-all down logs ps reset-volumes reset-all \
	etl-bootstrap etl-dry-run etl-backfill-2020-2025 etl-backfill-2020-today warehouse-status warehouse-status-json \
	devcontainer-join-course-network devcontainer-leave-course-network dbt-debug dbt-seed dbt-run dbt-test dbt-build \
	airflow-list-dags airflow-list-runs airflow-trigger-incremental airflow-trigger-backfill \
	airflow-unpause-dags airflow-pause-dags

help:
	@echo "Targets:"
	@echo "  make init           Copy .env.example to .env (if missing), set secret key, set AIRFLOW_UID"
	@echo "  make up-superset    Start Superset stack (profile: superset)"
	@echo "  make up-airflow     Start Airflow stack (profile: airflow)"
	@echo "  make up-all         Start Superset + Airflow"
	@echo "  make down           Stop/remove containers and detach devcontainer from course network if needed"
	@echo "  make ps             Show container status"
	@echo "  make print-host-workspace  Show the host path docker compose will use for bind mounts"
	@echo "  make logs SERVICE=<name>  Follow logs for one service"
	@echo "  make reset-volumes  Remove containers and named volumes"
	@echo "  make reset-all      Remove containers, volumes, and local images"
	@echo "  make etl-bootstrap  Ensure Lecture 4 advanced ETL warehouse schema objects exist"
	@echo "  make etl-dry-run    Run ETL extraction + validation without database writes"
	@echo "  make etl-backfill-2020-2025  Load Lecture 4 Airviro data for station 8 + pollen 25"
	@echo "  make etl-backfill-2020-today Load Lecture 4 Airviro data from 2020-01-01 to today"
	@echo "  make warehouse-status        Show Lecture 4 warehouse health + completeness report"
	@echo "  make warehouse-status-json   Same report in JSON format"
	@echo "    Optional: STATUS_INDICATOR_LIMIT=500 STATUS_AUDIT_LIMIT=10"
	@echo "    Optional: add VERBOSE=1 to ETL targets for progress logs"
	@echo "  make dbt-debug      Validate dbt connection/profile in airflow-scheduler"
	@echo "  make dbt-seed       Load dbt seeds (wind direction mapping)"
	@echo "  make dbt-run        Build dbt models (staging + marts)"
	@echo "  make dbt-test       Run dbt data tests"
	@echo "  make dbt-build      Run dbt seed + run + test"
	@echo "  make airflow-list-dags      List DAGs in airflow-scheduler"
	@echo "  make airflow-list-runs DAG_ID=<dag_id>  List recent DAG runs"
	@echo "  make airflow-unpause-dags   Unpause airviro_incremental and airviro_backfill"
	@echo "  make airflow-pause-dags     Pause airviro_incremental and airviro_backfill"
	@echo "  make airflow-trigger-incremental        Trigger airviro_incremental DAG"
	@echo "  make airflow-trigger-backfill BACKFILL_START=YYYY-MM-DD [BACKFILL_END=YYYY-MM-DD] [BACKFILL_CHUNK_DAYS=31] [BACKFILL_SOURCE_KEYS=air_quality_station_19] [BACKFILL_ADVANCE_WATERMARK=true]"
	@echo "  make devcontainer-join-course-network  Attach devcontainer to compose network"
	@echo "  make devcontainer-leave-course-network Detach devcontainer from compose network"

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
	@$(RESOLVE_HOST_WORKSPACE) :

print-host-workspace:
	@$(RESOLVE_HOST_WORKSPACE) \
	echo "Workspace path in devcontainer: $(WORKSPACE_DIR)"; \
	echo "Requested HOST_WORKSPACE: $(HOST_WORKSPACE)"; \
	if [ -n "$$inspected_path" ]; then \
		echo "Detected host mount source via docker inspect: $$inspected_path"; \
	else \
		echo "Detected host mount source via docker inspect: unavailable"; \
	fi; \
	echo "Resolved HOST_WORKSPACE for docker compose: $$path"

up-superset: init check-host-workspace
	@$(COMPOSE) $(PROFILES_SUPERSET) up -d

up-airflow: init check-host-workspace
	@$(COMPOSE) $(PROFILES_AIRFLOW) up -d

up-all: init check-host-workspace
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) up -d

down: check-host-workspace
	@$(MAKE) --no-print-directory devcontainer-leave-course-network
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) down --remove-orphans

ps: check-host-workspace
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) ps

logs: check-host-workspace
	@if [ -z "$(SERVICE)" ]; then echo "Usage: make logs SERVICE=<service-name>"; exit 1; fi
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) logs -f --tail=200 $(SERVICE)

reset-volumes: check-host-workspace
	@$(MAKE) --no-print-directory devcontainer-leave-course-network
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) down -v --remove-orphans

reset-all: check-host-workspace
	@$(MAKE) --no-print-directory devcontainer-leave-course-network
	@$(COMPOSE) $(PROFILES_SUPERSET) $(PROFILES_AIRFLOW) down -v --rmi local --remove-orphans

etl-bootstrap: init
	@.venv/bin/python -m etl.airviro.cli bootstrap-db

etl-dry-run: init
	@.venv/bin/python -m etl.airviro.cli run --from 2026-03-10 --to 2026-03-12 $(LECTURE4_SOURCE_KEYS) --dry-run $(ETL_VERBOSE_FLAG)

etl-backfill-2020-2025: init
	@.venv/bin/python -m etl.airviro.cli run --from 2020-01-01 --to 2025-12-31 $(LECTURE4_SOURCE_KEYS) $(ETL_VERBOSE_FLAG)

etl-backfill-2020-today: init
	@.venv/bin/python -m etl.airviro.cli backfill --from 2020-01-01 $(LECTURE4_SOURCE_KEYS) $(ETL_VERBOSE_FLAG)

warehouse-status: init
	@.venv/bin/python -m etl.airviro.cli warehouse-status --indicator-limit $(STATUS_INDICATOR_LIMIT) --audit-limit $(STATUS_AUDIT_LIMIT)

warehouse-status-json: init
	@.venv/bin/python -m etl.airviro.cli warehouse-status --json --indicator-limit $(STATUS_INDICATOR_LIMIT) --audit-limit $(STATUS_AUDIT_LIMIT)

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

devcontainer-leave-course-network:
	@project_name="course"; \
	if [ -f "$(ENV_FILE)" ]; then \
		project_name="$$(grep -E '^COMPOSE_PROJECT_NAME=' "$(ENV_FILE)" | cut -d '=' -f2-)"; \
	fi; \
	if [ -z "$$project_name" ]; then project_name="course"; fi; \
	network_name="$${project_name}_default"; \
	container_id="$$(hostname)"; \
	if ! sudo docker network inspect "$$network_name" >/dev/null 2>&1; then \
		echo "Compose network '$$network_name' is not present."; \
	elif disconnect_output="$$(sudo docker network disconnect "$$network_name" "$$container_id" 2>&1)"; then \
		echo "Disconnected devcontainer '$$container_id' from '$$network_name'."; \
	elif echo "$$disconnect_output" | grep -qi 'not connected'; then \
		echo "Devcontainer '$$container_id' is not attached to '$$network_name'."; \
	else \
		echo "$$disconnect_output"; \
		exit 1; \
	fi

dbt-debug: up-airflow
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler bash -lc "cd $(DBT_PROJECT_DIR) && dbt debug --project-dir . --profiles-dir ."

dbt-seed: up-airflow
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler bash -lc "cd $(DBT_PROJECT_DIR) && dbt seed --project-dir . --profiles-dir ."

dbt-run: up-airflow
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler bash -lc "cd $(DBT_PROJECT_DIR) && dbt run --project-dir . --profiles-dir ."

dbt-test: up-airflow
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler bash -lc "cd $(DBT_PROJECT_DIR) && dbt test --project-dir . --profiles-dir ."

dbt-build: up-airflow
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler bash -lc "cd $(DBT_PROJECT_DIR) && dbt seed --project-dir . --profiles-dir . && dbt run --project-dir . --profiles-dir . && dbt test --project-dir . --profiles-dir ."

airflow-list-dags: up-airflow
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler airflow dags list

airflow-list-runs: up-airflow
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler airflow dags list-runs $(DAG_ID)

airflow-trigger-incremental: up-airflow
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler airflow dags trigger airviro_incremental

airflow-trigger-backfill: up-airflow
	@conf="{\"start_date\":\"$(BACKFILL_START)\",\"end_date\":\"$(BACKFILL_END)\",\"chunk_days\":$(BACKFILL_CHUNK_DAYS),\"source_keys\":\"$(BACKFILL_SOURCE_KEYS)\",\"advance_watermark\":$(BACKFILL_ADVANCE_WATERMARK)}"; \
	echo "Trigger conf: $$conf"; \
	$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler airflow dags trigger airviro_backfill --conf "$$conf"

airflow-unpause-dags: up-airflow
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler airflow dags unpause -y airviro_incremental
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler airflow dags unpause -y airviro_backfill

airflow-pause-dags: up-airflow
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler airflow dags pause -y airviro_incremental
	@$(COMPOSE) $(PROFILES_AIRFLOW) exec -T airflow-scheduler airflow dags pause -y airviro_backfill
