# DECM Course Workspace (Lectures 3-5)

This repository is the hands-on workspace for lectures 3, 4, and 5.

Read and execute in order:
1. Lecture 3: VS Code, Docker, Git, devcontainers
2. Lecture 4: Python ETL + Superset basics
3. Lecture 5: Airflow + dbt orchestration

## 1) Start Here (Lecture 3)

Primary guide:
- `docs/lectures/lecture-03/README.md`

Topic guides:
- `docs/lectures/lecture-03/vscode.md`
- `docs/lectures/lecture-03/docker.md`
- `docs/lectures/lecture-03/git.md`
- `docs/lectures/lecture-03/git-troubleshooting.md`
- `docs/lectures/lecture-03/devcontainers.md`
- `docs/lectures/lecture-03/troubleshooting.md`

Class bootstrap (inside devcontainer):

```bash
make init
make up-superset
```

Open Superset:
- URL: <http://localhost:8088>
- username: `admin`
- password: `admin`

## 2) Continue With Lecture 4 (ETL + Superset)

Primary guide:
- `docs/lectures/lecture-04/README.md`

Operational notes:
- `docs/lectures/lecture-04/operations.md`

Recommended setup:

```bash
make up-superset
make devcontainer-join-course-network
```

Note: keep Airflow stopped during Lecture 4 to avoid background orchestration side effects.
Airflow is started in Lecture 5.

Simple ETL tutorial example:

```bash
.venv/bin/python etl/lecture4_simple_air_quality.py --from 2026-03-10 --to 2026-03-12 --load-mode replace
```

Advanced Lecture 4 ETL flow:

```bash
make etl-bootstrap
.venv/bin/python -m etl.airviro.cli run --from 2026-03-10 --to 2026-03-12 --source-key air_quality_station_8 --source-key pollen_station_25 --verbose
make warehouse-status
```

Optional larger historical load:

```bash
make etl-backfill-2020-2025 VERBOSE=1
```

Note: as of 2026-03-27, the live Ohuseire historical API around late October 2025 has mixed timestamp behavior. The advanced Lecture 4 CLI ETL now skips `2025-10-26` automatically for `air_quality_station_8` and warns when a requested range crosses that day. The API maintainer has been notified, and the source behavior may change without prior warning. See `docs/lectures/lecture-04/README.md` for the current caution note.

Lecture 4 warehouse relations:
- `l4_simple.air_quality_station_8_hourly`
- `l4_mart.v_air_quality_hourly_station_8`
- `l4_mart.v_pollen_daily_station_25`
- `l4_mart.v_airviro_measurements_long`

Superset SQL helper snippets:
- `superset/snippets.md`

## 3) Continue With Lecture 5 (Airflow + dbt)

Primary guide:
- `docs/lectures/lecture-05/README.md`

Operational notes:
- `docs/lectures/lecture-05/operations.md`

Core commands:

```bash
make up-airflow
make dbt-build
make airflow-unpause-dags
make airflow-trigger-incremental
```

Note: the Lecture 5 warehouse/dbt refactor is planned separately.
Lecture 4 now uses the lecture-specific `l4_*` schemas.

Open Airflow:
- URL: <http://localhost:8080>
- username: `airflow`
- password: `airflow`

## Stack Lifecycle (Quick Reference)

Start services:

```bash
make up-superset
make up-airflow
make up-all
```

Inspect services:

```bash
make ps
make logs SERVICE=superset
make logs SERVICE=airflow-scheduler
make logs SERVICE=postgres
```

Stop and reset:

```bash
make down
make reset-volumes
make reset-all
```

These lifecycle targets detach the devcontainer from the course Compose network before teardown so Docker can remove the project network cleanly.

## Environment Notes

- Work from inside the devcontainer.
- Host requirements: VS Code, Docker, Git.
- `.env` is local-only and ignored by git.
- `.env.example` is the template used by `make init`.
- Prefer the `make up-*` targets over raw `docker compose up` inside the devcontainer.
- Docker runs through the mounted host socket, so the Makefile resolves bind mount paths against the host daemon.
- Run `make print-host-workspace` to inspect the requested and resolved workspace paths when debugging bind mounts.

## Lecture Index

- `docs/lectures/index.md`
