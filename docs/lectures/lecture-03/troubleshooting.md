# Lecture 3 Troubleshooting

Use this checklist before asking for help.

## 1) `docker` commands fail with daemon/connection error

Symptom:

- `Cannot connect to the Docker daemon`

Fix:

1. Start Docker Desktop.
2. Wait until it shows healthy/running.
3. Retry command.

## 2) VS Code cannot reopen in container

Symptom:

- `Dev Containers: Reopen in Container` fails or is missing.

Fix:

1. Install Dev Containers extension (`ms-vscode-remote.remote-containers`).
2. Reload VS Code window.
3. Retry reopen command.

## 3) Windows students get path or mount issues

Symptom:

- Bind mounts not resolving as expected.

Fix:

1. Run the stack with `make up-superset`, `make up-airflow`, or `make up-all` from inside the devcontainer.
2. Run `make print-host-workspace` and confirm the resolved path looks like a real host path, not `/workspaces/...`.
3. Confirm Docker Desktop uses WSL2 backend.
4. Reopen folder from local filesystem path (not temporary archive path).
5. Rebuild/reopen devcontainer.

## 4) Permission denied on mounted files

Symptom:

- Cannot edit or write to files from inside container.

Fix:

1. Ensure project is opened with normal user permissions.
2. Rebuild container so user/group mappings apply cleanly.
3. Avoid running editor as administrator unless required.

## 5) Port already in use (`8088`, `8080`, `5432`)

Symptom:

- Compose up fails due to port conflict.

Fix:

1. Stop conflicting local service.
2. Or stop previous course containers:

```bash
make down
```

3. Retry:

```bash
make up-all
```

## 6) Git push fails due to authentication

Symptom:

- Push rejected or auth prompt loops.

Fix:

1. Sign in to GitHub in VS Code.
2. Confirm remote URL points to your own fork.
3. Retry `Sync Changes`.

If this does not resolve quickly, use the dedicated guide:

- [Git Troubleshooting](./git-troubleshooting.md)

## 7) `make` command not found in devcontainer

Symptom:

- `make: command not found`

Fix:

1. Rebuild/reopen devcontainer.
2. Check `.devcontainer/post-create.sh` completed.
3. Re-run setup commands.

## 8) Devcontainer setup fails during `post-create.sh` or `.venv` creation

Symptom:

- VS Code reports that `postCreateCommand` failed
- bootstrap stops while creating `.venv`
- rerunning the script manually works better than the first automatic run

Fix:

1. Rebuild/reopen the devcontainer once.
2. In the devcontainer terminal, rerun:

```bash
bash .devcontainer/post-create.sh /workspaces/course
```

3. If `.venv` still looks broken, remove it and rerun:

```bash
rm -rf .venv
bash .devcontainer/post-create.sh /workspaces/course
```

Notes:

- The script now creates the project virtual environment with copied files instead of symlinks, which is more reliable on Docker Desktop bind mounts.
- This issue has been seen more often on Windows hosts, but the retry steps are safe on macOS too.

## 9) Recover Cleanly After Windows Path Workarounds

Symptom:

- you temporarily edited `docker-compose.yml` or other repo files during class just to get the stack running;
- `make up-superset` partly works but later fails at `superset-init`;
- `make down` or `docker compose down` now behaves strangely because the old workaround and the current repo no longer match;
- you want to get back to the course setup exactly as the repository expects.

What usually went wrong:

- the temporary classroom workaround changed bind mount behavior;
- `HOST_WORKSPACE` affects bind mounts in Compose;
- Superset metadata itself is stored in the named Postgres volume, not in those bind mounts.

That means the first recovery attempt should preserve Docker volumes and only restore the repo and path handling. Wiping Docker volumes should be the last resort.

### Step A: Save Your Own Local Code Changes

Before replacing the local repo, keep anything you wrote yourself.

Good options:

1. Commit your own work to a personal local branch.
2. Or copy the files you care about to another folder.

Do not keep temporary workaround edits to files like:

- `docker-compose.yml`
- `Makefile`
- `.devcontainer/devcontainer.json`
- `.devcontainer/post-create.sh`

unless you intentionally want to maintain those changes yourself.

### Step B: Sync Your Fork On GitHub

1. Open your fork on GitHub.
2. If you have not forked the course repo yet, do that first.
3. If you already have a fork, sync its `main` branch with upstream.

### Path 1: Soft Recovery (Preserve Databases And Superset Content)

Use this first. It keeps Docker volumes in place, so existing Superset dashboards and Postgres data can survive.

#### Step C1: Restore Clean Repository Files

The simplest path is a fresh clone:

1. Move the old local folder aside after saving your own work.
2. Clone your fork again.
3. Open the fresh clone in VS Code.
4. Run `Dev Containers: Rebuild and Reopen in Container`.

If you prefer to keep the same local folder:

1. switch back to a clean `main` that matches your fork;
2. discard only the temporary local workaround edits;
3. rebuild and reopen the devcontainer.

#### Step D1: Fix The Current Containers Without Deleting Volumes

If `make down` works, you can use it.

If `make down` fails because `HOST_WORKSPACE` is still wrong, use a host terminal from the repo folder instead.

Windows PowerShell:

```powershell
$env:HOST_WORKSPACE = (Get-Location).Path
docker compose --profile superset --profile airflow down --remove-orphans
Remove-Item Env:HOST_WORKSPACE
```

Windows Command Prompt:

```bat
set HOST_WORKSPACE=%cd%
docker compose --profile superset --profile airflow down --remove-orphans
set HOST_WORKSPACE=
```

Important:

- do not add `-v` in this soft-recovery path;
- `down --remove-orphans` removes containers, but keeps named volumes.

#### Step E1: Start The Clean Course Setup Again

Inside the rebuilt devcontainer run:

```bash
make print-host-workspace
make init
make up-superset
make devcontainer-join-course-network
```

What to check:

- `make print-host-workspace` should show a real host path, not `/workspaces/...`
- `make up-superset` should run without editing `docker-compose.yml`

#### Step F1: If `superset-init` Still Fails

At that point, the problem is likely not the bind mounts anymore.

The most likely remaining issue is that Postgres was first initialized when the `postgres/init` bind mount was missing, so the expected databases and roles were never created.

Expected databases:

- `postgres`
- `superset_meta`
- `warehouse`
- `airflow_meta`

If those are missing, ask for help or recreate them manually before considering a full reset.

### Path 2: Hard Reset (Last Resort)

Use this only if:

- the soft recovery above failed;
- you do not care about existing local Superset content;
- or you backed it up first.

Important implication:

- this removes local Docker data for this course stack;
- this includes the shared Postgres volume, so Superset metadata, warehouse data, and Airflow metadata are lost unless you back them up first.

#### Step C2: Back Up Superset Content First

If you want to keep your Superset dashboards, charts, datasets, or saved connections, make a SQL backup before deleting course volumes.

Run inside the devcontainer while the old stack is still running:

```bash
project_name="$(grep -E '^COMPOSE_PROJECT_NAME=' .env 2>/dev/null | cut -d '=' -f2-)"
[ -n "$project_name" ] || project_name="course"
postgres_container="${project_name}-postgres-1"
mkdir -p backups
sudo docker exec "$postgres_container" pg_dump -U postgres -d superset_meta > "backups/superset_meta_backup.sql"
```

If you do not care about old Superset content, you can skip this backup.

#### Step D2: Remove Containers And Volumes

If `make reset-volumes` works, you can use it.

If it does not work because `HOST_WORKSPACE` is still wrong, use a host terminal from the repo folder instead.

Windows PowerShell:

```powershell
$env:HOST_WORKSPACE = (Get-Location).Path
docker compose --profile superset --profile airflow down -v --remove-orphans
Remove-Item Env:HOST_WORKSPACE
```

Windows Command Prompt:

```bat
set HOST_WORKSPACE=%cd%
docker compose --profile superset --profile airflow down -v --remove-orphans
set HOST_WORKSPACE=
```

#### Step E2: Recreate The Course Repo Cleanly

The simplest recovery path is a fresh clone.

1. Move the old broken local folder aside or delete it after saving your own work.
2. Clone your fork again.
3. Open the fresh clone in VS Code.
4. Run `Dev Containers: Rebuild and Reopen in Container`.

#### Step F2: Validate The Fixed Course Setup

Inside the rebuilt devcontainer run:

```bash
make print-host-workspace
make init
make up-superset
make devcontainer-join-course-network
.venv/bin/python etl/lecture4_simple_air_quality.py --from 2026-03-10 --to 2026-03-10 --load-mode update
```

What to check:

- `make print-host-workspace` should show a real host path, not `/workspaces/...`
- `make up-superset` should complete without needing local edits to `docker-compose.yml`
- the simple ETL command should finish and load one day successfully

#### Step G2: Restore Superset Content If You Backed It Up

After the clean setup is running, you can restore the saved `superset_meta` dump.

Run inside the devcontainer:

```bash
project_name="$(grep -E '^COMPOSE_PROJECT_NAME=' .env 2>/dev/null | cut -d '=' -f2-)"
[ -n "$project_name" ] || project_name="course"
postgres_container="${project_name}-postgres-1"
sudo docker exec "$postgres_container" psql -U postgres -d postgres -c "DROP DATABASE IF EXISTS superset_meta;"
sudo docker exec "$postgres_container" psql -U postgres -d postgres -c "CREATE DATABASE superset_meta OWNER superset;"
cat backups/superset_meta_backup.sql | sudo docker exec -i "$postgres_container" psql -U postgres -d superset_meta
```

Then restart the Superset stack:

```bash
make down
make up-superset
```

If you did not create a backup first, there is nothing to restore and Superset will start with a fresh empty local state.
