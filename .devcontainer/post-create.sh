#!/usr/bin/env bash
set -euo pipefail

workspace_dir="${1:-$PWD}"
venv_dir="$workspace_dir/.venv"
venv_python="$venv_dir/bin/python"
export DEBIAN_FRONTEND=noninteractive

on_error() {
	echo "post-create failed. You can retry with: bash .devcontainer/post-create.sh \"$workspace_dir\"" >&2
}

log_step() {
	echo
	echo "==> $1"
}

trap on_error ERR

# ensure safe home for codex
log_step "Preparing Codex home"
mkdir -p "$workspace_dir/.codex-home"
if [ -L "$HOME/.codex" ] && [ "$(readlink "$HOME/.codex")" = "$workspace_dir/.codex-home" ]; then
	:
else
	rm -rf "$HOME/.codex"
	ln -s "$workspace_dir/.codex-home" "$HOME/.codex"
fi

# update and install required system packages
log_step "Installing system packages"
sudo apt-get update

sudo apt-get install -y docker.io docker-compose make curl openssl python3-pip python3-venv

# create and populate python environment
log_step "Creating project virtual environment"
if [ ! -x "$venv_python" ]; then
	rm -rf "$venv_dir"
	# `--copies` is more reliable on Docker Desktop bind mounts than symlink-heavy venvs.
	python3 -m venv --copies "$venv_dir"
fi

log_step "Installing Python packages"
"$venv_python" -m pip install --upgrade pip
"$venv_python" -m pip install --no-cache-dir dbt-postgres requests sqlalchemy psycopg2-binary pandas pyarrow

log_step "Devcontainer bootstrap complete"
