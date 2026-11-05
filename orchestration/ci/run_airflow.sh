#!/usr/bin/env bash
# Airflow 3 on a throwaway GitHub runner, for end-to-end runs of the history DAG on real data
# (ADR-0004, amended). Called by the `history` workflow's `airflow` step from the repository
# root, after the project and engine environments are installed.
#
# Airflow gets its own environment (its constraints file pins pyarrow and importlib-metadata
# differently from the project's lock); its tasks call the project's `ge-intel` by path. A
# throwaway Postgres (preinstalled on the image) holds Airflow's metadata, the LocalExecutor
# runs the tasks, and every visitor is an admin: the API server listens on localhost only and
# the runner is gone when the job ends.
#
# Two runs: the full range, then the same range again, which finds every month recorded
# complete in storage and skips it (the contract's idempotency, seen from Airflow).
set -euo pipefail

AIRFLOW_VERSION=3.3.2
REPO=$(pwd)
OUT="$REPO/data/evidence/airflow"
VENV="$RUNNER_TEMP/airflow-venv"
export AIRFLOW_HOME="$RUNNER_TEMP/airflow"
mkdir -p "$OUT" "$AIRFLOW_HOME"

uv venv -q "$VENV" --python 3.12
uv pip install -q --python "$VENV/bin/python" "apache-airflow[postgres]==$AIRFLOW_VERSION" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-3.12.txt"

sudo systemctl start postgresql.service
sudo -u postgres psql -q -c "create user airflow with password 'airflow'" \
  -c "create database airflow owner airflow"

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@localhost/airflow"
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=4
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$REPO/orchestration/dags"
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
export AIRFLOW__API__HOST=127.0.0.1
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=30

# What the tasks call: the contract, in the project's environment; their run records on the
# runner's disk, marked as Airflow's.
export GE_INTEL="$REPO/.venv/bin/ge-intel"
export GE_SPARK="$REPO/engines/.venv/bin/ge-spark"
export GE_HISTORY_DIR="$REPO/data/history"
export GE_LAKE_ROOT="$REPO/data/runs"
export GE_ORCHESTRATOR=airflow
export GE_SPARK_THREADS=2 GE_SPARK_MEMORY=6g

"$VENV/bin/airflow" db migrate > "$AIRFLOW_HOME/migrate.log"
"$VENV/bin/airflow" pools set history_duckdb 1 "one writer: the DuckDB build of history/prices_1h"
"$VENV/bin/airflow" pools set history_spark 1 "one writer: the PySpark build of history/prices_1h"
"$VENV/bin/airflow" version > "$OUT/versions.txt"
for part in api-server scheduler dag-processor triggerer; do
  "$VENV/bin/airflow" "$part" > "$AIRFLOW_HOME/$part.log" 2>&1 &
done

status=0
"$VENV/bin/python" orchestration/ci/drive.py --dag ge_history_reprocess --label run1 --out "$OUT" || status=$?
"$VENV/bin/python" orchestration/ci/drive.py --dag ge_history_reprocess --label run2 --out "$OUT" \
  --timeout 3600 || status=$?

# The records the tasks wrote: completion lives in storage, not only in Airflow.
"$REPO/.venv/bin/python" - "$GE_LAKE_ROOT" > "$OUT/task_runs.json" <<'EOF'
import json, sys
from deltalake import DeltaTable
rows = DeltaTable(f"{sys.argv[1]}/ops/task_runs").to_pyarrow_table().to_pylist()
print(json.dumps(rows, default=str, indent=1))
EOF
cp "$AIRFLOW_HOME"/scheduler.log "$OUT/scheduler.log" || true
exit "$status"
