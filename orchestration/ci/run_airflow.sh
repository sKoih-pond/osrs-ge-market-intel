#!/usr/bin/env bash
# Airflow 3 on a throwaway GitHub runner, for end-to-end runs of the history DAG on real data
# (ADR-0004, amended). Called by the `history` workflow's `airflow` step from the repository
# root, after the project and engine environments and Airflow (install_airflow.sh) are in
# place.
#
# A throwaway Postgres (preinstalled on the image) holds Airflow's metadata, the LocalExecutor
# runs the tasks, and every visitor is an admin: the API server listens on localhost only and
# the runner is gone when the job ends. The tasks call the project's `ge-intel` by path.
#
# Two runs: the full range, then the same range again, which must find every month recorded
# complete in storage and add no run record at all (the contract's idempotency, from Airflow).
set -euo pipefail

REPO=$(pwd)
OUT="$REPO/data/evidence/airflow"
VENV="$RUNNER_TEMP/airflow-venv"
export AIRFLOW_HOME="$RUNNER_TEMP/airflow"
mkdir -p "$OUT" "$AIRFLOW_HOME"
# Keep the logs whatever happens: the task logs hold ge-intel's own JSON lines (counts, keys,
# tracebacks), no prices and no credentials.
trap 'mkdir -p "$OUT/logs"; cp -r "$AIRFLOW_HOME/logs" "$OUT/logs/tasks" 2>/dev/null; cp "$AIRFLOW_HOME"/*.log "$OUT/logs/" 2>/dev/null; true' EXIT

# Credentials go to a file that only the tasks needing them read (the cost guard, the pull and
# the R2 check): never into Airflow's own processes, its metadata database or other tasks.
export GE_SECRETS="$RUNNER_TEMP/ge-secrets.env"
(
  umask 077
  for name in CLOUDFLARE_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY CLOUDFLARE_ANALYTICS_TOKEN; do
    printf '%s=%q\n' "$name" "${!name:-}"
  done > "$GE_SECRETS"
)
unset R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY CLOUDFLARE_ANALYTICS_TOKEN

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
# End each task process, and the cost guard, once its work is committed: see exit_hard in
# src/ge_intel/cli.py.
export GE_EXIT_HARD=1

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

# The records the tasks wrote: completion lives in storage, not only in Airflow. The second
# run must have added none.
"$REPO/.venv/bin/python" - "$GE_LAKE_ROOT" "$OUT" <<'EOF' || status=1
import json, os, sys
from datetime import datetime
from pathlib import Path
from deltalake import DeltaTable
lake, out = sys.argv[1], Path(sys.argv[2])
rows = DeltaTable(f"{lake}/ops/task_runs").to_pyarrow_table().to_pylist()
(out / "task_runs.json").write_text(json.dumps(rows, default=str, indent=1))
second = json.loads((out / "run2_run.json").read_text())["start_date"]
since = datetime.fromisoformat(second.replace("Z", "+00:00"))
added = [r for r in rows if r["started_at"] >= since]
(out / "run2_new_task_records.json").write_text(json.dumps({"run2_start": second, "records_added": len(added)}))
print(json.dumps({"task_records": len(rows), "added_by_run2": len(added)}), flush=True)
os._exit(1 if added else 0)  # after a deltalake read: see exit_hard in src/ge_intel/cli.py
EOF
exit "$status"
