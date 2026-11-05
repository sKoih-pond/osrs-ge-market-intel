#!/usr/bin/env bash
# Airflow 3.3.2 in its own environment, from its constraints file (it pins pyarrow and
# importlib-metadata differently from the project's lock). Its own workflow step, with no
# secrets in reach (ADR-0004, amended).
set -euo pipefail

AIRFLOW_VERSION=3.3.2
VENV="$RUNNER_TEMP/airflow-venv"
uv venv -q "$VENV" --python 3.12
uv pip install -q --python "$VENV/bin/python" "apache-airflow[postgres]==$AIRFLOW_VERSION" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-3.12.txt"
"$VENV/bin/airflow" version
