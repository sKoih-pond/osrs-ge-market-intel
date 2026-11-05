"""Drive one run of a DAG through Airflow 3's REST API and keep its evidence (stdlib only).

    python drive.py --dag ge_history_reprocess --label run1 --out data/evidence/airflow

Waits for the DAG to be parsed, triggers it (its parameters as the run's conf), waits for the
run to finish, then writes the run and every task instance (state, tries, start, end,
duration) as JSON, a per-task summary, and a screenshot of the run's grid view taken with the
runner's headless Chrome. Exit code 0 only when the run succeeded.
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path

BASE = "http://localhost:8080"


def call(method: str, path: str, token: str | None = None, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{BASE}{path}", data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read() or b"{}")


def wait_for(what: str, check, timeout: float, every: float = 5):
    deadline = time.monotonic() + timeout
    while True:
        try:
            found = check()
            if found:
                return found
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f"gave up waiting for {what}")
        time.sleep(every)


def token() -> str:
    """With every user an admin (simple auth manager, a throwaway runner), a token needs no
    credentials."""
    return wait_for("the API server", lambda: call("GET", "/auth/token")["access_token"], 300)


def task_instances(tok: str, dag: str, run: str) -> list[dict]:
    out, offset = [], 0
    while True:
        page = call(
            "GET", f"/api/v2/dags/{dag}/dagRuns/{run}/taskInstances?limit=100&offset={offset}", tok
        )
        out += page["task_instances"]
        offset += 100
        if offset >= page["total_entries"]:
            return out


def summarise(tis: list[dict]) -> dict:
    """Per task: instances, states, tries beyond the first (retries), and durations."""
    by = defaultdict(list)
    for t in tis:
        by[t["task_id"]].append(t)
    out = {}
    for task_id, items in sorted(by.items()):
        durations = [t["duration"] for t in items if t.get("duration") is not None]
        states = defaultdict(int)
        for t in items:
            states[t["state"]] += 1
        out[task_id] = {
            "instances": len(items),
            "states": dict(states),
            "retried": sum(1 for t in items if (t.get("try_number") or 1) > 1),
            "duration_s": {
                "total": round(sum(durations), 1),
                "median": round(statistics.median(durations), 1) if durations else None,
                "max": round(max(durations), 1) if durations else None,
            },
        }
    return out


def screenshot(url: str, target: Path) -> bool:
    """Optional: a hung or missing Chrome never fails a run that succeeded."""
    chrome = shutil.which("google-chrome") or shutil.which("chromium")
    if not chrome:
        return False
    try:
        subprocess.run(
            [
                chrome,
                "--headless=new",
                "--no-sandbox",
                "--disable-gpu",
                "--hide-scrollbars",
                "--window-size=1920,1500",
                "--virtual-time-budget=30000",
                f"--screenshot={target}",
                url,
            ],
            check=False,
            timeout=180,
            capture_output=True,
        )
    except subprocess.TimeoutExpired:
        return False
    return target.exists() and target.stat().st_size > 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dag", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--conf", default="{}", help="the run's conf (its parameters), as JSON")
    ap.add_argument("--timeout", type=float, default=3 * 3600)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    tok = token()
    try:
        wait_for(
            f"DAG {args.dag} to be parsed",
            lambda: call("GET", f"/api/v2/dags/{args.dag}", tok).get("dag_id"),
            600,
        )
    except TimeoutError:
        errors = call("GET", "/api/v2/importErrors", token())
        (args.out / f"{args.label}_import_errors.json").write_text(json.dumps(errors, indent=2))
        print(json.dumps({"event": "not_parsed", "import_errors": errors}), flush=True)
        return 1
    run = call(
        "POST",
        f"/api/v2/dags/{args.dag}/dagRuns",
        tok,
        {
            "dag_run_id": f"{args.label}-{int(time.time())}",
            "logical_date": None,
            "conf": json.loads(args.conf),
        },
    )
    run_id = run["dag_run_id"]
    print(json.dumps({"event": "triggered", "dag": args.dag, "run_id": run_id}), flush=True)
    path = f"/api/v2/dags/{args.dag}/dagRuns/{run_id}"

    def finished():
        tok_now = token()
        state = call("GET", path, tok_now)
        return state if state["state"] in ("success", "failed") else None

    try:
        final = wait_for(f"run {run_id} to finish", finished, args.timeout, every=20)
    except TimeoutError:
        final = call("GET", path, token())  # keep what there is: the run and its tasks so far
    tok = token()
    tis = task_instances(tok, args.dag, run_id)
    summary = summarise(tis)
    (args.out / f"{args.label}_run.json").write_text(json.dumps(final, indent=2, sort_keys=True))
    (args.out / f"{args.label}_task_instances.json").write_text(
        json.dumps(tis, indent=2, sort_keys=True)
    )
    report = {
        "run_id": run_id,
        "state": final["state"],
        "start_date": final.get("start_date"),
        "end_date": final.get("end_date"),
        "duration_s": final.get("duration"),
        "task_instances": len(tis),
        "tasks": summary,
    }
    (args.out / f"{args.label}_summary.json").write_text(json.dumps(report, indent=2))
    shot = screenshot(f"{BASE}/dags/{args.dag}/runs/{run_id}", args.out / f"{args.label}_grid.png")
    report["screenshot"] = shot
    (args.out / f"{args.label}_summary.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({"event": "finished", **report}), flush=True)
    return 0 if final["state"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
