"""Watchdog: opens a GitHub issue when published data is stale or the hourly schedule is off.

Independent of the pipeline by design (ADR-0001): standard library only, no project secrets,
no R2 access. It reads the public `status.json` and the GitHub API, opens one issue labelled
`watchdog` when something is wrong, and closes it with a comment once everything is healthy.

Environment: GITHUB_TOKEN, GITHUB_REPOSITORY, STATUS_URL, and EXPECT_LIVE ("true" once the
dashboard is deployed; before that a missing status.json is expected, not a fault).
Run as a script: `python3 src/ge_intel/watchdog.py`.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime

LABEL = "watchdog"
TITLE = "Watchdog: GE data is stale or the hourly schedule is off"
STALE_MINUTES = 120  # AT3
WORKFLOW = "hourly.yml"
AGENT = "osrs-ge-market-intel-watchdog (+https://github.com/sKoih-pond/osrs-ge-market-intel)"

# (method, url, body or None) -> (status, parsed JSON or None)
Http = Callable[[str, str, dict | None], tuple[int, object]]


def make_http(token: str | None) -> Http:
    def call(method: str, url: str, body: dict | None = None) -> tuple[int, object]:
        headers = {"User-Agent": AGENT, "Accept": "application/json"}
        if token and url.startswith("https://api.github.com/"):
            headers["Authorization"] = f"Bearer {token}"
            headers["X-GitHub-Api-Version"] = "2022-11-28"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            return exc.code, None

    return call


def data_problems(http: Http, status_url: str, expect_live: bool, now: datetime) -> list[str]:
    if not expect_live:
        # Not deployed yet (GE_DEPLOY_ENABLED is not true): /ge/ may still be answered by the
        # portfolio origin, so there is no published data to judge.
        return []
    try:
        code, status = http("GET", status_url, None)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return [f"status.json could not be read: {exc}"]
    if code != 200 or not isinstance(status, dict):
        return [f"status.json returned HTTP {code} from {status_url}"]
    try:
        as_of = datetime.fromisoformat(status["data_as_of"].replace("Z", "+00:00"))
    except (KeyError, AttributeError, ValueError):
        return ["status.json has no readable data_as_of"]
    age = (now - as_of).total_seconds() / 60
    limit = status.get("stale_after_minutes", STALE_MINUTES)
    if age > limit:
        return [
            f"published data is {age / 60:.1f} hours old (data to {status['data_as_of']}); "
            f"the page shows its staleness warning after {limit} minutes"
        ]
    return []


def schedule_problems(http: Http, repo: str) -> list[str]:
    code, wf = http(
        "GET", f"https://api.github.com/repos/{repo}/actions/workflows/{WORKFLOW}", None
    )
    if code != 200 or not isinstance(wf, dict):
        return [f"could not read the {WORKFLOW} workflow (HTTP {code})"]
    if wf.get("state") != "active":
        return [f"the {WORKFLOW} schedule is {wf.get('state')}; re-enable it in the Actions tab"]
    return []


def latest_run_note(http: Http, repo: str) -> str:
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{WORKFLOW}/runs?per_page=1"
    code, body = http("GET", url, None)
    runs = body.get("workflow_runs") if isinstance(body, dict) else None
    if code != 200 or not runs:
        return "No recent hourly run found."
    r = runs[0]
    return f"Latest hourly run: {r.get('html_url')} ({r.get('status')}, {r.get('conclusion')})."


def open_issue(http: Http, repo: str) -> dict | None:
    url = f"https://api.github.com/repos/{repo}/issues?state=open&labels={LABEL}&per_page=1"
    code, issues = http("GET", url, None)
    if code != 200:
        raise RuntimeError(f"cannot list issues (HTTP {code})")
    return issues[0] if issues else None


def main(env=os.environ, http: Http | None = None, now: datetime | None = None) -> int:
    repo = env["GITHUB_REPOSITORY"]
    http = http or make_http(env.get("GITHUB_TOKEN"))
    now = now or datetime.now(UTC)
    problems = data_problems(http, env["STATUS_URL"], env.get("EXPECT_LIVE") == "true", now)
    problems += schedule_problems(http, repo)
    issue = open_issue(http, repo)
    stamp = now.strftime("%Y-%m-%d %H:%M UTC")
    if problems and issue is None:
        body = (
            f"Checked at {stamp}.\n\n"
            + "\n".join(f"- {p}" for p in problems)
            + f"\n\n{latest_run_note(http, repo)}\n\nRunbook: docs/runbook.md. "
            "This issue closes itself once the data is fresh and the schedule is active."
        )
        code, _ = http(
            "POST",
            f"https://api.github.com/repos/{repo}/issues",
            {"title": TITLE, "body": body, "labels": [LABEL]},
        )
        if code != 201:
            raise RuntimeError(f"cannot open issue (HTTP {code})")
        action = "opened"
    elif not problems and issue is not None:
        base = f"https://api.github.com/repos/{repo}/issues/{issue['number']}"
        http("POST", f"{base}/comments", {"body": f"Healthy again at {stamp}. Closing."})
        http("PATCH", base, {"state": "closed", "state_reason": "completed"})
        action = "closed"
    else:
        action = "unchanged"
    print(json.dumps({"event": "watchdog", "at": stamp, "problems": problems, "issue": action}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
