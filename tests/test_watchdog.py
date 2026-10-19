from datetime import UTC, datetime

from ge_intel import watchdog

NOW = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)
ENV = {
    "GITHUB_REPOSITORY": "o/r",
    "STATUS_URL": "https://example.test/status.json",
    "EXPECT_LIVE": "true",
}


class FakeGitHub:
    def __init__(
        self, data_as_of="2026-10-21T11:00:00Z", state="active", issue=None, status_code=200
    ):
        self.data_as_of, self.state, self.issue, self.status_code = (
            data_as_of,
            state,
            issue,
            status_code,
        )
        self.calls = []

    def __call__(self, method, url, body):
        self.calls.append((method, url, body))
        if url == ENV["STATUS_URL"]:
            if self.status_code != 200:
                return self.status_code, None
            return 200, {"data_as_of": self.data_as_of, "stale_after_minutes": 120}
        if url.endswith("/workflows/hourly.yml"):
            return 200, {"state": self.state}
        if "/issues?state=open" in url:
            return 200, [self.issue] if self.issue else []
        if url.endswith("/runs?per_page=1"):
            return 200, {"workflow_runs": []}
        if method == "POST" and url.endswith("/issues"):
            return 201, {"number": 7}
        return 200, {}


def posts(gh):
    return [(m, u) for m, u, _ in gh.calls if m in ("POST", "PATCH")]


def test_healthy_does_nothing():
    gh = FakeGitHub()
    watchdog.main(ENV, gh, NOW)
    assert posts(gh) == []


def test_stale_data_opens_one_issue():
    gh = FakeGitHub(data_as_of="2026-10-21T09:30:00Z")  # 2.5 hours old
    watchdog.main(ENV, gh, NOW)
    assert posts(gh) == [("POST", "https://api.github.com/repos/o/r/issues")]
    body = gh.calls[-1][2]
    assert "2.5 hours old" in body["body"] and body["labels"] == ["watchdog"]


def test_disabled_schedule_opens_an_issue_even_when_data_is_fresh():
    gh = FakeGitHub(state="disabled_inactivity")
    watchdog.main(ENV, gh, NOW)
    assert "disabled_inactivity" in gh.calls[-1][2]["body"]


def test_existing_issue_is_not_duplicated():
    gh = FakeGitHub(data_as_of="2026-10-21T08:00:00Z", issue={"number": 3})
    watchdog.main(ENV, gh, NOW)
    assert posts(gh) == []


def test_recovery_closes_the_issue():
    gh = FakeGitHub(issue={"number": 3})
    watchdog.main(ENV, gh, NOW)
    assert posts(gh) == [
        ("POST", "https://api.github.com/repos/o/r/issues/3/comments"),
        ("PATCH", "https://api.github.com/repos/o/r/issues/3"),
    ]


def test_missing_status_is_a_fault_only_once_live():
    assert watchdog.data_problems(FakeGitHub(status_code=404), ENV["STATUS_URL"], False, NOW) == []
    assert watchdog.data_problems(FakeGitHub(status_code=404), ENV["STATUS_URL"], True, NOW)
