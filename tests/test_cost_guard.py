from datetime import UTC, datetime, timedelta

import pytest

from ge_intel import cost_guard as cg

MID_MONTH = datetime(2026, 10, 16, 0, 0, tzinfo=UTC)  # 15 days into a 31-day month


def usage(a=0, b=0, gb=0.0, as_of=MID_MONTH):
    return cg.Usage(class_a=a, class_b=b, storage_bytes=int(gb * 1e9), as_of=as_of)


def test_operation_classes_follow_cloudflare_pricing():
    assert cg.op_class("PutObject") == "A"
    assert cg.op_class("ListObjectsV2") == "A"
    assert cg.op_class("GetObject") == "B"
    assert cg.op_class("HeadObject") == "B"
    assert cg.op_class("DeleteObject") == "free"
    assert cg.op_class("AbortMultipartUpload") == "free"


def test_unknown_operations_count_as_class_a():
    assert cg.op_class("SomeNewOperation") == "A"


def test_low_usage_is_allowed():
    verdict = cg.evaluate(usage(a=40_000, b=150_000, gb=2.0))
    assert verdict.allowed and not verdict.reasons and not verdict.warnings


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"a": 500_000}, "Class A"),
        ({"b": 5_000_000}, "Class B"),
        ({"gb": 6.0}, "storage"),
    ],
)
def test_halts_at_thresholds(kwargs, fragment):
    verdict = cg.evaluate(usage(**kwargs))
    assert not verdict.allowed
    assert any(fragment in r for r in verdict.reasons)


def test_just_below_thresholds_is_allowed():
    assert cg.evaluate(usage(a=499_999, b=4_999_999, gb=5.999)).allowed


def test_projection_warns_without_halting():
    # 300k Class A after 15 of 31 days projects to about 620k by month end.
    verdict = cg.evaluate(usage(a=300_000))
    assert verdict.allowed
    assert any("Class A projected" in w for w in verdict.warnings)


def test_no_projection_in_first_72_hours():
    early = datetime(2026, 10, 2, 0, 0, tzinfo=UTC)
    assert cg.evaluate(usage(a=100_000, as_of=early)).warnings == ()


def test_run_budget_refuses_before_crossing_the_cap():
    budget = cg.RunBudget(max_class_a=2, max_class_b=1, max_bytes_written=100)
    budget.charge("PutObject", bytes_written=60)
    budget.charge("DeleteObject")  # free, never counted
    budget.charge("GetObject")
    with pytest.raises(cg.BudgetExceeded):
        budget.charge("GetObject")  # would be the second Class B
    with pytest.raises(cg.BudgetExceeded):
        budget.charge("PutObject", bytes_written=50)  # would write 110 bytes
    assert (budget.class_a, budget.class_b, budget.bytes_written) == (1, 1, 60)


def test_default_run_caps_bound_a_month_below_halt_thresholds():
    runs_per_month = 31 * 24
    budget = cg.RunBudget()
    assert budget.max_class_a * runs_per_month < cg.FREE_CLASS_A * cg.HALT_FRACTION
    assert budget.max_class_b * runs_per_month < cg.FREE_CLASS_B * cg.HALT_FRACTION


def _payload(ops, storage):
    return {"data": {"viewer": {"accounts": [{"ops": ops, "storage": storage}]}}, "errors": None}


def test_parse_usage_sums_classes_and_takes_latest_storage_per_bucket():
    ops = [
        {"sum": {"requests": 120}, "dimensions": {"actionType": "PutObject", "bucketName": "x"}},
        {"sum": {"requests": 30}, "dimensions": {"actionType": "ListObjects", "bucketName": "x"}},
        {"sum": {"requests": 900}, "dimensions": {"actionType": "GetObject", "bucketName": "x"}},
        {"sum": {"requests": 50}, "dimensions": {"actionType": "DeleteObject", "bucketName": "x"}},
    ]
    storage = [  # newest first, as the query orders it
        {"max": {"payloadSize": 2_000, "metadataSize": 10}, "dimensions": {"bucketName": "x"}},
        {"max": {"payloadSize": 1_000, "metadataSize": 10}, "dimensions": {"bucketName": "x"}},
        {"max": {"payloadSize": 500, "metadataSize": 5}, "dimensions": {"bucketName": "y"}},
    ]
    u = cg.parse_usage(_payload(ops, storage), as_of=MID_MONTH)
    assert (u.class_a, u.class_b, u.storage_bytes) == (150, 900, 2_010 + 505)


def test_parse_usage_accepts_an_empty_account():
    u = cg.parse_usage(_payload([], []), as_of=MID_MONTH)
    assert (u.class_a, u.class_b, u.storage_bytes) == (0, 0, 0)


def test_parse_usage_fails_closed_on_errors():
    with pytest.raises(RuntimeError):
        cg.parse_usage({"data": None, "errors": [{"message": "not authorised"}]}, MID_MONTH)


def test_fetch_usage_queries_from_month_start():
    seen = {}

    def fake_post(url, body, headers):
        seen.update(url=url, variables=body["variables"], auth=headers["Authorization"])
        return _payload([], [])

    cg.fetch_usage("acct123", "tok", now=MID_MONTH, post_json=fake_post)
    assert seen["url"] == cg.GRAPHQL_URL
    assert seen["auth"] == "Bearer tok"
    assert seen["variables"]["start"] == "2026-10-01T00:00:00+00:00"
    assert seen["variables"]["acct"] == "acct123"


def test_cli_fails_closed_without_credentials(monkeypatch):
    monkeypatch.delenv("CLOUDFLARE_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("CLOUDFLARE_ANALYTICS_TOKEN", raising=False)
    assert cg.main() == 2


PREV = cg.PreviousRun(
    "gha-1-1", datetime(2026, 10, 16, 0, 5, tzinfo=UTC), datetime(2026, 10, 16, 0, 9, tzinfo=UTC)
)
LATER = datetime(2026, 10, 16, 1, 5, tzinfo=UTC)


def _with_prev(prev_ops):
    payload = _payload([], [])
    payload["data"]["viewer"]["accounts"][0]["prev"] = prev_ops
    return payload


def _ops(bucket, action, n):
    return {"sum": {"requests": n}, "dimensions": {"actionType": action, "bucketName": bucket}}


def test_previous_window_counts_only_the_project_bucket():
    payload = _with_prev(
        [
            _ops("ge-intel-lake", "PutObject", 90),
            _ops("ge-intel-lake", "GetObject", 400),
            _ops("other", "PutObject", 10_000),
        ]
    )
    u = cg.parse_usage(payload, as_of=LATER, bucket="ge-intel-lake")
    assert u.previous_window == (90, 400)


def test_previous_run_over_its_caps_blocks_until_acknowledged():
    usage = cg.Usage(0, 0, 0, LATER, previous_window=(501, 100))
    verdict = cg.check_previous_run(usage, PREV)
    assert not verdict.allowed and "GE_GUARD_ACK_RUN_ID=gha-1-1" in verdict.reasons[0]
    assert cg.check_previous_run(usage, PREV, acknowledged_run_id="gha-1-1").allowed
    assert not cg.check_previous_run(usage, PREV, acknowledged_run_id="gha-9-1").allowed


def test_previous_run_within_caps_is_allowed():
    usage = cg.Usage(0, 0, 0, LATER, previous_window=(120, 900))
    assert cg.check_previous_run(usage, PREV) == cg.Verdict(True, (), ())


def test_previous_run_that_just_ended_warns_that_counts_may_be_low():
    usage = cg.Usage(0, 0, 0, PREV.finished_at + timedelta(minutes=2), previous_window=(10, 10))
    verdict = cg.check_previous_run(usage, PREV)
    assert verdict.allowed and verdict.warnings


def test_one_query_carries_the_previous_window():
    seen = {}

    def fake_post(url, body, headers):
        seen.update(body["variables"])
        return _with_prev([])

    cg.fetch_usage("acct", "tok", now=LATER, post_json=fake_post, previous=PREV)
    assert seen["prevStart"] == "2026-10-16T00:04:00+00:00"
    assert seen["prevEnd"] == "2026-10-16T00:11:00+00:00"
