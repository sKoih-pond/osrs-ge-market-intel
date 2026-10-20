"""Wiki client etiquette and the response contract, against recorded payloads."""

import gzip
import json
from datetime import UTC, datetime, timedelta

import pytest
from conftest import FIXTURE_END, FIXTURE_NOW, FixtureTransport, _load

from ge_intel import wiki


@pytest.mark.parametrize(
    "agent",
    [
        "",
        None,
        "python-requests/2.32",
        "Python-urllib/3.12",
        "curl/8.4",
        "Mozilla/5.0 (X11)",
        "my-scraper/1.0",
    ],
)
def test_refuses_default_or_anonymous_user_agents(agent):
    with pytest.raises(ValueError):
        wiki.check_user_agent(agent)


def test_project_user_agent_names_repo_and_contact():
    assert "github.com/sKoih-pond/osrs-ge-market-intel" in wiki.USER_AGENT
    assert "sylvester@kohstack.au" in wiki.USER_AGENT
    assert wiki.check_user_agent(wiki.USER_AGENT) == wiki.USER_AGENT


def client(transport, now=FIXTURE_NOW):
    return wiki.WikiClient(transport=transport, clock=lambda: now, min_interval_s=0)


def test_requests_carry_the_agent_and_gzip_and_explicit_timestamps():
    seen = []

    def transport(url, headers):
        seen.append((url, headers))
        body = json.dumps({"data": {}, "timestamp": 1792446900}).encode()
        return 200, {"content-encoding": "gzip"}, gzip.compress(body)

    client(transport).bucket("5m", datetime(2026, 10, 19, 21, 55, tzinfo=UTC))
    url, headers = seen[0]
    assert url == "https://prices.runescape.wiki/api/v2/osrs/5m?timestamp=1792446900"
    assert headers["User-Agent"] == wiki.USER_AGENT
    assert headers["Accept-Encoding"] == "gzip"


def test_refuses_open_misaligned_or_naive_buckets():
    c = client(FixtureTransport())
    with pytest.raises(ValueError, match="not closed"):
        c.bucket("5m", FIXTURE_NOW - timedelta(minutes=5))  # closes at 22:30, not settled
    with pytest.raises(ValueError, match="aligned"):
        c.bucket("5m", datetime(2026, 10, 19, 21, 57, tzinfo=UTC))
    with pytest.raises(ValueError, match="timezone"):
        c.bucket("5m", datetime(2026, 10, 19, 21, 55))


def test_retries_server_errors_then_succeeds():
    calls = []

    def flaky(url, headers):
        calls.append(url)
        if len(calls) < 3:
            return 503, {}, b""
        return 200, {}, b'{"data": {}}'

    c = wiki.WikiClient(
        transport=flaky, clock=lambda: FIXTURE_NOW, min_interval_s=0, sleep=lambda s: None
    )
    assert c.latest().json() == {"data": {}}
    assert len(calls) == 3


def test_client_errors_are_not_retried():
    calls = []

    def missing(url, headers):
        calls.append(url)
        return 404, {}, b""

    with pytest.raises(RuntimeError, match="404"):
        wiki.WikiClient(transport=missing, clock=lambda: FIXTURE_NOW, min_interval_s=0).mapping()
    assert len(calls) == 1


def test_throttles_to_one_request_per_second():
    slept = []
    c = wiki.WikiClient(transport=FixtureTransport(), clock=lambda: FIXTURE_NOW, sleep=slept.append)
    c.latest()
    c.latest()
    assert slept and 0 < slept[0] <= 1.0


# ---- contract: the recorded payloads parse, and shape changes fail loudly


def test_recorded_bucket_payloads_parse():
    buckets = _load("5m.json.gz")
    assert len(buckets) == 288
    for ts, payload in buckets.items():
        rows = wiki.parse_bucket(payload, datetime.fromtimestamp(int(ts), tz=UTC))
        assert all(r["high_volume"] >= 0 and r["low_volume"] >= 0 for r in rows)
        assert all(isinstance(r["item_id"], int) for r in rows)


def test_bucket_payload_for_another_timestamp_is_refused():
    payload = {"data": {}, "timestamp": int(FIXTURE_END.timestamp())}
    with pytest.raises(wiki.ContractError, match="asked for bucket"):
        wiki.parse_bucket(payload, FIXTURE_END - timedelta(minutes=5))


@pytest.mark.parametrize(
    "bad",
    [
        {"data": {"2": {"avgHighPrice": "254", "highPriceVolume": 1}}},
        {"data": {"2": {"avgHighPrice": 254, "highPriceVolume": -1}}},
        {"data": {"two": {"avgHighPrice": 254, "highPriceVolume": 1}}},
        {"data": []},
    ],
)
def test_malformed_bucket_payloads_fail(bad):
    with pytest.raises(wiki.ContractError):
        wiki.parse_bucket({**bad, "timestamp": int(FIXTURE_END.timestamp())}, FIXTURE_END)


def test_prices_above_32_bits_are_kept():
    rows = wiki.parse_bucket(
        {
            "data": {
                "1": {
                    "avgHighPrice": 3_000_000_000_000,
                    "highPriceVolume": 1,
                    "avgLowPrice": None,
                    "lowPriceVolume": 0,
                }
            },
            "timestamp": int(FIXTURE_END.timestamp()),
        },
        FIXTURE_END,
    )
    assert rows[0]["avg_high_price"] == 3_000_000_000_000


def test_recorded_latest_and_mapping_parse():
    latest = wiki.parse_latest(_load("latest.json.gz"))
    mapping = wiki.parse_mapping(_load("mapping.json.gz"))
    assert latest and mapping
    assert any(r["buy_limit"] is None for r in mapping)  # some items publish no limit
    with pytest.raises(wiki.ContractError, match="duplicate"):
        wiki.parse_mapping(_load("mapping.json.gz") * 2)
