"""Shared fixtures: recorded Wiki responses served by URL, a frozen clock, a local lake."""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ge_intel.cost_guard import RunBudget
from ge_intel.ingest import Context
from ge_intel.lake import Lake
from ge_intel.objects import LocalStore
from ge_intel.wiki import WikiClient

FIXTURES = Path(__file__).parent / "fixtures" / "wiki"
# The fixtures cover the 24 hours before this hour end.
FIXTURE_END = datetime(2026, 10, 19, 22, tzinfo=UTC)
# /latest and /mapping were fetched at about 22:29 UTC, so the run "happens" just after.
FIXTURE_NOW = datetime(2026, 10, 19, 22, 30, tzinfo=UTC)


def _load(name):
    return json.loads(gzip.decompress((FIXTURES / name).read_bytes()))


class FixtureTransport:
    """Serves recorded payloads for the URLs the client builds; 404 for anything else."""

    def __init__(self, drop_buckets=()):
        self.buckets = _load("5m.json.gz")
        self.latest = _load("latest.json.gz")
        self.mapping = _load("mapping.json.gz")
        self.drop = {str(int(t.timestamp())) for t in drop_buckets}
        self.urls: list[str] = []

    def __call__(self, url, headers):
        self.urls.append(url)
        assert headers["Accept-Encoding"] == "gzip"
        path = url.split("/osrs", 1)[1]
        if path.startswith("/5m?timestamp="):
            ts = path.split("=", 1)[1]
            payload = self.buckets.get(ts)
            if payload is None or ts in self.drop:
                payload = {"data": {}, "timestamp": int(ts)}
        elif path == "/latest":
            payload = self.latest
        elif path == "/mapping":
            payload = self.mapping
        else:
            return 404, {}, b"not found"
        return 200, {"content-encoding": "gzip"}, gzip.compress(json.dumps(payload).encode())


class Clock:
    def __init__(self, now: datetime):
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def make_context(lake_root: Path, transport=None, now=FIXTURE_NOW, max_buckets=288) -> Context:
    clock = Clock(now)
    client = WikiClient(transport=transport or FixtureTransport(), clock=clock, min_interval_s=0)
    lake = Lake(root=str(lake_root), store=LocalStore(lake_root))
    return Context(lake=lake, client=client, now=clock, run_id="test-run", max_buckets=max_buckets)


@pytest.fixture
def lake_root(tmp_path):
    return tmp_path / "lake"


@pytest.fixture
def budget():
    return RunBudget()
