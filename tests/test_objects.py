from datetime import UTC, datetime

import pytest

from ge_intel.cost_guard import BudgetExceeded, RunBudget
from ge_intel.objects import LocalStore, R2Store, sign_v4

# AWS documentation examples for Signature Version 4 with S3 (example credentials).
KID = "AKIAIOSFODNN7EXAMPLE"
SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
WHEN = datetime(2013, 5, 24, tzinfo=UTC)
EMPTY_SHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def test_sigv4_matches_aws_get_object_example():
    headers = {
        "Host": "examplebucket.s3.amazonaws.com",
        "Range": "bytes=0-9",
        "x-amz-content-sha256": EMPTY_SHA,
        "x-amz-date": "20130524T000000Z",
    }
    auth = sign_v4(
        "GET",
        "https://examplebucket.s3.amazonaws.com/test.txt",
        headers,
        EMPTY_SHA,
        KID,
        SECRET,
        "us-east-1",
        WHEN,
    )
    assert auth == (
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
        "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,"
        "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
    )


def test_sigv4_matches_aws_put_object_example():
    body_sha = "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
    headers = {
        "Host": "examplebucket.s3.amazonaws.com",
        "Date": "Fri, 24 May 2013 00:00:00 GMT",
        "x-amz-date": "20130524T000000Z",
        "x-amz-storage-class": "REDUCED_REDUNDANCY",
        "x-amz-content-sha256": body_sha,
    }
    auth = sign_v4(
        "PUT",
        "https://examplebucket.s3.amazonaws.com/test$file.text",
        headers,
        body_sha,
        KID,
        SECRET,
        "us-east-1",
        WHEN,
    )
    assert auth.endswith(
        "Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
    )


class FakeR2:
    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.calls = []

    def __call__(self, method, url, headers, body):
        self.calls.append((method, url, headers, body))
        return self.statuses.pop(0), b""


def store(send, budget=None):
    return R2Store("acct", "ge-intel-lake", KID, SECRET, budget or RunBudget(), send=send)


def test_put_is_conditional_and_never_sets_a_storage_class():
    send = FakeR2([200])
    assert store(send).put_if_absent("bronze/x=1/a.json.gz", b"abc", "application/gzip", {})
    method, url, headers, _ = send.calls[0]
    assert method == "PUT"
    assert url == "https://acct.r2.cloudflarestorage.com/ge-intel-lake/bronze/x%3D1/a.json.gz"
    assert headers["if-none-match"] == "*"
    assert not any("storage-class" in h for h in headers)


def test_encoded_and_raw_paths_sign_identically():
    args = ({"host": "h", "x-amz-date": "20130524T000000Z"}, EMPTY_SHA, KID, SECRET, "auto", WHEN)
    raw = sign_v4("PUT", "https://h/b/bronze/date=2026-10-19/a.json.gz", *args)
    encoded = sign_v4("PUT", "https://h/b/bronze/date%3D2026-10-19/a.json.gz", *args)
    assert raw == encoded


def test_existing_object_is_not_rewritten():
    assert store(FakeR2([412])).put_if_absent("k", b"abc", "application/gzip", {}) is False


def test_every_put_is_charged_before_it_is_sent():
    budget = RunBudget(max_class_a=1)
    send = FakeR2([200, 200])
    s = store(send, budget)
    s.put_if_absent("a", b"x", "application/gzip", {})
    with pytest.raises(BudgetExceeded):
        s.put_if_absent("b", b"x", "application/gzip", {})
    assert len(send.calls) == 1


def test_local_store_writes_once(tmp_path):
    s = LocalStore(tmp_path)
    assert s.put_if_absent("bronze/a.gz", b"1", "application/gzip", {})
    assert not s.put_if_absent("bronze/a.gz", b"2", "application/gzip", {})
    assert (tmp_path / "bronze/a.gz").read_bytes() == b"1"
