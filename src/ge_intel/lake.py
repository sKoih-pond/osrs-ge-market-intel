"""Delta tables on R2 (or a local folder), written only through delta-rs (ADR-0003, spike S1).

Rules carried from S1: one commit per table per run, a checkpoint after every commit,
`aws_conditional_put: etag` set explicitly, tables created by delta-rs with the protocol
baseline reader 1 / writer 2 and no table features or properties.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, write_deltalake

from ge_intel.objects import LocalStore, ObjectStore, R2Store

BUCKET = "ge-intel-lake"


def r2_storage_options(account_id: str, key_id: str, secret: str) -> dict[str, str]:
    return {
        "AWS_ENDPOINT_URL": f"https://{account_id}.r2.cloudflarestorage.com",
        "AWS_REGION": "auto",
        "AWS_ACCESS_KEY_ID": key_id,
        "AWS_SECRET_ACCESS_KEY": secret,
        "aws_conditional_put": "etag",
    }


@dataclass
class Lake:
    root: str  # "s3://ge-intel-lake" or a local directory
    store: ObjectStore
    storage_options: dict[str, str] | None = None
    commits: list[dict] = field(default_factory=list)  # (table, version) written this run

    @classmethod
    def from_env(cls, budget) -> Lake:
        root = os.environ.get("GE_LAKE_ROOT", f"s3://{BUCKET}")
        if not root.startswith("s3://"):
            return cls(root=root.rstrip("/"), store=LocalStore(root))
        account = os.environ["CLOUDFLARE_ACCOUNT_ID"]
        kid = os.environ["R2_ACCESS_KEY_ID"]
        secret = os.environ["R2_SECRET_ACCESS_KEY"]
        bucket = root.removeprefix("s3://").split("/", 1)[0]
        return cls(
            root=root.rstrip("/"),
            store=R2Store(account, bucket, kid, secret, budget),
            storage_options=r2_storage_options(account, kid, secret),
        )

    def uri(self, table: str) -> str:
        return f"{self.root}/{table}"

    def table(self, name: str) -> DeltaTable | None:
        uri = self.uri(name)
        if not DeltaTable.is_deltatable(uri, storage_options=self.storage_options):
            return None
        return DeltaTable(uri, storage_options=self.storage_options)

    def read(self, name: str, columns=None, filters=None) -> pa.Table | None:
        dt = self.table(name)
        if dt is None:
            return None
        return dt.to_pyarrow_table(columns=columns, filters=filters)

    def write(
        self,
        name: str,
        data: pa.Table,
        *,
        predicate: str | None = None,
        partition_by: list[str] | None = None,
        metadata: Mapping[str, str] | None = None,
        mode: str = "overwrite",
    ) -> int:
        """One commit: replace rows matching `predicate` (or the whole table) with `data`."""
        dt = self.table(name)
        target = dt if dt is not None else self.uri(name)
        write_deltalake(
            target,
            data,
            mode=mode,
            predicate=predicate if dt is not None else None,
            partition_by=partition_by,
            storage_options=self.storage_options,
            commit_properties=CommitProperties(custom_metadata=dict(metadata or {})),
        )
        dt = DeltaTable(self.uri(name), storage_options=self.storage_options)
        dt.create_checkpoint()
        version = dt.version()
        self.commits.append({"table": name, "version": version, "rows": data.num_rows})
        return version


def content_hash(table: pa.Table | None, sort_keys: list[str]) -> str:
    """Order-independent SHA-256 of a table's rows, for idempotency and parity checks."""
    if table is None:
        return hashlib.sha256(b"").hexdigest()
    rows = table.sort_by([(k, "ascending") for k in sort_keys]).to_pylist()
    return hashlib.sha256(json.dumps(rows, default=str, sort_keys=True).encode()).hexdigest()


def in_list(column: str, values) -> str:
    """SQL `IN` predicate over string-literal values (dates, timestamps)."""
    quoted = ", ".join(f"'{v}'" for v in values)
    return f"{column} IN ({quoted})"
