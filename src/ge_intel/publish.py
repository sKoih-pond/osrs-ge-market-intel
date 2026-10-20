"""`publish`: the v1 data contract, validated, shipped as Worker static assets (ADR-0012, 0015).

Builds `site/dist/` from `site/src/` plus two data files, `ge/data/v1/snapshot.json` and
`ge/data/v1/status.json`, validates both against `contracts/v1/*.schema.json`, and, when
`GE_DEPLOY=1`, deploys the assets-only Worker with Wrangler. Only a deployed publish is
recorded as `success`, because the freshness objective (AT4) is about what visitors can see;
a build without a deploy is recorded as `built`.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from ge_intel import ops, tax
from ge_intel.build import SNAPSHOT
from ge_intel.ingest import BUCKETS, Context
from ge_intel.policies import CASUAL_V1, DATA_GRADE_V1
from ge_intel.reference import SEEDS
from ge_intel.timegrid import iso

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "site"
CONTRACTS = ROOT / "contracts" / "v1"
CONTRACT_VERSION = "1.0.0"
MAX_IDEAS = 100
STALE_AFTER_MINUTES = 120  # AT3: every page warns once data is more than 2 hours old
ATTRIBUTION = (
    "Prices: OSRS Wiki real-time prices API (RuneLite data before 30 September 2026, Jagex "
    "data after). OSRS Wiki content is licensed CC BY-NC-SA 3.0. Old School RuneScape is a "
    "trademark of Jagex Ltd; this non-commercial project is not affiliated with Jagex."
)
LICENCE = {"name": "CC BY-NC-SA 3.0", "url": "https://creativecommons.org/licenses/by-nc-sa/3.0/"}


class ContractViolation(ValueError):
    pass


def validate(name: str, document: dict) -> None:
    schema = json.loads((CONTRACTS / f"{name}.schema.json").read_text())
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(document),
        key=lambda e: list(e.path),
    )
    if errors:
        detail = "; ".join(
            f"{'/'.join(map(str, e.path)) or '(root)'}: {e.message}" for e in errors[:5]
        )
        raise ContractViolation(f"{name}.json breaks the v1 contract: {detail}")


def _utc(value: datetime | None) -> str | None:
    return None if value is None else iso(value)


def snapshot_document(rows: list[dict], end: datetime, generated_at: datetime) -> dict:
    ideas = sorted((r for r in rows if r["is_idea"]), key=lambda r: r["idea_rank"])
    grades = [r["data_grade"] for r in rows]
    exempt_count = sum(1 for _ in (SEEDS / "tax_exemptions.csv").open()) - 1
    era_rate = tax.tax_rate_bp(end)
    return {
        "contract": "ge-intel/v1/snapshot",
        "contract_version": CONTRACT_VERSION,
        "generated_at": iso(generated_at),
        "data_as_of": iso(end),
        "policy": CASUAL_V1.as_vars(),
        "tax": {
            "rate_percent": era_rate / 100,
            "cap_gp_per_item": tax.TAX_CAP_PER_ITEM,
            "exempt_item_count": exempt_count,
            "rule": "Seller pays floor(price x rate) per item, capped per item; exempt pay 0.",
        },
        "data_grade": {
            "version": DATA_GRADE_V1.version,
            "thresholds": {k: v for k, v in DATA_GRADE_V1.as_vars().items() if k != "version"},
        },
        "regime_id": rows[0]["regime_id"] if rows else "unknown",
        "counts": {
            "items_priced": sum(
                1 for r in rows if r["buy_price"] is not None and r["sell_price"] is not None
            ),
            "ideas": len(ideas),
            "ideas_published": min(len(ideas), MAX_IDEAS),
            "grade_a": grades.count("A"),
            "grade_b": grades.count("B"),
            "grade_c": grades.count("C"),
            "withheld_grade_c_with_margin": sum(
                1 for r in rows if r["data_grade"] == "C" and (r["net_gp"] or 0) > 0
            ),
        },
        "ideas": [
            {
                "rank": r["idea_rank"],
                "item_id": r["item_id"],
                "name": r["name"] or f"Item {r['item_id']}",
                "members": r["members"],
                "buy_price": r["buy_price"],
                "sell_price": r["sell_price"],
                "tax_per_item": r["tax_per_item"],
                "tax_exempt": r["tax_exempt"],
                "net_margin": r["net_margin"],
                "feasible_quantity": r["feasible_quantity"],
                "quantity_limited_by": r["quantity_limited_by"],
                "buy_limit": r["buy_limit"],
                "net_gp": r["net_gp"],
                "data_grade": r["data_grade"],
                "data_grade_reason": r["data_grade_reason"],
                "two_sided_hours_24h": r["two_sided_hours_24h"],
                "instant_buy_volume_1h": r["instant_buy_volume_1h"],
                "instant_sell_volume_1h": r["instant_sell_volume_1h"],
                "instant_sell_volume_entry": r["instant_sell_volume_entry"],
                "minutes_since_trade": r["minutes_since_trade"],
                "last_instant_buy_at": _utc(r["last_instant_buy_at"]),
                "last_instant_sell_at": _utc(r["last_instant_sell_at"]),
                "price_window_start": iso(r["price_window_start"]),
                "price_window_end": iso(r["price_window_end"]),
            }
            for r in ideas[:MAX_IDEAS]
        ],
        "attribution": ATTRIBUTION,
        "licence": LICENCE,
    }


def status_document(ctx: Context, end: datetime, generated_at: datetime) -> dict:
    runs = ops.runs(ctx.lake, ctx.cache)
    manifest = ctx.lake.read(BUCKETS, columns=["bucket_start", "status"])
    figures = ops.status_figures(
        manifest.to_pylist() if manifest is not None else [],
        [r for r in runs if r["task"] == "publish"],
        end,
        generated_at,
    )
    return {
        "contract": "ge-intel/v1/status",
        "contract_version": CONTRACT_VERSION,
        "generated_at": iso(generated_at),
        "data_as_of": iso(end),
        "stale_after_minutes": STALE_AFTER_MINUTES,
        "objective": {"on_time_minutes": 45, "on_time_share_target": 0.95},
        "last_successful_run": ops.describe(ops.last_run(runs, "hourly")),
        **figures,
        "attribution": ATTRIBUTION,
    }


def build_dist(snapshot: dict, status: dict, dist: Path) -> Path:
    if dist.exists():
        shutil.rmtree(dist)
    shutil.copytree(SITE / "src", dist)
    data = dist / "ge" / "data" / "v1"
    (data / "schema").mkdir(parents=True)
    for name, doc in (("snapshot", snapshot), ("status", status)):
        (data / f"{name}.json").write_text(json.dumps(doc, separators=(",", ":")) + "\n")
        shutil.copy(CONTRACTS / f"{name}.schema.json", data / "schema" / f"{name}.schema.json")
    return dist


def deploy(dist: Path) -> None:
    env = {**os.environ, "WRANGLER_SEND_METRICS": "false"}
    for name in ("CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID"):
        if not env.get(name):
            raise RuntimeError(f"{name} must be set to deploy")
    subprocess.run(
        ["npx", "--no-install", "wrangler", "deploy", "--config", str(SITE / "wrangler.jsonc")],
        cwd=SITE,
        env=env,
        check=True,
        timeout=300,
    )


def publish(ctx: Context, start: datetime, end: datetime) -> dict:
    table = ctx.lake.read(SNAPSHOT, filters=[("snapshot_date", "=", end.date())])
    rows = [r for r in (table.to_pylist() if table is not None else []) if r["snapshot_at"] == end]
    if not rows:
        raise RuntimeError(f"no gold snapshot for {iso(end)}; build_snapshot must run first")
    generated_at = ctx.now()
    snapshot = snapshot_document(rows, end, generated_at)
    status = status_document(ctx, end, generated_at)
    validate("snapshot", snapshot)
    validate("status", status)
    dist = build_dist(snapshot, status, Path(os.environ.get("GE_DIST_DIR", SITE / "dist")))
    deployed = os.environ.get("GE_DEPLOY") == "1"
    if deployed:
        deploy(dist)
    return {
        "status": "success" if deployed else "built",
        "deployed": deployed,
        "ideas_published": snapshot["counts"]["ideas_published"],
        "dist": str(dist),
    }
