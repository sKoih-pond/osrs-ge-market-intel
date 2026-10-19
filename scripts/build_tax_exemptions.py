"""Build the era-aware tax-exemption seed from the published lists and a /mapping snapshot.

Usage: python scripts/build_tax_exemptions.py data/raw/mapping/<ts>.json.gz

Sources (read 2026-10-20):
- 9 Dec 2021 list: Jagex news post "Grand Exchange Tax & Item Sink", as copied on the Wiki
  (https://oldschool.runescape.wiki/w/Update:Grand_Exchange_Tax_%26_Item_Sink): thirteen
  beginner tools, plus bonds.
- 29 May 2025 additions: Jagex's news post "Yama CAs & More!" names only categories ("low-tier
  food, Energy Potions, teleport tablets, early-game ammo, and Mind Runes"), so the item list
  comes from the Wiki's current "Exempt from tax" list on the Grand Exchange page
  (https://oldschool.runescape.wiki/w/Grand_Exchange#Exempt_from_tax), minus the 2021 items.

Names are matched exactly against /mapping. Names that do not match one item are resolved by
hand below and the resolution is written into the `note` column. Any unresolved name fails
the build, so a renamed item cannot silently lose its exemption.
"""

from __future__ import annotations

import csv
import gzip
import json
import sys
from pathlib import Path

OUT = Path("src/ge_intel/seeds/tax_exemptions.csv")
URL_2021 = "https://oldschool.runescape.wiki/w/Update:Grand_Exchange_Tax_%26_Item_Sink"
URL_2025 = "https://oldschool.runescape.wiki/w/Grand_Exchange#Exempt_from_tax"

LIST_2021 = [
    "Old school bond",
    "Chisel",
    "Seed dibber",
    "Gardening trowel",
    "Glassblowing pipe",
    "Hammer",
    "Needle",
    "Pestle and mortar",
    "Rake",
    "Saw",
    "Secateurs",
    "Shears",
    "Spade",
    "Watering can",
]
LIST_2025 = [
    "Energy potion(1)",
    "Energy potion(2)",
    "Energy potion(3)",
    "Energy potion(4)",
    "Bronze arrow",
    "Bronze dart",
    "Iron arrow",
    "Iron dart",
    "Mind rune",
    "Steel arrow",
    "Steel dart",
    "Bass",
    "Bread",
    "Cake",
    "Cooked chicken",
    "Cooked meat",
    "Herring",
    "Lobster",
    "Mackerel",
    "Meat pie",
    "Pike",
    "Salmon",
    "Shrimps",
    "Tuna",
    "Ardougne teleport (tablet)",
    "Camelot teleport (tablet)",
    "Civitas illa fortis teleport",
    "Falador teleport (tablet)",
    "Games necklace(8)",
    "Kourend castle teleport (tablet)",
    "Lumbridge teleport (tablet)",
    "Ring of dueling(8)",
    "Teleport to house (tablet)",
    "Varrock teleport (tablet)",
]
NOTES = {
    "Old school bond": "Bonds named separately in the 2021 post (they already pay a fee when "
    "made tradeable).",
    "Seed dibber": "Listed as 'Dibber' in the 2021 post.",
    "Watering can": "Listed as 'Watering Can (0)' in the 2021 post; /mapping names the empty "
    "can 'Watering can'. Filled cans (1 to 8) are not exempt.",
    "Energy potion(1)": "The Wiki lists 'Energy potion' without a dose; all four doses assumed.",
    "Energy potion(2)": "The Wiki lists 'Energy potion' without a dose; all four doses assumed.",
    "Energy potion(3)": "The Wiki lists 'Energy potion' without a dose; all four doses assumed.",
    "Energy potion(4)": "The Wiki lists 'Energy potion' without a dose; all four doses assumed.",
    "Civitas illa fortis teleport": "Wiki links 'Civitas illa fortis teleport (tablet)'; "
    "/mapping names it without the suffix.",
}


def main(mapping_path: str) -> None:
    raw = Path(mapping_path).read_bytes()
    mapping = json.loads(gzip.decompress(raw) if mapping_path.endswith(".gz") else raw)
    by_name: dict[str, list[int]] = {}
    for rec in mapping:
        by_name.setdefault(rec["name"], []).append(rec["id"])
    rows, problems = [], []
    for names, era_from, source in (
        (LIST_2021, "2021-12-09", URL_2021),
        (LIST_2025, "2025-05-29", URL_2025),
    ):
        for name in names:
            ids = by_name.get(name, [])
            if len(ids) != 1:
                problems.append(f"{name!r} matches {ids}")
                continue
            rows.append(
                {
                    "item_id": ids[0],
                    "item_name": name,
                    "exempt_from": era_from,
                    "exempt_to": "",
                    "source_url": source,
                    "note": NOTES.get(name, ""),
                }
            )
    if problems:
        sys.exit("unresolved names:\n" + "\n".join(problems))
    rows.sort(key=lambda r: (r["exempt_from"], r["item_id"]))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows to {OUT}")


if __name__ == "__main__":
    main(sys.argv[1])
