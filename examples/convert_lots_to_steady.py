"""Convert an acyclic lots/recipes snapshot (schema v1) into steady JSON (v2).

Each lot becomes one mixing process.  Raw lot masses become external feeds,
recipe inputs become transfers, the remaining mass of every lot becomes a
product sink and every recipe loss becomes a loss sink.  Zero flows are
omitted, as required by the steady-state contract.

The conversion is deliberately a reporting aid: it produces a loop-free
steady view whose exact origin proportions must match the batch pipeline,
demonstrating that the two accounting models agree on acyclic data.

Usage:
    python examples/convert_lots_to_steady.py fixtures/lots.json out.json
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

# This script is run from the repository root and only relies on the stdlib
# plus the existing batch loader for source validation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lot_lineage.graph import load  # noqa: E402


def convert(source: str) -> dict:
    lots, recipes, consumed = load(source)
    processes = [{"process_id": lot_id} for lot_id in sorted(lots)]
    feeds = []
    for lot_id in sorted(lot_id for lot_id, lot in lots.items() if lot["kind"] == "raw"):
        feeds.append(
            {
                "feed_id": f"feed-{lot_id}",
                "origin_id": lot_id,
                "to_process": lot_id,
                "mass_g": lots[lot_id]["mass_g"],
            }
        )
    transfers = []
    for output_lot in sorted(recipes):
        for item in sorted(recipes[output_lot]["inputs"], key=lambda row: row["lot_id"]):
            transfers.append(
                {
                    "transfer_id": f"recipe-{output_lot}-{item['lot_id']}",
                    "from_process": item["lot_id"],
                    "to_process": output_lot,
                    "mass_g": item["mass_g"],
                }
            )
    sinks = []
    for lot_id in sorted(lots):
        available = lots[lot_id]["mass_g"] - consumed[lot_id]
        if available > 0:
            sinks.append(
                {
                    "sink_id": f"product-{lot_id}",
                    "from_process": lot_id,
                    "kind": "product",
                    "mass_g": available,
                }
            )
    for output_lot in sorted(recipes):
        loss = recipes[output_lot]["loss_g"]
        if loss > 0:
            sinks.append(
                {
                    "sink_id": f"loss-{output_lot}",
                    "from_process": output_lot,
                    "kind": "loss",
                    "mass_g": loss,
                }
            )
    return {
        "schema_version": 2,
        "processes": processes,
        "feeds": feeds,
        "transfers": transfers,
        "sinks": sinks,
    }


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    payload = convert(argv[1])
    Path(argv[2]).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
