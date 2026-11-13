"""Replay of a self-contained counterexample without the original seed inputs.

A counterexample artifact embeds every CSV text, every operation and the
first-difference signature.  Replay rebuilds a scenario from the artifact
alone -- no generated files, no RNG, no seed-derived state -- and re-runs it
through real subprocesses against a fresh empty library.
"""
from __future__ import annotations

from .runner import run_scenario

REQUIRED_KEYS = (
    "format", "operations", "files", "signature",
    "divergent_operation",
)


def scenario_from_counterexample(counterexample: dict) -> dict:
    for key in REQUIRED_KEYS:
        if key not in counterexample:
            raise ValueError(f"counterexample is missing required key {key!r}")
    campaigns = sorted({op["campaign"] for op in counterexample["operations"]})
    return {
        "format": "lot_lineage_diff_scenario_v1",
        "seed": counterexample.get("seed"),
        "steps": counterexample.get("steps"),
        "campaigns": campaigns,
        "files": counterexample["files"],
        "operations": counterexample["operations"],
        "coverage_tags": sorted({
            tag
            for op in counterexample["operations"]
            for tag in op.get("tags", ())
        }),
    }


def replay(counterexample: dict, prefix: list[str] | None = None) -> dict:
    scenario = scenario_from_counterexample(counterexample)
    outcome = run_scenario(scenario, prefix)
    outcome["reproduces"] = (
        outcome["divergent"]
        and outcome["signature"] == counterexample["signature"]
    )
    return outcome
