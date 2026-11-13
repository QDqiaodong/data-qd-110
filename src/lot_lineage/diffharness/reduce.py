"""Counterexample minimisation (delta debugging) on fresh empty libraries.

Reduction proceeds strictly in three passes and never reuses a database:

1. delete whole operations (including the post-divergence tail);
2. delete individual CSV data records from the files still referenced;
3. shrink the ``mass`` cells of the remaining records.

Every candidate is rebuilt into a full scenario object and executed by
:func:`lot_lineage.diffharness.runner.run_scenario` in its own brand-new
temporary directory and SQLite file.  A candidate is accepted only when it
still diverges with the *same signature* (same operation id, difference kind,
field and outcome categories).  Unreferenced CSV files are pruned after each
accepted operation deletion.

When the budget is exhausted before minimality is proven the result is
honestly marked ``complete: false``.  On completion, deleting any one
remaining operation or any one remaining CSV record no longer reproduces the
signature; this minimality claim is itself verified by fresh runs.
"""
from __future__ import annotations

import copy
import hashlib

from .runner import run_scenario
from .scenario import csv_bytes

HEADER_LINE = "ticket_id,kind,lot_id,input_lot_id,mass,unit"

MASS_LIMIT = 10**12


# ---------------------------------------------------------------------------
# Scenario surgery helpers
# ---------------------------------------------------------------------------

def _rows(csv_text: str) -> list[list[str]]:
    lines = csv_text.splitlines()
    if not lines or lines[0] != HEADER_LINE:
        # Deliberately malformed fixtures (bad header, ragged rows) are not
        # eligible for record/mass surgery: changing them would describe a
        # different failure class.  Callers skip such files explicitly.
        return []
    return [line.split(",") for line in lines[1:]]


def _is_canonical(csv_text: str) -> bool:
    lines = csv_text.splitlines()
    return bool(lines) and lines[0] == HEADER_LINE


def _encode(rows: list[list[str]]) -> str:
    return csv_bytes(rows).decode("utf-8")


def prune_files(scenario: dict) -> dict:
    """Drop CSV files no longer referenced by any remaining operation."""
    referenced = {
        op["file"] for op in scenario["operations"] if op["op"] == "ingest"
    }
    scenario["files"] = [
        entry for entry in scenario["files"] if entry["file_id"] in referenced
    ]
    return scenario


def without_operation(scenario: dict, op_id: str) -> dict | None:
    operations = [op for op in scenario["operations"] if op["op_id"] != op_id]
    if len(operations) == len(scenario["operations"]):
        return None
    candidate = copy.deepcopy(scenario)
    candidate["operations"] = operations
    return prune_files(candidate)


def without_record(scenario: dict, file_id: str, record_index: int) -> dict | None:
    candidate = copy.deepcopy(scenario)
    for entry in candidate["files"]:
        if entry["file_id"] != file_id:
            continue
        if not _is_canonical(entry["csv_text"]):
            return None
        rows = _rows(entry["csv_text"])
        if not (0 <= record_index < len(rows)):
            return None
        del rows[record_index]
        if not rows:
            # A header-only file is a different malformed case and cannot
            # represent the same signature; skip this candidate.
            return None
        entry["csv_text"] = _encode(rows)
        entry["sha256"] = hashlib.sha256(entry["csv_text"].encode("utf-8")).hexdigest()
        return candidate
    return None


def _mass_ladder(current_grams: int) -> list[int]:
    """Deterministic strictly decreasing candidates in whole grams."""
    if current_grams <= 0:
        return []
    candidates = []
    value = current_grams
    while value > 0:
        value = value // 2
        if value not in candidates:
            candidates.append(value)
    candidates.extend(v for v in (100, 10, 1, 0) if v < current_grams)
    seen = set()
    ordered = []
    for value in candidates:
        if value not in seen and 0 <= value <= MASS_LIMIT:
            seen.add(value)
            ordered.append(value)
    return ordered


def _render_mass(grams: int, unit: str) -> str | None:
    if unit == "g":
        return str(grams)
    if unit == "kg":
        if grams % 1000 != 0:
            return None
        whole = grams // 1000
        return f"{whole}.000"
    return None


def with_shrunk_mass(scenario: dict, file_id: str, record_index: int,
                     new_grams: int) -> dict | None:
    candidate = copy.deepcopy(scenario)
    for entry in candidate["files"]:
        if entry["file_id"] != file_id:
            continue
        if not _is_canonical(entry["csv_text"]):
            return None
        rows = _rows(entry["csv_text"])
        if not (0 <= record_index < len(rows)):
            return None
        row = rows[record_index]
        kind = row[1]
        if kind != "loss" and new_grams == 0:
            return None
        rendered = _render_mass(new_grams, row[5])
        if rendered is None:
            return None
        row[4] = rendered
        entry["csv_text"] = _encode(rows)
        entry["sha256"] = hashlib.sha256(entry["csv_text"].encode("utf-8")).hexdigest()
        return candidate
    return None


def record_cells(scenario: dict) -> list[tuple[str, int, int, str]]:
    """Enumerate ``(file_id, record_index, grams, unit)`` deterministically."""
    cells = []
    for entry in sorted(scenario["files"], key=lambda item: item["file_id"]):
        if not _is_canonical(entry["csv_text"]):
            continue
        for index, row in enumerate(_rows(entry["csv_text"])):
            unit = row[5]
            mass_text = row[4]
            if unit == "g":
                grams = int(mass_text)
            elif unit == "kg":
                whole, _, fraction = mass_text.partition(".")
                denominator = 10 ** len(fraction)
                numerator = int(whole) * denominator + (int(fraction) if fraction else 0)
                grams = numerator * 1000 // denominator
            else:
                continue
            cells.append((entry["file_id"], index, grams, unit))
    return cells


# ---------------------------------------------------------------------------
# Candidate verification
# ---------------------------------------------------------------------------

class Reducer:
    def __init__(self, prefix: list[str], budget: int) -> None:
        self.prefix = list(prefix)
        self.budget = budget
        self.runs = 0

    def verify(self, candidate: dict, target_signature: dict) -> tuple[bool, dict]:
        """Run one candidate on a fresh empty library; budget-bound."""
        if self.runs >= self.budget:
            return False, {"out_of_budget": True}
        self.runs += 1
        outcome = run_scenario(candidate, self.prefix)
        if not outcome["divergent"]:
            return False, outcome
        if outcome["signature"] != target_signature:
            return False, outcome
        return True, outcome

    # -- passes -------------------------------------------------------------

    def reduce_operations(self, scenario: dict, signature: dict) -> dict:
        changed = True
        while changed:
            changed = False
            for op in list(scenario["operations"]):
                candidate = without_operation(scenario, op["op_id"])
                if candidate is None:
                    continue
                accepted, _ = self.verify(candidate, signature)
                if self.runs >= self.budget:
                    return scenario
                if accepted:
                    scenario = candidate
                    changed = True
        return scenario

    def reduce_records(self, scenario: dict, signature: dict) -> dict:
        changed = True
        while changed:
            changed = False
            for entry in list(scenario["files"]):
                if not _is_canonical(entry["csv_text"]):
                    continue
                row_count = len(_rows(entry["csv_text"]))
                for index in range(row_count - 1, -1, -1):
                    candidate = without_record(scenario, entry["file_id"], index)
                    if candidate is None:
                        continue
                    accepted, _ = self.verify(candidate, signature)
                    if self.runs >= self.budget:
                        return scenario
                    if accepted:
                        scenario = candidate
                        changed = True
                        break  # restart enumeration against the new text
                if changed:
                    break
        return scenario

    def reduce_masses(self, scenario: dict, signature: dict) -> dict:
        changed = True
        while changed:
            changed = False
            for file_id, index, grams, unit in record_cells(scenario):
                for smaller in _mass_ladder(grams):
                    candidate = with_shrunk_mass(
                        scenario, file_id, index, smaller
                    )
                    if candidate is None:
                        continue
                    accepted, _ = self.verify(candidate, signature)
                    if self.runs >= self.budget:
                        return scenario
                    if accepted:
                        scenario = candidate
                        changed = True
                        break
                if changed:
                    break
        return scenario

    # -- final minimality ---------------------------------------------------

    def minimality_check(self, scenario: dict, signature: dict) -> dict:
        """Verify no remaining operation or record can still be deleted."""
        removable_operation = None
        for op in scenario["operations"]:
            candidate = without_operation(scenario, op["op_id"])
            if candidate is None:
                continue
            accepted, _ = self.verify(candidate, signature)
            if self.runs >= self.budget:
                return {"out_of_budget": True}
            if accepted:
                removable_operation = op["op_id"]
                break
        removable_record = None
        if removable_operation is None:
            for entry in scenario["files"]:
                if not _is_canonical(entry["csv_text"]):
                    continue
                for index, _row in enumerate(_rows(entry["csv_text"])):
                    candidate = without_record(scenario, entry["file_id"], index)
                    if candidate is None:
                        continue
                    accepted, _ = self.verify(candidate, signature)
                    if self.runs >= self.budget:
                        return {"out_of_budget": True}
                    if accepted:
                        removable_record = (entry["file_id"], index)
                        break
                if removable_record is not None:
                    break
        return {
            "removable_operation": removable_operation,
            "removable_record": removable_record,
        }


def reduce_counterexample(counterexample: dict, prefix: list[str],
                          budget: int) -> dict:
    """Return the reduction report for one self-contained counterexample."""
    signature = counterexample["signature"]
    scenario = {
        "format": "lot_lineage_diff_scenario_v1",
        "seed": counterexample["seed"],
        "steps": counterexample["steps"],
        "campaigns": sorted({op["campaign"] for op in counterexample["operations"]}),
        "files": copy.deepcopy(counterexample["files"]),
        "operations": copy.deepcopy(counterexample["operations"]),
        "coverage_tags": sorted({
            tag
            for op in counterexample["operations"]
            for tag in op.get("tags", ())
        }),
    }
    reducer = Reducer(prefix, budget)
    start_operations = len(scenario["operations"])
    start_records = sum(len(_rows(entry["csv_text"])) for entry in scenario["files"])
    start_bytes = sum(len(entry["csv_text"].encode("utf-8"))
                      for entry in scenario["files"])

    scenario = reducer.reduce_operations(scenario, signature)
    if reducer.runs < budget:
        scenario = reducer.reduce_records(scenario, signature)
    if reducer.runs < budget:
        scenario = reducer.reduce_masses(scenario, signature)

    complete = reducer.runs < budget
    minimality = None
    if complete:
        minimality = reducer.minimality_check(scenario, signature)
        if minimality.get("out_of_budget"):
            complete = False
        else:
            complete = (
                minimality["removable_operation"] is None
                and minimality["removable_record"] is None
            )

    end_records = sum(len(_rows(entry["csv_text"])) for entry in scenario["files"])
    end_bytes = sum(len(entry["csv_text"].encode("utf-8"))
                    for entry in scenario["files"])
    reduced_counterexample = copy.deepcopy(counterexample)
    reduced_counterexample["operations"] = scenario["operations"]
    reduced_counterexample["files"] = scenario["files"]
    return {
        "complete": complete,
        "runs_used": reducer.runs,
        "budget": budget,
        "operations": {
            "before": start_operations,
            "after": len(scenario["operations"]),
        },
        "csv_records": {"before": start_records, "after": end_records},
        "csv_bytes": {"before": start_bytes, "after": end_bytes},
        "minimality": minimality,
        "signature": signature,
        "counterexample": reduced_counterexample,
    }
