"""Real-subprocess runner and independent state observation.

The runner treats the system under test (SUT) strictly as a black box:

* every full run gets a brand-new empty working directory and an SQLite file
  that never existed before, so there is no carry-over between runs;
* each SUT command is a real subprocess built from a configurable command
  prefix (so a buggy wrapper can replace the production CLI without touching
  production code);
* before and after every operation the database is read independently with
  the standard-library :mod:`sqlite3` module only -- production parsing,
  validation and sealing functions are never imported here;
* every predicted failure is additionally checked to have changed nothing
  (campaign state and any pre-existing export file);
* the first divergent operation produces a self-contained counterexample.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

from . import FORMAT_COUNTEREXAMPLE, categories as C
from .oracle import InMemoryOracle, predict_operation
from .scenario import file_map

DEFAULT_PREFIX = [sys.executable, "-m", "lot_lineage.intake"]
SUBPROCESS_TIMEOUT_SECONDS = 60.0


# ---------------------------------------------------------------------------
# SUT invocation
# ---------------------------------------------------------------------------

def build_command(prefix: list[str], operation: dict, database: Path,
                  work_dir: Path) -> list[str]:
    kind = operation["op"]
    campaign = operation["campaign"]
    # Absolute paths keep the SUT independent of its current working
    # directory, which matters when the configured prefix is a wrapper.
    database_arg = str(database.resolve())
    command = list(prefix)
    if kind == "ingest":
        source_arg = str((work_dir / operation["file"]).resolve())
        command += [
            "ingest", "--database", database_arg,
            "--campaign", campaign,
            "--source", source_arg,
        ]
    elif kind == "seal":
        command += ["seal", "--database", database_arg,
                    "--campaign", campaign]
    elif kind == "status":
        command += ["status", "--database", database_arg,
                    "--campaign", campaign]
    else:  # export
        output_arg = str((work_dir / operation["output"]).resolve())
        command += [
            "export", "--database", database_arg,
            "--campaign", campaign,
            "--output", output_arg,
        ]
    return command


def invoke(command: list[str], env: dict) -> dict:
    try:
        completed = subprocess.run(
            command,
            cwd=env.get("_cwd"),
            env={key: value for key, value in env.items() if not key.startswith("_")},
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=SUBPROCESS_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "returncode": -1,
            "stdout": "",
            "stderr": f"harness: subprocess timeout: {exc}",
            "receipt": None,
        }
    stdout = completed.stdout.decode("utf-8", errors="replace")
    stderr = completed.stderr.decode("utf-8", errors="replace")
    receipt = None
    if completed.returncode == 0:
        try:
            receipt = json.loads(stdout)
        except ValueError:
            receipt = None
    return {
        "returncode": completed.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "receipt": receipt,
    }


def classify_actual(operation: dict, result: dict) -> str:
    """Map an observed process result onto the shared category vocabulary.

    This mapping uses only the exit status and independent stderr substrings;
    it never imports production exceptions.
    """
    if result["returncode"] == 0:
        return C.OK
    stderr = result["stderr"]
    kind = operation["op"]
    if kind == "ingest":
        if "sealed" in stderr:
            return C.REJECTED_SEALED
        if "conflict" in stderr:
            return C.REJECTED_CONFLICT
        return C.REJECTED_MALFORMED
    if kind == "seal":
        if "unknown campaign" in stderr or "has no tickets" in stderr:
            return C.REJECTED_UNKNOWN_CAMPAIGN
        return C.REJECTED_SEAL_FAILED
    if kind == "status":
        return C.REJECTED_UNKNOWN_CAMPAIGN
    # export
    if "unknown campaign" in stderr:
        return C.REJECTED_UNKNOWN_CAMPAIGN
    return C.REJECTED_NOT_SEALED


# ---------------------------------------------------------------------------
# Independent SQLite observation
# ---------------------------------------------------------------------------

def read_campaign_view(database: Path, campaign_id: str) -> dict | None:
    """Read one campaign through plain SQL; production code is never used."""
    if not database.exists() or database.stat().st_size == 0:
        return None
    uri = f"file:{database.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "intake_campaign" not in tables:
            return None
        row = conn.execute(
            "SELECT status, seal_sha256, sealed_json FROM intake_campaign "
            "WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchone()
        if row is None:
            return None
        status, seal_sha256, sealed_blob = row
        ticket_count = conn.execute(
            "SELECT COUNT(*) FROM intake_ticket WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchone()[0]
        source_count = conn.execute(
            "SELECT COUNT(*) FROM intake_source WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchone()[0]
        tickets = [
            {
                "ticket_id": ticket_id,
                "kind": kind,
                "lot_id": lot_id,
                "input_lot_id": input_lot_id,
                "grams": grams,
            }
            for ticket_id, kind, lot_id, input_lot_id, grams in conn.execute(
                "SELECT ticket_id, kind, lot_id, input_lot_id, grams "
                "FROM intake_ticket WHERE campaign_id = ? "
                "ORDER BY ticket_id",
                (campaign_id,),
            )
        ]
        sources = [
            digest
            for (digest,) in conn.execute(
                "SELECT source_sha256 FROM intake_source WHERE campaign_id = ? "
                "ORDER BY source_sha256",
                (campaign_id,),
            )
        ]
        positions = [
            {
                "source_sha256": digest,
                "ticket_id": ticket_id,
                "record_no": record_no,
                "line_no": line_no,
            }
            for digest, ticket_id, record_no, line_no in conn.execute(
                "SELECT s.source_sha256, st.ticket_id, st.record_no, st.line_no "
                "FROM intake_source_ticket st JOIN intake_source s "
                "ON s.source_pk = st.source_pk "
                "WHERE s.campaign_id = ? "
                "ORDER BY s.source_sha256, st.record_no, st.ticket_id",
                (campaign_id,),
            )
        ]
        edges = []
        if "intake_sealed_edge" in tables:
            edge_rows = list(conn.execute(
                "SELECT output_lot, input_lot, total_grams FROM intake_sealed_edge "
                "WHERE campaign_id = ? ORDER BY output_lot, input_lot",
                (campaign_id,),
            ))
            for output_lot, input_lot, total_text in edge_rows:
                ticket_ids = [
                    ticket_id
                    for (ticket_id,) in conn.execute(
                        "SELECT ticket_id FROM intake_sealed_edge_ticket "
                        "WHERE campaign_id = ? AND output_lot = ? AND input_lot = ? "
                        "ORDER BY ticket_id",
                        (campaign_id, output_lot, input_lot),
                    )
                ]
                edges.append(
                    {
                        "output_lot": output_lot,
                        "input_lot": input_lot,
                        "total_grams": int(total_text),
                        "ticket_ids": ticket_ids,
                    }
                )
        sealed_text = (
            bytes(sealed_blob).decode("utf-8") if sealed_blob is not None else None
        )
        return {
            "status": status,
            "ticket_count": ticket_count,
            "source_count": source_count,
            "seal_sha256": seal_sha256,
            "sealed_json": sealed_text,
            "tickets": tickets,
            "sources": sources,
            "positions": positions,
            "edges": edges,
        }
    finally:
        conn.close()


def read_all_campaign_views(database: Path, campaigns: list[str]) -> dict:
    return {
        campaign: read_campaign_view(database, campaign)
        for campaign in sorted(set(campaigns))
    }


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

# Receipt keys compared per operation kind.  The "campaign" key is an echo of
# the input rather than a model prediction, so it is deliberately excluded.
_RECEIPT_KEYS = {
    "ingest": (
        "status", "records", "new_tickets", "replayed_occurrences",
        "new_source", "source_sha256", "ticket_count", "source_count",
    ),
    "seal": (
        "status", "seal_sha256", "json_bytes", "lot_count", "recipe_count",
        "ticket_count", "source_count", "already_sealed",
    ),
    "status": (
        "status", "ticket_count", "source_count", "seal_sha256",
    ),
    "export": ("status", "sha256", "json_bytes"),
}


def _project_receipt(kind: str, receipt: dict | None) -> dict:
    if not isinstance(receipt, dict):
        return {}
    return {key: receipt.get(key) for key in _RECEIPT_KEYS[kind]}


def _difference(kind: str, field: str, expected, actual) -> dict:
    return {"kind": kind, "field": field, "expected": expected, "actual": actual}


def compare_operation(
    operation: dict,
    expected_category: str,
    expected_receipt: dict,
    actual: dict,
    actual_category: str,
    oracle: InMemoryOracle,
    database: Path,
    campaigns: list[str],
    export_before: str | None,
    export_after: str | None,
    expected_export_bytes: bytes | None,
):
    """Return ``(difference_or_None, observed_state_views)``."""
    views = read_all_campaign_views(database, campaigns)
    kind = operation["op"]

    if actual_category != expected_category:
        return _difference(
            "outcome_category", "category", expected_category, actual_category
        ), views

    # A successful command must agree on the projected receipt.
    if expected_category == C.OK:
        projected = _project_receipt(kind, actual["receipt"])
        expected_projected = _project_receipt(kind, expected_receipt)
        if projected != expected_projected:
            return _difference(
                "receipt", "receipt", expected_projected, projected
            ), views

    # State comparison for every known campaign (read independently).
    for campaign in sorted(set(campaigns)):
        expected_view = oracle.campaign_view(campaign)
        actual_view = views.get(campaign)
        if expected_view != actual_view:
            return _difference(
                "state",
                f"campaign:{campaign}",
                expected_view,
                actual_view,
            ), views

    if kind == "export":
        output_path = database.parent / operation["output"]
        if expected_category == C.OK:
            if not output_path.exists():
                return _difference(
                    "export_file", "exists", True, False
                ), views
            actual_bytes = output_path.read_bytes()
            if actual_bytes != expected_export_bytes:
                return _difference(
                    "export_file",
                    "bytes",
                    hashlib.sha256(expected_export_bytes).hexdigest(),
                    hashlib.sha256(actual_bytes).hexdigest(),
                ), views
            if hashlib.sha256(actual_bytes).hexdigest() != expected_receipt["sha256"]:
                return _difference(
                    "export_file", "sha256", expected_receipt["sha256"],
                    hashlib.sha256(actual_bytes).hexdigest(),
                ), views
        else:
            # A failed export must leave the previous file byte-identical.
            if export_before != export_after:
                return _difference(
                    "export_file",
                    "preserved",
                    export_before,
                    export_after,
                ), views
    return None, views


# ---------------------------------------------------------------------------
# Scenario execution
# ---------------------------------------------------------------------------

def _sut_env(prefix: list[str]) -> dict:
    env = dict(os.environ)
    # The harness package lives at <src>/lot_lineage/diffharness, so its
    # parent's parent is the import root that makes ``python -m
    # lot_lineage.intake`` work in a freshly spawned interpreter.  Computing
    # it from this file does not require the package to be already importable
    # via PYTHONPATH (it is not, when *this* process was started with -m).
    inferred_src = str(Path(__file__).resolve().parents[2])
    existing = env.get("PYTHONPATH")
    if existing:
        parts = existing.split(os.pathsep)
        if inferred_src not in parts:
            env["PYTHONPATH"] = os.pathsep.join([inferred_src, *parts])
    else:
        env["PYTHONPATH"] = inferred_src
    return env


def run_scenario(scenario: dict, prefix: list[str] | None = None) -> dict:
    """Execute a full scenario against a fresh empty library."""
    prefix = list(prefix or DEFAULT_PREFIX)
    work_obj = tempfile.TemporaryDirectory(prefix="diffharness-")
    work_dir = Path(work_obj.name)
    database = work_dir / "intake.sqlite"
    env = _sut_env(prefix)
    env["_cwd"] = str(work_dir)

    files = file_map(scenario)
    for file_id, raw in files.items():
        (work_dir / file_id).write_bytes(raw)

    oracle = InMemoryOracle()
    campaigns = list(scenario["campaigns"])
    trace: list[dict] = []
    difference: dict | None = None
    divergent_op: dict | None = None
    observed_before = observed_after = None
    actual_result: dict | None = None

    for operation in scenario["operations"]:
        expected_category, expected_receipt, expected_export = predict_operation(
            oracle, operation, files
        )
        export_path = work_dir / operation["output"] if operation["op"] == "export" else None
        export_before = (
            hashlib.sha256(export_path.read_bytes()).hexdigest()
            if export_path is not None and export_path.exists()
            else None
        )
        state_before = read_all_campaign_views(database, campaigns)
        command = build_command(prefix, operation, database, work_dir)
        actual_result = invoke(command, env)
        actual_category = classify_actual(operation, actual_result)
        export_after = (
            hashlib.sha256(export_path.read_bytes()).hexdigest()
            if export_path is not None and export_path.exists()
            else None
        )
        difference, observed_after = compare_operation(
            operation,
            expected_category,
            expected_receipt,
            actual_result,
            actual_category,
            oracle,
            database,
            campaigns,
            export_before,
            export_after,
            expected_export,
        )
        trace.append(
            {
                "op_id": operation["op_id"],
                "op": operation["op"],
                "campaign": operation["campaign"],
                "expected_category": expected_category,
                "actual_category": actual_category,
                "returncode": actual_result["returncode"],
            }
        )
        if difference is not None:
            divergent_op = operation
            observed_before = state_before
            break

    work_obj.cleanup()

    result = {
        "divergent": difference is not None,
        "operations_executed": len(trace),
        "operations_total": len(scenario["operations"]),
        "trace": trace,
        "coverage_tags": scenario["coverage_tags"],
        "covered_count": len(scenario["coverage_tags"]),
    }
    if difference is not None:
        signature = {
            "op_id": divergent_op["op_id"],
            "op": divergent_op["op"],
            "campaign": divergent_op["campaign"],
            "difference_kind": difference["kind"],
            "field": difference["field"],
            "expected_category": trace[-1]["expected_category"],
            "actual_category": trace[-1]["actual_category"],
        }
        result["signature"] = signature
        result["counterexample"] = build_counterexample(
            scenario, divergent_op, signature, difference,
            actual_result, observed_before, observed_after, prefix,
        )
    else:
        result["signature"] = None
        result["counterexample"] = None
    return result


def build_counterexample(scenario: dict, divergent_op: dict, signature: dict,
                         difference: dict, actual_result: dict,
                         state_before: dict, state_after: dict,
                         prefix: list[str]) -> dict:
    """Assemble the fully self-contained counterexample artifact."""
    kept_files = {
        entry["file_id"]: entry
        for entry in scenario["files"]
    }
    return {
        "format": FORMAT_COUNTEREXAMPLE,
        "seed": scenario["seed"],
        "steps": scenario["steps"],
        "command_prefix": list(prefix),
        "signature": signature,
        "signature_sha256": hashlib.sha256(
            json.dumps(signature, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "first_difference": difference,
        "divergent_operation": divergent_op,
        "operations": scenario["operations"],
        "files": [kept_files[key] for key in sorted(kept_files)],
        "observed": {
            "returncode": actual_result["returncode"],
            "stdout": actual_result["stdout"],
            "stderr_tail": actual_result["stderr"][-2000:],
            "state_before": state_before,
            "state_after": state_after,
        },
    }
