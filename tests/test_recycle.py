"""Tests for steady-state closed-loop recycle accounting (schema_version 2).

Hand-computed expectations are written literally; the independent residual
checks below rebuild the balance equations from the stored fractions instead
of trusting the solver's own bookkeeping.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path
from unittest import mock

from lot_lineage.pipeline import build as build_batch
from lot_lineage.recycle import __main__ as recycle_cli
from lot_lineage.recycle.engine import NonUniqueError, compute
from lot_lineage.recycle.model import ContractError, load_steady, parse_steady
from lot_lineage.recycle.pipeline import build as build_recycle
from lot_lineage.recycle.store import SCHEMA_FAMILY, SCHEMA_VERSION

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
BATCH_FIXTURE = ROOT / "fixtures" / "lots.json"
PQ_FIXTURE = ROOT / "fixtures" / "recycle_pq.json"
CONVERTED_FIXTURE = ROOT / "fixtures" / "recycle_lots_converted.json"
DEMO_FIXTURE = ROOT / "fixtures" / "recycle_demo.json"


def write_json(directory: Path, name: str, payload: dict) -> Path:
    path = directory / name
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def pq_payload() -> dict:
    return json.loads(PQ_FIXTURE.read_text(encoding="utf-8"))


class BatchCompatibilityTest(unittest.TestCase):
    """Scenario 1: old fixture, old CLI contract and cyclic rejection stay intact."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "batch.sqlite"

    def test_batch_results_unchanged(self):
        result = build_batch(BATCH_FIXTURE, self.db)
        self.assertEqual(
            (result["raw_input_g"], result["process_loss_g"], result["available_g"]),
            (1800, 100, 1700),
        )
        y = next(row for row in result["lots"] if row["lot_id"] == "Y")
        self.assertEqual(y["origin_fraction"], {"A": "9/25", "B": "6/25", "C": "2/5"})
        self.assertEqual(result["available_origin_g"], {"A": "940", "B": "460", "C": "300"})

    def test_old_cli_still_rejects_cycles(self):
        cyclic = {
            "lots": [
                {"lot_id": "X", "kind": "produced", "mass_g": 10},
                {"lot_id": "Y", "kind": "produced", "mass_g": 10},
            ],
            "recipes": [
                {"output_lot": "X", "inputs": [{"lot_id": "Y", "mass_g": 10}], "loss_g": 0},
                {"output_lot": "Y", "inputs": [{"lot_id": "X", "mass_g": 10}], "loss_g": 0},
            ],
        }
        source = write_json(self.root, "cyclic.json", cyclic)
        completed = subprocess.run(
            [sys.executable, "-m", "lot_lineage.cli", "--source", str(source),
             "--database", str(self.root / "x.sqlite")],
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": str(SRC)},
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("cyclic", completed.stderr)


class PQFixtureTest(unittest.TestCase):
    """Scenario 2: two-unit loop with hand-computed fractions."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "pq.sqlite"
        self.data = load_steady(PQ_FIXTURE)
        self.result = compute(self.data)

    def test_hand_computed_fractions(self):
        fractions = self.result.fractions
        self.assertEqual(fractions["P"]["A"], Fraction(39, 41))
        self.assertEqual(fractions["P"]["B"], Fraction(2, 41))
        self.assertEqual(fractions["Q"]["A"], Fraction(36, 41))
        self.assertEqual(fractions["Q"]["B"], Fraction(5, 41))

    def test_terminal_totals(self):
        product_a = sum(self.result.product_total["A"].values())
        product_b = sum(self.result.product_total["B"].values())
        loss_a = sum(self.result.loss_total["A"].values())
        loss_b = sum(self.result.loss_total["B"].values())
        self.assertEqual(product_a, Fraction(3330, 41))
        self.assertEqual(product_b, Fraction(360, 41))
        self.assertEqual(loss_a, Fraction(360, 41))
        self.assertEqual(loss_b, Fraction(50, 41))
        self.assertEqual(self.result.feed_total, {"A": 90, "B": 10})

    def test_totals_are_not_inflated_by_internal_flows(self):
        summary = build_recycle(PQ_FIXTURE, self.db)
        self.assertEqual(summary["totals"]["external_feed_g"], "100")
        self.assertEqual(summary["totals"]["internal_transfer_g"], "180")
        self.assertEqual(summary["totals"]["product_g"], "90")
        self.assertEqual(summary["totals"]["loss_g"], "10")

    def test_independent_per_flow_and_per_node_residuals(self):
        # Rebuild every equation independently from raw masses + fractions.
        inflow_origin = {node: {"A": Fraction(0), "B": Fraction(0)} for node in ("P", "Q")}
        for feed in self.data.feeds:
            inflow_origin[feed.to_process][feed.origin_id] += Fraction(feed.mass_g)
        for transfer in self.data.transfers:
            for origin in ("A", "B"):
                attributed = Fraction(transfer.mass_g) * self.result.fractions[transfer.from_process][origin]
                # Stored attribution must equal the independent multiplication.
                self.assertEqual(self.result.transfer_origins[transfer.transfer_id][origin], attributed)
                inflow_origin[transfer.to_process][origin] += attributed
        for node in ("P", "Q"):
            for origin in ("A", "B"):
                lhs = Fraction(self.data.mass_in[node]) * self.result.fractions[node][origin]
                self.assertEqual(lhs, inflow_origin[node][origin])
            self.assertEqual(sum(self.result.fractions[node].values()), 1)
        for sink in self.data.sinks:
            self.assertEqual(
                sum(self.result.sink_origins[sink.sink_id].values()),
                Fraction(sink.mass_g),
            )

    def test_single_cyclic_scc(self):
        self.assertEqual([scc.members for scc in self.result.sccs], [["P", "Q"]])
        self.assertTrue(self.result.sccs[0].cyclic)
        self.assertEqual(self.result.condensation_order, [1])


class ConvertedBatchFixtureTest(unittest.TestCase):
    """Scenario 3: loop-free steady view reproduces the batch lineage exactly."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.batch_db = self.root / "batch.sqlite"
        self.steady_db = self.root / "steady.sqlite"
        self.batch = build_batch(BATCH_FIXTURE, self.batch_db)
        self.steady_summary = build_recycle(CONVERTED_FIXTURE, self.steady_db)

    def test_fractions_match_batch_pipeline(self):
        batch_fractions = {
            row["lot_id"]: {origin: Fraction(value) for origin, value in row["origin_fraction"].items()}
            for row in self.batch["lots"]
        }
        for process in self.steady_summary["processes"]:
            expected = batch_fractions[process["process_id"]]
            actual = {
                origin: Fraction(text)
                for origin, text in process["origin_fraction"].items()
            }
            # The batch model stores only non-zero origins; the steady model
            # stores explicit 0/1 rows.  Every batch value must agree.
            self.assertEqual({k: actual[k] for k in expected}, expected)
            self.assertTrue(all(value >= 0 for value in actual.values()))
            self.assertEqual(sum(actual.values()), Fraction(1))

    def test_product_origin_totals_match(self):
        terminal = self.steady_summary["origin_terminal"]
        self.assertEqual(Fraction(terminal["A"]["product_g"]), Fraction(940))
        self.assertEqual(Fraction(terminal["B"]["product_g"]), Fraction(460))
        self.assertEqual(Fraction(terminal["C"]["product_g"]), Fraction(300))
        # Loss attribution from the 100 g X recipe loss: A 60 g, B 40 g.
        self.assertEqual(Fraction(terminal["A"]["loss_g"]), Fraction(60))
        self.assertEqual(Fraction(terminal["B"]["loss_g"]), Fraction(40))
        self.assertEqual(Fraction(terminal["C"]["loss_g"]), Fraction(0))

    def test_all_sccs_are_acyclic(self):
        self.assertTrue(all(not scc["cyclic"] for scc in self.steady_summary["sccs"]))


class StructuralVariantsTest(unittest.TestCase):
    """Scenario 4: giant self-loop, legal self-loop, chained cyclic SCCs."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def _payload(self, processes, feeds, transfers, sinks):
        return {
            "schema_version": 2,
            "processes": [{"process_id": pid} for pid in processes],
            "feeds": feeds,
            "transfers": transfers,
            "sinks": sinks,
        }

    def test_giant_self_loop_exactly_one(self):
        payload = self._payload(
            ["S"],
            [{"feed_id": "f1", "origin_id": "A", "to_process": "S", "mass_g": 1}],
            [{"transfer_id": "loop", "from_process": "S", "to_process": "S",
              "mass_g": 999_999_999_999}],
            [{"sink_id": "out", "from_process": "S", "kind": "product", "mass_g": 1}],
        )
        source = write_json(self.root, "giant.json", payload)
        result = compute(load_steady(source))
        self.assertEqual(result.fractions["S"]["A"], Fraction(1))
        self.assertEqual(result.sccs[0].cyclic, True)

    def test_legal_self_loop_with_two_origins(self):
        payload = self._payload(
            ["S"],
            [
                {"feed_id": "f1", "origin_id": "A", "to_process": "S", "mass_g": 3},
                {"feed_id": "f2", "origin_id": "B", "to_process": "S", "mass_g": 1},
            ],
            [{"transfer_id": "loop", "from_process": "S", "to_process": "S", "mass_g": 8}],
            [{"sink_id": "out", "from_process": "S", "kind": "product", "mass_g": 4}],
        )
        source = write_json(self.root, "self.json", payload)
        result = compute(load_steady(source))
        # Equation: 12 x_A = 8 x_A + 3  -> x_A = 3/4, x_B = 1/4.
        self.assertEqual(result.fractions["S"]["A"], Fraction(3, 4))
        self.assertEqual(result.fractions["S"]["B"], Fraction(1, 4))

    def test_demo_has_chained_cyclic_and_independent_components(self):
        result = compute(load_steady(DEMO_FIXTURE))
        cyclic = [scc for scc in result.sccs if scc.cyclic]
        acyclic = [scc for scc in result.sccs if not scc.cyclic]
        self.assertEqual([scc.members for scc in cyclic], [["M1", "M2"], ["M3"], ["M4", "M5"]])
        self.assertEqual([scc.members for scc in acyclic], [["M6"], ["M7"]])
        # Condensation order respects dependencies: M3's SCC after M1/M2's.
        order = result.condensation_order
        self.assertLess(order.index(result.node_scc["M1"]), order.index(result.node_scc["M3"]))
        self.assertLess(order.index(result.node_scc["M3"]), order.index(result.node_scc["M4"]))

    def test_multiple_independent_valid_components(self):
        payload = self._payload(
            ["U1", "U2", "V1"],
            [
                {"feed_id": "fu", "origin_id": "A", "to_process": "U1", "mass_g": 5},
                {"feed_id": "fv", "origin_id": "B", "to_process": "V1", "mass_g": 7},
            ],
            [{"transfer_id": "tu", "from_process": "U1", "to_process": "U2", "mass_g": 5}],
            [
                {"sink_id": "su", "from_process": "U2", "kind": "product", "mass_g": 5},
                {"sink_id": "sv", "from_process": "V1", "kind": "loss", "mass_g": 7},
            ],
        )
        source = write_json(self.root, "independent.json", payload)
        result = compute(load_steady(source))
        self.assertEqual(result.fractions["U1"]["A"], Fraction(1))
        self.assertEqual(result.fractions["U2"]["A"], Fraction(1))
        self.assertEqual(result.fractions["V1"]["B"], Fraction(1))
        self.assertEqual(len(result.sccs), 3)


class RejectionTest(unittest.TestCase):
    """Scenario 5: structural, reference, numeric and identifiability failures."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def _load(self, payload):
        return parse_steady(json.dumps(payload).encode("utf-8"))

    def test_isolated_balanced_self_loop_is_rejected_with_node_name(self):
        payload = pq_payload()
        payload["processes"].append({"process_id": "Z"})
        payload["transfers"].append(
            {"transfer_id": "z-loop", "from_process": "Z", "to_process": "Z", "mass_g": 7}
        )
        with self.assertRaisesRegex(ContractError, "Z") as ctx:
            self._load(payload)
        message = str(ctx.exception)
        self.assertIn("undetermined source composition", message)

    def test_unknown_reference_localized(self):
        payload = pq_payload()
        payload["feeds"][0]["to_process"] = "GHOST"
        with self.assertRaisesRegex(ContractError, r"/feeds/0.*GHOST"):
            self._load(payload)

    def test_duplicate_ids_rejected_even_when_identical(self):
        payload = pq_payload()
        payload["feeds"].append(dict(payload["feeds"][0]))
        with self.assertRaisesRegex(ContractError, r"/feeds/2.*duplicate feed_id"):
            self._load(payload)
        duplicate_transfer = dict(payload["transfers"][0])
        payload = pq_payload()
        payload["transfers"].append(duplicate_transfer)
        with self.assertRaisesRegex(ContractError, "duplicate transfer_id"):
            self._load(payload)

    def test_boolean_mass_rejected(self):
        payload = pq_payload()
        payload["feeds"][0]["mass_g"] = True
        with self.assertRaisesRegex(ContractError, r"/feeds/0.*integer"):
            self._load(payload)

    def test_one_gram_imbalance_localized(self):
        payload = pq_payload()
        payload["sinks"][0]["mass_g"] = 31  # P inflow 150, outflow becomes 151
        with self.assertRaisesRegex(ContractError, r"process 'P'.*difference \+?\-?1 g"):
            self._load(payload)

    def test_schema_version_must_be_integer_two(self):
        payload = pq_payload()
        payload["schema_version"] = "2"
        with self.assertRaisesRegex(ContractError, "schema_version"):
            self._load(payload)
        payload = pq_payload()
        payload["schema_version"] = 1
        with self.assertRaisesRegex(ContractError, "only integer 2"):
            self._load(payload)

    def test_extra_and_missing_fields(self):
        payload = pq_payload()
        payload["processes"][0]["extra"] = 1
        with self.assertRaisesRegex(ContractError, r"/processes/0.*unexpected extra"):
            self._load(payload)
        payload = pq_payload()
        del payload["sinks"][0]["kind"]
        with self.assertRaisesRegex(ContractError, r"/sinks/0.*missing kind"):
            self._load(payload)

    def test_whitespace_and_empty_identifiers(self):
        payload = pq_payload()
        payload["processes"][0]["process_id"] = " P "
        with self.assertRaises(ContractError):
            self._load(payload)
        payload = pq_payload()
        payload["feeds"][0]["origin_id"] = ""
        with self.assertRaises(ContractError):
            self._load(payload)

    def test_empty_required_arrays(self):
        for array in ("processes", "feeds", "sinks"):
            payload = pq_payload()
            payload[array] = []
            with self.assertRaises(ContractError):
                self._load(payload)

    def test_mass_limits(self):
        payload = pq_payload()
        payload["feeds"][0]["mass_g"] = 10**12 + 1
        with self.assertRaisesRegex(ContractError, "10\\^12"):
            self._load(payload)
        payload = pq_payload()
        payload["feeds"][0]["mass_g"] = 0
        with self.assertRaisesRegex(ContractError, "between 1"):
            self._load(payload)

    def test_invalid_sink_kind(self):
        payload = pq_payload()
        payload["sinks"][0]["kind"] = "scrap"
        with self.assertRaisesRegex(ContractError, r"/sinks/0.*product.*loss"):
            self._load(payload)

    def test_encoding_and_json_boundaries(self):
        for raw in (b"\xff\xfe{x", b'{"schema_version":2,', b"[1,2,3]"):
            with self.assertRaises(ContractError):
                parse_steady(raw)
        payload = pq_payload()
        payload["unexpected_top"] = 1
        with self.assertRaises(ContractError):
            parse_steady(json.dumps(payload).encode("utf-8"))
        payload = pq_payload()
        payload["schema_version"] = True
        with self.assertRaisesRegex(ContractError, "schema_version"):
            parse_steady(json.dumps(payload).encode("utf-8"))

    def test_non_object_record_rejected(self):
        payload = pq_payload()
        payload["processes"][0] = "P"
        with self.assertRaisesRegex(ContractError, r"/processes/0.*object"):
            self._load(payload)

    def test_balanced_node_without_feed_or_sink_is_identifiability_failure(self):
        payload = pq_payload()
        payload["processes"].append({"process_id": "Z"})
        payload["transfers"].append(
            {"transfer_id": "z-loop", "from_process": "Z", "to_process": "Z", "mass_g": 7}
        )
        # The structural validator must reject before any equation is built.
        with self.assertRaises(ContractError):
            self._load(payload)
        self.assertNotIn("z-loop", [t.transfer_id for t in self._load(pq_payload()).transfers])


class InvarianceTest(unittest.TestCase):
    """Scenario 6: feed splits, ordering and parallel split invariance."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_split_feed_does_not_change_proportions(self):
        base = compute(load_steady(PQ_FIXTURE))
        payload = pq_payload()
        payload["feeds"][0] = {
            "feed_id": "f-A-P-1",
            "origin_id": "A",
            "to_process": "P",
            "mass_g": 35,
        }
        payload["feeds"].insert(
            1,
            {"feed_id": "f-A-P-2", "origin_id": "A", "to_process": "P", "mass_g": 55},
        )
        split_source = write_json(self.root, "split.json", payload)
        split = compute(load_steady(split_source))
        for node in ("P", "Q"):
            self.assertEqual(split.fractions[node], base.fractions[node])
        for origin in ("A", "B"):
            self.assertEqual(
                sum(split.product_total[origin].values()),
                sum(base.product_total[origin].values()),
            )
            self.assertEqual(
                sum(split.loss_total[origin].values()),
                sum(base.loss_total[origin].values()),
            )

    def test_array_reorder_changes_pointers_but_not_proportions(self):
        base = load_steady(PQ_FIXTURE)
        payload = pq_payload()
        payload["feeds"].reverse()
        reordered_source = write_json(self.root, "reordered.json", payload)
        reordered = load_steady(reordered_source)
        base_result = compute(base)
        reordered_result = compute(reordered)
        for node in ("P", "Q"):
            self.assertEqual(reordered_result.fractions[node], base_result.fractions[node])
        # The raw bytes and pointers genuinely reflect the changed input.
        self.assertNotEqual(reordered.sha256, base.sha256)
        pointer = reordered.pointers[("feed", "f-B-Q")]
        self.assertEqual(pointer, "/feeds/0")
        self.assertEqual(base.pointers[("feed", "f-B-Q")], "/feeds/1")

    def test_parallel_transfer_split_invariance(self):
        base = compute(load_steady(PQ_FIXTURE))
        payload = pq_payload()
        pq = next(t for t in payload["transfers"] if t["transfer_id"] == "t-P-Q")
        pq["mass_g"] = 70
        payload["transfers"].append(
            {"transfer_id": "t-P-Q-parallel", "from_process": "P", "to_process": "Q", "mass_g": 50}
        )
        split_source = write_json(self.root, "parallel.json", payload)
        split = compute(load_steady(split_source))
        for node in ("P", "Q"):
            self.assertEqual(split.fractions[node], base.fractions[node])
        for sink in base.data.sinks:
            self.assertEqual(
                split.sink_origins[sink.sink_id], base.sink_origins[sink.sink_id]
            )


class EndToEndCLITest(unittest.TestCase):
    """Scenario 7: real CLI, SQLite facts, source removal and failure boundaries."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "steady.json"
        self.source.write_bytes(PQ_FIXTURE.read_bytes())
        self.db = self.root / "out.sqlite"

    def _run_cli(self, source=None, database=None):
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "lot_lineage.recycle",
                "--source",
                str(source or self.source),
                "--database",
                str(database or self.db),
            ],
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": str(SRC)},
            capture_output=True,
            text=True,
        )

    def test_cli_publishes_exact_sqlite_and_survives_source_removal(self):
        completed = self._run_cli()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        first_stdout = completed.stdout

        # Deterministic stdout across rebuilds.
        rebuild = self._run_cli()
        self.assertEqual(rebuild.stdout, first_stdout)

        with sqlite3.connect(self.db) as db:
            self.assertEqual(
                db.execute(
                    "SELECT value FROM recycle_meta WHERE key='schema_family'"
                ).fetchone()[0],
                SCHEMA_FAMILY,
            )
            self.assertEqual(
                db.execute(
                    "SELECT schema_version FROM recycle_source"
                ).fetchone()[0],
                SCHEMA_VERSION,
            )
            row = db.execute(
                "SELECT numerator, denominator FROM recycle_process_origin "
                "WHERE process_id='Q' AND origin_id='A'"
            ).fetchone()
            self.assertEqual(row, ("36", "41"))
            row = db.execute(
                "SELECT numerator, denominator FROM recycle_sink_origin "
                "WHERE sink_id='s-Q-loss' AND origin_id='B'"
            ).fetchone()
            self.assertEqual(row, ("50", "41"))
            raw_json, stored_hash = db.execute(
                "SELECT raw_json, source_sha256 FROM recycle_source"
            ).fetchone()
            self.assertEqual(hashlib.sha256(raw_json).hexdigest(), stored_hash)
            self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])

            # Independent equation residual built straight from stored facts.
            masses = dict(
                db.execute("SELECT process_id, inflow_g FROM recycle_process").fetchall()
            )
            fractions = {
                (process_id, origin_id): Fraction(int(num), int(den))
                for process_id, origin_id, num, den in db.execute(
                    "SELECT process_id, origin_id, numerator, denominator "
                    "FROM recycle_process_origin"
                )
            }
            rhs = {node: {"A": Fraction(0), "B": Fraction(0)} for node in masses}
            for _fid, origin_id, to_process, mass_g, _ptr in db.execute(
                "SELECT feed_id, origin_id, to_process, mass_g, json_pointer FROM recycle_feed"
            ):
                rhs[to_process][origin_id] += Fraction(mass_g)
            for _tid, src, dst, mass_g, _ptr in db.execute(
                "SELECT transfer_id, from_process, to_process, mass_g, json_pointer "
                "FROM recycle_transfer"
            ):
                for origin in ("A", "B"):
                    rhs[dst][origin] += Fraction(mass_g) * fractions[(src, origin)]
            for node in masses:
                for origin in ("A", "B"):
                    self.assertEqual(
                        Fraction(masses[node]) * fractions[(node, origin)], rhs[node][origin]
                    )

        # Remove the input: the library still contains the original bytes and
        # allows an independent recomputation/verification.
        self.source.unlink()
        with sqlite3.connect(self.db) as db:
            raw_json, stored_hash = db.execute(
                "SELECT raw_json, source_sha256 FROM recycle_source"
            ).fetchone()
        self.assertEqual(hashlib.sha256(raw_json).hexdigest(), stored_hash)
        reloaded = parse_steady(bytes(raw_json))
        recomputed = compute(reloaded)
        self.assertEqual(recomputed.fractions["P"]["A"], Fraction(39, 41))

    def test_invalid_input_preserves_published_target_and_cleans_temp(self):
        # Publish a good database first.
        good = self._run_cli()
        self.assertEqual(good.returncode, 0, good.stderr)
        before = self.db.read_bytes()

        bad_payload = pq_payload()
        bad_payload["feeds"][0]["mass_g"] = True
        bad_source = self.root / "bad.json"
        bad_source.write_text(json.dumps(bad_payload), encoding="utf-8")
        failed = self._run_cli(source=bad_source)
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("integer", failed.stderr)
        self.assertEqual(self.db.read_bytes(), before)
        leftovers = [path for path in self.root.iterdir() if path.name.startswith(".recycle-")]
        self.assertEqual(leftovers, [])

        # A rerun with valid input succeeds and republishes cleanly.
        rerun = self._run_cli()
        self.assertEqual(rerun.returncode, 0, rerun.stderr)
        self.assertTrue(self.db.exists())

    def test_write_failure_keeps_old_target(self):
        good = self._run_cli()
        self.assertEqual(good.returncode, 0, good.stderr)
        before = self.db.read_bytes()
        with mock.patch(
            "lot_lineage.recycle.pipeline.write_database",
            side_effect=sqlite3.OperationalError("injected write failure"),
        ):
            with self.assertRaises(sqlite3.OperationalError):
                build_recycle(self.source, self.db)
        self.assertEqual(self.db.read_bytes(), before)
        leftovers = [path for path in self.root.iterdir() if path.name.startswith(".recycle-")]
        self.assertEqual(leftovers, [])

    def test_replace_failure_keeps_old_target(self):
        good = self._run_cli()
        self.assertEqual(good.returncode, 0, good.stderr)
        before = self.db.read_bytes()
        with mock.patch("lot_lineage.recycle.pipeline.os.replace", side_effect=OSError("injected replace failure")):
            with self.assertRaises(OSError):
                build_recycle(self.source, self.db)
        self.assertEqual(self.db.read_bytes(), before)
        leftovers = [path for path in self.root.iterdir() if path.name.startswith(".recycle-")]
        self.assertEqual(leftovers, [])

    def test_target_must_not_alias_source(self):
        with self.assertRaisesRegex(ContractError, "same file"):
            build_recycle(self.source, self.source)
        link = self.root / "link.json"
        os.symlink(self.source, link)
        with self.assertRaisesRegex(ContractError, "same file"):
            build_recycle(self.source, link)

    def test_stdout_failure_still_publishes_database(self):
        with mock.patch("builtins.print", side_effect=OSError("broken stdout")):
            code = recycle_cli.main(["--source", str(self.source), "--database", str(self.db)])
        self.assertEqual(code, 3)
        self.assertTrue(self.db.exists())
        with sqlite3.connect(self.db) as db:
            self.assertEqual(
                db.execute("SELECT source_sha256 FROM recycle_source").fetchone()[0],
                hashlib.sha256(self.source.read_bytes()).hexdigest(),
            )


if __name__ == "__main__":
    unittest.main()
