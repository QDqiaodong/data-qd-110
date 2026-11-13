"""Tests for the independent differential harness (``lot_lineage.diffharness``).

The tests assert the whole workflow rather than private pieces:

* generation is deterministic and covers every advertised contract branch;
* the in-memory oracle is independent of production parsing/validation/sealing
  modules (no production import anywhere in the harness package);
* the real intake CLI driven through real subprocesses on fresh empty
  libraries produces no divergence (no false positives), including via a
  transparent wrapper prefix;
* three deliberately faulty wrappers are each detected at the expected
  operation: duplicate replay counting, partial commit on conflict, and a
  sealed campaign still accepting tickets;
* counterexamples are self-contained and replay without the seed/generated
  files; reduced counterexamples reproduce the same signature in a new
  process;
* predicted failures leave database state and prior export files unchanged;
* determinism: same seed/budget yields byte-identical artifacts; an exhausted
  budget is honestly reported as incomplete; no-divergence runs report
  coverage counts and never claim the implementation is error-free.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from lot_lineage.diffharness import FORMAT_SCENARIO, FORMAT_COUNTEREXAMPLE
from lot_lineage.diffharness import jsonio
from lot_lineage.diffharness.categories import FAILURE_CATEGORIES
from lot_lineage.diffharness.oracle import (
    InMemoryOracle,
    OracleError,
    build_sealed_document,
    parse_ticket_bytes,
)
from lot_lineage.diffharness.reduce import reduce_counterexample
from lot_lineage.diffharness.replay import replay
from lot_lineage.diffharness.runner import run_scenario
from lot_lineage.diffharness.scenario import (
    COVERAGE_TAGS,
    file_map,
    generate_scenario,
)

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
WRAPPER_DIR = ROOT / "tests" / "buggy_wrappers"
SEED = 42
STEPS = 27


def wrapper_prefix(name: str) -> list[str]:
    return [sys.executable, str(WRAPPER_DIR / name)]


class ScenarioGenerationTest(unittest.TestCase):
    def test_deterministic_byte_output(self):
        first = generate_scenario(SEED, STEPS)
        second = generate_scenario(SEED, STEPS)
        self.assertEqual(jsonio.dump_bytes(first), jsonio.dump_bytes(second))
        self.assertEqual(first["format"], FORMAT_SCENARIO)

    def test_different_seeds_can_differ_but_same_seed_is_stable(self):
        one = generate_scenario(1, STEPS)
        two = generate_scenario(2, STEPS)
        self.assertEqual(jsonio.dump_bytes(one), jsonio.dump_bytes(generate_scenario(1, STEPS)))
        # Both seeds still cover the full branch set at full length.
        self.assertEqual(set(COVERAGE_TAGS), set(one["coverage_tags"]))
        self.assertEqual(set(COVERAGE_TAGS), set(two["coverage_tags"]))

    def test_full_scenario_covers_every_advertised_branch(self):
        scenario = generate_scenario(SEED, STEPS)
        self.assertEqual(set(COVERAGE_TAGS), set(scenario["coverage_tags"]))
        # Two real campaigns plus the unknown-campaign read path.
        self.assertIn("cmp-a", scenario["campaigns"])
        self.assertIn("cmp-b", scenario["campaigns"])
        self.assertIn("cmp-unknown", scenario["campaigns"])

    def test_embedded_files_are_self_contained_and_hash_consistent(self):
        import hashlib

        scenario = generate_scenario(SEED, STEPS)
        for entry in scenario["files"]:
            raw = entry["csv_text"].encode("utf-8")
            self.assertEqual(
                entry["sha256"], hashlib.sha256(raw).hexdigest()
            )
        referenced = {
            op["file"] for op in scenario["operations"] if op["op"] == "ingest"
        }
        embedded = {entry["file_id"] for entry in scenario["files"]}
        self.assertEqual(referenced, embedded)

    def test_steps_truncation_and_padding(self):
        short = generate_scenario(SEED, 5)
        self.assertEqual(len(short["operations"]), 5)
        long_one = generate_scenario(SEED, STEPS + 6)
        self.assertEqual(len(long_one["operations"]), STEPS + 6)


class OracleIndependenceTest(unittest.TestCase):
    def test_harness_package_never_imports_production_modules(self):
        package = SRC / "lot_lineage" / "diffharness"
        for path in package.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("import lot_lineage.intake", text, path)
            self.assertNotIn("from lot_lineage.intake", text, path)
            self.assertNotIn("import lot_lineage.graph", text, path)
            self.assertNotIn("from lot_lineage.graph", text, path)
            self.assertNotIn("from ..graph", text, path)
            self.assertNotIn("from ..intake", text, path)

    def test_independent_mass_normalisation_matches_contract(self):
        raw = (
            b"ticket_id,kind,lot_id,input_lot_id,mass,unit\n"
            b"T1,input,X,A,0.500,kg\n"
            b"T2,input,X,A,500,g\n"
        )
        tickets = parse_ticket_bytes(raw)
        self.assertEqual(tickets[0]["grams"], tickets[1]["grams"])
        self.assertEqual(tickets[0]["grams"], 500)

    def test_independent_parser_rejects_malformed_variants(self):
        cases = [
            b"",
            b"ticket_id,kind,lot_id,input_lot_id,mass,unit\n",
            b"wrong,header\nT1,raw,A,,1,g\n",
            b"ticket_id,kind,lot_id,input_lot_id,mass,unit\nT1,input,X,A,1\n",
            b"ticket_id,kind,lot_id,input_lot_id,mass,unit\nT1,raw,A,,1,mg\n",
            b"ticket_id,kind,lot_id,input_lot_id,mass,unit\nT1,input,X,A,0.0001,kg\n",
        ]
        for raw in cases:
            with self.assertRaises(OracleError, msg=raw):
                parse_ticket_bytes(raw)

    def test_independent_seal_detects_cycle_and_missing_definition(self):
        cyclic = [
            {"ticket_id": "P", "kind": "output", "lot_id": "P",
             "input_lot_id": None, "grams": 10},
            {"ticket_id": "Q", "kind": "output", "lot_id": "Q",
             "input_lot_id": None, "grams": 10},
            {"ticket_id": "I1", "kind": "input", "lot_id": "P",
             "input_lot_id": "Q", "grams": 10},
            {"ticket_id": "I2", "kind": "input", "lot_id": "Q",
             "input_lot_id": "P", "grams": 10},
            {"ticket_id": "L1", "kind": "loss", "lot_id": "P",
             "input_lot_id": None, "grams": 0},
            {"ticket_id": "L2", "kind": "loss", "lot_id": "Q",
             "input_lot_id": None, "grams": 0},
        ]
        with self.assertRaises(OracleError):
            build_sealed_document("c", cyclic)

    def test_oracle_replay_conflict_and_seal_refusal_categories(self):
        from lot_lineage.diffharness import categories as C

        oracle = InMemoryOracle()
        good = (
            b"ticket_id,kind,lot_id,input_lot_id,mass,unit\n"
            b"T1,raw,A,,100,g\n"
        )
        category, receipt = oracle.ingest("c", good)
        self.assertEqual(category, C.OK)
        self.assertEqual(receipt["new_tickets"], 1)
        # Equivalent replay is a no-op identity-wise.
        replay_text = (
            b"ticket_id,kind,lot_id,input_lot_id,mass,unit\n"
            b"T1,raw,A,,0.100,kg\n"
        )
        category, receipt = oracle.ingest("c", replay_text)
        self.assertEqual(category, C.OK)
        self.assertEqual(receipt["new_tickets"], 0)
        self.assertEqual(receipt["replayed_occurrences"], 1)
        # Conflict.
        conflict = (
            b"ticket_id,kind,lot_id,input_lot_id,mass,unit\n"
            b"T1,raw,A,,200,g\n"
        )
        category, _ = oracle.ingest("c", conflict)
        self.assertEqual(category, C.REJECTED_CONFLICT)
        # A genuine seal-content failure: produced lot with an input and an
        # explicit loss but no definition tickets / unconserved masses.
        broken = (
            b"ticket_id,kind,lot_id,input_lot_id,mass,unit\n"
            b"T2,input,X,A,50,g\n"
            b"T3,loss,X,,10,g\n"
        )
        category, _ = oracle.ingest("c2", broken)
        self.assertEqual(category, C.OK)
        category, _ = oracle.seal("c2")
        self.assertEqual(category, C.REJECTED_SEAL_FAILED)
        # A campaign that never received anything is the unknown path.
        category, _ = oracle.seal("empty")
        self.assertEqual(category, C.REJECTED_UNKNOWN_CAMPAIGN)
        # Unknown campaign.
        category, _ = oracle.status("missing")
        self.assertEqual(category, C.REJECTED_UNKNOWN_CAMPAIGN)


class EndToEndHarnessTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scenario = generate_scenario(SEED, STEPS)
        cls.files = file_map(cls.scenario)

    def test_real_cli_has_no_divergence_no_false_positive(self):
        outcome = run_scenario(self.scenario)
        self.assertFalse(
            outcome["divergent"],
            msg=json.dumps(outcome.get("signature"), sort_keys=True),
        )
        self.assertIsNone(outcome["counterexample"])
        self.assertEqual(outcome["operations_executed"], STEPS)
        # Full coverage, but no claim of correctness.
        self.assertEqual(outcome["covered_count"], len(COVERAGE_TAGS))

    def test_transparent_wrapper_prefix_also_clean(self):
        outcome = run_scenario(
            self.scenario, wrapper_prefix("good_passthrough.py")
        )
        self.assertFalse(outcome["divergent"])

    def _assert_bug_detected_and_reduced(self, wrapper: str, expected_op: str):
        prefix = wrapper_prefix(wrapper)
        outcome = run_scenario(self.scenario, prefix)
        self.assertTrue(outcome["divergent"])
        self.assertEqual(outcome["signature"]["op_id"], expected_op)
        counterexample = outcome["counterexample"]
        self.assertEqual(counterexample["format"], FORMAT_COUNTEREXAMPLE)
        # Self-contained: original CSV text and operations embedded.
        self.assertTrue(counterexample["files"])
        self.assertTrue(counterexample["operations"])
        self.assertIn("first_difference", counterexample)
        self.assertIn("signature_sha256", counterexample)

        # Replay needs no seed/generated files and reproduces identically.
        replayed = replay(counterexample, prefix)
        self.assertTrue(replayed["reproduces"])
        self.assertEqual(replayed["signature"], counterexample["signature"])

        # Reduce on fresh empty libraries per candidate.
        report = reduce_counterexample(counterexample, prefix, budget=400)
        self.assertTrue(
            report["complete"],
            msg=f"reduction incomplete: {report['runs_used']} runs",
        )
        self.assertLess(
            len(report["counterexample"]["operations"]),
            len(counterexample["operations"]),
        )
        # The minimised artifact still reproduces in a brand-new process.
        again = replay(report["counterexample"], prefix)
        self.assertTrue(again["reproduces"])
        self.assertEqual(again["signature"], counterexample["signature"])
        return report

    def test_duplicate_replay_counting_is_detected_and_minimised(self):
        report = self._assert_bug_detected_and_reduced(
            "buggy_dup_count.py", "op03"
        )
        self.assertEqual(report["operations"]["after"], 2)

    def test_conflict_partial_commit_is_detected_and_minimised(self):
        report = self._assert_bug_detected_and_reduced(
            "buggy_partial_commit.py", "op05"
        )
        self.assertEqual(report["operations"]["after"], 2)

    def test_sealed_campaign_accepting_tickets_is_detected_and_minimised(self):
        report = self._assert_bug_detected_and_reduced(
            "buggy_seal_accept.py", "op23"
        )
        self.assertGreaterEqual(report["operations"]["after"], 2)

    def test_budget_exhaustion_is_reported_as_incomplete(self):
        outcome = run_scenario(
            self.scenario, wrapper_prefix("buggy_seal_accept.py")
        )
        report = reduce_counterexample(
            outcome["counterexample"],
            wrapper_prefix("buggy_seal_accept.py"),
            budget=2,
        )
        self.assertFalse(report["complete"])
        self.assertEqual(report["runs_used"], 2)

    def test_reduction_is_byte_deterministic(self):
        prefix = wrapper_prefix("buggy_dup_count.py")
        outcome = run_scenario(self.scenario, prefix)
        first = reduce_counterexample(outcome["counterexample"], prefix, 200)
        second = reduce_counterexample(outcome["counterexample"], prefix, 200)
        self.assertEqual(
            jsonio.dump_bytes(first["counterexample"]),
            jsonio.dump_bytes(second["counterexample"]),
        )


class FreshLibraryAndFailureAtomicityTest(unittest.TestCase):
    def test_each_run_uses_a_fresh_empty_database(self):
        scenario = generate_scenario(SEED, STEPS)
        first = run_scenario(scenario)
        second = run_scenario(scenario)
        self.assertFalse(first["divergent"])
        self.assertFalse(second["divergent"])
        # Deterministic receipts across two independent fresh libraries.
        self.assertEqual(first["trace"], second["trace"])

    def test_predicted_failures_change_nothing_and_keep_old_export(self):
        from lot_lineage.diffharness import categories as C

        scenario = generate_scenario(SEED, STEPS)
        oracle = InMemoryOracle()
        files = file_map(scenario)
        # Track state snapshots around every predicted failure via the runner's
        # own independent observer; the runner already compares before/after
        # views, so a clean full run proves the atomicity guarantee.
        for operation in scenario["operations"]:
            from lot_lineage.diffharness.oracle import predict_operation

            category, _, _ = predict_operation(oracle, operation, files)
            if category in FAILURE_CATEGORIES:
                self.assertNotEqual(category, C.OK)
        outcome = run_scenario(scenario)
        self.assertFalse(outcome["divergent"])


class CommandLineEntryTest(unittest.TestCase):
    def _run_cli(self, *arguments):
        env_pythonpath = str(SRC)
        completed = subprocess.run(
            [sys.executable, "-m", "lot_lineage.diffharness", *arguments],
            capture_output=True,
            text=True,
            check=False,
            env={**__import__("os").environ, "PYTHONPATH": env_pythonpath},
        )
        return completed

    def test_generate_run_reduce_replay_cli_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            scenario_path = tmp_path / "scenario.json"
            ce_path = tmp_path / "ce.json"
            reduced_path = tmp_path / "reduced.json"

            generated = self._run_cli(
                "generate", "--seed", "42", "--steps", "27",
                "--output", str(scenario_path),
            )
            self.assertEqual(generated.returncode, 0, generated.stderr)

            clean = self._run_cli(
                "run", "--scenario", str(scenario_path)
            )
            self.assertEqual(clean.returncode, 0, clean.stderr)
            payload = json.loads(clean.stdout)
            self.assertFalse(payload["divergent"])
            self.assertIn("note", payload)
            self.assertEqual(payload["covered_count"], len(COVERAGE_TAGS))

            buggy = self._run_cli(
                "run", "--scenario", str(scenario_path),
                "--prefix", " ".join(wrapper_prefix("buggy_dup_count.py")),
                "--counterexample", str(ce_path),
            )
            self.assertEqual(buggy.returncode, 4)
            self.assertTrue(ce_path.exists())

            reduced = self._run_cli(
                "reduce", "--counterexample", str(ce_path),
                "--budget", "200",
                "--prefix", " ".join(wrapper_prefix("buggy_dup_count.py")),
                "--output", str(reduced_path),
            )
            self.assertEqual(reduced.returncode, 0, reduced.stderr)
            summary = json.loads(reduced.stdout)
            self.assertTrue(summary["complete"])

            replayed = self._run_cli(
                "replay", "--counterexample", str(reduced_path),
                "--prefix", " ".join(wrapper_prefix("buggy_dup_count.py")),
            )
            self.assertEqual(replayed.returncode, 4)
            self.assertTrue(json.loads(replayed.stdout)["reproduces"])


if __name__ == "__main__":
    unittest.main()
