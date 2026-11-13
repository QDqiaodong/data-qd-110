"""Differential harness for the offline weighing-ticket model.

A completely independent black-box differential tester for the
``lot_lineage.intake`` command family (``ingest`` / ``seal`` / ``status`` /
``export``).  The harness:

* generates deterministic multi-campaign ticket scenarios from a seed
  (:mod:`lot_lineage.diffharness.scenario`);
* predicts every outcome with its own in-memory model that re-derives the
  contract from scratch and never imports production parsing, validation or
  sealing code (:mod:`lot_lineage.diffharness.oracle`);
* drives the system under test as real subprocesses against a fresh empty
  SQLite library per run and compares receipts, database state, sealed JSON
  bytes and export files (:mod:`lot_lineage.diffharness.runner`);
* minimises any counterexample by deleting operations, deleting CSV records
  and shrinking masses, re-running every candidate on a fresh empty library
  (:mod:`lot_lineage.diffharness.reduce`);
* replays a self-contained counterexample without needing the original seed
  or any generated file.

Only the Python standard library is used.  Nothing in this package imports
:mod:`lot_lineage.intake`, :mod:`lot_lineage.graph` or any other production
module; the system under test is reached exclusively through its command
line.
"""

FORMAT_SCENARIO = "lot_lineage_diff_scenario_v1"
FORMAT_COUNTEREXAMPLE = "lot_lineage_diff_counterexample_v1"

__all__ = ["FORMAT_SCENARIO", "FORMAT_COUNTEREXAMPLE"]
