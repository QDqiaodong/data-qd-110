"""Tests for the offline weighing-ticket intake library.

Hand-computed expectations are written literally and independently of the
implementation: the delivered example is derived first on paper in
``test_end_to_end_cli_matches_independent_expectations`` and then checked
through the real command line, the old batch CLI and direct SQLite queries.

Deterministic concurrency tests use an explicit CLI ``--hook`` handshake
(print a line, wait for a line) instead of sleeping or guessing timings.
"""
from __future__ import annotations

import hashlib
import json
import os
import select
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock

from lot_lineage.intake.model import ContractError, Ticket, parse_csv
from lot_lineage.intake.store import (
    _build_document,
    _detect,
    _Facts,
    facts_from_tickets,
    export,
    ingest,
    seal,
    status,
)
from lot_lineage.pipeline import build as build_batch
from lot_lineage.recycle.pipeline import build as build_recycle

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
FIXTURES = ROOT / "fixtures"
BATCH1 = FIXTURES / "intake" / "batch1.csv"
BATCH2 = FIXTURES / "intake" / "batch2.csv"


def csv_text(rows, header="ticket_id,kind,lot_id,input_lot_id,mass,unit"):
    lines = [header]
    for row in rows:
        lines.append(",".join(str(cell) for cell in row))
    return "\n".join(lines) + "\n"


class Helpers(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "intake.sqlite"

    def write_csv(self, name, rows, header="ticket_id,kind,lot_id,input_lot_id,mass,unit"):
        path = self.root / name
        path.write_text(csv_text(rows, header), encoding="utf-8")
        return path

    def write_text(self, name, text):
        path = self.root / name
        path.write_text(text, encoding="utf-8")
        return path

    def q(self, sql, params=()):
        with sqlite3.connect(self.db) as conn:
            return conn.execute(sql, params).fetchall()

    def counts(self, campaign):
        return {
            "tickets": self.q(
                "SELECT COUNT(*) FROM intake_ticket WHERE campaign_id=?", (campaign,)
            )[0][0],
            "sources": self.q(
                "SELECT COUNT(*) FROM intake_source WHERE campaign_id=?", (campaign,)
            )[0][0],
            "positions": self.q(
                "SELECT COUNT(*) FROM intake_source_ticket WHERE campaign_id=?", (campaign,)
            )[0][0],
        }


# ---------------------------------------------------------------------------
# Parsing / single-row structure and numbers
# ---------------------------------------------------------------------------

class ParseContractTest(Helpers):
    def parse(self, rows, header="ticket_id,kind,lot_id,input_lot_id,mass,unit"):
        return parse_csv(csv_text(rows, header).encode("utf-8"))

    def assertRejected(self, rows, pattern, header="ticket_id,kind,lot_id,input_lot_id,mass,unit"):
        with self.assertRaisesRegex(ContractError, pattern):
            self.parse(rows, header)

    def test_header_must_be_exact_and_ordered(self):
        good = csv_text([["T", "raw", "A", "", "1", "g"]])
        self.assertEqual(len(parse_csv(good.encode())), 1)
        for header in (
            "kind,ticket_id,lot_id,input_lot_id,mass,unit",          # reordered
            "ticket_id,kind,lot_id,input_lot_id,mass",               # missing one
            "ticket_id,kind,lot_id,input_lot_id,mass,unit,extra",     # extra
            "Ticket_id,kind,lot_id,input_lot_id,mass,unit",          # case
        ):
            with self.assertRaisesRegex(ContractError, "header must be exactly"):
                parse_csv(csv_text([["T", "raw", "A", "", "1", "g"]], header).encode())

    def test_empty_file_and_header_only(self):
        with self.assertRaisesRegex(ContractError, "file is empty"):
            parse_csv(b"")
        with self.assertRaisesRegex(ContractError, "no data records"):
            parse_csv(b"ticket_id,kind,lot_id,input_lot_id,mass,unit\n")

    def test_ragged_row_localized_by_record(self):
        self.assertRejected(
            [["T1", "raw", "A", "", "1", "g"], ["T2", "raw", "B", "", "2"]],
            r"record 2.*expected 6 fields, got 5",
        )

    def test_kind_and_unit(self):
        self.assertRejected([["T", "scrap", "A", "", "1", "g"]], r"T.*kind must be one of")
        self.assertRejected([["T", "raw", "A", "", "1", "KG"]], r"T.*unit must be exactly")
        self.assertRejected([["T", "raw", "A", "", "1", "lb"]], r"T.*unit must be exactly")

    def test_input_lot_column_rules(self):
        self.assertRejected([["T", "input", "X", "", "1", "g"]], r"input.*require.*input_lot_id")
        self.assertRejected(
            [["T", "raw", "A", "B", "1", "g"]], r"input_lot_id must be empty for kind raw"
        )
        self.assertRejected(
            [["T", "output", "X", "B", "1", "g"]], r"input_lot_id must be empty for kind output"
        )
        self.assertRejected(
            [["T", "loss", "X", "B", "1", "g"]], r"input_lot_id must be empty for kind loss"
        )
        ticket = self.parse([["T", "input", "X", "A", "1", "g"]])[0]
        self.assertEqual(ticket.input_lot_id, "A")

    def test_identifiers(self):
        for ticket_id, lot in (("", "A"), (" T", "A"), ("T", " A"), ("T ", "A"), ("T", "A ")):
            self.assertRejected(
                [[ticket_id, "raw", lot, "", "1", "g"]],
                r"record 1.*(ticket_id|lot_id) must be a non-empty string",
            )
        # case is significant: different tickets, not duplicates
        tickets = self.parse(
            [["t", "raw", "a", "", "1", "g"], ["T", "raw", "A", "", "1", "g"]]
        )
        self.assertEqual({t.ticket_id for t in tickets}, {"t", "T"})

    def test_mass_text_rejects_sign_space_exponent_nan_infinity_fraction_digits(self):
        for bad in ("-1", "+1", " 1", "1 ", "1e3", "1E3", "NaN", "Infinity", "nan",
                    "1.2345", ".5", "5.", "0x10", "1_000"):
            self.assertRejected([["T", "raw", "A", "", bad, "g"]], r"T.*mass must be a plain decimal")

    def test_mass_must_be_whole_grams_and_limits(self):
        self.assertRejected([["T", "raw", "A", "", "1.5", "g"]], r"not a whole number of grams")
        self.assertRejected([["T", "raw", "A", "", "0", "g"]], r"between 1 and 10\^12")
        self.assertRejected([["T", "loss", "X", "", "1000000000.001", "kg"]], r"between 0 and 10\^12")
        self.assertRejected([["T", "raw", "A", "", "1000000000001", "g"]], r"between 1 and 10\^12")
        self.assertRejected([["T", "loss", "X", "", "0.001", "g"]], r"not a whole number of grams")
        # loss may be exactly zero; positive kinds may not
        self.assertEqual(self.parse([["T", "loss", "X", "", "0.000", "g"]])[0].grams, 0)
        self.assertEqual(self.parse([["T", "raw", "A", "", "1", "g"]])[0].grams, 1)

    def test_equivalent_units_normalize_without_float(self):
        tickets = self.parse(
            [
                ["T1", "input", "X", "A", "0.500", "kg"],
                ["T2", "input", "X", "A", "500", "g"],
                ["T3", "raw", "A", "", "999999999.999", "kg"],
                ["T4", "raw", "A", "", "0001.500", "kg"],
            ]
        )
        self.assertEqual([t.grams for t in tickets], [500, 500, 999_999_999_999, 1500])
        self.assertEqual(tickets[0].business_key, ( "input", "X", "A", 500))
        self.assertEqual(tickets[0].business_key, tickets[1].business_key)

    def test_record_and_line_numbering(self):
        tickets = self.parse(
            [["T1", "raw", "A", "", "1", "g"], ["T2", "raw", "B", "", "2", "g"]]
        )
        self.assertEqual([(t.record, t.line) for t in tickets], [(1, 2), (2, 3)])

    def test_quoting_with_embedded_newline_keeps_record_and_line(self):
        text = (
            "ticket_id,kind,lot_id,input_lot_id,mass,unit\n"
            "T1,raw,A,,1,g\n"
            "T2,raw,\"B\",,2,g\n"
        )
        self.assertEqual(parse_csv(text.encode())[1].line, 3)
        quoted_newline = (
            "ticket_id,kind,lot_id,input_lot_id,mass,unit\n"
            "T1,raw,A,,1,g\n"
            "T2,raw,\"B\ncontinued\",,2,g\n"
        )
        tickets = parse_csv(quoted_newline.encode())
        self.assertEqual(tickets[1].record, 2)
        self.assertEqual(tickets[1].line, 4)


# ---------------------------------------------------------------------------
# Identity, replay and conflict handling (same and cross file)
# ---------------------------------------------------------------------------

class IngestIdentityTest(Helpers):
    def test_equivalent_unit_replay_adds_no_ticket(self):
        ingest(self.db, "c1", self.write_csv("a.csv", [["T1", "input", "X", "A", "0.500", "kg"]]))
        receipt = ingest(
            self.db, "c1", self.write_csv("b.csv", [["T1", "input", "X", "A", "500", "g"]])
        )
        self.assertEqual(receipt["new_tickets"], 0)
        self.assertEqual(receipt["replayed_occurrences"], 1)
        self.assertEqual(receipt["new_source"], 1)
        self.assertEqual(self.counts("c1")["tickets"], 1)
        self.assertEqual(self.counts("c1")["sources"], 2)

    def test_same_id_same_file_replay_and_conflict(self):
        replay = self.write_csv(
            "same.csv",
            [["T1", "input", "X", "A", "0.5", "kg"], ["T1", "input", "X", "A", "500", "g"]],
        )
        receipt = ingest(self.db, "c1", replay)
        self.assertEqual((receipt["new_tickets"], receipt["replayed_occurrences"]), (1, 1))
        # one source, two record positions
        positions = self.q("SELECT record_no, line_no FROM intake_source_ticket ORDER BY record_no")
        self.assertEqual(positions, [(1, 2), (2, 3)])

        before = self.counts("c1")
        conflict = self.write_csv(
            "conflict.csv",
            [["T2", "raw", "A", "", "1", "g"], ["T1", "input", "X", "A", "501", "g"]],
        )
        with self.assertRaisesRegex(ContractError, r"T1.*conflicts"):
            ingest(self.db, "c1", conflict)
        # whole file rolled back, including the valid new T2
        self.assertEqual(self.counts("c1"), before)

    def test_cross_file_conflict_rolls_back_entire_file(self):
        ingest(self.db, "c1", self.write_csv("a.csv", [["T1", "raw", "A", "", "1", "g"]]))
        before = self.counts("c1")
        mixed = self.write_csv(
            "mixed.csv",
            [["T2", "raw", "B", "", "2", "g"], ["T1", "raw", "A", "", "2", "g"]],
        )
        with self.assertRaisesRegex(ContractError, r"T1.*conflicts with stored content"):
            ingest(self.db, "c1", mixed)
        self.assertEqual(self.counts("c1"), before)

    def test_distinct_ticket_same_content_is_independent(self):
        receipt = ingest(
            self.db, "c1",
            self.write_csv(
                "two.csv",
                [["T1", "raw", "A", "", "600", "g"], ["T2", "raw", "A", "", "600", "g"]],
            ),
        )
        self.assertEqual(receipt["new_tickets"], 2)
        self.assertEqual(self.q("SELECT COUNT(*) FROM intake_ticket")[0][0], 2)

    def test_identical_bytes_add_no_source_or_positions(self):
        path = self.write_csv("a.csv", [["T1", "raw", "A", "", "1", "g"]])
        ingest(self.db, "c1", path)
        before = self.counts("c1")
        receipt = ingest(self.db, "c1", path)
        self.assertEqual(receipt["new_source"], 0)
        self.assertEqual(receipt["new_tickets"], 0)
        self.assertEqual(self.counts("c1"), before)

    def test_cross_file_conflict_in_mixed_file_rolls_back_valid_new_ticket(self):
        # cross-file conflict inside a mixed file rolls back the valid new one too
        ingest(self.db, "c1", self.write_csv("base.csv",
                [["T1", "input", "X", "A", "0.500", "kg"]]))
        before = self.counts("c1")
        mixed = self.write_csv(
            "mixed.csv",
            [["T2", "input", "X", "B", "100", "g"],
             ["T1", "input", "X", "A", "501", "g"]],
        )
        with self.assertRaisesRegex(ContractError, r"T1.*conflicts with stored"):
            ingest(self.db, "c1", mixed)
        self.assertEqual(self.counts("c1"), before)

    def test_replay_keeps_every_source_position(self):
        ingest(self.db, "c1", self.write_csv("a.csv", [["T1", "raw", "A", "", "1", "g"]]))
        ingest(self.db, "c1", self.write_csv("b.csv", [["T1", "raw", "A", "", "1.0", "g"]]))
        rows = self.q(
            "SELECT s.source_sha256, st.record_no, st.line_no "
            "FROM intake_source_ticket st JOIN intake_source s USING(source_pk) "
            "WHERE st.ticket_id='T1' ORDER BY s.source_pk"
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(len({row[0] for row in rows}), 2)
        self.assertEqual([(row[1], row[2]) for row in rows], [(1, 2), (1, 2)])

    def test_invalid_file_does_not_touch_database(self):
        with self.assertRaises(ContractError):
            ingest(self.db, "c1", self.write_csv("bad.csv", [["T", "raw", "A", "", "NaN", "g"]]))
        # Parsing and hashing happen before any database access.
        self.assertFalse(self.db.exists())


# ---------------------------------------------------------------------------
# Sealing: full production contract on all received tickets
# ---------------------------------------------------------------------------

class SealContractTest(Helpers):
    def batch(self, name, rows):
        return ingest(self.db, "c1", self.write_csv(name, rows))

    def base_rows(self):
        return [
            ["R1", "raw", "A", "", "1000", "g"],
            ["R2", "raw", "B", "", "500", "g"],
            ["O1", "output", "X", "", "900", "g"],
            ["I1", "input", "X", "A", "600", "g"],
            ["I2", "input", "X", "B", "400", "g"],
            ["L1", "loss", "X", "", "100", "g"],
        ]

    def test_minimal_campaign_seals(self):
        self.batch("all.csv", self.base_rows())
        receipt = seal(self.db, "c1")
        self.assertEqual(receipt["status"], "sealed")
        self.assertFalse(receipt["already_sealed"])
        self.assertEqual(receipt["seal_sha256"], receipt["seal_sha256"].lower())
        self.assertEqual(len(receipt["seal_sha256"]), 64)

    def test_definition_cardinality(self):
        rows = self.base_rows()
        rows.append(["R1b", "raw", "A", "", "1000", "g"])  # second definition
        rows.append(["O1b", "output", "X", "", "900", "g"])  # both kinds at once
        self.batch("dup.csv", rows)
        with self.assertRaisesRegex(ContractError, r"lot 'A'.*exactly one"):
            seal(self.db, "c1")

    def test_output_requires_input_and_exactly_one_loss(self):
        rows = [r for r in self.base_rows() if r[1] != "loss"]
        self.batch("noloss.csv", rows)
        with self.assertRaisesRegex(ContractError, r"output lot 'X'.*exactly one loss"):
            seal(self.db, "c1")
        # explicit zero loss is required and accepted
        zero_db = self.root / "zero.sqlite"
        rows = [list(r) for r in self.base_rows()]
        rows[-1] = ["L1", "loss", "X", "", "0", "g"]
        rows[2] = ["O1", "output", "X", "", "1000", "g"]
        ingest(zero_db, "c1", self.write_csv("zero.csv", rows))
        self.assertEqual(seal(zero_db, "c1")["status"], "sealed")

    def test_raw_must_not_be_output_or_loss(self):
        rows = self.base_rows() + [["O2", "output", "A", "", "10", "g"],
                                   ["I3", "input", "A", "B", "10", "g"]]
        self.batch("rawout.csv", rows)
        with self.assertRaisesRegex(ContractError, r"lot 'A'.*exactly one"):
            seal(self.db, "c1")

        other = self.root / "other.sqlite"
        rows = self.base_rows() + [["L2", "loss", "A", "", "0", "g"]]
        ingest(other, "c1", self.write_csv("rawloss.csv", rows))
        with self.assertRaisesRegex(ContractError, r"raw lot 'A'.*must not have loss"):
            seal(other, "c1")

        third = self.root / "third.sqlite"
        rows = self.base_rows() + [["I3", "input", "A", "B", "1", "g"]]
        rows = [r for r in rows if r[0] != "I2"]  # keep 600+1 != 900+100 irrelevant
        ingest(third, "c1", self.write_csv("rawinput.csv", rows))
        with self.assertRaisesRegex(ContractError, r"raw lot 'A'.*must not have input"):
            seal(third, "c1")

    def test_reference_and_mass_conservation(self):
        rows = self.base_rows()
        rows[4] = ["I2", "input", "X", "B", "399", "g"]
        rows[3] = ["I1", "input", "X", "A", "600", "g"]
        self.batch("short.csv", rows)
        with self.assertRaisesRegex(ContractError, r"mass not conserved"):
            seal(self.db, "c1")

    def test_unknown_reference_is_missing_definition(self):
        rows = self.base_rows()
        rows.append(["I9", "input", "X", "GHOST", "1", "g"])
        rows[2] = ["O1", "output", "X", "", "901", "g"]
        self.batch("ghost.csv", rows)
        with self.assertRaisesRegex(ContractError, r"lot 'GHOST'.*exactly one"):
            seal(self.db, "c1")

    def test_overconsumption(self):
        rows = [
            ["R1", "raw", "A", "", "100", "g"],
            ["O1", "output", "X", "", "1000", "g"],
            ["I1", "input", "X", "A", "600", "g"],
            ["I2", "input", "X", "B", "400", "g"],
            ["R2", "raw", "B", "", "400", "g"],
            ["L1", "loss", "X", "", "0", "g"],
        ]
        self.batch("over.csv", rows)
        with self.assertRaisesRegex(ContractError, r"lot 'A' over-consumed"):
            seal(self.db, "c1")

    def test_cycle_even_in_unrelated_component(self):
        rows = self.base_rows() + [
            ["OU", "output", "U", "", "10", "g"],
            ["IU", "input", "U", "V", "10", "g"],
            ["LU", "loss", "U", "", "0", "g"],
            ["OV", "output", "V", "", "10", "g"],
            ["IV", "input", "V", "U", "10", "g"],
            ["LV", "loss", "V", "", "0", "g"],
        ]
        self.batch("cycle.csv", rows)
        with self.assertRaisesRegex(ContractError, "cyclic"):
            seal(self.db, "c1")

    def test_multiple_input_tickets_aggregate_to_one_edge(self):
        rows = [
            ["R1", "raw", "A", "", "1000", "g"],
            ["O1", "output", "X", "", "1000", "g"],
            ["I1", "input", "X", "A", "300", "g"],
            ["I2", "input", "X", "A", "700", "g"],
            ["L1", "loss", "X", "", "0", "g"],
        ]
        self.batch("agg.csv", rows)
        receipt = seal(self.db, "c1")
        self.assertEqual(receipt["status"], "sealed")
        payload = self.q("SELECT sealed_json FROM intake_campaign WHERE campaign_id='c1'")[0][0]
        document = json.loads(bytes(payload))
        self.assertEqual(
            document["recipes"][0]["inputs"], [{"lot_id": "A", "mass_g": 1000}]
        )
        edge = self.q(
            "SELECT total_grams, ticket_count FROM intake_sealed_edge "
            "WHERE output_lot='X' AND input_lot='A'"
        )
        self.assertEqual(edge, [("1000", 2)])
        tickets = self.q(
            "SELECT ticket_id FROM intake_sealed_edge_ticket "
            "WHERE output_lot='X' AND input_lot='A' ORDER BY ticket_id"
        )
        self.assertEqual(tickets, [("I1",), ("I2",)])

    def test_aggregate_edge_above_limit_is_rejected_and_rolled_back(self):
        # Two individually legal 10^12 g tickets whose sum exceeds the edge limit.
        rows = [
            ["R1", "raw", "A", "", "1000000000000", "g"],
            ["O1", "output", "X", "", "1000000000000", "g"],
            ["I1", "input", "X", "A", "1000000000000", "g"],
            ["I2", "input", "X", "A", "1", "g"],
            ["L1", "loss", "X", "", "1", "g"],
        ]
        self.batch("big.csv", rows)
        with self.assertRaisesRegex(ContractError, "exceeds 10\\^12"):
            seal(self.db, "c1")
        state = self.q("SELECT status, seal_sha256 FROM intake_campaign WHERE campaign_id='c1'")
        self.assertEqual(state, [("open", None)])
        self.assertEqual(self.q("SELECT COUNT(*) FROM intake_sealed_edge")[0][0], 0)

    def test_missing_tickets_then_supplemented(self):
        half = [row for row in self.base_rows() if row[1] in ("input", "loss")]
        self.batch("partial.csv", half)
        with self.assertRaisesRegex(ContractError, "exactly one raw or output"):
            seal(self.db, "c1")
        self.assertEqual(
            self.q("SELECT status FROM intake_campaign WHERE campaign_id='c1'"), [("open",)]
        )
        self.assertEqual(self.counts("c1")["tickets"], 3)
        supplement = [row for row in self.base_rows() if row[1] in ("raw", "output")]
        self.batch("rest.csv", supplement)
        self.assertEqual(seal(self.db, "c1")["status"], "sealed")

    def test_unknown_and_empty_campaign_rejected(self):
        ingest(self.db, "c1", self.write_csv("a.csv", [["T1", "raw", "A", "", "1", "g"]]))
        with self.assertRaisesRegex(ContractError, "unknown campaign 'nope'"):
            seal(self.db, "nope")
        with sqlite3.connect(self.db) as conn:
            conn.execute("INSERT INTO intake_campaign (campaign_id,status) VALUES ('empty','open')")
        with self.assertRaisesRegex(ContractError, "no tickets"):
            seal(self.db, "empty")
        self.assertEqual(
            self.q("SELECT status FROM intake_campaign WHERE campaign_id='empty'"), [("open",)]
        )

    def test_sealed_replay_is_idempotent_and_ingest_refused(self):
        self.batch("all.csv", self.base_rows())
        first = seal(self.db, "c1")
        before = {
            "campaign": self.q("SELECT status, seal_sha256, sealed_json FROM intake_campaign")[0],
            "edges": self.q("SELECT * FROM intake_sealed_edge ORDER BY output_lot,input_lot"),
        }
        second = seal(self.db, "c1")
        self.assertTrue(second["already_sealed"])
        self.assertEqual(second["seal_sha256"], first["seal_sha256"])
        after = {
            "campaign": self.q("SELECT status, seal_sha256, sealed_json FROM intake_campaign")[0],
            "edges": self.q("SELECT * FROM intake_sealed_edge ORDER BY output_lot,input_lot"),
        }
        self.assertEqual(before, after)
        # even a byte-identical replay of an already received file is refused
        with self.assertRaisesRegex(ContractError, "sealed"):
            ingest(self.db, "c1", self.write_csv("all.csv", self.base_rows()))
        with self.assertRaisesRegex(ContractError, "sealed"):
            ingest(self.db, "c1", self.write_csv("new.csv", [["N", "raw", "Z", "", "1", "g"]]))

    def test_seal_failure_rolls_back_all_seal_writes(self):
        self.batch("all.csv", self.base_rows())
        before_tickets = self.q("SELECT * FROM intake_ticket ORDER BY ticket_id")
        before_sources = self.q("SELECT source_pk, source_sha256 FROM intake_source")
        with mock.patch(
            "lot_lineage.intake.store._insert_sealed_edges",
            side_effect=sqlite3.OperationalError("injected seal write failure"),
        ):
            with self.assertRaises(sqlite3.OperationalError):
                seal(self.db, "c1")
        self.assertEqual(
            self.q("SELECT status, seal_sha256, sealed_json FROM intake_campaign"), [("open", None, None)]
        )
        self.assertEqual(self.q("SELECT COUNT(*) FROM intake_sealed_edge")[0][0], 0)
        self.assertEqual(self.q("SELECT * FROM intake_ticket ORDER BY ticket_id"), before_tickets)
        self.assertEqual(self.q("SELECT source_pk, source_sha256 FROM intake_source"), before_sources)
        self.assertEqual(seal(self.db, "c1")["status"], "sealed")

    def test_sealed_json_bytes_deterministic_across_arrival_order(self):
        rows = self.base_rows()
        ingest(self.db, "c1", self.write_csv("p1.csv", rows[:3]))
        ingest(self.db, "c1", self.write_csv("p2.csv", rows[3:]))
        seal(self.db, "c1")
        a = self.q("SELECT sealed_json FROM intake_campaign WHERE campaign_id='c1'")[0][0]

        other = self.root / "other.sqlite"
        # same ticket IDs, reversed order, equivalent spellings, single file
        rewritten = []
        for row in reversed(rows):
            row = list(row)
            if row[0] == "R1":
                row[4] = "1.000"
                row[5] = "kg"
            if row[0] == "R2":
                row[4] = "0.500"
                row[5] = "kg"
            rewritten.append(row)
        ingest(other, "c1", self.write_csv("all_reversed.csv", rewritten))
        seal(other, "c1")
        with sqlite3.connect(other) as conn:
            b = conn.execute("SELECT sealed_json FROM intake_campaign WHERE campaign_id='c1'").fetchone()[0]
        self.assertEqual(bytes(a), bytes(b))
        self.assertEqual(
            hashlib.sha256(bytes(a)).hexdigest(),
            self.q("SELECT seal_sha256 FROM intake_campaign WHERE campaign_id='c1'")[0][0],
        )


# ---------------------------------------------------------------------------
# Campaign isolation
# ---------------------------------------------------------------------------

class CampaignIsolationTest(Helpers):
    def test_same_ids_and_sources_do_not_mix(self):
        rows1 = [["T1", "raw", "A", "", "10", "g"], ["O1", "output", "X", "", "10", "g"],
                 ["I1", "input", "X", "A", "10", "g"], ["L1", "loss", "X", "", "0", "g"]]
        rows2 = [["T1", "raw", "A", "", "20", "g"], ["O1", "output", "Y", "", "20", "g"],
                 ["I1", "input", "Y", "A", "20", "g"], ["L1", "loss", "Y", "", "0", "g"]]
        ingest(self.db, "c1", self.write_csv("a.csv", rows1))
        ingest(self.db, "c2", self.write_csv("b.csv", rows2))
        self.assertEqual(self.counts("c1"), {"tickets": 4, "sources": 1, "positions": 4})
        self.assertEqual(self.counts("c2"), {"tickets": 4, "sources": 1, "positions": 4})
        seal(self.db, "c1")
        seal(self.db, "c2")
        docs = {
            campaign: json.loads(bytes(self.q(
                "SELECT sealed_json FROM intake_campaign WHERE campaign_id=?", (campaign,)
            )[0][0]))
            for campaign in ("c1", "c2")
        }
        self.assertEqual(docs["c1"]["lots"][-1]["lot_id"], "X")
        self.assertEqual(docs["c2"]["lots"][-1]["lot_id"], "Y")
        # sealing one campaign leaves the other's ticket rows untouched
        self.assertEqual(self.counts("c2")["tickets"], 4)
        with self.assertRaisesRegex(ContractError, "sealed"):
            ingest(self.db, "c1", self.write_csv("a.csv", rows1))
        # c2 is sealed too; a third open campaign is unaffected
        ingest(self.db, "c3", self.write_csv("c.csv", rows1))
        self.assertEqual(self.counts("c3")["tickets"], 4)


# ---------------------------------------------------------------------------
# Status / export and file-safety boundaries
# ---------------------------------------------------------------------------

class StatusExportTest(Helpers):
    def setup_sealed(self):
        rows = [
            ["R1", "raw", "A", "", "1000", "g"],
            ["O1", "output", "X", "", "900", "g"],
            ["I1", "input", "X", "A", "1000", "g"],
            ["L1", "loss", "X", "", "100", "g"],
        ]
        ingest(self.db, "c1", self.write_csv("a.csv", rows))
        seal(self.db, "c1")

    def test_status_read_only_and_does_not_create(self):
        missing = self.root / "missing.sqlite"
        with self.assertRaisesRegex(ContractError, "does not exist"):
            status(missing, "c1")
        self.assertFalse(missing.exists())

        ingest(self.db, "c1", self.write_csv("a.csv", [["T", "raw", "A", "", "1", "g"]]))
        before = self.db.read_bytes()
        info = status(self.db, "c1")
        self.assertEqual(info["status"], "open")
        self.assertEqual((info["ticket_count"], info["source_count"]), (1, 1))
        self.assertIsNone(info["seal_sha256"])
        self.assertEqual(self.db.read_bytes(), before)
        with self.assertRaisesRegex(ContractError, "unknown campaign"):
            status(self.db, "nope")

    def test_export_requires_sealed_existing_campaign(self):
        output = self.root / "out.json"
        with self.assertRaisesRegex(ContractError, "does not exist"):
            export(self.db, "c1", output)
        self.assertFalse(self.db.exists())
        self.assertFalse(output.exists())

        ingest(self.db, "c1", self.write_csv("a.csv", [["T", "raw", "A", "", "1", "g"]]))
        with self.assertRaisesRegex(ContractError, "is open"):
            export(self.db, "c1", output)
        self.assertFalse(output.exists())
        with self.assertRaisesRegex(ContractError, "unknown campaign"):
            export(self.db, "nope", output)
        self.assertFalse(output.exists())

    def test_export_is_repeatable_and_read_only_on_database(self):
        self.setup_sealed()
        before = self.db.read_bytes()
        out1 = self.root / "one.json"
        out2 = self.root / "two.json"
        r1 = export(self.db, "c1", out1)
        r2 = export(self.db, "c1", out2)
        self.assertEqual(out1.read_bytes(), out2.read_bytes())
        self.assertEqual(r1["sha256"], r2["sha256"])
        self.assertEqual(hashlib.sha256(out1.read_bytes()).hexdigest(), r1["sha256"])
        self.assertEqual(self.db.read_bytes(), before)
        # exported JSON is directly consumable by the existing batch build
        built = build_batch(out1, self.root / "batch.sqlite")
        self.assertEqual((built["raw_input_g"], built["process_loss_g"], built["available_g"]),
                         (1000, 100, 900))
    def test_output_must_not_alias_database(self):
        self.setup_sealed()
        with self.assertRaisesRegex(ContractError, "same file"):
            export(self.db, "c1", self.db)
        link = self.root / "alias.sqlite"
        os.symlink(self.db, link)
        with self.assertRaisesRegex(ContractError, "same file"):
            export(self.db, "c1", link)
        self.assertEqual(_detect(sqlite3.connect(self.db)), "intake")

    def test_export_replace_failure_keeps_old_output_and_is_retryable(self):
        self.setup_sealed()
        output = self.root / "out.json"
        output.write_bytes(b"OLD-CONTENT")
        with mock.patch(
            "lot_lineage.intake.store.os.replace",
            side_effect=OSError("injected replace failure"),
        ):
            with self.assertRaises(OSError):
                export(self.db, "c1", output)
        self.assertEqual(output.read_bytes(), b"OLD-CONTENT")
        self.assertEqual([p.name for p in self.root.iterdir() if p.name.startswith(".intake-")], [])
        # campaign remains sealed and a retry succeeds
        self.assertEqual(status(self.db, "c1")["status"], "sealed")
        export(self.db, "c1", output)
        self.assertTrue(output.read_bytes().startswith(b"{"))

    def test_export_after_seal_is_repeatable_across_seal_state(self):
        # Sealing twice and exporting twice must all agree byte-for-byte.
        self.setup_sealed()
        first = seal(self.db, "c1")
        second = seal(self.db, "c1")
        self.assertEqual(first["seal_sha256"], second["seal_sha256"])
        a = self.root / "a.json"
        b = self.root / "b.json"
        export(self.db, "c1", a)
        export(self.db, "c1", b)
        self.assertEqual(a.read_bytes(), b.read_bytes())
        self.assertEqual(a.read_bytes(), self.q(
            "SELECT sealed_json FROM intake_campaign WHERE campaign_id='c1'")[0][0])

    def test_export_does_not_treat_database_commit_as_output_transaction(self):
        # After a failed export replacement, the sealed fact remains and a
        # later retry publishes the same bytes.
        self.setup_sealed()
        output = self.root / "out.json"
        with mock.patch("lot_lineage.intake.store.os.replace",
                        side_effect=OSError("injected")):
            with self.assertRaises(OSError):
                export(self.db, "c1", output)
        self.assertFalse(output.exists())
        self.assertEqual(status(self.db, "c1")["status"], "sealed")
        export(self.db, "c1", output)
        stored = self.q("SELECT sealed_json FROM intake_campaign WHERE campaign_id='c1'")[0][0]
        self.assertEqual(output.read_bytes(), stored)

    def test_export_rejects_corrupted_sealed_json(self):
        self.setup_sealed()
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "UPDATE intake_campaign SET sealed_json=? WHERE campaign_id='c1'",
                (sqlite3.Binary(b'{"lots": [], "recipes": []}'),),
            )
        with self.assertRaisesRegex(ContractError, "digest mismatch"):
            export(self.db, "c1", self.root / "out.json")
        self.assertFalse((self.root / "out.json").exists())

    def test_status_reports_source_count_including_replays(self):
        self.setup_sealed()
        info = status(self.db, "c1")
        self.assertEqual((info["status"], info["ticket_count"], info["source_count"]),
                         ("sealed", 4, 1))
        self.assertEqual(info["seal_sha256"], self.q(
            "SELECT seal_sha256 FROM intake_campaign WHERE campaign_id='c1'")[0][0])

    def test_export_write_failure_leaves_no_temp_and_old_target(self):
        self.setup_sealed()
        output = self.root / "out.json"
        output.write_bytes(b"KEEP")
        with mock.patch("lot_lineage.intake.store.os.replace",
                        side_effect=OSError("injected write failure")):
            with self.assertRaises(OSError):
                export(self.db, "c1", output)
        self.assertEqual(output.read_bytes(), b"KEEP")
        self.assertEqual([p.name for p in self.root.iterdir()
                          if p.name.startswith(".intake-")], [])
        self.assertEqual(status(self.db, "c1")["status"], "sealed")
        # retry after the injected failure succeeds
        export(self.db, "c1", output)
        self.assertTrue(output.read_bytes().startswith(b"{"))

    def test_schema_declares_keys_foreign_keys_and_seal_boundary(self):
        ingest(self.db, "c1", self.write_csv("a.csv", [["T", "raw", "A", "", "1", "g"]]))
        with sqlite3.connect(self.db) as conn:
            tables = {
                row[0]: [tuple(info) for info in conn.execute(f"PRAGMA table_info({row[0]})")]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'intake_%'"
                )
            }
        self.assertEqual(
            set(tables),
            {"intake_meta", "intake_campaign", "intake_source", "intake_ticket",
             "intake_source_ticket", "intake_sealed_edge", "intake_sealed_edge_ticket"},
        )
        keys = {table: [row[1] for row in info if row[5]] for table, info in tables.items()}
        self.assertEqual(keys["intake_ticket"], ["campaign_id", "ticket_id"])
        self.assertEqual(keys["intake_source"], ["source_pk"])
        self.assertEqual(keys["intake_source_ticket"], ["source_pk", "record_no"])
        self.assertEqual(keys["intake_sealed_edge"],
                         ["campaign_id", "output_lot", "input_lot"])


# ---------------------------------------------------------------------------
# Foreign formats and write-failure rollback
# ---------------------------------------------------------------------------

class ForeignFormatTest(Helpers):
    def test_old_lineage_and_recycle_databases_are_refused_untouched(self):
        lineage = self.root / "lineage.sqlite"
        build_batch(FIXTURES / "lots.json", lineage)
        recycle = self.root / "recycle.sqlite"
        build_recycle(FIXTURES / "recycle_pq.json", recycle)
        source = self.write_csv("a.csv", [["T", "raw", "A", "", "1", "g"]])
        for database in (lineage, recycle):
            before = database.read_bytes()
            with self.assertRaisesRegex(ContractError, "not an intake ticket library"):
                ingest(database, "c1", source)
            with self.assertRaisesRegex(ContractError, "not an intake ticket library"):
                seal(database, "c1")
            with self.assertRaisesRegex(ContractError, "not an intake ticket library"):
                export(database, "c1", self.root / "x.json")
            with self.assertRaisesRegex(ContractError, "not an intake ticket library"):
                status(database, "c1")
            self.assertEqual(database.read_bytes(), before)
        self.assertFalse((self.root / "x.json").exists())

    def test_unknown_schema_family_refused(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("CREATE TABLE intake_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            conn.execute("INSERT INTO intake_meta VALUES ('schema_family','something_else')")
            conn.execute("INSERT INTO intake_meta VALUES ('schema_version','1')")
        before = self.db.read_bytes()
        with self.assertRaisesRegex(ContractError, "unsupported schema family"):
            ingest(self.db, "c1", self.write_csv("a.csv", [["T", "raw", "A", "", "1", "g"]]))
        self.assertEqual(self.db.read_bytes(), before)

    def test_ingest_write_failure_rolls_back_whole_file(self):
        good = self.write_csv(
            "good.csv",
            [["T1", "raw", "A", "", "1", "g"], ["T2", "raw", "B", "", "2", "g"]],
        )
        with mock.patch(
            "lot_lineage.intake.store._insert_positions",
            side_effect=sqlite3.OperationalError("injected write failure"),
        ):
            with self.assertRaises(sqlite3.OperationalError):
                ingest(self.db, "c1", good)
        if self.db.exists():
            self.assertEqual(_detect(sqlite3.connect(self.db)), "empty")
        # retry after the failure succeeds
        receipt = ingest(self.db, "c1", good)
        self.assertEqual(receipt["new_tickets"], 2)
        self.assertEqual(self.counts("c1")["tickets"], 2)

    def test_seal_write_failure_preserves_open_state_and_retry(self):
        rows = [
            ["R1", "raw", "A", "", "10", "g"],
            ["O1", "output", "X", "", "10", "g"],
            ["I1", "input", "X", "A", "10", "g"],
            ["L1", "loss", "X", "", "0", "g"],
        ]
        ingest(self.db, "c1", self.write_csv("a.csv", rows))
        with mock.patch(
            "lot_lineage.intake.store._write_seal",
            side_effect=sqlite3.OperationalError("injected seal failure"),
        ):
            with self.assertRaises(sqlite3.OperationalError):
                seal(self.db, "c1")
        self.assertEqual(status(self.db, "c1")["status"], "open")
        self.assertEqual(seal(self.db, "c1")["status"], "sealed")


# ---------------------------------------------------------------------------
# Precision and unbounded aggregate arithmetic
# ---------------------------------------------------------------------------

class PrecisionTest(Helpers):
    def test_integer_gram_boundaries(self):
        rows = [
            ["R1", "raw", "A", "", "1000000000", "kg"],       # exactly 10^12 g
            ["O1", "output", "X", "", "1000000000", "kg"],    # exactly 10^12 g
            ["I1", "input", "X", "A", "999999999.999", "kg"], # 999_999_999_999 g
            ["I2", "input", "X", "A", "0.001", "kg"],         # 1 g
            ["L1", "loss", "X", "", "0.000", "kg"],
        ]
        ingest(self.db, "c1", self.write_csv("a.csv", rows))
        seal(self.db, "c1")
        document = json.loads(bytes(self.q("SELECT sealed_json FROM intake_campaign")[0][0]))
        self.assertEqual(document["lots"][0]["mass_g"], 10**12)
        # The two edges are distinct ticket rows but aggregate to exactly 10^12.
        self.assertEqual(document["recipes"][0]["inputs"], [{"lot_id": "A", "mass_g": 10**12}])

    def test_cumulative_input_beyond_sqlite_integer_is_not_truncated(self):
        # Per-ticket grams are bounded by 10^12 in real files, so an aggregate
        # over 2^63 cannot be produced through SQLite; exercise the exact
        # Python-integer aggregation directly to prove there is no truncation.
        synthetic = [
            Ticket(f"S{i}", "input", "X", "A", 10**18, i, i + 1) for i in range(1, 21)
        ]
        synthetic += [
            Ticket("OUT", "output", "X", None, 10**18, 21, 22),
            Ticket("LOSS", "loss", "X", None, 0, 22, 23),
            Ticket("RAW", "raw", "A", None, 10**19, 23, 24),
        ]
        facts = facts_from_tickets(synthetic)
        self.assertEqual(facts.edges[("X", "A")], 20 * 10**18)  # 2e19 > 2^63
        # Aggregation is exact; the existing 10^12 edge contract then rejects
        # with the full, untruncated integer in the message.
        with self.assertRaisesRegex(ContractError, str(20 * 10**18)):
            _build_document("c1", facts)

    def test_storage_declares_unbounded_text_for_cumulative_totals(self):
        ingest(self.db, "c1", self.write_csv("a.csv", [["T", "raw", "A", "", "1", "g"]]))
        with sqlite3.connect(self.db) as conn:
            declared = dict(
                (row[1], row[2].upper())
                for row in conn.execute("PRAGMA table_info(intake_sealed_edge)")
            )
        self.assertEqual(declared["total_grams"], "TEXT")


# ---------------------------------------------------------------------------
# Real CLI: multi-batch intake -> seal -> export -> old batch CLI
# ---------------------------------------------------------------------------

class EndToEndCLITest(Helpers):
    def run_cli(self, *args, input_text=None):
        return subprocess.run(
            [sys.executable, "-m", "lot_lineage.intake", *args],
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": str(SRC)},
            capture_output=True,
            text=True,
            input=input_text,
        )

    def test_end_to_end_cli_matches_independent_expectations(self):
        # --- Independent hand computation of the delivered scenario ---
        # raw A=1000, B=500, C=300 -> 1800 g raw.
        # X: inputs 600+400=1000, output 900, loss 100 (1000 = 900+100).
        # Y: inputs 300+200=500, output 450, loss 50 (500 = 450+50).
        # total loss 100+50=150; available 1800-150=1650.
        # NB. the harness's second batch is fed first; both must be accepted.
        # Y = 0.6*X + 0.4*C ; X = 0.6*A + 0.4*B
        #   Y_A = 0.6*0.6 = 0.36 = 9/25
        #   Y_B = 0.6*0.4 = 0.24 = 6/25
        #   Y_C = 0.4              = 2/5
        # available origins = unconsumed lot mass times its own mix, summed:
        #   A: 400*1 + 600*(3/5) + 450*(9/25) = 400+360+162 = 922
        #   B: 100*1 + 600*(2/5) + 450*(6/25) = 100+240+108 = 448
        #   C: 100*1 + 0        + 450*(2/5)  = 100+0+180   = 280
        expected_raw, expected_loss, expected_available = 1800, 150, 1650
        expected_y = {"A": "9/25", "B": "6/25", "C": "2/5"}
        expected_origins = {"A": "922", "B": "448", "C": "280"}

        db = self.root / "campaign.sqlite"
        # Copy fixtures so the test can delete its inputs later.
        b2 = self.root / "b2.csv"
        b2.write_bytes(BATCH2.read_bytes())
        b1 = self.root / "b1.csv"
        b1.write_bytes(BATCH1.read_bytes())

        r = self.run_cli("ingest", "--database", str(db), "--campaign", "ceramic",
                         "--source", str(b1))
        self.assertEqual(r.returncode, 0, r.stderr)
        receipt1 = json.loads(r.stdout)
        self.assertEqual((receipt1["new_tickets"], receipt1["new_source"]), (3, 1))

        # identical bytes again: no new source, no new tickets
        again = self.run_cli("ingest", "--database", str(db), "--campaign", "ceramic",
                             "--source", str(b1))
        receipt_again = json.loads(again.stdout)
        self.assertEqual((receipt_again["new_tickets"], receipt_again["new_source"]), (0, 0))
        self.assertEqual(receipt_again["ticket_count"], 3)

        r = self.run_cli("ingest", "--database", str(db), "--campaign", "ceramic",
                         "--source", str(b2))
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout)["new_tickets"], 8)

        # equivalent-unit replay of batch1 from different bytes: new source only
        replay = self.root / "b1-equiv.csv"
        replay.write_text(
            "ticket_id,kind,lot_id,input_lot_id,mass,unit\n"
            "T-0003,loss,X,,0.100,kg\n"
            "T-0002,input,X,B,400,g\n"
            "T-0001,input,X,A,0.600,kg\n",
            encoding="utf-8",
        )
        r = self.run_cli("ingest", "--database", str(db), "--campaign", "ceramic",
                         "--source", str(replay))
        self.assertEqual(r.returncode, 0, r.stderr)
        receipt_replay = json.loads(r.stdout)
        self.assertEqual((receipt_replay["new_tickets"], receipt_replay["replayed_occurrences"]),
                         (0, 3))
        self.assertEqual(receipt_replay["new_source"], 1)
        self.assertEqual(receipt_replay["ticket_count"], 11)

        st = json.loads(self.run_cli("status", "--database", str(db),
                                     "--campaign", "ceramic").stdout)
        self.assertEqual((st["status"], st["ticket_count"], st["source_count"]), ("open", 11, 3))

        sealed = json.loads(self.run_cli("seal", "--database", str(db),
                                         "--campaign", "ceramic").stdout)
        self.assertEqual(sealed["status"], "sealed")
        self.assertEqual((sealed["lot_count"], sealed["recipe_count"]), (5, 2))

        output = self.root / "sealed.json"
        exported = json.loads(self.run_cli("export", "--database", str(db),
                                           "--campaign", "ceramic",
                                           "--output", str(output)).stdout)
        self.assertEqual(exported["sha256"], sealed["seal_sha256"])
        self.assertEqual(hashlib.sha256(output.read_bytes()).hexdigest(), exported["sha256"])
        repeat = self.run_cli("export", "--database", str(db), "--campaign", "ceramic",
                              "--output", str(output))
        self.assertEqual(hashlib.sha256(output.read_bytes()).hexdigest(),
                         json.loads(repeat.stdout)["sha256"])

        # Old batch CLI reads the exported file directly.
        old = subprocess.run(
            [sys.executable, "-m", "lot_lineage.cli", "--source", str(output),
             "--database", str(self.root / "lineage.sqlite")],
            cwd=ROOT, env={**os.environ, "PYTHONPATH": str(SRC)},
            capture_output=True, text=True,
        )
        self.assertEqual(old.returncode, 0, old.stderr)
        batch = json.loads(old.stdout)
        self.assertEqual(
            (batch["raw_input_g"], batch["process_loss_g"], batch["available_g"]),
            (expected_raw, expected_loss, expected_available),
        )
        y = next(row for row in batch["lots"] if row["lot_id"] == "Y")
        self.assertEqual(y["origin_fraction"], expected_y)
        self.assertEqual(batch["available_origin_g"], expected_origins)

        # The DB itself (not just the exported file) carries every fact.
        with sqlite3.connect(db) as conn:
            edge_rows = conn.execute(
                "SELECT output_lot, input_lot, total_grams, ticket_count "
                "FROM intake_sealed_edge ORDER BY output_lot, input_lot"
            ).fetchall()
            self.assertEqual(
                edge_rows,
                [("X", "A", "600", 1), ("X", "B", "400", 1),
                 ("Y", "C", "200", 1), ("Y", "X", "300", 1)],
            )
            t1_sources = conn.execute(
                "SELECT s.source_sha256, st.record_no, st.line_no "
                "FROM intake_source_ticket st JOIN intake_source s USING(source_pk) "
                "WHERE st.ticket_id='T-0001' ORDER BY s.source_pk, st.record_no"
            ).fetchall()
            # source 1 = batch1, source 2 = batch2, source 3 = equivalent replay
            self.assertEqual(len(t1_sources), 2)
            self.assertEqual((t1_sources[0][1], t1_sources[0][2]), (1, 2))
            self.assertEqual((t1_sources[1][1], t1_sources[1][2]), (3, 4))
            sources = {
                sha: (raw, hashv)
                for sha, raw, hashv in conn.execute(
                    "SELECT source_sha256, raw_bytes, source_sha256 FROM intake_source"
                )
            }
            for sha, (raw, _hash) in sources.items():
                self.assertEqual(hashlib.sha256(bytes(raw)).hexdigest(), sha)
            edge_tickets = conn.execute(
                "SELECT input_lot, ticket_id FROM intake_sealed_edge_ticket "
                "WHERE output_lot='X' ORDER BY input_lot, ticket_id"
            ).fetchall()
            self.assertEqual(edge_tickets, [("A", "T-0001"), ("B", "T-0002")])
            self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])

        # Remove the CSV inputs; raw bytes and digest still verifiable.
        b1.unlink(); b2.unlink(); replay.unlink()
        with sqlite3.connect(db) as conn:
            for sha, raw in conn.execute("SELECT source_sha256, raw_bytes FROM intake_source"):
                self.assertEqual(hashlib.sha256(bytes(raw)).hexdigest(), sha)
            # Every canonical fact has a real basis: each ticket has at least one
            # stored source position, and each sealed edge names its tickets.
            unbacked = conn.execute(
                "SELECT t.ticket_id FROM intake_ticket t "
                "WHERE NOT EXISTS (SELECT 1 FROM intake_source_ticket st "
                "  WHERE st.campaign_id=t.campaign_id AND st.ticket_id=t.ticket_id)"
            ).fetchall()
            self.assertEqual(unbacked, [])
            positions = conn.execute(
                "SELECT COUNT(*) FROM intake_source_ticket"
            ).fetchone()[0]
            self.assertEqual(positions, 14)  # b1(3)+b2(8)+replay(3)
            edge_sum = {
                (o, i): int(total)
                for o, i, total in conn.execute(
                    "SELECT output_lot, input_lot, total_grams FROM intake_sealed_edge"
                )
            }
            # independently re-sum the per-ticket inputs
            recomputed = {}
            for lot, src_lot, grams in conn.execute(
                "SELECT lot_id, input_lot_id, grams FROM intake_ticket WHERE kind='input'"
            ):
                recomputed[(lot, src_lot)] = recomputed.get((lot, src_lot), 0) + grams
            self.assertEqual(edge_sum, recomputed)
            # the sealed edge ticket association is exactly the input tickets
            associated = {
                (row[0], row[1], row[2])
                for row in conn.execute(
                    "SELECT campaign_id, lot_id, ticket_id FROM intake_ticket WHERE kind='input'"
                )
            }
            stored = {
                (row[0], row[1], row[2])
                for row in conn.execute(
                    "SELECT campaign_id, output_lot, ticket_id FROM intake_sealed_edge_ticket"
                )
            }
            self.assertEqual(associated, stored)

        # Post-seal ingest is refused through the CLI with a non-zero exit.
        blocked = self.run_cli("ingest", "--database", str(db), "--campaign", "ceramic",
                               "--source", str(self.root / "missing.csv"))
        self.assertNotEqual(blocked.returncode, 0)

    def test_cli_stdout_failure_does_not_deny_committed_ingest(self):
        from lot_lineage.intake import __main__ as intake_cli

        source = self.write_csv("a.csv", [["T1", "raw", "A", "", "1", "g"]])
        db = self.root / "x.sqlite"
        with mock.patch("builtins.print", side_effect=OSError("broken stdout")):
            code = intake_cli.main(
                ["ingest", "--database", str(db), "--campaign", "c1", "--source", str(source)]
            )
        self.assertEqual(code, 3)
        with sqlite3.connect(db) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM intake_ticket").fetchone()[0], 1
            )

    def test_cli_error_is_nonzero_and_does_not_claim_commit(self):
        bad = self.write_text("bad.csv", "ticket_id,kind,lot_id,input_lot_id,mass,unit\nT,raw,A,,NaN,g\n")
        r = self.run_cli("ingest", "--database", str(self.db), "--campaign", "c1",
                         "--source", str(bad))
        self.assertEqual(r.returncode, 2)
        self.assertIn("intake ingest failed", r.stderr)
        self.assertNotIn('"new_tickets"', r.stdout)


# ---------------------------------------------------------------------------
# Deterministic concurrency via the CLI hook handshake
# ---------------------------------------------------------------------------

class ConcurrencyTest(Helpers):
    def popen_cli(self, *args):
        return subprocess.Popen(
            [sys.executable, "-m", "lot_lineage.intake", *args, "--hook"],
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": str(SRC)},
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )

    def wait_hook(self, proc, name, timeout=20):
        ready, _, _ = select.select([proc.stdout], [], [], timeout)
        self.assertTrue(ready, f"hook {name} was not reached")
        line = proc.stdout.readline().strip()
        self.assertEqual(line, f"HOOK:{name}")

    def release(self, proc, timeout=20):
        try:
            proc.stdin.write("\n")
            proc.stdin.flush()
        except (BrokenPipeError, ValueError):
            pass
        out, err = proc.communicate(timeout=timeout)
        return proc.returncode, out, err

    def test_second_ingest_waits_and_fails_while_writer_holds_transaction(self):
        a = self.write_csv("a.csv", [["A1", "raw", "A", "", "1", "g"]])
        b = self.write_csv("b.csv", [["B1", "raw", "B", "", "2", "g"]])
        holder = self.popen_cli("ingest", "--database", str(self.db), "--campaign", "c1",
                                "--source", str(a))
        try:
            self.wait_hook(holder, "ingest_before_commit")
            contender = self.run_contender(b)
            self.assertEqual(contender.returncode, 2)
            self.assertIn("locked", contender.stderr)
        finally:
            code, out, err = self.release(holder)
        self.assertEqual(code, 0, err)
        # After the holder commits, the same contender succeeds and sees both.
        retry = self.run_contender(b)
        self.assertEqual(retry.returncode, 0, retry.stderr)
        self.assertEqual(json.loads(retry.stdout)["ticket_count"], 2)
        self.assertEqual(self.counts("c1")["tickets"], 2)

    def run_contender(self, source):
        return subprocess.run(
            [sys.executable, "-m", "lot_lineage.intake", "ingest",
             "--database", str(self.db), "--campaign", "c1",
             "--source", str(source), "--busy-timeout", "0.2"],
            cwd=ROOT, env={**os.environ, "PYTHONPATH": str(SRC)},
            capture_output=True, text=True, timeout=30,
        )

    def test_seal_cannot_commit_while_ingest_holds_transaction(self):
        a = self.write_csv("a.csv", [
            ["R1", "raw", "A", "", "10", "g"],
            ["O1", "output", "X", "", "10", "g"],
            ["I1", "input", "X", "A", "10", "g"],
            ["L1", "loss", "X", "", "0", "g"],
        ])
        holder = self.popen_cli("ingest", "--database", str(self.db), "--campaign", "c1",
                                "--source", str(a))
        try:
            self.wait_hook(holder, "ingest_before_commit")
            contender = subprocess.run(
                [sys.executable, "-m", "lot_lineage.intake", "seal",
                 "--database", str(self.db), "--campaign", "c1", "--busy-timeout", "0.2"],
                cwd=ROOT, env={**os.environ, "PYTHONPATH": str(SRC)},
                capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(contender.returncode, 2)
            self.assertIn("locked", contender.stderr)
        finally:
            code, _out, err = self.release(holder)
        self.assertEqual(code, 0, err)
        # The seal, run after the commit, sees the committed complete ticket
        # set: it must not act on the pre-transaction (empty) view.
        seal_run = subprocess.run(
            [sys.executable, "-m", "lot_lineage.intake", "seal",
             "--database", str(self.db), "--campaign", "c1"],
            cwd=ROOT, env={**os.environ, "PYTHONPATH": str(SRC)},
            capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(seal_run.returncode, 0, seal_run.stderr)
        self.assertEqual(json.loads(seal_run.stdout)["status"], "sealed")

    def test_ingest_after_seal_interleaving_is_refused(self):
        rows = [
            ["R1", "raw", "A", "", "10", "g"],
            ["O1", "output", "X", "", "10", "g"],
            ["I1", "input", "X", "A", "10", "g"],
            ["L1", "loss", "X", "", "0", "g"],
        ]
        ingest(self.db, "c1", self.write_csv("all.csv", rows))
        extra = self.write_csv("extra.csv", [["E1", "raw", "Z", "", "1", "g"]])
        holder = self.popen_cli("seal", "--database", str(self.db), "--campaign", "c1")
        try:
            self.wait_hook(holder, "seal_after_checks")
            contender = subprocess.run(
                [sys.executable, "-m", "lot_lineage.intake", "ingest",
                 "--database", str(self.db), "--campaign", "c1",
                 "--source", str(extra), "--busy-timeout", "0.2"],
                cwd=ROOT, env={**os.environ, "PYTHONPATH": str(SRC)},
                capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(contender.returncode, 2)
            self.assertIn("locked", contender.stderr)
        finally:
            code, _out, err = self.release(holder)
        self.assertEqual(code, 0, err)
        after = subprocess.run(
            [sys.executable, "-m", "lot_lineage.intake", "ingest",
             "--database", str(self.db), "--campaign", "c1", "--source", str(extra)],
            cwd=ROOT, env={**os.environ, "PYTHONPATH": str(SRC)},
            capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(after.returncode, 2)
        self.assertIn("sealed", after.stderr)


if __name__ == "__main__":
    unittest.main()
