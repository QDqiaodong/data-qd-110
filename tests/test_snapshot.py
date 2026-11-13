"""Tests for the offline content-addressed library snapshot store.

Expectations are computed independently of the implementation: the snapshot
image is compared against the live library's own query results and rebuilt
business outputs, chunk digests are recomputed from first principles, and the
reachable object set is recomputed straight from the manifest JSON.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import select
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from lot_lineage.intake import export as intake_export
from lot_lineage.intake import ingest as intake_ingest
from lot_lineage.intake import seal as intake_seal
from lot_lineage.intake import status as intake_status
from lot_lineage.pipeline import build as build_batch
from lot_lineage.recycle.pipeline import build as build_recycle
from lot_lineage.snapshot import __main__ as snapshot_cli
from lot_lineage.snapshot.model import (
    CHUNK_SIZE,
    ContractError,
    SnapshotError,
    canonical_json,
    parse_manifest,
)
from lot_lineage.snapshot.store import (
    capture,
    export_manifest,
    forget,
    gc,
    list_snapshots,
    restore,
    verify,
)

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
FIXTURES = ROOT / "fixtures"
BATCH_FIXTURE = FIXTURES / "lots.json"
RECYCLE_FIXTURE = FIXTURES / "recycle_pq.json"
BATCH1 = FIXTURES / "intake" / "batch1.csv"
BATCH2 = FIXTURES / "intake" / "batch2.csv"

LINEAGE_DDL = """
CREATE TABLE lots(lot_id TEXT PRIMARY KEY, kind TEXT, mass_g INTEGER, available_g INTEGER);
CREATE TABLE transforms(output_lot TEXT PRIMARY KEY, loss_g INTEGER);
CREATE TABLE inputs(output_lot TEXT, input_lot TEXT, mass_g INTEGER);
CREATE TABLE origin_mix(lot_id TEXT, origin_lot TEXT, numerator TEXT, denominator TEXT);
"""


def sha_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def object_path(store: Path, digest: str) -> Path:
    return Path(store) / "objects" / digest[:2] / digest


def manifest_path(store: Path, snapshot_id: str) -> Path:
    return Path(store) / "manifests" / f"{snapshot_id}.json"


def read_manifest(store: Path, snapshot_id: str) -> dict:
    return json.loads(manifest_path(store, snapshot_id).read_text(encoding="utf-8"))


class Helpers(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = self.root / "store"

    def lineage_library(self, name="batch.sqlite"):
        database = self.root / name
        build_batch(BATCH_FIXTURE, database)
        return database

    def recycle_library(self, name="recycle.sqlite"):
        database = self.root / name
        build_recycle(RECYCLE_FIXTURE, database)
        return database

    def intake_library(self, name="intake.sqlite"):
        database = self.root / name
        intake_ingest(database, "ceramic", BATCH1)
        intake_ingest(database, "ceramic", BATCH2)
        intake_seal(database, "ceramic")
        return database

    def altered_lineage_library(self, name="altered.sqlite", marker="999999"):
        database = self.root / name
        self.build_padded_library(database)
        with sqlite3.connect(database) as conn:
            conn.execute(
                "UPDATE origin_mix SET numerator=? "
                "WHERE lot_id='Y' AND origin_lot='A'",
                (marker,),
            )
        return database

    def build_padded_library(self, path, *, padding_bytes=900_000):
        """A real lineage library large enough to span several 64 KiB chunks."""
        build_batch(BATCH_FIXTURE, path)
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE padding(id INTEGER PRIMARY KEY, payload BLOB)")
            conn.execute(
                "INSERT INTO padding(payload) VALUES (?)", (b"P" * padding_bytes,)
            )
            conn.commit()

    def run_cli(self, *args, input_text=None):
        return subprocess.run(
            [sys.executable, "-m", "lot_lineage.snapshot", *args],
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": str(SRC)},
            capture_output=True,
            text=True,
            input=input_text,
        )


# ---------------------------------------------------------------------------
# capture / manifest
# ---------------------------------------------------------------------------

class CaptureTest(Helpers):
    def test_family_detected_for_all_three_libraries(self):
        expected = {
            "lineage": self.lineage_library(),
            "recycle": self.recycle_library(),
            "intake_tickets": self.intake_library(),
        }
        for family, database in expected.items():
            receipt = capture(database, self.store)
            self.assertEqual(receipt["library_family"], family)
            self.assertEqual(receipt["chunk_size"], CHUNK_SIZE)
            self.assertTrue(receipt["new_manifest"])

    def test_explicit_family_must_match(self):
        database = self.lineage_library()
        with self.assertRaisesRegex(ContractError, "expected 'recycle'"):
            capture(database, self.store, expected_family="recycle")
        self.assertFalse(self.store.exists())

    def test_manifest_is_canonical_sorted_newline_free(self):
        receipt = capture(self.intake_library(), self.store)
        raw = manifest_path(self.store, receipt["snapshot_id"]).read_bytes()
        self.assertFalse(raw.endswith(b"\n"))
        self.assertNotIn(b" ", raw)
        self.assertEqual(hashlib.sha256(raw).hexdigest(), receipt["snapshot_id"])
        parsed = json.loads(raw)
        self.assertEqual(
            list(parsed),
            ["chunk_size", "chunks", "image_length", "image_sha256",
             "library_family", "snapshot_version"],
        )
        self.assertEqual(parsed["snapshot_version"], 1)
        self.assertEqual(parsed["library_family"], "intake_tickets")
        self.assertEqual(parsed["chunk_size"], 65536)
        self.assertEqual(parsed["image_length"], receipt["image_length"])
        self.assertEqual(parse_manifest(raw).snapshot_id, receipt["snapshot_id"])

    def test_image_digest_is_the_restored_file_digest(self):
        receipt = capture(self.recycle_library(), self.store)
        restored = self.root / "restored.sqlite"
        restore(self.store, receipt["snapshot_id"], restored)
        self.assertEqual(sha_file(restored), receipt["image_sha256"])
        self.assertEqual(restored.stat().st_size, receipt["image_length"])

    def test_same_library_captured_twice_is_one_snapshot(self):
        database = self.lineage_library()
        first = capture(database, self.store)
        before = {p: sha_file(p) for p in self.store.rglob("*") if p.is_file()}
        second = capture(database, self.store)
        self.assertEqual(second["snapshot_id"], first["snapshot_id"])
        self.assertFalse(second["new_manifest"])
        self.assertEqual(second["new_objects"], 0)
        after = {p: sha_file(p) for p in self.store.rglob("*") if p.is_file()}
        self.assertEqual(before, after)
        self.assertEqual(list_snapshots(self.store)["count"], 1)

    def test_changed_library_produces_new_snapshot_with_shared_chunks(self):
        first = capture(self.altered_lineage_library("first.sqlite", "123456"), self.store)
        size = (self.root / "first.sqlite").stat().st_size
        self.assertGreater(size, CHUNK_SIZE)
        second = capture(self.altered_lineage_library("second.sqlite", "999999"), self.store)
        self.assertNotEqual(second["snapshot_id"], first["snapshot_id"])
        self.assertGreater(second["reused_objects"], 0)
        self.assertGreater(second["chunk_count"], 1)
        first_chunks = read_manifest(self.store, first["snapshot_id"])["chunks"]
        second_chunks = read_manifest(self.store, second["snapshot_id"])["chunks"]
        # The short tail chunk (and other unchanged pages) are shared, and the
        # short chunk keeps its own distinct length.
        self.assertLess(first_chunks[-1]["length"], CHUNK_SIZE)
        self.assertEqual(first_chunks[-1], second_chunks[-1])
        self.assertGreater(
            len({chunk["sha256"] for chunk in first_chunks} & {chunk["sha256"] for chunk in second_chunks}),
            1,
        )
        self.assertEqual(list_snapshots(self.store)["count"], 2)
        rows = {row["snapshot_id"]: row for row in list_snapshots(self.store)["snapshots"]}
        self.assertEqual(rows[first["snapshot_id"]]["library_family"], "lineage")
        self.assertEqual(rows[second["snapshot_id"]]["library_family"], "lineage")

    def test_source_is_never_modified(self):
        database = self.intake_library()
        before = {p.name: sha_file(p) for p in self.root.iterdir() if p.is_file()}
        capture(database, self.store)
        capture(database, self.store)
        after = {p.name: sha_file(p) for p in self.root.iterdir() if p.is_file()}
        self.assertEqual(before, after)
        self.assertFalse(Path(str(database) + "-wal").exists())

    def test_source_validated_before_store_creation(self):
        bogus = self.root / "not-a-library.bin"
        bogus.write_bytes(b"SQLite format 3\x00" + b"\x00" * 100)
        with self.assertRaises(ContractError):
            capture(bogus, self.store)
        self.assertFalse(self.store.exists())

    def test_missing_source(self):
        with self.assertRaisesRegex(ContractError, "does not exist"):
            capture(self.root / "missing.sqlite", self.store)
        self.assertFalse(self.store.exists())

    def test_rollback_journal_uncommitted_writer_is_invisible(self):
        database = self.root / "rollback.sqlite"
        conn = sqlite3.connect(database)
        conn.executescript(LINEAGE_DDL)
        conn.execute("INSERT INTO lots VALUES ('A','raw',10,10)")
        conn.commit()
        conn.close()
        writer = sqlite3.connect(database, isolation_level=None)
        self.addCleanup(writer.close)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO lots VALUES ('PENDING','raw',1,1)")
        receipt = capture(database, self.store)
        writer.execute("ROLLBACK")
        output = self.root / "restored.sqlite"
        restore(self.store, receipt["snapshot_id"], output)
        with sqlite3.connect(output) as check:
            self.assertEqual(
                {row[0] for row in check.execute("SELECT lot_id FROM lots")}, {"A"}
            )

    def test_non_business_database_refused(self):
        database = self.root / "plain.sqlite"
        with sqlite3.connect(database) as conn:
            conn.execute("CREATE TABLE something(x)")
        with self.assertRaisesRegex(ContractError, "not one of the supported business"):
            capture(database, self.store)
        self.assertFalse(self.store.exists())

    def test_preexisting_sidecars_are_left_untouched(self):
        database = self.root / "wal.sqlite"
        conn = sqlite3.connect(database)
        conn.executescript(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;"
            + LINEAGE_DDL
            + "INSERT INTO lots VALUES ('A','raw',1,1);"
        )
        conn.commit()
        self.addCleanup(conn.close)
        # Force the WAL sidecar to exist before capture.
        conn.execute("SELECT COUNT(*) FROM lots").fetchone()
        before = {name: sha_file(self.root / name) for name in
                  ("wal.sqlite", "wal.sqlite-wal", "wal.sqlite-shm")
                  if (self.root / name).exists()}
        self.assertIn("wal.sqlite-wal", before)
        capture(database, self.store)
        after = {name: sha_file(self.root / name) for name in before}
        self.assertEqual(before, after)
        self.assertTrue((self.root / "wal.sqlite-wal").exists())

    def test_sidecars_absent_at_open_are_not_reported_as_created(self):
        # If neither sidecar existed before the open, capture cleans up only
        # empty cache files it created; the library stays readable and unchanged.
        database = self.root / "plain.sqlite"
        with sqlite3.connect(database) as conn:
            conn.executescript(LINEAGE_DDL + "INSERT INTO lots VALUES ('A','raw',1,1);")
        before = sha_file(database)
        receipt = capture(database, self.store)
        self.assertEqual(sha_file(database), before)
        self.assertFalse((self.root / "plain.sqlite-wal").exists())
        output = self.root / "restored.sqlite"
        restore(self.store, receipt["snapshot_id"], output)
        with sqlite3.connect(output) as check:
            self.assertEqual(
                [row[0] for row in check.execute("SELECT lot_id FROM lots")], ["A"]
            )

    def test_symlinked_source_refused(self):
        database = self.lineage_library()
        link = self.root / "link.sqlite"
        os.symlink(database, link)
        with self.assertRaisesRegex(ContractError, "symlink"):
            capture(link, self.store)


# ---------------------------------------------------------------------------
# chunking: order, duplicates, short tail, BLOB > 3 chunks
# ---------------------------------------------------------------------------

class ChunkingTest(Helpers):
    def build_large_library(self, path: Path, *, page_size=65536, payload=100_000,
                            blob_bytes=None):
        conn = sqlite3.connect(path)
        try:
            conn.execute(f"PRAGMA page_size={page_size}")
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.executescript(LINEAGE_DDL)
            conn.execute("INSERT INTO lots VALUES ('A','raw',?,?)", (payload, payload))
            conn.execute("INSERT INTO origin_mix VALUES ('A','A','1','1')")
            if blob_bytes:
                conn.execute("CREATE TABLE blobs(id INTEGER PRIMARY KEY, payload BLOB)")
                conn.execute("INSERT INTO blobs(payload) VALUES (?)", (b"Z" * blob_bytes,))
            conn.commit()
        finally:
            conn.close()

    def test_short_tail_and_ordered_chunks(self):
        receipt = capture(self.lineage_library(), self.store)
        manifest = parse_manifest(manifest_path(self.store, receipt["snapshot_id"]).read_bytes())
        lengths = [chunk.length for chunk in manifest.chunks]
        self.assertEqual(sum(lengths), manifest.image_length)
        self.assertTrue(all(length == CHUNK_SIZE for length in lengths[:-1]))
        self.assertLessEqual(lengths[-1], CHUNK_SIZE)
        if manifest.image_length % CHUNK_SIZE:
            self.assertLess(lengths[-1], CHUNK_SIZE)
        image = b"".join(object_path(self.store, chunk.sha256).read_bytes()
                         for chunk in manifest.chunks)
        self.assertEqual(hashlib.sha256(image).hexdigest(), manifest.image_sha256)

    def test_duplicate_chunks_are_stored_once(self):
        database = self.root / "large.sqlite"
        self.build_large_library(database)
        receipt = capture(database, self.store)
        manifest = parse_manifest(manifest_path(self.store, receipt["snapshot_id"]).read_bytes())
        digests = [chunk.sha256 for chunk in manifest.chunks]
        self.assertGreater(len(digests), len(set(digests)))
        objects = [path for path in (self.store / "objects").rglob("*") if path.is_file()]
        self.assertEqual(len(objects), len(set(digests)))
        self.assertEqual(receipt["unique_chunk_count"], len(set(digests)))

    def test_large_blob_spanning_more_than_three_chunks(self):
        database = self.root / "blob.sqlite"
        self.build_large_library(database, page_size=8192, payload=100_000,
                                 blob_bytes=400_000)
        receipt = capture(database, self.store)
        self.assertGreater(receipt["chunk_count"], 3)
        restored = self.root / "restored.sqlite"
        restore(self.store, receipt["snapshot_id"], restored)
        self.assertEqual(sha_file(restored), receipt["image_sha256"])
        with sqlite3.connect(restored) as conn:
            mass, = conn.execute("SELECT mass_g FROM lots WHERE lot_id='A'").fetchone()
            self.assertEqual(mass, 100_000)
            blob, = conn.execute("SELECT length(payload) FROM blobs").fetchone()
            self.assertEqual(blob, 400_000)
            self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_snapshots_of_identical_content_share_all_objects(self):
        first_db = self.root / "first.sqlite"
        second_db = self.root / "second.sqlite"
        build_batch(BATCH_FIXTURE, first_db)
        build_batch(BATCH_FIXTURE, second_db)
        first = capture(first_db, self.store)
        second = capture(second_db, self.store)
        self.assertEqual(first["snapshot_id"], second["snapshot_id"])
        self.assertEqual(second["reused_objects"], second["chunk_count"])
        self.assertEqual(second["new_objects"], 0)


# ---------------------------------------------------------------------------
# manifest strictness
# ---------------------------------------------------------------------------

class ManifestContractTest(Helpers):
    def setUp(self):
        super().setUp()
        self.receipt = capture(self.lineage_library(), self.store)
        self.raw = manifest_path(self.store, self.receipt["snapshot_id"]).read_bytes()

    def variant(self, **overrides) -> bytes:
        data = json.loads(self.raw)
        data.update(overrides)
        return canonical_json(data)

    def test_rejects_extra_and_missing_fields(self):
        with self.assertRaisesRegex(ContractError, "unexpected"):
            parse_manifest(self.variant(extra=1))
        data = json.loads(self.raw)
        del data["chunk_size"]
        with self.assertRaisesRegex(ContractError, "missing chunk_size"):
            parse_manifest(canonical_json(data))

    def test_rejects_duplicate_keys(self):
        duplicated = self.raw.replace(
            b'"image_length":', b'"image_length":1,"image_length":', 1
        )
        with self.assertRaisesRegex(ContractError, "duplicate key"):
            parse_manifest(duplicated)

    def test_rejects_non_canonical_bytes(self):
        pretty = json.dumps(json.loads(self.raw), indent=2).encode()
        with self.assertRaisesRegex(ContractError, "not the canonical"):
            parse_manifest(pretty)
        with self.assertRaisesRegex(ContractError, "not the canonical"):
            parse_manifest(self.raw + b"\n")
        reordered = canonical_json(
            dict(reversed(list(json.loads(self.raw).items())))
        )
        self.assertEqual(reordered, self.raw)

    def test_rejects_boolean_integer_and_bad_types(self):
        with self.assertRaisesRegex(ContractError, "snapshot_version"):
            parse_manifest(self.variant(snapshot_version=True))
        with self.assertRaisesRegex(ContractError, "image_length"):
            parse_manifest(self.variant(image_length=True))
        with self.assertRaisesRegex(ContractError, "snapshot_version"):
            parse_manifest(self.variant(snapshot_version=2))
        with self.assertRaisesRegex(ContractError, "library_family"):
            parse_manifest(self.variant(library_family="unknown"))
        with self.assertRaisesRegex(ContractError, "image_sha256"):
            parse_manifest(self.variant(image_sha256="A" * 64))
        with self.assertRaisesRegex(ContractError, "chunk_size"):
            parse_manifest(self.variant(chunk_size=4096))

    def test_rejects_chunk_shape_lengths_and_order(self):
        data = json.loads(self.raw)
        data["chunks"][0]["length"] += 1
        with self.assertRaisesRegex(ContractError, "does not match the expected"):
            parse_manifest(canonical_json(data))
        data = json.loads(self.raw)
        data["chunks"].append(dict(data["chunks"][0]))
        with self.assertRaisesRegex(ContractError, "needs"):
            parse_manifest(canonical_json(data))
        data = json.loads(self.raw)
        data["chunks"][0]["extra"] = 1
        with self.assertRaisesRegex(ContractError, "exactly sha256, length"):
            parse_manifest(canonical_json(data))
        data = json.loads(self.raw)
        data["chunks"] = data["chunks"][:-1]
        with self.assertRaisesRegex(ContractError, "needs"):
            parse_manifest(canonical_json(data))

    def test_snapshot_id_is_manifest_sha(self):
        exported = export_manifest(self.store, self.receipt["snapshot_id"])
        self.assertEqual(exported["manifest_sha256"], self.receipt["snapshot_id"])
        self.assertEqual(
            exported["manifest_sha256"],
            hashlib.sha256(exported["manifest_utf8"].encode()).hexdigest(),
        )


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

class VerifyTest(Helpers):
    def setUp(self):
        super().setUp()
        self.receipt = capture(self.lineage_library(), self.store)
        self.manifest = parse_manifest(
            manifest_path(self.store, self.receipt["snapshot_id"]).read_bytes()
        )

    def test_verifies_family_and_integrity(self):
        result = verify(self.store, self.receipt["snapshot_id"])
        self.assertEqual(result["verified"], 1)
        row = result["snapshots"][0]
        self.assertEqual(row["status"], "verified")
        self.assertEqual(row["library_family"], "lineage")
        with self.assertRaisesRegex(ContractError, "expected 'recycle'"):
            verify(self.store, self.receipt["snapshot_id"], expected_family="recycle")

    def test_corrupted_object_is_detected(self):
        path = object_path(self.store, self.manifest.chunks[0].sha256)
        data = bytearray(path.read_bytes())
        data[100] ^= 0xFF
        path.write_bytes(bytes(data))
        with self.assertRaisesRegex(ContractError, "failed its digest check"):
            verify(self.store, self.receipt["snapshot_id"])

    def test_missing_object_is_detected(self):
        object_path(self.store, self.manifest.chunks[0].sha256).unlink()
        with self.assertRaisesRegex(ContractError, "does not exist"):
            verify(self.store, self.receipt["snapshot_id"])

    def test_truncated_object_is_detected(self):
        path = object_path(self.store, self.manifest.chunks[0].sha256)
        path.write_bytes(path.read_bytes()[:-1])
        with self.assertRaisesRegex(ContractError, r"length 36863, manifest says 36864|ended early"):
            verify(self.store, self.receipt["snapshot_id"])

    def test_symlinked_object_is_refused(self):
        path = object_path(self.store, self.manifest.chunks[0].sha256)
        copy = self.root / "copied-object"
        copy.write_bytes(path.read_bytes())
        path.unlink()
        os.symlink(copy, path)
        with self.assertRaisesRegex(ContractError, "symbolic link"):
            verify(self.store, self.receipt["snapshot_id"])

    def test_verify_all_and_unknown_id(self):
        self.assertEqual(verify(self.store)["verified"], 1)
        with self.assertRaisesRegex(ContractError, "does not exist"):
            verify(self.store, "0" * 64)
        with self.assertRaisesRegex(ContractError, "64 lowercase"):
            verify(self.store, "../escape")

    def test_manifest_family_must_match_the_image(self):
        receipt = capture(self.intake_library(), self.store)
        data = read_manifest(self.store, receipt["snapshot_id"])
        data["library_family"] = "lineage"
        raw = canonical_json(data)
        false_id = hashlib.sha256(raw).hexdigest()
        (self.store / "manifests" / f"{false_id}.json").write_bytes(raw)
        with self.assertRaisesRegex(ContractError, "declares family 'lineage'"):
            verify(self.store, false_id)
        self.assertEqual(
            verify(self.store, receipt["snapshot_id"])["verified"], 1
        )

    def test_manifest_file_name_must_match_digest(self):
        source = manifest_path(self.store, self.receipt["snapshot_id"])
        source.rename(self.store / "manifests" / ("f" * 64 + ".json"))
        with self.assertRaisesRegex(ContractError, "does not hash to its file name"):
            verify(self.store)


# ---------------------------------------------------------------------------
# restore
# ---------------------------------------------------------------------------

class RestoreTest(Helpers):
    def test_restored_batch_library_still_answers_its_own_queries(self):
        database = self.lineage_library()
        receipt = capture(database, self.store)
        output = self.root / "restored-lineage.sqlite"
        result = restore(self.store, receipt["snapshot_id"], output, library=database)
        self.assertEqual(result["status"], "restored")
        self.assertEqual(sha_file(output), receipt["image_sha256"])
        with sqlite3.connect(output) as conn:
            self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(
                conn.execute(
                    "SELECT numerator, denominator FROM origin_mix "
                    "WHERE lot_id='Y' AND origin_lot='A'"
                ).fetchone(),
                ("9", "25"),
            )
            estimate = dict(conn.execute(
                "SELECT origin_lot, available_g FROM available_origin_estimate"
            ).fetchall())
            self.assertAlmostEqual(estimate["A"], 940.0)
            restored_lots = conn.execute(
                "SELECT lot_id, kind, mass_g, available_g FROM lots ORDER BY lot_id"
            ).fetchall()
        with sqlite3.connect(database) as conn:
            live_lots = conn.execute(
                "SELECT lot_id, kind, mass_g, available_g FROM lots ORDER BY lot_id"
            ).fetchall()
        self.assertEqual(restored_lots, live_lots)

    def test_restored_intake_library_can_still_export_and_old_cli_reads_it(self):
        database = self.intake_library()
        receipt = capture(database, self.store)
        output = self.root / "restored-intake.sqlite"
        restore(self.store, receipt["snapshot_id"], output)

        info = intake_status(output, "ceramic")
        self.assertEqual(
            (info["status"], info["ticket_count"], info["source_count"]),
            ("sealed", 11, 2),
        )
        exported = self.root / "sealed.json"
        intake_export(output, "ceramic", exported)
        built = build_batch(exported, self.root / "from-export.sqlite")
        self.assertEqual(
            (built["raw_input_g"], built["process_loss_g"], built["available_g"]),
            (1800, 150, 1650),
        )
        y = next(row for row in built["lots"] if row["lot_id"] == "Y")
        self.assertEqual(y["origin_fraction"], {"A": "9/25", "B": "6/25", "C": "2/5"})
        self.assertEqual(built["available_origin_g"], {"A": "922", "B": "448", "C": "280"})

        # The restored library is writable as its own file: a new campaign can
        # be ingested and sealed there without touching the source library.
        extra = self.root / "more.csv"
        extra.write_text(
            "ticket_id,kind,lot_id,input_lot_id,mass,unit\n"
            "T-0101,raw,D,,10,g\n",
            encoding="utf-8",
        )
        intake_ingest(output, "second", extra)
        self.assertEqual(intake_status(output, "second")["status"], "open")
        self.assertEqual(intake_seal(output, "second")["status"], "sealed")
        # The source library did not gain that campaign.
        with sqlite3.connect(database) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT campaign_id FROM intake_campaign WHERE campaign_id='second'"
                ).fetchone()
            )

    def test_sealed_campaign_on_restored_copy_still_refuses_ingest(self):
        database = self.intake_library()
        receipt = capture(database, self.store)
        output = self.root / "restored-sealed.sqlite"
        restore(self.store, receipt["snapshot_id"], output)
        extra = self.root / "late.csv"
        extra.write_text(
            "ticket_id,kind,lot_id,input_lot_id,mass,unit\n"
            "T-9999,raw,Z,,1,g\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(Exception, "sealed"):
            intake_ingest(output, "ceramic", extra)
        self.assertEqual(intake_status(output, "ceramic")["status"], "sealed")

    def test_restore_checks_declared_family_against_image(self):
        receipt = capture(self.intake_library(), self.store)
        with self.assertRaisesRegex(ContractError, "expected 'lineage'"):
            restore(self.store, receipt["snapshot_id"],
                    self.root / "wrong.sqlite", expected_family="lineage")
        self.assertFalse((self.root / "wrong.sqlite").exists())

    def test_restored_recycle_library_preserves_exact_fractions_and_source(self):
        database = self.recycle_library()
        receipt = capture(database, self.store)
        output = self.root / "restored-recycle.sqlite"
        restore(self.store, receipt["snapshot_id"], output)
        with sqlite3.connect(output) as conn:
            self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(
                conn.execute(
                    "SELECT numerator, denominator FROM recycle_process_origin "
                    "WHERE process_id='P' AND origin_id='A'"
                ).fetchone(),
                ("39", "41"),
            )
            raw, digest = conn.execute(
                "SELECT raw_json, source_sha256 FROM recycle_source"
            ).fetchone()
        self.assertEqual(hashlib.sha256(bytes(raw)).hexdigest(), digest)

    def test_restore_refuses_existing_symlink_and_store_paths(self):
        receipt = capture(self.lineage_library(), self.store)
        target = self.root / "existing.sqlite"
        target.write_bytes(b"KEEP")
        with self.assertRaisesRegex(ContractError, "overwrite"):
            restore(self.store, receipt["snapshot_id"], target)
        self.assertEqual(target.read_bytes(), b"KEEP")

        link = self.root / "link.sqlite"
        os.symlink(target, link)
        with self.assertRaisesRegex(ContractError, "symbolic link"):
            restore(self.store, receipt["snapshot_id"], link)

        inside = self.store / "objects" / "restored.sqlite"
        with self.assertRaisesRegex(ContractError, "inside the snapshot store"):
            restore(self.store, receipt["snapshot_id"], inside)

    def test_restore_refuses_to_overwrite_the_live_library_or_sidecar(self):
        database = self.lineage_library()
        receipt = capture(database, self.store)
        before = sha_file(database)
        with self.assertRaisesRegex(ContractError, "overwrite|same file"):
            restore(self.store, receipt["snapshot_id"], database, library=database)
        self.assertEqual(sha_file(database), before)

        sidecar = Path(str(database) + "-wal")
        sidecar.write_bytes(b"not really")
        with self.assertRaisesRegex(ContractError, "overwrite|same file"):
            restore(self.store, receipt["snapshot_id"], sidecar, library=database)
        self.assertEqual(sidecar.read_bytes(), b"not really")

    def test_restored_file_is_outside_the_library(self):
        database = self.lineage_library()
        receipt = capture(database, self.store)
        restore(self.store, receipt["snapshot_id"], self.root / "copy.sqlite")
        self.assertTrue((self.root / "copy.sqlite").exists())
        self.assertFalse(Path(str(database) + "-wal").exists())
        with self.assertRaisesRegex(ContractError, "overwrite"):
            restore(self.store, receipt["snapshot_id"], self.root / "copy.sqlite")

    def test_restore_after_moving_the_source_inputs_away(self):
        # All three libraries are captured, then their source files and the
        # original CSV inputs are removed.  Each snapshot still restores a
        # complete, usable library.
        lineage = self.lineage_library()
        recycle = self.recycle_library()
        intake = self.intake_library()
        captured = {
            "lineage": capture(lineage, self.store),
            "recycle": capture(recycle, self.store),
            "intake_tickets": capture(intake, self.store),
        }
        outputs = {
            family: self.root / f"moved-{family}.sqlite" for family in captured
        }
        for path in (lineage, recycle, intake, BATCH1, BATCH2):
            self.assertTrue(Path(path).exists())
        lineage.unlink()
        recycle.unlink()
        intake.unlink()
        moved = self.root / "moved-away"
        moved.mkdir()
        (moved / BATCH1.name).write_bytes(BATCH1.read_bytes())
        (moved / BATCH2.name).write_bytes(BATCH2.read_bytes())

        for family, receipt in captured.items():
            restore(self.store, receipt["snapshot_id"], outputs[family])
            self.assertEqual(sha_file(outputs[family]), receipt["image_sha256"])
        self.assertEqual(
            verify(self.store)["verified"], 3
        )
        # Lineage library: business content is intact.
        with sqlite3.connect(outputs["lineage"]) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM lots WHERE kind='raw'"
                ).fetchone()[0],
                3,
            )
        # Recycle library: its original JSON and digest are still in the image.
        with sqlite3.connect(outputs["recycle"]) as conn:
            raw, digest = conn.execute(
                "SELECT raw_json, source_sha256 FROM recycle_source"
            ).fetchone()
        self.assertEqual(hashlib.sha256(bytes(raw)).hexdigest(), digest)
        # Intake library: still sealed, still exportable, still readable by the
        # old CLI after every original input file is gone.
        self.assertEqual(intake_status(outputs["intake_tickets"], "ceramic")["status"], "sealed")
        exported = self.root / "moved-sealed.json"
        intake_export(outputs["intake_tickets"], "ceramic", exported)
        built = build_batch(exported, self.root / "moved-built.sqlite")
        self.assertEqual(
            (built["raw_input_g"], built["process_loss_g"], built["available_g"]),
            (1800, 150, 1650),
        )
        with sqlite3.connect(outputs["intake_tickets"]) as conn:
            for digest, raw in conn.execute(
                "SELECT source_sha256, raw_bytes FROM intake_source"
            ):
                self.assertEqual(hashlib.sha256(bytes(raw)).hexdigest(), digest)


# ---------------------------------------------------------------------------
# forget / gc
# ---------------------------------------------------------------------------

class ForgetGCTest(Helpers):
    def test_forget_removes_only_the_manifest(self):
        receipt = capture(self.lineage_library(), self.store)
        objects_before = {p for p in (self.store / "objects").rglob("*") if p.is_file()}
        result = forget(self.store, receipt["snapshot_id"])
        self.assertEqual(result["status"], "forgotten")
        self.assertFalse(manifest_path(self.store, receipt["snapshot_id"]).exists())
        self.assertEqual(objects_before,
                         {p for p in (self.store / "objects").rglob("*") if p.is_file()})
        self.assertEqual(list_snapshots(self.store)["count"], 0)
        with self.assertRaisesRegex(ContractError, "unknown snapshot"):
            forget(self.store, receipt["snapshot_id"])

    def test_gc_keeps_objects_shared_with_remaining_snapshots(self):
        first = capture(self.lineage_library(), self.store)
        second = capture(self.altered_lineage_library(), self.store)
        first_ids = {c["sha256"] for c in read_manifest(self.store, first["snapshot_id"])["chunks"]}
        second_ids = {c["sha256"] for c in read_manifest(self.store, second["snapshot_id"])["chunks"]}
        shared = first_ids & second_ids
        forget(self.store, first["snapshot_id"])
        result = gc(self.store)
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["referenced_objects"], len(second_ids))
        for digest in shared:
            self.assertTrue(object_path(self.store, digest).exists())
        self.assertEqual(verify(self.store)["verified"], 1)
        self.assertEqual(
            verify(self.store, second["snapshot_id"])["snapshots"][0]["status"], "verified"
        )

    def test_gc_deletes_orphans_after_forget(self):
        first = capture(self.lineage_library(), self.store)
        capture(self.altered_lineage_library(), self.store)
        forget(self.store, first["snapshot_id"])
        result = gc(self.store)
        self.assertEqual(result["errors"], [])
        self.assertGreater(result["deleted_objects"], 0)
        self.assertEqual(result["kept_objects"], result["referenced_objects"])
        self.assertGreater(result["freed_bytes"], 0)

    def test_gc_refuses_to_delete_anything_if_a_reference_is_broken(self):
        captured = capture(self.lineage_library(), self.store)
        manifest = parse_manifest(manifest_path(self.store, captured["snapshot_id"]).read_bytes())
        victim = manifest.chunks[0]
        object_path(self.store, victim.sha256).unlink()
        orphan = self.store / "objects" / "aa"
        orphan.mkdir(exist_ok=True)
        orphan_file = orphan / ("af" * 32)
        orphan_file.write_bytes(b"orphan")
        with self.assertRaisesRegex(SnapshotError, "refusing to collect garbage"):
            gc(self.store)
        self.assertTrue(orphan_file.exists())
        self.assertFalse(object_path(self.store, victim.sha256).exists())

    def test_gc_reports_partial_io_deletion_and_keeps_going(self):
        first = capture(self.lineage_library(), self.store)
        second = capture(self.altered_lineage_library(), self.store)
        forget(self.store, first["snapshot_id"])
        # Choose a deletable orphan digest deterministically and refuse it.
        second_ids = {c["sha256"] for c in read_manifest(self.store, second["snapshot_id"])["chunks"]}
        orphans = sorted(
            p.name for p in (self.store / "objects").rglob("*")
            if p.is_file() and p.name not in second_ids
        )
        self.assertTrue(orphans, "fixture should leave at least one orphan")
        victim = orphans[0]
        real_unlink = os.unlink

        def flaky_unlink(path, *args, **kwargs):
            if str(path).endswith("/" + victim):
                raise PermissionError("injected I/O failure")
            return real_unlink(path, *args, **kwargs)

        with mock.patch("lot_lineage.snapshot.store.os.unlink", side_effect=flaky_unlink):
            result = gc(self.store)
        self.assertEqual(len(result["errors"]), 1)
        self.assertIn("could not delete object " + victim, result["errors"][0])
        self.assertTrue(object_path(self.store, victim).exists())
        self.assertEqual(verify(self.store)["verified"], 1)

    def test_gc_refuses_when_objects_tree_contains_untrusted_entries(self):
        captured = capture(self.lineage_library(), self.store)
        foreign = self.store / "objects" / "not-a-shard"
        foreign.mkdir()
        (foreign / "payload").write_bytes(b"x")
        with self.assertRaisesRegex(SnapshotError, "not content-addressed object files"):
            gc(self.store)
        self.assertTrue((foreign / "payload").exists())
        # Remove the offending entry and gc succeeds without touching references.
        (foreign / "payload").unlink()
        foreign.rmdir()
        result = gc(self.store)
        self.assertEqual(result["errors"], [])
        self.assertEqual(verify(self.store, captured["snapshot_id"])["verified"], 1)

    def test_gc_removes_stale_temp_files(self):
        captured = capture(self.lineage_library(), self.store)
        stale = self.store / "tmp" / ".object-stale.tmp"
        stale.write_bytes(b"stale")
        result = gc(self.store)
        self.assertEqual(result["removed_temp_entries"], 1)
        self.assertFalse(stale.exists())
        self.assertEqual(verify(self.store, captured["snapshot_id"])["verified"], 1)


# ---------------------------------------------------------------------------
# independent reachable set
# ---------------------------------------------------------------------------

class IndependentReachableSetTest(Helpers):
    def test_reachable_set_recomputed_from_manifest_json(self):
        first = capture(self.lineage_library(), self.store)
        second = capture(self.altered_lineage_library(), self.store)
        expected = set()
        for snapshot_id in (first["snapshot_id"], second["snapshot_id"]):
            data = json.loads(manifest_path(self.store, snapshot_id).read_text())
            expected |= {chunk["sha256"] for chunk in data["chunks"]}
        actual = {p.name for p in (self.store / "objects").rglob("*") if p.is_file()}
        self.assertEqual(actual, expected)

        forget(self.store, first["snapshot_id"])
        remaining = json.loads(manifest_path(self.store, second["snapshot_id"]).read_text())
        remaining_ids = {chunk["sha256"] for chunk in remaining["chunks"]}
        result = gc(self.store)
        kept = {p.name for p in (self.store / "objects").rglob("*") if p.is_file()}
        self.assertEqual(kept, remaining_ids)
        self.assertEqual(result["referenced_objects"], len(remaining_ids))
        self.assertEqual(result["scanned_objects"], len(expected))


# ---------------------------------------------------------------------------
# concurrency / WAL snapshot semantics
# ---------------------------------------------------------------------------

class WalSnapshotTest(Helpers):
    def make_wal_library(self, path: Path):
        conn = sqlite3.connect(path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA wal_autocheckpoint=0")
            conn.executescript(LINEAGE_DDL)
            conn.execute("INSERT INTO lots VALUES ('A','raw',1000,1000)")
            conn.commit()
        finally:
            conn.close()

    def open_writer(self, database):
        writer = sqlite3.connect(database, isolation_level=None)
        self.addCleanup(writer.close)
        writer.execute("PRAGMA wal_autocheckpoint=0")
        return writer

    def start_hooked_capture(self, database):
        proc = subprocess.Popen(
            [sys.executable, "-m", "lot_lineage.snapshot", "capture",
             "--database", str(database), "--store", str(self.store), "--hook"],
            cwd=ROOT, env={**os.environ, "PYTHONPATH": str(SRC)},
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )
        self.addCleanup(lambda: proc.poll() is None and proc.kill())
        return proc

    def wait_hook(self, proc, name):
        ready, _, _ = select.select([proc.stdout], [], [], 20)
        self.assertTrue(ready, f"hook {name} was not reached")
        self.assertEqual(proc.stdout.readline().strip(), f"HOOK:{name}")

    def release_hook(self, proc):
        proc.stdin.write("\n")
        proc.stdin.flush()

    def test_uncommitted_writer_changes_are_invisible(self):
        database = self.root / "wal.sqlite"
        self.make_wal_library(database)
        writer = self.open_writer(database)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO lots VALUES ('UNCOMMITTED','raw',5,5)")
        receipt = capture(database, self.store)
        writer.execute("COMMIT")
        output = self.root / "restored.sqlite"
        restore(self.store, receipt["snapshot_id"], output)
        with sqlite3.connect(output) as conn:
            ids = {row[0] for row in conn.execute("SELECT lot_id FROM lots")}
        self.assertEqual(ids, {"A"})
        with sqlite3.connect(database) as conn:
            self.assertEqual(
                {row[0] for row in conn.execute("SELECT lot_id FROM lots")},
                {"A", "UNCOMMITTED"},
            )

    def test_committed_wal_state_is_included(self):
        database = self.root / "wal.sqlite"
        self.make_wal_library(database)
        writer = self.open_writer(database)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO lots VALUES ('COMMITTED','raw',9,9)")
        writer.execute("COMMIT")
        receipt = capture(database, self.store)
        output = self.root / "restored.sqlite"
        restore(self.store, receipt["snapshot_id"], output)
        with sqlite3.connect(output) as conn:
            ids = {row[0] for row in conn.execute("SELECT lot_id FROM lots")}
        self.assertEqual(ids, {"A", "COMMITTED"})
        # The source is untouched and still has its writer connection open.
        with sqlite3.connect(database) as conn:
            self.assertEqual(
                {row[0] for row in conn.execute("SELECT lot_id FROM lots")},
                {"A", "COMMITTED"},
            )

    def test_cli_barrier_pins_before_a_later_commit(self):
        database = self.root / "wal.sqlite"
        self.make_wal_library(database)
        writer = self.open_writer(database)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO lots VALUES ('LATE','raw',7,7)")
        proc = self.start_hooked_capture(database)
        self.wait_hook(proc, "capture_after_pin")
        # Writer commits after the pin but before the copy starts.
        writer.execute("COMMIT")
        self.release_hook(proc)
        self.wait_hook(proc, "capture_before_publish")
        self.release_hook(proc)
        stdout, stderr = proc.communicate(timeout=30)
        self.assertEqual(proc.returncode, 0, stderr)
        receipt = json.loads(stdout)
        output = self.root / "restored.sqlite"
        restore(self.store, receipt["snapshot_id"], output)
        with sqlite3.connect(output) as conn:
            ids = {row[0] for row in conn.execute("SELECT lot_id FROM lots")}
        self.assertEqual(ids, {"A"})
        with sqlite3.connect(database) as conn:
            self.assertEqual(
                {row[0] for row in conn.execute("SELECT lot_id FROM lots")},
                {"A", "LATE"},
            )

    def test_writer_commits_and_new_capture_sees_it(self):
        database = self.root / "wal.sqlite"
        self.make_wal_library(database)
        writer = self.open_writer(database)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO lots VALUES ('FIRST','raw',1,1)")
        writer.execute("COMMIT")
        first = capture(database, self.store)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO lots VALUES ('SECOND','raw',2,2)")
        writer.execute("COMMIT")
        second = capture(database, self.store)
        self.assertNotEqual(first["snapshot_id"], second["snapshot_id"])
        a = self.root / "first.sqlite"
        b = self.root / "second.sqlite"
        restore(self.store, first["snapshot_id"], a)
        restore(self.store, second["snapshot_id"], b)
        with sqlite3.connect(a) as conn:
            self.assertEqual({row[0] for row in conn.execute("SELECT lot_id FROM lots")},
                             {"A", "FIRST"})
        with sqlite3.connect(b) as conn:
            self.assertEqual({row[0] for row in conn.execute("SELECT lot_id FROM lots")},
                             {"A", "FIRST", "SECOND"})

    def test_restoring_while_capture_holds_a_snapshot_is_independent(self):
        database = self.lineage_library()
        receipt = capture(database, self.store)
        # Capture again with a hook held, then restore the earlier snapshot; the
        # pinned read transaction must not block or alter the restore.
        proc = self.start_hooked_capture(database)
        try:
            self.wait_hook(proc, "capture_after_pin")
            output = self.root / "while-pinned.sqlite"
            restore(self.store, receipt["snapshot_id"], output)
            self.assertEqual(sha_file(output), receipt["image_sha256"])
        finally:
            self.release_hook(proc)
            self.wait_hook(proc, "capture_before_publish")
            self.release_hook(proc)
            proc.communicate(timeout=30)

    def test_killing_capture_before_publish_keeps_prior_snapshot_verifiable(self):
        database = self.lineage_library()
        first = capture(database, self.store)
        proc = self.start_hooked_capture(database)
        self.wait_hook(proc, "capture_after_pin")
        self.release_hook(proc)
        self.wait_hook(proc, "capture_before_publish")
        proc.kill()
        proc.communicate(timeout=30)
        self.assertEqual(list_snapshots(self.store)["count"], 1)
        self.assertEqual(verify(self.store, first["snapshot_id"])["verified"], 1)
        result = gc(self.store)
        self.assertEqual(result["errors"], [])
        self.assertEqual(verify(self.store, first["snapshot_id"])["verified"], 1)


# ---------------------------------------------------------------------------
# failure boundaries
# ---------------------------------------------------------------------------

class FailureBoundaryTest(Helpers):
    def test_capture_failure_before_publish_leaves_no_manifest(self):
        database = self.lineage_library()
        with mock.patch(
            "lot_lineage.snapshot.store._install_file",
            side_effect=OSError("injected publish failure"),
        ):
            with self.assertRaises(OSError):
                capture(database, self.store)
        self.assertEqual(list((self.store / "manifests").iterdir()), [])
        self.assertEqual(list_snapshots(self.store)["count"], 0)
        result = gc(self.store)
        self.assertEqual(result["errors"], [])
        receipt = capture(database, self.store)
        self.assertEqual(verify(self.store, receipt["snapshot_id"])["verified"], 1)

    def test_capture_failure_keeps_existing_store_and_source(self):
        database = self.lineage_library()
        first = capture(database, self.store)
        source_before = sha_file(database)
        with mock.patch(
            "lot_lineage.snapshot.store._store_object",
            side_effect=OSError("injected object write failure"),
        ):
            with self.assertRaises(OSError):
                capture(database, self.store)
        self.assertEqual(sha_file(database), source_before)
        self.assertEqual(list_snapshots(self.store)["count"], 1)
        self.assertEqual(verify(self.store, first["snapshot_id"])["verified"], 1)
        self.assertEqual(list((self.store / "tmp").iterdir()), [])

    def test_foreign_directory_is_not_a_store(self):
        foreign = self.root / "foreign"
        foreign.mkdir()
        (foreign / "something.txt").write_text("x")
        with self.assertRaisesRegex(ContractError, "has no store.json"):
            capture(self.lineage_library(), foreign)
        (foreign / "something.txt").unlink()
        (foreign / "store.json").write_text(
            '{"schema_family":"other","schema_version":1}'
        )
        with self.assertRaisesRegex(ContractError, "not a snapshot store"):
            list_snapshots(foreign)

    def test_restore_failure_keeps_existing_target_and_cleans_temp(self):
        receipt = capture(self.lineage_library(), self.store)
        output = self.root / "target.sqlite"
        output.write_bytes(b"ORIGINAL")
        with self.assertRaisesRegex(ContractError, "overwrite"):
            restore(self.store, receipt["snapshot_id"], output)
        self.assertEqual(output.read_bytes(), b"ORIGINAL")
        self.assertEqual(
            [p.name for p in self.root.iterdir() if ".snapshot.tmp" in p.name], []
        )
        fresh = self.root / "fresh.sqlite"
        with mock.patch(
            "lot_lineage.snapshot.store.os.replace",
            side_effect=OSError("injected replace failure"),
        ):
            with self.assertRaises(OSError):
                restore(self.store, receipt["snapshot_id"], fresh)
        self.assertFalse(fresh.exists())
        self.assertEqual(
            [p.name for p in self.root.iterdir() if ".snapshot.tmp" in p.name], []
        )
        restore(self.store, receipt["snapshot_id"], fresh)
        self.assertEqual(sha_file(fresh), receipt["image_sha256"])

    def test_restore_cli_reports_failure_without_touching_target(self):
        receipt = capture(self.lineage_library(), self.store)
        output = self.root / "target.sqlite"
        output.write_bytes(b"KEEP")
        result = self.run_cli(
            "restore", "--store", str(self.store),
            "--snapshot", receipt["snapshot_id"], "--output", str(output),
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("refusing to overwrite", result.stderr)
        self.assertEqual(output.read_bytes(), b"KEEP")


# ---------------------------------------------------------------------------
# CLI end to end
# ---------------------------------------------------------------------------

class CliEndToEndTest(Helpers):
    def test_cli_round_trip_all_commands(self):
        database = self.intake_library()
        captured = self.run_cli(
            "capture", "--database", str(database), "--store", str(self.store),
            "--family", "intake_tickets",
        )
        self.assertEqual(captured.returncode, 0, captured.stderr)
        receipt = json.loads(captured.stdout)
        snapshot_id = receipt["snapshot_id"]

        listed = json.loads(self.run_cli("list", "--store", str(self.store)).stdout)
        self.assertEqual([row["snapshot_id"] for row in listed["snapshots"]], [snapshot_id])

        verified = json.loads(self.run_cli(
            "verify", "--store", str(self.store), "--snapshot", snapshot_id,
            "--family", "intake_tickets",
        ).stdout)
        self.assertEqual(verified["snapshots"][0]["library_family"], "intake_tickets")

        output = self.root / "restored.sqlite"
        restored = json.loads(self.run_cli(
            "restore", "--store", str(self.store), "--snapshot", snapshot_id,
            "--output", str(output), "--library", str(database),
        ).stdout)
        self.assertEqual(restored["image_sha256"], receipt["image_sha256"])

        exported = self.root / "sealed.json"
        intake_export(output, "ceramic", exported)
        old = subprocess.run(
            [sys.executable, "-m", "lot_lineage.cli", "--source", str(exported),
             "--database", str(self.root / "batch.sqlite")],
            cwd=ROOT, env={**os.environ, "PYTHONPATH": str(SRC)},
            capture_output=True, text=True,
        )
        self.assertEqual(old.returncode, 0, old.stderr)
        built = json.loads(old.stdout)
        self.assertEqual(
            (built["raw_input_g"], built["process_loss_g"], built["available_g"]),
            (1800, 150, 1650),
        )

        forgotten = json.loads(self.run_cli(
            "forget", "--store", str(self.store), "--snapshot", snapshot_id,
        ).stdout)
        self.assertEqual(forgotten["status"], "forgotten")
        collected = json.loads(self.run_cli("gc", "--store", str(self.store)).stdout)
        self.assertGreater(collected["deleted_objects"], 0)
        self.assertEqual(
            json.loads(self.run_cli("list", "--store", str(self.store)).stdout)["count"], 0
        )

    def test_cli_errors_are_nonzero_and_explained(self):
        missing = self.run_cli("list", "--store", str(self.root / "missing"))
        self.assertEqual(missing.returncode, 2)
        self.assertIn("does not exist", missing.stderr)
        self.assertEqual(missing.stdout, "")

        capture(self.lineage_library(), self.store)
        bad = self.run_cli(
            "restore", "--store", str(self.store), "--snapshot", "not-an-id",
            "--output", str(self.root / "x.sqlite"),
        )
        self.assertEqual(bad.returncode, 2)
        self.assertIn("64 lowercase", bad.stderr)

    def test_cli_gc_reports_partial_io_deletion_as_nonzero(self):
        first = capture(self.lineage_library(), self.store)
        second = capture(self.altered_lineage_library(), self.store)
        forget(self.store, first["snapshot_id"])
        referenced = {
            chunk["sha256"]
            for chunk in read_manifest(self.store, second["snapshot_id"])["chunks"]
        }
        orphans = sorted(
            path.name for path in (self.store / "objects").rglob("*")
            if path.is_file() and path.name not in referenced
        )
        self.assertTrue(orphans, "fixture should leave at least one orphan")
        victim = orphans[0]
        real_unlink = os.unlink

        def flaky_unlink(path, *args, **kwargs):
            if str(path).endswith("/" + victim):
                raise PermissionError("injected I/O failure")
            return real_unlink(path, *args, **kwargs)

        with mock.patch("lot_lineage.snapshot.store.os.unlink", side_effect=flaky_unlink):
            receipt = gc(self.store)
        self.assertEqual(len(receipt["errors"]), 1)
        self.assertIn("could not delete object " + victim, receipt["errors"][0])
        self.assertTrue(object_path(self.store, victim).exists())
        # The CLI must exit non-zero when its receipt carries I/O errors.
        with mock.patch("lot_lineage.snapshot.store.os.unlink", side_effect=flaky_unlink), \
                mock.patch("sys.stdout", new_callable=io.StringIO), \
                mock.patch("sys.stderr", new_callable=io.StringIO):
            self.assertEqual(snapshot_cli.main(["gc", "--store", str(self.store)]), 2)
        self.assertEqual(verify(self.store)["verified"], 1)

    def test_cli_rejects_path_traversal_and_odd_snapshot_ids(self):
        capture(self.lineage_library(), self.store)
        for bad_id in ("../../etc/passwd", "..", "a/b", "0" * 63, "A" * 64, ""):
            for command in ("verify", "forget", "restore", "manifest"):
                args = [command, "--store", str(self.store), "--snapshot", bad_id]
                if command == "restore":
                    args += ["--output", str(self.root / "out.sqlite")]
                result = self.run_cli(*args)
                self.assertEqual(result.returncode, 2, (command, bad_id))
                self.assertIn("64 lowercase", result.stderr, (command, bad_id))
        self.assertFalse((self.root / "out.sqlite").exists())

    def test_cli_forget_and_verify_unknown_ids_are_nonzero(self):
        capture(self.lineage_library(), self.store)
        for args in (
            ("forget", "--store", str(self.store), "--snapshot", "0" * 64),
            ("verify", "--store", str(self.store), "--snapshot", "0" * 64),
        ):
            result = self.run_cli(*args)
            self.assertEqual(result.returncode, 2, args)
            self.assertEqual(result.stdout, "")

    def test_cli_stdout_failure_does_not_deny_published_snapshot(self):
        database = self.lineage_library()
        with mock.patch("builtins.print", side_effect=OSError("broken stdout")), \
                mock.patch("sys.stderr", new_callable=io.StringIO):
            code = snapshot_cli.main(
                ["capture", "--database", str(database), "--store", str(self.store)]
            )
        self.assertEqual(code, 3)
        self.assertEqual(list_snapshots(self.store)["count"], 1)


if __name__ == "__main__":
    unittest.main()
