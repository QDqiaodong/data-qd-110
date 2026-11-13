#!/usr/bin/env python3
"""Runnable demonstration of the offline library snapshot store.

It builds the three business libraries from the shipped synthetic fixtures in
a throw-away directory, snapshots and restores them, and shows the behaviours
described in ``docs/contract_snapshot.md``:

1. capture/list/verify/restore/forget/gc of the batch, recycle and intake
   libraries, including restoring after the source libraries and the original
   CSV inputs have been moved away;
2. fixed committed snapshot semantics against a concurrent WAL writer
   (uncommitted data is invisible; a writer may commit);
3. repeated shared chunks and a BLOB spanning more than three chunks;
4. the independently recomputed reachable object set before and after gc.

Every output path is under the directory given by ``--output`` (default
``run/snapshot-demo``); the script deletes and recreates only that directory.
No source fixture is modified and nothing is written into the repository.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lot_lineage.intake import export as intake_export  # noqa: E402
from lot_lineage.intake import ingest as intake_ingest  # noqa: E402
from lot_lineage.intake import seal as intake_seal  # noqa: E402
from lot_lineage.pipeline import build as build_batch  # noqa: E402
from lot_lineage.recycle.pipeline import build as build_recycle  # noqa: E402
from lot_lineage.snapshot import (  # noqa: E402
    capture,
    forget,
    gc,
    list_snapshots,
    restore,
    verify,
)

FIXTURES = ROOT / "fixtures"
BATCH1 = FIXTURES / "intake" / "batch1.csv"
BATCH2 = FIXTURES / "intake" / "batch2.csv"


def section(title: str) -> None:
    print(f"\n== {title} ==")


def make_intake(path: Path, inputs: Path) -> Path:
    intake_ingest(path, "ceramic", inputs / BATCH1.name)
    intake_ingest(path, "ceramic", inputs / BATCH2.name)
    intake_seal(path, "ceramic")
    return path


def make_duplicate_page_library(path: Path) -> Path:
    """A real lineage library whose file contains byte-identical 64 KiB pages."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA page_size=65536")
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.executescript(
            """
            CREATE TABLE lots(lot_id TEXT PRIMARY KEY, kind TEXT, mass_g INTEGER, available_g INTEGER);
            CREATE TABLE transforms(output_lot TEXT PRIMARY KEY, loss_g INTEGER);
            CREATE TABLE inputs(output_lot TEXT, input_lot TEXT, mass_g INTEGER);
            CREATE TABLE origin_mix(lot_id TEXT, origin_lot TEXT, numerator TEXT, denominator TEXT);
            """
        )
        conn.execute("INSERT INTO lots VALUES ('A','raw',1000,1000)")
        conn.execute("INSERT INTO origin_mix VALUES ('A','A','1','1')")
        for table in ("page_a", "page_b", "page_c"):
            conn.execute(
                f"CREATE TABLE {table}(id INTEGER PRIMARY KEY, payload BLOB)"
            )
            conn.execute(
                f"INSERT INTO {table}(payload) VALUES (?)", (b"Q" * 60000,)
            )
        conn.commit()
    finally:
        conn.close()
    return path


def make_lineage_with_blob(path: Path, payload: int, page_size: int = 8192) -> Path:
    conn = sqlite3.connect(path)
    try:
        conn.execute(f"PRAGMA page_size={page_size}")
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.executescript(
            """
            CREATE TABLE lots(lot_id TEXT PRIMARY KEY, kind TEXT, mass_g INTEGER, available_g INTEGER);
            CREATE TABLE transforms(output_lot TEXT PRIMARY KEY, loss_g INTEGER);
            CREATE TABLE inputs(output_lot TEXT, input_lot TEXT, mass_g INTEGER);
            CREATE TABLE origin_mix(lot_id TEXT, origin_lot TEXT, numerator TEXT, denominator TEXT);
            CREATE TABLE blobs(id INTEGER PRIMARY KEY, payload BLOB);
            """
        )
        conn.execute("INSERT INTO lots VALUES ('A','raw',?,?)", (payload, payload))
        conn.execute("INSERT INTO origin_mix VALUES ('A','A','1','1')")
        conn.execute("INSERT INTO blobs(payload) VALUES (?)", (b"Z" * payload,))
        conn.commit()
    finally:
        conn.close()
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", default=str(ROOT / "run" / "snapshot-demo"))
    args = parser.parse_args()
    work = Path(args.output)
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    store = work / "snapshots"

    section("build the three business libraries")
    inputs = work / "inputs"
    inputs.mkdir()
    for source in (BATCH1, BATCH2):
        shutil.copy2(source, inputs / source.name)
    lineage = work / "lineage.sqlite"
    recycle = work / "recycle.sqlite"
    intake = work / "intake.sqlite"
    batch_result = build_batch(FIXTURES / "lots.json", lineage)
    build_recycle(FIXTURES / "recycle_pq.json", recycle)
    make_intake(intake, inputs)
    print("batch  :", {k: batch_result[k] for k in ("raw_input_g", "process_loss_g", "available_g")})
    print("recycle: P A/B = 39/41; intake: sealed with 11 tickets")

    section("capture / list / verify / manifest")
    receipts = {}
    for family, path in (
        ("lineage", lineage),
        ("recycle", recycle),
        ("intake_tickets", intake),
    ):
        receipts[family] = capture(path, store, expected_family=family)
        print(f"{family:15s} id={receipts[family]['snapshot_id'][:16]}… "
              f"chunks={receipts[family]['chunk_count']} "
              f"unique={receipts[family]['unique_chunk_count']}")
    print("list:", list_snapshots(store)["count"], "snapshots")
    print("verify:", verify(store)["verified"], "verified")

    manifest_path = store / "manifests" / f"{receipts['lineage']['snapshot_id']}.json"
    raw = manifest_path.read_bytes()
    print("manifest is canonical sorted, no newline:", b"\n" not in raw)
    print("manifest SHA256 == snapshot ID:",
          hashlib.sha256(raw).hexdigest() == receipts["lineage"]["snapshot_id"])

    section("move sources and CSV inputs away, then restore")
    away = work / "moved-away"
    away.mkdir()
    for path in (lineage, recycle, intake):
        shutil.move(str(path), away / path.name)
    for source in (BATCH1, BATCH2):
        shutil.move(str(inputs / source.name), away / source.name)
    restored = {}
    for family, receipt in receipts.items():
        target = work / f"restored-{family}.sqlite"
        restore(store, receipt["snapshot_id"], target)
        restored[family] = target
        digest_ok = hashlib.sha256(target.read_bytes()).hexdigest() == receipt["image_sha256"]
        print(f"{family:15s} restored={target.name} image_sha_ok={digest_ok}")
    exported = work / "restored-sealed.json"
    intake_export(restored["intake_tickets"], "ceramic", exported)
    rebuilt = build_batch(exported, work / "restored-lineage-from-intake.sqlite")
    print("old CLI on restored intake export:",
          {k: rebuilt[k] for k in ("raw_input_g", "process_loss_g", "available_g")})

    section("WAL: uncommitted writes invisible, writers keep committing")
    wal = work / "wal.sqlite"
    conn = sqlite3.connect(wal)
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA wal_autocheckpoint=0;
        CREATE TABLE lots(lot_id TEXT PRIMARY KEY, kind TEXT, mass_g INTEGER, available_g INTEGER);
        CREATE TABLE transforms(output_lot TEXT PRIMARY KEY, loss_g INTEGER);
        CREATE TABLE inputs(output_lot TEXT, input_lot TEXT, mass_g INTEGER);
        CREATE TABLE origin_mix(lot_id TEXT, origin_lot TEXT, numerator TEXT, denominator TEXT);
        INSERT INTO lots VALUES ('A','raw',1000,1000);
        """
    )
    conn.commit()
    writer = sqlite3.connect(wal, isolation_level=None)
    writer.execute("BEGIN IMMEDIATE")
    writer.execute("INSERT INTO lots VALUES ('UNCOMMITTED','raw',5,5)")
    wal_receipt = capture(wal, store)
    writer.execute("COMMIT")
    target = work / "restored-wal.sqlite"
    restore(store, wal_receipt["snapshot_id"], target)
    with sqlite3.connect(target) as check:
        snapshot_ids = {row[0] for row in check.execute("SELECT lot_id FROM lots")}
    with sqlite3.connect(wal) as check:
        live_ids = {row[0] for row in check.execute("SELECT lot_id FROM lots")}
    print("snapshot lot ids     :", sorted(snapshot_ids))
    print("live library ids     :", sorted(live_ids))
    writer.close()
    conn.close()

    section("repeated shared chunks and a BLOB > 3 chunks")
    dup_db = make_duplicate_page_library(work / "duplicate-pages.sqlite")
    dup_receipt = capture(dup_db, store)
    print(f"duplicate-page library: chunks={dup_receipt['chunk_count']} "
          f"unique={dup_receipt['unique_chunk_count']} "
          f"({dup_receipt['chunk_count'] - dup_receipt['unique_chunk_count']} "
          "identical pages stored once)")
    dup_target = work / "restored-duplicate-pages.sqlite"
    restore(store, dup_receipt["snapshot_id"], dup_target)
    print("duplicate-page image digest matches:",
          hashlib.sha256(dup_target.read_bytes()).hexdigest() == dup_receipt["image_sha256"])

    blob_db = make_lineage_with_blob(work / "blob.sqlite", 400_000)
    blob_receipt = capture(blob_db, store)
    print(f"BLOB library: chunks={blob_receipt['chunk_count']} "
          f"(more than three 65536-byte chunks)")
    blob_target = work / "restored-blob.sqlite"
    restore(store, blob_receipt["snapshot_id"], blob_target)
    with sqlite3.connect(blob_target) as check:
        blob_length = check.execute("SELECT length(payload) FROM blobs").fetchone()[0]
    print("restored BLOB length:", blob_length)
    shared = capture(blob_db, store)
    print("recapturing the same bytes reuses objects:",
          shared["reused_objects"] == shared["chunk_count"])

    section("independent reachable object set, forget and gc")
    reachable = set()
    for manifest in (store / "manifests").glob("*.json"):
        reachable |= {chunk["sha256"] for chunk in json.loads(manifest.read_text())["chunks"]}
    stored = {p.name for p in (store / "objects").rglob("*") if p.is_file()}
    print("stored objects == union of manifest chunks:", stored == reachable)
    doomed = receipts["lineage"]["snapshot_id"]
    forget(store, doomed)
    result = gc(store)
    remaining = set()
    for manifest in (store / "manifests").glob("*.json"):
        remaining |= {chunk["sha256"] for chunk in json.loads(manifest.read_text())["chunks"]}
    still_stored = {p.name for p in (store / "objects").rglob("*") if p.is_file()}
    print("after forget+gc, objects == remaining references:", still_stored == remaining)
    print("gc receipt:", {k: result[k] for k in
                         ("referenced_objects", "deleted_objects", "kept_objects")})
    print("verify remaining:", verify(store)["verified"])

    section("summary")
    print("All outputs are under:", work)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
