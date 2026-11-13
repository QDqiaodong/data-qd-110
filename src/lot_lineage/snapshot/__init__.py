"""Offline, content-addressed library snapshots of the three business stores.

This package is independent of the manifest formats it carries.  It copies a
*source* SQLite library (the acyclic batch library, the steady-state recycle
library or the weighing-ticket intake library) into a self-contained, immutable
snapshot made of two parts:

``objects``
    Raw library bytes split into ordered 65536-byte chunks.  Each chunk is
    stored once under its SHA-256 (identical chunks, even in different
    snapshots of different libraries, share one object).  Object bytes are
    written and fsynced *before* the manifest that references them is
    published; only after an object is complete and digest-verified does any
    manifest mention it.

``manifests``
    One immutable JSON manifest per published snapshot.  The manifest records
    the snapshot version, the library family, the whole-image length and
    SHA-256, the chunk size, and the ordered chunk digests and lengths.  The
    manifest's own SHA-256 *is* the snapshot ID.  A snapshot is committed at
    exactly the moment its manifest becomes visible; a crash before that
    leaves only unreferenced objects behind, which ``gc`` may collect.

Source libraries are only ever *read*: the library is never modified, never
checkpointed and never replaced.  Its WAL state is read as one fixed,
committed snapshot; uncommitted writes of a concurrent writer are invisible,
while a writer may keep committing (or commit during the copy) without
changing what was captured.  Restoring a snapshot writes a *new* database
file outside the library whose bytes are exactly the captured image, so all
data, SQLite types and schema survive and status/export/ingest/seal still
work on the restored copy.

This baseline does not provide logical de-duplication (beyond reusing identical
byte chunks), signing, cross-version determinism guarantees or power-loss
recovery guarantees.  See ``docs/contract_snapshot.md``.
"""
from .model import (
    CHUNK_SIZE,
    LIBRARY_FAMILIES,
    SNAPSHOT_VERSION,
    ContractError,
    Manifest,
    SnapshotError,
    canonical_json,
    parse_manifest,
)
from .store import (
    META_FAMILY,
    capture,
    export_manifest,
    forget,
    gc,
    list_snapshots,
    restore,
    verify,
)

__all__ = [
    "CHUNK_SIZE",
    "ContractError",
    "LIBRARY_FAMILIES",
    "META_FAMILY",
    "Manifest",
    "SNAPSHOT_VERSION",
    "SnapshotError",
    "canonical_json",
    "capture",
    "export_manifest",
    "forget",
    "gc",
    "list_snapshots",
    "parse_manifest",
    "restore",
    "verify",
]
