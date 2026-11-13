"""Content-addressed snapshot store: capture, verify, restore, forget, gc.

On-disk layout (all names are content-derived; nothing time- or path-based)::

    <store>/store.json                 marker: schema family + version
    <store>/objects/<aa>/<sha256>      immutable 65536-byte chunk objects
    <store>/manifests/<sha256>.json    immutable snapshot manifests
    <store>/tmp/                       in-progress writes (never referenced)

Commit rule: an object file is installed and fsynced *before* any manifest can
mention it; a manifest is installed with one atomic ``os.replace`` and its
SHA-256 is the snapshot ID.  A crash before the manifest replacement therefore
leaves at most object files that no manifest references ("orphans"), which
``gc`` may remove.  A crash after it leaves one complete, verifiable snapshot.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

from .model import (
    CHUNK_SIZE,
    SNAPSHOT_VERSION,
    ChunkRef,
    ContractError,
    Manifest,
    SnapshotError,
    build_manifest,
    canonical_json,
    parse_manifest,
)
from .source import (
    copy_pinned_image,
    detect_family,
    ensure_not_alias,
    normalise_copy,
    open_source,
    pinned_snapshot,
    remove_sidecars_created_by_read,
    restorable_check,
    sidecar_state,
)

META_FAMILY = "lot_lineage_snapshots"
META_FILE = "store.json"
_HEX = frozenset("0123456789abcdef")


# ---------------------------------------------------------------------------
# small file-system primitives
# ---------------------------------------------------------------------------

def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _regular_file(path: Path, *, what: str) -> None:
    """Reject symlinks, directories, devices and dangling links."""
    if path.is_symlink():
        raise ContractError(f"{what} must not be a symbolic link: {path}")
    if not path.exists():
        raise ContractError(f"{what} does not exist: {path}")
    if not path.is_file():
        raise ContractError(f"{what} is not a regular file: {path}")


def _valid_id(snapshot_id: str) -> str:
    if (
        not isinstance(snapshot_id, str)
        or len(snapshot_id) != 64
        or any(ch not in _HEX for ch in snapshot_id)
    ):
        raise ContractError(
            "snapshot ID must be exactly 64 lowercase hexadecimal digits"
        )
    return snapshot_id


def _valid_object_name(name: str) -> str:
    if len(name) != 64 or any(ch not in _HEX for ch in name):
        raise ContractError(f"unexpected object file name {name!r}")
    return name


def _manifest_path(root: Path, snapshot_id: str) -> Path:
    return root / "manifests" / f"{_valid_id(snapshot_id)}.json"


def _object_path(root: Path, digest: str) -> Path:
    if len(digest) != 64 or any(ch not in _HEX for ch in digest):
        raise ContractError(f"invalid object digest {digest!r}")
    return root / "objects" / digest[:2] / digest


# ---------------------------------------------------------------------------
# store lifecycle
# ---------------------------------------------------------------------------

def _read_marker(root: Path) -> None:
    marker = root / META_FILE
    _regular_file(marker, what="snapshot store marker")
    raw = marker.read_bytes()
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ContractError(f"snapshot store marker {marker} is not valid JSON: {exc}") from exc
    if not isinstance(data, dict) or set(data) != {"schema_family", "schema_version"}:
        raise ContractError(f"snapshot store marker {marker} has invalid fields")
    if data["schema_family"] != META_FAMILY:
        raise ContractError(
            f"directory is not a snapshot store (family {data['schema_family']!r})"
        )
    if data["schema_version"] != SNAPSHOT_VERSION:
        raise ContractError(
            f"unsupported snapshot store version {data['schema_version']!r}; "
            f"expected {SNAPSHOT_VERSION}"
        )


def _write_marker(root: Path) -> None:
    raw = canonical_json(
        {"schema_family": META_FAMILY, "schema_version": SNAPSHOT_VERSION}
    )
    marker = root / META_FILE
    fd, temp = tempfile.mkstemp(prefix=".store-", suffix=".tmp", dir=str(root))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, marker)
        _fsync_dir(root)
    except BaseException:
        Path(temp).unlink(missing_ok=True)
        raise


def _prepare_store(store, *, create: bool) -> Path:
    root = Path(store)
    if root.is_symlink():
        raise ContractError(f"snapshot store must not be a symbolic link: {root}")
    if not root.exists():
        if not create:
            raise ContractError(f"snapshot store does not exist: {root}")
        root.mkdir(parents=True)
    if not root.is_dir():
        raise ContractError(f"snapshot store is not a directory: {root}")
    marker = root / META_FILE
    if not marker.exists():
        if not create:
            raise ContractError(f"directory is not a snapshot store: {root}")
        leftovers = [entry for entry in root.iterdir() if entry.name != META_FILE]
        if leftovers:
            raise ContractError(
                f"refusing to initialise snapshot store at {root}: directory "
                f"is not empty and has no {META_FILE} marker"
            )
        _write_marker(root)
    _read_marker(root)
    for name in ("objects", "manifests", "tmp"):
        path = root / name
        if path.is_symlink():
            raise ContractError(f"snapshot store entry must not be a symlink: {path}")
        if path.exists() and not path.is_dir():
            raise ContractError(f"snapshot store entry is not a directory: {path}")
        if not path.exists():
            if not create:
                raise ContractError(f"snapshot store is missing its {name}/ directory")
            path.mkdir()
    return root


def _install_file(temp: Path, destination: Path) -> None:
    os.replace(temp, destination)
    _fsync_dir(destination.parent)


def _existing_object(root: Path, digest: str, block: bytes) -> bool:
    path = _object_path(root, digest)
    if not path.exists() and not path.is_symlink():
        return False
    _regular_file(path, what="snapshot object")
    if path.stat().st_size != len(block):
        raise ContractError(f"snapshot object {digest} has an unexpected length")
    with open(path, "rb") as handle:
        stored = hashlib.sha256()
        while True:
            chunk = handle.read(CHUNK_SIZE)
            if not chunk:
                break
            stored.update(chunk)
    if stored.hexdigest() != digest:
        raise ContractError(f"snapshot object {digest} is corrupted")
    return True


def _store_object(root: Path, digest: str, block: bytes) -> bool:
    """Persist one chunk; returns True when a new object was written."""
    if hashlib.sha256(block).hexdigest() != digest:  # pragma: no cover - defensive
        raise ContractError("chunk digest does not match its bytes")
    if _existing_object(root, digest, block):
        return False
    path = _object_path(root, digest)
    path.parent.mkdir(exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=".object-", suffix=".tmp", dir=str(root / "tmp"))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(block)
            handle.flush()
            os.fsync(handle.fileno())
        if path.exists():
            # Another writer installed it concurrently: keep their identical copy.
            _regular_file(path, what="snapshot object")
            Path(temp).unlink(missing_ok=True)
            return False
        _install_file(Path(temp), path)
    except BaseException:
        Path(temp).unlink(missing_ok=True)
        raise
    return True


def _load_manifests(root: Path) -> dict[str, Manifest]:
    directory = root / "manifests"
    manifests: dict[str, Manifest] = {}
    for entry in sorted(directory.iterdir(), key=lambda item: item.name):
        _regular_file(entry, what="entry in manifests/")
        if not entry.name.endswith(".json"):
            raise ContractError(f"unexpected entry in manifests/: {entry.name}")
        snapshot_id = entry.name[: -len(".json")]
        _valid_id(snapshot_id)
        raw = entry.read_bytes()
        manifest = parse_manifest(raw, source_label=str(entry))
        digest = hashlib.sha256(raw).hexdigest()
        if digest != snapshot_id:
            raise ContractError(
                f"manifest {entry.name} does not hash to its file name"
            )
        if manifest.snapshot_id != snapshot_id:  # pragma: no cover - defensive
            raise ContractError(f"manifest {entry.name} has an inconsistent ID")
        manifests[snapshot_id] = manifest
    return manifests


def _require_manifest(root: Path, snapshot_id: str) -> tuple[Manifest, bytes]:
    path = _manifest_path(root, snapshot_id)
    _regular_file(path, what="snapshot manifest")
    raw = path.read_bytes()
    manifest = parse_manifest(raw, source_label=str(path))
    if hashlib.sha256(raw).hexdigest() != snapshot_id:
        raise ContractError(f"manifest {snapshot_id} does not hash to its file name")
    return manifest, raw


# ---------------------------------------------------------------------------
# capture
# ---------------------------------------------------------------------------

def capture(
    database,
    store,
    *,
    expected_family: str | None = None,
    _sync_hook=None,
) -> dict:
    """Publish one immutable snapshot of a live business library."""
    source_path = Path(database)
    before = sidecar_state(source_path)
    conn = open_source(source_path)
    after_open = sidecar_state(source_path)
    image_temp: Path | None = None
    root: Path | None = None
    try:
        try:
            with pinned_snapshot(conn):
                family = detect_family(conn)
                if expected_family is not None and family != expected_family:
                    raise ContractError(
                        f"library {database} is {family!r}, expected "
                        f"{expected_family!r}"
                    )
                # The store is only touched after the source proved to be a
                # supported library of the expected family.
                root = _prepare_store(store, create=True)
                if _sync_hook is not None:
                    _sync_hook("capture_after_pin")
                fd, temp_name = tempfile.mkstemp(
                    prefix=".image-", suffix=".tmp", dir=str(root / "tmp")
                )
                os.close(fd)
                image_temp = Path(temp_name)
                copy_pinned_image(conn, image_temp)
        finally:
            conn.close()
            # Read-only sidecars *we* caused SQLite to materialise are removed
            # once the pinned read is over; pre-existing sidecars stay untouched.
            remove_sidecars_created_by_read(source_path, before, after_open)

        normalise_copy(image_temp)
        image_sha = hashlib.sha256()
        chunks: list[ChunkRef] = []
        image_length = 0
        new_objects = 0
        reused_objects = 0
        with open(image_temp, "rb") as handle:
            while True:
                block = handle.read(CHUNK_SIZE)
                if not block:
                    break
                image_length += len(block)
                image_sha.update(block)
                digest = hashlib.sha256(block).hexdigest()
                chunks.append(ChunkRef(digest, len(block)))
                if _store_object(root, digest, block):
                    new_objects += 1
                else:
                    reused_objects += 1
        manifest = build_manifest(
            family, image_sha.hexdigest(), chunks, image_length
        )
        raw = manifest.to_bytes()
        snapshot_id = hashlib.sha256(raw).hexdigest()
        if snapshot_id != manifest.snapshot_id:  # pragma: no cover - defensive
            raise ContractError("manifest ID does not match its bytes")
        if _sync_hook is not None:
            _sync_hook("capture_before_publish")
        path = _manifest_path(root, snapshot_id)
        existed = path.exists()
        if existed:
            _regular_file(path, what="snapshot manifest")
            if path.read_bytes() != raw:
                raise ContractError(
                    f"snapshot {snapshot_id} already exists with different bytes"
                )
        else:
            fd, temp_name = tempfile.mkstemp(
                prefix=".manifest-", suffix=".tmp", dir=str(root / "tmp")
            )
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(raw)
                    handle.flush()
                    os.fsync(handle.fileno())
                _install_file(Path(temp_name), path)
            except BaseException:
                Path(temp_name).unlink(missing_ok=True)
                raise
    finally:
        if image_temp is not None:
            image_temp.unlink(missing_ok=True)

    return {
        "snapshot_id": snapshot_id,
        "library_family": family,
        "image_length": image_length,
        "image_sha256": image_sha.hexdigest(),
        "chunk_size": CHUNK_SIZE,
        "chunk_count": len(chunks),
        "unique_chunk_count": len({chunk.sha256 for chunk in chunks}),
        "new_objects": new_objects,
        "reused_objects": reused_objects,
        "new_manifest": not existed,
    }


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

def list_snapshots(store) -> dict:
    root = _prepare_store(store, create=False)
    manifests = _load_manifests(root)
    rows = []
    for snapshot_id in sorted(manifests):
        manifest = manifests[snapshot_id]
        rows.append(
            {
                "snapshot_id": snapshot_id,
                "library_family": manifest.library_family,
                "image_length": manifest.image_length,
                "image_sha256": manifest.image_sha256,
                "chunk_count": len(manifest.chunks),
                "unique_chunk_count": len(set(manifest.object_ids())),
            }
        )
    return {"count": len(rows), "snapshots": rows}


# ---------------------------------------------------------------------------
# manifest export
# ---------------------------------------------------------------------------

def export_manifest(store, snapshot_id: str) -> dict:
    """Return the stored manifest exactly as published, plus its ID."""
    root = _prepare_store(store, create=False)
    _valid_id(snapshot_id)
    manifest, raw = _require_manifest(root, snapshot_id)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:  # pragma: no cover - parse_manifest checked
        raise ContractError(f"manifest {snapshot_id} is not UTF-8: {exc}") from exc
    return {
        "snapshot_id": snapshot_id,
        "manifest_sha256": hashlib.sha256(raw).hexdigest(),
        "manifest_utf8": text,
        "manifest_bytes": len(raw),
        "library_family": manifest.library_family,
        "image_length": manifest.image_length,
        "image_sha256": manifest.image_sha256,
        "chunk_size": manifest.chunk_size,
        "chunk_count": len(manifest.chunks),
    }


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

def _assemble(root: Path, manifest: Manifest, image_path: Path) -> None:
    """Rebuild the closed image from ordered chunks, checking every chunk."""
    image_sha = hashlib.sha256()
    total = 0
    with open(image_path, "wb") as out:
        for index, chunk in enumerate(manifest.chunks):
            path = _object_path(root, chunk.sha256)
            _regular_file(path, what=f"snapshot object for chunk {index}")
            if path.stat().st_size != chunk.length:
                raise ContractError(
                    f"chunk {index} object {chunk.sha256} has length "
                    f"{path.stat().st_size}, manifest says {chunk.length}"
                )
            digest = hashlib.sha256()
            remaining = chunk.length
            with open(path, "rb") as handle:
                while remaining:
                    block = handle.read(min(CHUNK_SIZE, remaining))
                    if not block:
                        raise ContractError(
                            f"chunk {index} object {chunk.sha256} ended early"
                        )
                    digest.update(block)
                    image_sha.update(block)
                    out.write(block)
                    remaining -= len(block)
            if digest.hexdigest() != chunk.sha256:
                raise ContractError(
                    f"chunk {index} object {chunk.sha256} failed its digest check"
                )
            total += chunk.length
        out.flush()
        os.fsync(out.fileno())
    if total != manifest.image_length:
        raise ContractError(
            f"assembled image is {total} bytes, manifest says {manifest.image_length}"
        )
    if image_sha.hexdigest() != manifest.image_sha256:
        raise ContractError("assembled image failed its whole-image digest check")


def _verify_manifest(root: Path, snapshot_id: str, manifest: Manifest) -> dict:
    fd, temp_name = tempfile.mkstemp(prefix=".snapshot-verify-", suffix=".sqlite")
    os.close(fd)
    image = Path(temp_name)
    try:
        _assemble(root, manifest, image)
        family = restorable_check(image, expected_family=manifest.library_family)
    finally:
        image.unlink(missing_ok=True)
    return {
        "snapshot_id": snapshot_id,
        "status": "verified",
        "library_family": family,
        "image_length": manifest.image_length,
        "image_sha256": manifest.image_sha256,
        "chunk_count": len(manifest.chunks),
    }


def verify(store, snapshot_id: str | None = None, *, expected_family: str | None = None) -> dict:
    root = _prepare_store(store, create=False)
    if snapshot_id is not None:
        manifest, _raw = _require_manifest(root, _valid_id(snapshot_id))
        rows = [_verify_manifest(root, snapshot_id, manifest)]
    else:
        manifests = _load_manifests(root)
        rows = [
            _verify_manifest(root, sid, manifests[sid]) for sid in sorted(manifests)
        ]
    if expected_family is not None:
        for row in rows:
            if row["library_family"] != expected_family:
                raise ContractError(
                    f"snapshot {row['snapshot_id']} is {row['library_family']!r}, "
                    f"expected {expected_family!r}"
                )
    return {"verified": len(rows), "snapshots": rows}


# ---------------------------------------------------------------------------
# restore
# ---------------------------------------------------------------------------

def _inside(root: Path, candidate: Path) -> bool:
    try:
        root_resolved = root.resolve(strict=True)
    except OSError:
        return False
    try:
        candidate_resolved = candidate.resolve(strict=False)
    except OSError:
        return False
    try:
        candidate_resolved.relative_to(root_resolved)
        return True
    except ValueError:
        return False


def restore(
    store,
    snapshot_id: str,
    output,
    *,
    expected_family: str | None = None,
    library=None,
    _sync_hook=None,
) -> dict:
    """Publish the snapshot's exact image as a new file outside the library."""
    root = _prepare_store(store, create=False)
    _valid_id(snapshot_id)
    manifest, _raw = _require_manifest(root, snapshot_id)
    if expected_family is not None and manifest.library_family != expected_family:
        raise ContractError(
            f"snapshot is {manifest.library_family!r}, expected {expected_family!r}"
        )

    target = Path(output)
    if target.is_symlink():
        raise ContractError(f"refusing to restore onto a symbolic link: {target}")
    if target.exists():
        raise ContractError(
            f"refusing to overwrite an existing file: {target}"
        )
    if _inside(root, target):
        raise ContractError(
            f"refusing to restore inside the snapshot store: {target}"
        )
    if library is not None:
        ensure_not_alias(target, Path(library))
    target.parent.mkdir(parents=True, exist_ok=True)
    if _inside(root, target):  # pragma: no cover - parent creation cannot move it
        raise ContractError(f"refusing to restore inside the snapshot store: {target}")

    fd, temp_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".snapshot.tmp", dir=str(target.parent)
    )
    os.close(fd)
    image = Path(temp_name)
    try:
        _assemble(root, manifest, image)
        family = restorable_check(image, expected_family=manifest.library_family)
        if _sync_hook is not None:
            _sync_hook("restore_before_publish")
        if target.exists() or target.is_symlink():
            raise ContractError(f"refusing to overwrite an existing file: {target}")
        os.replace(image, target)
        _fsync_dir(target.parent)
    finally:
        image.unlink(missing_ok=True)
    return {
        "snapshot_id": snapshot_id,
        "status": "restored",
        "library_family": family,
        "image_length": manifest.image_length,
        "image_sha256": manifest.image_sha256,
        "chunk_count": len(manifest.chunks),
    }


# ---------------------------------------------------------------------------
# forget
# ---------------------------------------------------------------------------

def forget(store, snapshot_id: str) -> dict:
    """Delete only the manifest; its objects stay until ``gc`` sees no references."""
    root = _prepare_store(store, create=False)
    _valid_id(snapshot_id)
    path = _manifest_path(root, snapshot_id)
    if not path.exists() and not path.is_symlink():
        raise ContractError(f"unknown snapshot {snapshot_id}")
    _regular_file(path, what="snapshot manifest")
    manifest = parse_manifest(path.read_bytes(), source_label=str(path))
    if hashlib.sha256(path.read_bytes()).hexdigest() != snapshot_id:
        raise ContractError(f"manifest {snapshot_id} does not hash to its file name")
    os.unlink(path)
    _fsync_dir(path.parent)
    return {
        "snapshot_id": snapshot_id,
        "status": "forgotten",
        "objects_kept": len(set(manifest.object_ids())),
    }


# ---------------------------------------------------------------------------
# gc
# ---------------------------------------------------------------------------

def _walk_objects(root: Path) -> tuple[list[tuple[Path, str]], list[tuple[ContractError, str]]]:
    found: list[tuple[Path, str]] = []
    problems: list[tuple[ContractError, str]] = []
    objects = root / "objects"
    for shard in sorted(objects.iterdir(), key=lambda item: item.name):
        if shard.is_symlink() or not shard.is_dir():
            problems.append(
                (ContractError(f"unexpected entry in objects/: {shard.name}"), "object")
            )
            continue
        if len(shard.name) != 2 or any(ch not in _HEX for ch in shard.name):
            problems.append(
                (ContractError(f"unexpected object shard name: {shard.name}"), "shard")
            )
            continue
        for entry in sorted(shard.iterdir(), key=lambda item: item.name):
            if entry.is_symlink() or not entry.is_file():
                problems.append(
                    (ContractError(f"unexpected object entry: {entry.name}"), "object")
                )
                continue
            try:
                _valid_object_name(entry.name)
            except ContractError:
                problems.append(
                    (ContractError(f"unexpected object entry: {entry.name}"), "object")
                )
                continue
            if entry.name[:2] != shard.name:
                problems.append(
                    (ContractError(f"object {entry.name} is in the wrong shard"), "object")
                )
                continue
            found.append((entry, entry.name))
    return found, problems


def _corrupt_gc_target(problems) -> None:
    """An unreachable entry whose name cannot be trusted must not be deleted."""
    details = "; ".join(str(problem) for problem, _kind in problems)
    raise SnapshotError(
        "refusing to collect garbage: the objects directory contains entries "
        f"that are not content-addressed object files: {details}"
    )


def gc(store) -> dict:
    """Validate every manifest reference, then delete only unreachable objects."""
    root = _prepare_store(store, create=False)
    manifests = _load_manifests(root)

    referenced: set[str] = set()
    missing: list[str] = []
    for snapshot_id, manifest in sorted(manifests.items()):
        for chunk in manifest.chunks:
            referenced.add(chunk.sha256)
    # Pre-flight: every referenced object must exist, be regular, and match
    # both its recorded length and its digest before anything is deleted.
    for snapshot_id, manifest in sorted(manifests.items()):
        for index, chunk in enumerate(manifest.chunks):
            path = _object_path(root, chunk.sha256)
            try:
                _regular_file(path, what="referenced snapshot object")
                if path.stat().st_size != chunk.length:
                    raise ContractError("length mismatch")
                digest = hashlib.sha256()
                with open(path, "rb") as handle:
                    while True:
                        block = handle.read(CHUNK_SIZE)
                        if not block:
                            break
                        digest.update(block)
                if digest.hexdigest() != chunk.sha256:
                    raise ContractError("digest mismatch")
            except (ContractError, OSError) as exc:
                missing.append(
                    f"snapshot {snapshot_id} chunk {index}: {chunk.sha256}: {exc}"
                )
    if missing:
        raise SnapshotError(
            "refusing to collect garbage: referenced objects failed pre-flight "
            "validation: " + "; ".join(missing)
        )

    # Checked *before* any manifest or temp file is touched: an untrusted
    # objects/ tree means the reference set itself cannot be trusted.
    objects, problems = _walk_objects(root)
    if problems:
        _corrupt_gc_target(problems)
    errors: list[str] = []
    removed_temp = 0
    for entry in sorted((root / "tmp").iterdir(), key=lambda item: item.name):
        try:
            if entry.is_symlink() or entry.is_file():
                entry.unlink()
            elif entry.is_dir():
                shutil.rmtree(entry)
            removed_temp += 1
        except OSError as exc:
            errors.append(f"could not remove temporary entry {entry.name}: {exc}")

    scanned = len(objects)
    deleted = 0
    freed = 0
    for path, digest in objects:
        if digest in referenced:
            continue
        try:
            size = path.stat().st_size
            os.unlink(path)
        except OSError as exc:
            errors.append(f"could not delete object {digest}: {exc}")
            continue
        deleted += 1
        freed += size
    # Remove now-empty shard directories, but never the objects root itself.
    for shard in sorted((root / "objects").iterdir(), key=lambda item: item.name):
        if shard.is_dir() and not shard.is_symlink():
            try:
                if not any(shard.iterdir()):
                    shard.rmdir()
            except OSError as exc:
                errors.append(f"could not remove empty shard {shard.name}: {exc}")
    return {
        "referenced_objects": len(referenced),
        "scanned_objects": scanned,
        "deleted_objects": deleted,
        "kept_objects": scanned - deleted,
        "freed_bytes": freed,
        "removed_temp_entries": removed_temp,
        "errors": errors,
    }
