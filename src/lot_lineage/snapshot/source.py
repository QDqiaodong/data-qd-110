"""Source-library access: family detection and fixed committed snapshots.

The source library is opened read-only for the whole capture.  SQLite is asked
for the rollback journal's *committed* view of the database exactly once: the
copy is taken from a ``BEGIN``-pinned read snapshot, so a concurrent writer's
uncommitted changes are invisible and commits that land after the pin do not
change the captured image either.  The copy step reads the pinned connection
through ``sqlite3.Connection.backup`` into a private temporary database; the
source files themselves are never checkpointed, truncated, replaced or
written to.  A WAL that happens to contain already-committed frames is
included in the fixed snapshot just like any other committed state.

Uncommitted WAL frames in the source are never copied, because they are not
visible to any reader.  Writers are free to keep committing: that changes only
states after our pin, never the snapshot.
"""
from __future__ import annotations

import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path
from urllib.parse import quote

from .model import ContractError

def table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def detect_family(conn: sqlite3.Connection) -> str:
    """Return ``lineage``, ``recycle``, ``intake_tickets`` or raise.

    Detection is deliberately conservative: a stray table with a familiar name
    (for example a lone ``inputs``) is not enough to call a database a business
    library, so unknown databases are refused rather than snapshotted under a
    wrong family.
    """
    tables = table_names(conn)
    if {"recycle_source", "recycle_process", "recycle_sink"} <= tables:
        return "recycle"
    if "recycle_meta" in tables and "recycle_source" in tables:
        return "recycle"
    if {"intake_campaign", "intake_ticket"} <= tables:
        return "intake_tickets"
    if "intake_meta" in tables and "intake_campaign" in tables:
        return "intake_tickets"
    if {"lots", "transforms", "inputs", "origin_mix"} <= tables:
        return "lineage"
    raise ContractError(
        "source is not one of the supported business libraries "
        "(batch lineage, steady-state recycle, weighing-ticket intake)"
    )


def open_source(database: Path) -> sqlite3.Connection:
    """Open the source read-only; refuses symlinks, devices and other oddities."""
    path = Path(database)
    if not path.exists():
        raise ContractError(f"library does not exist: {path}")
    if path.is_symlink():
        raise ContractError(f"refusing symlink library path: {path}")
    if not path.is_file():
        raise ContractError(f"library path is not a regular file: {path}")
    try:
        conn = sqlite3.connect(
            f"file:{quote(str(path))}?mode=ro", uri=True, isolation_level=None
        )
    except sqlite3.Error as exc:
        raise ContractError(f"cannot read library {path}: {exc}") from exc
    try:
        conn.execute("PRAGMA query_only=ON")
        # Touch the file once: a directory or garbage file fails here, before
        # we claim anything about the library.
        conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
    except sqlite3.DatabaseError as exc:
        conn.close()
        raise ContractError(f"library {path} is not a readable SQLite database: {exc}") from exc
    return conn


def sidecars(database: Path) -> list[Path]:
    return [Path(str(database) + suffix) for suffix in ("-wal", "-shm", "-journal")]


def sidecar_state(database: Path) -> dict[Path, tuple[int, int] | None]:
    """Record which SQLite sidecars exist (and their identity) before reading."""
    state: dict[Path, tuple[int, int] | None] = {}
    for path in sidecars(database):
        try:
            stat = path.stat()
        except OSError:
            state[path] = None
        else:
            state[path] = (stat.st_dev, stat.st_ino)
    return state


def remove_sidecars_created_by_read(
    database: Path,
    before: dict[Path, tuple[int, int] | None],
    after_open: dict[Path, tuple[int, int] | None],
) -> None:
    """Delete only the empty SQLite cache sidecars our read-only open created.

    Reading a WAL-mode database read-only makes SQLite materialise its own
    ``-wal``/``-shm`` cache files; the library's main file is never written.
    If a sidecar did not exist before we opened the library, we remove exactly
    the file our open created (same device and inode) once we are done, so an
    offline capture leaves the library directory as it found it.  Sidecars
    that already existed are left strictly untouched.  A non-empty ``-wal``
    may carry frames some other process committed, so it is never removed;
    its ``-shm`` is kept alongside it.
    """
    wal_path = Path(str(database) + "-wal")
    shm_path = Path(str(database) + "-shm")
    for path in (wal_path, shm_path):
        if before.get(path) is not None:
            continue  # existed before our open: not ours
        try:
            stat = path.stat()
        except OSError:
            continue
        if after_open.get(path) != (stat.st_dev, stat.st_ino):
            continue  # someone replaced it meanwhile: not ours to remove
        if path == wal_path and stat.st_size:
            continue  # non-empty WAL: keep it together with its -shm
        if path == shm_path:
            try:
                wal_size = wal_path.stat().st_size
            except OSError:
                wal_size = 0
            if wal_size:
                continue
        try:
            path.unlink()
        except OSError:  # pragma: no cover - best effort cleanup
            pass


@contextmanager
def pinned_snapshot(conn: sqlite3.Connection):
    """Hold one fixed committed read snapshot for the duration of the block."""
    conn.execute("BEGIN")
    try:
        # The first read pins the snapshot; every later read in this
        # connection sees exactly that committed state.
        conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
        yield
    finally:
        try:
            conn.execute("COMMIT")
        except sqlite3.Error:  # pragma: no cover - read-only transaction
            pass


def copy_pinned_image(conn: sqlite3.Connection, destination: Path) -> None:
    """Copy the pinned committed image into ``destination`` (close before return)."""
    destination = Path(destination)
    if destination.exists():
        destination.unlink()
    with closing(sqlite3.connect(str(destination))) as target:
        try:
            conn.backup(target)
        except sqlite3.Error as exc:  # pragma: no cover - defensive
            raise ContractError(f"cannot copy the committed library image: {exc}") from exc


def normalise_copy(destination: Path) -> None:
    """Turn the copied image into self-contained rollback-journal bytes.

    ``backup`` leaves the destination in WAL mode with its own ``-wal``/``-shm``
    sidecars (only while the copying connection is alive, but possibly still on
    disk).  Switching to ``DELETE`` mode checkpoints everything into the main
    file and removes the sidecars, so the image is exactly the bytes of one
    file and nothing else.
    """
    destination = Path(destination)
    with closing(sqlite3.connect(str(destination), isolation_level=None)) as conn:
        mode = conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
        if str(mode).lower() != "delete":  # pragma: no cover - defensive
            raise ContractError(f"could not normalise copied image journal mode: {mode}")
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = destination.with_name(destination.name + suffix)
        if sidecar.exists():
            sidecar.unlink()


def restorable_check(image: Path, expected_family: str | None = None) -> str:
    """Open the image read-only and re-check integrity, foreign keys and family."""
    image = Path(image)
    try:
        conn = sqlite3.connect(f"file:{quote(str(image))}?mode=ro", uri=True)
    except sqlite3.Error as exc:  # pragma: no cover - defensive
        raise ContractError(f"cannot read snapshot image: {exc}") from exc
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()
        if integrity is None or integrity[0] != "ok":
            raise ContractError(f"snapshot image failed integrity check: {integrity}")
        broken = conn.execute("PRAGMA foreign_key_check").fetchall()
        if broken:
            raise ContractError(
                f"snapshot image has {len(broken)} foreign key violation(s)"
            )
        family = detect_family(conn)
    except sqlite3.DatabaseError as exc:
        raise ContractError(f"snapshot image is not a usable SQLite library: {exc}") from exc
    finally:
        conn.close()
    if expected_family is not None and family != expected_family:
        raise ContractError(
            f"snapshot declares family {expected_family!r} but its image is "
            f"{family!r}"
        )
    return family


def same_file(left: Path, right: Path) -> bool:
    """Alias detection by resolved path *and* device/inode identity."""
    try:
        left_resolved = Path(left).resolve(strict=True)
    except OSError:
        left_resolved = None
    try:
        right_resolved = Path(right).resolve(strict=False)
    except OSError:
        right_resolved = None
    if left_resolved is not None and right_resolved is not None:
        if left_resolved == right_resolved:
            return True
    try:
        left_stat = Path(left).stat()
        right_stat = Path(right).stat()
    except OSError:
        return False
    return (left_stat.st_dev, left_stat.st_ino) == (right_stat.st_dev, right_stat.st_ino)


def ensure_not_alias(target: Path, library: Path) -> None:
    if same_file(target, library):
        raise ContractError(
            f"refusing to write {target}: it is the same file as the library"
        )
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(str(library) + suffix)
        if sidecar.exists() and same_file(target, sidecar):
            raise ContractError(
                f"refusing to write {target}: it is a sidecar of the library"
            )
