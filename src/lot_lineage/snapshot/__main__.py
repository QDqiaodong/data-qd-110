"""Command line entry point: ``python -m lot_lineage.snapshot``.

Subcommands::

    capture --database DB --store DIR [--family F]
    list    --store DIR
    verify  --store DIR [--snapshot ID] [--family F]
    restore --store DIR --snapshot ID --output DB [--family F] [--library DB]
    forget  --store DIR --snapshot ID
    gc      --store DIR

Each command prints a deterministic JSON receipt on stdout.  Failures explain
the reason on stderr and exit non-zero.  A command that already published its
result but could not write stdout exits 3 instead of pretending it failed.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys

from .model import LIBRARY_FAMILIES
from .store import (
    capture,
    export_manifest,
    forget,
    gc,
    list_snapshots,
    restore,
    verify,
)

EXIT_ERROR = 2
EXIT_STDOUT = 3


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lot_lineage.snapshot",
        description=(
            "Offline content-addressed snapshots of the batch, recycle and "
            "weighing-ticket libraries"
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    capture_parser = sub.add_parser(
        "capture", help="publish an immutable snapshot of a live library"
    )
    capture_parser.add_argument("--database", required=True,
                                help="source library (only read, never modified)")
    capture_parser.add_argument("--store", required=True)
    capture_parser.add_argument("--family", choices=LIBRARY_FAMILIES, default=None,
                                help="require this library family")
    capture_parser.add_argument("--hook", action="store_true", help=argparse.SUPPRESS)

    list_parser = sub.add_parser("list", help="list published snapshots")
    list_parser.add_argument("--store", required=True)

    verify_parser = sub.add_parser(
        "verify", help="check chunks, whole-image digest, integrity and family"
    )
    verify_parser.add_argument("--store", required=True)
    verify_parser.add_argument("--snapshot", default=None)
    verify_parser.add_argument("--family", choices=LIBRARY_FAMILIES, default=None)

    restore_parser = sub.add_parser(
        "restore", help="publish a snapshot image as a new library file"
    )
    restore_parser.add_argument("--store", required=True)
    restore_parser.add_argument("--snapshot", required=True)
    restore_parser.add_argument("--output", required=True)
    restore_parser.add_argument("--family", choices=LIBRARY_FAMILIES, default=None)
    restore_parser.add_argument("--library", default=None,
                                help="refuse to overwrite this library or its sidecars")
    restore_parser.add_argument("--hook", action="store_true", help=argparse.SUPPRESS)

    forget_parser = sub.add_parser("forget", help="delete only the manifest")
    forget_parser.add_argument("--store", required=True)
    forget_parser.add_argument("--snapshot", required=True)

    gc_parser = sub.add_parser(
        "gc", help="delete unreferenced objects after validating every reference"
    )
    gc_parser.add_argument("--store", required=True)

    manifest_parser = sub.add_parser(
        "manifest", help="print one manifest's canonical bytes and snapshot ID"
    )
    manifest_parser.add_argument("--store", required=True)
    manifest_parser.add_argument("--snapshot", required=True)
    return parser


def _cli_hook(name: str) -> None:
    """Controlled synchronisation point for tests: print, then wait for a line."""
    sys.stdout.write(f"HOOK:{name}\n")
    sys.stdout.flush()
    sys.stdin.readline()


def _run(args) -> dict:
    hook = _cli_hook if getattr(args, "hook", False) else None
    if args.command == "capture":
        return capture(
            args.database, args.store,
            expected_family=args.family, _sync_hook=hook,
        )
    if args.command == "list":
        return list_snapshots(args.store)
    if args.command == "verify":
        return verify(args.store, args.snapshot, expected_family=args.family)
    if args.command == "restore":
        return restore(
            args.store, args.snapshot, args.output,
            expected_family=args.family, library=args.library, _sync_hook=hook,
        )
    if args.command == "forget":
        return forget(args.store, args.snapshot)
    if args.command == "gc":
        # A partial deletion caused by an I/O error is reported, never hidden:
        # the receipt still lists what was and was not removed.
        return gc(args.store)
    return export_manifest(args.store, args.snapshot)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        receipt = _run(args)
    except (ValueError, OSError, sqlite3.Error) as exc:
        print(f"snapshot {args.command} failed: {exc}", file=sys.stderr)
        return EXIT_ERROR

    payload = json.dumps(receipt, sort_keys=True, ensure_ascii=False)
    try:
        print(payload)
    except OSError as exc:
        sys.stderr.write(
            f"snapshot {args.command} completed but stdout failed: {exc}\n"
        )
        return EXIT_STDOUT
    if args.command == "gc" and receipt["errors"]:
        sys.stderr.write(
            f"snapshot gc deleted {receipt['deleted_objects']} object(s) but "
            f"{len(receipt['errors'])} I/O error(s) left unreferenced objects "
            "in place; see the errors field\n"
        )
        return EXIT_ERROR
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
