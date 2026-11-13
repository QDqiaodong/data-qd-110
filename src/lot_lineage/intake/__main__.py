"""Command line entry point: ``python -m lot_lineage.intake``.

Subcommands::

    ingest --database DB --campaign ID --source CSV
    seal   --database DB --campaign ID
    export --database DB --campaign ID --output JSON
    status --database DB --campaign ID

Each command prints a deterministic JSON receipt on stdout.  Failures exit
non-zero and explain the reason on stderr; a transaction that failed to commit
is never reported as committed.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

from .store import export, ingest, seal, status

EXIT_ERROR = 2
EXIT_STDOUT = 3


def _cli_hook(name: str) -> None:
    """Controlled synchronisation point for tests: print, then wait for a line.

    Enabled only by the internal ``--hook`` flag.  It never sleeps or guesses
    timings; the parent test decides exactly when this process may continue,
    which makes write-transaction interleaving deterministic.
    """
    sys.stdout.write(f"HOOK:{name}\n")
    sys.stdout.flush()
    sys.stdin.readline()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lot_lineage.intake",
        description="Offline weighing-ticket intake, sealing and canonical export",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    ingest_parser = sub.add_parser("ingest", help="append one CSV ticket batch")
    ingest_parser.add_argument("--database", required=True)
    ingest_parser.add_argument("--campaign", required=True)
    ingest_parser.add_argument("--source", required=True)
    ingest_parser.add_argument("--busy-timeout", type=float, default=5.0,
                               help="seconds to wait for a concurrent writer (default 5)")
    ingest_parser.add_argument("--hook", action="store_true", help=argparse.SUPPRESS)

    seal_parser = sub.add_parser("seal", help="validate all tickets and freeze the campaign")
    seal_parser.add_argument("--database", required=True)
    seal_parser.add_argument("--campaign", required=True)
    seal_parser.add_argument("--busy-timeout", type=float, default=5.0)
    seal_parser.add_argument("--hook", action="store_true", help=argparse.SUPPRESS)

    export_parser = sub.add_parser("export", help="write the sealed canonical JSON")
    export_parser.add_argument("--database", required=True)
    export_parser.add_argument("--campaign", required=True)
    export_parser.add_argument("--output", required=True)
    export_parser.add_argument("--busy-timeout", type=float, default=5.0)

    status_parser = sub.add_parser("status", help="report campaign status (read-only)")
    status_parser.add_argument("--database", required=True)
    status_parser.add_argument("--campaign", required=True)
    status_parser.add_argument("--busy-timeout", type=float, default=5.0)
    return parser


def _run(args) -> dict:
    hook = _cli_hook if getattr(args, "hook", False) else None
    if args.command == "ingest":
        return ingest(
            args.database, args.campaign, args.source,
            busy_timeout=args.busy_timeout, _sync_hook=hook,
        )
    if args.command == "seal":
        return seal(
            args.database, args.campaign,
            busy_timeout=args.busy_timeout, _sync_hook=hook,
        )
    if args.command == "export":
        return export(
            args.database, args.campaign, args.output, busy_timeout=args.busy_timeout
        )
    return status(args.database, args.campaign, busy_timeout=args.busy_timeout)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        receipt = _run(args)
    except (ValueError, OSError, sqlite3.Error) as exc:
        print(f"intake {args.command} failed: {exc}", file=sys.stderr)
        return EXIT_ERROR

    payload = json.dumps(receipt, sort_keys=True, ensure_ascii=False)
    try:
        print(payload)
    except OSError as exc:
        # The transaction (ingest/seal) or atomic replacement (export) has
        # already happened for this command; do not pretend it rolled back.
        sys.stderr.write(
            f"intake {args.command} completed but stdout failed: {exc}\n"
        )
        return EXIT_STDOUT
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
