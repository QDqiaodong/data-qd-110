"""Command line entry point: ``python -m lot_lineage.recycle``."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys

from .pipeline import build


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Steady-state closed-loop recycle origin accounting (schema_version 2)"
    )
    parser.add_argument("--source", required=True, help="UTF-8 JSON input (schema_version 2)")
    parser.add_argument("--database", required=True, help="SQLite analysis library to publish")
    args = parser.parse_args(argv)

    try:
        summary = build(args.source, args.database)
    except (ValueError, OSError, sqlite3.Error) as exc:
        print(f"recycle build failed: {exc}", file=sys.stderr)
        return 2

    payload = json.dumps(summary, sort_keys=True, ensure_ascii=False)
    try:
        print(payload)
    except OSError as exc:
        # The database has already been atomically published at this point;
        # failing to write stdout must not pretend the build failed.  Use
        # stderr.write directly so a broken print hook cannot mask the notice.
        sys.stderr.write(
            f"recycle database published at {args.database} but stdout failed: {exc}\n"
        )
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
