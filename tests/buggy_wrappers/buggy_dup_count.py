#!/usr/bin/env python3
"""Buggy intake wrapper #1: equivalent replays are counted twice.

The real CLI is invoked first and its exit code/stdout/stderr are preserved.
When an *ingest* succeeds **and its receipt reports replay occurrences**, the
wrapper independently inspects the supplied CSV and overwrites each replayed
ticket's stored grams with a different (doubled, capped) value, so the
replay changes the account instead of being a no-op.  A genuinely new
ingest (zero replays) is left untouched, so the divergence appears exactly
at the equivalent-replay operation.

Self-contained, standard library only; used via the diffharness ``--prefix``
option.  Production code is never modified.
"""
from __future__ import annotations

import csv
import json
import sqlite3
import subprocess
import sys
from pathlib import Path


def _parse(path: str):
    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    header, data = rows[0], rows[1:]
    assert header == [
        "ticket_id", "kind", "lot_id", "input_lot_id", "mass", "unit"
    ]
    parsed = []
    for row in data:
        ticket_id, kind, lot_id, input_lot_id, mass, unit = row
        factor = 1000 if unit == "kg" else 1
        whole, _, fraction = mass.partition(".")
        denominator = 10 ** len(fraction)
        numerator = (int(whole) * denominator + (int(fraction) if fraction else 0)) * factor
        grams = numerator // denominator
        parsed.append((ticket_id, grams))
    return parsed


def main(argv: list[str]) -> int:
    proc = subprocess.Popen(
        [sys.executable, "-m", "lot_lineage.intake", *argv],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = proc.communicate()
    sys.stdout.buffer.write(out)
    sys.stderr.buffer.write(err)
    if proc.returncode != 0:
        return proc.returncode

    values = {name: None for name in ("database", "campaign", "source")}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token in ("--database", "--campaign", "--source"):
            values[token[2:]] = argv[index + 1]
            index += 2
        else:
            index += 1
    if values["source"] is None:
        return proc.returncode
    try:
        receipt = json.loads(out.decode("utf-8"))
    except ValueError:
        return proc.returncode
    # Only replays trigger the bug; brand-new ingests behave correctly.
    if receipt.get("replayed_occurrences", 0) <= 0:
        return proc.returncode

    database = Path(values["database"])
    if not database.exists() or database.stat().st_size == 0:
        return proc.returncode
    try:
        parsed = _parse(values["source"])
    except Exception:
        return proc.returncode

    conn = sqlite3.connect(str(database))
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        for ticket_id, grams in parsed:
            stored = conn.execute(
                "SELECT grams FROM intake_ticket "
                "WHERE campaign_id = ? AND ticket_id = ?",
                (values["campaign"], ticket_id),
            ).fetchone()
            if stored is None:
                continue
            doubled = min(grams * 2, 10**12)
            if doubled == grams:
                doubled = max(1, grams // 2)
            conn.execute(
                "UPDATE intake_ticket SET grams = ? "
                "WHERE campaign_id = ? AND ticket_id = ?",
                (doubled, values["campaign"], ticket_id),
            )
        conn.commit()
    except sqlite3.Error:
        conn.rollback()
    finally:
        conn.close()
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
