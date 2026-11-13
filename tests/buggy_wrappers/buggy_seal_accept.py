#!/usr/bin/env python3
"""Buggy intake wrapper #3: a sealed campaign still accepts new tickets.

The real CLI refuses every ingest against a sealed campaign (including pure
replays) and exits non-zero.  This wrapper keeps that visible failure
(return code and stderr) but then forcibly inserts the rejected tickets into
the sealed campaign itself, breaking the immutable-seal guarantee.  The
oracle predicts the sealed state to be unchanged, so the harness reports a
state divergence at the post-seal ingest.

Self-contained, standard library only; used via the diffharness ``--prefix``
option.
"""
from __future__ import annotations

import csv
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
        parsed.append((ticket_id, kind, lot_id, input_lot_id or None, grams))
    return parsed


def main(argv: list[str]) -> int:
    command = [sys.executable, "-m", "lot_lineage.intake", *argv]
    completed = subprocess.run(command, check=False)
    if completed.returncode == 0:
        return completed.returncode

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
        return completed.returncode
    database = Path(values["database"])
    if not database.exists() or database.stat().st_size == 0:
        return completed.returncode
    try:
        parsed = _parse(values["source"])
    except Exception:
        return completed.returncode

    conn = sqlite3.connect(str(database))
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        sealed = conn.execute(
            "SELECT 1 FROM intake_campaign "
            "WHERE campaign_id = ? AND status = 'sealed'",
            (values["campaign"],),
        ).fetchone()
        if sealed is None:
            return completed.returncode
        for ticket_id, kind, lot_id, input_lot_id, grams in parsed:
            try:
                conn.execute(
                    "INSERT INTO intake_ticket "
                    "(campaign_id, ticket_id, kind, lot_id, input_lot_id, grams) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (values["campaign"], ticket_id, kind, lot_id,
                     input_lot_id, grams),
                )
            except sqlite3.Error:
                # Replays hit the primary key; the new-ticket case is the one
                # that must visibly break the sealed state.
                pass
        conn.commit()
    except sqlite3.Error:
        conn.rollback()
    finally:
        conn.close()
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
