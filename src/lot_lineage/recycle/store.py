"""Self-contained SQLite analysis library for steady-state recycle runs.

Schema family ``recycle_steady`` at version 2.  All exact quantities are
stored as canonical decimal text (``numerator/denominator`` with a positive
denominator); SQLite REAL is never used for formal values.  Per-record input
masses fit SQLite's 64-bit signed integers by contract (<= 10^12), while
cumulated totals are stored as unbounded decimal text so they can never be
truncated by integer overflow.

Zero-valued origin attributions are stored explicitly as ``'0/1'`` rows
rather than omitted, so every (flow, origin) pair is directly queryable.
"""
from __future__ import annotations

import sqlite3
from contextlib import closing
from fractions import Fraction

from .engine import SteadyResult

SCHEMA_FAMILY = "recycle_steady"
SCHEMA_VERSION = 2


def canonical(value: Fraction) -> str:
    """Canonical reduced fraction text, denominator always positive."""
    return f"{value.numerator}/{value.denominator}"


def write_database(database_path: str, result: SteadyResult, raw_bytes: bytes) -> None:
    """Create and fully populate the SQLite file at ``database_path``.

    The caller owns the temp-file / atomic-replace boundary; this function
    only writes into the path it is given and closes the connection before
    returning.
    """
    data = result.data
    with closing(sqlite3.connect(database_path)) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(
            """
            CREATE TABLE recycle_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE recycle_source (
                source_id INTEGER PRIMARY KEY CHECK (source_id = 1),
                schema_family TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                source_sha256 TEXT NOT NULL,
                raw_json BLOB NOT NULL
            );
            CREATE TABLE recycle_record_pointer (
                record_kind TEXT NOT NULL CHECK (record_kind IN ('process','feed','transfer','sink','origin')),
                business_key TEXT NOT NULL,
                json_pointer TEXT NOT NULL,
                PRIMARY KEY (record_kind, business_key)
            );
            CREATE TABLE recycle_process (
                process_id TEXT PRIMARY KEY,
                scc_id INTEGER NOT NULL REFERENCES recycle_scc(scc_id),
                inflow_g INTEGER NOT NULL,
                outflow_g INTEGER NOT NULL
            );
            CREATE TABLE recycle_scc (
                scc_id INTEGER PRIMARY KEY,
                cyclic INTEGER NOT NULL CHECK (cyclic IN (0, 1)),
                member_count INTEGER NOT NULL
            );
            CREATE TABLE recycle_scc_member (
                scc_id INTEGER NOT NULL REFERENCES recycle_scc(scc_id),
                process_id TEXT NOT NULL REFERENCES recycle_process(process_id),
                member_order INTEGER NOT NULL,
                PRIMARY KEY (scc_id, process_id)
            );
            CREATE TABLE recycle_feed (
                feed_id TEXT PRIMARY KEY,
                origin_id TEXT NOT NULL,
                to_process TEXT NOT NULL REFERENCES recycle_process(process_id),
                mass_g INTEGER NOT NULL,
                json_pointer TEXT NOT NULL
            );
            CREATE TABLE recycle_transfer (
                transfer_id TEXT PRIMARY KEY,
                from_process TEXT NOT NULL REFERENCES recycle_process(process_id),
                to_process TEXT NOT NULL REFERENCES recycle_process(process_id),
                mass_g INTEGER NOT NULL,
                json_pointer TEXT NOT NULL
            );
            CREATE TABLE recycle_sink (
                sink_id TEXT PRIMARY KEY,
                from_process TEXT NOT NULL REFERENCES recycle_process(process_id),
                kind TEXT NOT NULL CHECK (kind IN ('product','loss')),
                mass_g INTEGER NOT NULL,
                json_pointer TEXT NOT NULL
            );
            CREATE TABLE recycle_process_origin (
                process_id TEXT NOT NULL REFERENCES recycle_process(process_id),
                origin_id TEXT NOT NULL,
                numerator TEXT NOT NULL,
                denominator TEXT NOT NULL,
                PRIMARY KEY (process_id, origin_id)
            );
            CREATE TABLE recycle_transfer_origin (
                transfer_id TEXT NOT NULL REFERENCES recycle_transfer(transfer_id),
                origin_id TEXT NOT NULL,
                numerator TEXT NOT NULL,
                denominator TEXT NOT NULL,
                PRIMARY KEY (transfer_id, origin_id)
            );
            CREATE TABLE recycle_sink_origin (
                sink_id TEXT NOT NULL REFERENCES recycle_sink(sink_id),
                origin_id TEXT NOT NULL,
                numerator TEXT NOT NULL,
                denominator TEXT NOT NULL,
                PRIMARY KEY (sink_id, origin_id)
            );
            CREATE TABLE recycle_origin_terminal (
                origin_id TEXT NOT NULL,
                feed_total_text TEXT NOT NULL,
                product_total_num TEXT NOT NULL,
                product_total_den TEXT NOT NULL,
                loss_total_num TEXT NOT NULL,
                loss_total_den TEXT NOT NULL,
                PRIMARY KEY (origin_id)
            );
            CREATE TABLE recycle_global_total (
                metric TEXT PRIMARY KEY,
                value_text TEXT NOT NULL
            );
            """
        )

        db.execute(
            "INSERT INTO recycle_source VALUES (1, ?, ?, ?, ?)",
            (SCHEMA_FAMILY, SCHEMA_VERSION, data.sha256, sqlite3.Binary(raw_bytes)),
        )
        db.executemany(
            "INSERT INTO recycle_meta VALUES (?, ?)",
            [
                ("schema_family", SCHEMA_FAMILY),
                ("schema_version", str(SCHEMA_VERSION)),
                ("source_sha256", data.sha256),
                ("origin_count", str(len(data.origins))),
                ("process_count", str(len(data.processes))),
                ("feed_count", str(len(data.feeds))),
                ("transfer_count", str(len(data.transfers))),
                ("sink_count", str(len(data.sinks))),
                (
                    "condensation_order",
                    ",".join(str(scc_id) for scc_id in result.condensation_order),
                ),
            ],
        )

        pointer_rows = [("process", pid, f"/processes/{idx}") for idx, pid in enumerate(data.processes)]
        for feed in data.feeds:
            pointer_rows.append(("feed", feed.feed_id, feed.pointer))
        for transfer in data.transfers:
            pointer_rows.append(("transfer", transfer.transfer_id, transfer.pointer))
        for sink in data.sinks:
            pointer_rows.append(("sink", sink.sink_id, sink.pointer))
        for origin in data.origins:
            pointer_rows.append(("origin", origin, data.pointers[("origin", origin)]))
        db.executemany("INSERT INTO recycle_record_pointer VALUES (?, ?, ?)", pointer_rows)

        for scc in result.sccs:
            db.execute(
                "INSERT INTO recycle_scc VALUES (?, ?, ?)",
                (scc.scc_id, 1 if scc.cyclic else 0, len(scc.members)),
            )
        for order, scc_id in enumerate(result.condensation_order):
            db.execute(
                "INSERT INTO recycle_meta VALUES (?, ?)",
                (f"condensation_step_{order}", str(scc_id)),
            )

        for node in data.processes:
            db.execute(
                "INSERT INTO recycle_process VALUES (?, ?, ?, ?)",
                (node, result.node_scc[node], data.mass_in[node], data.mass_out[node]),
            )
        for scc in result.sccs:
            for order, member in enumerate(scc.members):
                db.execute(
                    "INSERT INTO recycle_scc_member VALUES (?, ?, ?)",
                    (scc.scc_id, member, order),
                )

        db.executemany(
            "INSERT INTO recycle_feed VALUES (?, ?, ?, ?, ?)",
            [(f.feed_id, f.origin_id, f.to_process, f.mass_g, f.pointer) for f in data.feeds],
        )
        db.executemany(
            "INSERT INTO recycle_transfer VALUES (?, ?, ?, ?, ?)",
            [
                (t.transfer_id, t.from_process, t.to_process, t.mass_g, t.pointer)
                for t in data.transfers
            ],
        )
        db.executemany(
            "INSERT INTO recycle_sink VALUES (?, ?, ?, ?, ?)",
            [(s.sink_id, s.from_process, s.kind, s.mass_g, s.pointer) for s in data.sinks],
        )

        process_rows = []
        for node in data.processes:
            for origin in data.origins:
                value = result.fractions[node][origin]
                process_rows.append((node, origin, str(value.numerator), str(value.denominator)))
        db.executemany("INSERT INTO recycle_process_origin VALUES (?, ?, ?, ?)", process_rows)

        transfer_rows = []
        for transfer in data.transfers:
            for origin in data.origins:
                value = result.transfer_origins[transfer.transfer_id][origin]
                transfer_rows.append(
                    (transfer.transfer_id, origin, str(value.numerator), str(value.denominator))
                )
        db.executemany("INSERT INTO recycle_transfer_origin VALUES (?, ?, ?, ?)", transfer_rows)

        sink_rows = []
        for sink in data.sinks:
            for origin in data.origins:
                value = result.sink_origins[sink.sink_id][origin]
                sink_rows.append((sink.sink_id, origin, str(value.numerator), str(value.denominator)))
        db.executemany("INSERT INTO recycle_sink_origin VALUES (?, ?, ?, ?)", sink_rows)

        for origin in data.origins:
            product = sum(result.product_total[origin].values(), Fraction(0))
            loss = sum(result.loss_total[origin].values(), Fraction(0))
            db.execute(
                "INSERT INTO recycle_origin_terminal VALUES (?, ?, ?, ?, ?, ?)",
                (
                    origin,
                    str(result.feed_total[origin]),
                    str(product.numerator),
                    str(product.denominator),
                    str(loss.numerator),
                    str(loss.denominator),
                ),
            )

        totals = {
            "external_feed_g": sum((f.mass_g for f in data.feeds), 0),
            "internal_transfer_g": sum((t.mass_g for t in data.transfers), 0),
            "product_g": sum((s.mass_g for s in data.sinks if s.kind == "product"), 0),
            "loss_g": sum((s.mass_g for s in data.sinks if s.kind == "loss"), 0),
            "node_throughput_g": sum(data.mass_in.values(), 0),
        }
        db.executemany(
            "INSERT INTO recycle_global_total VALUES (?, ?)",
            sorted((metric, str(value)) for metric, value in totals.items()),
        )
        db.commit()
        if db.execute("PRAGMA foreign_key_check").fetchall():  # pragma: no cover - defensive
            raise sqlite3.IntegrityError("foreign key check failed in recycle database")
