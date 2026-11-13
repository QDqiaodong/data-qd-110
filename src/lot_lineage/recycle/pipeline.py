"""Offline single-writer build boundary for the steady-state recycle library.

The whole database is written, checked and closed inside a temporary SQLite
file in the target directory; only then is it atomically moved onto the
target.  Any failure before the replace leaves a previously published target
byte-identical and removes this run's temporary file.  The target must never
alias the input file, including through recognizable symlink paths.
"""
from __future__ import annotations

import os
import tempfile
from fractions import Fraction
from pathlib import Path

from .engine import SteadyResult, compute
from .model import ContractError, load_steady
from .store import SCHEMA_FAMILY, SCHEMA_VERSION, canonical, write_database


def _same_file(source: Path, target: Path) -> bool:
    try:
        source_resolved = source.resolve(strict=True)
        target_resolved = target.resolve(strict=False)
    except OSError:
        return False
    if source_resolved == target_resolved:
        return True
    # Compare by identity when both happen to exist; catches hard links and
    # equivalent link aliases on the same filesystem.
    try:
        source_stat = source.stat()
        target_stat = target.stat()
    except OSError:
        return False
    return (
        source_stat.st_dev == target_stat.st_dev
        and source_stat.st_ino == target_stat.st_ino
    )


def build_summary(result: SteadyResult) -> dict:
    data = result.data
    product_sinks = [sink for sink in data.sinks if sink.kind == "product"]
    loss_sinks = [sink for sink in data.sinks if sink.kind == "loss"]

    origin_terminal = {}
    for origin in data.origins:
        product = sum(result.product_total[origin].values(), Fraction(0))
        loss = sum(result.loss_total[origin].values(), Fraction(0))
        origin_terminal[origin] = {
            "feed_g": str(result.feed_total[origin]),
            "loss_g": canonical(loss),
            "product_g": canonical(product),
        }

    return {
        "schema": {
            "family": SCHEMA_FAMILY,
            "version": SCHEMA_VERSION,
            "source_sha256": data.sha256,
        },
        "totals": {
            "external_feed_g": str(sum((f.mass_g for f in data.feeds), 0)),
            "internal_transfer_g": str(sum((t.mass_g for t in data.transfers), 0)),
            "loss_g": str(sum((s.mass_g for s in data.sinks if s.kind == "loss"), 0)),
            "product_g": str(sum((s.mass_g for s in data.sinks if s.kind == "product"), 0)),
        },
        "processes": [
            {
                "process_id": node,
                "scc_id": result.node_scc[node],
                "throughput_g": str(data.mass_in[node]),
                "origin_fraction": {
                    origin: canonical(result.fractions[node][origin]) for origin in data.origins
                },
            }
            for node in data.processes
        ],
        "sccs": [
            {
                "scc_id": scc.scc_id,
                "cyclic": scc.cyclic,
                "members": list(scc.members),
            }
            for scc in result.sccs
        ],
        "condensation_order": list(result.condensation_order),
        "feeds": [
            {
                "feed_id": feed.feed_id,
                "mass_g": feed.mass_g,
                "origin_id": feed.origin_id,
                "pointer": feed.pointer,
                "to_process": feed.to_process,
            }
            for feed in data.feeds
        ],
        "transfers": [
            {
                "transfer_id": transfer.transfer_id,
                "from_process": transfer.from_process,
                "mass_g": transfer.mass_g,
                "origin_mass_g": {
                    origin: canonical(result.transfer_origins[transfer.transfer_id][origin])
                    for origin in data.origins
                },
                "pointer": transfer.pointer,
                "to_process": transfer.to_process,
            }
            for transfer in data.transfers
        ],
        "sinks": [
            {
                "sink_id": sink.sink_id,
                "from_process": sink.from_process,
                "kind": sink.kind,
                "mass_g": sink.mass_g,
                "origin_mass_g": {
                    origin: canonical(result.sink_origins[sink.sink_id][origin])
                    for origin in data.origins
                },
                "pointer": sink.pointer,
            }
            for sink in data.sinks
        ],
        "product_sink_count": len(product_sinks),
        "loss_sink_count": len(loss_sinks),
        "origin_terminal": origin_terminal,
    }


def build(source, target) -> dict:
    """Validate, solve and publish one steady-state recycle database.

    Mirrors :func:`lot_lineage.pipeline.build` in spirit but keeps the
    schema_version 2 data fully isolated from the acyclic lot/recipe model.
    """
    source_path = Path(source)
    target_path = Path(target)
    if _same_file(source_path, target_path):
        raise ContractError("database target must not be the same file as the JSON source")

    # Raw bytes are captured exactly once; parsing and hashing use that copy.
    steady = load_steady(source_path)
    result = compute(steady)
    summary = build_summary(result)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(
        prefix=".recycle-", suffix=".tmp", dir=str(target_path.parent)
    )
    os.close(fd)
    try:
        write_database(temp_path, result, steady.raw_bytes)
        os.replace(temp_path, target_path)
    except BaseException:
        # Normal and abnormal failure paths: never leave the temp file behind
        # and never touch an already published target.
        Path(temp_path).unlink(missing_ok=True)
        raise
    return summary
