"""Parsing and structural validation of schema_version 2 steady-state JSON.

Every structural, reference or numeric error is reported together with the
JSON Pointer of the offending record (for example ``/feeds/0``) and, when
available, the business key of that record.  Only exact field sets are
accepted, masses are strict integers (booleans rejected), and identifiers
are case-sensitive strings without surrounding whitespace.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

MASS_LIMIT = 10**12
TOP_LEVEL = ("schema_version", "processes", "feeds", "transfers", "sinks")


class ContractError(ValueError):
    """Raised when the steady-state input violates the schema_version 2 contract."""


def _is_identifier(value: Any) -> bool:
    return isinstance(value, str) and value != "" and value == value.strip()


def _mass(value: Any, pointer: str) -> int:
    # bool is a subclass of int; reject it explicitly.
    if type(value) is not int:
        raise ContractError(f"{pointer}: mass_g must be an integer number of grams")
    if not 1 <= value <= MASS_LIMIT:
        raise ContractError(f"{pointer}: mass_g must be between 1 and 10^12 grams")
    return value


def _exact_fields(record: Any, expected: set[str], pointer: str, key_name: str | None) -> None:
    if not isinstance(record, dict):
        raise ContractError(f"{pointer}: record must be an object")
    missing = expected - set(record)
    extra = set(record) - expected
    if missing or extra:
        detail = []
        if missing:
            detail.append("missing " + ",".join(sorted(missing)))
        if extra:
            detail.append("unexpected " + ",".join(sorted(extra)))
        raise ContractError(f"{pointer}: invalid fields ({'; '.join(detail)})")
    if key_name is not None and not _is_identifier(record.get(key_name)):
        raise ContractError(
            f"{pointer}: {key_name} must be a non-empty string without surrounding whitespace"
        )


@dataclass(frozen=True)
class FeedRecord:
    feed_id: str
    origin_id: str
    to_process: str
    mass_g: int
    pointer: str


@dataclass(frozen=True)
class TransferRecord:
    transfer_id: str
    from_process: str
    to_process: str
    mass_g: int
    pointer: str


@dataclass(frozen=True)
class SinkRecord:
    sink_id: str
    from_process: str
    kind: str
    mass_g: int
    pointer: str


@dataclass
class SteadyInput:
    raw_bytes: bytes
    sha256: str
    processes: list[str]
    feeds: list[FeedRecord]
    transfers: list[TransferRecord]
    sinks: list[SinkRecord]
    origins: list[str]
    pointers: dict[tuple[str, str], str] = field(default_factory=dict)
    mass_in: dict[str, int] = field(default_factory=dict)
    mass_out: dict[str, int] = field(default_factory=dict)


def _read_raw(source: str | Path) -> bytes:
    path = Path(source)
    try:
        return path.read_bytes()
    except OSError:
        raise
    except MemoryError as exc:  # pragma: no cover - defensive
        raise ContractError(f"cannot read source {path}: {exc}") from exc


def parse_steady(raw_bytes: bytes, *, source_label: str = "<input>") -> SteadyInput:
    """Validate already-captured raw bytes; never re-reads the source file."""
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ContractError(f"{source_label}: input is not valid UTF-8: {exc}") from exc
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ContractError(f"{source_label}: invalid JSON at line {exc.lineno} col {exc.colno}: {exc.msg}") from exc
    if not isinstance(data, dict) or set(data) != set(TOP_LEVEL):
        raise ContractError(
            "top-level object must contain exactly " + ", ".join(TOP_LEVEL)
        )
    if type(data["schema_version"]) is not int or isinstance(data["schema_version"], bool):
        raise ContractError("/schema_version: must be an integer")
    if data["schema_version"] != 2:
        raise ContractError(f"/schema_version: only integer 2 is supported, got {data['schema_version']!r}")

    for name in ("processes", "feeds", "transfers", "sinks"):
        if not isinstance(data[name], list):
            raise ContractError(f"/{name}: must be an array")

    # --- processes ---
    processes: list[str] = []
    process_set: set[str] = set()
    for index, record in enumerate(data["processes"]):
        pointer = f"/processes/{index}"
        _exact_fields(record, {"process_id"}, pointer, "process_id")
        pid = record["process_id"]
        if pid in process_set:
            raise ContractError(f"{pointer}: duplicate process_id {pid!r}")
        process_set.add(pid)
        processes.append(pid)
    if not processes:
        raise ContractError("/processes: at least one process is required")

    pointers: dict[tuple[str, str], str] = {}

    def _check_process_ref(value: Any, pointer: str, field_name: str) -> str:
        if not _is_identifier(value):
            raise ContractError(
                f"{pointer}: {field_name} must be a non-empty string without surrounding whitespace"
            )
        if value not in process_set:
            raise ContractError(f"{pointer}: unknown process reference {value!r}")
        return value

    # --- feeds ---
    feeds: list[FeedRecord] = []
    feed_ids: set[str] = set()
    origins: set[str] = set()
    origin_pointer: dict[str, str] = {}
    for index, record in enumerate(data["feeds"]):
        pointer = f"/feeds/{index}"
        _exact_fields(record, {"feed_id", "origin_id", "to_process", "mass_g"}, pointer, "feed_id")
        fid = record["feed_id"]
        if fid in feed_ids:
            raise ContractError(f"{pointer}: duplicate feed_id {fid!r}")
        feed_ids.add(fid)
        if not _is_identifier(record["origin_id"]):
            raise ContractError(
                f"{pointer}: origin_id must be a non-empty string without surrounding whitespace"
            )
        origin_id = record["origin_id"]
        origin_pointer.setdefault(origin_id, pointer)
        to_process = _check_process_ref(record["to_process"], pointer, "to_process")
        mass = _mass(record["mass_g"], pointer)
        feeds.append(FeedRecord(fid, origin_id, to_process, mass, pointer))
        origins.add(origin_id)
        pointers[("feed", fid)] = pointer
    if not feeds:
        raise ContractError("/feeds: at least one external feed is required")

    # --- transfers (may be empty) ---
    transfers: list[TransferRecord] = []
    transfer_ids: set[str] = set()
    for index, record in enumerate(data["transfers"]):
        pointer = f"/transfers/{index}"
        _exact_fields(
            record, {"transfer_id", "from_process", "to_process", "mass_g"}, pointer, "transfer_id"
        )
        tid = record["transfer_id"]
        if tid in transfer_ids:
            raise ContractError(f"{pointer}: duplicate transfer_id {tid!r}")
        transfer_ids.add(tid)
        from_process = _check_process_ref(record["from_process"], pointer, "from_process")
        to_process = _check_process_ref(record["to_process"], pointer, "to_process")
        mass = _mass(record["mass_g"], pointer)
        transfers.append(TransferRecord(tid, from_process, to_process, mass, pointer))
        pointers[("transfer", tid)] = pointer

    # --- sinks ---
    sinks: list[SinkRecord] = []
    sink_ids: set[str] = set()
    for index, record in enumerate(data["sinks"]):
        pointer = f"/sinks/{index}"
        _exact_fields(
            record, {"sink_id", "from_process", "kind", "mass_g"}, pointer, "sink_id"
        )
        sid = record["sink_id"]
        if sid in sink_ids:
            raise ContractError(f"{pointer}: duplicate sink_id {sid!r}")
        sink_ids.add(sid)
        from_process = _check_process_ref(record["from_process"], pointer, "from_process")
        if record["kind"] not in ("product", "loss"):
            raise ContractError(f"{pointer}: kind must be 'product' or 'loss'")
        mass = _mass(record["mass_g"], pointer)
        sinks.append(SinkRecord(sid, from_process, record["kind"], mass, pointer))
        pointers[("sink", sid)] = pointer
    if not sinks:
        raise ContractError("/sinks: at least one sink is required")

    for origin_id, pointer in origin_pointer.items():
        pointers[("origin", origin_id)] = pointer

    result = SteadyInput(
        raw_bytes=raw_bytes,
        sha256="",
        processes=processes,
        feeds=feeds,
        transfers=transfers,
        sinks=sinks,
        origins=sorted(origins),
        pointers=pointers,
    )
    _check_balance_and_reachability(result)
    return result


def _check_balance_and_reachability(data: SteadyInput) -> None:
    mass_in = {pid: 0 for pid in data.processes}
    mass_out = {pid: 0 for pid in data.processes}
    forward: dict[str, list[str]] = {pid: [] for pid in data.processes}
    reverse: dict[str, list[str]] = {pid: [] for pid in data.processes}
    for feed in data.feeds:
        mass_in[feed.to_process] += feed.mass_g
    for transfer in data.transfers:
        mass_in[transfer.to_process] += transfer.mass_g
        mass_out[transfer.from_process] += transfer.mass_g
        forward[transfer.from_process].append(transfer.to_process)
        reverse[transfer.to_process].append(transfer.from_process)
    for sink in data.sinks:
        mass_out[sink.from_process] += sink.mass_g

    # Per-node balance M_v > 0 and inflow == outflow exactly.
    for pid in data.processes:
        if mass_in[pid] <= 0:
            raise ContractError(
                f"process {pid!r}: total inflow must be greater than zero (no feed or incoming transfer)"
            )
        if mass_in[pid] != mass_out[pid]:
            diff = mass_in[pid] - mass_out[pid]
            raise ContractError(
                f"process {pid!r}: inflow {mass_in[pid]} g != outflow {mass_out[pid]} g "
                f"(difference {diff:+d} g)"
            )
    data.mass_in = mass_in
    data.mass_out = mass_out

    feed_reachable = {pid for pid in data.processes if mass_in_from_feed(pid, data)}
    _bfs(feed_reachable, forward)
    can_reach_sink = {sink.from_process for sink in data.sinks}
    _bfs(can_reach_sink, reverse)

    unreachable_in = sorted(pid for pid in data.processes if pid not in feed_reachable)
    unreachable_out = sorted(pid for pid in data.processes if pid not in can_reach_sink)
    problems: list[str] = []
    if unreachable_in:
        problems.append(
            "cannot be reached from any external feed along transfers: " + ", ".join(unreachable_in)
        )
    if unreachable_out:
        problems.append(
            "cannot reach any sink along transfers: " + ", ".join(unreachable_out)
        )
    if problems:
        raise ContractError(
            "undetermined source composition (internal loop with no external origin and/or no exit); "
            + "; ".join(problems)
        )


def mass_in_from_feed(pid: str, data: SteadyInput) -> bool:
    return any(feed.to_process == pid for feed in data.feeds)


def _bfs(seeds: set[str], adjacency: dict[str, list[str]]) -> None:
    queue = sorted(seeds)
    while queue:
        current = queue.pop(0)
        for nxt in adjacency[current]:
            if nxt not in seeds:
                seeds.add(nxt)
                queue.append(nxt)


def load_steady(source: str | Path) -> SteadyInput:
    """Capture the raw bytes exactly once, then parse and hash the same bytes."""
    import hashlib

    raw = _read_raw(source)
    parsed = parse_steady(raw, source_label=str(source))
    parsed.sha256 = hashlib.sha256(raw).hexdigest()
    return parsed
