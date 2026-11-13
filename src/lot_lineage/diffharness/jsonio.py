"""Deterministic JSON (de)serialisation for scenario and counterexample files.

Every byte written by the differential harness is reproducible: keys are
sorted, no whitespace is added beyond a fixed two-space indent and a single
trailing newline.  Two runs with the same seed and budget therefore produce
byte-identical artifacts on any machine.
"""
from __future__ import annotations

import json
from pathlib import Path

_DUMP_KWARGS = {
    "ensure_ascii": False,
    "sort_keys": True,
    "indent": 2,
    "separators": (",", ": "),
}


def dumps(payload) -> str:
    return json.dumps(payload, **_DUMP_KWARGS) + "\n"


def dump_bytes(payload) -> bytes:
    return dumps(payload).encode("utf-8")


def write_json(path, payload) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(dump_bytes(payload))


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))
