"""Deterministic scenario generation for the differential harness.

A scenario is a self-contained description of several real CLI operations
spread over **two independent campaigns**.  The generator is fully
deterministic: the same ``seed`` and ``steps`` produce byte-identical
scenario JSON (including file contents, operation order and the embedded
coverage tags).  No real database or production module is involved.

The generated plan deliberately exercises every intake contract branch:

* equivalent replay (``500 g`` vs ``0.500 kg``; also identical bytes);
* same-ticket conflict both inside one file and across files;
* missing-ticket supplementation before a later successful seal;
* every write after sealing (ingest of new, equivalent-replay and malformed
  files, and a repeated seal);
* structurally malformed CSV files that must be rejected before any write;
* status/export on an unknown campaign and export while open;
* cross-campaign ticket/lot ID reuse that must never mix.
"""
from __future__ import annotations

from . import FORMAT_SCENARIO

HEADER = "ticket_id,kind,lot_id,input_lot_id,mass,unit"
CAMPAIGN_A = "cmp-a"
CAMPAIGN_B = "cmp-b"


class _Rng:
    """Small deterministic LCG so generation never depends on ``random``'s
    global seeding or version-specific internals."""

    __slots__ = ("_state",)

    def __init__(self, seed: int) -> None:
        self._state = int(seed) & 0xFFFFFFFFFFFFFFFF

    def next_u32(self) -> int:
        # Numerical Recipes constants, 64-bit state.
        self._state = (self._state * 6364136223846793005 + 1442695) & 0xFFFFFFFFFFFFFFFF
        return (self._state >> 32) & 0xFFFFFFFF

    def choice(self, options):
        return options[self.next_u32() % len(options)]


def csv_bytes(rows: list[list[str]]) -> bytes:
    """Build a canonical CSV file: exact ordered header, LF line endings."""
    lines = [HEADER]
    lines.extend(",".join(cell for cell in row) for row in rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


def _ticket(ticket_id, kind, lot_id, input_lot_id, mass, unit) -> list[str]:
    return [ticket_id, kind, lot_id, input_lot_id, mass, unit]


# ---------------------------------------------------------------------------
# File catalogue.  Every file is self-contained CSV bytes.
# ---------------------------------------------------------------------------

def _file_catalogue(rng: _Rng) -> dict[str, dict]:
    # The equivalent replay of TA02 may arrive with a different-but-equal
    # unit spelling; the choice is driven by the deterministic RNG.
    ta02_replay_mass, ta02_replay_unit = rng.choice(
        [("500", "g"), ("0.500", "kg")]
    )
    files: dict[str, dict] = {
        # --- campaign A: partial arrival, supplementation, then seal ------
        "f_a_ingest_1": {
            "campaign": CAMPAIGN_A,
            "text": csv_bytes([
                _ticket("TA01", "input", "X", "A", "600", "g"),
                _ticket("TA02", "input", "X", "B", "0.400", "kg"),
                _ticket("TA03", "loss", "X", "", "100", "g"),
            ]),
        },
        "f_a_replay_equiv": {
            "campaign": CAMPAIGN_A,
            "text": csv_bytes([
                _ticket("TA02", "input", "X", "B", ta02_replay_mass, ta02_replay_unit),
            ]),
        },
        "f_a_replay_bytes": {
            # Byte-identical replay: not even a new source.
            "campaign": CAMPAIGN_A,
            "text": csv_bytes([
                _ticket("TA01", "input", "X", "A", "600", "g"),
                _ticket("TA02", "input", "X", "B", "0.400", "kg"),
                _ticket("TA03", "loss", "X", "", "100", "g"),
            ]),
        },
        "f_a_infile_conflict": {
            # Same ID, two different contents in one file: whole file rejects.
            "campaign": CAMPAIGN_A,
            "text": csv_bytes([
                _ticket("TA01", "input", "X", "A", "600", "g"),
                _ticket("TA01", "input", "X", "A", "601", "g"),
            ]),
        },
        "f_a_cross_conflict": {
            # TA01 already stored as 600 g; conflict rejects the whole file
            # and the brand-new TA99 row must roll back with it.
            "campaign": CAMPAIGN_A,
            "text": csv_bytes([
                _ticket("TA01", "input", "X", "A", "700", "g"),
                _ticket("TA99", "raw", "Z", "", "10", "g"),
            ]),
        },
        "f_a_malformed_ragged": {
            "campaign": CAMPAIGN_A,
            "text": b"ticket_id,kind,lot_id,input_lot_id,mass,unit\n"
                    b"TA01,input,X,A,600\n",
        },
        "f_a_malformed_unit": {
            "campaign": CAMPAIGN_A,
            "text": csv_bytes([
                _ticket("TA10", "raw", "A", "", "1000", "mg"),
            ]),
        },
        "f_a_supplement": {
            # Missing definitions/inputs/losses are补齐 here.
            "campaign": CAMPAIGN_A,
            "text": csv_bytes([
                _ticket("TA04", "raw", "A", "", "1000", "g"),
                _ticket("TA05", "raw", "B", "", "500", "g"),
                _ticket("TA06", "raw", "C", "", "300", "g"),
                _ticket("TA07", "output", "X", "", "900", "g"),
                _ticket("TA08", "input", "Y", "X", "300", "g"),
                _ticket("TA09", "input", "Y", "C", "200", "g"),
                _ticket("TA10", "output", "Y", "", "450", "g"),
                _ticket("TA11", "loss", "Y", "", "50", "g"),
            ]),
        },
        "f_a_after_seal_new": {
            "campaign": CAMPAIGN_A,
            "text": csv_bytes([
                _ticket("TA12", "raw", "Z", "", "50", "g"),
            ]),
        },
        "f_a_after_seal_replay": {
            "campaign": CAMPAIGN_A,
            "text": csv_bytes([
                _ticket("TA04", "raw", "A", "", "1000", "g"),
            ]),
        },
        # --- campaign B: smaller independent model, reuses IDs/lot names ---
        "f_b_ingest_1": {
            # Same ticket ID "TA01" as campaign A and same lot names: must
            # never mix across campaigns.
            "campaign": CAMPAIGN_B,
            "text": csv_bytes([
                _ticket("TA01", "raw", "A", "", "100", "g"),
                _ticket("TA02", "output", "P", "", "80", "g"),
                _ticket("TA03", "input", "P", "A", "80", "g"),
                _ticket("TA04", "loss", "P", "", "0", "g"),
            ]),
        },
        "f_b_identical_content": {
            # Different ticket ID but otherwise identical content: an
            # independent weighing, never deduplicated by content.
            "campaign": CAMPAIGN_B,
            "text": csv_bytes([
                _ticket("TA05", "raw", "A", "", "100", "g"),
            ]),
        },
        "f_b_malformed_header": {
            "campaign": CAMPAIGN_B,
            "text": b"ticket_id,kind,lot_id,input_lot_id,mass\n"
                    + csv_bytes([_ticket("TA01", "raw", "A", "", "100", "g")]),
        },
        "f_b_malformed_decimal": {
            "campaign": CAMPAIGN_B,
            "text": csv_bytes([
                _ticket("TA06", "input", "P", "A", "0.0001", "kg"),
            ]),
        },
    }
    return files


# Coverage tags are stable identifiers of exercised contract branches.
COVERAGE_TAGS = (
    "ingest_new_partial",
    "ingest_equivalent_replay",
    "ingest_identical_bytes_replay",
    "ingest_in_file_conflict",
    "ingest_cross_file_conflict",
    "ingest_malformed",
    "ingest_missing_ticket_supplement",
    "seal_failure_open",
    "seal_success_after_supplement",
    "seal_repeat_idempotent",
    "ingest_after_seal_new_refused",
    "ingest_after_seal_equivalent_refused",
    "ingest_malformed_after_seal",
    "status_open",
    "status_sealed",
    "status_unknown",
    "export_open_refused",
    "export_sealed",
    "export_unknown_refused",
    "cross_campaign_id_reuse",
)


def _base_operations() -> list[dict]:
    a, b = CAMPAIGN_A, CAMPAIGN_B
    return [
        {"op_id": "op01", "op": "ingest", "campaign": a,
         "file": "f_a_ingest_1", "tags": ["ingest_new_partial"]},
        {"op_id": "op02", "op": "ingest", "campaign": a,
         "file": "f_a_replay_equiv", "tags": ["ingest_equivalent_replay"]},
        {"op_id": "op03", "op": "ingest", "campaign": a,
         "file": "f_a_replay_bytes", "tags": ["ingest_identical_bytes_replay"]},
        {"op_id": "op04", "op": "ingest", "campaign": a,
         "file": "f_a_infile_conflict", "tags": ["ingest_in_file_conflict"]},
        {"op_id": "op05", "op": "ingest", "campaign": a,
         "file": "f_a_cross_conflict", "tags": ["ingest_cross_file_conflict"]},
        {"op_id": "op06", "op": "ingest", "campaign": a,
         "file": "f_a_malformed_ragged", "tags": ["ingest_malformed"]},
        {"op_id": "op07", "op": "status", "campaign": a,
         "tags": ["status_open"]},
        {"op_id": "op08", "op": "export", "campaign": a,
         "output": "out_a_early.json",
         "tags": ["export_open_refused"]},
        {"op_id": "op09", "op": "seal", "campaign": a,
         "tags": ["seal_failure_open"]},
        # Cross-campaign operations interleave; IDs/lot names intentionally
        # overlap with campaign A.
        {"op_id": "op10", "op": "ingest", "campaign": b,
         "file": "f_b_ingest_1", "tags": ["cross_campaign_id_reuse"]},
        {"op_id": "op11", "op": "ingest", "campaign": b,
         "file": "f_b_identical_content",
         "tags": ["cross_campaign_id_reuse"]},
        {"op_id": "op12", "op": "ingest", "campaign": b,
         "file": "f_b_malformed_header", "tags": ["ingest_malformed"]},
        {"op_id": "op13", "op": "ingest", "campaign": b,
         "file": "f_b_malformed_decimal", "tags": ["ingest_malformed"]},
        {"op_id": "op14", "op": "seal", "campaign": b,
         "tags": ["seal_success_after_supplement"]},
        {"op_id": "op15", "op": "status", "campaign": b,
         "tags": ["status_sealed"]},
        {"op_id": "op16", "op": "export", "campaign": b,
         "output": "out_b.json", "tags": ["export_sealed"]},
        # Back to campaign A: supplement the missing tickets, then seal.
        {"op_id": "op17", "op": "ingest", "campaign": a,
         "file": "f_a_malformed_unit", "tags": ["ingest_malformed"]},
        {"op_id": "op18", "op": "ingest", "campaign": a,
         "file": "f_a_supplement",
         "tags": ["ingest_missing_ticket_supplement"]},
        {"op_id": "op19", "op": "seal", "campaign": a,
         "tags": ["seal_success_after_supplement"]},
        {"op_id": "op20", "op": "seal", "campaign": a,
         "tags": ["seal_repeat_idempotent"]},
        {"op_id": "op21", "op": "status", "campaign": a,
         "tags": ["status_sealed"]},
        {"op_id": "op22", "op": "export", "campaign": a,
         "output": "out_a.json", "tags": ["export_sealed"]},
        # Post-seal writes must all be refused.
        {"op_id": "op23", "op": "ingest", "campaign": a,
         "file": "f_a_after_seal_new",
         "tags": ["ingest_after_seal_new_refused"]},
        {"op_id": "op24", "op": "ingest", "campaign": a,
         "file": "f_a_after_seal_replay",
         "tags": ["ingest_after_seal_equivalent_refused"]},
        {"op_id": "op25", "op": "ingest", "campaign": a,
         "file": "f_a_malformed_ragged",
         "tags": ["ingest_malformed_after_seal"]},
        # Unknown campaign reads never create a library.
        {"op_id": "op26", "op": "status", "campaign": "cmp-unknown",
         "tags": ["status_unknown"]},
        {"op_id": "op27", "op": "export", "campaign": "cmp-unknown",
         "output": "out_unknown.json",
         "tags": ["export_unknown_refused"]},
    ]


_PADDING_FILES = ("f_a_replay_bytes", "f_b_ingest_1")
_PADDING_STATUS = (CAMPAIGN_A, CAMPAIGN_B)


def generate_scenario(seed: int, steps: int) -> dict:
    """Return the deterministic, self-contained scenario object."""
    rng = _Rng(seed)
    base = _base_operations()
    files = _file_catalogue(rng)
    if steps < len(base):
        operations = [dict(op) for op in base[:steps]]
    else:
        operations = [dict(op) for op in base]
        extra = steps - len(base)
        for index in range(extra):
            op_id = f"op{len(operations) + 1:02d}"
            variant = rng.next_u32() % 3
            if variant == 0:
                file_id = _PADDING_FILES[rng.next_u32() % len(_PADDING_FILES)]
                campaign = files[file_id]["campaign"]
                operations.append(
                    {"op_id": op_id, "op": "ingest", "campaign": campaign,
                     "file": file_id, "tags": ["ingest_identical_bytes_replay"]}
                )
            elif variant == 1:
                campaign = rng.choice(_PADDING_STATUS)
                operations.append(
                    {"op_id": op_id, "op": "status", "campaign": campaign,
                     "tags": ["status_sealed"]}
                )
            else:
                campaign = rng.choice(_PADDING_STATUS)
                output = f"out_pad_{index:02d}.json"
                operations.append(
                    {"op_id": op_id, "op": "export", "campaign": campaign,
                     "output": output, "tags": ["export_sealed"]}
                )

    tag_set = sorted({tag for op in operations for tag in op.get("tags", ())})
    used_files = {
        op["file"] for op in operations if op["op"] == "ingest"
    }
    file_entries = []
    for file_id in sorted(used_files):
        entry = files[file_id]
        file_entries.append(
            {
                "file_id": file_id,
                "campaign": entry["campaign"],
                "csv_text": entry["text"].decode("utf-8"),
                "sha256": _sha256(entry["text"]),
            }
        )
    return {
        "format": FORMAT_SCENARIO,
        "seed": seed,
        "steps": steps,
        "campaigns": [CAMPAIGN_A, CAMPAIGN_B, "cmp-unknown"],
        "files": file_entries,
        "operations": operations,
        "coverage_tags": tag_set,
    }


def _sha256(raw: bytes) -> str:
    import hashlib

    return hashlib.sha256(raw).hexdigest()


def file_map(scenario: dict) -> dict[str, bytes]:
    """Materialise the embedded CSV texts as bytes for execution."""
    return {entry["file_id"]: entry["csv_text"].encode("utf-8")
            for entry in scenario["files"]}


def coverage_tags_for(operations) -> list[str]:
    tags = set()
    for op in operations:
        tags.update(op.get("tags", ()))
    return sorted(tags)
