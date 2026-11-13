"""Independent in-memory oracle for the weighing-ticket contract.

This module is a *separate* implementation of the intake/seal/export contract
described in ``docs/contract_intake.md``.  It is deliberately written without
importing any production module (``lot_lineage.intake.*``,
``lot_lineage.graph`` and friends never appear below): the CSV parsing,
identity/replay rules, seal validation, acyclicity check and canonical JSON
construction are all re-derived here from the contract text.  If the
production implementation drifts from the contract, the harness can catch it;
if the oracle merely called production functions, it could not.

Only Python standard-library modules are used.  The :mod:`csv` tokenizer is
used purely as a CSV grammar reader; every business rule below is local to
this module.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections import defaultdict

from . import categories as C

HEADER = ("ticket_id", "kind", "lot_id", "input_lot_id", "mass", "unit")
KINDS = ("raw", "output", "input", "loss")
MASS_LIMIT = 10**12
JSON_KWARGS = {"ensure_ascii": False, "sort_keys": True, "separators": (",", ":")}

# Plain non-negative decimal, no sign, exponent, whitespace, NaN or Infinity.
_MASS_RE = re.compile(r"[0-9]+(?:\.[0-9]{1,3})?", re.ASCII)


class OracleError(ValueError):
    """Raised by this independent implementation when the contract is broken."""


class MalformedTicketFile(OracleError):
    """The CSV bytes cannot be accepted even before identity checks."""


# ---------------------------------------------------------------------------
# Independent CSV / single-row parsing
# ---------------------------------------------------------------------------

def _is_identifier(value: str) -> bool:
    return value != "" and value == value.strip()


def _parse_mass(text: str, unit: str, *, zero_allowed: bool) -> int:
    """Convert a mass cell to whole grams in exact integer arithmetic."""
    if not _MASS_RE.fullmatch(text):
        raise MalformedTicketFile("mass cell is not a plain <=3-fraction decimal")
    if unit == "g":
        factor = 1
    elif unit == "kg":
        factor = 1000
    else:
        raise MalformedTicketFile("unit must be g or kg")
    whole, _, fraction = text.partition(".")
    denominator = 10 ** len(fraction)
    numerator = (int(whole) * denominator + (int(fraction) if fraction else 0)) * factor
    if numerator % denominator != 0:
        raise MalformedTicketFile("mass does not denote a whole number of grams")
    grams = numerator // denominator
    low = 0 if zero_allowed else 1
    if not low <= grams <= MASS_LIMIT:
        raise MalformedTicketFile("mass outside the allowed gram range")
    return grams


def parse_ticket_bytes(raw_bytes: bytes) -> list[dict]:
    """Parse one CSV file independently.

    Returns one dict per data record with keys
    ``ticket_id/kind/lot_id/input_lot_id/grams/record/line``.  Any structural
    or single-row contract violation raises :class:`MalformedTicketFile`.
    """
    if not isinstance(raw_bytes, bytes) or not raw_bytes:
        raise MalformedTicketFile("empty input")
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MalformedTicketFile(f"not valid UTF-8: {exc}") from exc
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    try:
        header = next(reader)
    except StopIteration as exc:
        raise MalformedTicketFile("file is empty; header required") from exc
    except csv.Error as exc:
        raise MalformedTicketFile(f"header is not valid CSV: {exc}") from exc
    if tuple(header) != HEADER:
        raise MalformedTicketFile("header tuple/order mismatch")
    tickets: list[dict] = []
    record = 0
    try:
        for row in reader:
            record += 1
            line = reader.line_num
            if len(row) != len(HEADER):
                raise MalformedTicketFile(
                    f"record {record}: ragged row with {len(row)} fields"
                )
            ticket_id, kind, lot_id, input_lot_id, mass_text, unit = row
            if not _is_identifier(ticket_id):
                raise MalformedTicketFile(f"record {record}: bad ticket_id")
            if kind not in KINDS:
                raise MalformedTicketFile(f"record {record}: bad kind")
            if not _is_identifier(lot_id):
                raise MalformedTicketFile(f"record {record}: bad lot_id")
            if kind == "input":
                if not _is_identifier(input_lot_id):
                    raise MalformedTicketFile(
                        f"record {record}: input ticket requires input_lot_id"
                    )
                normalized_input = input_lot_id
            else:
                if input_lot_id != "":
                    raise MalformedTicketFile(
                        f"record {record}: input_lot_id must be empty for {kind}"
                    )
                normalized_input = None
            grams = _parse_mass(
                mass_text, unit, zero_allowed=(kind == "loss")
            )
            tickets.append(
                {
                    "ticket_id": ticket_id,
                    "kind": kind,
                    "lot_id": lot_id,
                    "input_lot_id": normalized_input,
                    "grams": grams,
                    "record": record,
                    "line": line,
                }
            )
    except csv.Error as exc:
        raise MalformedTicketFile(f"invalid CSV near line {reader.line_num}: {exc}") from exc
    if not tickets:
        raise MalformedTicketFile("header only; at least one data record required")
    return tickets


def _business_key(ticket: dict) -> tuple:
    return (
        ticket["kind"],
        ticket["lot_id"],
        ticket["input_lot_id"],
        ticket["grams"],
    )


# ---------------------------------------------------------------------------
# Independent seal validation and canonical document
# ---------------------------------------------------------------------------

def _is_acyclic(document: dict) -> bool:
    """Independently check that the recipe graph has no directed cycle."""
    adjacency: dict[str, list[str]] = defaultdict(list)
    nodes = {lot["lot_id"] for lot in document["lots"]}
    for recipe in document["recipes"]:
        for edge in recipe["inputs"]:
            adjacency[edge["lot_id"]].append(recipe["output_lot"])
    WHITE, GREY, BLACK = 0, 1, 2
    colour = {node: WHITE for node in nodes}

    def visit(node: str) -> bool:
        colour[node] = GREY
        for nxt in sorted(adjacency.get(node, ())):
            if colour[nxt] == GREY:
                return False
            if colour[nxt] == WHITE and not visit(nxt):
                return False
        colour[node] = BLACK
        return True

    return all(visit(node) for node in sorted(nodes) if colour[node] == WHITE)


def build_sealed_document(campaign_id: str, tickets: list[dict]) -> dict:
    """Apply the full seal-time contract to one campaign's canonical tickets.

    Raises :class:`OracleError` for any violation; otherwise returns the
    canonical ``lots``/``recipes`` document in stable order.  Every check is
    implemented locally rather than delegated to production validators.
    """
    definitions: dict[str, list[dict]] = defaultdict(list)
    losses: dict[str, list[dict]] = defaultdict(list)
    edges: dict[tuple[str, str], int] = defaultdict(int)
    edge_tickets: dict[tuple[str, str], list[str]] = defaultdict(list)
    outputs_with_input: set[str] = set()
    mentioned: set[str] = set()

    for ticket in sorted(tickets, key=lambda item: item["ticket_id"]):
        kind = ticket["kind"]
        lot = ticket["lot_id"]
        mentioned.add(lot)
        if kind in ("raw", "output"):
            definitions[lot].append(ticket)
        elif kind == "loss":
            losses[lot].append(ticket)
        else:  # input
            mentioned.add(ticket["input_lot_id"])
            key = (lot, ticket["input_lot_id"])
            edges[key] += ticket["grams"]
            edge_tickets[key].append(ticket["ticket_id"])
            outputs_with_input.add(lot)

    complete: dict[str, tuple[str, int]] = {}
    for lot in sorted(mentioned):
        defs = definitions.get(lot, [])
        if len(defs) != 1:
            raise OracleError(
                f"{campaign_id}: lot {lot!r} needs exactly one definition ticket, "
                f"found {len(defs)}"
            )
        definition = defs[0]
        complete[lot] = (
            "raw" if definition["kind"] == "raw" else "produced",
            definition["grams"],
        )

    for lot, (kind, _grams) in complete.items():
        if kind == "raw":
            if losses.get(lot):
                raise OracleError(f"{campaign_id}: raw lot {lot!r} carries a loss ticket")
            if lot in outputs_with_input:
                raise OracleError(f"{campaign_id}: raw lot {lot!r} carries an input ticket")

    recipes: dict[str, dict] = {}
    for output_lot in sorted(complete):
        kind, output_grams = complete[output_lot]
        if kind != "produced":
            continue
        if output_lot not in outputs_with_input:
            raise OracleError(
                f"{campaign_id}: produced lot {output_lot!r} has no input ticket"
            )
        lot_losses = losses.get(output_lot, [])
        if len(lot_losses) != 1:
            raise OracleError(
                f"{campaign_id}: produced lot {output_lot!r} needs exactly one loss "
                f"ticket, found {len(lot_losses)}"
            )
        inputs = []
        total_input = 0
        for (out, input_lot), grams in sorted(edges.items()):
            if out != output_lot:
                continue
            if input_lot not in complete:
                raise OracleError(
                    f"{campaign_id}: edge {output_lot!r} <- {input_lot!r} references "
                    "a lot without a definition ticket"
                )
            if grams > MASS_LIMIT:
                raise OracleError(
                    f"{campaign_id}: aggregated edge {output_lot!r} <- {input_lot!r} "
                    f"= {grams} g exceeds 10^12"
                )
            inputs.append({"lot_id": input_lot, "mass_g": grams})
            total_input += grams
        loss_grams = lot_losses[0]["grams"]
        if total_input != output_grams + loss_grams:
            raise OracleError(
                f"{campaign_id}: lot {output_lot!r} not conserved: inputs "
                f"{total_input} != output {output_grams} + loss {loss_grams}"
            )
        inputs.sort(key=lambda item: item["lot_id"])
        recipes[output_lot] = {
            "output_lot": output_lot,
            "inputs": inputs,
            "loss_g": loss_grams,
        }

    consumed: dict[str, int] = defaultdict(int)
    for (output_lot, input_lot), grams in edges.items():
        if output_lot in recipes:
            consumed[input_lot] += grams
    for lot, total in consumed.items():
        if total > complete[lot][1]:
            raise OracleError(
                f"{campaign_id}: lot {lot!r} over-consumed {total} > {complete[lot][1]}"
            )

    document = {
        "lots": [
            {"lot_id": lot, "kind": complete[lot][0], "mass_g": complete[lot][1]}
            for lot in sorted(complete)
        ],
        "recipes": [recipes[lot] for lot in sorted(recipes)],
    }
    if not _is_acyclic(document):
        raise OracleError(f"{campaign_id}: recipe graph is cyclic")
    return document


def canonical_json_bytes(document: dict) -> bytes:
    return json.dumps(document, **JSON_KWARGS).encode("utf-8")


# ---------------------------------------------------------------------------
# In-memory campaign state
# ---------------------------------------------------------------------------

class _Campaign:
    __slots__ = (
        "status", "tickets", "sources", "seal_sha256", "sealed_json",
        "sealed_edges",
    )

    def __init__(self) -> None:
        self.status = "open"
        # ticket_id -> business key tuple
        self.tickets: dict[str, tuple] = {}
        # source_sha256 -> {"bytes": bytes, "positions": [{ticket_id, record_no, line_no}]}
        self.sources: dict[str, dict] = {}
        self.seal_sha256: str | None = None
        self.sealed_json: bytes | None = None
        # (output_lot, input_lot) -> {"total_grams": int, "ticket_ids": [..]}
        self.sealed_edges: dict[tuple, dict] = {}


class InMemoryOracle:
    """Stateful oracle replaying a whole scenario with no real database."""

    def __init__(self) -> None:
        self._campaigns: dict[str, _Campaign] = {}

    # -- state helpers ------------------------------------------------------

    def _get(self, campaign_id: str) -> _Campaign | None:
        return self._campaigns.get(campaign_id)

    def _open(self, campaign_id: str) -> _Campaign:
        campaign = self._campaigns.get(campaign_id)
        if campaign is None:
            campaign = _Campaign()
            self._campaigns[campaign_id] = campaign
        return campaign

    # -- operations ---------------------------------------------------------

    def ingest(self, campaign_id: str, raw_bytes: bytes) -> tuple[str, dict]:
        try:
            parsed = parse_ticket_bytes(raw_bytes)
        except MalformedTicketFile:
            return C.REJECTED_MALFORMED, {}

        campaign = self._campaigns.get(campaign_id)
        if campaign is not None and campaign.status == "sealed":
            return C.REJECTED_SEALED, {}

        # File-internal canonicalisation: first occurrence wins; any same-ID
        # row with different content rejects the whole file.
        file_first: dict[str, dict] = {}
        for ticket in parsed:
            seen = file_first.get(ticket["ticket_id"])
            if seen is None:
                file_first[ticket["ticket_id"]] = ticket
            elif _business_key(seen) != _business_key(ticket):
                return C.REJECTED_CONFLICT, {}

        if campaign is None:
            campaign = self._open(campaign_id)

        # Cross-file identity check against stored canonical tickets.
        new_ids: list[str] = []
        for ticket_id, ticket in file_first.items():
            stored = campaign.tickets.get(ticket_id)
            if stored is None:
                new_ids.append(ticket_id)
            elif stored != _business_key(ticket):
                return C.REJECTED_CONFLICT, {}

        # Commit phase (the contract makes this one atomic write).
        for ticket_id in sorted(new_ids):
            campaign.tickets[ticket_id] = _business_key(file_first[ticket_id])

        digest = hashlib.sha256(raw_bytes).hexdigest()
        new_source = 0
        if digest not in campaign.sources:
            campaign.sources[digest] = {"bytes": raw_bytes, "positions": []}
            new_source = 1
            for ticket in parsed:
                campaign.sources[digest]["positions"].append(
                    {
                        "ticket_id": ticket["ticket_id"],
                        "record_no": ticket["record"],
                        "line_no": ticket["line"],
                    }
                )

        receipt = {
            "campaign": campaign_id,
            "status": "open",
            "records": len(parsed),
            "new_tickets": len(new_ids),
            "replayed_occurrences": len(parsed) - len(new_ids),
            "new_source": new_source,
            "source_sha256": digest,
            "ticket_count": len(campaign.tickets),
            "source_count": len(campaign.sources),
        }
        return C.OK, receipt

    def seal(self, campaign_id: str) -> tuple[str, dict]:
        campaign = self._campaigns.get(campaign_id)
        if campaign is None:
            return C.REJECTED_UNKNOWN_CAMPAIGN, {}
        if campaign.status == "sealed":
            document = json.loads(campaign.sealed_json.decode("utf-8"))
            return C.OK, self._seal_receipt(campaign, document, already=True)
        if not campaign.tickets:
            return C.REJECTED_UNKNOWN_CAMPAIGN, {}

        # Reconstruct full ticket records from stored business keys.
        tickets = [
            {
                "ticket_id": ticket_id,
                "kind": key[0],
                "lot_id": key[1],
                "input_lot_id": key[2],
                "grams": key[3],
            }
            for ticket_id, key in sorted(campaign.tickets.items())
        ]
        try:
            document = build_sealed_document(campaign_id, tickets)
        except OracleError:
            return C.REJECTED_SEAL_FAILED, {}

        payload = canonical_json_bytes(document)
        digest = hashlib.sha256(payload).hexdigest()
        edges: dict[tuple, dict] = {}
        for recipe in document["recipes"]:
            for edge in recipe["inputs"]:
                key = (recipe["output_lot"], edge["lot_id"])
                edges[key] = {"total_grams": edge["mass_g"], "ticket_ids": []}
        for ticket in tickets:
            if ticket["kind"] == "input":
                key = (ticket["lot_id"], ticket["input_lot_id"])
                edges[key]["ticket_ids"].append(ticket["ticket_id"])
        for key in edges:
            edges[key]["ticket_ids"].sort()

        campaign.status = "sealed"
        campaign.sealed_json = payload
        campaign.seal_sha256 = digest
        campaign.sealed_edges = edges
        return C.OK, self._seal_receipt(campaign, document, already=False)

    @staticmethod
    def _seal_receipt(campaign: _Campaign, document: dict, *, already: bool) -> dict:
        return {
            "status": "sealed",
            "seal_sha256": campaign.seal_sha256,
            "json_bytes": len(campaign.sealed_json),
            "lot_count": len(document["lots"]),
            "recipe_count": len(document["recipes"]),
            "ticket_count": len(campaign.tickets),
            "source_count": len(campaign.sources),
            "already_sealed": already,
        }

    def status(self, campaign_id: str) -> tuple[str, dict]:
        campaign = self._campaigns.get(campaign_id)
        if campaign is None:
            return C.REJECTED_UNKNOWN_CAMPAIGN, {}
        return C.OK, {
            "campaign": campaign_id,
            "status": campaign.status,
            "ticket_count": len(campaign.tickets),
            "source_count": len(campaign.sources),
            "seal_sha256": campaign.seal_sha256,
        }

    def export(self, campaign_id: str) -> tuple[str, dict, bytes | None]:
        campaign = self._campaigns.get(campaign_id)
        if campaign is None:
            return C.REJECTED_UNKNOWN_CAMPAIGN, {}, None
        if campaign.status != "sealed":
            return C.REJECTED_NOT_SEALED, {}, None
        receipt = {
            "campaign": campaign_id,
            "status": "sealed",
            "sha256": campaign.seal_sha256,
            "json_bytes": len(campaign.sealed_json),
        }
        return C.OK, receipt, campaign.sealed_json

    # -- state view for divergence comparison -------------------------------

    def campaign_view(self, campaign_id: str) -> dict | None:
        """Structural snapshot used to compare against the real SQLite state."""
        campaign = self._campaigns.get(campaign_id)
        if campaign is None:
            return None
        positions = []
        for digest in sorted(campaign.sources):
            source = campaign.sources[digest]
            for pos in source["positions"]:
                positions.append(
                    {
                        "source_sha256": digest,
                        "ticket_id": pos["ticket_id"],
                        "record_no": pos["record_no"],
                        "line_no": pos["line_no"],
                    }
                )
        positions.sort(
            key=lambda item: (
                item["source_sha256"], item["record_no"], item["ticket_id"]
            )
        )
        edges = [
            {
                "output_lot": key[0],
                "input_lot": key[1],
                "total_grams": value["total_grams"],
                "ticket_ids": list(value["ticket_ids"]),
            }
            for key, value in sorted(campaign.sealed_edges.items())
        ]
        return {
            "status": campaign.status,
            "ticket_count": len(campaign.tickets),
            "source_count": len(campaign.sources),
            "seal_sha256": campaign.seal_sha256,
            "sealed_json": (
                campaign.sealed_json.decode("utf-8")
                if campaign.sealed_json is not None
                else None
            ),
            "tickets": [
                {
                    "ticket_id": ticket_id,
                    "kind": key[0],
                    "lot_id": key[1],
                    "input_lot_id": key[2],
                    "grams": key[3],
                }
                for ticket_id, key in sorted(campaign.tickets.items())
            ],
            "sources": sorted(campaign.sources),
            "positions": positions,
            "edges": edges,
        }


def predict_operation(oracle: InMemoryOracle, operation: dict, file_bytes: dict[str, bytes]):
    """Drive one scenario operation through the in-memory model.

    Returns ``(category, receipt, export_bytes)`` where the last two are
    empty/``None`` when not applicable.
    """
    kind = operation["op"]
    campaign = operation["campaign"]
    if kind == "ingest":
        category, receipt = oracle.ingest(campaign, file_bytes[operation["file"]])
        return category, receipt, None
    if kind == "seal":
        category, receipt = oracle.seal(campaign)
        return category, receipt, None
    if kind == "status":
        category, receipt = oracle.status(campaign)
        return category, receipt, None
    category, receipt, payload = oracle.export(campaign)
    return category, receipt, payload
