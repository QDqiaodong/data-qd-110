"""Parsing and validation of raw weighing-ticket CSV batches.

A ticket is one row of a UTF-8 CSV whose header is exactly and in order::

    ticket_id,kind,lot_id,input_lot_id,mass,unit

Rules enforced here (single-row structure/number/identity only; cross-ticket
identity is decided inside the write transaction by :mod:`store`):

* ``kind`` is one of ``raw``, ``output``, ``input``, ``loss``.
* ``lot_id`` names the material lot: the raw lot for ``raw``, the produced lot
  for the other three kinds.
* exactly the ``input`` kind carries ``input_lot_id``; every other kind must
  leave that column empty.
* identifiers are non-empty strings without leading/trailing whitespace,
  case-sensitive.
* ``unit`` is exactly ``g`` or ``kg``.
* ``mass`` is a plain non-negative decimal string (no sign, whitespace,
  exponent, ``NaN`` or ``Infinity``) with at most three fraction digits.  The
  conversion to integer grams is done in exact integer arithmetic, never via
  ``float``; a mass that does not denote a whole number of grams (for example
  ``0.0001 kg`` = 0.1 g) is rejected.
* ``raw``/``output``/``input`` masses must be 1..10^12 g; ``loss`` may be
  0..10^12 g.

Every error names the 1-based data record number (the header is not counted)
and, once known, the ticket ID.  The whole file is rejected on any error;
nothing from a rejected file is meant to be written.
"""
from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass, field

SCHEMA_FAMILY = "intake_tickets"
SCHEMA_VERSION = 1
HEADER = ("ticket_id", "kind", "lot_id", "input_lot_id", "mass", "unit")
KINDS = ("raw", "output", "input", "loss")
MASS_LIMIT = 10**12

# Plain decimal: no sign, no exponent, no whitespace, no NaN/Infinity.
_MASS_RE = re.compile(r"[0-9]+(?:\.[0-9]{1,3})?", re.ASCII)


class ContractError(ValueError):
    """Raised when a ticket file, campaign state or sealed artifact is invalid."""


@dataclass(frozen=True)
class Ticket:
    ticket_id: str
    kind: str
    lot_id: str
    input_lot_id: str | None
    grams: int
    record: int  # 1-based data record number (header excluded)
    line: int    # 1-based physical line number in the file

    @property
    def business_key(self) -> tuple:
        """Normalized content used to decide identical replay vs conflict."""
        return (self.kind, self.lot_id, self.input_lot_id, self.grams)

    @property
    def raw_header(self) -> tuple:
        return HEADER


@dataclass
class ParsedBatch:
    raw_bytes: bytes
    sha256: str
    tickets: list[Ticket] = field(default_factory=list)


def _parse_mass(text: str, unit_factor: int, *, zero_allowed: bool, where: str) -> int:
    if not _MASS_RE.fullmatch(text):
        raise ContractError(
            f"{where}: mass must be a plain decimal string without sign, whitespace, "
            f"exponent, NaN or Infinity, with at most three fraction digits"
        )
    whole, _, fraction = text.partition(".")
    denominator = 10 ** len(fraction)
    numerator = (int(whole) * denominator + (int(fraction) if fraction else 0)) * unit_factor
    if numerator % denominator != 0:
        raise ContractError(f"{where}: mass is not a whole number of grams")
    grams = numerator // denominator
    low = 0 if zero_allowed else 1
    if not low <= grams <= MASS_LIMIT:
        raise ContractError(
            f"{where}: mass must be between {low} and 10^12 grams after conversion"
        )
    return grams


def _is_identifier(value: str) -> bool:
    return value != "" and value == value.strip()


def parse_csv(raw_bytes: bytes, *, source_label: str = "<input>") -> list[Ticket]:
    """Parse already-captured bytes; the source file is never re-read."""
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ContractError(f"{source_label}: input is not valid UTF-8: {exc}") from exc
    # newline='' keeps embedded newlines inside quoted fields intact.
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    tickets: list[Ticket] = []
    try:
        header = next(reader)
    except StopIteration:
        raise ContractError(f"{source_label}: file is empty; expected header {','.join(HEADER)}")
    except csv.Error as exc:
        raise ContractError(f"{source_label}: header is not valid CSV: {exc}") from exc
    if tuple(header) != HEADER:
        raise ContractError(
            f"{source_label}: header must be exactly and in order "
            f"{','.join(HEADER)}; got {','.join(header)!r}"
        )
    # csv.reader counts physical lines; record numbers are counted separately.
    record = 0
    try:
        for row in reader:
            record += 1
            line = reader.line_num
            where = f"{source_label}: record {record}"
            if len(row) != len(HEADER):
                raise ContractError(
                    f"{where}: expected {len(HEADER)} fields, got {len(row)} (ragged row)"
                )
            ticket_id, kind, lot_id, input_lot_id, mass_text, unit = row
            if _is_identifier(ticket_id):
                where = f"{where} (ticket {ticket_id!r})"
            if not _is_identifier(ticket_id):
                raise ContractError(
                    f"{where}: ticket_id must be a non-empty string without surrounding whitespace"
                )
            if kind not in KINDS:
                raise ContractError(f"{where}: kind must be one of {', '.join(KINDS)}")
            if not _is_identifier(lot_id):
                raise ContractError(
                    f"{where}: lot_id must be a non-empty string without surrounding whitespace"
                )
            if kind == "input":
                if not _is_identifier(input_lot_id):
                    raise ContractError(
                        f"{where}: input tickets require a non-empty input_lot_id "
                        f"without surrounding whitespace"
                    )
                normalized_input: str | None = input_lot_id
            else:
                if input_lot_id != "":
                    raise ContractError(f"{where}: input_lot_id must be empty for kind {kind}")
                normalized_input = None
            if unit == "g":
                factor = 1
            elif unit == "kg":
                factor = 1000
            else:
                raise ContractError(f"{where}: unit must be exactly 'g' or 'kg'")
            grams = _parse_mass(
                mass_text, factor, zero_allowed=(kind == "loss"), where=where
            )
            tickets.append(
                Ticket(
                    ticket_id=ticket_id,
                    kind=kind,
                    lot_id=lot_id,
                    input_lot_id=normalized_input,
                    grams=grams,
                    record=record,
                    line=line,
                )
            )
    except csv.Error as exc:
        raise ContractError(
            f"{source_label}: record {record + 1}: invalid CSV near line {reader.line_num}: {exc}"
        ) from exc
    if not tickets:
        raise ContractError(f"{source_label}: no data records; at least one ticket is required")
    return tickets


def capture(source) -> ParsedBatch:
    """Capture the raw bytes exactly once, then parse and hash that copy."""
    from pathlib import Path

    raw = Path(source).read_bytes()
    tickets = parse_csv(raw, source_label=str(source))
    return ParsedBatch(raw_bytes=raw, sha256=hashlib.sha256(raw).hexdigest(), tickets=tickets)
