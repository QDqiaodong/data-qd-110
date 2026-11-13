"""Offline weighing-ticket intake, sealing and export.

This package is independent of both the acyclic batch library
(:mod:`lot_lineage.pipeline`) and the steady-state recycle library
(:mod:`lot_lineage.recycle`).  It accumulates raw weighing tickets that may
arrive in several CSV batches, keeps them in an own SQLite format, and only
after an operator seals a campaign exports a canonical ``lots``/``recipes``
JSON that the existing batch build and CLI can consume unchanged.
"""
from .model import ContractError, ParsedBatch, Ticket, capture, parse_csv
from .store import SCHEMA_FAMILY, SCHEMA_VERSION, export, ingest, seal, status

__all__ = [
    "ContractError",
    "ParsedBatch",
    "SCHEMA_FAMILY",
    "SCHEMA_VERSION",
    "Ticket",
    "capture",
    "export",
    "ingest",
    "parse_csv",
    "seal",
    "status",
]
