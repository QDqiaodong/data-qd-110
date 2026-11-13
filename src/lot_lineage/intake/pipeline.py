"""Python-callable pipeline surface for the offline intake library.

Thin, stable wrappers around :mod:`lot_lineage.intake.store` so callers can
``from lot_lineage.intake.pipeline import ingest, seal, export, status`` or use
the package names re-exported from :mod:`lot_lineage.intake`.
"""
from __future__ import annotations

from .store import export, ingest, seal, status

__all__ = ["export", "ingest", "seal", "status"]
