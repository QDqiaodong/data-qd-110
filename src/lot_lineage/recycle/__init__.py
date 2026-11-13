"""Steady-state closed-loop recycle accounting (schema_version 2).

This package is intentionally separate from :mod:`lot_lineage.pipeline`:
the lot/recipe model describes real acyclic batches, while this model
describes a steady-state period with no net inventory change, where
recycle loops and self-loops are first-class facts.
"""
from .model import ContractError, SteadyInput, load_steady
from .engine import NonUniqueError, SteadyResult, compute
from .pipeline import build

__all__ = [
    "ContractError",
    "NonUniqueError",
    "SteadyInput",
    "SteadyResult",
    "build",
    "compute",
    "load_steady",
]
