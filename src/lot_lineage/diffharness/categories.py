"""Outcome categories shared by the in-memory oracle and the subprocess runner.

The vocabulary describes the *kind* of result the contract assigns to one
operation.  The oracle predicts a category; the runner maps the real process
exit status and stderr onto the same vocabulary using independent textual
signals, then compares the two without ever calling production validation.
"""
from __future__ import annotations

# The command succeeded (exit status 0 semantics).
OK = "ok"

# ingest: the file is structurally/ numerically invalid (whole file rejected).
REJECTED_MALFORMED = "rejected_malformed"

# ingest: a ticket ID conflicts (inside one file or with stored content).
REJECTED_CONFLICT = "rejected_conflict"

# ingest: target campaign is already sealed.
REJECTED_SEALED = "rejected_sealed"

# seal: the open ticket set violates the full production contract.
REJECTED_SEAL_FAILED = "rejected_seal_failed"

# seal/status/export: the campaign does not exist in the library.
REJECTED_UNKNOWN_CAMPAIGN = "rejected_unknown_campaign"

# export: campaign exists but is still open.
REJECTED_NOT_SEALED = "rejected_not_sealed"

FAILURE_CATEGORIES = frozenset(
    {
        REJECTED_MALFORMED,
        REJECTED_CONFLICT,
        REJECTED_SEALED,
        REJECTED_SEAL_FAILED,
        REJECTED_UNKNOWN_CAMPAIGN,
        REJECTED_NOT_SEALED,
    }
)
