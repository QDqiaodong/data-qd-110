"""SQLite library for offline weighing-ticket intake and sealed export.

Schema family ``intake_tickets`` version 1.  It is deliberately separate from
the acyclic batch library (tables ``lots``/``transforms``/...) and from the
steady-state recycle library (``recycle_*``): an existing database of either
old family is detected and refused *without* being modified.

Two lifetimes share one file:

``open``
    Raw files arrive one at a time.  Every file is parsed and validated before
    the database is touched, then its canonical tickets, the raw source bytes
    and every ticket record position are written in a single write
    transaction.  A ticket identity is ``(campaign_id, ticket_id)``; identical
    normalized content is a replay, different content under the same ID is a
    conflict that rejects the whole file.

``sealed``
    ``seal`` re-reads every canonical ticket of the campaign as one final
    batch, checks the complete production contract, writes the immutable
    canonical ``lots``/``recipes`` JSON and its SHA256, records the sealed
    recipe-edge breakdown, and flips the campaign to ``sealed`` in the same
    transaction.  From then on every ``ingest`` is refused; there is no
    unseal/overwrite/correction path.

All formal masses are exact Python integers.  Per-ticket gram values fit a
SQLite INTEGER (<= 10^12 by contract), but cumulative sums are accumulated in
Python and stored as decimal TEXT, so a sum beyond SQLite's signed 64-bit
range can never be silently truncated.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote

from ..graph import propagate, validate_document
from .model import (
    ContractError,
    ParsedBatch,
    Ticket,
    capture,
)

SCHEMA_FAMILY = "intake_tickets"
SCHEMA_VERSION = 1
DEFAULT_BUSY_TIMEOUT = 5.0
_JSON_KWARGS = {"ensure_ascii": False, "sort_keys": True, "separators": (",", ":")}

_SCHEMA_SQL = """
CREATE TABLE intake_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE intake_campaign (
    campaign_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('open','sealed')),
    seal_sha256 TEXT,
    sealed_json BLOB,
    sealed_ticket_count INTEGER,
    sealed_source_count INTEGER,
    CHECK (
        (status = 'open' AND seal_sha256 IS NULL AND sealed_json IS NULL)
        OR (status = 'sealed' AND seal_sha256 IS NOT NULL AND sealed_json IS NOT NULL)
    )
);
CREATE TABLE intake_source (
    source_pk INTEGER PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES intake_campaign(campaign_id),
    source_sha256 TEXT NOT NULL,
    raw_bytes BLOB NOT NULL,
    byte_length INTEGER NOT NULL,
    UNIQUE (campaign_id, source_sha256)
);
CREATE TABLE intake_ticket (
    campaign_id TEXT NOT NULL REFERENCES intake_campaign(campaign_id),
    ticket_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('raw','output','input','loss')),
    lot_id TEXT NOT NULL,
    input_lot_id TEXT,
    grams INTEGER NOT NULL CHECK (grams >= 0 AND grams <= 1000000000000),
    PRIMARY KEY (campaign_id, ticket_id),
    CHECK (
        (kind = 'input' AND input_lot_id IS NOT NULL)
        OR (kind <> 'input' AND input_lot_id IS NULL)
    )
);
CREATE TABLE intake_source_ticket (
    source_pk INTEGER NOT NULL REFERENCES intake_source(source_pk),
    campaign_id TEXT NOT NULL,
    ticket_id TEXT NOT NULL,
    record_no INTEGER NOT NULL CHECK (record_no >= 1),
    line_no INTEGER NOT NULL CHECK (line_no >= 1),
    PRIMARY KEY (source_pk, record_no),
    FOREIGN KEY (campaign_id, ticket_id)
        REFERENCES intake_ticket(campaign_id, ticket_id)
);
CREATE INDEX intake_source_ticket_by_ticket
    ON intake_source_ticket (campaign_id, ticket_id);
CREATE TABLE intake_sealed_edge (
    campaign_id TEXT NOT NULL,
    output_lot TEXT NOT NULL,
    input_lot TEXT NOT NULL,
    total_grams TEXT NOT NULL,
    ticket_count INTEGER NOT NULL,
    PRIMARY KEY (campaign_id, output_lot, input_lot),
    FOREIGN KEY (campaign_id) REFERENCES intake_campaign(campaign_id)
);
CREATE TABLE intake_sealed_edge_ticket (
    campaign_id TEXT NOT NULL,
    output_lot TEXT NOT NULL,
    input_lot TEXT NOT NULL,
    ticket_id TEXT NOT NULL,
    PRIMARY KEY (campaign_id, output_lot, input_lot, ticket_id),
    FOREIGN KEY (campaign_id, output_lot, input_lot)
        REFERENCES intake_sealed_edge(campaign_id, output_lot, input_lot)
);
"""


def _validate_campaign(campaign_id: str) -> str:
    if not isinstance(campaign_id, str) or campaign_id == "" or campaign_id != campaign_id.strip():
        raise ContractError(
            "campaign_id must be a non-empty string without surrounding whitespace"
        )
    return campaign_id


# ---------------------------------------------------------------------------
# format detection and connections
# ---------------------------------------------------------------------------

def _tables(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _detect(conn: sqlite3.Connection) -> str:
    tables = _tables(conn)
    if "intake_meta" in tables:
        rows = dict(conn.execute("SELECT key, value FROM intake_meta").fetchall())
        if rows.get("schema_family") != SCHEMA_FAMILY:
            raise ContractError(
                f"database declares unsupported schema family {rows.get('schema_family')!r}"
            )
        if rows.get("schema_version") != str(SCHEMA_VERSION):
            raise ContractError(
                f"database declares unsupported intake schema version "
                f"{rows.get('schema_version')!r}; expected {SCHEMA_VERSION}"
            )
        return "intake"
    if "recycle_meta" in tables:
        return "recycle"
    if {"lots", "transforms", "inputs", "origin_mix"} <= tables:
        return "lineage"
    if not tables:
        return "empty"
    return "unknown"


def _refuse_foreign(database: Path, *, allow_empty: bool = False) -> None:
    """Read-only pre-flight; raises without writing for any non-intake file."""
    try:
        conn = sqlite3.connect(
            f"file:{quote(str(database))}?mode=ro", uri=True, isolation_level=None
        )
    except sqlite3.Error as exc:
        raise ContractError(f"cannot read database {database}: {exc}") from exc
    try:
        family = _detect(conn)
    except sqlite3.DatabaseError as exc:
        raise ContractError(
            f"database {database} is not a readable SQLite library: {exc}"
        ) from exc
    finally:
        conn.close()
    if family == "intake":
        return
    if family == "empty" and allow_empty:
        return
    raise ContractError(
        f"database {database} is not an intake ticket library "
        f"(detected {family}); refusing to modify it"
    )


def _connect_write(database: Path, busy_timeout: float) -> sqlite3.Connection:
    conn = sqlite3.connect(str(database), timeout=busy_timeout, isolation_level=None)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(f"PRAGMA busy_timeout={max(0, int(busy_timeout * 1000))}")
    return conn


def _connect_read(database: Path, busy_timeout: float) -> sqlite3.Connection:
    conn = sqlite3.connect(
        f"file:{quote(str(database))}?mode=ro", uri=True,
        timeout=busy_timeout, isolation_level=None,
    )
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(f"PRAGMA busy_timeout={max(0, int(busy_timeout * 1000))}")
    conn.execute("PRAGMA query_only=ON")
    return conn


@contextmanager
def _write_transaction(conn: sqlite3.Connection):
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:  # pragma: no cover - connection already unusable
            pass
        raise
    else:
        conn.execute("COMMIT")


def _schema_statements() -> list[str]:
    # ``Connection.executescript`` commits any pending transaction, which would
    # silently destroy our BEGIN IMMEDIATE boundary.  Split the DDL and execute
    # the statements one by one so schema creation is part of the same write
    # transaction and concurrency is serialized by SQLite itself.
    return [statement.strip() for statement in _SCHEMA_SQL.split(";") if statement.strip()]


def _ensure_schema(conn: sqlite3.Connection) -> None:
    family = _detect(conn)
    if family == "intake":
        return
    if family != "empty":
        raise ContractError(
            f"database is not an intake ticket library (detected {family}); "
            "refusing to modify it"
        )
    for statement in _schema_statements():
        conn.execute(statement)
    conn.executemany(
        "INSERT INTO intake_meta VALUES (?, ?)",
        [("schema_family", SCHEMA_FAMILY), ("schema_version", str(SCHEMA_VERSION))],
    )


# ---------------------------------------------------------------------------
# file-level canonicalisation
# ---------------------------------------------------------------------------

def _canonicalise_file(tickets: list[Ticket]) -> dict[str, Ticket]:
    """Collapse a file by ticket ID, rejecting conflicting duplicates.

    Identical normalized content repeated inside one file is a replay and is
    accepted; the first occurrence supplies the canonical content.  Different
    content under one ID inside the same file rejects the whole file.
    """
    first: dict[str, Ticket] = {}
    for ticket in tickets:
        seen = first.get(ticket.ticket_id)
        if seen is None:
            first[ticket.ticket_id] = ticket
        elif seen.business_key != ticket.business_key:
            raise ContractError(
                f"ticket {ticket.ticket_id!r} conflicts within the file: "
                f"record {seen.record} has {seen.business_key}, "
                f"record {ticket.record} has {ticket.business_key}"
            )
    return first


def _existing_tickets(
    conn: sqlite3.Connection, campaign_id: str, ids: list[str]
) -> dict[str, tuple]:
    found: dict[str, tuple] = {}
    for start in range(0, len(ids), 500):
        chunk = ids[start:start + 500]
        placeholders = ",".join("?" for _ in chunk)
        for row in conn.execute(
            f"SELECT ticket_id, kind, lot_id, input_lot_id, grams FROM intake_ticket "
            f"WHERE campaign_id = ? AND ticket_id IN ({placeholders})",
            (campaign_id, *chunk),
        ):
            found[row[0]] = (row[1], row[2], row[3], row[4])
    return found


# Small insertion helpers are module-level so tests can inject a write failure
# at a precise point inside the transaction without patching private closures.
def _insert_ticket(conn: sqlite3.Connection, campaign_id: str, ticket: Ticket) -> None:
    conn.execute(
        "INSERT INTO intake_ticket "
        "(campaign_id, ticket_id, kind, lot_id, input_lot_id, grams) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            campaign_id,
            ticket.ticket_id,
            ticket.kind,
            ticket.lot_id,
            ticket.input_lot_id,
            ticket.grams,
        ),
    )


def _source_exists(conn: sqlite3.Connection, campaign_id: str, sha256: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM intake_source WHERE campaign_id = ? AND source_sha256 = ?",
        (campaign_id, sha256),
    ).fetchone() is not None


def _insert_source(
    conn: sqlite3.Connection, campaign_id: str, raw_bytes: bytes, sha256: str
) -> int:
    cursor = conn.execute(
        "INSERT INTO intake_source (campaign_id, source_sha256, raw_bytes, byte_length) "
        "VALUES (?, ?, ?, ?)",
        (campaign_id, sha256, sqlite3.Binary(raw_bytes), len(raw_bytes)),
    )
    return cursor.lastrowid


def _insert_positions(
    conn: sqlite3.Connection, source_pk: int, campaign_id: str, tickets: list[Ticket]
) -> None:
    conn.executemany(
        "INSERT INTO intake_source_ticket "
        "(source_pk, campaign_id, ticket_id, record_no, line_no) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (source_pk, campaign_id, t.ticket_id, t.record, t.line)
            for t in tickets
        ],
    )


def _insert_sealed_edges(
    conn: sqlite3.Connection, campaign_id: str, facts: "_Facts"
) -> None:
    edge_rows = []
    for (output_lot, input_lot), grams in sorted(facts.edges.items()):
        edge_rows.append(
            (
                campaign_id,
                output_lot,
                input_lot,
                str(grams),
                facts.edge_counts[(output_lot, input_lot)],
            )
        )
    conn.executemany(
        "INSERT INTO intake_sealed_edge "
        "(campaign_id, output_lot, input_lot, total_grams, ticket_count) "
        "VALUES (?, ?, ?, ?, ?)",
        edge_rows,
    )
    conn.execute(
        "INSERT INTO intake_sealed_edge_ticket "
        "(campaign_id, output_lot, input_lot, ticket_id) "
        "SELECT campaign_id, lot_id, input_lot_id, ticket_id FROM intake_ticket "
        "WHERE campaign_id = ? AND kind = 'input'",
        (campaign_id,),
    )


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------

def ingest(
    database,
    campaign,
    source,
    *,
    busy_timeout: float = DEFAULT_BUSY_TIMEOUT,
    _sync_hook=None,
) -> dict:
    """Append one CSV batch of raw tickets to an open campaign.

    The file is captured and fully validated before any database access.
    Canonical tickets, the source bytes and all record positions commit in a
    single write transaction; any conflict or write error rolls the whole file
    back and leaves the library unchanged so a corrected file can be retried.
    """
    campaign_id = _validate_campaign(campaign)
    batch: ParsedBatch = capture(source)
    file_by_id = _canonicalise_file(batch.tickets)

    database = Path(database)
    if database.exists():
        # An empty file left by a previously rolled-back first ingest is
        # acceptable; schema creation happens inside the write transaction.
        _refuse_foreign(database, allow_empty=True)
    else:
        database.parent.mkdir(parents=True, exist_ok=True)

    conn = _connect_write(database, busy_timeout)
    try:
        with _write_transaction(conn):
            _ensure_schema(conn)
            row = conn.execute(
                "SELECT status FROM intake_campaign WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO intake_campaign (campaign_id, status) VALUES (?, 'open')",
                    (campaign_id,),
                )
            elif row[0] == "sealed":
                raise ContractError(
                    f"campaign {campaign_id!r} is sealed; further ingest is refused"
                )

            existing = _existing_tickets(conn, campaign_id, list(file_by_id))
            new_ids: list[str] = []
            for ticket_id, ticket in file_by_id.items():
                stored = existing.get(ticket_id)
                if stored is None:
                    new_ids.append(ticket_id)
                elif stored != ticket.business_key:
                    raise ContractError(
                        f"ticket {ticket_id!r} conflicts with stored content: "
                        f"stored {stored}, file record {ticket.record} has "
                        f"{ticket.business_key}"
                    )

            for ticket_id in sorted(new_ids):
                ticket = file_by_id[ticket_id]
                _insert_ticket(conn, campaign_id, ticket)

            duplicate_source = _source_exists(conn, campaign_id, batch.sha256)
            if not duplicate_source:
                source_pk = _insert_source(conn, campaign_id, batch.raw_bytes, batch.sha256)
                _insert_positions(conn, source_pk, campaign_id, batch.tickets)

            if _sync_hook is not None:
                _sync_hook("ingest_before_commit")

            ticket_count = conn.execute(
                "SELECT COUNT(*) FROM intake_ticket WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()[0]
            source_count = conn.execute(
                "SELECT COUNT(*) FROM intake_source WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()[0]
    finally:
        conn.close()

    new_tickets = len(new_ids)
    replay_occurrences = len(batch.tickets) - new_tickets
    receipt = {
        "campaign": campaign_id,
        "status": "open",
        "records": len(batch.tickets),
        "new_tickets": new_tickets,
        "replayed_occurrences": replay_occurrences,
        "new_source": 0 if duplicate_source else 1,
        "source_sha256": batch.sha256,
        "ticket_count": ticket_count,
        "source_count": source_count,
    }
    return receipt


# ---------------------------------------------------------------------------
# seal
# ---------------------------------------------------------------------------

class _Facts:
    __slots__ = (
        "definitions", "losses", "edges", "edge_counts", "outputs_with_input",
        "mentioned_lots", "ticket_count",
    )

    def __init__(self) -> None:
        # lot -> [ticket_id, kind, grams]
        self.definitions: dict[str, list[tuple[str, str, int]]] = defaultdict(list)
        # lot -> [grams] (must end with exactly one entry)
        self.losses: dict[str, list[int]] = defaultdict(list)
        # (output_lot, input_lot) -> exact Python integer sum
        self.edges: dict[tuple[str, str], int] = defaultdict(int)
        self.edge_counts: dict[tuple[str, str], int] = defaultdict(int)
        self.outputs_with_input: set[str] = set()
        self.mentioned_lots: set[str] = set()
        self.ticket_count = 0


def _apply_fact(facts: _Facts, ticket_id: str, kind: str, lot_id: str,
                input_lot_id: str | None, grams: int) -> None:
    facts.ticket_count += 1
    facts.mentioned_lots.add(lot_id)
    if kind in ("raw", "output"):
        facts.definitions[lot_id].append((ticket_id, kind, grams))
    elif kind == "loss":
        facts.losses[lot_id].append(grams)
    else:  # input
        facts.mentioned_lots.add(input_lot_id)
        facts.edges[(lot_id, input_lot_id)] += grams
        facts.edge_counts[(lot_id, input_lot_id)] += 1
        facts.outputs_with_input.add(lot_id)


def _load_facts(conn: sqlite3.Connection, campaign_id: str) -> _Facts:
    facts = _Facts()
    cursor = conn.execute(
        "SELECT ticket_id, kind, lot_id, input_lot_id, grams "
        "FROM intake_ticket WHERE campaign_id = ? ORDER BY ticket_id",
        (campaign_id,),
    )
    for ticket_id, kind, lot_id, input_lot_id, grams in cursor:
        _apply_fact(facts, ticket_id, kind, lot_id, input_lot_id, grams)
    return facts


def facts_from_tickets(tickets) -> _Facts:
    """Build the same fact set from in-memory tickets (used by unit tests).

    Tickets supplied this way are not written to SQLite; this exists so the
    aggregate arithmetic can be exercised beyond SQLite's integer range
    without a ticket that violates the per-ticket 10^12 contract.
    """
    facts = _Facts()
    for ticket in sorted(tickets, key=lambda item: item.ticket_id):
        _apply_fact(
            facts, ticket.ticket_id, ticket.kind, ticket.lot_id,
            ticket.input_lot_id, ticket.grams,
        )
    return facts


def _build_document(campaign_id: str, facts: _Facts) -> dict:
    """Apply the full batch contract and return sorted ``lots``/``recipes``.

    Mass sums are Python integers throughout; an aggregate can exceed both the
    per-ticket 10^12 limit and SQLite's 64-bit integer range without ever being
    truncated.  Aggregated sums are then compared with the existing contract.
    """
    complete_lots: dict[str, tuple[str, int]] = {}
    for lot in sorted(facts.mentioned_lots):
        definitions = facts.definitions.get(lot, [])
        if len(definitions) != 1:
            found = ", ".join(
                f"{kind} ticket {ticket_id!r}" for ticket_id, kind, _grams in definitions
            )
            raise ContractError(
                f"campaign {campaign_id!r}: lot {lot!r} must have exactly one "
                f"raw or output definition ticket, found {len(definitions)}"
                + (f" ({found})" if found else "")
            )
        _ticket_id, kind, grams = definitions[0]
        complete_lots[lot] = ("raw" if kind == "raw" else "produced", grams)

    for lot, (kind, _grams) in complete_lots.items():
        if kind != "raw":
            continue
        if facts.losses.get(lot):
            raise ContractError(
                f"campaign {campaign_id!r}: raw lot {lot!r} must not have loss tickets"
            )
        if lot in facts.outputs_with_input:
            raise ContractError(
                f"campaign {campaign_id!r}: raw lot {lot!r} must not have input tickets"
            )

    recipes: dict[str, dict] = {}
    for output_lot, (_kind, output_grams) in complete_lots.items():
        if _kind != "produced":
            continue
        if output_lot not in facts.outputs_with_input:
            raise ContractError(
                f"campaign {campaign_id!r}: output lot {output_lot!r} requires at "
                "least one input ticket"
            )
        losses = facts.losses.get(output_lot, [])
        if len(losses) != 1:
            raise ContractError(
                f"campaign {campaign_id!r}: output lot {output_lot!r} must have "
                f"exactly one loss ticket (explicit zero loss required), found {len(losses)}"
            )
        inputs = []
        total_input = 0
        for (lot_id, input_lot_id), grams in facts.edges.items():
            if lot_id != output_lot:
                continue
            # Re-checked independently of the per-ticket limit: the aggregated
            # recipe edge must also be within the existing contract bound.
            if grams > 10**12:
                raise ContractError(
                    f"campaign {campaign_id!r}: recipe edge {output_lot!r} <- "
                    f"{input_lot_id!r} totals {grams} g, exceeds 10^12 grams"
                )
            inputs.append({"lot_id": input_lot_id, "mass_g": grams})
            total_input += grams
        loss_grams = losses[0]
        if total_input != output_grams + loss_grams:
            raise ContractError(
                f"campaign {campaign_id!r}: output lot {output_lot!r} mass not "
                f"conserved: inputs {total_input} g != output {output_grams} g + "
                f"loss {loss_grams} g"
            )
        inputs.sort(key=lambda item: item["lot_id"])
        recipes[output_lot] = {
            "output_lot": output_lot,
            "inputs": inputs,
            "loss_g": loss_grams,
        }

    consumed: dict[str, int] = defaultdict(int)
    for (output_lot, input_lot_id), grams in facts.edges.items():
        if output_lot in recipes:
            consumed[input_lot_id] += grams
    for lot, total in consumed.items():
        lot_mass = complete_lots[lot][1]
        if total > lot_mass:
            raise ContractError(
                f"campaign {campaign_id!r}: lot {lot!r} over-consumed: "
                f"{total} g > mass {lot_mass} g"
            )

    document = {
        "lots": [
            {"lot_id": lot, "kind": complete_lots[lot][0], "mass_g": complete_lots[lot][1]}
            for lot in sorted(complete_lots)
        ],
        "recipes": [recipes[output_lot] for output_lot in sorted(recipes)],
    }
    # Reuse the existing batch parser and topology check verbatim so the sealed
    # artifact is exactly what the old build/CLI contract accepts.
    try:
        lots, recipes_map, _consumed = validate_document(document)
        propagate(lots, recipes_map)
    except ValueError as exc:
        raise ContractError(
            f"campaign {campaign_id!r}: complete ticket set violates the batch "
            f"contract: {exc}"
        ) from exc
    return document


def _write_seal(
    conn: sqlite3.Connection,
    campaign_id: str,
    payload: bytes,
    digest: str,
    facts: _Facts,
) -> None:
    _insert_sealed_edges(conn, campaign_id, facts)
    ticket_count = conn.execute(
        "SELECT COUNT(*) FROM intake_ticket WHERE campaign_id = ?", (campaign_id,)
    ).fetchone()[0]
    source_count = conn.execute(
        "SELECT COUNT(*) FROM intake_source WHERE campaign_id = ?", (campaign_id,)
    ).fetchone()[0]
    conn.execute(
        "UPDATE intake_campaign SET status = 'sealed', seal_sha256 = ?, "
        "sealed_json = ?, sealed_ticket_count = ?, sealed_source_count = ? "
        "WHERE campaign_id = ? AND status = 'open'",
        (digest, sqlite3.Binary(payload), ticket_count, source_count, campaign_id),
    )
    applied = conn.execute(
        "SELECT status FROM intake_campaign WHERE campaign_id = ?", (campaign_id,)
    ).fetchone()[0]
    if applied != "sealed":  # pragma: no cover - defensive
        raise ContractError(
            f"campaign {campaign_id!r}: seal status transition did not apply"
        )


def _seal_receipt(
    campaign_id: str,
    digest: str,
    payload: bytes,
    document: dict,
    ticket_count: int,
    source_count: int,
    *,
    already_sealed: bool,
) -> dict:
    return {
        "campaign": campaign_id,
        "status": "sealed",
        "seal_sha256": digest,
        "json_bytes": len(payload),
        "lot_count": len(document["lots"]),
        "recipe_count": len(document["recipes"]),
        "ticket_count": ticket_count,
        "source_count": source_count,
        "already_sealed": already_sealed,
    }


def seal(
    database,
    campaign,
    *,
    busy_timeout: float = DEFAULT_BUSY_TIMEOUT,
    _sync_hook=None,
) -> dict:
    """Validate every received ticket and freeze the campaign as one batch.

    Success stores the canonical ``lots``/``recipes`` JSON and its SHA256 and
    flips the status in the same transaction.  Failure changes nothing (no
    partial recipes, no sealed marker), so missing tickets can be appended and
    sealing retried.  Sealing an already sealed campaign is idempotent and
    returns the stored digest.
    """
    campaign_id = _validate_campaign(campaign)
    database = Path(database)
    if not database.exists():
        raise ContractError(f"database does not exist: {database}")
    # ``allow_empty`` covers the empty file a rolled-back first ingest leaves
    # behind; the seal transaction itself re-checks campaign state.
    _refuse_foreign(database, allow_empty=True)

    conn = _connect_write(database, busy_timeout)
    try:
        with _write_transaction(conn):
            _ensure_schema(conn)
            row = conn.execute(
                "SELECT status, seal_sha256, sealed_json, sealed_ticket_count, "
                "sealed_source_count FROM intake_campaign WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()
            if row is None:
                raise ContractError(f"unknown campaign {campaign_id!r}")
            status, stored_digest, stored_json, stored_tickets, stored_sources = row
            if status == "sealed":
                payload = bytes(stored_json)
                if hashlib.sha256(payload).hexdigest() != stored_digest:
                    raise ContractError(
                        f"campaign {campaign_id!r}: stored sealed JSON digest mismatch"
                    )
                document = json.loads(payload.decode("utf-8"))
                return _seal_receipt(
                    campaign_id, stored_digest, payload, document,
                    stored_tickets, stored_sources, already_sealed=True,
                )

            if _sync_hook is not None:
                _sync_hook("seal_after_checks")

            facts = _load_facts(conn, campaign_id)
            if facts.ticket_count == 0:
                raise ContractError(
                    f"campaign {campaign_id!r} has no tickets; refusing to seal"
                )
            document = _build_document(campaign_id, facts)
            payload = json.dumps(document, **_JSON_KWARGS).encode("utf-8")
            digest = hashlib.sha256(payload).hexdigest()
            _write_seal(conn, campaign_id, payload, digest, facts)
            ticket_count = conn.execute(
                "SELECT COUNT(*) FROM intake_ticket WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()[0]
            source_count = conn.execute(
                "SELECT COUNT(*) FROM intake_source WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()[0]
    finally:
        conn.close()

    return _seal_receipt(
        campaign_id, digest, payload, document,
        ticket_count, source_count, already_sealed=False,
    )


# ---------------------------------------------------------------------------
# export and status
# ---------------------------------------------------------------------------

def _same_file(left: Path, right: Path) -> bool:
    try:
        left_resolved = left.resolve(strict=True)
    except OSError:
        left_resolved = None
    try:
        right_resolved = right.resolve(strict=False)
    except OSError:
        return False
    if left_resolved is not None and left_resolved == right_resolved:
        return True
    try:
        left_stat = left.stat()
        right_stat = right.stat()
    except OSError:
        return False
    return (
        left_stat.st_dev == right_stat.st_dev
        and left_stat.st_ino == right_stat.st_ino
    )


def _atomic_write(output: Path, payload: bytes) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(
        prefix=".intake-", suffix=".tmp", dir=str(output.parent)
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, output)
    except BaseException:
        Path(temp_path).unlink(missing_ok=True)
        raise


def export(database, campaign, output, *, busy_timeout: float = DEFAULT_BUSY_TIMEOUT) -> dict:
    """Write the sealed canonical JSON to ``output`` from one read snapshot.

    The output must not alias the database (including recognizable symlink or
    hard-link aliases).  Everything is read and digest-checked before a
    temporary file in the output directory atomically replaces the target; a
    failure never unseals the campaign and can simply be retried.
    """
    campaign_id = _validate_campaign(campaign)
    database = Path(database)
    output = Path(output)
    if _same_file(database, output):
        raise ContractError("output must not be the same file as the database")
    if not database.exists():
        raise ContractError(f"database does not exist: {database}")
    _refuse_foreign(database)

    conn = _connect_read(database, busy_timeout)
    try:
        try:
            conn.execute("BEGIN")
            row = conn.execute(
                "SELECT status, seal_sha256, sealed_json FROM intake_campaign "
                "WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()
            if row is None:
                raise ContractError(f"unknown campaign {campaign_id!r}")
            status, digest, blob = row
            if status != "sealed":
                raise ContractError(
                    f"campaign {campaign_id!r} is open; export requires a sealed campaign"
                )
            payload = bytes(blob)
            if hashlib.sha256(payload).hexdigest() != digest:
                raise ContractError(
                    f"campaign {campaign_id!r}: sealed JSON digest mismatch; refusing export"
                )
            try:
                json.loads(payload.decode("utf-8"))
            except ValueError as exc:
                raise ContractError(
                    f"campaign {campaign_id!r}: sealed JSON is not valid JSON: {exc}"
                ) from exc
        except sqlite3.DatabaseError as exc:
            raise ContractError(
                f"campaign {campaign_id!r}: database read failed: {exc}"
            ) from exc
        finally:
            try:
                conn.execute("COMMIT")
            except sqlite3.Error:  # pragma: no cover - read-only transaction
                pass
    finally:
        conn.close()

    _atomic_write(output, payload)
    return {
        "campaign": campaign_id,
        "status": "sealed",
        "sha256": digest,
        "json_bytes": len(payload),
    }


def status(database, campaign, *, busy_timeout: float = DEFAULT_BUSY_TIMEOUT) -> dict:
    """Read-only campaign status; never creates a database."""
    campaign_id = _validate_campaign(campaign)
    database = Path(database)
    if not database.exists():
        raise ContractError(f"database does not exist: {database}")
    _refuse_foreign(database)
    conn = _connect_read(database, busy_timeout)
    try:
        try:
            conn.execute("BEGIN")
            row = conn.execute(
                "SELECT status, seal_sha256 FROM intake_campaign WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()
            if row is None:
                raise ContractError(f"unknown campaign {campaign_id!r}")
            state, digest = row
            ticket_count = conn.execute(
                "SELECT COUNT(*) FROM intake_ticket WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()[0]
            source_count = conn.execute(
                "SELECT COUNT(*) FROM intake_source WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()[0]
        except sqlite3.DatabaseError as exc:
            raise ContractError(
                f"campaign {campaign_id!r}: database read failed: {exc}"
            ) from exc
        finally:
            try:
                conn.execute("COMMIT")
            except sqlite3.Error:  # pragma: no cover - read-only transaction
                pass
    finally:
        conn.close()
    return {
        "campaign": campaign_id,
        "status": state,
        "ticket_count": ticket_count,
        "source_count": source_count,
        "seal_sha256": digest,
    }
