"""Manifest model: strict parsing, canonical bytes and snapshot IDs.

A manifest is the immutable commit record of one snapshot.  Its canonical
encoding is UTF-8 JSON with sorted keys, no insignificant whitespace and no
trailing newline; the SHA-256 of exactly those bytes is the snapshot ID.  Any
decoder must therefore *re-serialise* what it parsed and compare byte-for-byte
with what was read: that single check rejects duplicate keys, extra fields,
non-canonical whitespace and every other altered spelling at once.

Structural rules enforced here:

* top level has exactly ``snapshot_version``, ``library_family``,
  ``image_length``, ``image_sha256``, ``chunk_size``, ``chunks``;
* ``snapshot_version`` is the integer 1 (booleans are rejected);
* ``library_family`` is one of the three supported library families;
* ``image_length`` is a non-negative integer; ``image_sha256`` is 64 lowercase
  hex digits;
* ``chunk_size`` is exactly 65536;
* ``chunks`` is an ordered list of exactly ``ceil(image_length / chunk_size)``
  objects with exactly ``sha256`` and ``length``; the last chunk may be short,
  every other chunk is exactly ``chunk_size`` bytes; identical chunk digests
  are legal and keep their positions.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

SNAPSHOT_VERSION = 1
CHUNK_SIZE = 65536
LIBRARY_FAMILIES = ("lineage", "recycle", "intake_tickets")
MANIFEST_FIELDS = (
    "snapshot_version",
    "library_family",
    "image_length",
    "image_sha256",
    "chunk_size",
    "chunks",
)
CHUNK_FIELDS = ("sha256", "length")
_HEX = re.compile(r"[0-9a-f]{64}", re.ASCII)


class ContractError(ValueError):
    """Raised when a manifest, snapshot store or library image is invalid."""


class SnapshotError(ContractError):
    """Raised for snapshot-specific store/layout failures."""


def canonical_json(value) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _reject_constant(token: str):
    raise ContractError(f"manifest contains non-finite number {token!r}")


def _no_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ContractError(f"manifest has duplicate key {key!r}")
        result[key] = value
    return result


def _hex_digest(value, field: str) -> str:
    if not isinstance(value, str) or _HEX.fullmatch(value) is None:
        raise ContractError(f"{field} must be 64 lowercase hexadecimal digits")
    return value


def _plain_int(value, field: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ContractError(f"{field} must be an integer >= {minimum}")
    return value


def _image_length(value, field: str) -> int:
    # A SQLite library image is never empty; 0 chunks would not be restorable.
    return _plain_int(value, field, minimum=1)


@dataclass(frozen=True)
class ChunkRef:
    sha256: str
    length: int


@dataclass(frozen=True)
class Manifest:
    snapshot_id: str
    library_family: str
    image_length: int
    image_sha256: str
    chunks: tuple[ChunkRef, ...]
    chunk_size: int = CHUNK_SIZE
    snapshot_version: int = SNAPSHOT_VERSION

    def as_dict(self) -> dict:
        return {
            "snapshot_version": self.snapshot_version,
            "library_family": self.library_family,
            "image_length": self.image_length,
            "image_sha256": self.image_sha256,
            "chunk_size": self.chunk_size,
            "chunks": [
                {"sha256": chunk.sha256, "length": chunk.length}
                for chunk in self.chunks
            ],
        }

    def to_bytes(self) -> bytes:
        return canonical_json(self.as_dict())

    def object_ids(self) -> list[str]:
        return [chunk.sha256 for chunk in self.chunks]


def expected_chunk_lengths(image_length: int, chunk_size: int = CHUNK_SIZE) -> list[int]:
    if image_length == 0:
        return []
    full, tail = divmod(image_length, chunk_size)
    lengths = [chunk_size] * full
    if tail:
        lengths.append(tail)
    return lengths


def build_manifest(library_family: str, image_sha256: str, chunks: list[ChunkRef],
                   image_length: int) -> Manifest:
    """Validate the pieces and returns a Manifest with its content-derived ID."""
    if library_family not in LIBRARY_FAMILIES:
        raise ContractError(
            f"library_family must be one of {', '.join(LIBRARY_FAMILIES)}"
        )
    _hex_digest(image_sha256, "image_sha256")
    _image_length(image_length, "image_length")
    expected = expected_chunk_lengths(image_length)
    if len(chunks) != len(expected):
        raise ContractError(
            f"image of {image_length} bytes needs {len(expected)} chunks of "
            f"{CHUNK_SIZE} bytes, got {len(chunks)}"
        )
    for index, (chunk, length) in enumerate(zip(chunks, expected)):
        if chunk.length != length:
            raise ContractError(
                f"chunk {index} length {chunk.length} does not match the "
                f"expected {length} bytes"
            )
        _hex_digest(chunk.sha256, f"chunks/{index}/sha256")
    placeholder = Manifest(
        snapshot_id="0" * 64,
        library_family=library_family,
        image_length=image_length,
        image_sha256=image_sha256,
        chunks=tuple(chunks),
    )
    return Manifest(
        snapshot_id=hashlib.sha256(placeholder.to_bytes()).hexdigest(),
        library_family=placeholder.library_family,
        image_length=placeholder.image_length,
        image_sha256=placeholder.image_sha256,
        chunks=placeholder.chunks,
    )


def parse_manifest(raw: bytes, *, source_label: str = "<manifest>") -> Manifest:
    """Parse and fully validate one manifest encoding.

    The bytes must already be canonical: re-serialising the parsed object must
    reproduce them exactly, which also rejects duplicate keys.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ContractError(f"{source_label}: manifest is not valid UTF-8: {exc}") from exc
    try:
        data = json.loads(
            text,
            object_pairs_hook=_no_duplicates,
            parse_constant=_reject_constant,
        )
    except json.JSONDecodeError as exc:
        raise ContractError(
            f"{source_label}: invalid manifest JSON at line {exc.lineno} "
            f"col {exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(data, dict):
        raise ContractError(f"{source_label}: manifest must be an object")
    extra = set(data) - set(MANIFEST_FIELDS)
    missing = set(MANIFEST_FIELDS) - set(data)
    if extra or missing:
        detail = []
        if missing:
            detail.append("missing " + ",".join(sorted(missing)))
        if extra:
            detail.append("unexpected " + ",".join(sorted(extra)))
        raise ContractError(f"{source_label}: invalid manifest fields ({'; '.join(detail)})")

    if canonical_json(data) != raw:
        raise ContractError(
            f"{source_label}: manifest bytes are not the canonical sorted, "
            "compact, newline-free encoding"
        )

    version = data["snapshot_version"]
    if type(version) is not int or version != SNAPSHOT_VERSION:
        raise ContractError(
            f"{source_label}: unsupported snapshot_version {version!r}; "
            f"expected {SNAPSHOT_VERSION}"
        )
    family = data["library_family"]
    if not isinstance(family, str) or family not in LIBRARY_FAMILIES:
        raise ContractError(
            f"{source_label}: library_family must be one of "
            f"{', '.join(LIBRARY_FAMILIES)}"
        )
    image_length = _image_length(data["image_length"], f"{source_label}: image_length")
    image_sha = _hex_digest(data["image_sha256"], f"{source_label}: image_sha256")
    chunk_size = data["chunk_size"]
    if type(chunk_size) is not int or chunk_size != CHUNK_SIZE:
        raise ContractError(
            f"{source_label}: chunk_size must be exactly {CHUNK_SIZE}"
        )
    raw_chunks = data["chunks"]
    if not isinstance(raw_chunks, list):
        raise ContractError(f"{source_label}: chunks must be an array")
    chunks: list[ChunkRef] = []
    for index, item in enumerate(raw_chunks):
        if not isinstance(item, dict):
            raise ContractError(f"{source_label}: chunks/{index} must be an object")
        if set(item) != set(CHUNK_FIELDS):
            raise ContractError(
                f"{source_label}: chunks/{index} must have exactly "
                f"{', '.join(CHUNK_FIELDS)}"
            )
        digest = _hex_digest(item["sha256"], f"{source_label}: chunks/{index}/sha256")
        length = _plain_int(item["length"], f"{source_label}: chunks/{index}/length")
        chunks.append(ChunkRef(digest, length))

    manifest = build_manifest(family, image_sha, chunks, image_length)
    if manifest.chunks != tuple(chunks):  # pragma: no cover - build_manifest re-checks
        raise ContractError(f"{source_label}: inconsistent chunk list")
    return manifest


def manifest_id(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()
