"""Unit tests for :mod:`src.index.persistence`.

These tests cover the on-disk JSONL+gzip segment format end-to-end:
round-trip equality, checksum validation, atomic-write safety (no
orphan temp files on either success or mid-write failure), directory
enumeration + sort order, and the ``next_segment_id`` allocator.

Every test is sync and uses pytest's ``tmp_path`` fixture for
isolation. The tests never touch the app's lifespan or Redis — this
is a pure-stdlib module.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from src.index.persistence import (
    delete_segments,
    list_segment_files,
    next_segment_id,
    read_segment,
    write_segment,
)
from src.index.segment import Segment
from src.models import LogEntry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entry(doc_id: int, msg: str | None = None) -> LogEntry:
    """Build a LogEntry with a distinct, reproducible payload per doc."""
    return LogEntry(
        doc_id=doc_id,
        message=msg or f"log row #{doc_id} with timeout and retry",
        timestamp=1_700_000_000.0 + doc_id,
        service=f"svc-{doc_id % 3}",
        level="INFO" if doc_id % 2 == 0 else "ERROR",
        stream_id=f"stream-{doc_id}",
    )


def make_segment(segment_id: str = "seg-000001", n: int = 5) -> Segment:
    """Build an in-memory Segment with *n* documents, varying terms.

    The term sets are deliberately overlapping so the postings maps
    exercise both fresh-term and existing-term append paths.
    """
    seg = Segment(segment_id=segment_id)
    for i in range(n):
        doc_id = i + 1
        # Mix of shared ("common"), doc-specific ("doc-i") and parity
        # terms to keep posting lists non-trivial.
        terms = ["common", f"doc-{doc_id}", "even" if doc_id % 2 == 0 else "odd"]
        seg.add(doc_id, _make_entry(doc_id), terms)
    return seg


def _read_gz_bytes(path: Path) -> bytes:
    """Decompress *path* and return its raw bytes."""
    with gzip.open(path, "rb") as fh:
        return fh.read()


def _write_gz_bytes(path: Path, data: bytes) -> None:
    """Recompress *data* with gzip and write it back to *path*."""
    path.write_bytes(gzip.compress(data))


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------

def test_write_read_round_trip(tmp_path: Path) -> None:
    """A written segment round-trips every posting list and doc body."""
    original = make_segment(segment_id="seg-000001", n=5)
    written = write_segment(tmp_path, original)
    loaded = read_segment(written)

    assert loaded.segment_id == original.segment_id
    assert loaded.min_doc_id == original.min_doc_id
    assert loaded.max_doc_id == original.max_doc_id
    assert loaded.doc_count() == original.doc_count()
    assert loaded.term_count() == original.term_count()

    # Every posting list matches exactly.
    for term, postings in original.term_postings.items():
        assert loaded.search_term(term) == postings

    # Every doc body round-trips.
    for doc_id, entry, terms in original.iter_docs():
        assert loaded.doc_entries[doc_id] == entry
        assert loaded.doc_terms[doc_id] == terms


def test_round_trip_preserves_log_entry_fields(tmp_path: Path) -> None:
    """LogEntry fields (message/timestamp/service/level/stream_id) survive."""
    seg = Segment(segment_id="seg-000001")
    entry = LogEntry(
        doc_id=1,
        message="payment-service timeout 10.0.0.1",
        timestamp=1_712_345_678.5,
        service="payment-service",
        level="ERROR",
        stream_id="1712345678000-0",
    )
    seg.add(1, entry, ["payment", "timeout", "10.0.0.1"])

    path = write_segment(tmp_path, seg)
    loaded = read_segment(path)

    got = loaded.doc_entries[1]
    assert got.doc_id == 1
    assert got.message == "payment-service timeout 10.0.0.1"
    assert got.timestamp == 1_712_345_678.5
    assert got.service == "payment-service"
    assert got.level == "ERROR"
    assert got.stream_id == "1712345678000-0"


# ---------------------------------------------------------------------------
# Directory listing
# ---------------------------------------------------------------------------

def test_empty_dir_list_returns_empty(tmp_path: Path) -> None:
    """A fresh empty directory yields an empty list."""
    assert list_segment_files(tmp_path) == []


def test_nonexistent_dir_list_returns_empty(tmp_path: Path) -> None:
    """A missing directory is treated as empty — no exception."""
    assert list_segment_files(tmp_path / "nope") == []


def test_list_segment_files_sorted(tmp_path: Path) -> None:
    """Listing returns paths in lexicographic (== numeric) order."""
    # Write out of order so any insertion-order bias is exposed.
    for sid in ("seg-000003", "seg-000001", "seg-000002"):
        write_segment(tmp_path, make_segment(segment_id=sid, n=2))

    names = [p.name for p in list_segment_files(tmp_path)]
    assert names == [
        "seg-000001.jsonl.gz",
        "seg-000002.jsonl.gz",
        "seg-000003.jsonl.gz",
    ]


def test_list_ignores_tempfiles(tmp_path: Path) -> None:
    """Half-written ``.tmp-*`` files never leak into the listing."""
    write_segment(tmp_path, make_segment(segment_id="seg-000001", n=1))

    # Manufacture a stray temp file that a crashed write might leave.
    stray = tmp_path / ".tmp-garbage.jsonl.gz"
    stray.write_bytes(gzip.compress(b""))

    names = [p.name for p in list_segment_files(tmp_path)]
    assert ".tmp-garbage.jsonl.gz" not in names
    assert names == ["seg-000001.jsonl.gz"]


# ---------------------------------------------------------------------------
# next_segment_id
# ---------------------------------------------------------------------------

def test_next_segment_id_empty_dir(tmp_path: Path) -> None:
    """With no segments on disk, the allocator starts at seq 1."""
    assert next_segment_id(tmp_path) == "seg-000001"


def test_next_segment_id_missing_dir(tmp_path: Path) -> None:
    """A non-existent directory also starts the sequence at 1."""
    assert next_segment_id(tmp_path / "nope") == "seg-000001"


def test_next_segment_id_monotonic(tmp_path: Path) -> None:
    """Allocator takes max(existing) + 1, even across gaps."""
    for sid in ("seg-000001", "seg-000002", "seg-000007"):
        write_segment(tmp_path, make_segment(segment_id=sid, n=1))

    assert next_segment_id(tmp_path) == "seg-000008"


# ---------------------------------------------------------------------------
# delete_segments
# ---------------------------------------------------------------------------

def test_delete_segments_missing_ok(tmp_path: Path) -> None:
    """Deleting a path that never existed does not raise."""
    delete_segments([tmp_path / "missing.jsonl.gz"])  # should be a no-op


def test_delete_segments_removes_existing(tmp_path: Path) -> None:
    """Delete actually unlinks files that do exist."""
    path = write_segment(tmp_path, make_segment(segment_id="seg-000001", n=1))
    assert path.exists()
    delete_segments([path])
    assert not path.exists()


# ---------------------------------------------------------------------------
# Atomic write guarantees
# ---------------------------------------------------------------------------

def test_atomic_write_no_tempfile_after_success(tmp_path: Path) -> None:
    """A successful write leaves no ``.tmp-*`` residue in the target dir."""
    write_segment(tmp_path, make_segment(segment_id="seg-000001", n=3))

    leftovers = [p.name for p in tmp_path.iterdir() if p.name.startswith(".tmp-")]
    assert leftovers == []


def test_atomic_write_no_orphan_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash mid-write must not leave an orphan temp file or a target."""
    from src.index import persistence as persistence_module

    # Patch ``GzipFile.write`` to blow up after a handful of calls so
    # we crash partway through the segment. The temp-file cleanup path
    # should swallow our garbage and re-raise.
    original_write = gzip.GzipFile.write
    call_count = {"n": 0}

    def exploding_write(self: gzip.GzipFile, data: bytes) -> int:  # type: ignore[override]
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise RuntimeError("simulated gzip failure")
        return original_write(self, data)

    monkeypatch.setattr(persistence_module.gzip.GzipFile, "write", exploding_write)

    seg = make_segment(segment_id="seg-000001", n=3)
    with pytest.raises(RuntimeError, match="simulated gzip failure"):
        write_segment(tmp_path, seg)

    # Target file must not exist.
    assert not (tmp_path / "seg-000001.jsonl.gz").exists()

    # And no .tmp- orphan is left lying around.
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.startswith(".tmp-")]
    assert leftovers == [], f"unexpected temp files: {leftovers}"


# ---------------------------------------------------------------------------
# Integrity / format-violation paths
# ---------------------------------------------------------------------------

def test_checksum_detects_tampering(tmp_path: Path) -> None:
    """Flipping a byte in a doc line causes ``read_segment`` to raise."""
    path = write_segment(tmp_path, make_segment(segment_id="seg-000001", n=3))

    raw = _read_gz_bytes(path)
    lines = raw.split(b"\n")

    # Find the first doc line and flip a character inside its message.
    tampered_lines: list[bytes] = []
    tampered_once = False
    for line in lines:
        if (
            not tampered_once
            and line.startswith(b"{")
            and b'"type": "doc"' in line
        ):
            # Replace "log row" with "LOG ROW" — same length, still
            # valid JSON, but changes the hashed bytes.
            tampered = line.replace(b"log row", b"LOG ROW", 1)
            tampered_lines.append(tampered)
            tampered_once = True
        else:
            tampered_lines.append(line)

    assert tampered_once, "no doc line found to tamper with"
    _write_gz_bytes(path, b"\n".join(tampered_lines))

    with pytest.raises(ValueError, match="checksum"):
        read_segment(path)


def test_missing_footer_raises(tmp_path: Path) -> None:
    """A segment file without a footer line fails validation."""
    path = write_segment(tmp_path, make_segment(segment_id="seg-000001", n=2))

    raw = _read_gz_bytes(path)
    # Drop any footer line by keeping only header + doc lines.
    kept: list[bytes] = []
    for line in raw.split(b"\n"):
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("type") == "footer":
            continue
        kept.append(line)
    _write_gz_bytes(path, b"\n".join(kept) + b"\n")

    with pytest.raises(ValueError, match="footer"):
        read_segment(path)


def test_invalid_header_raises(tmp_path: Path) -> None:
    """A file whose first line is not a header is rejected."""
    path = tmp_path / "seg-999999.jsonl.gz"
    # First line claims to be a doc — that's illegal before the header.
    bogus = (
        json.dumps({"type": "doc", "doc_id": 1}).encode("utf-8") + b"\n"
        + json.dumps({"type": "footer", "sha256": "deadbeef"}).encode("utf-8") + b"\n"
    )
    _write_gz_bytes(path, bogus)

    with pytest.raises(ValueError, match="header"):
        read_segment(path)


# ---------------------------------------------------------------------------
# File-name ↔ segment_id contract
# ---------------------------------------------------------------------------

def test_segment_id_in_file_matches_path_stem(tmp_path: Path) -> None:
    """The written file's name always ends with ``<segment_id>.jsonl.gz``."""
    seg = make_segment(segment_id="seg-000042", n=2)
    path = write_segment(tmp_path, seg)
    assert path.name == "seg-000042.jsonl.gz"
    assert path.parent == tmp_path
