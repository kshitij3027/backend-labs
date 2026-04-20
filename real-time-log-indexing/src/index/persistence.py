"""Warm-tier segment persistence — atomic JSONL+gzip read and write.

A disk segment is a single gzipped file containing one JSON object per
line: a header, a series of ``doc`` lines, and a footer holding the
SHA-256 digest of everything that came before it (header + doc lines,
raw utf-8 bytes with their trailing newlines, before gzip). This is
deliberately boring — it opens with ``zcat | head`` and never requires
a custom parser — while still giving us strong crash-safety:

* **Atomic writes** via ``tempfile.mkstemp`` in the same directory and
  ``os.replace`` at the end, so a partial write is never visible to
  readers or to ``list_segment_files``. Temp files are prefixed with
  ``".tmp-"`` so the glob in :func:`list_segment_files` skips them, and
  so an orphan (from a crash) is obvious to sweep up.
* **Checksum footer** — :func:`read_segment` rehashes the plaintext
  header+docs bytes on the way in and compares against the footer; a
  mismatch raises ``ValueError`` so the orchestrator can quarantine.
* **Sequence ids** via :func:`next_segment_id` — re-scans the segment
  directory on every call so the allocator stays monotonic across
  process restarts without needing any shared state.

Everything here is stdlib-only and synchronous. The :class:`Segment`
object it reads/writes comes from :mod:`src.index.segment` and each
document body is rehydrated via :meth:`src.models.LogEntry.model_validate`.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Iterable

from src.index.segment import Segment
from src.models import LogEntry


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Format version bump this if the on-disk schema changes incompatibly.
_FORMAT_VERSION: int = 1

# Prefix used for in-progress temp files. The glob in
# :func:`list_segment_files` deliberately skips these so readers never
# see a half-written segment.
_TMP_PREFIX: str = ".tmp-"

# File-name convention: ``seg-NNNNNN.jsonl.gz`` where N is a 6-digit
# zero-padded sequence number. Both writers and the id allocator rely
# on this exact regex.
_SEGMENT_NAME_RE: re.Pattern[str] = re.compile(r"seg-(\d{6})\.jsonl\.gz")


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------

def write_segment(dir: Path, segment: Segment) -> Path:
    """Write *segment* to *dir* as a gzipped JSONL file, atomically.

    Parameters
    ----------
    dir:
        Target directory. Created (with parents) if missing.
    segment:
        The in-memory :class:`Segment` to persist. Must have at least a
        ``segment_id``; an empty segment is legal (header + footer, no
        doc lines).

    Returns
    -------
    pathlib.Path
        Absolute path to the finalised file — ``dir / f"{segment.segment_id}.jsonl.gz"``.

    Notes
    -----
    The file appears under its final name only after ``os.replace``
    completes. If any exception occurs mid-write, the temp file is
    unlinked and the exception re-raised.
    """
    dir = Path(dir)
    dir.mkdir(parents=True, exist_ok=True)
    target = dir / f"{segment.segment_id}.jsonl.gz"

    # ``tempfile.mkstemp`` in the same directory guarantees the final
    # rename is atomic on POSIX (same filesystem). We keep the fd so we
    # can close it deterministically in the finally block.
    fd, tmp_name = tempfile.mkstemp(
        dir=str(dir),
        prefix=_TMP_PREFIX,
        suffix=".jsonl.gz",
    )
    tmp_path = Path(tmp_name)

    sha = hashlib.sha256()

    # Count docs + terms up front so the header can carry them. This
    # also forces a full iteration, which matches what we're about to
    # write out — no chance of a header/body mismatch.
    docs: list[tuple[int, LogEntry, list[str]]] = list(segment.iter_docs())
    doc_count = len(docs)
    term_count = len(segment.term_postings)

    # Header carries enough to rehydrate the Segment shell without
    # needing to re-scan every doc. ``min_doc_id`` / ``max_doc_id`` may
    # be ``None`` for an empty segment — JSON happily encodes that.
    header = {
        "type": "header",
        "segment_id": segment.segment_id,
        "min_doc_id": segment.min_doc_id,
        "max_doc_id": segment.max_doc_id,
        "doc_count": doc_count,
        "term_count": term_count,
        "created_at": segment.created_at,
        "format_version": _FORMAT_VERSION,
    }

    gz: gzip.GzipFile | None = None
    raw = os.fdopen(fd, "wb")
    try:
        gz = gzip.GzipFile(fileobj=raw, mode="wb")

        # Header first, feeding the running digest.
        header_line = json.dumps(header).encode("utf-8") + b"\n"
        sha.update(header_line)
        gz.write(header_line)

        # One line per doc. ``model_dump`` gives us a plain dict that
        # ``json.dumps`` can serialise without a custom encoder.
        for doc_id, entry, terms in docs:
            payload = {
                "type": "doc",
                "doc_id": doc_id,
                "entry": entry.model_dump(),
                "terms": terms,
            }
            line = json.dumps(payload).encode("utf-8") + b"\n"
            sha.update(line)
            gz.write(line)

        # Footer carries the digest over the plaintext header+doc
        # bytes. The footer itself is NOT part of the hashed region;
        # that would be a chicken-and-egg problem.
        footer = {"type": "footer", "sha256": sha.hexdigest()}
        footer_line = json.dumps(footer).encode("utf-8") + b"\n"
        gz.write(footer_line)

        # Close gzip + underlying fd cleanly before the rename so the
        # replaced file is fully flushed. ``close()`` on the GzipFile
        # writes the trailer; the outer ``raw`` close flushes the fd.
        gz.close()
        gz = None
        raw.close()
        raw = None  # type: ignore[assignment]

        os.replace(tmp_path, target)
        return target
    except BaseException:
        # On any failure, best-effort close both handles and remove the
        # temp file so we don't leave orphans behind. We swallow errors
        # from the cleanup itself — the original exception is what the
        # caller needs to see.
        try:
            if gz is not None:
                gz.close()
        except Exception:
            pass
        try:
            if raw is not None:
                raw.close()
        except Exception:
            pass
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        except Exception:
            pass
        raise


# ---------------------------------------------------------------------------
# Read path
# ---------------------------------------------------------------------------

def read_segment(path: Path) -> Segment:
    """Read a gzipped JSONL segment from *path* and return a :class:`Segment`.

    Parameters
    ----------
    path:
        Absolute or relative path to a ``seg-NNNNNN.jsonl.gz`` file
        previously written by :func:`write_segment`.

    Returns
    -------
    Segment
        The rehydrated segment with every posting/doc map reconstructed.

    Raises
    ------
    ValueError
        If the header is missing or invalid, the footer is absent, or
        the SHA-256 checksum doesn't match the plaintext bytes.
    """
    path = Path(path)
    sha = hashlib.sha256()

    header: dict | None = None
    footer: dict | None = None
    # Parse doc lines into memory in order; we can't start adding them
    # to the Segment until we've seen the header (which supplies the
    # segment_id and created_at), so we buffer them.
    doc_lines: list[dict] = []

    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for raw_line in fh:
            # ``gzip.open`` in text mode hands us decoded strings, but
            # the checksum was computed over utf-8 bytes *with* the
            # trailing newline, so we re-encode here to match.
            as_bytes = raw_line.encode("utf-8")
            try:
                obj = json.loads(raw_line)
            except json.JSONDecodeError as e:
                raise ValueError(f"invalid segment json in {path}: {e}") from e

            kind = obj.get("type")

            if header is None:
                # First line must be the header.
                if kind != "header":
                    raise ValueError(f"invalid segment header: {path}")
                # Header must carry the fields the orchestrator relies
                # on. ``min_doc_id`` / ``max_doc_id`` are allowed to be
                # ``None`` (empty segment) so we don't require them to
                # be truthy — just present in the dict.
                required = (
                    "segment_id",
                    "doc_count",
                    "term_count",
                    "created_at",
                )
                missing = [k for k in required if k not in obj]
                if missing:
                    raise ValueError(
                        f"invalid segment header: {path} missing {missing}"
                    )
                header = obj
                sha.update(as_bytes)
                continue

            if kind == "doc":
                doc_lines.append(obj)
                sha.update(as_bytes)
                continue

            if kind == "footer":
                footer = obj
                # Footer is NOT hashed — it carries the digest itself.
                break

            raise ValueError(f"invalid segment line type {kind!r} in {path}")

    if header is None:
        raise ValueError(f"invalid segment header: {path}")
    if footer is None:
        raise ValueError(f"segment missing footer: {path}")

    expected = footer.get("sha256")
    actual = sha.hexdigest()
    if expected != actual:
        raise ValueError(f"segment checksum mismatch: {path}")

    # Construct the shell. ``Segment.__init__`` stamps ``created_at``
    # to now; we override with the persisted value so merge ordering
    # stays stable across restarts.
    seg = Segment(segment_id=header["segment_id"])
    seg.created_at = float(header["created_at"])

    for doc in doc_lines:
        doc_id = int(doc["doc_id"])
        entry_dict = doc["entry"]
        terms = list(doc.get("terms", []))
        entry = LogEntry.model_validate(entry_dict)
        seg.add(doc_id, entry, terms)

    # If the header's min/max disagree with what we rebuilt, log and
    # prefer the header values. Tests only exercise the clean path.
    hdr_min = header.get("min_doc_id")
    hdr_max = header.get("max_doc_id")
    if hdr_min is not None and seg.min_doc_id != hdr_min:
        logger.warning(
            "segment %s: header min_doc_id=%s disagrees with rebuilt=%s; "
            "using header value",
            seg.segment_id,
            hdr_min,
            seg.min_doc_id,
        )
        seg.min_doc_id = int(hdr_min)
    if hdr_max is not None and seg.max_doc_id != hdr_max:
        logger.warning(
            "segment %s: header max_doc_id=%s disagrees with rebuilt=%s; "
            "using header value",
            seg.segment_id,
            hdr_max,
            seg.max_doc_id,
        )
        seg.max_doc_id = int(hdr_max)

    return seg


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

def list_segment_files(dir: Path) -> list[Path]:
    """Return the sorted list of finalised segment files in *dir*.

    Temp files (``.tmp-*``) are excluded by the glob pattern. A missing
    directory returns an empty list rather than raising — callers at
    startup time often hit this before anything has been flushed.
    """
    dir = Path(dir)
    if not dir.exists():
        return []
    # The glob pattern itself excludes anything not matching
    # ``seg-*.jsonl.gz`` — in particular, ``.tmp-*`` files.
    return sorted(dir.glob("seg-*.jsonl.gz"))


def delete_segments(paths: Iterable[Path]) -> None:
    """Delete each path if it exists. Missing files are ignored.

    Used by the merger to retire source segments after a successful
    merged write. ``missing_ok=True`` keeps the call idempotent.
    """
    for p in paths:
        Path(p).unlink(missing_ok=True)


def next_segment_id(dir: Path) -> str:
    """Return the next monotonic segment id based on the files in *dir*.

    Scans the directory for files matching ``seg-(\\d{6})\\.jsonl\\.gz``
    and returns ``seg-{max+1:06d}``. If *dir* is missing or contains no
    matching files, returns ``"seg-000001"``. Re-scanning on every call
    means the allocator survives process restarts without any extra
    bookkeeping.
    """
    dir = Path(dir)
    if not dir.exists():
        return "seg-000001"

    max_seq = 0
    for entry in dir.iterdir():
        match = _SEGMENT_NAME_RE.fullmatch(entry.name)
        if match is None:
            continue
        seq = int(match.group(1))
        if seq > max_seq:
            max_seq = seq

    return f"seg-{(max_seq + 1):06d}"
