"""Opaque keyset cursors: ``(timestamp, id)`` <-> a single URL-safe string.

Pure functions over stdlib types. No session, no schema, no Strawberry — which is the point: the
encoding is the part of pagination that is fiddly and testable, and it does not belong inlined in a
resolver where the only way to exercise it is to stand up a database.

.. rubric:: What "opaque" buys, and why it is not decoration

A cursor is a **resume token**, not an identifier. Publishing ``id`` directly (or a bare offset)
invites clients to construct one, and the moment they do, the server can never change how it pages
without breaking them — the pagination key becomes part of the public contract by accident. Base64
over a versioned payload says "this is ours" loudly enough that nobody builds one by hand, and the
``logv1:`` scheme tag means a future encoding change is a clean rejection with a readable message
rather than a silently misinterpreted position.

.. rubric:: What it deliberately does NOT buy: integrity

There is no signature and no checksum. A cursor is not a capability — it carries no authority, it
selects nothing the same client could not select with ``startTime``, and it is not a secret. So a
tampered cursor produces a *different valid position*, not an error, and that is fine: the worst
outcome is a client paging from somewhere unexpected in a result set it was already allowed to
read. Adding an HMAC would mean a key to distribute, rotate and break pagination with; the
threat it defends against does not exist here. This paragraph is here so the absence reads as a
decision rather than as an oversight.

The corollary matters for tests: *truncating* a cursor does not reliably raise. Chopping the tail
of the base64 chops the tail of the payload, and a payload that lost the last digit of its id is
still a well-formed cursor pointing somewhere else. What is reliably rejected is anything that
fails to be base64, fails to be UTF-8, or fails to have the shape ``scheme|timestamp|id`` — which
covers every hand-typed or corrupted-in-transit value the server actually sees.

.. rubric:: Encoding

::

    "logv1|2026-07-25T11:59:59.123456+00:00|1200"  ->  urlsafe base64, padding stripped

``|`` is the separator because it cannot occur in an ISO-8601 timestamp or in a decimal integer, so
splitting can never be ambiguous. Padding is stripped so the result matches ``[A-Za-z0-9_-]+`` and
survives a query string, a cookie or a JSON body untouched; :func:`decode_cursor` restores it,
which is unambiguous because base64 length modulo 4 determines the padding exactly.
"""

from __future__ import annotations

import base64
import binascii
import re
from datetime import datetime

from src.db.repository import as_utc

#: Version tag carried inside every cursor. Bump it when the payload layout changes: an old cursor
#: then fails the scheme check and the client is told to restart pagination, instead of having its
#: position silently misread by a decoder that happens to still parse the old shape.
CURSOR_SCHEME = "logv1"

#: Payload field separator. Safe because neither component can contain it — see the module
#: docstring. Not configurable: it is part of the wire format, and a configurable wire format is a
#: wire format that differs between two processes serving the same clients.
CURSOR_SEPARATOR = "|"

#: The id must be a run of ASCII digits and nothing else.
#:
#: ``int()`` is far more permissive than it looks: it accepts surrounding whitespace, a leading
#: sign, PEP 515 underscores (``int("1_2") == 12``) and non-ASCII decimal digits. Every one of
#: those would let two textually different cursors decode to the same position, which is precisely
#: the ambiguity an opaque token exists to prevent.
_LOG_ID_PATTERN = re.compile(r"[0-9]+")


class InvalidCursorError(ValueError):
    """A cursor could not be decoded.

    Subclasses :class:`ValueError` rather than :class:`graphql.GraphQLError` on purpose: this
    module stays free of the GraphQL layer so it can be unit tested as pure logic. The resolver in
    :mod:`src.graphql.query` catches it and re-raises a ``GraphQLError``, which is what turns a
    bad cursor into a clean ``errors`` envelope instead of an unhandled exception.
    """


def encode_cursor(timestamp: datetime, log_id: int) -> str:
    """Encode the ``(timestamp, id)`` keyset position of one row into an opaque cursor.

    Args:
        timestamp: The row's ``timestamp``. Normalised to UTC by
            :func:`src.db.repository.as_utc`, so a naive value and the same instant tagged UTC
            encode identically — the same rule the repository applies to filter bounds, reused
            rather than restated so the two cannot drift.
        log_id: The row's primary key. The tiebreak half of the total order; without it two rows
            sharing an instant make the cursor ambiguous and a page can repeat or skip one.

    Returns:
        A string matching ``[A-Za-z0-9_-]+``.
    """
    normalised = as_utc(timestamp)
    if normalised is None:  # pragma: no cover - `as_utc` only returns None for a None input
        raise ValueError("cannot encode a cursor for a null timestamp")

    payload = CURSOR_SEPARATOR.join((CURSOR_SCHEME, normalised.isoformat(), str(int(log_id))))
    encoded = base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")
    return encoded.rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, int]:
    """Decode a cursor back into ``(timestamp, id)``, or raise :class:`InvalidCursorError`.

    Every failure mode below raises the same exception type with a message that names what was
    wrong, because the caller's only sensible response to any of them is identical: reject the
    request with a readable GraphQL error. What must **not** happen is an unhandled
    ``binascii.Error`` or ``UnicodeDecodeError`` escaping into the executor, where it becomes an
    internal error and — with C4's ``MaskErrors`` installed — a masked one, so the client is told
    "something went wrong" about a request it could have fixed itself.

    Raises:
        InvalidCursorError: The value is empty, is not URL-safe base64, does not decode as UTF-8,
            does not carry the expected ``scheme|timestamp|id`` shape, carries an unknown scheme,
            has a timestamp that is not ISO-8601 or is missing its UTC offset, or has an id that
            is not a plain run of digits.
    """
    if not isinstance(cursor, str) or not cursor:
        raise InvalidCursorError("cursor must be a non-empty string")

    # Restore the padding stripped by `encode_cursor`. Unambiguous: base64 encodes 3 bytes into
    # 4 characters, so the number of `=` needed is fully determined by the length modulo 4.
    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        # `validate=True` matters. WITHOUT it, `b64decode` silently DISCARDS every character
        # outside the alphabet, so "not a cursor!" would be decoded as "notacursor" and fail much
        # later (or, with unlucky input, not at all). `altchars` translates the URL-safe alphabet
        # back before validation, so `-` and `_` are accepted and everything else is not.
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (binascii.Error, ValueError) as exc:
        raise InvalidCursorError(f"cursor is not valid base64: {cursor!r}") from exc

    try:
        payload = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidCursorError(f"cursor does not decode as UTF-8 text: {cursor!r}") from exc

    parts = payload.split(CURSOR_SEPARATOR)
    if len(parts) != 3:
        raise InvalidCursorError(
            f"cursor payload must have the shape scheme{CURSOR_SEPARATOR}timestamp"
            f"{CURSOR_SEPARATOR}id, got {len(parts)} field(s)"
        )

    scheme, raw_timestamp, raw_log_id = parts
    if scheme != CURSOR_SCHEME:
        raise InvalidCursorError(
            f"unknown cursor scheme {scheme!r} (this server issues {CURSOR_SCHEME!r}); "
            "restart pagination without an `after` argument"
        )

    if not _LOG_ID_PATTERN.fullmatch(raw_log_id):
        raise InvalidCursorError(f"cursor id must be a run of digits, got {raw_log_id!r}")

    try:
        timestamp = datetime.fromisoformat(raw_timestamp)
    except ValueError as exc:
        raise InvalidCursorError(
            f"cursor timestamp is not ISO-8601: {raw_timestamp!r}"
        ) from exc

    if timestamp.tzinfo is None:
        # `encode_cursor` always writes an offset, so a naive timestamp means the payload was
        # rewritten by something other than this module. Rejecting it is what stops a hand-made
        # cursor from being compared against a `timestamptz` column under the server's local
        # TimeZone setting — the same class of bug `as_utc` exists to prevent on the filter path.
        raise InvalidCursorError(
            f"cursor timestamp is missing its UTC offset: {raw_timestamp!r}"
        )

    normalised = as_utc(timestamp)
    assert normalised is not None  # noqa: S101 - `timestamp` is not None, so neither is this
    return normalised, int(raw_log_id)
