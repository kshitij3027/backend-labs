"""Unit tests for :mod:`src.graphql.cursor` — the keyset cursor encoding.

Pure functions, so no database and no schema. This is the part of pagination where a defect is
both easy to introduce and hard to see from the outside: a cursor that decodes to *slightly* the
wrong position produces pages that are individually plausible and collectively wrong, and a decoder
that raises the wrong exception type turns a client's typo into an internal server error.

The paging behaviour those cursors drive — every row exactly once, no duplicates, no skips — is
proved against the real database in ``tests/integration/test_graphql_query.py``. Neither suite is
sufficient alone: this one cannot tell you the SQL resumes in the right place, and that one cannot
tell you *why* a cursor was rejected.
"""

from __future__ import annotations

import base64
import re
from datetime import datetime, timedelta, timezone

import pytest

from src.graphql.cursor import (
    CURSOR_SCHEME,
    InvalidCursorError,
    decode_cursor,
    encode_cursor,
)

#: A timestamp with microseconds and a non-zero offset is not decoration: microseconds are where a
#: lossy encoding loses precision (and two rows a microsecond apart are exactly the case the
#: tiebreak exists for), and a non-UTC offset is where a decoder that ignores timezones silently
#: shifts the position by hours.
MOMENT = datetime(2026, 7, 25, 11, 59, 59, 123456, tzinfo=timezone.utc)


def _url_safe(value: str) -> bool:
    """True when ``value`` needs no escaping in a URL, a cookie or a JSON string."""
    return re.fullmatch(r"[A-Za-z0-9_-]+", value) is not None


# --- Round trips ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "log_id",
    [
        1,
        42,
        1_200,
        9_223_372_036_854_775_807,  # the BIGSERIAL ceiling; a 32-bit encoding would truncate it
    ],
)
def test_a_cursor_round_trips_the_exact_position(log_id: int) -> None:
    """Decode(encode(x)) is x, to the microsecond and to the last digit of the id."""
    timestamp, decoded_id = decode_cursor(encode_cursor(MOMENT, log_id))

    assert timestamp == MOMENT
    assert timestamp.utcoffset() == timedelta(0)
    assert decoded_id == log_id


def test_microsecond_neighbours_encode_to_different_cursors() -> None:
    """One microsecond apart is a different position, not a rounding artefact.

    An encoding that dropped sub-second precision would still round-trip "a" timestamp, and pages
    would still look right — until two rows landed in the same second, which under ``createLog``
    load is routine. Then the cursor resumes at the wrong one and a row is repeated or skipped.
    """
    earlier = encode_cursor(MOMENT, 10)
    later = encode_cursor(MOMENT + timedelta(microseconds=1), 10)

    assert earlier != later
    assert decode_cursor(later)[0] - decode_cursor(earlier)[0] == timedelta(microseconds=1)


def test_the_id_is_part_of_the_position_not_only_the_timestamp() -> None:
    """Two rows sharing an instant produce different cursors — that is the whole tiebreak."""
    assert encode_cursor(MOMENT, 10) != encode_cursor(MOMENT, 11)


def test_a_naive_timestamp_encodes_as_the_same_utc_instant() -> None:
    """Naive means UTC, the same rule the repository applies to filter bounds.

    Reused from :func:`src.db.repository.as_utc` rather than restated, so the cursor and the
    ``WHERE`` clause it feeds can never disagree about what a bare wall-clock value means.
    """
    naive = MOMENT.replace(tzinfo=None)

    assert encode_cursor(naive, 7) == encode_cursor(MOMENT, 7)


def test_an_offset_timestamp_is_normalised_to_the_same_instant() -> None:
    """The same instant written in another zone is the same position, not a different one."""
    in_tokyo = MOMENT.astimezone(timezone(timedelta(hours=9)))

    assert in_tokyo != MOMENT.replace(tzinfo=None)  # different wall clock...
    assert encode_cursor(in_tokyo, 7) == encode_cursor(MOMENT, 7)  # ...same instant, same cursor


# --- Opacity and shape ----------------------------------------------------------------------------


def test_a_cursor_is_opaque_rather_than_a_readable_position() -> None:
    """Neither the id nor the timestamp is legible in the cursor.

    Opacity is not obfuscation-for-its-own-sake: the moment a client can read (and therefore
    construct) a cursor, the pagination key becomes part of the public contract by accident and can
    never change. A cursor that *was* the id would pass every round-trip test above.
    """
    cursor = encode_cursor(MOMENT, 1200)

    assert cursor != "1200"
    assert not cursor.isdigit()
    assert "1200" not in cursor
    assert MOMENT.isoformat() not in cursor
    assert CURSOR_SCHEME not in cursor


def test_a_cursor_needs_no_escaping_anywhere_it_travels() -> None:
    """URL-safe alphabet, padding stripped: ``[A-Za-z0-9_-]+`` and nothing else.

    Cursors travel in JSON variables today and could travel in a query string tomorrow. The
    standard base64 alphabet contains ``+`` and ``/`` (which a query string re-encodes) and ``=``
    padding (which a naive parser splits on), so the URL-safe alphabet is used and the padding is
    stripped. This is asserted over ids of every length modulo 3, because that is what changes how
    much padding the encoder would otherwise emit.
    """
    for log_id in range(1, 40):
        cursor = encode_cursor(MOMENT, log_id)
        assert _url_safe(cursor), f"cursor for id={log_id} is not URL-safe: {cursor!r}"
        assert decode_cursor(cursor)[1] == log_id


def test_the_payload_survives_a_timestamp_full_of_characters_that_need_escaping() -> None:
    """The ISO timestamp carries ``:``, ``-``, ``.`` and ``+`` — none of which reach the wire.

    ``+00:00`` is the interesting part: in a query string ``+`` means a space, so an encoding that
    passed the timestamp through unescaped would decode to ``00:00`` preceded by a space and fail
    (or, worse, parse as a different offset).
    """
    cursor = encode_cursor(MOMENT, 5)
    payload = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_").decode("utf-8")

    assert "+00:00" in payload, "the payload really does contain the characters under test"
    assert _url_safe(cursor)
    assert decode_cursor(cursor) == (MOMENT, 5)


# --- Rejection ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    ["", "not a cursor!", "%%%", "1200; DROP TABLE log_entries"],
    ids=["empty", "outside-the-alphabet", "percent-signs", "sql-ish-string"],
)
def test_input_that_is_not_base64_is_rejected(value: str) -> None:
    """Anything outside the URL-safe alphabet raises :class:`InvalidCursorError`.

    ``base64.b64decode`` **silently discards** characters outside the alphabet unless
    ``validate=True`` is passed, so without it ``"not a cursor!"`` would decode as ``"notacursor"``
    and fail somewhere further downstream — or, with unlucky input, not fail at all. That is the
    defect this parametrisation exists for.
    """
    with pytest.raises(InvalidCursorError):
        decode_cursor(value)


def test_valid_base64_carrying_the_wrong_payload_is_rejected() -> None:
    """Decodable bytes are not enough; the payload must have the ``scheme|timestamp|id`` shape."""
    encoded = base64.urlsafe_b64encode(b"hello world").decode("ascii").rstrip("=")

    with pytest.raises(InvalidCursorError, match="shape"):
        decode_cursor(encoded)


def test_a_cursor_from_another_scheme_is_rejected_with_a_usable_message() -> None:
    """A future encoding change must reject old cursors loudly, not misread them.

    That is the entire reason the scheme tag is in the payload: without it, a v2 decoder handed a
    v1 cursor would parse whatever it could and resume from a position nobody chose.
    """
    encoded = base64.urlsafe_b64encode(
        f"logv99|{MOMENT.isoformat()}|1200".encode()
    ).decode("ascii").rstrip("=")

    with pytest.raises(InvalidCursorError, match="unknown cursor scheme"):
        decode_cursor(encoded)


def test_a_truncated_cursor_that_loses_a_payload_field_is_rejected() -> None:
    """Halving a cursor destroys both the id and part of the timestamp, and it is refused.

    Which guard fires (base64 length, UTF-8, or the ``scheme|timestamp|id`` shape) depends on where
    the cut lands, and the test deliberately does not pin that — what matters is that all three
    funnel into the one exception type the resolver knows how to convert into a clean error.

    Note what this test does **not** claim. Cursors are unsigned, so truncation is not detectable
    in general — chopping the last character removes the last digit of the id and leaves a
    perfectly well-formed cursor pointing somewhere else. That is an accepted property (see the
    module docstring of :mod:`src.graphql.cursor`): a cursor carries no authority and selects
    nothing the same client could not select with ``startTime``. What must never happen is a
    *crash*, and every malformed shape raises the one exception the resolver knows how to convert.
    """
    cursor = encode_cursor(MOMENT, 1200)

    with pytest.raises(InvalidCursorError):
        decode_cursor(cursor[: len(cursor) // 2])


def test_a_payload_with_a_non_numeric_id_is_rejected() -> None:
    """``int()`` accepts whitespace, signs and PEP 515 underscores; the cursor format does not.

    Without the explicit digit check, ``1_200``, ``+1200`` and ``" 1200 "`` would all decode to the
    same position as ``1200`` — several textually different cursors naming one row, which is
    exactly the ambiguity an opaque token exists to remove.
    """
    for bogus_id in ("1_200", "+1200", " 1200", "12.0", "١٢٠٠"):
        encoded = base64.urlsafe_b64encode(
            f"{CURSOR_SCHEME}|{MOMENT.isoformat()}|{bogus_id}".encode()
        ).decode("ascii").rstrip("=")

        with pytest.raises(InvalidCursorError, match="run of digits"):
            decode_cursor(encoded)


def test_a_payload_whose_timestamp_lost_its_offset_is_rejected() -> None:
    """A naive timestamp in a payload means somebody else wrote it, and it is ambiguous.

    Accepting it would put a wall-clock value into a comparison against a ``timestamptz`` column,
    where PostgreSQL would interpret it in the *server's* TimeZone setting — the same class of bug
    :func:`src.db.repository.as_utc` exists to close on the filter path.
    """
    encoded = base64.urlsafe_b64encode(
        f"{CURSOR_SCHEME}|{MOMENT.replace(tzinfo=None).isoformat()}|1200".encode()
    ).decode("ascii").rstrip("=")

    with pytest.raises(InvalidCursorError, match="UTC offset"):
        decode_cursor(encoded)


def test_a_payload_whose_timestamp_is_not_a_timestamp_is_rejected() -> None:
    """Shape alone is not enough — the middle field has to parse as ISO-8601."""
    encoded = base64.urlsafe_b64encode(
        f"{CURSOR_SCHEME}|yesterday afternoon|1200".encode()
    ).decode("ascii").rstrip("=")

    with pytest.raises(InvalidCursorError, match="ISO-8601"):
        decode_cursor(encoded)


def test_invalid_cursor_error_is_a_value_error() -> None:
    """So a caller that has not been taught about cursors still catches it as a bad input.

    It is deliberately **not** a ``GraphQLError``: this module stays free of the GraphQL layer so
    it can be tested as pure logic, and :mod:`src.graphql.query` converts at the boundary.
    """
    assert issubclass(InvalidCursorError, ValueError)
