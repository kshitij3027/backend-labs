"""Unit tests for the cursor codec and for seq-anchored pagination as a whole.

The README makes one promise about ``GET /logs`` above all others: *"the cursor encodes the
last-seen entry's position, so concurrent appends can't cause skips or duplicates"*. It also
makes one honest admission: offset paging *"drifts under concurrent writes"*. Both are asserted
here as executable claims rather than left as prose — see
:func:`test_cursor_walk_stable_under_concurrent_append` and
:func:`test_offset_pagination_drifts_under_append_but_cursor_does_not`.

:func:`walk` reproduces the loop C5's route will run — decode the incoming cursor, scan, mint the
next one — so these tests exercise the codec on every page rather than calling ``scan`` with a
raw integer anchor. A bug in ``encode_cursor``/``decode_cursor`` fails these tests, not only an
integration test written later.

Corpora are built inline from :class:`~src.models.LogEntry`; ``src.generators`` is deliberately
not imported (a pagination test must fail when pagination is wrong, never when the corpus
generator changes).
"""

from __future__ import annotations

import base64
import json
from collections import Counter
from datetime import UTC, datetime, timedelta

import pytest

from src.models import LogEntry, SortOrder
from src.store import (
    CURSOR_PREFIX,
    CursorState,
    Filter,
    InvalidCursor,
    LogStore,
    ScanResult,
    StoredEntry,
    decode_cursor,
    encode_cursor,
)

BASE_TS = datetime(2026, 7, 27, 10, 0, 0, tzinfo=UTC)


def make_entry(i: int, *, entry_id: str | None = None, level: str = "INFO") -> LogEntry:
    """One deterministic entry. Ids are ordered so a failure names a place in the corpus."""
    return LogEntry(
        id=entry_id if entry_id is not None else f"e{i:06d}",
        ts=BASE_TS + timedelta(seconds=i),
        level=level,
        service="auth-svc",
        host="node-1",
        message=f"event {i}",
    )


def make_corpus(count: int, *, prefix: str = "e") -> list[LogEntry]:
    """``count`` entries with ids ``{prefix}NNNNNN`` — the prefix keeps two corpora distinct."""
    return [make_entry(i, entry_id=f"{prefix}{i:06d}") for i in range(count)]


def filled(capacity: int, entries: list[LogEntry]) -> LogStore:
    """A store of ``capacity`` with ``entries`` already appended, in order."""
    store = LogStore(capacity=capacity)
    store.append_many(entries)
    return store


def ids_of(records: list[StoredEntry]) -> list[str]:
    """The entry ids of a page, in page order."""
    return [record.entry.id for record in records]


def seqs_of(records: list[StoredEntry]) -> list[int]:
    """The seqs of a page, in page order."""
    return [record.seq for record in records]


def b64(payload: object) -> str:
    """Encode an arbitrary object as a cursor body, bypassing :func:`encode_cursor`.

    Used only to build the *malformed* cursors — the point is to feed the decoder payloads the
    encoder would never produce.
    """
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return CURSOR_PREFIX + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def walk(
    store: LogStore,
    flt: Filter,
    order: SortOrder,
    page_size: int,
    *,
    mutate_after: int | None = None,
    mutate: object = None,
) -> list[ScanResult]:
    """Page through the store exactly the way C5's ``GET /logs`` will.

    Every page after the first goes through :func:`encode_cursor` and :func:`decode_cursor`, so
    these tests cover the codec end-to-end instead of poking ``scan`` with a bare integer.

    Args:
        mutate_after: Run ``mutate`` once, immediately after this many pages have been served —
            i.e. *between* two pages of a walk, which is precisely where a concurrent append
            lands in production.
        mutate: Zero-argument callable applied to the store mid-walk.

    Returns:
        Every page produced, in order.
    """
    fingerprint = flt.fingerprint()
    total = store.count(flt)
    cursor: str | None = None
    pages: list[ScanResult] = []

    while True:
        anchor: int | None = None
        if cursor is not None:
            state = decode_cursor(
                cursor, expected_fingerprint=fingerprint, expected_order=order
            )
            anchor = state.seq
            assert state.total == total, "total is as-of-walk-start and must not be re-derived"

        page = store.scan(flt, order, limit=page_size, start_after_seq=anchor)
        pages.append(page)

        if mutate is not None and len(pages) == mutate_after:
            mutate()  # type: ignore[operator]

        if not page.has_more or page.next_seq is None:
            return pages

        cursor = encode_cursor(
            seq=page.next_seq, order=order, fingerprint=fingerprint, total=total
        )
        assert len(pages) < 10_000, "walk failed to terminate — cursor is not advancing"


def collected(pages: list[ScanResult]) -> list[str]:
    """Every id the walk returned, flattened in page order."""
    return [record.entry.id for page in pages for record in page.items]


# ---------------------------------------------------------------------------------------------
# codec
# ---------------------------------------------------------------------------------------------


def test_cursor_roundtrips() -> None:
    """Everything put into a cursor comes back out, unchanged and correctly typed."""
    fingerprint = Filter(levels=frozenset({"ERROR"})).fingerprint()

    cursor = encode_cursor(
        seq=4242, order=SortOrder.DESC, fingerprint=fingerprint, total=98765
    )
    state = decode_cursor(
        cursor, expected_fingerprint=fingerprint, expected_order=SortOrder.DESC
    )

    assert state == CursorState(
        seq=4242, order=SortOrder.DESC, fingerprint=fingerprint, total=98765
    )
    assert isinstance(state.seq, int)
    assert state.order is SortOrder.DESC


def test_cursor_has_b64_prefix() -> None:
    """The wire form is the README's ``"next_cursor": "b64:…"`` — prefixed and URL-clean.

    The prefix makes a cursor self-identifying: a client that pastes a leftover offset or a raw
    base64 blob into ``?cursor=`` gets a clean 400 instead of a plausible-looking wrong page.
    Padding is stripped so the value never contains ``=``, which is what invites double-encoding
    bugs in clients that assemble URLs by hand.
    """
    cursor = encode_cursor(seq=1, order=SortOrder.ASC, fingerprint="deadbeef", total=1)

    assert cursor.startswith("b64:")
    body = cursor[len(CURSOR_PREFIX) :]
    assert "=" not in body
    assert set(body) <= set(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    )


def test_fingerprint_is_stable_and_order_independent() -> None:
    """Two independently-built identical filters fingerprint identically; different ones do not.

    Set iteration order must not leak into the fingerprint, or a cursor minted from
    ``?level=ERROR&level=FATAL`` would sometimes fail to validate against the very same query.
    And the fingerprint cannot use Python's builtin ``hash()``: string hashing is salted per
    process, so a cursor would stop validating across a restart.
    """
    one = Filter(levels=frozenset({"ERROR", "FATAL"}), services=frozenset({"auth-svc"}))
    same = Filter(levels=frozenset({"FATAL", "ERROR"}), services=frozenset({"auth-svc"}))
    other = Filter(levels=frozenset({"ERROR"}), services=frozenset({"auth-svc"}))

    assert one.fingerprint() == same.fingerprint()
    assert one.fingerprint() != other.fingerprint()
    assert Filter().fingerprint() != other.fingerprint()
    assert len(one.fingerprint()) == 16, "blake2b(digest_size=8) rendered as hex"


def test_cursor_total_is_carried_through() -> None:
    """``total`` is as-of-walk-start: it survives the round trip and ignores later appends.

    Without this, ``page.total`` would climb during a walk and the client's progress bar would
    move backwards — the walk is over a snapshot the anchor already fixed.
    """
    store = filled(1000, make_corpus(50))
    flt = Filter()
    fingerprint = flt.fingerprint()
    total_at_start = store.count(flt)

    cursor = encode_cursor(
        seq=39, order=SortOrder.DESC, fingerprint=fingerprint, total=total_at_start
    )
    store.append_many(make_corpus(20, prefix="late"))
    state = decode_cursor(
        cursor, expected_fingerprint=fingerprint, expected_order=SortOrder.DESC
    )

    assert total_at_start == 50
    assert state.total == 50, "the cursor reports the walk's total, not the store's current one"
    assert store.count(flt) == 70


def test_cursor_rejected_when_fingerprint_differs() -> None:
    """A cursor from one filter replayed against another is an error, never a wrong answer.

    The anchor is a perfectly valid integer under the new filter, so without this check the
    store would serve a page that is internally consistent and completely wrong — the client
    would silently skip or repeat an arbitrary slice of the corpus and never learn.
    """
    errors = Filter(levels=frozenset({"ERROR"}))
    infos = Filter(levels=frozenset({"INFO"}))
    cursor = encode_cursor(
        seq=10, order=SortOrder.DESC, fingerprint=errors.fingerprint(), total=5
    )

    with pytest.raises(InvalidCursor, match="different filter"):
        decode_cursor(
            cursor,
            expected_fingerprint=infos.fingerprint(),
            expected_order=SortOrder.DESC,
        )

    # The same filter rebuilt from scratch still validates — the check is on identity, not on
    # object identity.
    rebuilt = Filter(levels=frozenset({"ERROR"}))
    assert (
        decode_cursor(
            cursor,
            expected_fingerprint=rebuilt.fingerprint(),
            expected_order=SortOrder.DESC,
        ).seq
        == 10
    )


def test_cursor_rejected_when_order_differs() -> None:
    """Flipping ``order`` mid-walk invalidates the cursor: the anchor means the opposite thing.

    ``seq < anchor`` and ``seq > anchor`` select disjoint halves of the corpus. Honouring a DESC
    cursor on an ASC request would hand back the entire region the client had already read.
    """
    fingerprint = Filter().fingerprint()
    cursor = encode_cursor(
        seq=10, order=SortOrder.DESC, fingerprint=fingerprint, total=5
    )

    with pytest.raises(InvalidCursor, match="other sort order"):
        decode_cursor(
            cursor, expected_fingerprint=fingerprint, expected_order=SortOrder.ASC
        )


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param("", id="empty-string"),
        pytest.param("b64:", id="prefix-with-no-payload"),
        pytest.param("200", id="an-offset-pasted-into-cursor"),
        pytest.param("eyJzIjoxfQ", id="valid-base64-but-no-prefix"),
        pytest.param("b64:!!!!", id="not-the-urlsafe-base64-alphabet"),
        pytest.param(
            "b64:" + base64.urlsafe_b64encode(b"not json at all").decode().rstrip("="),
            id="decodes-but-is-not-json",
        ),
        pytest.param(b64([1, 2, 3]), id="json-array-not-object"),
        pytest.param(b64("just a string"), id="json-string-not-object"),
        pytest.param(b64({"s": 1}), id="missing-order-fingerprint-total"),
        pytest.param(b64({"o": "desc", "f": "x", "t": 0}), id="missing-anchor"),
        pytest.param(b64({"s": "ten", "o": "desc", "f": "x", "t": 0}), id="anchor-not-an-int"),
        pytest.param(b64({"s": True, "o": "desc", "f": "x", "t": 0}), id="anchor-is-a-bool"),
        pytest.param(b64({"s": -1, "o": "desc", "f": "x", "t": 0}), id="negative-anchor"),
        pytest.param(b64({"s": 1, "o": "desc", "f": "x", "t": -3}), id="negative-total"),
        pytest.param(b64({"s": 1, "o": "desc", "f": "x", "t": "many"}), id="total-not-an-int"),
        pytest.param(b64({"s": 1, "o": "sideways", "f": "x", "t": 0}), id="unknown-sort-order"),
        pytest.param(b64({"s": 1, "o": 7, "f": "x", "t": 0}), id="order-not-a-string"),
        pytest.param(b64({"s": 1, "o": "desc", "f": 99, "t": 0}), id="fingerprint-not-a-string"),
    ],
)
def test_malformed_cursor_raises_invalid_cursor(raw: str) -> None:
    """Every way of getting a cursor wrong raises, so C5 has exactly one failure to map to 400.

    A decoder that fell back to "start from the beginning" on unparseable input would turn a
    client bug into an infinite pagination loop that never errors.
    """
    with pytest.raises(InvalidCursor):
        decode_cursor(raw, expected_fingerprint="x", expected_order=SortOrder.DESC)


def test_invalid_cursor_is_a_value_error() -> None:
    """:class:`InvalidCursor` subclasses ``ValueError`` so unaware callers still behave."""
    assert issubclass(InvalidCursor, ValueError)


# ---------------------------------------------------------------------------------------------
# walking the corpus
# ---------------------------------------------------------------------------------------------


def test_cursor_walk_covers_corpus_exactly_once() -> None:
    """A full DESC walk returns every entry once — no gaps, no repeats, no phantom last page.

    The page size deliberately does not divide the corpus size, so the walk ends on a partial
    page; ``237 / 25`` also makes an off-by-one in ``has_more`` show up as a missing or extra
    row rather than as a silent boundary case.
    """
    corpus = make_corpus(237)
    store = filled(1000, corpus)

    pages = walk(store, Filter(), SortOrder.DESC, 25)
    seen = collected(pages)

    assert Counter(seen) == Counter(entry.id for entry in corpus)
    assert len(seen) == 237
    assert len(pages) == 10, "9 full pages plus a partial one"
    assert seen == [entry.id for entry in reversed(corpus)], "strictly newest-first throughout"
    assert pages[-1].has_more is False
    assert pages[-1].next_seq is None
    assert all(page.truncated is False for page in pages)


def test_cursor_walk_asc_covers_corpus_exactly_once() -> None:
    """The same guarantee in the other direction: an ASC walk is oldest-first and complete."""
    corpus = make_corpus(237)
    store = filled(1000, corpus)

    pages = walk(store, Filter(), SortOrder.ASC, 25)
    seen = collected(pages)

    assert Counter(seen) == Counter(entry.id for entry in corpus)
    assert seen == [entry.id for entry in corpus], "strictly oldest-first throughout"
    assert pages[-1].has_more is False


def test_cursor_walk_with_filter_covers_matching_set_exactly_once() -> None:
    """A filtered walk covers the *match set* exactly once, through the index-hinted path.

    The unfiltered walks above exercise the linear scan. This corpus is deliberately selective
    enough (1 ERROR in 25) that the planner takes the index hint, so the bisect-and-merge over a
    secondary index — the path the common ``?level=ERROR&limit=50`` request follows — is walked
    end-to-end through the cursor codec rather than only unit-tested in isolation.
    """
    corpus = [
        make_entry(i, entry_id=f"e{i:06d}", level="ERROR" if i % 25 == 0 else "INFO")
        for i in range(2000)
    ]
    store = filled(5000, corpus)
    flt = Filter(levels=frozenset({"ERROR"}))
    expected = [entry.id for entry in reversed(corpus) if entry.level == "ERROR"]

    hint = flt.index_hint(store)
    assert hint is not None and sum(len(seqs) for seqs in hint) * 8 <= store.size(), (
        "corpus must be selective enough to take the index hint"
    )

    pages = walk(store, flt, SortOrder.DESC, 7)
    seen = collected(pages)

    assert seen == expected
    assert len(seen) == store.count(flt) == 80
    assert len(pages) == 12, "11 full pages of 7 plus a partial one"


def test_cursor_walk_stable_under_concurrent_append() -> None:
    """**The one that matters.** 500 appends mid-walk change nothing about what the walk returns.

    This is the README's headline promise made executable: *"the cursor encodes the last-seen
    entry's position, so concurrent appends can't cause skips or duplicates"*. The mechanism is
    invariant 1 — seqs only increase — so every entry appended during the walk gets a seq above
    every anchor the walk will ever use, which puts it structurally out of reach of a DESC page
    defined as ``seq < anchor``. Nothing can move an existing entry's seq, so nothing can slide
    across an anchor either.

    Both halves are asserted: each pre-existing id appears exactly once (no duplicates, no
    skips), and no id appended mid-walk leaks into the walk at all.
    """
    original = make_corpus(300, prefix="orig")
    late = make_corpus(500, prefix="late")
    store = filled(10_000, original)

    pages = walk(
        store,
        Filter(),
        SortOrder.DESC,
        25,
        mutate_after=4,
        mutate=lambda: store.append_many(late),
    )
    seen = collected(pages)

    assert Counter(seen) == Counter(entry.id for entry in original), (
        "every pre-existing entry exactly once — no skips, no duplicates"
    )
    assert len(seen) == 300
    assert not any(entry_id.startswith("late") for entry_id in seen), (
        "entries appended mid-walk land above the anchor and must stay invisible to it"
    )
    assert store.size() == 800, "the appends really did happen"
    assert seen == [entry.id for entry in reversed(original)]


def test_cursor_walk_stable_under_interleaved_appends_between_every_page() -> None:
    """Appending between *every* page, not just once, still yields the corpus exactly once."""
    original = make_corpus(120, prefix="orig")
    store = filled(10_000, original)
    flt = Filter()
    fingerprint = flt.fingerprint()
    total = store.count(flt)

    seen: list[str] = []
    cursor: str | None = None
    round_number = 0
    while True:
        anchor = (
            None
            if cursor is None
            else decode_cursor(
                cursor, expected_fingerprint=fingerprint, expected_order=SortOrder.DESC
            ).seq
        )
        page = store.scan(flt, SortOrder.DESC, limit=10, start_after_seq=anchor)
        seen.extend(ids_of(page.items))
        store.append_many(make_corpus(5, prefix=f"r{round_number}-"))
        round_number += 1
        if not page.has_more or page.next_seq is None:
            break
        cursor = encode_cursor(
            seq=page.next_seq, order=SortOrder.DESC, fingerprint=fingerprint, total=total
        )

    assert Counter(seen) == Counter(entry.id for entry in original)


# ---------------------------------------------------------------------------------------------
# eviction
# ---------------------------------------------------------------------------------------------


def test_cursor_anchor_evicted_resumes_and_flags_truncated() -> None:
    """A cursor whose anchor fell out of the ring is flagged, never raised and never silent.

    Silently returning fewer rows is the one behaviour that must not happen: a client walking a
    hot ring would conclude the corpus had ended when in fact it had lapped. So the store sets
    ``truncated`` — C5's ``X-Cursor-Truncated: true`` — and resumes as close to the anchor as the
    ring still allows.

    ``ASC`` resumes at the oldest resident record (the ``auto.offset.reset=earliest`` rule):
    real rows, and the flag says how many were missed. ``DESC`` has nowhere to resume *to* —
    it travels downward and everything below the anchor is exactly what was evicted — so the
    honest page is an empty, flagged, terminal one. Snapping the anchor **up** to the oldest
    resident record would emit a record the client has very likely already seen: a silent
    duplicate in place of a flagged empty page, which is the wrong trade for a paginator whose
    entire reason to exist is that it never duplicates.
    """
    flt = Filter()
    fingerprint = flt.fingerprint()

    # --- newest-first ------------------------------------------------------------------
    desc_store = filled(100, make_corpus(100, prefix="orig"))
    desc_first = desc_store.scan(flt, SortOrder.DESC, limit=10)
    assert desc_first.truncated is False
    desc_cursor = encode_cursor(
        seq=desc_first.next_seq,  # type: ignore[arg-type]
        order=SortOrder.DESC,
        fingerprint=fingerprint,
        total=100,
    )

    desc_store.append_many(make_corpus(200, prefix="late"))  # laps the ring twice over
    desc_anchor = decode_cursor(
        desc_cursor, expected_fingerprint=fingerprint, expected_order=SortOrder.DESC
    ).seq
    desc_second = desc_store.scan(
        flt, SortOrder.DESC, limit=10, start_after_seq=desc_anchor
    )

    assert desc_anchor < desc_store.oldest_seq()  # type: ignore[operator]
    assert desc_second.truncated is True, "the client must be told it fell off the ring"
    assert desc_second.has_more is False
    assert desc_second.next_seq is None
    desc_seen = ids_of(desc_first.items) + ids_of(desc_second.items)
    assert len(desc_seen) == len(set(desc_seen)), "resuming must never repeat a row"

    # --- oldest-first ------------------------------------------------------------------
    asc_store = filled(100, make_corpus(100, prefix="orig"))
    asc_first = asc_store.scan(flt, SortOrder.ASC, limit=10)
    asc_cursor = encode_cursor(
        seq=asc_first.next_seq,  # type: ignore[arg-type]
        order=SortOrder.ASC,
        fingerprint=fingerprint,
        total=100,
    )

    asc_store.append_many(make_corpus(200, prefix="late"))
    asc_anchor = decode_cursor(
        asc_cursor, expected_fingerprint=fingerprint, expected_order=SortOrder.ASC
    ).seq
    asc_second = asc_store.scan(flt, SortOrder.ASC, limit=10, start_after_seq=asc_anchor)

    assert asc_second.truncated is True
    assert seqs_of(asc_second.items) == list(range(200, 210)), "resumes at the oldest resident"
    assert asc_second.has_more is True
    asc_seen = ids_of(asc_first.items) + ids_of(asc_second.items)
    assert len(asc_seen) == len(set(asc_seen))


def test_resident_anchor_is_not_flagged_truncated() -> None:
    """Eviction that stays *below* the anchor is ordinary ring rotation, not truncation."""
    store = filled(100, make_corpus(100))
    first = store.scan(Filter(), SortOrder.DESC, limit=10)

    store.append_many(make_corpus(20, prefix="late"))  # resident: seq 20..119
    second = store.scan(Filter(), SortOrder.DESC, limit=10, start_after_seq=first.next_seq)

    assert store.oldest_seq() == 20
    assert first.next_seq == 90
    assert second.truncated is False
    assert seqs_of(second.items) == list(range(89, 79, -1))


# ---------------------------------------------------------------------------------------------
# the documented difference between the two paging styles
# ---------------------------------------------------------------------------------------------


def test_offset_pagination_drifts_under_append_but_cursor_does_not() -> None:
    """The README's honesty about offset paging, turned into an assertion.

    *"Offset … supported for human/ad-hoc use and for a 'jump to page N' UI, with the honest
    caveat that it drifts under concurrent writes."* Both stores below hold the same corpus and
    receive the same ten appends at the same point in the walk. The offset walk re-serves its
    entire first page, because ``skip=10`` counts from a head that moved; the cursor walk is
    anchored to a seq that cannot move, so it advances exactly as intended.

    This is not a bug being tolerated — it is inherent to counting from a moving end, which is
    why cursor is the default and offset is opt-in.
    """
    corpus = make_corpus(100, prefix="orig")
    late = make_corpus(10, prefix="late")

    # --- offset paging -----------------------------------------------------------------
    offset_store = filled(1000, corpus)
    offset_first = offset_store.scan(Filter(), SortOrder.DESC, limit=10, skip=0)
    offset_store.append_many(late)
    offset_second = offset_store.scan(Filter(), SortOrder.DESC, limit=10, skip=10)
    offset_seen = ids_of(offset_first.items) + ids_of(offset_second.items)

    # --- cursor paging, same corpus, same interleaving ---------------------------------
    cursor_store = filled(1000, corpus)
    flt = Filter()
    fingerprint = flt.fingerprint()
    cursor_first = cursor_store.scan(flt, SortOrder.DESC, limit=10)
    cursor = encode_cursor(
        seq=cursor_first.next_seq,  # type: ignore[arg-type]
        order=SortOrder.DESC,
        fingerprint=fingerprint,
        total=100,
    )
    cursor_store.append_many(late)
    anchor = decode_cursor(
        cursor, expected_fingerprint=fingerprint, expected_order=SortOrder.DESC
    ).seq
    cursor_second = cursor_store.scan(flt, SortOrder.DESC, limit=10, start_after_seq=anchor)
    cursor_seen = ids_of(cursor_first.items) + ids_of(cursor_second.items)

    assert ids_of(offset_second.items) == ids_of(offset_first.items), (
        "ten appends shifted the window by ten, so page 2 re-serves page 1 verbatim"
    )
    assert len(offset_seen) != len(set(offset_seen)), "offset paging duplicated rows"

    assert len(cursor_seen) == len(set(cursor_seen)), "cursor paging duplicated nothing"
    assert ids_of(cursor_second.items) == [entry.id for entry in reversed(corpus[80:90])]
