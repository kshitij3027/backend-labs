"""The positional-ordering contract, generalised — C11's half of spec §2 item 29 / §3 Area D.

C5 proved the contract for one edge (``trace_id -> log entries``). C11 has ten loaders across four
tables, and they all share two pure functions: :func:`~src.graphql.loaders.group_rows_by_key` and
:func:`~src.graphql.loaders.align_rows_by_key`. This module is what those two are graded against.

.. rubric:: WHY THIS IS A UNIT TEST AT ALL, AND WHY IT IS THE ONE THAT MATTERS MOST

A load function is handed ``keys`` and must return a sequence **of the same length, in the same
order**. Nothing checks that beyond the length — Strawberry raises only for a length mismatch — so a
function returning the right number of results in the *wrong order* is accepted silently and hands
every parent somebody else's rows. The response is well formed, the counts are plausible, and an
assertion like "every payment in ``order.payments`` has an orderId" passes against it.

The three cases that produce that failure are a **shuffled** batch, a batch containing **misses**,
and a batch containing **duplicates**. A GraphQL document cannot ask for any of them on purpose —
it cannot request a key that does not exist, and Strawberry's per-key cache normally collapses
duplicates before the load function sees them — which is exactly why they are exercised here, on
the pure functions, with no database in the way.

The integration suite then proves the same contract against what PostgreSQL really returns,
ordering and all. Both are needed: this one can construct the awkward batch, that one can prove the
rows are real.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.db.models import OrderEventORM, PaymentEventORM, UserEventORM
from src.graphql.ecommerce import OrderEvent, PaymentEvent, UserEvent
from src.graphql.loaders import align_rows_by_key, group_rows_by_key

ANCHOR = datetime(2026, 7, 30, 9, 0, 0, tzinfo=timezone.utc)


def _order(row_id: int, order_id: str, user_id: str = "u-1001", trace: str | None = "t-1") -> OrderEventORM:
    """One order-event row, newest-first-friendly: a larger id is a later instant."""
    return OrderEventORM(
        id=row_id,
        timestamp=ANCHOR + timedelta(seconds=row_id),
        service="order-service",
        level="INFO",
        trace_id=trace,
        order_id=order_id,
        user_id=user_id,
        status="CREATED",
        metadata_=None,
    )


def _payment(row_id: int, order_id: str, trace: str | None = "t-1") -> PaymentEventORM:
    return PaymentEventORM(
        id=row_id,
        timestamp=ANCHOR + timedelta(seconds=row_id),
        service="payment-service",
        level="INFO",
        trace_id=trace,
        order_id=order_id,
        method="CARD",
        outcome="AUTHORIZED",
        metadata_=None,
    )


def _user(row_id: int, user_id: str, trace: str | None = "t-1") -> UserEventORM:
    return UserEventORM(
        id=row_id,
        timestamp=ANCHOR + timedelta(seconds=row_id),
        service="user-service",
        level="INFO",
        trace_id=trace,
        user_id=user_id,
        activity_type="LOGIN",
        metadata_=None,
    )


# --- group_rows_by_key: the list-shaped edges ------------------------------------------------------


def test_groups_come_back_in_key_order_not_in_row_order() -> None:
    """The result is aligned to ``keys``, whatever order the database returned the rows in.

    The rows below arrive interleaved and with the *second* key's row first, which is exactly what a
    single ``ORDER BY timestamp DESC`` over a multi-key ``IN`` produces. A function that bucketed
    correctly but returned ``list(grouped.values())`` would pass a length check, pass a "the right
    rows came back" check, and hand each parent the wrong bucket.
    """
    rows = [_payment(3, "ord-b"), _payment(2, "ord-a"), _payment(1, "ord-b")]

    groups = group_rows_by_key(
        rows, ["ord-a", "ord-b"], lambda row: row.order_id, PaymentEvent.from_orm
    )

    assert [[event.id for event in group] for group in groups] == [["2"], ["3", "1"]]


def test_a_key_with_no_rows_gets_an_empty_list_at_its_own_position() -> None:
    """Absence is an ordinary answer and it must not shift the other keys along.

    This is the case a GraphQL document cannot construct — every key a resolver loads came off a row
    that exists — and it is the one that shifts every later group by one when it is mishandled.
    """
    rows = [_user(5, "u-2087")]

    groups = group_rows_by_key(
        rows, ["u-1001", "u-2087", "u-9999"], lambda row: row.user_id, UserEvent.from_orm
    )

    assert groups[0] == []
    assert [event.id for event in groups[1]] == ["5"]
    assert groups[2] == []


def test_a_repeated_key_is_answered_identically_at_both_positions() -> None:
    """Two parents sharing a key both get the group, and neither gets the other's.

    Belt and braces against Strawberry's per-key cache, which normally collapses duplicates before
    the load function sees them — but a function that assumed unique keys would fail the day the
    cache is turned off, and ``PaymentEvent.order`` genuinely does load one ``order_id`` once per
    payment event of that order.
    """
    rows = [_order(9, "ord-a"), _order(4, "ord-b")]

    groups = group_rows_by_key(
        rows, ["ord-a", "ord-b", "ord-a"], lambda row: row.order_id, OrderEvent.from_orm
    )

    assert groups[0] == groups[2]
    assert groups[0] != groups[1]
    assert [event.id for event in groups[1]] == ["4"]


def test_a_row_whose_key_is_none_is_skipped_rather_than_bucketed() -> None:
    """An untraced event belongs to no correlation group, and must not join an arbitrary one.

    ``trace_id`` is nullable on all four tables (~40% of the log corpus carries none), so this is
    not a defensive branch — it is the ordinary case for the by-trace loaders. Without the guard a
    ``None`` key would look up ``grouped.get(None)``, which is ``None`` today and would become a
    real bucket the moment anybody loaded ``None`` as a key.
    """
    rows = [_order(1, "ord-a", trace=None), _order(2, "ord-b", trace="t-7")]

    groups = group_rows_by_key(
        rows, ["t-7"], lambda row: row.trace_id, OrderEvent.from_orm
    )

    assert [event.id for event in groups[0]] == ["2"]


def test_rows_outside_the_requested_keys_are_ignored_rather_than_fatal() -> None:
    """The statement cannot produce them; dropping one beats failing a whole operation if it does."""
    rows = [_payment(1, "ord-a"), _payment(2, "ord-elsewhere")]

    groups = group_rows_by_key(
        rows, ["ord-a"], lambda row: row.order_id, PaymentEvent.from_orm
    )

    assert [event.id for event in groups[0]] == ["1"]


def test_each_group_is_capped_independently_at_max_per_key() -> None:
    """``max_per_key`` bounds each bucket, not the batch — spec §2 item 22 on a nested list.

    Independence is the part worth testing: a cap applied to the flat row list instead would let one
    hot key (a user with thousands of events) consume the whole allowance and leave a quieter key in
    the same batch wrongly empty. The rows arrive newest first, so the cap keeps the newest.
    """
    rows = [_user(index, "u-hot") for index in (9, 8, 7, 6)] + [_user(1, "u-quiet")]

    groups = group_rows_by_key(
        rows,
        ["u-hot", "u-quiet"],
        lambda row: row.user_id,
        UserEvent.from_orm,
        max_per_key=2,
    )

    assert [event.id for event in groups[0]] == ["9", "8"]
    assert [event.id for event in groups[1]] == ["1"]


def test_an_empty_batch_produces_an_empty_result() -> None:
    """No keys, no groups — and no exception on the way there."""
    assert group_rows_by_key([_order(1, "ord-a")], [], lambda row: row.order_id, OrderEvent.from_orm) == []


def test_the_projection_is_the_published_type_and_not_the_row() -> None:
    """The loader hands resolvers published objects, projected while the rows are still attached.

    Asserted on a real field rather than on ``isinstance`` alone: ``id`` is a ``strawberry.ID``
    (a string) on the published type and an ``int`` on the row, so a function that forgot to project
    would be caught here rather than at serialisation time, mid-response.
    """
    projected = group_rows_by_key(
        [_order(42, "ord-a")], ["ord-a"], lambda row: row.order_id, OrderEvent.from_orm
    )[0][0]

    assert isinstance(projected, OrderEvent)
    assert projected.id == "42"
    assert projected.order_id == "ord-a"
    assert projected.status.value == "CREATED"


# --- align_rows_by_key: the single-valued edges ----------------------------------------------------


def test_values_are_aligned_to_the_ids_that_were_asked_for() -> None:
    """``result[i]`` answers ``keys[i]``, and the rows arrive in neither that order nor any order."""
    rows = [_payment(30, "ord-c"), _payment(10, "ord-a"), _payment(20, "ord-b")]

    aligned = align_rows_by_key(rows, [20, 10, 30], lambda row: row.id, PaymentEvent.from_orm)

    assert [event.id for event in aligned] == ["20", "10", "30"]


def test_an_absent_id_is_none_at_its_own_position() -> None:
    """``null`` for a miss, with no ``errors`` entry — and without shifting its neighbours."""
    aligned = align_rows_by_key(
        [_user(2, "u-1001")], [1, 2, 3], lambda row: row.id, UserEvent.from_orm
    )

    assert aligned[0] is None
    assert aligned[1] is not None
    assert aligned[2] is None


def test_a_repeated_id_is_answered_at_both_positions() -> None:
    """Two aliases naming one row both get it; the miss between them stays a miss."""
    aligned = align_rows_by_key(
        [_order(4, "ord-a")], [4, 9, 4], lambda row: row.id, OrderEvent.from_orm
    )

    assert aligned[0] is not None
    assert aligned[1] is None
    assert aligned[2] is not None
    assert aligned[0].id == aligned[2].id == "4"


@pytest.mark.parametrize(
    ("make_row", "project", "expected_type"),
    [
        (lambda: _order(1, "ord-a"), OrderEvent.from_orm, OrderEvent),
        (lambda: _payment(1, "ord-a"), PaymentEvent.from_orm, PaymentEvent),
        (lambda: _user(1, "u-1001"), UserEvent.from_orm, UserEvent),
    ],
)
def test_every_event_type_projects_through_the_same_alignment(
    make_row: object, project: object, expected_type: type
) -> None:
    """One contract, three tables — which is the whole reason these functions are generic.

    Three copies of the alignment loop would be three chances to write ``grouped[key]`` where
    ``grouped.get(key)`` was meant, or to drop the ``None``-key guard in exactly one of them. This
    parameterisation is what says all three edges really do go through the one implementation.
    """
    row = make_row()  # type: ignore[operator]

    aligned = align_rows_by_key([row], [1], lambda item: item.id, project)  # type: ignore[arg-type]

    assert isinstance(aligned[0], expected_type)
