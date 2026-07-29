"""Unit tests for the C10 e-commerce corpus — :func:`src.generators.generate_event_corpus`.

The log corpus is an oracle because it is *pure*. This one has to be an oracle for a second reason
as well: it has to be **coherent**. C11's cross-entity traversals, C12's ``orderStatusStream`` and
C13's order board all assert things about the *shape* of an order's history — that its statuses
advance, that its payments belong to it, that the user who checked out is the user the order names
— and every one of those assertions is only falsifiable if the corpus really has that shape rather
than merely having rows in the right tables.

So this module pins two things:

* the same purity contract :mod:`tests.unit.test_generators` pins for the log corpus (private RNG,
  required ``end_time``, byte-identical output for identical arguments);
* the coherence properties nothing downstream can check for itself — one trace per order shared by
  all three streams, statuses drawn from a declared lifecycle rather than independently, payment
  outcomes consistent with the order's fate, and every declared roster value actually reachable.

A test that asserted only "some order events were generated" would pass against a generator that
emitted 120 unrelated rows per table, which is precisely the corpus that makes every downstream
join look correct while proving nothing.
"""

from __future__ import annotations

import inspect
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import pytest

from src.db.models import (
    ORDER_ID_MAX_LENGTH,
    ORDER_STATUS_MAX_LENGTH,
    PAYMENT_METHOD_MAX_LENGTH,
    PAYMENT_OUTCOME_MAX_LENGTH,
    TRACE_ID_MAX_LENGTH,
    USER_ACTIVITY_MAX_LENGTH,
    USER_ID_MAX_LENGTH,
)
from src.generators import (
    DEFAULT_WINDOW,
    LOG_LEVELS,
    ORDER_CHANNELS,
    ORDER_CLUSTER_MAX_SPAN,
    ORDER_EVENT_SERVICE,
    ORDER_LIFECYCLES,
    ORDER_STATUS_LEVELS,
    ORDER_STATUSES,
    PAYMENT_EVENT_SERVICE,
    PAYMENT_METHODS,
    PAYMENT_OUTCOME_LEVELS,
    PAYMENT_OUTCOMES,
    REGIONS,
    USER_ACTIVITIES,
    USER_ACTIVITY_LEVELS,
    USER_EVENT_SERVICE,
    USER_IDS,
    EventCorpus,
    generate_event_corpus,
    generate_log_records,
    order_id_for,
)

#: A fixed instant years from any plausible wall clock, so a generator that consulted
#: :func:`datetime.now` would fail by five years rather than by a rounding error.
ANCHOR = datetime(2026, 7, 25, 12, 0, 0, tzinfo=timezone.utc)

#: Orders for the statistical assertions. Seven lifecycle paths drawn uniformly, so 300 orders
#: expects ~43 of each — large enough that "every declared status appears" is a statement about the
#: generator rather than about luck, small enough that the module runs in well under a second.
SAMPLE_ORDERS = 300

SEED = 20260725


@pytest.fixture(scope="module")
def corpus() -> EventCorpus:
    """One corpus for the read-only assertions. Module-scoped because it is never mutated."""
    return generate_event_corpus(SAMPLE_ORDERS, seed=SEED, end_time=ANCHOR)


def _by_order(corpus: EventCorpus) -> dict[str, list[str]]:
    """``order_id -> its status sequence``, in timestamp order."""
    sequences: dict[str, list[str]] = defaultdict(list)
    for record in corpus.orders:
        sequences[record.order_id].append(record.status)
    return dict(sequences)


# --- Purity: the property the entire test strategy rests on ---------------------------------------


def test_same_arguments_produce_equal_corpora() -> None:
    """Same ``(count, seed, end_time, window)`` in, equal corpus out — the oracle guarantee.

    Value equality across every field of every record in all three streams, which is what the
    frozen record dataclasses buy. Had the generator emitted ORM instances this comparison would be
    identity-based and would pass for two corpora that shared nothing but their lengths.
    """
    first = generate_event_corpus(80, seed=SEED, end_time=ANCHOR)
    second = generate_event_corpus(80, seed=SEED, end_time=ANCHOR)

    assert first == second
    assert first is not second, "two calls must build two corpora, not memoise one"
    assert first.orders is not second.orders


def test_global_random_state_does_not_affect_the_corpus() -> None:
    """Reseeding the module-level :mod:`random` between calls changes nothing.

    The executable form of "never the global RNG". Any import in the process may call
    :func:`random.seed`; a generator that drew from shared module state would produce a different
    corpus depending on import order, and the flake would be blamed on whichever test ran first.
    """
    random.seed(1)
    first = generate_event_corpus(40, seed=SEED, end_time=ANCHOR)
    random.seed(999_999)
    second = generate_event_corpus(40, seed=SEED, end_time=ANCHOR)

    assert first == second


def test_the_two_corpora_do_not_share_a_random_stream() -> None:
    """Generating a log corpus first does not change the event corpus that follows it.

    ``SEED_ENTRIES`` and ``SEED_ORDERS`` are independent settings, and an operator who raises one
    must not silently rewrite the other's corpus. That only holds because each generator owns a
    private :class:`random.Random`; a shared module-level stream would couple them invisibly.
    """
    baseline = generate_event_corpus(40, seed=SEED, end_time=ANCHOR)
    generate_log_records(500, seed=SEED, end_time=ANCHOR)
    after = generate_event_corpus(40, seed=SEED, end_time=ANCHOR)

    assert baseline == after


def test_end_time_is_a_required_parameter() -> None:
    """``end_time`` has no default, so no code path can fall back to the wall clock.

    Structural rather than behavioural: "it produced the same corpus twice in a row" is weak
    evidence (two calls a millisecond apart would too). "There is no default" is a property a
    future edit cannot break silently.
    """
    parameter = inspect.signature(generate_event_corpus).parameters["end_time"]

    assert parameter.default is inspect.Parameter.empty


def test_a_different_seed_produces_a_different_corpus() -> None:
    """Otherwise the seed is decorative and every "deterministic" claim is vacuous."""
    first = generate_event_corpus(60, seed=SEED, end_time=ANCHOR)
    second = generate_event_corpus(60, seed=SEED + 1, end_time=ANCHOR)

    assert first != second


def test_a_different_end_time_translates_the_corpus() -> None:
    """Shifting ``end_time`` shifts every timestamp by the same delta and changes nothing else."""
    shift = timedelta(days=3)
    first = generate_event_corpus(40, seed=SEED, end_time=ANCHOR)
    second = generate_event_corpus(40, seed=SEED, end_time=ANCHOR + shift)

    assert [record.order_id for record in first.orders] == [
        record.order_id for record in second.orders
    ]
    assert [record.status for record in first.orders] == [
        record.status for record in second.orders
    ]
    assert [record.timestamp + shift for record in first.orders] == [
        record.timestamp for record in second.orders
    ]


# --- Time: containment and ordering ---------------------------------------------------------------


def test_every_event_lies_strictly_inside_the_window(corpus: EventCorpus) -> None:
    """No event escapes ``[end_time - window, end_time)``, in any of the three streams.

    This is what the shortened start window buys: an order's cluster of events extends past its
    start instant, so drawing starts from the full window would let the tail of a late order land
    after ``end_time`` — a row the E2E verifier's "newest entry" probe would then see as being in
    the future.
    """
    oldest = ANCHOR - DEFAULT_WINDOW
    timestamps = [
        record.timestamp
        for stream in (corpus.orders, corpus.payments, corpus.user_activity)
        for record in stream
    ]

    assert timestamps
    assert min(timestamps) >= oldest
    assert max(timestamps) < ANCHOR


def test_each_stream_is_sorted_oldest_first(corpus: EventCorpus) -> None:
    """Seeding inserts in list order, so list order has to be time order.

    That is what makes ``BIGSERIAL`` ids ascend with time, which is what makes the database's
    ``ORDER BY timestamp DESC, id DESC`` exactly ``reversed(...)`` of the generated list — the
    assumption every integration oracle in this project is written against.
    """
    for stream in (corpus.orders, corpus.payments, corpus.user_activity):
        timestamps = [record.timestamp for record in stream]
        assert timestamps == sorted(timestamps)


def test_one_orders_events_are_strictly_increasing(corpus: EventCorpus) -> None:
    """Within a single order, no two events share an instant and none goes backwards.

    Without this "the current status of order X" is undefined — two events at the same microsecond
    with different statuses would make the answer depend on which row the planner returned first.
    """
    per_order: dict[str, list[datetime]] = defaultdict(list)
    for record in corpus.orders:
        per_order[record.order_id].append(record.timestamp)

    for order_id, timestamps in per_order.items():
        assert timestamps == sorted(timestamps), order_id
        assert len(set(timestamps)) == len(timestamps), f"{order_id} has duplicate instants"


def test_an_orders_whole_timeline_fits_inside_the_declared_cluster_span(
    corpus: EventCorpus,
) -> None:
    """No order's events span more than ``ORDER_CLUSTER_MAX_SPAN``.

    The span bound is not decoration: the start window is shortened by exactly this much, so if a
    cluster could exceed it the containment guarantee above would be false by construction rather
    than by accident.
    """
    per_trace: dict[str, list[datetime]] = defaultdict(list)
    for stream in (corpus.orders, corpus.payments, corpus.user_activity):
        for record in stream:
            assert record.trace_id is not None
            per_trace[record.trace_id].append(record.timestamp)

    for trace_id, timestamps in per_trace.items():
        assert max(timestamps) - min(timestamps) <= ORDER_CLUSTER_MAX_SPAN, trace_id


# --- Coherence: the property this corpus exists for -----------------------------------------------


def test_every_order_follows_a_declared_lifecycle(corpus: EventCorpus) -> None:
    """An order's status sequence is one of :data:`ORDER_LIFECYCLES`, exactly.

    The single most important assertion in this module. Statuses drawn independently from the
    roster would produce orders that are DELIVERED before they are PAID — which does not merely
    look wrong, it makes every downstream assertion vacuous: "the newest status" would be
    meaningless, C12's replay would be nonsense, and a funnel chart would be noise.
    """
    sequences = _by_order(corpus)

    assert sequences, "the corpus must contain orders or this proves nothing"
    for order_id, statuses in sequences.items():
        assert tuple(statuses) in ORDER_LIFECYCLES, f"{order_id} took the path {statuses}"


def test_every_declared_lifecycle_is_actually_taken(corpus: EventCorpus) -> None:
    """All seven paths appear at this corpus size, so no branch is dead in the fixtures."""
    taken = {tuple(statuses) for statuses in _by_order(corpus).values()}

    assert taken == set(ORDER_LIFECYCLES)


def test_an_orders_payments_and_user_activity_share_its_trace_id(corpus: EventCorpus) -> None:
    """One trace per order, carried by all three streams — the correlation requirement itself.

    This is what ``Query.correlatedEvents`` returns and what makes an order's history one story
    rather than three unrelated tables. Asserted in both directions: every order's trace is present
    in the payment and user streams, and every payment/user trace belongs to some order.
    """
    order_traces: dict[str, str] = {}
    for record in corpus.orders:
        assert record.trace_id is not None
        existing = order_traces.setdefault(record.order_id, record.trace_id)
        assert existing == record.trace_id, f"{record.order_id} spans two traces"

    traces = set(order_traces.values())
    assert len(traces) == len(order_traces), "two orders share a trace id"

    payment_traces = {record.trace_id for record in corpus.payments}
    user_traces = {record.trace_id for record in corpus.user_activity}

    assert payment_traces == traces
    assert user_traces == traces


def test_payments_are_filed_under_the_order_that_created_them(corpus: EventCorpus) -> None:
    """A payment's ``order_id`` and ``trace_id`` agree with the order's, and every order has one.

    "Every order has at least one payment" is a deliberate invariant rather than a coincidence: it
    is what lets a broken order -> payments join in C11 produce a *wrong* answer rather than an
    empty one that looks like a legitimately unpaid order.
    """
    order_traces = {record.order_id: record.trace_id for record in corpus.orders}
    payments_per_order: Counter[str] = Counter()

    for payment in corpus.payments:
        assert payment.order_id in order_traces, payment.order_id
        assert payment.trace_id == order_traces[payment.order_id]
        payments_per_order[payment.order_id] += 1

    assert set(payments_per_order) == set(order_traces)
    assert min(payments_per_order.values()) >= 1


def test_a_payments_method_is_constant_within_one_order(corpus: EventCorpus) -> None:
    """The instrument is a property of the attempt; the outcome is the event's own verb."""
    methods: dict[str, set[str]] = defaultdict(set)
    for payment in corpus.payments:
        methods[payment.order_id].add(payment.method)

    multi = {order_id for order_id, used in methods.items() if len(used) > 1}
    assert not multi, f"these orders changed payment method mid-stream: {sorted(multi)}"


def test_payment_outcomes_are_consistent_with_the_orders_fate(corpus: EventCorpus) -> None:
    """A cancelled order was DECLINED; a refunded one was captured *before* it was refunded.

    Drawing outcomes independently would produce a captured payment on a cancelled order — a corpus
    in which the payment stream and the order stream describe two different businesses.
    """
    sequences = _by_order(corpus)
    outcomes: dict[str, list[str]] = defaultdict(list)
    for payment in corpus.payments:
        outcomes[payment.order_id].append(payment.outcome)

    cancelled = [oid for oid, statuses in sequences.items() if "CANCELLED" in statuses]
    refunded = [oid for oid, statuses in sequences.items() if "REFUNDED" in statuses]
    paid = [
        oid
        for oid, statuses in sequences.items()
        if "PAID" in statuses and "REFUNDED" not in statuses
    ]

    assert cancelled and refunded and paid, "all three shapes must occur or this proves nothing"

    for order_id in cancelled:
        assert outcomes[order_id] == ["DECLINED"], order_id
    for order_id in refunded:
        assert outcomes[order_id] == ["AUTHORIZED", "CAPTURED", "REFUNDED"], order_id
    for order_id in paid:
        assert outcomes[order_id] == ["AUTHORIZED", "CAPTURED"], order_id


def test_the_user_who_acts_is_the_user_the_order_names(corpus: EventCorpus) -> None:
    """The order -> user edge is real: user activity on an order's trace names the order's user."""
    order_users = {record.trace_id: record.user_id for record in corpus.orders}

    for event in corpus.user_activity:
        assert event.user_id == order_users[event.trace_id], event.trace_id


def test_the_user_logs_in_before_the_order_exists_and_checks_out_before_it_is_paid(
    corpus: EventCorpus,
) -> None:
    """The three streams are one interleaved timeline, not three independent ones.

    A corpus in which each stream was generated over its own private clock would satisfy every
    "shares a trace id" assertion above and would still be incoherent — a checkout landing after
    the order it produced. The ordering across streams is what makes the trace a *story*.
    """
    created_at = {record.trace_id: record.timestamp for record in corpus.orders if record.status == "CREATED"}
    paid_at = {record.trace_id: record.timestamp for record in corpus.orders if record.status == "PAID"}

    logins: dict[str, datetime] = {}
    checkouts: dict[str, datetime] = {}
    for event in corpus.user_activity:
        if event.activity_type == "LOGIN":
            logins[event.trace_id] = event.timestamp
        elif event.activity_type == "CHECKOUT":
            checkouts[event.trace_id] = event.timestamp

    assert logins and checkouts

    for trace_id, login in logins.items():
        assert login < created_at[trace_id], f"{trace_id} logged in after the order was created"
    for trace_id, checkout in checkouts.items():
        assert checkout > created_at[trace_id]
        if trace_id in paid_at:
            assert checkout < paid_at[trace_id], f"{trace_id} checked out after paying"


def test_an_authorization_precedes_the_paid_status_and_a_capture_follows_it(
    corpus: EventCorpus,
) -> None:
    """Money is held before the order is marked paid and taken after — not the other way round."""
    paid_at = {
        record.trace_id: record.timestamp for record in corpus.orders if record.status == "PAID"
    }
    authorized: dict[str, datetime] = {}
    captured: dict[str, datetime] = {}
    for payment in corpus.payments:
        if payment.outcome == "AUTHORIZED":
            authorized[payment.trace_id] = payment.timestamp
        elif payment.outcome == "CAPTURED":
            captured[payment.trace_id] = payment.timestamp

    assert paid_at

    for trace_id, paid in paid_at.items():
        assert authorized[trace_id] < paid, trace_id
        assert captured[trace_id] > paid, trace_id


# --- The rosters are promises the corpus keeps ----------------------------------------------------


def test_every_declared_status_method_outcome_and_activity_appears(corpus: EventCorpus) -> None:
    """The four rosters are complete at a reasonable corpus size.

    They are exported as ground truth for C11's tests, C12's verifier and C13's filter dropdowns.
    A declared value the corpus never produces is a filter that can only ever return an empty list,
    which a client cannot tell apart from a quiet period.
    """
    assert {record.status for record in corpus.orders} == set(ORDER_STATUSES)
    assert {record.method for record in corpus.payments} == set(PAYMENT_METHODS)
    assert {record.outcome for record in corpus.payments} == set(PAYMENT_OUTCOMES)
    assert {record.activity_type for record in corpus.user_activity} == set(USER_ACTIVITIES)


def test_each_stream_is_emitted_by_its_declared_service(corpus: EventCorpus) -> None:
    """One service per stream, and all three are names the log corpus can also produce.

    That shared vocabulary is what makes the two corpora describe one system: a ``logs(service:
    "order-service")`` query and ``orderEvents`` are two views of the same component.
    """
    assert {record.service for record in corpus.orders} == {ORDER_EVENT_SERVICE}
    assert {record.service for record in corpus.payments} == {PAYMENT_EVENT_SERVICE}
    assert {record.service for record in corpus.user_activity} == {USER_EVENT_SERVICE}


def test_severity_is_mapped_from_the_event_not_drawn(corpus: EventCorpus) -> None:
    """Every event's level is exactly what its roster mapping says, and is a declared ``LogLevel``.

    Mapped rather than drawn so that ``level`` filters on the event streams are gradeable against
    the oracle *exactly* — a random severity would make the expected set a probability rather than
    a set.
    """
    for record in corpus.orders:
        assert record.level == ORDER_STATUS_LEVELS[record.status]
    for record in corpus.payments:
        assert record.level == PAYMENT_OUTCOME_LEVELS[record.outcome]
    for record in corpus.user_activity:
        assert record.level == USER_ACTIVITY_LEVELS[record.activity_type]

    levels = (
        {record.level for record in corpus.orders}
        | {record.level for record in corpus.payments}
        | {record.level for record in corpus.user_activity}
    )
    assert levels <= set(LOG_LEVELS)
    assert len(levels) > 1, "a single-severity corpus makes the level filter untestable"


def test_declines_are_the_only_errors(corpus: EventCorpus) -> None:
    """``paymentEvents(filters: {level: ERROR})`` must mean "the declines" and nothing else."""
    errors = [record for record in corpus.payments if record.level == "ERROR"]

    assert errors
    assert {record.outcome for record in errors} == {"DECLINED"}


def test_order_ids_are_unique_and_derived_from_the_index(corpus: EventCorpus) -> None:
    """Predictable ids, one per order — a drawn id would collide and merge two histories."""
    order_ids = {record.order_id for record in corpus.orders}

    assert len(order_ids) == SAMPLE_ORDERS
    assert order_ids == {order_id_for(index) for index in range(SAMPLE_ORDERS)}


def test_the_acting_users_come_from_the_shared_roster(corpus: EventCorpus) -> None:
    """Users are drawn from :data:`USER_IDS`, which the log message templates also interpolate.

    A pool far smaller than the order count on purpose: C11 must be able to traverse from a user to
    *several* orders, which a corpus with one user per order could not demonstrate.
    """
    users = {record.user_id for record in corpus.orders}

    assert users <= set(USER_IDS)
    assert len(users) > 1
    assert len(users) < SAMPLE_ORDERS, "users must repeat across orders for C11 to have a fan-out"


# --- Metadata -------------------------------------------------------------------------------------


def test_metadata_is_present_on_some_orders_and_absent_on_others(corpus: EventCorpus) -> None:
    """Both branches are exercised by the seeded corpus alone.

    The absent branch is stored as SQL ``NULL`` rather than the JSONB scalar ``'null'`` — which is
    a claim about ``JSONB(none_as_null=True)`` on the column, not about this test, and is asserted
    in SQL by the integration suite because Python cannot tell the two apart.
    """
    present = [record for record in corpus.orders if record.metadata is not None]
    absent = [record for record in corpus.orders if record.metadata is None]

    assert present and absent


def test_metadata_is_decided_per_order_not_per_event(corpus: EventCorpus) -> None:
    """A whole session either carries context or does not — the SDK is configured or it is not."""
    per_order: dict[str, set[bool]] = defaultdict(set)
    for record in corpus.orders:
        per_order[record.order_id].add(record.metadata is None)

    mixed = {order_id for order_id, flags in per_order.items() if len(flags) > 1}
    assert not mixed, f"these orders have metadata on some events and not others: {sorted(mixed)}"


def test_metadata_objects_carry_a_channel_and_a_region_from_the_rosters(
    corpus: EventCorpus,
) -> None:
    """One shape for the column, drawn from declared vocabularies C11 can aggregate on."""
    objects = [record.metadata for record in corpus.orders if record.metadata is not None]

    assert objects
    for context in objects:
        assert set(context) == {"channel", "region"}
        assert context["channel"] in ORDER_CHANNELS
        assert context["region"] in REGIONS


def test_metadata_objects_are_not_shared_between_events(corpus: EventCorpus) -> None:
    """Each record holds its own dict, so mutating one cannot rewrite its siblings.

    A frozen dataclass freezes the *binding*, not the object behind it. One shared dict across an
    order's events would make the oracle quietly mutable, which is the one thing an oracle must not
    be.
    """
    objects = [record.metadata for record in corpus.orders if record.metadata is not None]
    identities = {id(context) for context in objects}

    assert len(identities) == len(objects)


# --- Column fit and immutability -------------------------------------------------------------------


def test_generated_values_fit_their_columns(corpus: EventCorpus) -> None:
    """Checked against the model's constants, not against restated literals.

    A value that could not be stored would surface as an asyncpg ``DataError`` on the first seeding
    INSERT of a thousand-row chunk — several layers from the roster entry that caused it.
    """
    for record in corpus.orders:
        assert len(record.order_id) <= ORDER_ID_MAX_LENGTH
        assert len(record.user_id) <= USER_ID_MAX_LENGTH
        assert len(record.status) <= ORDER_STATUS_MAX_LENGTH
        assert record.trace_id is not None and len(record.trace_id) <= TRACE_ID_MAX_LENGTH
    for record in corpus.payments:
        assert len(record.method) <= PAYMENT_METHOD_MAX_LENGTH
        assert len(record.outcome) <= PAYMENT_OUTCOME_MAX_LENGTH
    for record in corpus.user_activity:
        assert len(record.activity_type) <= USER_ACTIVITY_MAX_LENGTH


def test_records_are_immutable(corpus: EventCorpus) -> None:
    """Frozen: an oracle is compared against, never adjusted until a comparison passes."""
    with pytest.raises(Exception):
        corpus.orders[0].status = "DELIVERED"  # type: ignore[misc]
    with pytest.raises(Exception):
        corpus.payments[0].outcome = "CAPTURED"  # type: ignore[misc]
    with pytest.raises(Exception):
        corpus.user_activity[0].activity_type = "LOGIN"  # type: ignore[misc]


# --- Degenerate inputs ----------------------------------------------------------------------------


def test_zero_orders_returns_an_empty_corpus() -> None:
    """``SEED_ORDERS=0`` — the compose ``test`` service's own configuration — is a normal case."""
    corpus = generate_event_corpus(0, seed=SEED, end_time=ANCHOR)

    assert corpus.orders == []
    assert corpus.payments == []
    assert corpus.user_activity == []
    assert corpus.total_events() == 0


@pytest.mark.parametrize("count", [-1, -1000])
def test_negative_count_is_rejected(count: int) -> None:
    """A negative count is a caller bug, not a request for an empty corpus."""
    with pytest.raises(ValueError, match="count must be >= 0"):
        generate_event_corpus(count, seed=SEED, end_time=ANCHOR)


def test_a_window_narrower_than_one_cluster_is_rejected_with_a_useful_message() -> None:
    """A window that cannot hold one order's timeline is refused, not silently clamped.

    Clamping would produce events after ``end_time``, which is the one thing the shortened start
    window exists to make impossible.
    """
    with pytest.raises(ValueError, match="ORDER_CLUSTER_MAX_SPAN"):
        generate_event_corpus(
            5, seed=SEED, end_time=ANCHOR, window=ORDER_CLUSTER_MAX_SPAN
        )


def test_a_custom_window_is_respected() -> None:
    """A narrower window still contains every event, cluster tail included."""
    window = timedelta(hours=8)
    corpus = generate_event_corpus(20, seed=SEED, end_time=ANCHOR, window=window)

    timestamps = [
        record.timestamp
        for stream in (corpus.orders, corpus.payments, corpus.user_activity)
        for record in stream
    ]

    assert min(timestamps) >= ANCHOR - window
    assert max(timestamps) < ANCHOR


def test_naive_end_time_is_interpreted_as_utc() -> None:
    """Naive means UTC — the same rule the repository applies to filter bounds."""
    aware = generate_event_corpus(20, seed=SEED, end_time=ANCHOR)
    naive = generate_event_corpus(20, seed=SEED, end_time=ANCHOR.replace(tzinfo=None))

    assert aware == naive


def test_trace_ids_and_totals_are_reported_consistently(corpus: EventCorpus) -> None:
    """The two convenience projections agree with the streams they summarise."""
    assert len(corpus.trace_ids()) == SAMPLE_ORDERS
    assert corpus.total_events() == (
        len(corpus.orders) + len(corpus.payments) + len(corpus.user_activity)
    )
