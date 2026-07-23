"""Unit tests for C9's filter tree: the wire vocabulary, the compiler, and the index hint.

Three things are pinned here, and they fail in three very different ways:

* **The vocabulary** (``src/models.py``) — which ``(field, op, value)`` triples are legal. A gap
  here is a ``422`` that should have been a query, or a query that should have been a ``422``.
* **The compiler** (``src.store.compile_filter``) — what each triple *means* once it meets a
  record. A gap here is a wrong answer that looks exactly like a right one.
* **The index hint** — which is the only part of this file that can be wrong *silently*. A hint
  narrower than the true match set drops rows from a page that is otherwise perfectly well
  formed, so :func:`test_index_hint_is_never_unsound` runs every tree down both the hinted and
  the unhinted path and demands byte-identical answers. It is the most important test in the
  module.

No HTTP anywhere: everything is ``(tree, entry) -> bool`` or ``(tree, store) -> ids``. The route
that wraps all of this is ``tests/integration/test_search_api.py``'s subject.

Records are obtained from :meth:`~src.store.LogStore.append` rather than by hand-building a
:class:`~src.store.StoredEntry`. The compiled predicate reads ``ts_epoch`` and ``message_lower``,
which are precomputed *by the store*; a hand-built record could carry a stale or differently-cased
cache and every assertion here would then be testing the test.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from src.models import (
    FIELD_OPS,
    MAX_FILTER_DEPTH,
    MAX_FILTER_NODES,
    MAX_FILTER_VALUES,
    FilterAll,
    FilterField,
    FilterLeaf,
    FilterNode,
    FilterOp,
    LogEntry,
    SearchRequest,
    SortOrder,
)
from src.store import (
    INDEX_HINT_MIN_SELECTIVITY,
    CompiledFilter,
    Filter,
    LogStore,
    StoredEntry,
    compile_filter,
)

#: Anchor for every timestamp below. Fixed, never ``datetime.now()``: a corpus whose contents
#: depend on when the suite ran cannot pin an inclusive comparison boundary.
BASE_TS = datetime(2026, 7, 27, 10, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------


def make_entry(
    *,
    entry_id: str = "e0",
    level: str = "ERROR",
    service: str = "auth-svc",
    host: str = "node-3",
    message: str = "Invalid Token For User",
    ts: datetime | None = None,
) -> LogEntry:
    """One deterministic entry. Mixed case in ``message`` is deliberate — ``contains`` is
    case-insensitive and ``eq`` is not, and only a mixed-case value can tell them apart."""
    return LogEntry(
        id=entry_id,
        ts=BASE_TS if ts is None else ts,
        level=level,
        service=service,
        host=host,
        message=message,
    )


def stored(entry: LogEntry) -> StoredEntry:
    """The stored form of one entry, produced by the store itself. See the module docstring."""
    return LogStore(capacity=1).append(entry)


def parse(tree: dict[str, Any] | None) -> FilterNode | None:
    """Validate a raw tree exactly as the HTTP body does, and hand back the parsed node.

    Deliberately through :class:`~src.models.SearchRequest` rather than by constructing a
    ``FilterAll``/``FilterLeaf`` directly: the depth and node-count caps live on the request's
    ``mode="before"`` validator, so a test that skipped it would be exercising a shape no client
    can actually send.
    """
    return SearchRequest(filter=tree).filter


def compiled(tree: dict[str, Any] | None, order: SortOrder = SortOrder.DESC) -> CompiledFilter:
    """Parse and compile a raw tree in one step."""
    return compile_filter(parse(tree), order)


def evaluate(tree: dict[str, Any] | None, entry: LogEntry) -> bool:
    """The whole pipeline for one record: validate, compile, match."""
    return compiled(tree).matches(stored(entry))


def leaf(field: str, op: str, value: Any) -> dict[str, Any]:
    """A leaf node, spelled the way a client would."""
    return {"field": field, "op": op, "value": value}


def ids_of(records: list[StoredEntry]) -> list[str]:
    """The entry ids of a scan result, in scan order."""
    return [record.entry.id for record in records]


class Unhinted:
    """The same compiled filter with its index hint suppressed, forcing the linear scan.

    Not a subclass — :class:`~src.store.CompiledFilter` binds ``matches`` as an instance
    attribute, so a subclass could not override it anyway. Delegating instead is also closer to
    the point being made: the store's contract with a filter is four members and nothing more, so
    anything exposing those four is a filter as far as it is concerned.

    This is what makes :func:`test_index_hint_is_never_unsound` possible at all: it runs one
    predicate down both strategies and compares, which is the only way to prove the index is an
    optimisation rather than a second, subtly different implementation.
    """

    def __init__(self, inner: CompiledFilter) -> None:
        self._inner = inner

    @property
    def is_empty(self) -> bool:
        return self._inner.is_empty

    def matches(self, rec: StoredEntry) -> bool:
        return self._inner.matches(rec)

    def fingerprint(self) -> str:
        return self._inner.fingerprint()

    def index_hint(self, store: LogStore) -> None:  # noqa: ARG002 - signature is the contract
        return None


# ---------------------------------------------------------------------------------------------
# Leaf predicates — every operator, on every field it is valid for
# ---------------------------------------------------------------------------------------------

#: The reference record every row in :data:`LEAF_CASES` is evaluated against.
SAMPLE = make_entry()

#: **The field x operator matrix, as executable cases.** Each row is
#: ``(leaf, expected_verdict_for_SAMPLE)``, and every valid ``(field, op)`` pair appears at least
#: twice — once matching and once not. A row that only ever matched would pass just as happily
#: against a predicate that returned ``True`` unconditionally.
#:
#: :func:`test_leaf_table_covers_every_valid_field_operator_pair` asserts this table is complete
#: against :data:`~src.models.FIELD_OPS`, so a new operator cannot be added to the vocabulary
#: without a case landing here.
LEAF_CASES: list[tuple[dict[str, Any], bool]] = [
    # -- level: identity ----------------------------------------------------------------------
    (leaf("level", "eq", "ERROR"), True),
    (leaf("level", "eq", "INFO"), False),
    (leaf("level", "ne", "INFO"), True),
    (leaf("level", "ne", "ERROR"), False),
    (leaf("level", "in", ["ERROR", "FATAL"]), True),
    (leaf("level", "in", ["DEBUG", "INFO"]), False),
    (leaf("level", "nin", ["DEBUG", "INFO"]), True),
    (leaf("level", "nin", ["ERROR"]), False),
    # -- level: severity order, NOT alphabetical ----------------------------------------------
    (leaf("level", "gt", "WARN"), True),
    (leaf("level", "gt", "ERROR"), False),
    (leaf("level", "gte", "ERROR"), True),
    (leaf("level", "gte", "FATAL"), False),
    (leaf("level", "lt", "FATAL"), True),
    (leaf("level", "lt", "ERROR"), False),
    (leaf("level", "lte", "ERROR"), True),
    (leaf("level", "lte", "WARN"), False),
    # -- service ------------------------------------------------------------------------------
    (leaf("service", "eq", "auth-svc"), True),
    (leaf("service", "eq", "api-svc"), False),
    (leaf("service", "ne", "api-svc"), True),
    (leaf("service", "ne", "auth-svc"), False),
    (leaf("service", "in", ["auth-svc", "api-svc"]), True),
    (leaf("service", "in", ["api-svc"]), False),
    (leaf("service", "nin", ["api-svc"]), True),
    (leaf("service", "nin", ["auth-svc"]), False),
    (leaf("service", "contains", "auth"), True),
    (leaf("service", "contains", "AUTH"), True),
    (leaf("service", "contains", "search"), False),
    # -- host ---------------------------------------------------------------------------------
    (leaf("host", "eq", "node-3"), True),
    (leaf("host", "eq", "node-9"), False),
    (leaf("host", "ne", "node-9"), True),
    (leaf("host", "ne", "node-3"), False),
    (leaf("host", "in", ["node-3", "node-4"]), True),
    (leaf("host", "in", ["node-4"]), False),
    (leaf("host", "nin", ["node-4"]), True),
    (leaf("host", "nin", ["node-3"]), False),
    (leaf("host", "contains", "NODE"), True),
    (leaf("host", "contains", "rack"), False),
    # -- message: `eq` is exact and case-SENSITIVE, `contains` is neither ----------------------
    (leaf("message", "eq", "Invalid Token For User"), True),
    (leaf("message", "eq", "invalid token for user"), False),
    (leaf("message", "ne", "something else"), True),
    (leaf("message", "ne", "Invalid Token For User"), False),
    (leaf("message", "in", ["Invalid Token For User", "other"]), True),
    (leaf("message", "in", ["other"]), False),
    (leaf("message", "nin", ["other"]), True),
    (leaf("message", "nin", ["Invalid Token For User"]), False),
    (leaf("message", "contains", "token"), True),
    (leaf("message", "contains", "TOKEN FOR"), True),
    (leaf("message", "contains", "expired"), False),
    # -- ts: RFC-3339 and epoch spellings, inclusive and exclusive bounds ----------------------
    (leaf("ts", "eq", "2026-07-27T10:00:00Z"), True),
    (leaf("ts", "eq", "2026-07-27T11:00:00Z"), False),
    (leaf("ts", "ne", "2026-07-27T11:00:00Z"), True),
    (leaf("ts", "ne", "2026-07-27T10:00:00Z"), False),
    (leaf("ts", "gt", "2026-07-27T09:59:59Z"), True),
    (leaf("ts", "gt", "2026-07-27T10:00:00Z"), False),
    (leaf("ts", "gte", "2026-07-27T10:00:00Z"), True),
    (leaf("ts", "gte", "2026-07-27T10:00:01Z"), False),
    (leaf("ts", "lt", "2026-07-27T10:00:01Z"), True),
    (leaf("ts", "lt", "2026-07-27T10:00:00Z"), False),
    (leaf("ts", "lte", "2026-07-27T10:00:00Z"), True),
    (leaf("ts", "lte", "2026-07-27T09:59:59Z"), False),
]


@pytest.mark.parametrize(
    ("tree", "expected"),
    LEAF_CASES,
    ids=[f"{c['field']}-{c['op']}-{'T' if e else 'F'}" for c, e in LEAF_CASES],
)
def test_leaf_operator_matrix(tree: dict[str, Any], expected: bool) -> None:
    """Every legal ``(field, op)`` pair, evaluated against one known record."""
    assert evaluate(tree, SAMPLE) is expected


def test_leaf_table_covers_every_valid_field_operator_pair() -> None:
    """The table above is complete, so the vocabulary cannot grow an untested operator.

    Derived from :data:`~src.models.FIELD_OPS` rather than typed out: adding an operator to a
    field's row there fails this test until a case for it exists, which is the only way a matrix
    test stays honest as the matrix changes.
    """
    covered = {(case["field"], case["op"]) for case, _ in LEAF_CASES}
    expected = {
        (field.value, op.value) for field, ops in FIELD_OPS.items() for op in ops
    }

    assert covered == expected


def test_eq_and_ne() -> None:
    """``eq`` and ``ne`` are exact opposites on every field, for hits and misses alike."""
    for field, hit, miss in (
        ("level", "ERROR", "INFO"),
        ("service", "auth-svc", "api-svc"),
        ("host", "node-3", "node-9"),
        ("message", "Invalid Token For User", "nope"),
        ("ts", "2026-07-27T10:00:00Z", "2026-07-27T10:00:01Z"),
    ):
        assert evaluate(leaf(field, "eq", hit), SAMPLE) is True, field
        assert evaluate(leaf(field, "ne", hit), SAMPLE) is False, field
        assert evaluate(leaf(field, "eq", miss), SAMPLE) is False, field
        assert evaluate(leaf(field, "ne", miss), SAMPLE) is True, field


def test_in_and_nin() -> None:
    """``in`` is set membership and ``nin`` is its complement — including on a duplicate list.

    The duplicate case matters because ``coerce_filter_value`` collapses the list into a
    ``frozenset``: the answer must not depend on how many times a value was repeated.
    """
    assert evaluate(leaf("level", "in", ["ERROR", "ERROR", "FATAL"]), SAMPLE) is True
    assert evaluate(leaf("level", "nin", ["ERROR", "ERROR"]), SAMPLE) is False
    assert evaluate(leaf("service", "in", ["a", "b", "auth-svc"]), SAMPLE) is True
    assert evaluate(leaf("service", "nin", ["a", "b"]), SAMPLE) is True


def test_contains_is_case_insensitive() -> None:
    """``contains`` matches a substring in either case, on all three text fields.

    ``message`` is the interesting one: the store precomputes ``message_lower`` and the needle is
    lower-cased at compile time, so the comparison never touches the original casing on either
    side. ``service``/``host`` have no precomputed cache and must still behave identically —
    which is exactly the kind of asymmetry that produces a filter that works on one field and
    quietly does not on another.
    """
    entry = make_entry(service="Auth-SVC", host="NODE-3", message="Invalid Token")

    for spelling in ("token", "TOKEN", "ToKeN"):
        assert evaluate(leaf("message", "contains", spelling), entry) is True, spelling
    for spelling in ("auth", "AUTH", "AuTh"):
        assert evaluate(leaf("service", "contains", spelling), entry) is True, spelling
    for spelling in ("node", "NODE"):
        assert evaluate(leaf("host", "contains", spelling), entry) is True, spelling

    assert evaluate(leaf("message", "contains", "expired"), entry) is False
    # `eq` is deliberately NOT case-folded: an exact match is an exact match.
    assert evaluate(leaf("service", "eq", "auth-svc"), entry) is False


def test_ts_comparison_operators() -> None:
    """Time ranges work, and RFC-3339 and POSIX-epoch spellings of one instant agree exactly.

    Both go through pydantic's own datetime rules — the same ones behind ``?since=`` — so the two
    entry points cannot drift into accepting different vocabularies.
    """
    early = make_entry(entry_id="early", ts=BASE_TS - timedelta(minutes=5))
    late = make_entry(entry_id="late", ts=BASE_TS + timedelta(minutes=5))
    boundary_rfc = BASE_TS.isoformat().replace("+00:00", "Z")
    boundary_epoch = BASE_TS.timestamp()

    for spelling in (boundary_rfc, boundary_epoch):
        assert evaluate(leaf("ts", "gte", spelling), SAMPLE) is True, spelling
        assert evaluate(leaf("ts", "gt", spelling), SAMPLE) is False, spelling
        assert evaluate(leaf("ts", "lte", spelling), SAMPLE) is True, spelling
        assert evaluate(leaf("ts", "lt", spelling), SAMPLE) is False, spelling
        assert evaluate(leaf("ts", "gt", spelling), late) is True, spelling
        assert evaluate(leaf("ts", "lt", spelling), early) is True, spelling

    # A closed range is just two leaves in an `all`, with both bounds inclusive.
    window = {
        "all": [
            leaf("ts", "gte", (BASE_TS - timedelta(minutes=1)).isoformat()),
            leaf("ts", "lte", (BASE_TS + timedelta(minutes=1)).isoformat()),
        ]
    }
    assert evaluate(window, SAMPLE) is True
    assert evaluate(window, early) is False
    assert evaluate(window, late) is False


def test_level_comparison_uses_severity_order() -> None:
    """``level gte WARN`` means *at least as severe as WARN*, not the alphabetical reading.

    This is the whole reason :data:`~src.models.LEVEL_ORDER` exists. Sorted as text,
    ``"WARN" > "INFO" > "FATAL" > "ERROR" > "DEBUG"`` — so a lexicographic implementation would
    match WARN alone and exclude ERROR and FATAL, which for a severity filter is not a subtle
    inaccuracy but the opposite of the intent. Somebody paging on ``level >= WARN`` would stop
    being paged for FATAL.
    """
    verdicts = {
        level: evaluate(leaf("level", "gte", "WARN"), make_entry(level=level))
        for level in ("DEBUG", "INFO", "WARN", "ERROR", "FATAL")
    }

    assert verdicts == {
        "DEBUG": False,
        "INFO": False,
        "WARN": True,
        "ERROR": True,
        "FATAL": True,
    }

    # And the mirror direction, which alphabetical order would also get wrong.
    assert evaluate(leaf("level", "lt", "WARN"), make_entry(level="DEBUG")) is True
    assert evaluate(leaf("level", "lt", "WARN"), make_entry(level="FATAL")) is False
    assert evaluate(leaf("level", "lte", "INFO"), make_entry(level="INFO")) is True
    assert evaluate(leaf("level", "gt", "DEBUG"), make_entry(level="INFO")) is True


# ---------------------------------------------------------------------------------------------
# Boolean structure
# ---------------------------------------------------------------------------------------------


def test_all_is_conjunction() -> None:
    """``all`` requires every child — one refusal is enough to reject the record."""
    both = {"all": [leaf("level", "eq", "ERROR"), leaf("service", "eq", "auth-svc")]}

    assert evaluate(both, SAMPLE) is True
    assert evaluate(both, make_entry(level="INFO")) is False
    assert evaluate(both, make_entry(service="api-svc")) is False
    assert evaluate(both, make_entry(level="INFO", service="api-svc")) is False


def test_any_is_disjunction() -> None:
    """``any`` requires one child — and one is enough even when the others refuse."""
    either = {"any": [leaf("level", "eq", "FATAL"), leaf("service", "eq", "auth-svc")]}

    assert evaluate(either, SAMPLE) is True  # second child only
    assert evaluate(either, make_entry(level="FATAL", service="api-svc")) is True
    assert evaluate(either, make_entry(level="FATAL")) is True  # both
    assert evaluate(either, make_entry(level="INFO", service="api-svc")) is False


def test_not_negates() -> None:
    """``not`` inverts its child, whether the child is a leaf or a whole subtree."""
    assert evaluate({"not": leaf("level", "eq", "ERROR")}, SAMPLE) is False
    assert evaluate({"not": leaf("level", "eq", "INFO")}, SAMPLE) is True

    subtree = {"not": {"all": [leaf("level", "eq", "ERROR"), leaf("host", "eq", "node-3")]}}
    assert evaluate(subtree, SAMPLE) is False
    assert evaluate(subtree, make_entry(host="node-9")) is True

    # Double negation is the identity — nothing normalises it away, so it has to evaluate.
    assert evaluate({"not": {"not": leaf("level", "eq", "ERROR")}}, SAMPLE) is True


def test_nested_all_any_not_combination() -> None:
    """A four-level tree: *(ERROR or FATAL) and not (service is search-svc and host is node-9)*.

    The point of the shape is that no single child decides it. Each of the three entries below
    fails for a different reason and the fourth passes, so a compiler that dropped a level, lost a
    negation or flattened the disjunction into a conjunction fails on at least one of them.
    """
    tree = {
        "all": [
            {"any": [leaf("level", "eq", "ERROR"), leaf("level", "eq", "FATAL")]},
            {
                "not": {
                    "all": [
                        leaf("service", "eq", "search-svc"),
                        leaf("host", "eq", "node-9"),
                    ]
                }
            },
        ]
    }

    assert evaluate(tree, SAMPLE) is True
    assert evaluate(tree, make_entry(level="FATAL")) is True
    # Excluded by the negated subtree — both of its conjuncts hold.
    assert evaluate(tree, make_entry(service="search-svc", host="node-9")) is False
    # ...but only both. Either one alone leaves the record matching.
    assert evaluate(tree, make_entry(service="search-svc", host="node-3")) is True
    # Excluded by the disjunction.
    assert evaluate(tree, make_entry(level="WARN")) is False


def test_empty_all_matches_everything() -> None:
    """``{"all": []}`` is vacuously TRUE — the identity of AND, and the documented contract.

    It is what makes the natural client behaviour work: a UI that starts with an empty ``all`` and
    pushes conditions into it as boxes are ticked must show everything before the first tick, not
    an empty result that reads as a broken search. It also makes an empty filter and an omitted
    filter agree, which is why both compile to the same "matches everything" predicate.
    """
    for level in ("DEBUG", "INFO", "WARN", "ERROR", "FATAL"):
        assert evaluate({"all": []}, make_entry(level=level)) is True, level

    assert compiled({"all": []}).is_empty is True
    assert compiled(None).is_empty is True
    # Vacuous truth composes: an empty `all` nested inside another is still everything, and
    # ANDing it with a real predicate must leave that predicate untouched.
    assert compiled({"all": [{"all": []}]}).is_empty is True
    assert evaluate({"all": [leaf("level", "eq", "INFO"), {"all": []}]}, SAMPLE) is False
    assert evaluate({"all": [leaf("level", "eq", "ERROR"), {"all": []}]}, SAMPLE) is True


def test_empty_any_matches_nothing() -> None:
    """``{"any": []}`` is vacuously FALSE — the mirror rule, and the one that catches people out.

    ``False`` is the identity of OR, so "at least one of these zero conditions holds" is false for
    every record. The critical half is the second assertion: a filter that matches nothing must
    **not** report ``is_empty``, because the store reads that as "unconstrained" and answers
    ``page.total`` with the size of the whole ring — which would print the corpus count above a
    page with no items on it.
    """
    for level in ("DEBUG", "INFO", "WARN", "ERROR", "FATAL"):
        assert evaluate({"any": []}, make_entry(level=level)) is False, level

    assert compiled({"any": []}).is_empty is False
    # Falsity propagates the same way truth does.
    assert evaluate({"all": [leaf("level", "eq", "ERROR"), {"any": []}]}, SAMPLE) is False
    assert evaluate({"any": [leaf("level", "eq", "INFO"), {"any": []}]}, SAMPLE) is False
    # ...and negating "nothing" is "everything".
    assert compiled({"not": {"any": []}}).is_empty is True
    assert evaluate({"not": {"any": []}}, SAMPLE) is True


# ---------------------------------------------------------------------------------------------
# Rejected trees — a 422 at parse time, never a filter that silently matches nothing
# ---------------------------------------------------------------------------------------------


def test_unknown_field_rejected() -> None:
    """An unaddressable field names the valid set instead of matching nothing.

    "No such logs" and "you spelled the field wrong" look identical to someone debugging an
    incident, which is why the vocabulary is a closed enum rather than an open string.
    """
    with pytest.raises(ValidationError) as exc:
        parse(leaf("severity", "eq", "ERROR"))

    assert "field" in str(exc.value)

    with pytest.raises(ValidationError):
        parse(leaf("attrs.request_id", "eq", "b1c2d3"))


def test_unknown_op_rejected() -> None:
    """An operator outside the nine is a validation error, not an ignored clause."""
    for op in ("regex", "like", "startswith", "=="):
        with pytest.raises(ValidationError):
            parse(leaf("message", op, "x"))


@pytest.mark.parametrize(
    ("field", "op"),
    [
        pytest.param(field.value, op.value, id=f"{field.value}-{op.value}")
        for field in FilterField
        for op in FilterOp
        if op not in FIELD_OPS[field]
    ],
)
def test_op_field_mismatch_rejected(field: str, op: str) -> None:
    """Every ``(field, op)`` pair the matrix forbids is refused, with the valid set named.

    Generated from :data:`~src.models.FIELD_OPS` so the negative space is covered as
    exhaustively as the positive space above — ``contains`` on ``level`` (a slower, subtly wrong
    ``eq``), ordering on ``service`` (sorting host names answers no question anybody asks),
    ``in`` on ``ts`` (an explicit set of exact instants is not a query anyone writes).
    """
    value: Any
    if op in {"in", "nin"}:
        value = ["ERROR"] if field == "level" else ["x"]
    elif field == "level":
        value = "ERROR"
    elif field == "ts":
        value = "2026-07-27T10:00:00Z"
    else:
        value = "x"

    with pytest.raises(ValidationError) as exc:
        parse(leaf(field, op, value))

    assert "valid operators" in str(exc.value) or op in str(exc.value)


def test_in_requires_a_list() -> None:
    """``in``/``nin`` take a list. A bare scalar is a client bug worth reporting."""
    for op in ("in", "nin"):
        with pytest.raises(ValidationError) as exc:
            parse(leaf("level", op, "ERROR"))
        assert "list" in str(exc.value)

    # An EMPTY list is refused too: it matches nothing, which is indistinguishable from
    # "no such logs" — the same reasoning that makes `since > until` an error rather than a page.
    with pytest.raises(ValidationError) as exc:
        parse(leaf("level", "in", []))
    assert "empty list" in str(exc.value)


def test_eq_rejects_a_list() -> None:
    """A scalar operator handed a list is refused, and the message suggests the fix."""
    with pytest.raises(ValidationError) as exc:
        parse(leaf("level", "eq", ["ERROR", "FATAL"]))

    assert "did you mean 'in'" in str(exc.value)

    for op in ("ne", "contains"):
        with pytest.raises(ValidationError):
            parse(leaf("service", op, ["a", "b"]))


def test_leaf_value_type_is_checked_against_the_field() -> None:
    """A well-shaped leaf whose *value* cannot mean anything on that field is still a 422."""
    with pytest.raises(ValidationError) as exc:
        parse(leaf("level", "eq", "TRACE"))
    assert "not a log level" in str(exc.value)

    with pytest.raises(ValidationError):
        parse(leaf("service", "eq", 42))
    with pytest.raises(ValidationError):
        parse(leaf("ts", "gt", "yesterday"))
    # `isinstance(True, int)` is True in Python, so a boolean would otherwise read as epoch 1.
    with pytest.raises(ValidationError):
        parse(leaf("ts", "gt", True))
    # An empty `contains` needle is a substring of every string — not a filter at all.
    with pytest.raises(ValidationError):
        parse(leaf("message", "contains", ""))


def test_node_carrying_two_combinators_is_rejected() -> None:
    """``{"all": [...], "any": [...]}`` has no meaning, and says so in one sentence."""
    with pytest.raises(ValidationError) as exc:
        parse({"all": [leaf("level", "eq", "ERROR")], "any": []})

    assert "each node is exactly one of" in str(exc.value)


# ---------------------------------------------------------------------------------------------
# The three caps — a hostile body must be cheap to reject, not expensive to evaluate
# ---------------------------------------------------------------------------------------------


def nest(depth: int) -> dict[str, Any]:
    """A tree exactly ``depth`` levels deep: ``depth - 1`` nested ``all``s around one leaf."""
    node: dict[str, Any] = leaf("level", "eq", "ERROR")
    for _ in range(depth - 1):
        node = {"all": [node]}
    return node


def test_depth_limit_rejects_deep_tree() -> None:
    """The boundary is pinned from both sides: ``MAX_FILTER_DEPTH`` passes, one more does not."""
    accepted = parse(nest(MAX_FILTER_DEPTH))
    assert accepted is not None
    assert compile_filter(accepted, SortOrder.DESC).matches(stored(SAMPLE)) is True

    with pytest.raises(ValidationError) as exc:
        parse(nest(MAX_FILTER_DEPTH + 1))

    assert "nested deeper" in str(exc.value)


def test_compiler_re_checks_depth_for_python_built_trees() -> None:
    """The cap is enforced a second time in the compiler, on the path that skips validation.

    :func:`~src.models.check_filter_shape` runs as a ``mode="before"`` validator on the *request*,
    so a tree assembled from model instances in Python — a test, or a future internal caller —
    never passes through it. The bound exists to prevent a ``RecursionError`` (an availability bug
    reachable by anyone who can post a body), so the compiler cannot rely on someone else having
    checked.
    """
    node: FilterNode = FilterLeaf(field="level", op="eq", value="ERROR")
    for _ in range(MAX_FILTER_DEPTH):  # one level past the cap
        node = FilterAll(all=[node])

    with pytest.raises(ValueError, match="nested deeper"):
        compile_filter(node, SortOrder.DESC)


def test_node_count_limit() -> None:
    """Depth alone does not bound a tree: a very wide, very shallow one is capped too.

    ``{"all": [ …fifty thousand leaves… ]}`` is two levels deep and still a denial of service,
    which is why the total node count has its own ceiling.
    """
    one_leaf = leaf("level", "eq", "ERROR")

    # Root + (MAX - 1) children == exactly MAX nodes.
    assert parse({"all": [one_leaf] * (MAX_FILTER_NODES - 1)}) is not None

    with pytest.raises(ValidationError) as exc:
        parse({"all": [one_leaf] * MAX_FILTER_NODES})

    assert "more than" in str(exc.value)


def test_value_list_length_limit() -> None:
    """``in``/``nin`` carry the only collection a leaf has, so it gets its own cap."""
    values = [f"svc-{i}" for i in range(MAX_FILTER_VALUES)]
    assert parse(leaf("service", "in", values)) is not None

    with pytest.raises(ValidationError) as exc:
        parse(leaf("service", "in", [*values, "one-too-many"]))

    assert "at most" in str(exc.value)


def test_deeply_nested_tree_does_not_recurse_to_death() -> None:
    """A 5,000-level body is a ``422``, never a ``RecursionError`` surfacing as a ``500``.

    This is why the depth check is a ``mode="before"`` validator over the raw decoded JSON *and*
    why its walk is iterative. Pydantic validates a recursive model bottom-up, so any check that
    waited for the model to exist would already have blown the interpreter stack building it — and
    a recursive depth-checker that blows its own stack while proving the input is too deep has not
    checked anything.
    """
    deep = nest(5_000)

    with pytest.raises(ValidationError) as exc:
        SearchRequest(filter=deep)

    assert "nested deeper" in str(exc.value)


# ---------------------------------------------------------------------------------------------
# Fingerprint — the identity a cursor is bound to
# ---------------------------------------------------------------------------------------------


def test_fingerprint_is_stable_and_semantic() -> None:
    """Same meaning, same fingerprint; different meaning, different fingerprint.

    The cursor embeds this, so the two directions have very different costs. A fingerprint that
    changed for an identical filter would invalidate a perfectly good cursor mid-walk (annoying).
    One that *collided* across genuinely different filters would let a cursor from one search
    resume another — a page that is internally consistent and completely wrong (the failure the
    fingerprint exists to prevent).

    What is deliberately normalised — value order inside ``in``, child order inside a commutative
    ``all``, an omitted filter versus an empty one, the two spellings of an instant — is exactly
    the set of differences that cannot change which records match or in what order.
    """
    tree = {
        "all": [
            leaf("level", "in", ["ERROR", "FATAL"]),
            leaf("service", "eq", "auth-svc"),
        ]
    }

    assert compiled(tree).fingerprint() == compiled(tree).fingerprint()
    assert len(compiled(tree).fingerprint()) == 16, "blake2b(digest_size=8) rendered as hex"

    # Set iteration order must not leak in: `in` values are compared as a set.
    reordered_values = {
        "all": [
            leaf("level", "in", ["FATAL", "ERROR"]),
            leaf("service", "eq", "auth-svc"),
        ]
    }
    assert compiled(reordered_values).fingerprint() == compiled(tree).fingerprint()

    # AND is commutative, so ticking the boxes in the other order is the same search.
    reordered_children = {"all": list(reversed(tree["all"]))}
    assert compiled(reordered_children).fingerprint() == compiled(tree).fingerprint()

    # Two spellings of one instant are one predicate.
    rfc = compiled(leaf("ts", "gte", "2026-07-27T10:00:00Z")).fingerprint()
    epoch = compiled(leaf("ts", "gte", BASE_TS.timestamp())).fingerprint()
    assert rfc == epoch

    # An omitted filter and an empty conjunction both mean "everything".
    assert compiled(None).fingerprint() == compiled({"all": []}).fingerprint()

    # ...and every genuine difference is a different fingerprint.
    assert compiled({"any": tree["all"]}).fingerprint() != compiled(tree).fingerprint()
    assert compiled({"not": tree}).fingerprint() != compiled(tree).fingerprint()
    assert compiled({"all": []}).fingerprint() != compiled({"any": []}).fingerprint()
    assert (
        compiled(leaf("level", "eq", "ERROR")).fingerprint()
        != compiled(leaf("level", "ne", "ERROR")).fingerprint()
    )
    assert (
        compiled(leaf("level", "eq", "ERROR")).fingerprint()
        != compiled(leaf("service", "eq", "ERROR")).fingerprint()
    )

    # The sort order is part of the walk's identity, which is why `compile_filter` takes it.
    assert (
        compiled(tree, SortOrder.ASC).fingerprint()
        != compiled(tree, SortOrder.DESC).fingerprint()
    )

    # And a compiled filter never collides with the flat one, even for the same match set: the
    # rendering is namespaced, so the two routes' cursors are distinct without being detectably
    # different in *shape* (both are 16 hex characters).
    flat = Filter(levels=frozenset({"ERROR", "FATAL"}), services=frozenset({"auth-svc"}))
    assert compiled(tree).fingerprint() != flat.fingerprint()
    assert len(flat.fingerprint()) == len(compiled(tree).fingerprint())


# ---------------------------------------------------------------------------------------------
# Index hints — the only thing here that can be wrong *silently*
# ---------------------------------------------------------------------------------------------

#: A corpus wide enough for the hint to be worth taking. ``INDEX_HINT_MIN_SELECTIVITY`` means a
#: candidate set must be under ~1/8 of the ring before the planner follows it, so the services are
#: spread thinly enough (1/12 of the corpus each) that a ``service`` constraint really does take
#: the hinted path — otherwise every case below would quietly test the linear scan twice.
HINT_CORPUS_SIZE = 240
HINT_SERVICES = tuple(f"svc-{i}" for i in range(12))
HINT_HOSTS = tuple(f"node-{i}" for i in range(8))
HINT_LEVELS = ("DEBUG", "INFO", "WARN", "ERROR", "FATAL")


def hint_store() -> LogStore:
    """A store holding :data:`HINT_CORPUS_SIZE` entries spread across all three indexes."""
    store = LogStore(capacity=HINT_CORPUS_SIZE * 2)
    store.append_many(
        make_entry(
            entry_id=f"h{i:04d}",
            level=HINT_LEVELS[i % 5],
            service=HINT_SERVICES[i % 12],
            host=HINT_HOSTS[i % 8],
            message=f"Request {i} Completed",
            ts=BASE_TS + timedelta(seconds=i),
        )
        for i in range(HINT_CORPUS_SIZE)
    )
    return store


#: Trees whose hinted and unhinted answers must agree. The first three are hintable; the rest
#: exist because they must **not** be — a hint drawn from under an ``any`` or a ``not`` is the one
#: bug in this file that returns a well-formed page with rows missing from it.
HINT_TREES: list[tuple[str, dict[str, Any] | None]] = [
    ("leaf-eq-service", leaf("service", "eq", "svc-3")),
    (
        "all-with-two-hintable-leaves",
        {"all": [leaf("service", "in", ["svc-1", "svc-2"]), leaf("level", "eq", "ERROR")]},
    ),
    (
        "all-mixing-hintable-and-not",
        {
            "all": [
                leaf("service", "eq", "svc-5"),
                leaf("message", "contains", "request"),
                leaf("ts", "gte", (BASE_TS + timedelta(seconds=30)).isoformat()),
            ]
        },
    ),
    ("any-at-the-root", {"any": [leaf("service", "eq", "svc-3"), leaf("level", "eq", "FATAL")]}),
    ("not-at-the-root", {"not": leaf("service", "eq", "svc-3")}),
    (
        "not-inside-a-root-all",
        {"all": [{"not": leaf("service", "eq", "svc-3")}, leaf("level", "eq", "WARN")]},
    ),
    (
        "any-inside-a-root-all",
        {
            "all": [
                {"any": [leaf("service", "eq", "svc-3"), leaf("host", "eq", "node-1")]},
                leaf("level", "in", ["ERROR", "FATAL"]),
            ]
        },
    ),
    ("negated-in-over-an-indexed-field", {"not": leaf("level", "in", ["DEBUG", "INFO"])}),
    ("ne-over-an-indexed-field", leaf("host", "ne", "node-1")),
    (
        "contradictory-conjunction",
        {"all": [leaf("host", "eq", "node-1"), leaf("host", "eq", "node-2")]},
    ),
    ("nothing-indexed", leaf("message", "contains", "completed")),
    ("empty-filter", None),
]


@pytest.mark.parametrize(("label", "tree"), HINT_TREES, ids=[label for label, _ in HINT_TREES])
def test_index_hint_is_never_unsound(label: str, tree: dict[str, Any] | None) -> None:
    """**The most important test in this module.** Hinted and unhinted must agree, always.

    An index hint is a *candidate* set: the predicate runs on every candidate afterwards, so a
    hint that is too wide costs time and nothing else. A hint that is too **narrow** silently
    drops matching rows — the page is well formed, the total looks plausible, and the client has
    no way to know. That is the failure mode this test exists to make impossible, so it does not
    inspect the hint at all: it runs the same compiled filter down the hinted path and the linear
    path and demands identical ids in identical order, then checks both against a brute-force
    sweep of the corpus so that "both paths agree" cannot mean "both are wrong the same way".

    The disjunction and negation cases are the ones that matter. A record can satisfy an ``any``
    through a branch that says nothing about ``service``; a ``not`` describes precisely the
    records to exclude. Harvesting a hint from either would produce a candidate set that is
    confidently, specifically wrong.
    """
    store = hint_store()
    flt = compiled(tree, SortOrder.DESC)

    hinted = store.scan(flt, SortOrder.DESC, limit=HINT_CORPUS_SIZE)
    linear = store.scan(Unhinted(flt), SortOrder.DESC, limit=HINT_CORPUS_SIZE)
    brute = [
        record.entry.id
        for record in reversed(list(store.iter_matching(Unhinted(flt), SortOrder.ASC)))
    ]

    assert ids_of(hinted.items) == ids_of(linear.items), label
    assert ids_of(hinted.items) == brute, label
    assert store.count(flt) == store.count(Unhinted(flt)) == len(brute), label
    # Ascending too: the hinted walk merges its candidate lists in the scan direction, and a
    # bisect that is right in one direction can be off by one in the other.
    assert ids_of(store.scan(flt, SortOrder.ASC, limit=HINT_CORPUS_SIZE).items) == list(
        reversed(brute)
    ), label


def test_only_sound_shapes_produce_a_hint() -> None:
    """The hint is *taken* where it is sound and *declined* everywhere else.

    :func:`test_index_hint_is_never_unsound` proves the answers agree; this proves the hinted path
    is actually being exercised by some of those cases rather than all twelve of them quietly
    falling back to a linear scan and agreeing trivially.
    """
    store = hint_store()

    hinted = compiled(leaf("service", "eq", "svc-3")).index_hint(store)
    assert hinted is not None
    candidates = sum(len(seqs) for seqs in hinted)
    assert candidates == HINT_CORPUS_SIZE // len(HINT_SERVICES)
    assert candidates * INDEX_HINT_MIN_SELECTIVITY <= HINT_CORPUS_SIZE, (
        "the fixture must be selective enough that the planner actually follows the hint, or "
        "this suite never exercises the hinted walk at all"
    )

    # The most selective constrained dimension wins: 20 seqs for one service beats 48 for a level.
    both = compiled({"all": [leaf("service", "eq", "svc-3"), leaf("level", "eq", "ERROR")]})
    assert sum(len(seqs) for seqs in both.index_hint(store) or []) == candidates

    # Everything unsound (or unindexed) declines.
    for tree in (
        {"any": [leaf("service", "eq", "svc-3"), leaf("level", "eq", "FATAL")]},
        {"not": leaf("service", "eq", "svc-3")},
        {"all": [{"not": leaf("service", "eq", "svc-3")}]},
        {"all": [{"any": [leaf("service", "eq", "svc-3")]}]},
        leaf("service", "ne", "svc-3"),
        leaf("service", "nin", ["svc-3"]),
        leaf("service", "contains", "svc-3"),
        leaf("message", "contains", "request"),
        leaf("ts", "gte", BASE_TS.isoformat()),
        None,
    ):
        assert compiled(tree).index_hint(store) is None, tree

    # A contradiction is the one case that hints an EMPTY candidate list rather than None: two
    # `eq`s on one dimension intersect, and nothing can satisfy both. `[]` and `None` mean
    # different things to the store, which is why this is asserted rather than assumed.
    contradiction = compiled(
        {"all": [leaf("host", "eq", "node-1"), leaf("host", "eq", "node-2")]}
    )
    assert contradiction.index_hint(store) == []


def test_hint_for_a_value_absent_from_the_store_is_empty_not_none() -> None:
    """A constrained dimension holding none of the requested values is a real zero-match answer.

    ``[]`` says "I looked, there is nothing"; ``None`` says "I have no opinion, scan everything".
    Confusing the two would turn a filter for a service that has never logged into a full sweep.
    """
    store = hint_store()
    flt = compiled(leaf("service", "eq", "svc-does-not-exist"))

    assert flt.index_hint(store) == []
    assert store.scan(flt, SortOrder.DESC, limit=10).items == []
    assert store.count(flt) == 0


def test_compiled_filter_matches_equivalent_flat_filter() -> None:
    """A compiled tree and the flat ``Filter`` for the same predicate return the same rows.

    This is the guarantee the whole design rests on: ``GET /logs`` and ``POST /logs/search`` are
    two vocabularies over **one** evaluator. If the two ever disagreed for a predicate both can
    express, then "the same filter means the same thing on every route" would be prose rather
    than a property — and the stats route, which shares the same filter, would agree with
    whichever of them it happened to be built from.
    """
    store = hint_store()
    flat = Filter(levels=frozenset({"ERROR", "FATAL"}), services=frozenset({"svc-1"}))
    tree = compiled(
        {
            "all": [
                leaf("level", "in", ["ERROR", "FATAL"]),
                leaf("service", "eq", "svc-1"),
            ]
        }
    )

    for order in (SortOrder.DESC, SortOrder.ASC):
        expected = store.scan(flat, order, limit=HINT_CORPUS_SIZE)
        actual = store.scan(tree, order, limit=HINT_CORPUS_SIZE)
        assert ids_of(actual.items) == ids_of(expected.items), order
        assert actual.items, "the fixture must produce a non-empty match set to prove anything"

    assert store.count(tree) == store.count(flat)
    # The two fingerprints differ by design (see `test_fingerprint_is_stable_and_semantic`), so a
    # cursor cannot cross routes even though the match set is identical.
    assert tree.fingerprint() != flat.fingerprint()

    # The same equivalence for a time range and a substring, which take the other code paths:
    # `ts_epoch` comparisons and the precomputed `message_lower`.
    window_flat = Filter(
        since_epoch=(BASE_TS + timedelta(seconds=50)).timestamp(),
        until_epoch=(BASE_TS + timedelta(seconds=100)).timestamp(),
        q_lower="completed",
    )
    window_tree = compiled(
        {
            "all": [
                leaf("ts", "gte", (BASE_TS + timedelta(seconds=50)).isoformat()),
                leaf("ts", "lte", (BASE_TS + timedelta(seconds=100)).isoformat()),
                leaf("message", "contains", "COMPLETED"),
            ]
        }
    )

    assert ids_of(store.scan(window_tree, SortOrder.DESC, limit=500).items) == ids_of(
        store.scan(window_flat, SortOrder.DESC, limit=500).items
    )
    assert store.count(window_tree) == 51, "both `ts` bounds are inclusive"


def test_empty_filter_short_circuits_the_count() -> None:
    """``is_empty`` is what lets ``page.total`` cost O(1) on the most common request there is."""
    store = hint_store()

    assert compiled(None).is_empty is True
    assert store.count(compiled(None)) == HINT_CORPUS_SIZE
    assert store.count(compiled({"all": []})) == HINT_CORPUS_SIZE
    # A filter that matches nothing is emphatically not "empty" — it must walk and find zero.
    assert store.count(compiled({"any": []})) == 0
