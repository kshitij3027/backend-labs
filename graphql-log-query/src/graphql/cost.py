"""Pre-execution cost analysis — spec §2 item 33, §5 "complexity analysis blocks expensive operations".

A budget that is checked *after* the database has answered protects nothing. So this runs where a
rejection is still free: as a :class:`~graphql.validation.ValidationRule`, during **validation**,
after the document is parsed and before a single resolver is invoked. An over-budget operation
therefore issues zero SQL statements, opens zero sessions and allocates zero DataLoaders — which is
asserted with a statement counter and a resolver spy in ``tests/integration/test_cost_gate.py``,
because "before execution" is exactly the kind of claim that quietly stops being true.

.. rubric:: Strawberry ships no complexity extension, so this module is the whole implementation

``strawberry/extensions/`` contains ``query_depth_limiter.py``, ``max_tokens.py``,
``max_aliases.py``, ``add_validation_rules.py``, ``mask_errors.py``, ``validation_cache.py`` and
``parser_cache.py`` — and **no** ``complexity.py`` or ``cost.py``. Depth, tokens and aliases are
therefore Strawberry's; the cost model is ours, written in the shape of its ``QueryDepthLimiter``
(a validator class built by a factory, subclassing ``ValidationRule``, reporting through
``context.report_error``, installed via ``AddValidationRules``) so the two read as siblings.

.. rubric:: Depth and cost are different bounds and neither one implies the other

``MAX_QUERY_DEPTH`` catches the *narrow and deep* document — ``logs { relatedLogs { relatedLogs …`` —
which is a **stack** problem: cyclic types let a client nest forever and every level is another
resolver frame. ``MAX_QUERY_COMPLEXITY`` catches the *shallow and wide* one —
``logs(limit: 500) { … }`` — which is a **row** problem: one level deep, half a million cells on the
wire. A depth limit scores a 500-row query at 1 and a complexity limit scores a 12-deep query on one
row at almost nothing, so shipping either alone leaves the other attack entirely open. Tokens and
aliases close the remaining two (an enormous but shallow document, and one field requested ten
thousand times under ten thousand aliases); all four are stacked in :mod:`src.graphql.schema`.

.. rubric:: The formula

Every field carries a **static weight**, and every field is charged that weight *once per time it
will be resolved* — which is the product of the list sizes above it::

    cost(selection set, M) = Σ over fields f:  weight(f) × M  +  cost(f's selection set, M × size(f))

with ``M = 1`` at the root of the operation. ``size(f)`` is 1 unless ``f`` is a list, in which case
it is the number of entries the client asked for. **Nesting therefore multiplies rather than
adds**, which is the entire point: ``logs(limit: 10) { relatedLogs { id } }`` is ten separate
correlated lookups, not one lookup plus one correlation. That is also the gradient spec §3 Feature
Area D asks for once C10 makes it a three-entity traversal — the same shape costs 1,110 at ten
parents and 55,010 at five hundred, so the budget can admit the first and refuse the second.

Note that a field with no sub-selection — every scalar leaf — is charged ``weight × M`` and
multiplies nothing further. The multiplier answers "how many times is the *child* selection set
resolved", and a leaf has no children; that is also why a list of scalars (``logStats.services``)
costs one unit rather than a hundred.

.. rubric:: What the shipped budget admits, and the first shape that trips it

``MAX_QUERY_COMPLEXITY`` is **25,000**, calibrated on one requirement: **one** level of correlation
at ``DEFAULT_QUERY_LIMIT`` must be admitted, because that is the query this schema exists to serve
(spec §2 items 17 and 29, and the reason the C5 DataLoader exists at all). One field on each level
prices at 11,110 and two on each at 21,210, so both run. Refused are the shapes the multiplication
runs away with: two levels of correlation (1,101,010 — forty-four times the budget) and a
``MAX_QUERY_LIMIT``-wide page with one level attached (55,010).

The boundary in between, stated so a reader can find it without re-deriving it: at the default page
size every additional field selected under ``relatedLogs`` costs ``100 × 100 = 10,000``, so the
budget affords two of them, and the first realistic shape it trips is the full seven-field
projection on *both* levels past 34 parents (``10 + N × 717 > 25,000`` at ``N = 35``). The weights
below are calibrated against that boundary; ``.env.example`` carries the same table for operators,
and ``tests/unit/test_cost.py`` pins both of its sides so neither can move unnoticed.

.. rubric:: C11: the same budget, unchanged, now also gating the e-commerce traversals

Spec §3 Feature Area D asks for "complexity analysis tuned so deep nested e-commerce queries are
rejected", and the tuning turned out to be **weights only** — ``MAX_QUERY_COMPLEXITY`` did not move
and did not need to, which is worth stating because widening a budget to admit a query one has just
made expensive is the failure C8 already met once. The nested traversals
(``OrderEvent.payments`` / ``.userActivity`` / ``.relatedLogs``, ``UserEvent.orders``,
``PaymentEvent.order``) are each one batched indexed read, so they are each priced at 10 — exactly
what ``LogEntry.relatedLogs`` costs, because it is exactly the same work — and the multiplication
does the rest. One traversal over a full page is 11,010 and runs; three of them over a full page is
33,010 and does not; the flagship dossier declares ``limit: 10`` and prices at 10,360. The
three cached aggregates carry an explicit ``size`` (7, 7 and 20 — their vocabularies) because a
``GROUP BY`` returns one row per bucket rather than one per matching row, so the whole dashboard
query prices at 220. Every one of those numbers is pinned in ``tests/unit/test_ecommerce_cost.py``.

.. rubric:: THE MOST IMPORTANT LINE IN THIS FILE: an omitted bound is not a free query

``LogEntry.relatedLogs`` takes **no client-supplied size argument at all**, and ``logs`` may omit
``filters.limit``. If an unbounded list scored ``size = 0`` — or ``1``, which is the same mistake
wearing a nicer number — then the single most expensive field in the schema would be the cheapest
thing a client could ask for, and the gate would be decorative. So an unbounded list is priced at
``DEFAULT_QUERY_LIMIT``: exactly the number of rows the server will actually return. See
:meth:`CostConfig.list_size`, which resolves and clamps through the same ``[1, MAX_QUERY_LIMIT]``
window :func:`src.db.repository.clamp_limit` applies at execution time — the cost model prices what
the executor will really do, so ``limit: 0`` is charged for the one row it returns and
``limit: 100000`` for the five hundred it is clamped to, and neither is a way round the gate.

.. rubric:: A declared page size travels to the nearest list beneath it

``logsConnection(first: 25)`` returns ``LogConnection``, an **object** — the list is one level down
at ``edges``. Charging ``edges`` the default assumption would ignore ``first`` entirely and price a
25-row page at 100, so a declared size is carried down the walk and consumed by the first list field
that has no bound of its own. That rule is general (it is why C11's e-commerce connections need no
special case) and it is why ``totalCount`` and ``pageInfo``, which sit beside ``edges`` rather than
under it, are charged once rather than twenty-five times — which is correct: they are one COUNT and
one struct, however big the page is.

.. rubric:: Fragments are resolved and counted, not skipped

An attacker who moves the expensive half of a document into a fragment must not get a free pass, so
a spread is walked as if it had been written inline — ``tests/unit/test_cost.py`` asserts *equality*
between the two spellings rather than merely that both are non-zero. Fragment **cycles** are
survived by tracking the spreads active on the current path: graphql-core has its own
``NoFragmentCyclesRule``, but it runs in the same pass as this one and nothing orders it first, so a
walker that trusted it would hang inside a live request rather than fail one.

.. rubric:: Variables

``logs(filters: $f)`` hides the bound where the AST cannot see it. Supplied variable values are
threaded into the rule from the execution context and read through
(:meth:`_Walker._resolve_value` also honours a variable's *declared default*, which is where a
generated client usually puts its page size). When the value is genuinely unknowable at validation
time the fallback is the **default assumption**, never zero — the same rule as an omitted argument,
for the same reason.

.. rubric:: Introspection is free, deliberately rather than accidentally

A field whose name begins with ``__`` costs nothing and its subtree is not walked. GraphiQL's
introspection query is deep, wide and entirely bounded by the schema's own size: a client cannot
make it bigger, and it cannot touch the database. Pricing it would break the playground for no
security gain, and Strawberry's ``QueryDepthLimiter`` makes exactly the same exemption
(``is_introspection_key``), so depth and cost agree about what introspection is worth. There is an
integration test asserting the real ``get_introspection_query()`` survives the shipped defaults,
because that is the regression that would make the IDE unusable.

.. rubric:: Subscriptions are costed per EVENT, and that is the honest unit

``Subscription.logStream`` yields one ``LogEntry`` per published event, so the walk of a
subscription document prices **one delivery**, not the stream. That is the number worth bounding:
the stream's length is bounded elsewhere (``SUBSCRIPTION_QUEUE_MAXSIZE`` and the drop-on-overflow
policy in :mod:`src.broker`), while what this bounds is the work done for every entry that arrives —
``logStream { relatedLogs { relatedLogs { … } } }`` would otherwise run a correlated fan-out per
event, for as long as the socket stays open.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Optional

from graphql import (
    GraphQLField,
    GraphQLInterfaceType,
    GraphQLList,
    GraphQLNamedType,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSchema,
)
from graphql.language import (
    DocumentNode,
    FieldNode,
    FragmentDefinitionNode,
    FragmentSpreadNode,
    InlineFragmentNode,
    IntValueNode,
    NamedTypeNode,
    ObjectValueNode,
    OperationDefinitionNode,
    OperationType,
    SelectionSetNode,
    ValueNode,
    VariableNode,
)
from graphql.validation import ValidationContext, ValidationRule
from strawberry.extensions import AddValidationRules

from src.config import Settings, get_settings
from src.graphql.errors import CostLimitExceededError

# =================================================================================================
# The weight table — DATA, not branches
#
# C11 tunes this for the e-commerce types (spec §3 Feature Area D: "complexity analysis tuned so
# deep nested e-commerce queries are rejected"). It must be able to do that by adding rows, so the
# walker below never mentions a field name: it looks every field up here and falls back to
# DEFAULT_COST. A key is "TypeName.fieldName"; a bare "fieldName" key is also honoured, which is
# what prices a field the same way wherever it appears (and what keeps a weight meaningful when the
# parent type cannot be resolved, e.g. under a union or in an otherwise-invalid document).
# =================================================================================================

#: What an unlisted field costs. One, not zero: a query is charged for the cells it puts on the
#: wire, so `logs { id service level message }` at the default limit is 4 x 100 rather than nothing.
DEFAULT_FIELD_WEIGHT = 1


@dataclass(frozen=True, slots=True)
class FieldCost:
    """The price of one field, and optionally the cardinality of the list it returns.

    Attributes:
        weight: Charged once per resolution — i.e. multiplied by every list size above the field.
            It is a statement about *how expensive this field is to produce once*: a filtered index
            read, a correlated lookup and a ``GROUP BY`` over the whole window are not the same
            thing and a flat model that priced them identically would reject the wrong queries.
        size: For a list whose cardinality is the **server's** rather than the client's. ``None``
            (the default) means "ask the client's arguments, and assume ``DEFAULT_QUERY_LIMIT``
            when they say nothing" — which is the right answer for every list a client can widen.
            ``LogStats.levelBreakdown`` can never exceed the five members of ``LogLevel``, so
            charging it a hundred would price a cheap, bounded aggregate like a table scan.
    """

    weight: int = DEFAULT_FIELD_WEIGHT
    size: Optional[int] = None


#: The default cost of a field nobody has weighted.
DEFAULT_COST = FieldCost()

#: The explicit table. Only the fields whose cost is *not* one unit appear; everything else — every
#: scalar on ``LogEntry``, every field on ``PageInfo`` — is deliberately absent and takes
#: :data:`DEFAULT_COST`. The numbers are calibrated against ``MAX_QUERY_COMPLEXITY``; see the
#: calibration table in ``tests/unit/test_cost.py``, which pins several of these totals exactly.
DEFAULT_WEIGHTS: Mapping[str, FieldCost] = MappingProxyType(
    {
        # --- Query roots -------------------------------------------------------------------------
        #: One filtered SELECT over an indexed column, capped by `limit`.
        "Query.logs": FieldCost(weight=10),
        #: The same read PLUS a COUNT over the entire filter set (`totalCount` deliberately answers
        #: "how big is this result", so it cannot be bounded by the page) PLUS cursor decoding.
        "Query.logsConnection": FieldCost(weight=15),
        #: One row by primary key, batched with its siblings by the per-operation DataLoader.
        "Query.log": FieldCost(weight=5),
        #: Two GROUP BY-class scans over the whole window with **no LIMIT at all** — the one field
        #: here whose work does not shrink when the client asks for less.
        "Query.logStats": FieldCost(weight=30),
        # --- Query roots: the C10 e-commerce entry points -----------------------------------------
        #
        # A list field with NO entry in this table is priced at DEFAULT_COST — weight 1 — which is a
        # hole in the gate, not a neutral default: `orderEvents(filters: {limit: 500})` would score
        # 1 for the read itself and be indistinguishable from selecting a scalar. So every new list
        # field gets a row here, and `tests/unit/test_cost.py` asserts that (the table is checked
        # against the schema's actual list fields rather than against a hand-written list).
        #
        #: One filtered SELECT over an indexed column, capped by `limit` — the same work `Query.logs`
        #: does against a table with the same index shape, so deliberately the same weight. Pricing
        #: them differently would be a claim about relative cost that nothing has measured.
        "Query.orderEvents": FieldCost(weight=10),
        "Query.userEvents": FieldCost(weight=10),
        "Query.paymentEvents": FieldCost(weight=10),
        #: FOUR indexed reads across four tables in one field, each capped independently — so the
        #: worst case is 4 x MAX_QUERY_LIMIT rows from one selection. Priced at 4x a single list
        #: read for exactly that reason: the multiplier the walker applies to the sub-selection is
        #: the client's `limit`, which prices the ROWS, and this weight prices the four ROUND TRIPS
        #: the field costs however few rows come back. `{ correlatedEvents(traceId: "…") { timestamp
        #: service } }` therefore prices at 40 + 2 x 100 = 240, comfortably inside the 25,000 budget
        #: and comfortably above a scalar.
        "Query.correlatedEvents": FieldCost(weight=40),
        # --- Query roots: the C11 by-id entry points ----------------------------------------------
        #
        #: One row by primary key, batched with its siblings by the per-operation DataLoader —
        #: deliberately the same weight as `Query.log`, which does exactly the same work against a
        #: table with exactly the same primary key. Pricing them differently would be a claim about
        #: relative cost that nothing has measured.
        "Query.orderEvent": FieldCost(weight=5),
        "Query.paymentEvent": FieldCost(weight=5),
        "Query.userEvent": FieldCost(weight=5),
        # --- Query roots: the C11 cached aggregates (spec §3 Feature Area D) -----------------------
        #
        # ALL THREE CARRY AN EXPLICIT `size`, and that is what makes them affordable. They are lists
        # the SERVER sizes rather than the client: the row count of a GROUP BY over a controlled
        # vocabulary is bounded by the VOCABULARY, never by the corpus. Without a `size` each would
        # inherit the DEFAULT_QUERY_LIMIT assumption and the three-panel dashboard query would price
        # at roughly fourteen times what it costs — a gate rejecting the dashboard it was built for,
        # which is the exact miscalibration C8's budget of 1000 already made once. The sizes are
        # pinned against `len(OrderStatus)` and `len(PaymentMethod) x len(PaymentOutcome)` by a unit
        # test rather than trusted, because an enum gaining a member has to move them.
        #
        #: DISTINCT ON over the order stream and then a GROUP BY over the result — one pass with a
        #: per-order seek, i.e. a shade more work than `logStats`' pair of scans. 7 OrderStatus
        #: members, so at most 7 buckets whatever the corpus size.
        "Query.orderStatusDistribution": FieldCost(weight=35, size=7),
        #: A COUNT(DISTINCT order_id) per group — the most expensive of the three, because a
        #: distinct count needs a sort or a hash per group where COUNT(*) needs a counter. Same 7
        #: buckets.
        "Query.orderFunnel": FieldCost(weight=40, size=7),
        #: One GROUP BY with a plain and a distinct count in the same pass — the same shape as
        #: `logStats`, hence the same weight. Bounded by 5 methods x 4 outcomes = 20 cells.
        "Query.paymentOutcomeBreakdown": FieldCost(weight=30, size=20),
        # --- LogEntry ----------------------------------------------------------------------------
        #: The correlated lookup. C5's DataLoader collapses N of these into one statement, so the
        #: weight prices rows rather than round trips: batching bounds the number of queries, not
        #: the number of entries a trace group can contain.
        "LogEntry.relatedLogs": FieldCost(weight=10),
        # --- C11: the nested e-commerce traversals (spec §3 Feature Areas B and D) -----------------
        #
        # THE GRADIENT SPEC §3 FEATURE AREA D ASKS FOR ("complexity analysis tuned so deep nested
        # e-commerce queries are rejected") IS ALREADY IN THE MODEL — nesting MULTIPLIES — so what
        # these weights have to do is make one level affordable and two levels not. They are all 10,
        # the same as `LogEntry.relatedLogs`, and the sameness is the calibration rather than
        # laziness: every one of them is one batched `WHERE <key> IN (…)` against an indexed column,
        # which is the identical unit of work, and pricing identical work differently would put a
        # number in the table that no measurement supports.
        #
        # None carries a `size`. Every one of these lists is unbounded from the client's side — an
        # order can have any number of payment events, a user any number of activities — so they
        # take THE default assumption (DEFAULT_QUERY_LIMIT rows), exactly as `relatedLogs` does and
        # for the reason the module docstring calls the most important line in the file: an omitted
        # bound is not a free query. Giving them a small `size` because the seeded corpus happens to
        # produce three payments per order would price the corpus rather than the schema.
        #
        # WHAT THIS BUYS, at the shipped 25,000 with DEFAULT_QUERY_LIMIT = 100 (arithmetic pinned in
        # `tests/unit/test_ecommerce_cost.py`, both sides of the boundary):
        #   ADMITTED  { orderEvents { payments { id } } }                      11,010
        #   ADMITTED  the flagship dossier at limit 10                         10,360
        #   REJECTED  { orderEvents { payments { id } userActivity { id }
        #                             relatedLogs { id } } }                   33,010
        #   REJECTED  { orderEvents { payments { order { payments { id } } } } } 1,151,010
        # The middle two are the interesting pair: three traversals over a hundred parents is
        # refused, and the same three over ten is served. That is the gate doing its job rather than
        # a wall — the fix a client is told to apply is `limit`, and it works.
        #
        #: The order -> payments edge. One batched read of an order's payment stream.
        "OrderEvent.payments": FieldCost(weight=10),
        #: The order -> user edge, traversed into that user's activity stream.
        "OrderEvent.userActivity": FieldCost(weight=10),
        #: The correlation edge into `log_entries` — the SAME loader `LogEntry.relatedLogs` uses, so
        #: deliberately the same weight. A client selecting both must be charged the same for each.
        "OrderEvent.relatedLogs": FieldCost(weight=10),
        #: The payment -> order edge. NOT a list — it is the head of the order's history — so it
        #: multiplies nothing further and is priced like the other single-row batched lookups.
        #: That makes it the cheapest field in this block and the most dangerous one, because it is
        #: what lets a document alternate `payments { order { payments { … } } }` and multiply
        #: without ever selecting a list twice in a row. The budget catches that at 1,111,010.
        "PaymentEvent.order": FieldCost(weight=5),
        "PaymentEvent.relatedLogs": FieldCost(weight=10),
        #: The order -> user edge read from the user's side.
        "UserEvent.orders": FieldCost(weight=10),
        "UserEvent.relatedLogs": FieldCost(weight=10),
        # --- LogStats: lists whose length the server, not the client, decides -------------------
        #: Bounded by the number of distinct services in the window (the generated corpus has a
        #: handful). Conservative rather than exact, because it is an assumption either way.
        "LogStats.serviceBreakdown": FieldCost(size=12),
        #: Bounded by the LogLevel enum, exactly.
        "LogStats.levelBreakdown": FieldCost(size=5),
        # --- Connection wrappers -----------------------------------------------------------------
        #: A COUNT(*) over the filter set, charged once per connection rather than once per row —
        #: it sits beside `edges`, not under it, so the page multiplier never reaches it.
        "LogConnection.totalCount": FieldCost(weight=5),
        # --- Mutation ----------------------------------------------------------------------------
        #: One INSERT plus the broker publish and its Redis fan-out.
        "Mutation.createLog": FieldCost(weight=10),
        #: C12. Exactly the same work against a table with the same shape — one INSERT, one commit,
        #: one fan-out over the same bounded queues — so deliberately the same weight. Pricing them
        #: differently would be a claim about relative cost that nothing has measured.
        "Mutation.createOrderEvent": FieldCost(weight=10),
        # --- Subscription (priced PER EVENT — see the module docstring) --------------------------
        "Subscription.logStream": FieldCost(weight=10),
        #: C12, spec §3 Feature Area C. Priced per DELIVERED EVENT exactly as `logStream` is, and at
        #: the same weight, because one delivery is one dequeue and one serialisation either way.
        #:
        #: The unit matters more here than it does for `logStream`, so it is worth stating: this
        #: field returns `OrderEvent!` — ONE event, not a list — so the root multiplier stays 1 and
        #: what is being priced is genuinely "the work one arriving transition costs". The stream's
        #: LENGTH is bounded elsewhere (SUBSCRIPTION_QUEUE_MAXSIZE and the drop policy in
        #: `src.broker`); what this bounds is what a client can attach to each event, for as long as
        #: the socket stays open.
        #:
        #: That is a real exposure, because `OrderEvent` carries THREE traversals where `LogEntry`
        #: carries one. The arithmetic, pinned in `tests/unit/test_order_stream_cost.py` rather than
        #: trusted here:
        #:   ADMITTED  orderStatusStream { orderId status }                             12
        #:   ADMITTED  ... { payments { id } }                                         120
        #:   ADMITTED  ... { payments { id } userActivity { id } relatedLogs { id } }   340
        #:   REJECTED  ... { payments { order { payments { order { payments { id }
        #:                                                        } } } } }      1,151,520
        #: The last one is the alternating list/single shape C11's `PaymentEvent.order` note calls
        #: the most dangerous in the schema, reached through the subscription root instead of
        #: through `orderEvents` — and the same multiplication refuses it here.
        "Subscription.orderStatusStream": FieldCost(weight=10),
    }
)

#: Argument names that declare how many entries a field will produce, checked in this order at the
#: top level of a field's arguments and then one level inside an input object (``filters.limit``).
#: Data for the same reason the weights are: C11's connections spell it ``first`` and this project's
#: core field spells it ``limit``, and neither should require a change to the walker.
SIZE_ARGUMENTS: tuple[str, ...] = ("first", "limit", "last")


@dataclass(frozen=True)
class CostConfig:
    """Everything the walker needs, resolved from :class:`~src.config.Settings` in one place.

    A plain value object rather than a live settings reference, so the walk cannot consult a
    process-wide global halfway through and so a test can price a document under budgets that
    differ from the ones this process booted with. :class:`QueryCostLimiter` builds one per
    operation from the operation's own context; see its docstring for why that indirection exists.
    """

    #: ``MAX_QUERY_COMPLEXITY``. An operation is rejected when its cost is **greater than** this,
    #: so a document costing exactly the budget is accepted — both sides of that boundary are
    #: pinned by an integration test.
    max_complexity: int
    #: ``DEFAULT_QUERY_LIMIT``: what an unbounded list is assumed to return. See the module
    #: docstring — this is the assumption the whole gate rests on.
    default_list_size: int
    #: ``MAX_QUERY_LIMIT``: the ceiling the executor clamps every requested size to, mirrored here
    #: so an absurd ``limit`` is priced at what it will actually cost rather than at what it says.
    max_list_size: int
    #: The weight table. Swappable so C11 can extend it and so a test can price against a table it
    #: wrote itself.
    #:
    #: ``default_factory``, not a plain default, and NOT because the table is mutable — it is a
    #: ``MappingProxyType`` precisely so it is not. Python 3.11 changed the dataclass guard from
    #: "is a list/dict/set" to "is unhashable", and ``mappingproxy.__hash__`` is ``None``, so the
    #: obvious spelling raises `ValueError: mutable default ... use default_factory` at IMPORT
    #: time. The image is python:3.11-slim, `src/main.py` imports this module, so that spelling
    #: is not a test failure — it is the service failing to boot. The factory returns the same
    #: shared proxy every time; nothing is copied.
    weights: Mapping[str, FieldCost] = field(default_factory=lambda: DEFAULT_WEIGHTS)
    #: How many field nodes the analyser will visit before giving up and rejecting the operation
    #: unpriced. ``MAX_QUERY_TOKENS`` already bounds the document, but fragments let a small
    #: document describe an exponentially large tree, and an analyser that can be made to run for a
    #: minute is itself the denial of service it was installed to prevent.
    max_analysed_nodes: int = 50_000

    @classmethod
    def from_settings(
        cls, settings: Settings, weights: Optional[Mapping[str, FieldCost]] = None
    ) -> CostConfig:
        """Read the four numbers this gate needs off ``settings``."""
        return cls(
            max_complexity=settings.max_query_complexity,
            default_list_size=settings.default_query_limit,
            max_list_size=settings.max_query_limit,
            weights=weights if weights is not None else DEFAULT_WEIGHTS,
        )

    def list_size(self, requested: Optional[int]) -> int:
        """How many entries a list field will really produce, given what the client asked for.

        Mirrors :func:`src.db.repository.clamp_limit` exactly — ``None`` becomes
        ``DEFAULT_QUERY_LIMIT`` and anything else is clamped into ``[1, MAX_QUERY_LIMIT]`` — and a
        unit test asserts the two agree across a table of inputs rather than trusting this comment.
        The mirroring is the point: a cost model that priced ``limit: 0`` at zero would hand every
        client a one-word bypass, because the executor clamps that same 0 up to one real row.
        """
        if requested is None:
            # THE default assumption. An unbounded list is not a free list; see the module docstring.
            return self.default_list_size
        return max(1, min(int(requested), self.max_list_size))

    def cost_of(self, type_name: Optional[str], field_name: str) -> FieldCost:
        """The table entry for ``Type.field``, then for ``field``, then the default."""
        if type_name is not None:
            specific = self.weights.get(f"{type_name}.{field_name}")
            if specific is not None:
                return specific
        return self.weights.get(field_name, DEFAULT_COST)


@dataclass(frozen=True)
class OperationCost:
    """What one operation in the document was priced at.

    Attributes:
        node: The operation definition, carried so a rejection can point at it.
        name: The operation name, or ``None`` for an anonymous operation.
        cost: The computed complexity.
        truncated: ``True`` when the walk hit :attr:`CostConfig.max_analysed_nodes` and stopped, in
            which case ``cost`` is a **lower bound** and the operation is rejected on that basis
            alone. Kept as a distinct flag rather than folded into a huge number, so the error can
            say which of the two things happened.
    """

    node: OperationDefinitionNode
    name: Optional[str]
    cost: int
    truncated: bool = False


class _AnalysisBudgetExceeded(Exception):
    """Internal: the walk visited more nodes than :attr:`CostConfig.max_analysed_nodes` allows."""

    def __init__(self, partial_cost: int) -> None:
        super().__init__("query cost analysis budget exceeded")
        self.partial_cost = partial_cost


# =================================================================================================
# The walk
# =================================================================================================


def _unwrap(type_: Any) -> Any:  # noqa: ANN401 - any GraphQLType, wrappers included
    """Strip ``!`` and ``[]`` wrappers down to the named type."""
    while isinstance(type_, (GraphQLNonNull, GraphQLList)):
        type_ = type_.of_type
    return type_


def _is_list(type_: Any) -> bool:  # noqa: ANN401 - any GraphQLType, wrappers included
    """Does this type produce many values? ``[X]``, ``[X]!`` and ``[X!]!`` all do."""
    if isinstance(type_, GraphQLNonNull):
        type_ = type_.of_type
    return isinstance(type_, GraphQLList)


def _field_definition(parent_type: Optional[GraphQLNamedType], name: str) -> Optional[GraphQLField]:
    """The schema's definition of ``parent_type.name``, or ``None`` when it cannot be resolved.

    ``None`` is an ordinary answer rather than an error: this rule runs in the same pass as
    graphql-core's own ``FieldsOnCorrectTypeRule``, so it is routinely handed documents naming
    fields that do not exist. Such a field is priced at the default weight and multiplies nothing —
    the *real* validation error is reported by the rule that owns it, and this one must not crash on
    the way there.
    """
    if isinstance(parent_type, (GraphQLObjectType, GraphQLInterfaceType)):
        return parent_type.fields.get(name)
    return None


class _Walker:
    """Prices one document. Instantiated per analysis; holds the node budget as it counts down."""

    def __init__(
        self,
        schema: GraphQLSchema,
        document: DocumentNode,
        config: CostConfig,
        variables: Mapping[str, Any],
    ) -> None:
        self._schema = schema
        self._config = config
        self._variables = variables
        self._fragments: dict[str, FragmentDefinitionNode] = {
            definition.name.value: definition
            for definition in document.definitions
            if isinstance(definition, FragmentDefinitionNode)
        }
        self._variable_defaults: dict[str, ValueNode] = {}
        self._visited = 0
        #: Running total, kept alongside the recursion's return value for one reason: a walk that
        #: is cut short by the node budget still has to report an honest lower bound, and the sum
        #: it never finished returning is not available to the handler that catches it.
        self._charged = 0

    # -- entry point -------------------------------------------------------------------------

    def operation_cost(self, operation: OperationDefinitionNode) -> OperationCost:
        """Price one operation, from its root type down."""
        self._visited = 0
        self._charged = 0
        self._variable_defaults = {
            definition.variable.name.value: definition.default_value
            for definition in operation.variable_definitions or ()
            if definition.default_value is not None
        }
        name = operation.name.value if operation.name is not None else None

        try:
            cost = self._selection_set_cost(
                operation.selection_set,
                parent_type=self._root_type(operation),
                multiplier=1,
                page_size=None,
                active_fragments=frozenset(),
            )
        except _AnalysisBudgetExceeded as exceeded:
            return OperationCost(
                node=operation, name=name, cost=exceeded.partial_cost, truncated=True
            )

        return OperationCost(node=operation, name=name, cost=cost)

    def _root_type(self, operation: OperationDefinitionNode) -> Optional[GraphQLNamedType]:
        """The schema root the operation starts from."""
        if operation.operation is OperationType.MUTATION:
            return self._schema.mutation_type
        if operation.operation is OperationType.SUBSCRIPTION:
            # Priced per delivered event; see the module docstring.
            return self._schema.subscription_type
        return self._schema.query_type

    # -- the recursion -----------------------------------------------------------------------

    def _selection_set_cost(
        self,
        selection_set: SelectionSetNode,
        parent_type: Optional[GraphQLNamedType],
        multiplier: int,
        page_size: Optional[int],
        active_fragments: frozenset[str],
    ) -> int:
        """Sum the cost of every selection in one set.

        Args:
            selection_set: What to price.
            parent_type: The type the selections are being read off, or ``None`` when it could not
                be resolved (an invalid document, or a union).
            multiplier: How many times this whole set will be resolved — the product of the list
                sizes above it.
            page_size: A size declared by an ancestor that no list has consumed yet. See the
                module docstring's note on ``logsConnection(first:)`` reaching ``edges``.
            active_fragments: Fragment names already open on this path. The cycle guard.
        """
        total = 0
        for selection in selection_set.selections:
            if isinstance(selection, FieldNode):
                total += self._field_cost(
                    selection, parent_type, multiplier, page_size, active_fragments
                )
            elif isinstance(selection, InlineFragmentNode):
                total += self._selection_set_cost(
                    selection.selection_set,
                    parent_type=self._condition_type(selection.type_condition, parent_type),
                    multiplier=multiplier,
                    page_size=page_size,
                    active_fragments=active_fragments,
                )
            elif isinstance(selection, FragmentSpreadNode):
                total += self._spread_cost(
                    selection, parent_type, multiplier, page_size, active_fragments
                )
        return total

    def _spread_cost(
        self,
        spread: FragmentSpreadNode,
        parent_type: Optional[GraphQLNamedType],
        multiplier: int,
        page_size: Optional[int],
        active_fragments: frozenset[str],
    ) -> int:
        """Price a named fragment as if it had been written inline.

        Two ways this returns 0, both of them deliberate and neither an error:

        * **The fragment is already open on this path.** That is a cycle, and following it would
          hang the analyser inside a live request. graphql-core reports the cycle itself through
          ``NoFragmentCyclesRule`` — in the *same* validation pass, with no ordering guarantee — so
          this rule cannot rely on never being handed one.
        * **The fragment is not defined.** ``KnownFragmentNamesRule`` reports that; this rule's job
          is to not crash first.
        """
        name = spread.name.value
        if name in active_fragments:
            return 0
        fragment = self._fragments.get(name)
        if fragment is None:
            return 0
        return self._selection_set_cost(
            fragment.selection_set,
            parent_type=self._condition_type(fragment.type_condition, parent_type),
            multiplier=multiplier,
            page_size=page_size,
            active_fragments=active_fragments | {name},
        )

    def _field_cost(
        self,
        node: FieldNode,
        parent_type: Optional[GraphQLNamedType],
        multiplier: int,
        page_size: Optional[int],
        active_fragments: frozenset[str],
    ) -> int:
        """Price one field and, if it has one, its whole sub-selection."""
        name = node.name.value
        if name.startswith("__"):
            # Introspection is free — see the module docstring. The subtree is not walked at all,
            # so `__schema { types { fields { … } } }` costs nothing however deep it goes.
            return 0

        self._visited += 1
        if self._visited > self._config.max_analysed_nodes:
            raise _AnalysisBudgetExceeded(self._charged)

        type_name = parent_type.name if parent_type is not None else None
        spec = self._config.cost_of(type_name, name)
        field_definition = _field_definition(parent_type, name)

        # Charged once per resolution: the field is reached `multiplier` times.
        total = spec.weight * multiplier
        self._charged += total
        if node.selection_set is None:
            return total

        declared = self._declared_size(node)
        inherited: Optional[int] = None
        if spec.size is not None:
            # A list the server sizes rather than the client (see `FieldCost.size`). It is still a
            # list, so it consumes any page size an ancestor declared.
            size = max(1, spec.size)
        elif field_definition is not None and _is_list(field_definition.type):
            # The list consumes the page size — its own argument first, then one an ancestor
            # declared, and finally the default assumption.
            size = self._config.list_size(declared if declared is not None else page_size)
        else:
            # Not a list: it is resolved once per parent, and any declared size is looking for a
            # list further down (`logsConnection(first: 25) { edges { … } }`).
            size = 1
            inherited = declared if declared is not None else page_size

        return total + self._selection_set_cost(
            node.selection_set,
            parent_type=_unwrap(field_definition.type) if field_definition is not None else None,
            multiplier=multiplier * size,
            page_size=inherited,
            active_fragments=active_fragments,
        )

    def _condition_type(
        self,
        condition: Optional[NamedTypeNode],
        fallback: Optional[GraphQLNamedType],
    ) -> Optional[GraphQLNamedType]:
        """Resolve a fragment's ``on X`` type condition, keeping ``fallback`` when it cannot be."""
        if condition is None:
            return fallback
        resolved = self._schema.get_type(condition.name.value)
        return resolved if resolved is not None else fallback

    # -- arguments and variables ---------------------------------------------------------------

    def _declared_size(self, node: FieldNode) -> Optional[int]:
        """The size this field's arguments ask for, or ``None`` when they do not say.

        Direct arguments win over nested ones: ``logsConnection(filters: {limit: 5}, first: 10)``
        returns ten, because ``first`` is the connection spelling and
        :meth:`src.graphql.query.Query.logs_connection` lets it override ``filters.limit`` — the
        cost model and the resolver have to agree about which argument means the page size, or the
        gate would be pricing an operation the server is not going to run.
        """
        for argument in node.arguments:
            if argument.name.value in SIZE_ARGUMENTS:
                size = _as_int(self._resolve_value(argument.value))
                if size is not None:
                    return size

        # One level inside an input object: `logs(filters: {limit: 25})`, and the same filter set
        # arriving whole as a variable.
        for argument in node.arguments:
            value = self._resolve_value(argument.value)
            if isinstance(value, Mapping):
                for key in SIZE_ARGUMENTS:
                    size = _as_int(value.get(key))
                    if size is not None:
                        return size
        return None

    def _resolve_value(self, node: Optional[ValueNode]) -> Any:  # noqa: ANN401 - any JSON value
        """Turn an argument's AST value into a Python value, as far as validation time allows.

        Returns ``None`` for anything unknowable — an unsupplied variable with no default, an enum,
        a value the walker has no use for — and every caller treats ``None`` as "no bound stated",
        which routes back to the default assumption rather than to zero.
        """
        if node is None:
            return None
        if isinstance(node, IntValueNode):
            return int(node.value)
        if isinstance(node, ObjectValueNode):
            return {field.name.value: self._resolve_value(field.value) for field in node.fields}
        if isinstance(node, VariableNode):
            name = node.name.value
            if name in self._variables:
                return self._variables[name]
            # Not supplied: the operation may still have declared a default, which is where a
            # generated client usually puts its page size (`query Q($first: Int = 20)`).
            return self._resolve_value(self._variable_defaults.get(name))
        return None


def _as_int(value: Any) -> Optional[int]:  # noqa: ANN401 - any JSON value
    """``value`` as an ``int``, or ``None``. ``bool`` is rejected: ``True`` is not a page size."""
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) else None


# =================================================================================================
# The public surface: pricing, the rule, and the extension that installs it
# =================================================================================================


def analyse_document(
    schema: GraphQLSchema,
    document: DocumentNode,
    config: CostConfig,
    variables: Optional[Mapping[str, Any]] = None,
) -> list[OperationCost]:
    """Price **every** operation in ``document``.

    Every one, because validation is not told which operation the client intends to run —
    ``operationName`` is applied at execution — so a rule that priced only the first would be
    trivially bypassed by putting the expensive operation second. Strawberry's own
    ``QueryDepthLimiter`` walks the document the same way, for the same reason.
    """
    walker = _Walker(schema, document, config, dict(variables or {}))
    return [
        walker.operation_cost(definition)
        for definition in document.definitions
        if isinstance(definition, OperationDefinitionNode)
    ]


def document_cost(
    schema: GraphQLSchema,
    document: DocumentNode,
    config: CostConfig,
    variables: Optional[Mapping[str, Any]] = None,
) -> int:
    """The cost of the most expensive operation in ``document`` — 0 for a document with none."""
    analysed = analyse_document(schema, document, config, variables)
    return max((operation.cost for operation in analysed), default=0)


def cost_limit_error(analysed: OperationCost, max_complexity: int) -> CostLimitExceededError:
    """The rejection, carrying the numbers a client needs in order to shrink deliberately.

    ``extensions`` gets ``computedCost`` and ``maxCost`` alongside the ``COST_LIMIT_EXCEEDED`` code
    (see :class:`src.graphql.errors.ErrorCode`). Without them the client is told only that it asked
    for too much and has to bisect its own query to find out by how much — which is the difference
    between a gate and a wall.

    It is a :class:`~src.graphql.errors.DomainError`, not a bare ``GraphQLError``, so C4's
    machinery treats it as what it is: :func:`~src.graphql.errors.is_expected_error` recognises it,
    ``MaskInternalErrors`` leaves the message intact, and ``process_errors`` logs **one INFO line**
    instead of a stack trace. A rejected query is a client mistake, and the load harness sends
    thousands of them.
    """
    name = f"operation '{analysed.name}'" if analysed.name else "anonymous operation"
    if analysed.truncated:
        return CostLimitExceededError(
            f"{name} is too large to analyse: it describes more than "
            f"{analysed.cost:,} units of work through nested fragments and was rejected without "
            "being priced exactly. Flatten the fragments or request fewer fields.",
            nodes=[analysed.node],
            extensions={
                "computedCost": analysed.cost,
                "maxCost": max_complexity,
                "truncated": True,
            },
        )
    return CostLimitExceededError(
        f"{name} has a complexity of {analysed.cost}, which exceeds the maximum of "
        f"{max_complexity}. The cost of a nested list is the PRODUCT of the sizes above it, so "
        "lowering `limit`/`first` or dropping one nested list selection reduces it fastest.",
        nodes=[analysed.node],
        extensions={"computedCost": analysed.cost, "maxCost": max_complexity},
    )


def create_cost_validator(
    config: CostConfig, variables: Optional[Mapping[str, Any]] = None
) -> type[ValidationRule]:
    """Build the :class:`~graphql.validation.ValidationRule` class that enforces ``config``.

    A factory returning a class, exactly like Strawberry's ``query_depth_limiter.create_validator``:
    ``graphql.validate`` instantiates each rule with nothing but the validation context, so the
    budget and the variables have to be closed over rather than passed in.

    The work happens in ``__init__`` — again as Strawberry's depth limiter does — rather than in
    visitor callbacks. The walk needs to see whole operations (fragments are resolved from the
    document, and the multiplier at a field depends on every list above it), which a
    field-at-a-time visitor would have to reconstruct with a stack of its own.
    """
    supplied = dict(variables or {})

    class CostLimitValidator(ValidationRule):
        """Rejects an operation whose computed complexity exceeds the configured budget."""

        def __init__(self, validation_context: ValidationContext) -> None:
            super().__init__(validation_context)
            for analysed in analyse_document(
                validation_context.schema, validation_context.document, config, supplied
            ):
                if analysed.truncated or analysed.cost > config.max_complexity:
                    self.report_error(cost_limit_error(analysed, config.max_complexity))

    return CostLimitValidator


class QueryCostLimiter(AddValidationRules):
    """Installs the cost rule for each operation, priced against **that operation's** budget.

    An ``AddValidationRules`` subclass, so the install path is the documented one: the rule ends up
    in ``execution_context.validation_rules`` and runs inside the single ``graphql.validate`` pass,
    before execution.

    .. rubric:: Why the rule is rebuilt per operation instead of once at schema construction

    Two things it needs are not knowable when the schema is assembled:

    * **The variables.** ``logs(filters: $f)`` states its bound nowhere in the document. The
      operation's variable values live on ``execution_context`` and are the only place the walker
      can read them.
    * **The budget.** Settings reach a resolver through ``info.context.settings`` in this project
      rather than through :func:`~src.config.get_settings` (see :class:`src.graphql.context.Context`
      — it exists so a test can run an operation under deliberately different limits without
      touching a process-wide LRU cache). A gate that read the global instead would be the one
      component of the request that could not be configured the same way as everything else.

    So the budget is the **operation context's** settings when there is one, and the configuration
    captured at schema-build time otherwise — which is what an introspection query executed with no
    context at all gets. Both paths are explicit; neither reads a global mid-walk.

    Registered as a zero-argument factory (``lambda: QueryCostLimiter(config)``), never as an
    instance: Strawberry constructs every extension per request and warns since 0.323 about sharing
    one instance across concurrent operations. See the ordering note in :mod:`src.graphql.schema`.
    """

    def __init__(self, config: Optional[CostConfig] = None) -> None:
        """Capture the fallback budget.

        Args:
            config: The configuration to use when an operation arrives without a context carrying
                settings. Defaults to one built from :func:`~src.config.get_settings`, so the class
                is usable as a bare zero-argument factory.
        """
        if config is None:
            config = CostConfig.from_settings(get_settings())
        self.fallback_config = config
        super().__init__([])

    def config_for_operation(self) -> CostConfig:
        """The budget this operation is priced against. Public so a test can assert on it."""
        context = getattr(self.execution_context, "context", None)
        settings = getattr(context, "settings", None)
        if isinstance(settings, Settings):
            return CostConfig.from_settings(settings, weights=self.fallback_config.weights)
        return self.fallback_config

    def on_operation(self) -> Iterator[None]:
        """Append this operation's cost rule to the rules ``graphql.validate`` will run.

        The append is spelled out here rather than delegated to ``super().on_operation()`` because
        the rule cannot exist until this hook runs: it closes over the operation's variables. The
        line itself is precisely what ``AddValidationRules.on_operation`` does.
        """
        config = self.config_for_operation()
        self.validation_rules = [create_cost_validator(config, self.execution_context.variables)]
        existing = tuple(self.execution_context.validation_rules)
        self.execution_context.validation_rules = existing + tuple(self.validation_rules)
        yield
