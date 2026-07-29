"""The cost walker as a pure function: parsed document in, a number out.

Every expectation here is an **exact** integer, never a range and never "greater than zero". A
range hides the regression this file exists to catch — a walker that stopped multiplying nested
lists, or started charging a leaf once instead of once per row, would still return "a big number"
for a big query and every loose assertion would stay green. The arithmetic is written out in each
docstring so a failing number can be diagnosed without re-deriving the model.

The schema under test is the **real** one, round-tripped through its own SDL
(:func:`graphql.build_schema` over ``schema.as_str()``). That keeps this file to public API while
still pricing against the actual published types: what the walker reads is field names, list-ness
and named types, all of which survive the round trip exactly. The integration suite then drives the
same walker through the live Strawberry schema, so a divergence between the two would surface
there.

``CONFIG`` deliberately pins ``default_list_size``/``max_list_size`` to the shipped
``DEFAULT_QUERY_LIMIT``/``MAX_QUERY_LIMIT`` rather than reading them from the environment: these
tests are arithmetic, and arithmetic that changes when a container sets a variable is not a test.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from graphql import GraphQLSchema, build_schema as build_graphql_schema, parse, validate

from src.config import Settings
from src.db.repository import clamp_limit
from src.graphql.cost import (
    DEFAULT_WEIGHTS,
    CostConfig,
    FieldCost,
    analyse_document,
    create_cost_validator,
    document_cost,
)
from src.graphql.errors import ErrorCode
from src.graphql.schema import build_schema, schema as strawberry_schema

#: The budget these tests price against: 25,000 is the shipped ``MAX_QUERY_COMPLEXITY`` and the two
#: list sizes are the shipped ``DEFAULT_QUERY_LIMIT`` and ``MAX_QUERY_LIMIT``. Stated as literals so
#: the expected costs below are readable arithmetic rather than a function of the environment — and
#: tied back to what ``src/config.py`` actually declares by
#: :func:`test_the_configuration_these_tests_price_against_is_the_shipped_one`, so the literals
#: cannot drift away from the shipped values without a failure.
CONFIG = CostConfig(max_complexity=25_000, default_list_size=100, max_list_size=500)


@pytest.fixture(scope="module")
def gql_schema() -> GraphQLSchema:
    """The published schema as a ``GraphQLSchema``, built from the SDL the server renders."""
    return build_graphql_schema(strawberry_schema.as_str())


def cost(
    gql_schema: GraphQLSchema,
    document: str,
    config: CostConfig = CONFIG,
    variables: Optional[dict[str, Any]] = None,
) -> int:
    """Price ``document`` — the most expensive operation in it, as the rule does."""
    return document_cost(gql_schema, parse(document), config, variables)


# --- The table itself ----------------------------------------------------------------------------


def test_the_configuration_these_tests_price_against_is_the_shipped_one() -> None:
    """``CONFIG``'s three literals are the values ``src/config.py`` declares.

    The two calibration tests at the end of this file assert what the **shipped** budget admits and
    refuses, and that claim is only worth anything while this module's literals and ``Settings``
    agree. The defaults are read off ``model_fields`` rather than off a constructed ``Settings``
    deliberately: compose's ``test`` service raises ``MAX_QUERY_COMPLEXITY`` for the suite, and a
    comparison against the environment would be comparing this file to that override instead of to
    what the project ships.
    """
    declared = Settings.model_fields

    assert CONFIG.max_complexity == declared["max_query_complexity"].default == 25_000
    assert CONFIG.default_list_size == declared["default_query_limit"].default == 100
    assert CONFIG.max_list_size == declared["max_query_limit"].default == 500


def test_an_unweighted_field_costs_one_and_a_weighted_one_costs_its_entry() -> None:
    """The lookup order is ``Type.field``, then ``field``, then the default."""
    assert CONFIG.cost_of("Query", "logs") is DEFAULT_WEIGHTS["Query.logs"]
    assert CONFIG.cost_of("LogEntry", "message") == FieldCost(weight=1)
    assert CONFIG.cost_of(None, "logs") == FieldCost(weight=1), (
        "an unresolvable parent type must not silently pick up Query.logs' weight"
    )


def test_a_bare_field_name_key_applies_wherever_the_field_appears() -> None:
    """C11 tunes weights by adding rows, so the bare-name form has to work too."""
    config = CostConfig(
        max_complexity=1000,
        default_list_size=100,
        max_list_size=500,
        weights={"message": FieldCost(weight=7)},
    )
    assert config.cost_of("LogEntry", "message") == FieldCost(weight=7)
    assert config.cost_of("SomethingElse", "message") == FieldCost(weight=7)


@pytest.mark.parametrize("requested", [None, 0, 1, 10, 100, 500, 501, 100_000, -5])
def test_the_assumed_list_size_matches_what_the_executor_will_actually_run(
    requested: Optional[int],
) -> None:
    """:meth:`CostConfig.list_size` mirrors :func:`src.db.repository.clamp_limit`, value for value.

    The mirror is the whole reason the gate cannot be talked around. ``limit: 0`` is clamped **up**
    to one row by the statement builder, so a cost model that scored it as zero would hand every
    client a one-word bypass; ``limit: 100000`` is clamped **down** to ``MAX_QUERY_LIMIT``, so
    pricing it at 100000 would reject a query that is going to be cheap. Asserting equality against
    the executor's own function is stronger than asserting the numbers separately, which is exactly
    how the two would drift.
    """
    settings = Settings(_env_file=None, default_query_limit=100, max_query_limit=500)
    assert CONFIG.list_size(requested) == clamp_limit(requested, settings)


# --- Known documents, known costs ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        # logs(10) + id(1 x 100 assumed rows)
        ("{ logs { id } }", 110),
        # The spec's own acceptance query: logs(10) + 4 leaves x 100 rows.
        ("{ logs { id service level message } }", 410),
        # Every published field on LogEntry: 10 + 7 x 100.
        ("{ logs { id timestamp service level message metadata traceId } }", 710),
        # An explicit limit replaces the assumption: 10 + 10.
        ("{ logs(filters: {limit: 10}) { id } }", 20),
        # `limit: 0` is clamped UP to one row by the executor, so it is priced at one.
        ("{ logs(filters: {limit: 0}) { id } }", 11),
        # ... and an absurd limit is clamped DOWN to MAX_QUERY_LIMIT: 10 + 500.
        ("{ logs(filters: {limit: 100000}) { id } }", 510),
        # A single row by id is not a list: log(5) + id(1).
        ('{ log(id: "1") { id } }', 6),
        # Two GROUP BY scans, three leaves resolved once: 30 + 3.
        ("{ logStats { totalLogs errorCount services } }", 33),
        # `services` is a list of SCALARS — a leaf. It multiplies nothing: 30 + 1.
        ("{ logStats { services } }", 31),
        # serviceBreakdown is a list the SERVER sizes (12): 30 + 1 + 2 x 12.
        ("{ logStats { serviceBreakdown { service count } } }", 55),
        # levelBreakdown is bounded by the LogLevel enum (5): 30 + 1 + 2 x 5.
        ("{ logStats { levelBreakdown { level count } } }", 41),
        # One INSERT plus the publish: 10 + 1.
        (
            'mutation { createLog(logData: {service: "api", level: INFO, message: "m"}) { id } }',
            11,
        ),
        # A subscription is priced PER EVENT: 10 + 1.
        ("subscription { logStream { id } }", 11),
    ],
)
def test_known_documents_score_known_costs(
    gql_schema: GraphQLSchema, document: str, expected: int
) -> None:
    """Each of these is the arithmetic in its comment, and nothing else."""
    assert cost(gql_schema, document) == expected


def test_an_omitted_limit_is_priced_at_the_default_and_not_at_zero(
    gql_schema: GraphQLSchema,
) -> None:
    """THE assumption the whole gate rests on — see the module docstring in ``src/graphql/cost.py``.

    Stated as a relationship rather than as a constant: asking for the default explicitly and not
    asking at all must produce the *same* number, and it must be the number that corresponds to
    ``DEFAULT_QUERY_LIMIT`` rows rather than to none.
    """
    omitted = cost(gql_schema, "{ logs { id } }")
    explicit = cost(gql_schema, "{ logs(filters: {limit: 100}) { id } }")
    one_row = cost(gql_schema, "{ logs(filters: {limit: 1}) { id } }")

    assert omitted == explicit == 110
    assert omitted > one_row, "an unbounded list must not be cheaper than a one-row one"


def test_an_unbounded_nested_list_is_priced_at_the_default_too(gql_schema: GraphQLSchema) -> None:
    """``relatedLogs`` takes no size argument at all, which is why cost analysis exists here.

    10 (relatedLogs) + 1 x 100 (its id, once per assumed related entry) = 110 under one parent.
    """
    assert cost(gql_schema, '{ log(id: "1") { relatedLogs { id } } }') == 5 + 110


# --- Nesting MULTIPLIES ---------------------------------------------------------------------------


def test_nesting_multiplies_rather_than_adds(gql_schema: GraphQLSchema) -> None:
    """``logs(limit: N) { relatedLogs { id } }`` is N separate correlated lookups, not one.

    The subtree ``relatedLogs { id }`` costs 110 when it is resolved once (10 for the field, 100
    for its leaf over the assumed 100 related entries). Under ``logs(limit: 10)`` it is resolved
    ten times, so the document must cost ``10 (logs) + 10 x 110``. The additive model that this
    test exists to rule out would score it at ``10 + 10 + 110``.
    """
    subtree = cost(gql_schema, '{ log(id: "1") { relatedLogs { id } } }') - 5
    assert subtree == 110

    nested = cost(gql_schema, "{ logs(filters: {limit: 10}) { relatedLogs { id } } }")

    assert nested == 10 + 10 * subtree == 1110
    assert nested != 10 + 10 + subtree, "the multiplier collapsed into an addition"


def test_doubling_the_limit_doubles_everything_under_it(gql_schema: GraphQLSchema) -> None:
    """The only term that does not double is the root field itself, which is resolved once."""
    ten = cost(gql_schema, "{ logs(filters: {limit: 10}) { relatedLogs { id } } }")
    twenty = cost(gql_schema, "{ logs(filters: {limit: 20}) { relatedLogs { id } } }")

    root_weight = DEFAULT_WEIGHTS["Query.logs"].weight
    assert (twenty - root_weight) == 2 * (ten - root_weight)
    assert (ten, twenty) == (1110, 2210)


def test_a_second_level_of_nesting_multiplies_again(gql_schema: GraphQLSchema) -> None:
    """The shape spec §3D wants rejected: 10 + 10x100 + 10x100x100 + 1x100x100x100."""
    assert cost(gql_schema, "{ logs { relatedLogs { relatedLogs { id } } } }") == 1_101_010


def test_a_declared_page_size_reaches_the_list_one_level_below_it(
    gql_schema: GraphQLSchema,
) -> None:
    """``logsConnection(first:)`` sizes ``edges``, which is where the list actually is.

    15 (logsConnection) + 5 (totalCount, resolved once beside the page rather than per row)
    + 1 (edges) + 5 (cursor) + 5 (node) + 5 (its id) + 1 (pageInfo) + 1 (hasNextPage) = 38.

    The two failures this pins: charging ``edges`` the default 100 instead of the requested 5
    (which would make ``first`` decorative), and multiplying ``totalCount`` by the page size (it is
    one COUNT however big the page is).
    """
    document = """
    {
      logsConnection(first: 5) {
        totalCount
        edges { cursor node { id } }
        pageInfo { hasNextPage }
      }
    }
    """
    assert cost(gql_schema, document) == 38
    assert cost(gql_schema, "{ logsConnection(first: 10) { edges { node { id } } } }") == (
        15 + 1 + 10 + 10
    )


def test_a_connection_with_no_page_size_falls_back_to_the_default_assumption(
    gql_schema: GraphQLSchema,
) -> None:
    """15 + 1 (edges) + 100 (node, once per assumed row) + 100 (its id)."""
    assert cost(gql_schema, "{ logsConnection { edges { node { id } } } }") == 216


# --- Aliases -------------------------------------------------------------------------------------


def test_aliases_multiply_the_cost(gql_schema: GraphQLSchema) -> None:
    """Three aliased ``logs`` are three queries, and the walker charges for three.

    An alias is the cheapest way to multiply server work without touching a single limit, which is
    why ``MAX_QUERY_ALIASES`` exists as well — but a cost model that deduplicated by field name
    would price this at one third of the truth and let 30 aliases through as if they were one.
    """
    one = cost(gql_schema, "{ a: logs(filters: {limit: 1}) { id } }")
    three = cost(
        gql_schema,
        """
        {
          a: logs(filters: {limit: 1}) { id }
          b: logs(filters: {limit: 1}) { id }
          c: logs(filters: {limit: 1}) { id }
        }
        """,
    )

    assert one == 11
    assert three == 3 * one


# --- Fragments -----------------------------------------------------------------------------------


def test_a_fragment_costs_exactly_what_the_inlined_spelling_costs(
    gql_schema: GraphQLSchema,
) -> None:
    """Equality, not "both are non-zero": a fragment must not be a discount.

    Skipping fragments is the classic hole in a hand-rolled cost gate — the expensive half of the
    document moves into ``fragment F on LogEntry`` and the score drops to nothing.
    """
    inlined = cost(gql_schema, "{ logs(filters: {limit: 10}) { id relatedLogs { id } } }")
    spread = cost(
        gql_schema,
        """
        query Spread { logs(filters: {limit: 10}) { ...F } }
        fragment F on LogEntry { id relatedLogs { id } }
        """,
    )

    assert spread == inlined == 1120


def test_an_inline_fragment_costs_exactly_what_the_inlined_spelling_costs(
    gql_schema: GraphQLSchema,
) -> None:
    """The same property for ``... on LogEntry { … }``, which needs no fragment definition."""
    plain = cost(gql_schema, "{ logs(filters: {limit: 10}) { id } }")
    inline = cost(gql_schema, "{ logs(filters: {limit: 10}) { ... on LogEntry { id } } }")

    assert inline == plain == 20


def test_a_nested_fragment_keeps_the_multiplier_it_was_spread_under(
    gql_schema: GraphQLSchema,
) -> None:
    """A fragment spread inside a list's selection set is resolved once per entry."""
    document = """
    query Nested { logs(filters: {limit: 4}) { ...Entry } }
    fragment Entry on LogEntry { id ...Related }
    fragment Related on LogEntry { relatedLogs { id } }
    """
    # 10 + 4 x (1 for id + 110 for the relatedLogs subtree)
    assert cost(gql_schema, document) == 10 + 4 * 111


def test_a_fragment_cycle_terminates_instead_of_recursing_forever(
    gql_schema: GraphQLSchema,
) -> None:
    """A cycle is walked once and then dropped, so the analyser returns rather than hangs.

    graphql-core reports the cycle itself, through ``NoFragmentCyclesRule`` — but that rule runs in
    the **same** validation pass as this one and nothing orders it first, so a walker that assumed
    it had already fired would hang inside a live request. The bound is structural (each fragment
    is entered at most once per path), which is why this can assert an exact number rather than
    just "it came back".

    10 (logs) + 1 (A's id) + 1 (B's id); B's spread of A is the cycle and contributes nothing.
    """
    document = """
    query Cyclic { logs(filters: {limit: 1}) { ...A } }
    fragment A on LogEntry { id ...B }
    fragment B on LogEntry { id ...A }
    """
    assert cost(gql_schema, document) == 12


def test_a_fragment_that_does_not_exist_is_skipped_rather_than_fatal(
    gql_schema: GraphQLSchema,
) -> None:
    """``KnownFragmentNamesRule`` owns that error; this rule's job is to not crash before it."""
    assert cost(gql_schema, "{ logs(filters: {limit: 1}) { id ...Missing } }") == 11


# --- Variables -----------------------------------------------------------------------------------


def test_a_size_supplied_through_a_variable_is_used(gql_schema: GraphQLSchema) -> None:
    """``logs(filters: {limit: $limit})`` with ``$limit = 10`` costs what the literal 10 costs."""
    document = "query Q($limit: Int!) { logs(filters: {limit: $limit}) { id } }"

    assert cost(gql_schema, document, variables={"limit": 10}) == 20
    assert cost(gql_schema, document, variables={"limit": 250}) == 260


def test_a_whole_filter_object_supplied_as_a_variable_is_looked_inside(
    gql_schema: GraphQLSchema,
) -> None:
    """``logs(filters: $filters)`` — the spelling every generated client actually sends."""
    document = "query Q($filters: LogFilterInput) { logs(filters: $filters) { id } }"

    assert cost(gql_schema, document, variables={"filters": {"limit": 25}}) == 35
    assert cost(gql_schema, document, variables={"filters": {"service": "api"}}) == 110


def test_an_unknowable_variable_falls_back_to_the_default_assumption_not_to_zero(
    gql_schema: GraphQLSchema,
) -> None:
    """The one that matters: a variable the gate cannot see must not make a query free.

    Validation runs before variable coercion in some clients' request shapes and a variable may
    simply be absent, so "I cannot tell" has to resolve to the same conservative answer an omitted
    argument gets. Scoring it at zero would turn ``$limit`` into a universal bypass.
    """
    document = "query Q($limit: Int) { logs(filters: {limit: $limit}) { id } }"

    assert cost(gql_schema, document, variables=None) == 110
    assert cost(gql_schema, document, variables={"somethingElse": 3}) == 110


def test_a_variables_declared_default_is_honoured(gql_schema: GraphQLSchema) -> None:
    """``query Q($limit: Int = 5)`` states the bound in the document; the walker reads it."""
    document = "query Q($limit: Int = 5) { logs(filters: {limit: $limit}) { id } }"
    assert cost(gql_schema, document, variables=None) == 15


def test_a_non_integer_variable_value_is_treated_as_no_bound(gql_schema: GraphQLSchema) -> None:
    """``True`` is an ``int`` in Python and is not a page size; neither is a string."""
    document = "query Q($limit: Int) { logs(filters: {limit: $limit}) { id } }"

    assert cost(gql_schema, document, variables={"limit": True}) == 110
    assert cost(gql_schema, document, variables={"limit": "10"}) == 110


# --- Introspection -------------------------------------------------------------------------------


def test_introspection_is_free_and_that_is_a_decision(gql_schema: GraphQLSchema) -> None:
    """``__``-prefixed fields cost nothing and their subtrees are not walked at all.

    An introspection query is deep and wide, entirely bounded by the schema's own size, and cannot
    touch the database — a client cannot make it bigger. Pricing it would break GraphiQL for no
    security gain, and Strawberry's ``QueryDepthLimiter`` exempts exactly the same fields, so the
    two limiters agree about what introspection is worth. The integration suite asserts the real
    ``get_introspection_query()`` survives the shipped defaults over HTTP.
    """
    assert cost(gql_schema, "{ __schema { types { name fields { name } } } }") == 0
    assert cost(gql_schema, "{ __typename }") == 0
    assert cost(gql_schema, '{ __type(name: "LogEntry") { fields { name } } }') == 0


def test_a_typename_beside_real_fields_changes_nothing(gql_schema: GraphQLSchema) -> None:
    """The exemption is per-field, so a mixed selection still prices everything else."""
    assert cost(gql_schema, "{ logs { id __typename } }") == cost(gql_schema, "{ logs { id } }")


# --- Operations ----------------------------------------------------------------------------------


def test_every_operation_in_the_document_is_priced(gql_schema: GraphQLSchema) -> None:
    """Validation is not told which operation will run, so pricing only the first is a bypass."""
    document = """
    query Cheap { logs(filters: {limit: 1}) { id } }
    query Expensive { logs { relatedLogs { id } } }
    """
    analysed = analyse_document(gql_schema, parse(document), CONFIG)

    assert [operation.name for operation in analysed] == ["Cheap", "Expensive"]
    assert [operation.cost for operation in analysed] == [11, 10 + 100 * 110]
    assert document_cost(gql_schema, parse(document), CONFIG) == 11010


def test_an_anonymous_operation_is_priced_and_reported_without_a_name(
    gql_schema: GraphQLSchema,
) -> None:
    """The rejection message has to read sensibly for a document with no operation name."""
    (analysed,) = analyse_document(gql_schema, parse("{ logs { id } }"), CONFIG)

    assert analysed.name is None
    assert analysed.cost == 110


# --- Robustness ----------------------------------------------------------------------------------


def test_a_field_that_is_not_in_the_schema_is_priced_at_the_default_rather_than_crashing(
    gql_schema: GraphQLSchema,
) -> None:
    """This rule runs in the same pass as ``FieldsOnCorrectTypeRule``, so it meets bad documents.

    The real error is that rule's to report. This one must reach it — 10 + 1 x 100 rows.
    """
    assert cost(gql_schema, "{ logs { notAField } }") == 110


def test_an_exponentially_large_document_is_rejected_rather_than_analysed_forever(
    gql_schema: GraphQLSchema,
) -> None:
    """The analyser's own bound: fragments let a small document describe a huge tree.

    ``MAX_QUERY_TOKENS`` bounds the document, not the tree it denotes, and an analyser that can be
    made to run for a minute is itself the denial of service it was installed to prevent. Hitting
    the node budget rejects the operation on that basis alone and says so, rather than reporting a
    number the walk never finished computing.
    """
    tiny_budget = CostConfig(
        max_complexity=1000, default_list_size=100, max_list_size=500, max_analysed_nodes=3
    )
    (analysed,) = analyse_document(
        gql_schema, parse("{ logs { id service level message } }"), tiny_budget
    )

    assert analysed.truncated is True
    assert analysed.cost > 0, "a truncated walk still reports the lower bound it did compute"


# --- The rule, as graphql-core will run it -------------------------------------------------------


def test_the_rule_reports_a_coded_error_through_the_validation_context(
    gql_schema: GraphQLSchema,
) -> None:
    """The plumbing: ``graphql.validate`` + the rule class = one error with the numbers attached.

    Driven through :func:`graphql.validate` rather than by calling the class directly, because the
    contract being tested is the one graphql-core imposes — instantiate with a ``ValidationContext``
    and report through it.

    The document is a ``MAX_QUERY_LIMIT``-wide page with one level of correlation attached —
    ``10 + 500 x (10 + 100)`` — rather than the same shape at the default page size, which the
    shipped budget deliberately **admits** (see the calibration pair at the end of this file).
    """
    document = parse("{ logs(filters: {limit: 500}) { relatedLogs { id } } }")
    rule = create_cost_validator(CONFIG)

    errors = validate(gql_schema, document, [rule])

    assert len(errors) == 1
    error = errors[0]
    assert error.extensions["code"] == ErrorCode.COST_LIMIT_EXCEEDED.value
    assert error.extensions["computedCost"] == 10 + 500 * 110 == 55_010
    assert error.extensions["maxCost"] == 25_000
    assert "55010" in error.message and "25000" in error.message, (
        "the message must carry both numbers so a client can shrink deliberately"
    )
    assert error.locations, "the rejection must point at the operation it rejected"


def test_the_rule_accepts_a_document_costing_exactly_the_budget(
    gql_schema: GraphQLSchema,
) -> None:
    """The boundary is ``>``, not ``>=`` — pinned here so it cannot drift by one."""
    document = parse("{ logs(filters: {limit: 10}) { id } }")  # exactly 20
    exact = CostConfig(max_complexity=20, default_list_size=100, max_list_size=500)
    tight = CostConfig(max_complexity=19, default_list_size=100, max_list_size=500)

    assert validate(gql_schema, document, [create_cost_validator(exact)]) == []
    assert len(validate(gql_schema, document, [create_cost_validator(tight)])) == 1


def test_the_rule_reads_the_variables_it_was_built_with(gql_schema: GraphQLSchema) -> None:
    """Same document, same budget, opposite outcomes — the variables are the only difference."""
    document = parse("query Q($limit: Int) { logs(filters: {limit: $limit}) { id } }")
    config = CostConfig(max_complexity=60, default_list_size=100, max_list_size=500)

    assert validate(gql_schema, document, [create_cost_validator(config, {"limit": 50})]) == []
    errors = validate(gql_schema, document, [create_cost_validator(config, {"limit": 200})])
    assert len(errors) == 1
    assert errors[0].extensions["computedCost"] == 210


# --- The calibration: what the SHIPPED budget admits, and what it still refuses -------------------
#
# A matched pair, and they only mean anything together. The first says the gate is not too tight to
# serve the capability this API is built around; the second says it is not so loose that it has
# stopped being a gate. Moving MAX_QUERY_COMPLEXITY in either direction fails one of them, which is
# the point: a budget is a calibration, and a calibration nobody pinned is a number nobody checked.


def test_the_flagship_correlated_query_is_admitted_at_the_default_page_size(
    gql_schema: GraphQLSchema,
) -> None:
    """``logs`` at ``DEFAULT_QUERY_LIMIT`` with ONE level of ``relatedLogs`` must be accepted.

    THE regression this budget exists to prevent. That document is spec §2 item 17 (correlated
    entries) asked for at spec §2 item 29's default page size: it is the single query C5's
    DataLoader exists to make cheap, the one C13's dashboard sends and the one C11's
    multi-dimensional traversals are built on. ``MAX_QUERY_COMPLEXITY`` shipped at 1000 while the
    cheapest spelling of it prices at 11,010, so the default rejected the API's own headline
    capability — a broken default rather than a strict one, and one that stayed invisible because
    the suite ran with the gate turned off.

    The arithmetic, with the parent list at its assumed 100 rows:

    * ``{ logs { relatedLogs { id } } }``               10 + 10x100 + 1x100x100          = 11,010
    * ``{ logs { id relatedLogs { id } } }``            the same, plus 1x100             = 11,110
    * two fields on each level                          10 + 2x100 + 10x100 + 2x100x100  = 21,210
    """
    minimal = "{ logs { relatedLogs { id } } }"
    flagship = "{ logs { id relatedLogs { id } } }"
    detailed = "{ logs { id message relatedLogs { id message } } }"

    assert cost(gql_schema, minimal) == 11_010
    assert cost(gql_schema, flagship) == 11_110
    assert cost(gql_schema, detailed) == 21_210

    for document in (minimal, flagship, detailed):
        assert cost(gql_schema, document) <= CONFIG.max_complexity, document
        assert validate(gql_schema, parse(document), [create_cost_validator(CONFIG)]) == [], (
            f"the shipped budget rejected `{document}` — one level of correlation at the default "
            "page size is the thing this API is for, so a budget that refuses it is miscalibrated"
        )


def test_two_levels_of_correlation_are_still_refused_by_the_shipped_budget(
    gql_schema: GraphQLSchema,
) -> None:
    """Admitting the flagship must not have admitted the fan-out behind it.

    The two shapes spec §3 Feature Area D wants refused, priced against the same shipped budget the
    test above proves accepts the flagship. The wide page is the interesting one: it is only one
    level deep, so no depth limit would ever see it, and it is exactly what a client reaches for
    when told to "just ask for a bigger page".

    * ``{ logs { relatedLogs { relatedLogs { id } } } }``     44x the budget    = 1,101,010
    * ``{ logs(limit: 500) { relatedLogs { id } } }``         10 + 500x110      =    55,010
    """
    two_levels = "{ logs { relatedLogs { relatedLogs { id } } } }"
    wide_page = "{ logs(filters: {limit: 500}) { relatedLogs { id } } }"

    assert cost(gql_schema, two_levels) == 1_101_010
    assert cost(gql_schema, wide_page) == 55_010

    for document in (two_levels, wide_page):
        errors = validate(gql_schema, parse(document), [create_cost_validator(CONFIG)])
        assert len(errors) == 1, f"the shipped budget accepted `{document}`"
        assert errors[0].extensions["code"] == ErrorCode.COST_LIMIT_EXCEEDED.value


# --- The settings-to-extension wiring in src/graphql/schema.py ------------------------------------
#
# All four budgets are turned into behaviour by `build_schema(settings)`, and all four are asserted
# here by BUILDING a schema and watching an operation bounce off it. Nothing below touches a
# database: every one of these documents is refused during parsing or validation, which is the
# property under test. `execute` is called with no context at all, so the cost gate falls back to
# the configuration `build_schema` captured — which is the other half of the wiring.


async def test_a_schema_carries_the_complexity_budget_it_was_built_with() -> None:
    """The fallback path: no request context, so the budget is the one captured at build time."""
    tight = build_schema(Settings(_env_file=None, max_query_complexity=100))

    result = await tight.execute("{ logs { id } }")  # costs 110

    assert result.errors is not None
    assert result.errors[0].extensions["code"] == ErrorCode.COST_LIMIT_EXCEEDED.value
    assert result.errors[0].extensions["computedCost"] == 110
    assert result.errors[0].extensions["maxCost"] == 100


async def test_the_same_document_passes_the_gate_one_unit_higher() -> None:
    """The other side of it, on a schema that differs only in its budget.

    The operation goes on to fail for an unrelated reason — there is no context and therefore no
    database — which is exactly what "the gate let it through" looks like from here, and why the
    assertion is on the error *code* rather than on the absence of errors.
    """
    exact = build_schema(Settings(_env_file=None, max_query_complexity=110))

    result = await exact.execute("{ logs { id } }")

    assert ErrorCode.COST_LIMIT_EXCEEDED.value not in [
        (error.extensions or {}).get("code") for error in result.errors or ()
    ]


async def test_a_schema_carries_the_depth_budget_it_was_built_with() -> None:
    """``QueryDepthLimiter`` gets its number from the same ``Settings``.

    The complexity budget is raised out of the way so the rejection can only be the depth one —
    in this schema anything deep is also expensive (see the module docstring in
    ``src/graphql/cost.py``), so the two gates have to be separated deliberately.
    """
    shallow = build_schema(
        Settings(_env_file=None, max_query_depth=1, max_query_complexity=10**9)
    )

    result = await shallow.execute("{ logs { relatedLogs { id } } }")

    assert result.errors is not None
    assert "depth of 1" in result.errors[0].message
    assert (result.errors[0].extensions or {}).get("code") != ErrorCode.COST_LIMIT_EXCEEDED.value


async def test_a_schema_carries_the_token_and_alias_budgets_it_was_built_with() -> None:
    """The remaining two, so all four settings are proven to reach an extension rather than three."""
    few_tokens = build_schema(Settings(_env_file=None, max_query_tokens=5))
    few_aliases = build_schema(Settings(_env_file=None, max_query_aliases=1))

    tokens = await few_tokens.execute("{ logs { id service level } }")
    aliases = await few_aliases.execute(
        "{ x: logs(filters: {limit: 1}) { id } y: logs(filters: {limit: 1}) { id } }"
    )

    assert tokens.errors is not None
    assert "5 tokens" in tokens.errors[0].message
    assert aliases.errors is not None
    assert "Allowed: 1" in aliases.errors[0].message
