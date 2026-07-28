"""Schema-shape tests, asserted through **introspection** rather than through Python objects.

Every assertion below is made against the schema as a *client* sees it: an introspection query, the
same one GraphiQL and Apollo Codegen issue. That is deliberate. Reaching into
``LogEntry.__strawberry_definition__`` would test the decorator's bookkeeping; a schema is only
wrong in ways that matter when the **published** contract is wrong, and the two can differ (a
camel-casing setting, a scalar registration, a nullability that Strawberry resolves differently
from how the annotation reads).

These are also the tests that would catch a real regression rather than restate the source:

* a field renamed (``metadata_`` leaking out, ``traceId`` becoming ``trace_id``)
* a nullability flipped (``metadata: JSON!`` would break every seeded row without a metadata object)
* ``logs`` quietly turned into a connection, which breaks the spec's own acceptance command
* the ``filters`` argument renamed, which breaks every published example
* a ``LogFilterInput`` field made required, which breaks "omitted filters are ignored"
* the ``LogLevel`` enum drifting from the corpus vocabulary

No database, no lifespan, no HTTP — the schema is a pure function of the source.

.. rubric:: Why introspection runs through ``schema.execute`` on a private event loop (C5)

It used to be ``schema.execute_sync``, which was simpler and is no longer available: the schema
carries :class:`~src.graphql.context.PerOperationResources`, whose ``on_operation`` hook is an
**async generator** (closing a database session requires an await), and Strawberry refuses to enter
an async extension hook from a synchronous execution — ``RuntimeError: SchemaExtension hook …
failed to complete synchronously``.

Nothing is lost by that. Every resolver in this project is a coroutine, so ``execute_sync`` could
only ever have served introspection in the first place; the schema is async-only in exactly the way
the server always was. The loop is created, used and closed by :func:`_run` rather than through
:func:`asyncio.run`, so the ambient event-loop policy pytest-asyncio manages is never touched.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any, Optional, TypeVar

from src.generators import LOG_LEVELS
from src.graphql.schema import schema

_T = TypeVar("_T")


def _run(coro: Awaitable[_T]) -> _T:
    """Run ``coro`` to completion on a private event loop. See the module docstring."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)  # type: ignore[arg-type]
    finally:
        loop.close()

#: One introspection document reused for every type. ``fields`` is null for input objects,
#: ``inputFields`` for output objects and ``enumValues`` for both — the introspection schema makes
#: all three nullable, so one query serves every shape and the tests differ only in what they read.
SHAPE_QUERY = """
query Shape($name: String!) {
  __type(name: $name) {
    kind
    name
    fields {
      name
      description
      args { name type { ...Ref } }
      type { ...Ref }
    }
    inputFields { name type { ...Ref } }
    enumValues { name }
  }
}

fragment Ref on __Type {
  kind
  name
  ofType { kind name ofType { kind name ofType { kind name } } }
}
"""


def _render_type(ref: Optional[dict[str, Any]]) -> str:
    """Render an introspection type reference back into SDL notation (``[LogEntry!]!``).

    Comparing rendered strings rather than nested dicts is what makes a failure readable: the
    assertion message says ``'JSON!' != 'JSON'`` instead of printing four levels of ``ofType``.
    """
    if ref is None:
        return "<null>"
    kind = ref["kind"]
    if kind == "NON_NULL":
        return f"{_render_type(ref['ofType'])}!"
    if kind == "LIST":
        return f"[{_render_type(ref['ofType'])}]"
    return str(ref["name"])


def _introspect(name: str) -> dict[str, Any]:
    """Return the introspection record for one named type, failing loudly if it is absent."""
    result = _run(schema.execute(SHAPE_QUERY, variable_values={"name": name}))

    assert result.errors is None, f"introspecting {name} failed: {result.errors}"
    assert result.data is not None
    type_record = result.data["__type"]
    assert type_record is not None, f"the schema publishes no type named {name!r}"
    return type_record


def _field_types(name: str) -> dict[str, str]:
    """``{field name: rendered SDL type}`` for an object type."""
    record = _introspect(name)
    assert record["fields"] is not None, f"{name} is a {record['kind']}, not an object type"
    return {field["name"]: _render_type(field["type"]) for field in record["fields"]}


def _input_field_types(name: str) -> dict[str, str]:
    """``{field name: rendered SDL type}`` for an input object type."""
    record = _introspect(name)
    assert record["inputFields"] is not None, f"{name} is a {record['kind']}, not an input type"
    return {field["name"]: _render_type(field["type"]) for field in record["inputFields"]}


def _field(type_name: str, field_name: str) -> dict[str, Any]:
    """One field's introspection record, by name."""
    record = _introspect(type_name)
    matches = [field for field in record["fields"] if field["name"] == field_name]
    assert matches, f"{type_name} has no field {field_name!r}"
    return matches[0]


# --- The enum ------------------------------------------------------------------------------------


def test_log_level_publishes_exactly_the_corpus_vocabulary() -> None:
    """``LogLevel`` and :data:`src.generators.LOG_LEVELS` agree, in order.

    The enum is the *published* half of the contract and ``LOG_LEVELS`` is the half the seeded
    corpus and every oracle is built from. A member here that the corpus cannot produce is a filter
    that always returns nothing; a level in the corpus that is missing here is a stored row that
    cannot be serialised back out. Order is asserted too, because the SDL prints them in this order
    and the SDL is a committed, diffed artefact.
    """
    record = _introspect("LogLevel")

    assert record["kind"] == "ENUM"
    assert tuple(value["name"] for value in record["enumValues"]) == LOG_LEVELS


# --- LogEntry ------------------------------------------------------------------------------------


def test_log_entry_publishes_the_seven_spec_fields_plus_related_logs() -> None:
    """Spec §2 items 15 and 17, asserted as an equality so extra and missing fields both fail.

    The nullability half is the part that is easy to get wrong and expensive to notice:
    ``metadata`` and ``traceId`` are the two nullable columns, and roughly a third of the seeded
    corpus has each of them null. Publishing either as non-null would make those rows unresolvable
    — a client would get an error for data that is perfectly valid.

    ``relatedLogs`` (C5) is the eighth field and the only one backed by a resolver rather than a
    column. It is pinned here as well as in its own tests below so that "the published type is
    exactly this" stays a single assertion.
    """
    assert _field_types("LogEntry") == {
        "id": "ID!",
        "timestamp": "DateTime!",
        "service": "String!",
        "level": "LogLevel!",
        "message": "String!",
        "metadata": "JSON",
        "traceId": "String",
        "relatedLogs": "[LogEntry!]!",
    }


# --- LogEntry.relatedLogs (C5) ---------------------------------------------------------------


def test_related_logs_is_a_non_null_list_of_non_null_entries_taking_no_arguments() -> None:
    """``relatedLogs: [LogEntry!]!`` — and specifically **not** nullable at either level.

    "No correlated entries" is an empty list, never ``null``: a nullable list would give a client
    two spellings of the same fact and force it to handle both. Non-null *elements* say the same
    thing about the members — a correlation group cannot contain a hole.

    No arguments, deliberately. Filtering related entries is a different feature (and one the cost
    gate in C8 would have to price), and adding an argument later is backwards-compatible while
    removing one is not.
    """
    field = _field("LogEntry", "relatedLogs")

    assert _render_type(field["type"]) == "[LogEntry!]!"
    assert field["args"] == []


def test_related_logs_documents_that_it_excludes_the_entry_itself() -> None:
    """The exclude-self decision has to reach a client, not just a code reviewer.

    "All logs sharing the same trace_id" read literally includes the entry being resolved from.
    This server deliberately returns the *others* — see the resolver's docstring for why
    ``[itself]`` is a useless answer — and a deliberate departure from the obvious reading is only
    a decision if it is published. The description is what GraphiQL shows on hover and what lands
    in the committed ``schema.graphql``, so this assertion is the one that keeps the two honest.
    """
    description = _field("LogEntry", "relatedLogs")["description"] or ""

    assert description, "relatedLogs must carry a description; it is the only place this is stated"
    assert "excluded" in description.lower()
    assert "traceid is null" in description.lower(), (
        "the null-trace behaviour is half the requirement (spec §2 item 17) and belongs in the "
        "published description too"
    )


def test_metadata_is_published_under_that_exact_name() -> None:
    """Not ``metadata_``, not ``metadataField``, not ``meta``.

    The storage attribute has to be ``metadata_`` because ``metadata`` is reserved on a SQLAlchemy
    declarative class, and :meth:`~src.graphql.types.LogEntry.from_orm` is the one place that
    rename is undone. If that ever moves or is "simplified", the underscore reaches the API and
    every client breaks on a field that used to exist. The equality test above would also catch it;
    this one exists so the failure *names* the problem instead of printing a seven-entry dict diff.
    """
    names = set(_field_types("LogEntry"))

    assert "metadata" in names
    assert not {"metadata_", "metadataField", "meta"} & names


# --- Query.logs: the shape the spec's acceptance command depends on --------------------------------


def test_query_publishes_exactly_the_four_read_entry_points() -> None:
    """``logs``, ``log``, ``logsConnection``, ``logStats``.

    Camel-casing is on, so it is not ``logs_connection`` or ``log_stats`` — the spec's own
    acceptance commands are written in that casing and would not validate otherwise.
    """
    assert set(_field_types("Query")) == {"logs", "log", "logsConnection", "logStats"}


def test_logs_returns_a_non_null_list_of_non_null_log_entries() -> None:
    """``[LogEntry!]!`` — a bare list, NOT a connection.

    This is the assertion that protects the spec's §5 acceptance command::

        curl -X POST /graphql -d '{"query": "{ logs { id service level message } }"}'

    Against a ``LogConnection`` that document does not validate — ``id`` and ``service`` are not
    fields of a connection — so "upgrading" ``logs`` to a connection would break the acceptance
    criteria while leaving pagination-shaped tests perfectly green. Cursor pagination lives on
    ``logsConnection`` precisely so both can be true at once.
    """
    assert _field_types("Query")["logs"] == "[LogEntry!]!"


def test_logs_takes_an_argument_literally_named_filters() -> None:
    """The spec writes ``Query.logs(filters)``; the name is part of every published example."""
    args = {arg["name"]: _render_type(arg["type"]) for arg in _field("Query", "logs")["args"]}

    assert args == {"filters": "LogFilterInput"}


def test_log_takes_an_id_and_may_return_null() -> None:
    """A miss is ``null``, so the return type must be nullable; the id itself is required."""
    args = {arg["name"]: _render_type(arg["type"]) for arg in _field("Query", "log")["args"]}

    assert args == {"id": "ID!"}
    assert _field_types("Query")["log"] == "LogEntry"


# --- LogFilterInput ------------------------------------------------------------------------------


def test_log_filter_input_has_all_six_fields_and_every_one_is_optional() -> None:
    """Spec §2 items 18-19: six filters, and omitting any of them must be legal.

    A single ``NON_NULL`` here would make that filter mandatory, so "omitted filters are ignored"
    could not even be expressed — the request would fail validation before a resolver saw it. The
    rendered types are compared exactly, which pins both the set of fields and their optionality in
    one assertion.
    """
    assert _input_field_types("LogFilterInput") == {
        "service": "String",
        "level": "LogLevel",
        "startTime": "DateTime",
        "endTime": "DateTime",
        "searchText": "String",
        "limit": "Int",
    }


def test_no_log_filter_input_field_is_required() -> None:
    """The same guarantee stated the way it fails: nothing in the filter set is ``NON_NULL``."""
    required = [
        name for name, rendered in _input_field_types("LogFilterInput").items()
        if rendered.endswith("!")
    ]

    assert required == [], f"these filters are required and must not be: {required}"


# --- The connection (the §4 bonus, on its own field) -----------------------------------------------


def test_logs_connection_publishes_a_relay_shaped_window() -> None:
    """Edges, page info and a total count — and it is a separate field from ``logs``."""
    assert _field_types("Query")["logsConnection"] == "LogConnection!"
    assert _field_types("LogConnection") == {
        "edges": "[LogEdge!]!",
        "pageInfo": "PageInfo!",
        "totalCount": "Int!",
    }
    assert _field_types("LogEdge") == {"cursor": "String!", "node": "LogEntry!"}
    assert _field_types("PageInfo") == {
        "hasNextPage": "Boolean!",
        "hasPreviousPage": "Boolean!",
        "startCursor": "String",
        "endCursor": "String",
    }


def test_logs_connection_takes_filters_first_and_after() -> None:
    """It accepts the **same** filter input ``logs`` does, so a filter means one thing everywhere."""
    args = {
        arg["name"]: _render_type(arg["type"])
        for arg in _field("Query", "logsConnection")["args"]
    }

    assert args == {"filters": "LogFilterInput", "first": "Int", "after": "String"}


# --- logStats: the shape the spec's OTHER acceptance command depends on (C4) ------------------------


def test_log_stats_takes_two_optional_time_bounds() -> None:
    """Spec §5 runs ``{ logStats { … } }`` with **no arguments**.

    A single ``NON_NULL`` here would make that document fail validation, so the optionality is not
    a convenience — it is what the acceptance command requires. Omitting a bound means that end is
    unbounded, matching ``LogFilterInput``.
    """
    args = {arg["name"]: _render_type(arg["type"]) for arg in _field("Query", "logStats")["args"]}

    assert args == {"startTime": "DateTime", "endTime": "DateTime"}
    assert _field_types("Query")["logStats"] == "LogStats!"


def test_services_is_a_leaf_list_so_the_spec_acceptance_command_validates() -> None:
    """``{ logStats { totalLogs errorCount services } }`` selects ``services`` with NO sub-selection.

    GraphQL forbids a sub-selection on a scalar field and *requires* one on an object field, so
    this single rendered type decides whether the spec's §5 command parses at all. Publishing
    ``services`` as ``[ServiceCount!]!`` would break it — which is why the richer per-service view
    lives on the separate ``serviceBreakdown`` field instead of replacing this one. See the block
    comment above ``LogStats`` in ``src/graphql/types.py``.
    """
    assert _field_types("LogStats")["services"] == "[String!]!"


def test_log_stats_publishes_the_spec_minimum_plus_the_dashboard_extras() -> None:
    """Asserted as an equality, so an added or removed field fails here rather than in a client.

    ``earliest``/``latest`` are the two nullable fields and must stay nullable: they are SQL
    ``min``/``max`` over the matching rows, which are ``NULL`` when a window matched nothing — and
    "no logs in this window" has to be an ordinary answer, not an error.
    """
    assert _field_types("LogStats") == {
        "totalLogs": "Int!",
        "errorCount": "Int!",
        "services": "[String!]!",
        "serviceBreakdown": "[ServiceCount!]!",
        "levelBreakdown": "[LevelCount!]!",
        "earliest": "DateTime",
        "latest": "DateTime",
    }


def test_the_breakdown_types_are_name_count_pairs() -> None:
    """``levelBreakdown`` carries the **enum**, not a string — the same contract ``LogEntry`` has."""
    assert _field_types("ServiceCount") == {"service": "String!", "count": "Int!"}
    assert _field_types("LevelCount") == {"level": "LogLevel!", "count": "Int!"}


def test_error_count_counts_the_published_error_enum_member() -> None:
    """The string the aggregate filters on is the enum member's value, not a lookalike.

    ``ERROR_LEVEL`` lives in the store (which must not import the API layer) and ``LogLevel`` is
    the published contract. Nothing at run time forces them to agree, and a typo would make
    ``errorCount`` a permanent zero — a plausible number, in a field the spec verifies by eye.
    """
    from src.db.repository import ERROR_LEVEL
    from src.graphql.enums import LogLevel

    assert ERROR_LEVEL == LogLevel.ERROR.value
    assert ERROR_LEVEL in LOG_LEVELS


# --- Mutation.createLog (C4) -----------------------------------------------------------------------


def test_the_schema_publishes_a_mutation_root_with_create_log() -> None:
    """Spec §2 item 24. A schema with no ``Mutation`` type makes every write document invalid."""
    assert set(_field_types("Mutation")) == {"createLog"}
    assert _field_types("Mutation")["createLog"] == "LogEntry!", (
        "the spec requires the created object back, and non-null because a successful mutation "
        "always has one — a nullable return would force every client to branch on a case that "
        "cannot happen"
    )


def test_create_log_takes_an_argument_literally_named_log_data() -> None:
    """Spec §5 writes ``createLog(logData: {...})``.

    ``input`` (the Relay convention) or ``entry`` would read just as well and would break the
    acceptance command while every behavioural test stayed green. The name is the contract.
    """
    args = {arg["name"]: _render_type(arg["type"]) for arg in _field("Mutation", "createLog")["args"]}

    assert args == {"logData": "CreateLogInput!"}


def test_create_log_input_requires_a_source_a_severity_and_a_message() -> None:
    """Three required, three optional — and the split is the domain's, not a convenience.

    ``level`` being the enum is what makes ``level: "EROR"`` a validation error naming the five
    legal values instead of a stored row nothing can ever filter for.
    """
    assert _input_field_types("CreateLogInput") == {
        "service": "String!",
        "level": "LogLevel!",
        "message": "String!",
        "timestamp": "DateTime",
        "metadata": "JSON",
        "traceId": "String",
    }


def test_create_log_input_publishes_metadata_and_trace_id_under_the_wire_names() -> None:
    """``metadata`` (no underscore, matching the column) and ``traceId`` (camel-cased).

    The write input and the read type have to agree on both, or a client would create an entry
    with ``trace_id`` and read it back as ``traceId``.
    """
    assert set(_input_field_types("CreateLogInput")) >= {"metadata", "traceId"}
    assert set(_input_field_types("CreateLogInput")) & {"metadata_", "trace_id"} == set()


# --- Subscription.logStream (C6) ---------------------------------------------------------------


def test_the_schema_publishes_a_subscription_root_with_log_stream() -> None:
    """Spec §2 item 25.

    Declaring a subscription root is also what makes ``GraphQLRouter``'s WebSocket half reachable:
    the mount and its ``subscription_protocols`` have been in place since C1, so without this type
    a ``graphql-transport-ws`` client negotiates a socket successfully and then finds that no
    ``subscribe`` message can ever validate.
    """
    assert set(_field_types("Subscription")) == {"logStream"}
    assert _field_types("Subscription")["logStream"] == "LogEntry!", (
        "a streamed entry is never null — a null frame would be a payload a client has to branch "
        "on for a case the server cannot produce"
    )


def test_log_stream_takes_two_optional_server_side_filters() -> None:
    """Spec §2 item 26: ``service`` and ``level``, both optional, filtered on the server.

    Optional in both directions matters. Required arguments would make "stream everything" —
    which is what the C13 dashboard's live tail asks for — impossible to express. And ``level``
    being the enum rather than ``String`` is what makes ``level: EROR`` a validation error that
    names the five legal values, rather than a socket that opens, validates, and then stays
    silent forever because nothing can match.
    """
    args = {
        arg["name"]: _render_type(arg["type"]) for arg in _field("Subscription", "logStream")["args"]
    }

    assert args == {"service": "String", "level": "LogLevel"}


def test_log_stream_documents_its_back_pressure_policy() -> None:
    """The lossiness has to reach a client, not just a code reviewer.

    A subscriber that falls far enough behind is **dropped**, not buffered without limit. That is
    the right trade for a server, and it is a surprise to a client that has not been told — so the
    description carries it into the SDL and into GraphiQL, where somebody writing a subscription
    will actually read it.
    """
    description = _field("Subscription", "logStream")["description"] or ""

    assert "SLOW_CONSUMER" in description
    assert "SERVER-SIDE" in description
