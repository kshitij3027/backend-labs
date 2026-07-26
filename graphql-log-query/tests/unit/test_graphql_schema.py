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
"""

from __future__ import annotations

from typing import Any, Optional

from src.generators import LOG_LEVELS
from src.graphql.schema import schema

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
    result = schema.execute_sync(SHAPE_QUERY, variable_values={"name": name})

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


def test_log_entry_publishes_exactly_the_seven_spec_fields_with_the_right_nullability() -> None:
    """Spec §2 item 15, asserted as an equality so extra and missing fields both fail.

    The nullability half is the part that is easy to get wrong and expensive to notice:
    ``metadata`` and ``traceId`` are the two nullable columns, and roughly a third of the seeded
    corpus has each of them null. Publishing either as non-null would make those rows unresolvable
    — a client would get an error for data that is perfectly valid.
    """
    assert _field_types("LogEntry") == {
        "id": "ID!",
        "timestamp": "DateTime!",
        "service": "String!",
        "level": "LogLevel!",
        "message": "String!",
        "metadata": "JSON",
        "traceId": "String",
    }


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


def test_query_publishes_exactly_the_three_read_entry_points() -> None:
    """``logs``, ``log``, ``logsConnection`` — and camel-casing is on, so it is not ``logs_connection``."""
    assert set(_field_types("Query")) == {"logs", "log", "logsConnection"}


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
