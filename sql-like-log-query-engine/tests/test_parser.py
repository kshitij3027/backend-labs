from __future__ import annotations

import statistics
import time

import pytest

from src.parser import ParseError, parse_sql
from src.shared import ast


def test_select_star():
    node = parse_sql("SELECT * FROM logs")
    assert isinstance(node, ast.Select)
    assert node.table == "logs"
    assert len(node.columns) == 1
    assert isinstance(node.columns[0].expr, ast.Star)
    assert node.columns[0].alias is None
    assert node.where is None
    assert node.group_by == tuple()
    assert node.having is None
    assert node.order_by == tuple()
    assert node.limit is None
    assert node.offset is None


def test_columns_with_alias():
    node = parse_sql("SELECT col1, col2 AS alias FROM logs")
    assert len(node.columns) == 2
    c1, c2 = node.columns
    assert isinstance(c1.expr, ast.Identifier) and c1.expr.name == "col1"
    assert c1.alias is None
    assert isinstance(c2.expr, ast.Identifier) and c2.expr.name == "col2"
    assert c2.alias == "alias"


def test_count_star_with_alias():
    node = parse_sql("SELECT COUNT(*) AS n FROM logs")
    assert len(node.columns) == 1
    col = node.columns[0]
    assert isinstance(col.expr, ast.FuncCall)
    assert col.expr.name == "COUNT"
    assert len(col.expr.args) == 1
    assert isinstance(col.expr.args[0], ast.Star)
    assert col.alias == "n"


def test_where_equals_string_literal():
    node = parse_sql("SELECT * FROM logs WHERE level = 'ERROR'")
    assert isinstance(node.where, ast.BinOp)
    assert node.where.op == "="
    assert isinstance(node.where.left, ast.Identifier)
    assert node.where.left.name == "level"
    assert isinstance(node.where.right, ast.StringLit)
    assert node.where.right.value == "ERROR"


def test_where_boolean_precedence_and_not():
    # Expected grouping: ((a > 5) AND (b < 10)) OR (NOT (c = 1))
    node = parse_sql("SELECT * FROM logs WHERE a > 5 AND b < 10 OR NOT c = 1")
    where = node.where
    assert isinstance(where, ast.BinOp)
    assert where.op == "OR"

    left = where.left
    assert isinstance(left, ast.BinOp)
    assert left.op == "AND"
    assert isinstance(left.left, ast.BinOp) and left.left.op == ">"
    assert isinstance(left.right, ast.BinOp) and left.right.op == "<"
    assert isinstance(left.left.left, ast.Identifier)
    assert left.left.left.name == "a"
    assert isinstance(left.left.right, ast.NumberLit)
    assert left.left.right.value == 5.0

    right = where.right
    assert isinstance(right, ast.Not)
    inner = right.expr
    assert isinstance(inner, ast.BinOp) and inner.op == "="
    assert isinstance(inner.left, ast.Identifier) and inner.left.name == "c"
    assert isinstance(inner.right, ast.NumberLit) and inner.right.value == 1.0


def test_where_in_clause():
    node = parse_sql("SELECT * FROM logs WHERE level IN ('ERROR','WARN')")
    where = node.where
    assert isinstance(where, ast.In)
    assert where.field.name == "level"
    assert len(where.values) == 2
    assert all(isinstance(v, ast.StringLit) for v in where.values)
    assert [v.value for v in where.values] == ["ERROR", "WARN"]


def test_where_between_clause():
    node = parse_sql(
        "SELECT * FROM logs WHERE ts BETWEEN '2026-04-01' AND '2026-04-10'"
    )
    where = node.where
    assert isinstance(where, ast.Between)
    assert where.field.name == "ts"
    assert isinstance(where.low, ast.StringLit) and where.low.value == "2026-04-01"
    assert isinstance(where.high, ast.StringLit) and where.high.value == "2026-04-10"


def test_where_contains_clause():
    node = parse_sql("SELECT * FROM logs WHERE message CONTAINS 'timeout'")
    where = node.where
    assert isinstance(where, ast.Contains)
    assert where.field.name == "message"
    assert isinstance(where.needle, ast.StringLit)
    assert where.needle.value == "timeout"


def test_full_group_by_having_order_limit_offset():
    sql = (
        "SELECT service, COUNT(*) AS cnt FROM logs "
        "GROUP BY service "
        "HAVING COUNT(*) > 1 "
        "ORDER BY cnt DESC "
        "LIMIT 10 OFFSET 5"
    )
    node = parse_sql(sql)

    # Columns
    assert len(node.columns) == 2
    assert isinstance(node.columns[0].expr, ast.Identifier)
    assert node.columns[0].expr.name == "service"
    assert node.columns[0].alias is None
    assert isinstance(node.columns[1].expr, ast.FuncCall)
    assert node.columns[1].expr.name == "COUNT"
    assert node.columns[1].alias == "cnt"

    # Table
    assert node.table == "logs"

    # GROUP BY
    assert node.group_by == (ast.Identifier(name="service"),)

    # HAVING: COUNT(*) > 1
    having = node.having
    assert isinstance(having, ast.BinOp) and having.op == ">"
    assert isinstance(having.left, ast.FuncCall) and having.left.name == "COUNT"
    assert isinstance(having.right, ast.NumberLit) and having.right.value == 1.0

    # ORDER BY cnt DESC
    assert len(node.order_by) == 1
    ob = node.order_by[0]
    assert ob.field.name == "cnt"
    assert ob.direction == "DESC"

    # LIMIT / OFFSET
    assert node.limit == 10
    assert node.offset == 5


def test_order_by_defaults_to_asc():
    node = parse_sql("SELECT * FROM logs ORDER BY ts")
    assert len(node.order_by) == 1
    assert node.order_by[0].direction == "ASC"


def test_case_insensitive_keywords():
    node = parse_sql("select * from logs where level = 'ERROR'")
    assert node.table == "logs"
    assert isinstance(node.where, ast.BinOp)


def test_distinct_is_parsed_and_ignored():
    # Must not raise; AST is identical to non-DISTINCT form.
    node = parse_sql("SELECT DISTINCT service FROM logs")
    assert node.table == "logs"
    assert len(node.columns) == 1
    assert isinstance(node.columns[0].expr, ast.Identifier)
    assert node.columns[0].expr.name == "service"


def test_trailing_semicolon_is_allowed():
    node = parse_sql("SELECT * FROM logs;")
    assert node.table == "logs"


def test_parenthesized_expression_overrides_precedence():
    # a AND (b OR c)  — without parens it'd parse as (a AND b) OR c.
    node = parse_sql("SELECT * FROM logs WHERE a = 1 AND (b = 2 OR c = 3)")
    where = node.where
    assert isinstance(where, ast.BinOp) and where.op == "AND"
    assert isinstance(where.right, ast.BinOp) and where.right.op == "OR"


def test_not_equal_comparison():
    node = parse_sql("SELECT * FROM logs WHERE level != 'INFO'")
    where = node.where
    assert isinstance(where, ast.BinOp) and where.op == "!="


# --- error cases -----------------------------------------------------------


def test_empty_query_raises():
    with pytest.raises(ParseError) as exc:
        parse_sql("")
    assert "empty query" in exc.value.msg


def test_whitespace_only_query_raises():
    with pytest.raises(ParseError):
        parse_sql("   \n  \t ")


def test_select_from_with_no_columns_raises():
    with pytest.raises(ParseError) as exc:
        parse_sql("SELECT FROM logs")
    # The parser should complain while trying to parse the column list.
    assert exc.value.line == 1


def test_unclosed_string_in_where_raises():
    with pytest.raises(ParseError):
        parse_sql("SELECT * FROM logs WHERE x = 'abc")


def test_trailing_junk_raises():
    with pytest.raises(ParseError) as exc:
        parse_sql("SELECT * FROM logs garbage")
    assert "trailing" in exc.value.msg or "garbage" in (exc.value.got or "")


def test_missing_from_raises():
    with pytest.raises(ParseError):
        parse_sql("SELECT * logs")


def test_missing_table_after_from_raises():
    with pytest.raises(ParseError):
        parse_sql("SELECT * FROM WHERE x = 1")


def test_unclosed_in_paren_raises():
    with pytest.raises(ParseError):
        parse_sql("SELECT * FROM logs WHERE level IN ('ERROR'")


def test_in_requires_identifier_on_left():
    with pytest.raises(ParseError):
        parse_sql("SELECT * FROM logs WHERE 5 IN (1, 2, 3)")


def test_contains_requires_string_literal():
    with pytest.raises(ParseError):
        parse_sql("SELECT * FROM logs WHERE message CONTAINS 42")


def test_limit_must_be_non_negative_integer():
    with pytest.raises(ParseError):
        parse_sql("SELECT * FROM logs LIMIT 1.5")


# --- perf ------------------------------------------------------------------


def test_parse_perf():
    """Tokenize + parse of a representative query must have median <= 10 ms."""
    sql = (
        "SELECT service, COUNT(*) AS cnt FROM logs "
        "GROUP BY service "
        "HAVING COUNT(*) > 1 "
        "ORDER BY cnt DESC "
        "LIMIT 10 OFFSET 5"
    )
    # Sanity check: representative ~100–200 char query.
    assert 80 <= len(sql) <= 260

    timings: list[float] = []
    for _ in range(100):
        start = time.perf_counter()
        parse_sql(sql)
        timings.append(time.perf_counter() - start)

    median_ms = statistics.median(timings) * 1000
    assert median_ms <= 10.0, f"median parse time {median_ms:.3f} ms exceeds 10 ms"
