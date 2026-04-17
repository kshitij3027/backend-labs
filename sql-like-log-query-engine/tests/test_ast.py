from __future__ import annotations

import dataclasses

import pytest

from src.shared.ast import (
    BinOp,
    Column,
    FuncCall,
    Identifier,
    NumberLit,
    OrderByItem,
    Select,
    Star,
    StringLit,
    walk,
)


def test_identifier_equality():
    a = Identifier("level")
    b = Identifier("level")
    assert a == b
    assert hash(a) == hash(b)


def test_select_equality_with_tuples():
    left = Select(
        columns=(Column(expr=Star(), alias=None),),
        table="logs",
        where=BinOp(op="=", left=Identifier("level"), right=StringLit("ERROR")),
        group_by=(Identifier("service"),),
        having=None,
        order_by=(OrderByItem(field=Identifier("ts"), direction="DESC"),),
        limit=10,
        offset=None,
    )
    right = Select(
        columns=(Column(expr=Star(), alias=None),),
        table="logs",
        where=BinOp(op="=", left=Identifier("level"), right=StringLit("ERROR")),
        group_by=(Identifier("service"),),
        having=None,
        order_by=(OrderByItem(field=Identifier("ts"), direction="DESC"),),
        limit=10,
        offset=None,
    )
    assert left == right


def test_walk_yields_select_and_nested_exprs():
    select = Select(
        columns=(
            Column(expr=Identifier("service"), alias=None),
            Column(
                expr=FuncCall(name="COUNT", args=(Star(),)),
                alias="n",
            ),
        ),
        table="logs",
        where=BinOp(
            op="AND",
            left=BinOp(op="=", left=Identifier("level"), right=StringLit("ERROR")),
            right=BinOp(op=">", left=Identifier("ts"), right=NumberLit(100.0)),
        ),
        group_by=(Identifier("service"),),
        having=None,
        order_by=(),
        limit=None,
        offset=None,
    )
    nodes = list(walk(select))
    assert nodes[0] is select
    identifier_names = {n.name for n in nodes if isinstance(n, Identifier)}
    assert "level" in identifier_names
    assert "ts" in identifier_names
    assert "service" in identifier_names
    assert any(isinstance(n, FuncCall) and n.name == "COUNT" for n in nodes)
    assert any(isinstance(n, StringLit) and n.value == "ERROR" for n in nodes)


def test_frozen_dataclass_disallows_assignment():
    ident = Identifier("level")
    with pytest.raises(dataclasses.FrozenInstanceError):
        ident.name = "changed"  # type: ignore[misc]
