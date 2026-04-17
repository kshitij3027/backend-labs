from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Union


@dataclass(frozen=True)
class Identifier:
    name: str


@dataclass(frozen=True)
class StringLit:
    value: str


@dataclass(frozen=True)
class NumberLit:
    value: float


@dataclass(frozen=True)
class BoolLit:
    value: bool


@dataclass(frozen=True)
class Star:
    pass


@dataclass(frozen=True)
class FuncCall:
    name: str
    args: tuple["Expr", ...]


@dataclass(frozen=True)
class BinOp:
    op: str
    left: "Expr"
    right: "Expr"


@dataclass(frozen=True)
class In:
    field: Identifier
    values: tuple["Expr", ...]


@dataclass(frozen=True)
class Between:
    field: Identifier
    low: "Expr"
    high: "Expr"


@dataclass(frozen=True)
class Contains:
    field: Identifier
    needle: StringLit


@dataclass(frozen=True)
class Not:
    expr: "Expr"


Expr = Union[Identifier, StringLit, NumberLit, BoolLit, Star, FuncCall, BinOp, In, Between, Contains, Not]


@dataclass(frozen=True)
class Column:
    expr: Expr
    alias: str | None


@dataclass(frozen=True)
class OrderByItem:
    field: Identifier
    direction: str


@dataclass(frozen=True)
class Select:
    columns: tuple[Column, ...]
    table: str
    where: Expr | None
    group_by: tuple[Identifier, ...]
    having: Expr | None
    order_by: tuple[OrderByItem, ...]
    limit: int | None
    offset: int | None


def walk(node: object) -> Iterator[object]:
    yield node
    if isinstance(node, Select):
        for col in node.columns:
            yield from walk(col)
        if node.where is not None:
            yield from walk(node.where)
        for gb in node.group_by:
            yield from walk(gb)
        if node.having is not None:
            yield from walk(node.having)
        for ob in node.order_by:
            yield from walk(ob)
    elif isinstance(node, Column):
        yield from walk(node.expr)
    elif isinstance(node, OrderByItem):
        yield from walk(node.field)
    elif isinstance(node, FuncCall):
        for arg in node.args:
            yield from walk(arg)
    elif isinstance(node, BinOp):
        yield from walk(node.left)
        yield from walk(node.right)
    elif isinstance(node, In):
        yield from walk(node.field)
        for v in node.values:
            yield from walk(v)
    elif isinstance(node, Between):
        yield from walk(node.field)
        yield from walk(node.low)
        yield from walk(node.high)
    elif isinstance(node, Contains):
        yield from walk(node.field)
        yield from walk(node.needle)
    elif isinstance(node, Not):
        yield from walk(node.expr)
