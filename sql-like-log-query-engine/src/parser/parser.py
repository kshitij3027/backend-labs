from __future__ import annotations

from src.shared import ast

from .errors import ParseError
from .tokenizer import Token, TokenType, tokenize


_AGG_FUNCS: frozenset[str] = frozenset({"COUNT", "SUM", "AVG", "MIN", "MAX"})
_COMPARISON_OPS: frozenset[str] = frozenset({"=", "!=", "<", "<=", ">", ">="})


class Parser:
    """Recursive-descent parser for the SQL-like query subset.

    Grammar (case-insensitive keywords):
        select      := SELECT [DISTINCT] col_list FROM IDENT
                       [WHERE expr]
                       [GROUP BY id_list]
                       [HAVING expr]
                       [ORDER BY IDENT (ASC|DESC)?]
                       [LIMIT NUMBER]
                       [OFFSET NUMBER]
                       [;] EOF
        col_list    := '*' | column (',' column)*
        column      := (IDENT | FUNC '(' (IDENT | '*') ')') (AS IDENT)?
        expr        := or_expr
        or_expr     := and_expr ('OR' and_expr)*
        and_expr    := not_expr ('AND' not_expr)*
        not_expr    := 'NOT' not_expr | predicate
        predicate   := primary ( comparison primary
                                | 'IN' '(' primary (',' primary)* ')'
                                | 'BETWEEN' primary 'AND' primary
                                | 'CONTAINS' STRING )?
        primary     := NUMBER | STRING | TRUE | FALSE
                     | IDENT | '*'
                     | FUNC '(' (IDENT | '*') ')'
                     | '(' expr ')'
    """

    def __init__(self, tokens: list[Token]) -> None:
        self.tokens = tokens
        self.pos = 0

    # --- cursor helpers ---------------------------------------------------

    def _peek(self, offset: int = 0) -> Token:
        idx = self.pos + offset
        if idx >= len(self.tokens):
            return self.tokens[-1]
        return self.tokens[idx]

    def _advance(self) -> Token:
        tok = self.tokens[self.pos]
        if tok.type is not TokenType.EOF:
            self.pos += 1
        return tok

    def _at_eof(self) -> bool:
        return self._peek().type is TokenType.EOF

    def _error(self, msg: str, expected: str | None = None) -> ParseError:
        tok = self._peek()
        got = tok.value if tok.type is not TokenType.EOF else "<eof>"
        return ParseError(msg, tok.line, tok.col, got=got, expected=expected)

    def _expect_keyword(self, kw: str) -> Token:
        tok = self._peek()
        if tok.type is TokenType.KEYWORD and tok.value == kw:
            return self._advance()
        raise self._error(f"expected keyword {kw}", expected=kw)

    def _match_keyword(self, *keywords: str) -> Token | None:
        tok = self._peek()
        if tok.type is TokenType.KEYWORD and tok.value in keywords:
            return self._advance()
        return None

    def _expect_punct(self, ch: str) -> Token:
        tok = self._peek()
        if tok.type is TokenType.PUNCT and tok.value == ch:
            return self._advance()
        raise self._error(f"expected '{ch}'", expected=ch)

    def _match_punct(self, ch: str) -> Token | None:
        tok = self._peek()
        if tok.type is TokenType.PUNCT and tok.value == ch:
            return self._advance()
        return None

    def _expect_ident(self, what: str = "identifier") -> Token:
        tok = self._peek()
        if tok.type is TokenType.IDENT:
            return self._advance()
        raise self._error(f"expected {what}", expected=what)

    # --- public entry point ----------------------------------------------

    def parse(self) -> ast.Select:
        if self._at_eof():
            raise self._error("empty query", expected="SELECT")

        select_node = self._parse_select()

        # Optional trailing semicolon
        self._match_punct(";")

        if not self._at_eof():
            raise self._error("unexpected trailing tokens", expected="end of query")

        return select_node

    # --- statement -------------------------------------------------------

    def _parse_select(self) -> ast.Select:
        self._expect_keyword("SELECT")

        # DISTINCT is parsed but ignored downstream.
        self._match_keyword("DISTINCT")

        columns = self._parse_col_list()

        self._expect_keyword("FROM")

        table_tok = self._peek()
        if table_tok.type is not TokenType.IDENT:
            raise self._error("expected table name after FROM", expected="table name")
        self._advance()
        table = table_tok.value

        where: ast.Expr | None = None
        if self._match_keyword("WHERE") is not None:
            where = self._parse_expr()

        group_by: tuple[ast.Identifier, ...] = tuple()
        if self._match_keyword("GROUP") is not None:
            self._expect_keyword("BY")
            group_by = tuple(self._parse_id_list())

        having: ast.Expr | None = None
        if self._match_keyword("HAVING") is not None:
            having = self._parse_expr()

        order_by: tuple[ast.OrderByItem, ...] = tuple()
        if self._match_keyword("ORDER") is not None:
            self._expect_keyword("BY")
            field_tok = self._expect_ident("ORDER BY column")
            direction = "ASC"
            dir_tok = self._match_keyword("ASC", "DESC")
            if dir_tok is not None:
                direction = dir_tok.value
            order_by = (
                ast.OrderByItem(
                    field=ast.Identifier(name=field_tok.value), direction=direction
                ),
            )

        limit: int | None = None
        if self._match_keyword("LIMIT") is not None:
            limit = self._parse_non_negative_int("LIMIT")

        offset: int | None = None
        if self._match_keyword("OFFSET") is not None:
            offset = self._parse_non_negative_int("OFFSET")

        return ast.Select(
            columns=columns,
            table=table,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    # --- select column list ---------------------------------------------

    def _parse_col_list(self) -> tuple[ast.Column, ...]:
        tok = self._peek()

        # Bare * with nothing after means SELECT * FROM ...
        if tok.type is TokenType.OP and tok.value == "*":
            self._advance()
            # Disallow `SELECT *, foo FROM ...` — keep grammar simple.
            return (ast.Column(expr=ast.Star(), alias=None),)

        columns: list[ast.Column] = [self._parse_column()]
        while self._match_punct(",") is not None:
            columns.append(self._parse_column())
        return tuple(columns)

    def _parse_column(self) -> ast.Column:
        expr: ast.Expr
        tok = self._peek()

        if tok.type is TokenType.KEYWORD and tok.value in _AGG_FUNCS:
            expr = self._parse_func_call()
        elif tok.type is TokenType.IDENT:
            self._advance()
            expr = ast.Identifier(name=tok.value)
        else:
            raise self._error(
                "expected column name or aggregate function",
                expected="identifier or aggregate",
            )

        alias: str | None = None
        if self._match_keyword("AS") is not None:
            alias_tok = self._expect_ident("alias name")
            alias = alias_tok.value

        return ast.Column(expr=expr, alias=alias)

    def _parse_func_call(self) -> ast.FuncCall:
        name_tok = self._advance()  # the function keyword itself
        fn_name = name_tok.value  # already uppercase from the tokenizer
        self._expect_punct("(")

        args: list[ast.Expr] = []
        star_tok = self._peek()
        if star_tok.type is TokenType.OP and star_tok.value == "*":
            self._advance()
            args.append(ast.Star())
        else:
            ident_tok = self._expect_ident("function argument")
            args.append(ast.Identifier(name=ident_tok.value))

        self._expect_punct(")")
        return ast.FuncCall(name=fn_name, args=tuple(args))

    # --- GROUP BY column list -------------------------------------------

    def _parse_id_list(self) -> list[ast.Identifier]:
        ident_tok = self._expect_ident("column name")
        idents = [ast.Identifier(name=ident_tok.value)]
        while self._match_punct(",") is not None:
            ident_tok = self._expect_ident("column name")
            idents.append(ast.Identifier(name=ident_tok.value))
        return idents

    # --- LIMIT / OFFSET --------------------------------------------------

    def _parse_non_negative_int(self, clause: str) -> int:
        tok = self._peek()
        if tok.type is not TokenType.NUMBER:
            raise self._error(
                f"expected non-negative integer after {clause}",
                expected="integer",
            )
        self._advance()
        if "." in tok.value:
            raise self._error(
                f"{clause} requires an integer, not a decimal",
                expected="integer",
            )
        try:
            value = int(tok.value)
        except ValueError as exc:  # pragma: no cover - tokenizer guards this
            raise self._error(
                f"invalid integer literal for {clause}", expected="integer"
            ) from exc
        if value < 0:
            raise self._error(
                f"{clause} must be non-negative", expected="non-negative integer"
            )
        return value

    # --- expressions -----------------------------------------------------

    def _parse_expr(self) -> ast.Expr:
        return self._parse_or()

    def _parse_or(self) -> ast.Expr:
        left = self._parse_and()
        while self._match_keyword("OR") is not None:
            right = self._parse_and()
            left = ast.BinOp(op="OR", left=left, right=right)
        return left

    def _parse_and(self) -> ast.Expr:
        left = self._parse_not()
        while self._match_keyword("AND") is not None:
            right = self._parse_not()
            left = ast.BinOp(op="AND", left=left, right=right)
        return left

    def _parse_not(self) -> ast.Expr:
        if self._match_keyword("NOT") is not None:
            inner = self._parse_not()
            return ast.Not(expr=inner)
        return self._parse_predicate()

    def _parse_predicate(self) -> ast.Expr:
        left = self._parse_primary()

        # IN / BETWEEN / CONTAINS require the LHS to be an identifier.
        kw = self._peek()
        if kw.type is TokenType.KEYWORD and kw.value in ("IN", "BETWEEN", "CONTAINS"):
            if not isinstance(left, ast.Identifier):
                raise self._error(
                    f"{kw.value} requires an identifier on the left side",
                    expected="identifier",
                )
            self._advance()
            if kw.value == "IN":
                return self._parse_in_tail(left)
            if kw.value == "BETWEEN":
                return self._parse_between_tail(left)
            # CONTAINS
            return self._parse_contains_tail(left)

        # Comparison operators
        op_tok = self._peek()
        if op_tok.type is TokenType.OP and op_tok.value in _COMPARISON_OPS:
            self._advance()
            right = self._parse_primary()
            return ast.BinOp(op=op_tok.value, left=left, right=right)

        return left

    def _parse_in_tail(self, field: ast.Identifier) -> ast.In:
        open_tok = self._peek()
        if not (open_tok.type is TokenType.PUNCT and open_tok.value == "("):
            raise self._error("expected '(' after IN", expected="(")
        self._advance()

        values: list[ast.Expr] = []
        # Require at least one value.
        if self._peek().type is TokenType.PUNCT and self._peek().value == ")":
            raise self._error(
                "IN (...) requires at least one value",
                expected="literal or identifier",
            )
        values.append(self._parse_primary())
        while self._match_punct(",") is not None:
            values.append(self._parse_primary())

        close_tok = self._peek()
        if not (close_tok.type is TokenType.PUNCT and close_tok.value == ")"):
            raise self._error("unclosed IN (...)", expected=")")
        self._advance()
        return ast.In(field=field, values=tuple(values))

    def _parse_between_tail(self, field: ast.Identifier) -> ast.Between:
        low = self._parse_primary()
        self._expect_keyword("AND")
        high = self._parse_primary()
        return ast.Between(field=field, low=low, high=high)

    def _parse_contains_tail(self, field: ast.Identifier) -> ast.Contains:
        tok = self._peek()
        if tok.type is not TokenType.STRING:
            raise self._error(
                "CONTAINS requires a string literal on the right side",
                expected="string literal",
            )
        self._advance()
        return ast.Contains(field=field, needle=ast.StringLit(value=tok.value))

    def _parse_primary(self) -> ast.Expr:
        tok = self._peek()

        if tok.type is TokenType.NUMBER:
            self._advance()
            return ast.NumberLit(value=float(tok.value))

        if tok.type is TokenType.STRING:
            self._advance()
            return ast.StringLit(value=tok.value)

        if tok.type is TokenType.KEYWORD and tok.value in ("TRUE", "FALSE"):
            self._advance()
            return ast.BoolLit(value=(tok.value == "TRUE"))

        if tok.type is TokenType.KEYWORD and tok.value in _AGG_FUNCS:
            return self._parse_func_call()

        if tok.type is TokenType.IDENT:
            self._advance()
            return ast.Identifier(name=tok.value)

        if tok.type is TokenType.OP and tok.value == "*":
            self._advance()
            return ast.Star()

        if tok.type is TokenType.PUNCT and tok.value == "(":
            self._advance()
            inner = self._parse_expr()
            close_tok = self._peek()
            if not (close_tok.type is TokenType.PUNCT and close_tok.value == ")"):
                raise self._error("unclosed parenthesis", expected=")")
            self._advance()
            return inner

        raise self._error("unexpected token in expression", expected="expression")


def parse_sql(sql: str) -> ast.Select:
    """Tokenize and parse a SQL query string, returning the AST root."""

    tokens = tokenize(sql)
    return Parser(tokens).parse()
