from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .errors import ParseError


class TokenType(str, Enum):
    KEYWORD = "KEYWORD"
    IDENT = "IDENT"
    NUMBER = "NUMBER"
    STRING = "STRING"
    OP = "OP"
    PUNCT = "PUNCT"
    EOF = "EOF"


@dataclass
class Token:
    type: TokenType
    value: str
    line: int
    col: int


KEYWORDS: frozenset[str] = frozenset(
    {
        "SELECT",
        "DISTINCT",
        "FROM",
        "WHERE",
        "GROUP",
        "BY",
        "ORDER",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "AS",
        "AND",
        "OR",
        "NOT",
        "IN",
        "BETWEEN",
        "CONTAINS",
        "ASC",
        "DESC",
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "TRUE",
        "FALSE",
        "NULL",
    }
)


# Two-character operators checked before single-character ones.
_TWO_CHAR_OPS: tuple[str, ...] = ("!=", "<=", ">=")
_ONE_CHAR_OPS: tuple[str, ...] = ("=", "<", ">", "*")
_PUNCT: tuple[str, ...] = (",", "(", ")", ";")


def tokenize(sql: str) -> list[Token]:
    """Break a SQL string into a list of tokens, terminated by an EOF token.

    - Whitespace (space, tab, newline, carriage return) is skipped.
    - Line/column are 1-based.
    - Single-quoted strings support SQL-style escaped quotes via doubling (`''`).
    - Keywords are stored uppercase to make lookup case-insensitive.
    - Identifiers preserve their original case.
    - Raises ParseError on unclosed strings or unknown characters.
    """

    tokens: list[Token] = []
    line = 1
    col = 1
    i = 0
    n = len(sql)

    def advance(ch: str) -> None:
        nonlocal line, col
        if ch == "\n":
            line += 1
            col = 1
        else:
            col += 1

    while i < n:
        ch = sql[i]

        # Whitespace
        if ch == " " or ch == "\t" or ch == "\n" or ch == "\r":
            advance(ch)
            i += 1
            continue

        start_line = line
        start_col = col

        # Single-quoted string literal
        if ch == "'":
            i += 1
            col += 1
            buf: list[str] = []
            closed = False
            while i < n:
                c = sql[i]
                if c == "'":
                    # Doubled quote = escaped single quote
                    if i + 1 < n and sql[i + 1] == "'":
                        buf.append("'")
                        i += 2
                        col += 2
                        continue
                    # Closing quote
                    i += 1
                    col += 1
                    closed = True
                    break
                if c == "\n":
                    buf.append(c)
                    i += 1
                    line += 1
                    col = 1
                    continue
                buf.append(c)
                i += 1
                col += 1
            if not closed:
                raise ParseError(
                    "unclosed string literal",
                    start_line,
                    start_col,
                    got="'",
                    expected="closing single-quote",
                )
            tokens.append(Token(TokenType.STRING, "".join(buf), start_line, start_col))
            continue

        # Numbers: integer or decimal. Leading `-` is a separate OP token.
        if ch.isdigit() or (ch == "." and i + 1 < n and sql[i + 1].isdigit()):
            num_buf: list[str] = []
            saw_dot = False
            while i < n:
                c = sql[i]
                if c.isdigit():
                    num_buf.append(c)
                    i += 1
                    col += 1
                    continue
                if c == "." and not saw_dot:
                    saw_dot = True
                    num_buf.append(c)
                    i += 1
                    col += 1
                    continue
                break
            tokens.append(
                Token(TokenType.NUMBER, "".join(num_buf), start_line, start_col)
            )
            continue

        # Identifiers / keywords
        if ch.isalpha() or ch == "_":
            id_buf: list[str] = []
            while i < n:
                c = sql[i]
                if c.isalnum() or c == "_":
                    id_buf.append(c)
                    i += 1
                    col += 1
                    continue
                break
            raw = "".join(id_buf)
            upper = raw.upper()
            if upper in KEYWORDS:
                tokens.append(Token(TokenType.KEYWORD, upper, start_line, start_col))
            else:
                tokens.append(Token(TokenType.IDENT, raw, start_line, start_col))
            continue

        # Two-char operators first
        if i + 1 < n:
            two = sql[i : i + 2]
            if two in _TWO_CHAR_OPS:
                tokens.append(Token(TokenType.OP, two, start_line, start_col))
                i += 2
                col += 2
                continue

        # Single-char operators
        if ch in _ONE_CHAR_OPS:
            tokens.append(Token(TokenType.OP, ch, start_line, start_col))
            i += 1
            col += 1
            continue

        # Punctuation
        if ch in _PUNCT:
            tokens.append(Token(TokenType.PUNCT, ch, start_line, start_col))
            i += 1
            col += 1
            continue

        # Unknown character
        raise ParseError(
            "unknown character",
            start_line,
            start_col,
            got=ch,
            expected="valid SQL token",
        )

    tokens.append(Token(TokenType.EOF, "", line, col))
    return tokens
