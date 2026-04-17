from __future__ import annotations

import pytest

from src.parser import ParseError, Token, TokenType, tokenize


def _types(tokens: list[Token]) -> list[TokenType]:
    return [t.type for t in tokens]


def _values(tokens: list[Token]) -> list[str]:
    return [t.value for t in tokens]


def test_tokenize_returns_eof_for_empty_input():
    toks = tokenize("")
    assert len(toks) == 1
    assert toks[0].type is TokenType.EOF
    assert toks[0].value == ""


def test_tokenize_select_star_from_table():
    toks = tokenize("SELECT * FROM logs")
    assert _types(toks) == [
        TokenType.KEYWORD,
        TokenType.OP,
        TokenType.KEYWORD,
        TokenType.IDENT,
        TokenType.EOF,
    ]
    assert _values(toks) == ["SELECT", "*", "FROM", "logs", ""]


def test_keywords_are_case_insensitive_and_uppercased():
    for form in ("select", "SELECT", "Select", "SeLeCt"):
        toks = tokenize(f"{form} * FROM logs")
        assert toks[0].type is TokenType.KEYWORD
        assert toks[0].value == "SELECT"


def test_identifiers_preserve_case():
    toks = tokenize("SELECT UserId FROM Logs")
    # SELECT is keyword, UserId is ident, FROM is keyword, Logs is ident
    assert toks[1].type is TokenType.IDENT
    assert toks[1].value == "UserId"
    assert toks[3].type is TokenType.IDENT
    assert toks[3].value == "Logs"


def test_single_quoted_string_with_spaces_and_specials():
    toks = tokenize("SELECT 'hello world, !@#' FROM logs")
    string_tok = toks[1]
    assert string_tok.type is TokenType.STRING
    assert string_tok.value == "hello world, !@#"


def test_escaped_single_quotes_via_doubling():
    toks = tokenize("SELECT 'it''s fine' FROM logs")
    string_tok = toks[1]
    assert string_tok.type is TokenType.STRING
    assert string_tok.value == "it's fine"


def test_unclosed_string_raises_parse_error():
    with pytest.raises(ParseError) as exc:
        tokenize("SELECT 'abc FROM logs")
    assert exc.value.line == 1
    # Column of the opening quote (1-based): after "SELECT "
    assert exc.value.col == 8


def test_unknown_character_raises_parse_error():
    with pytest.raises(ParseError) as exc:
        tokenize("SELECT @foo FROM logs")
    assert exc.value.got == "@"
    assert exc.value.line == 1
    assert exc.value.col == 8


def test_all_operators_tokenize_correctly():
    toks = tokenize("= != < <= > >= *")
    assert _types(toks[:-1]) == [TokenType.OP] * 7
    assert _values(toks[:-1]) == ["=", "!=", "<", "<=", ">", ">=", "*"]


def test_two_char_operators_preferred_over_single_char():
    toks = tokenize("a<=b")
    # a, <=, b, EOF
    assert _types(toks) == [
        TokenType.IDENT,
        TokenType.OP,
        TokenType.IDENT,
        TokenType.EOF,
    ]
    assert toks[1].value == "<="


def test_not_equal_operator():
    toks = tokenize("a != b")
    assert toks[1].type is TokenType.OP
    assert toks[1].value == "!="


def test_punctuation_tokens():
    toks = tokenize(", ( ) ;")
    assert _types(toks[:-1]) == [TokenType.PUNCT] * 4
    assert _values(toks[:-1]) == [",", "(", ")", ";"]


def test_integer_and_decimal_numbers():
    toks = tokenize("123 45.67 0 .5")
    assert _types(toks[:-1]) == [TokenType.NUMBER] * 4
    assert _values(toks[:-1]) == ["123", "45.67", "0", ".5"]


def test_line_and_column_tracking_multiline():
    sql = "SELECT *\nFROM logs\n  WHERE x = 1"
    toks = tokenize(sql)
    # SELECT on line 1 col 1
    assert toks[0].line == 1 and toks[0].col == 1
    # * on line 1 col 8
    assert toks[1].value == "*" and toks[1].line == 1 and toks[1].col == 8
    # FROM on line 2 col 1
    assert toks[2].value == "FROM" and toks[2].line == 2 and toks[2].col == 1
    # logs on line 2 col 6
    assert toks[3].value == "logs" and toks[3].line == 2 and toks[3].col == 6
    # WHERE on line 3 col 3 (two-space indent)
    assert toks[4].value == "WHERE" and toks[4].line == 3 and toks[4].col == 3
    # x on line 3 col 9
    assert toks[5].value == "x" and toks[5].line == 3


def test_keywords_set_contains_expected_members():
    from src.parser.tokenizer import KEYWORDS

    expected = {
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
    assert expected <= KEYWORDS


def test_eof_is_always_last_token():
    toks = tokenize("SELECT 1 FROM logs ;")
    assert toks[-1].type is TokenType.EOF


def test_mixed_query_tokenizes_all_types():
    sql = "SELECT COUNT(*) FROM logs WHERE level IN ('ERROR','WARN')"
    toks = tokenize(sql)
    types = _types(toks)
    assert TokenType.KEYWORD in types
    assert TokenType.IDENT in types
    assert TokenType.STRING in types
    assert TokenType.OP in types
    assert TokenType.PUNCT in types
    assert toks[-1].type is TokenType.EOF
