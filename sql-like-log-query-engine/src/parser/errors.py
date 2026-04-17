from __future__ import annotations


class ParseError(Exception):
    """Error raised by the tokenizer or parser on malformed SQL.

    Attributes:
        msg: Human-readable description of the problem.
        line: 1-based line number where the error was detected.
        col: 1-based column number where the error was detected.
        got: The offending token/character, if available.
        expected: A description of what was expected, if available.
    """

    def __init__(
        self,
        msg: str,
        line: int,
        col: int,
        got: str | None = None,
        expected: str | None = None,
    ) -> None:
        self.msg = msg
        self.line = line
        self.col = col
        self.got = got
        self.expected = expected
        super().__init__(self._format())

    def _format(self) -> str:
        parts = [f"{self.msg} at line {self.line}, col {self.col}"]
        if self.got is not None:
            parts.append(f"got={self.got!r}")
        if self.expected is not None:
            parts.append(f"expected={self.expected!r}")
        return " [" + "; ".join(parts) + "]"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self._format()
