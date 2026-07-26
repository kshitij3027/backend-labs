"""Export the code-first GraphQL schema to ``schema.graphql``.

::

    python -m scripts.export_sdl              # rewrite <repo>/schema.graphql
    python -m scripts.export_sdl --stdout     # print it instead (for a container, where the
                                              # filesystem is thrown away on exit)

.. rubric:: Why an SDL file is committed at all

This project builds its schema from Python types, so the SDL does not exist as source — it is a
**build output**. Left uncommitted, every schema change would be invisible in review: making a
field nullable, renaming an argument or dropping a type all look like ordinary edits to a
dataclass, and the person reviewing them never sees the published contract move.

Committing the generated SDL and failing a test when it drifts
(``tests/unit/test_schema_sdl.py``) turns each of those into an explicit diff sitting beside the
Python that caused it. Intended changes cost one regeneration; unintended ones cost a red test
instead of a silently broken client.

.. rubric:: The one normalisation, stated so nobody has to guess

:func:`render_sdl` is ``schema.as_str()`` with the trailing newline normalised to exactly one —
the POSIX text-file convention, and the thing an editor, ``git diff`` and a shell heredoc all
quietly disagree about. Both the writer and the drift test call this same function, so the
committed file is compared against exactly what regenerating would produce; the test additionally
asserts the content matches ``schema.as_str()`` itself once trailing whitespace is set aside, so
the normalisation cannot hide a real difference.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from src.graphql.schema import schema

#: Repository root. ``scripts/export_sdl.py`` -> ``scripts/`` -> the project directory. Derived
#: from ``__file__`` rather than from the working directory so the target is the same whether this
#: is run from the repo root, from inside ``scripts/``, or as ``python -m`` in the container.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

#: The committed SDL. Deliberately absent from ``.gitignore`` and explicitly kept in the Docker
#: build context (see the notes in ``.gitignore`` and ``.dockerignore``) — the tester image has to
#: hold a copy for the drift test to compare against.
SCHEMA_PATH = PROJECT_ROOT / "schema.graphql"


def render_sdl() -> str:
    """Return the SDL exactly as it should appear on disk: one trailing newline, no more, no less."""
    return schema.as_str().rstrip("\n") + "\n"


def export(path: Path = SCHEMA_PATH) -> Path:
    """Write :func:`render_sdl` to ``path`` and return it.

    ``encoding="utf-8"`` and ``newline="\\n"`` are both explicit. The default encoding is
    locale-dependent and the default newline translation rewrites ``\\n`` to ``\\r\\n`` on Windows,
    either of which would produce a file that differs from the one the test renders in memory —
    a drift failure caused entirely by where the exporter was run.
    """
    path.write_text(render_sdl(), encoding="utf-8", newline="\n")
    return path


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns a process exit status."""
    parser = argparse.ArgumentParser(description="Export the GraphQL schema as SDL.")
    parser.add_argument(
        "--stdout",
        action="store_true",
        help=(
            "print the SDL instead of writing it. Use this inside the container — its filesystem "
            "is discarded on exit, so a written file cannot reach the repository."
        ),
    )
    parser.add_argument(
        "--path",
        type=Path,
        default=SCHEMA_PATH,
        help=f"where to write the SDL (default: {SCHEMA_PATH})",
    )
    args = parser.parse_args(argv)

    if args.stdout:
        # `end=""` — render_sdl already ends in exactly one newline, and print would add a second.
        print(render_sdl(), end="")
        return 0

    written = export(args.path)
    print(f"wrote {written} ({written.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
