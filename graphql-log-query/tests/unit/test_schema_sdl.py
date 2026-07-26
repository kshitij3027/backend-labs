"""The SDL drift test: ``schema.graphql`` must equal what the live schema renders.

.. rubric:: Why a generated file is committed and then policed by a test

This project is **code-first**: the schema is derived from Python types, so the SDL is a build
output rather than a source file. The cost of that — and it is the only real cost — is that a
schema change is invisible in review. Making a field nullable, renaming an argument, dropping a
type or adding one all look like ordinary edits to a dataclass, and the person reviewing the pull
request never sees the published contract move. Clients find out at run time.

Committing the generated SDL turns every schema change into an explicit diff sitting next to the
Python that caused it, and this test is what makes the committed copy trustworthy: the moment the
two disagree, the suite goes red. An **intended** change costs one regeneration
(``make sdl``, then paste); an unintended one costs a failing test instead of a broken client.

The failure message deliberately carries the entire expected SDL. When this test fails the next
question is always "what should the file say?", and the answer is the most useful thing the failure
can print — particularly in CI, where the schema is rendered inside a container whose filesystem is
discarded before anyone can look at it.
"""

from __future__ import annotations

from pathlib import Path

from scripts.export_sdl import SCHEMA_PATH, export, render_sdl
from src.graphql.schema import schema


def test_the_committed_sdl_matches_the_live_schema() -> None:
    """``schema.graphql`` is byte-for-byte what the current code produces."""
    assert SCHEMA_PATH.is_file(), (
        f"{SCHEMA_PATH} is missing. It is a committed build output — see the note in .gitignore — "
        "and Dockerfile.test copies it into the tester image so this comparison can run."
    )

    committed = SCHEMA_PATH.read_text(encoding="utf-8")
    expected = render_sdl()

    assert committed == expected, (
        "schema.graphql has drifted from the live schema.\n"
        "If the schema change was intended, regenerate the file:\n"
        "    make sdl        # prints the SDL from inside the image; paste it over schema.graphql\n"
        "    python -m scripts.export_sdl      # or, on a host with the dependencies installed\n"
        "\n--- what the live schema renders -------------------------------------------\n"
        f"{expected}"
        "----------------------------------------------------------------------------"
    )


def test_the_committed_sdl_is_schema_as_str_modulo_the_trailing_newline() -> None:
    """The one normalisation :func:`render_sdl` applies hides nothing.

    ``render_sdl`` pins the file to exactly one trailing newline (the POSIX convention, and the
    thing editors and ``git`` quietly disagree about). This asserts the *content* against
    ``schema.as_str()`` itself, so the normalisation cannot be where a real difference is being
    swallowed — if these two ever diverged, the test above would be comparing the committed file
    against something other than the published schema.
    """
    committed = SCHEMA_PATH.read_text(encoding="utf-8")

    assert committed.rstrip("\n") == schema.as_str().rstrip("\n")
    assert committed.endswith("\n")
    assert not committed.endswith("\n\n")


def test_the_exporter_writes_exactly_what_is_committed(tmp_path: Path) -> None:
    """Running the exporter for real reproduces the committed file byte for byte.

    Goes through :func:`~scripts.export_sdl.export` rather than comparing strings, so the *writing*
    path is covered too — explicit UTF-8 and explicit ``\\n`` newlines. A default-encoding or
    newline-translating write would produce a file that differs from the one rendered in memory,
    and the drift test above would then fail for reasons that have nothing to do with the schema.
    """
    written = export(tmp_path / "schema.graphql")

    assert written.read_bytes() == SCHEMA_PATH.read_bytes()


def test_the_sdl_carries_the_field_the_spec_acceptance_command_depends_on() -> None:
    """``logs`` is a bare list in the *published document*, not only in the Python.

    The drift test above is an equality against a file, so it passes just as happily against a file
    that was regenerated from a broken schema. This one states the invariant that must survive any
    regeneration: the spec's §5 command ``{ logs { id service level message } }`` only validates
    when ``logs`` returns ``[LogEntry!]!``, so a connection type appearing here would break the
    acceptance criteria no matter how faithfully the SDL was exported.
    """
    sdl = SCHEMA_PATH.read_text(encoding="utf-8")

    assert "logs(filters: LogFilterInput" in sdl
    assert "): [LogEntry!]!" in sdl
    assert "logsConnection(" in sdl, "the cursor-pagination bonus lives on its own field"
