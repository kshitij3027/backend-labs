"""JSON-on-disk loaders for :class:`RedactionConfig`.

Two entry points
----------------
* :func:`load_config_file` — load and validate a single JSON file by path.
* :func:`load_preset`      — resolve a preset name to a path under
  ``config/`` (``default`` lives at the root, the rest under
  ``presets/<name>.json``) and delegate to :func:`load_config_file`.

Error contract
--------------
We deliberately do NOT catch :class:`pydantic.ValidationError` here. The
caller (either :func:`ConfigurationManager.reload_from_json` at runtime,
or the application bootstrap at startup) decides how to react: bootstrap
should bubble the error up to the operator with a non-zero exit code,
whereas the hot-reload path should leave the previous valid config in
place and surface the error to the API caller as a 400.

For missing files we raise :class:`FileNotFoundError` with the resolved
path embedded in the message — much easier to triage than a generic
``[Errno 2]`` because the operator can paste the path straight into a
``ls`` to confirm typos.
"""
from __future__ import annotations

import logging
from pathlib import Path

from .models import RedactionConfig

logger = logging.getLogger(__name__)


def load_config_file(path: Path) -> RedactionConfig:
    """Load and validate a redaction config from a JSON file on disk.

    The file is read as UTF-8 and handed verbatim to
    :meth:`RedactionConfig.model_validate_json`, which performs schema
    validation in one pass (including the nested ``PatternRule`` dict).

    Raises
    ------
    FileNotFoundError
        If ``path`` does not exist. Python's ``open()`` already raises
        this; we let it propagate verbatim.
    pydantic.ValidationError
        If the JSON parses but doesn't satisfy the :class:`RedactionConfig`
        schema (missing rules, unknown pattern_name, etc.).
    json.JSONDecodeError
        If the file is not valid JSON. ``model_validate_json`` raises
        a :class:`pydantic.ValidationError` for malformed JSON in pydantic
        v2, so this branch is rare in practice but documented for clarity.
    """
    # ``read_text`` is preferred over ``open(...).read()`` because it
    # handles file-close on exceptions and explicitly forces UTF-8 (the
    # platform default on Linux/macOS but not necessarily on Windows; the
    # explicit encoding makes the behavior portable).
    content = path.read_text(encoding="utf-8")
    # ``model_validate_json`` parses + validates in a single C-level call,
    # which is meaningfully faster than ``json.loads`` followed by
    # ``model_validate`` for documents of this size.
    return RedactionConfig.model_validate_json(content)


def load_preset(name: str, config_dir: Path) -> RedactionConfig:
    """Load a named preset from the ``config/`` directory.

    Resolution rules
    ----------------
    * ``name == "default"`` → ``config_dir / "default.json"``.
    * any other name      → ``config_dir / "presets" / f"{name}.json"``.

    The split exists because ``default.json`` is the *baseline* shipped
    with every deployment and is treated specially in the bootstrap
    (e.g., audit setup wires off its rules), while ``presets/*`` are
    operator-pickable variants for the three documented compliance
    regimes (healthcare / financial / general).

    Raises
    ------
    FileNotFoundError
        If the resolved path does not exist on disk. The exception
        message names the preset and the absolute path so operators
        can spot typos without digging through the source.
    pydantic.ValidationError
        Forwarded verbatim from :func:`load_config_file`.
    """
    # Two-armed dispatch instead of a single ``Path`` join so the
    # ``default`` case stays readable and the error message below can
    # name the exact path we tried.
    if name == "default":
        path = config_dir / "default.json"
    else:
        path = config_dir / "presets" / f"{name}.json"

    if not path.exists():
        # Repr-format the name so ``""`` and ``"unknown name"`` both
        # render unambiguously. Including ``path`` here gives the
        # operator a one-line copy-paste target for ``ls``.
        raise FileNotFoundError(f"preset {name!r} not found at {path}")

    logger.info("loading config preset %r from %s", name, path)
    return load_config_file(path)
