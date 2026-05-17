"""Field-name substring matching for the PII detection layer.

Many sensitive fields can be identified by their KEY alone — e.g. a column
called ``password`` is sensitive even when the value is innocuous. This
matcher does a case-insensitive substring containment check against a
configured list (``config/field_names.yaml``).

A field-name hit has a high fixed confidence (``0.95``) and is intended to
OUTRANK any value-regex hit. The orchestrator (``Detector``) therefore tries
this matcher first and skips value regex if it fires.
"""
from __future__ import annotations

import logging
from pathlib import Path

import yaml

from .patterns import Detection

logger = logging.getLogger(__name__)


class FieldNameMatcher:
    """Returns a :class:`Detection` when a field name contains a sensitive substring.

    Parameters
    ----------
    config_path : Path | str
        YAML file with one top-level key ``sensitive_names`` mapping to a
        list of substrings. Substrings are lowercased at load time so the
        hot path only does a single case-fold on the candidate name.
    """

    # Field-name hits get a uniform high confidence. See module docstring.
    _CONFIDENCE: float = 0.95

    def __init__(self, config_path: Path | str) -> None:
        self._config_path = Path(config_path)
        self._substrings: list[str] = self._load(self._config_path)
        logger.debug(
            "FieldNameMatcher loaded %d sensitive name substrings from %s",
            len(self._substrings),
            self._config_path,
        )

    # -- public ----------------------------------------------------------

    def match(self, field_name: str) -> Detection | None:
        """Return a :class:`Detection` on the FIRST substring hit, else ``None``.

        The order of substrings in the YAML file determines precedence — keep
        the most specific names first (e.g. ``credit_card`` before ``card_no``)
        if you want them to win.
        """
        if not isinstance(field_name, str) or not field_name:
            return None
        lower = field_name.lower()
        for sub in self._substrings:
            if sub in lower:
                return Detection(
                    field_path="",
                    field_type=sub,
                    confidence=self._CONFIDENCE,
                    reason=f"field_name:{sub}",
                    value_preview="",
                )
        return None

    # -- internal --------------------------------------------------------

    @staticmethod
    def _load(path: Path) -> list[str]:
        with path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        if not isinstance(raw, dict) or "sensitive_names" not in raw:
            raise ValueError(
                f"Field-names config at {path} must be a mapping with a "
                "'sensitive_names' key"
            )
        names = raw["sensitive_names"]
        if not isinstance(names, list):
            raise ValueError(
                f"'sensitive_names' in {path} must be a list, got {type(names).__name__}"
            )
        # Pre-lowercase once at construction so `match()` only lowers the
        # candidate (typically <40 chars) on the hot path.
        return [str(s).lower() for s in names]


def default_field_names_path() -> Path:
    """Resolve ``<repo-root>/config/field_names.yaml`` relative to this file."""
    return Path(__file__).resolve().parents[2] / "config" / "field_names.yaml"
