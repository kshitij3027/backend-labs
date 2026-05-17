"""Top-level detection orchestrator.

The :class:`Detector` walks a JSON-decoded log dict recursively and emits a
:class:`Detection` for every sensitive leaf, combining:

1. **Field-name match first** — if a key contains a known sensitive
   substring, that wins (confidence 0.95). The value's content is NOT
   examined further; this is intentional, see :mod:`field_names`.
2. **Value-regex fallback** — for keys that don't trigger a field-name
   hit, every compiled regex is tried; the highest-confidence hit wins.

Lists / tuples are treated as opaque scalars in v1 — we don't recurse into
them. This keeps the v1 algorithm simple and avoids accidentally encrypting
e.g. an array of order line items. List-aware detection can be added in a
later commit if needed.
"""
from __future__ import annotations

import logging
from typing import Any

from .field_names import FieldNameMatcher, default_field_names_path
from .patterns import Detection, PatternMatcher, default_pattern_path

logger = logging.getLogger(__name__)


# Truncate previews so the audit trail never carries a recoverable plaintext.
_PREVIEW_LEN: int = 8


class Detector:
    """Walks a log dict and emits all detections.

    Both matchers are constructed eagerly on first use unless explicit
    instances are provided. This keeps test injection clean while keeping
    production startup zero-config.

    Parameters
    ----------
    patterns : PatternMatcher | None
        Regex matcher; defaults to one loaded from ``config/patterns.yaml``.
    names : FieldNameMatcher | None
        Field-name matcher; defaults to one loaded from
        ``config/field_names.yaml``.
    """

    def __init__(
        self,
        patterns: PatternMatcher | None = None,
        names: FieldNameMatcher | None = None,
    ) -> None:
        self._patterns = patterns if patterns is not None else PatternMatcher(
            default_pattern_path()
        )
        self._names = names if names is not None else FieldNameMatcher(
            default_field_names_path()
        )

    # -- public ----------------------------------------------------------

    def detect(self, log: dict, *, parent_path: str = "") -> list[Detection]:
        """Walk ``log`` and return every detection, sorted by descending confidence.

        Sort order is ``(-confidence, field_path)`` so that ties are
        deterministic and the highest-confidence finding is always first
        (callers like :mod:`processor.log_processor` rely on this).
        """
        raw: list[Detection] = []
        self._walk(log, parent_path=parent_path, sink=raw)
        # Sort: highest confidence first, alphabetical path on ties.
        raw.sort(key=lambda d: (-d.confidence, d.field_path))
        return raw

    # -- internal --------------------------------------------------------

    def _walk(
        self,
        node: Any,
        *,
        parent_path: str,
        sink: list[Detection],
    ) -> None:
        """Recursive helper. Mutates ``sink`` in-place to avoid list concat."""
        if not isinstance(node, dict):
            return  # We only recurse into dicts; nothing else is structured.

        for key, value in node.items():
            path = f"{parent_path}.{key}" if parent_path else key

            if isinstance(value, dict):
                # Recurse: nested objects may contain PII at any depth.
                self._walk(value, parent_path=path, sink=sink)
                continue

            if isinstance(value, (list, tuple)):
                # v1: lists are opaque. See module docstring.
                continue

            # Leaf scalar (str / int / float / bool / None / etc.)
            detection = self._classify_leaf(key=key, value=value, path=path)
            if detection is not None:
                sink.append(detection)

    def _classify_leaf(
        self,
        *,
        key: str,
        value: Any,
        path: str,
    ) -> Detection | None:
        """Apply the field-name-then-regex rule to one leaf."""
        # The audit preview is computed once: 8 chars of the *string* form.
        str_value = "" if value is None else str(value)
        preview = str_value[:_PREVIEW_LEN]

        # 1) Field-name match wins outright when it fires.
        name_hit = self._names.match(key)
        if name_hit is not None:
            return Detection(
                field_path=path,
                field_type=name_hit.field_type,
                confidence=name_hit.confidence,
                reason=name_hit.reason,
                value_preview=preview,
            )

        # 2) Otherwise, run every regex; keep the highest-confidence hit.
        if not str_value:
            return None  # Empty value — nothing to scan.

        regex_hits = self._patterns.match(str_value)
        if not regex_hits:
            return None

        best = max(regex_hits, key=lambda d: d.confidence)
        return Detection(
            field_path=path,
            field_type=best.field_type,
            confidence=best.confidence,
            reason=best.reason,
            value_preview=preview,
        )
