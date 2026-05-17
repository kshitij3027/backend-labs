"""PII detection layer.

Public surface:

* :class:`Detection`       — frozen dataclass for one finding
* :class:`PatternMatcher`  — scans values against compiled regexes (+ Luhn)
* :class:`FieldNameMatcher` — substring lookup on field names
* :class:`Detector`        — orchestrator: walks a dict, combines matchers

Detection is intentionally **pure** — no global state, no I/O, no logging
beyond ``logger.debug``. Side effects (audit, stats, encryption) live in
later layers (C5+).
"""
from __future__ import annotations

from .detector import Detector
from .field_names import FieldNameMatcher, default_field_names_path
from .patterns import Detection, PatternMatcher, default_pattern_path

__all__ = [
    "Detection",
    "Detector",
    "FieldNameMatcher",
    "PatternMatcher",
    "default_field_names_path",
    "default_pattern_path",
]
