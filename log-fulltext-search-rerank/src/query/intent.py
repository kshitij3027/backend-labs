"""Regex-based intent detection for incoming search queries.

Five buckets, matching ``project_requirements.md`` §2: ``troubleshooting``,
``performance_analysis``, ``user_activity``, ``payment_analysis``, and
the fallback ``general_search``. Patterns compile once at module import
so the hot path is a bounded sequence of ``re.search`` calls — O(n_intents)
per query, which at five intents is effectively constant time.

Priority ordering is deliberate and must be preserved: payment,
user_activity, and performance buckets are narrower than
troubleshooting (which alone swallows words like ``error`` and ``fail``
that also show up in payment/user contexts), so the narrow buckets
come first. Any query that matches nothing falls through to
``general_search``.
"""

from __future__ import annotations

import re
from typing import Iterable


# First pattern that matches wins. Ordering is deliberate: payment/
# user/performance are narrower buckets than troubleshooting, so they
# come first. ``general_search`` is the fallback.
_INTENT_PATTERNS: list[tuple[str, re.Pattern]] = [
    (
        "payment_analysis",
        re.compile(
            r"\b(payment|charge|refund|invoice|billing|transaction|checkout|stripe|paypal|card)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "user_activity",
        re.compile(
            r"\b(user|login|logout|signup|signin|session|authentication|auth|password|profile|account)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "performance_analysis",
        re.compile(
            r"\b(slow|latency|timeout|performance|throughput|p9[59]|p99|rps|qps|bottleneck|memory|cpu|load)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "troubleshooting",
        re.compile(
            r"\b(error|exception|fail|failure|crash|panic|stacktrace|traceback|unhandled|refused|unavailable|5\d{2})\b",
            re.IGNORECASE,
        ),
    ),
]


class IntentDetector:
    """First-match-wins intent classifier over the raw query text.

    Patterns are compiled once at module load. Detection is
    ``O(n_intents)`` per query — effectively constant for five intents,
    which is why we can afford to re-run it on every request instead of
    caching per-query results.

    The constructor accepts a custom pattern list so tests (and future
    config-driven overrides loaded from ``settings.intent_patterns_path``)
    can inject their own buckets without touching the module-level
    default.
    """

    def __init__(
        self,
        patterns: Iterable[tuple[str, re.Pattern]] | None = None,
    ) -> None:
        # Materialize the iterable eagerly — a generator would be
        # single-shot and silently break on the second ``detect`` call.
        self._patterns = list(patterns) if patterns is not None else _INTENT_PATTERNS

    def detect(self, text: str) -> str:
        """Return the first matching intent label or ``general_search``.

        Matching is done against the raw query text so lemma-stripped
        forms never hide the intent keyword (e.g. ``"authentications"``
        still fires the ``user_activity`` bucket because the root word
        is present in the substring match).
        """
        for label, pattern in self._patterns:
            if pattern.search(text):
                return label
        return "general_search"


__all__ = ("IntentDetector",)
