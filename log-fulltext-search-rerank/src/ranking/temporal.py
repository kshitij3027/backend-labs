"""Exponential recency decay for the multi-factor reranker.

A log from 5 minutes ago is almost always more useful than a log
from 5 hours ago; the temporal scorer captures that intuition with a
classic exponential half-life curve. ``half_life_s`` is supplied per
call so the reranker can swap the normal half-life for the shorter
incident-mode half-life without stashing state on the scorer itself.
"""

from __future__ import annotations

import math


class TemporalScorer:
    """Exponential decay: ``score = exp(-ln2 / half_life_s * age_s)``.

    Returns ``1.0`` when ``now == ts`` and halves every ``half_life_s``
    seconds of age. Clamps to ``[0.0, 1.0]`` — future timestamps
    (clock skew) resolve to ``1.0`` rather than blowing past the upper
    bound.
    """

    def score(self, ts: float, now: float, half_life_s: float) -> float:
        """Return the temporal score in ``[0.0, 1.0]``.

        A non-positive ``half_life_s`` degenerates to ``0.0`` — the
        sane fallback for a misconfigured setting, because returning
        ``1.0`` would silently promote every document.
        """
        if half_life_s <= 0:
            return 0.0
        age = max(now - ts, 0.0)
        return math.exp(-math.log(2.0) / half_life_s * age)
