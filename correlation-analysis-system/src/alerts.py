"""Real-time alerting: turns a detection cycle's correlations into operator alerts.

The :class:`AlertManager` runs a small ordered rule set over every freshly
detected correlation — the FIRST matching rule wins per correlation:

    1. ``cascade_critical``  — an ``error_cascade`` at strength >=
       :data:`CASCADE_STRENGTH_THRESHOLD` pages as **critical** (a propagating
       failure is urgent regardless of statistical confidence).
    2. ``strong_correlation`` — ANY type at strength >=
       ``settings.alert_strength_threshold`` AND confidence >=
       ``settings.alert_confidence_threshold`` raises a **warning**.

A per-(rule, type, source-pair) cooldown (``settings.alert_cooldown_seconds``)
keeps a persistent condition to one alert per minute instead of one per
detection cycle. Fired alerts land in a bounded in-memory deque (the C7
dashboard's feed) and are handed back to the caller — the engine mirrors them
to Redis via :meth:`src.store.RedisStore.push_alerts`.
"""

from __future__ import annotations

from collections import deque

from src.config import Settings
from src.engine.base import DedupeCache, new_correlation_id, pair_key
from src.models import Alert, Correlation, CorrelationType

#: Cascade correlations at or above this strength page as critical — set below
#: the generic threshold on purpose: a moderately scored cascade still means a
#: failure is actively propagating across services.
CASCADE_STRENGTH_THRESHOLD = 0.6

#: Bounded in-memory alert history (mirrors the ``corr:alerts:recent`` cap).
RECENT_MAX = 200


class AlertManager:
    """Ordered alert rules + cooldowns over each cycle's fresh correlations."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        #: Every fired alert, oldest -> newest, bounded like the Redis mirror.
        self.alerts: deque[Alert] = deque(maxlen=RECENT_MAX)
        self._cooldowns = DedupeCache()

    def evaluate(self, corrs: list[Correlation], now: float) -> list[Alert]:
        """Run the rules over ``corrs``; record and return the alerts that fired.

        Per correlation the first matching rule wins; the winning (rule, type,
        source-pair) key is then cooldown-gated so the same ongoing condition
        alerts at most once per ``alert_cooldown_seconds``.
        """
        fired: list[Alert] = []
        ttl = float(self.settings.alert_cooldown_seconds)
        for corr in corrs:
            match = self._first_match(corr)
            if match is None:
                continue
            rule_name, severity, title, message = match
            key = pair_key(
                rule_name,
                corr.correlation_type.value,
                corr.event_a.source.value,
                corr.event_b.source.value,
            )
            if not self._cooldowns.seen(key, now, ttl):
                continue  # same condition alerted within the cooldown window
            alert = Alert(
                id=new_correlation_id(),
                created_at=now,
                severity=severity,
                title=title,
                message=message,
                correlation_type=corr.correlation_type,
                strength=corr.strength,
                confidence=corr.confidence,
            )
            self.alerts.append(alert)
            fired.append(alert)
        return fired

    def recent(self, limit: int = 20) -> list[Alert]:
        """The newest fired alerts, newest first (at most ``limit``)."""
        if limit <= 0:
            return []
        newest = list(self.alerts)[-limit:]
        newest.reverse()
        return newest

    # --- Rules (ordered; first match per correlation wins) ---------------------
    def _first_match(self, corr: Correlation) -> tuple[str, str, str, str] | None:
        """The first rule ``corr`` trips: (rule_name, severity, title, message)."""
        if (
            corr.correlation_type is CorrelationType.CASCADE
            and corr.strength >= CASCADE_STRENGTH_THRESHOLD
        ):
            # Prefer the detector's chain details; fall back to what the two
            # event refs alone can tell us (fabricated/minimal correlations).
            services = corr.details.get(
                "distinct_services", len({corr.event_a.source, corr.event_b.source})
            )
            span = corr.details.get(
                "span_seconds",
                round(corr.event_b.timestamp - corr.event_a.timestamp, 3),
            )
            return (
                "cascade_critical",
                "critical",
                "Error cascade detected",
                f"{corr.event_a.source.value}→{corr.event_b.source.value} cascade "
                f"({services} services, span {span}s)",
            )
        if (
            corr.strength >= self.settings.alert_strength_threshold
            and corr.confidence >= self.settings.alert_confidence_threshold
        ):
            return (
                "strong_correlation",
                "warning",
                "Strong correlation detected",
                f"{corr.correlation_type.value} correlation between "
                f"{corr.event_a.source.value} and {corr.event_b.source.value} "
                f"(strength {corr.strength:.2f}, confidence {corr.confidence:.2f})",
            )
        # C7 hook: the anomaly rule ("pattern deviates >2σ from its learned
        # baseline" -> info/warning) slots in here once PatternLearner lands.
        return None
