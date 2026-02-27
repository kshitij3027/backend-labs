"""Core enrichment orchestrator that ties collectors, rules, and stats together."""

from __future__ import annotations

import time

from src.collectors import create_default_registry
from src.config import AppConfig, load_config, load_rules
from src.engine.rules import RuleEngine
from src.models import EnrichedLog, EnrichmentRequest, EnrichmentStats
from src.stats import StatsTracker


class LogEnricher:
    """Enriches log messages with metadata based on configurable rules."""

    def __init__(self, config: AppConfig = None) -> None:
        if config is None:
            config = load_config()
        self.config = config
        self.registry = create_default_registry(config)
        try:
            self.rule_engine = RuleEngine.from_yaml(load_rules(config.rules_path))
        except Exception:
            self.rule_engine = RuleEngine.default()
        self.stats = StatsTracker()

    def enrich(self, request: EnrichmentRequest) -> EnrichedLog:
        """Enrich a log message with metadata. Never raises exceptions."""
        start = time.time()
        try:
            collector_names = self.rule_engine.evaluate(request.log_message)
            metadata, errors = self.registry.collect_from(collector_names)

            enriched = EnrichedLog(
                message=request.log_message,
                source=request.source,
                **metadata,
                enrichment_duration_ms=(time.time() - start) * 1000,
                collectors_applied=collector_names,
                enrichment_errors=errors,
            )

            if not errors:
                self.stats.record_success()
            else:
                self.stats.record_error()

            return enriched
        except Exception as e:
            self.stats.record_error()
            return EnrichedLog(
                message=request.log_message,
                source=request.source,
                enrichment_errors=[str(e)],
            )

    def get_stats(self) -> EnrichmentStats:
        """Return a snapshot of current enrichment statistics."""
        return self.stats.snapshot()
