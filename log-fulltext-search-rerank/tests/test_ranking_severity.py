"""Unit tests for :class:`~src.ranking.severity.SeverityScorer`,
:class:`~src.ranking.service_authority.ServiceAuthorityScorer`,
and the helpers in :mod:`src.ranking.context`.

Grouped together because they are all small lookup-table primitives
with the same testing shape — fewer test files, same coverage.
"""

from __future__ import annotations

from src.config import Settings, get_settings
from src.ranking.context import context_bonus, effective_weights
from src.ranking.service_authority import ServiceAuthorityScorer
from src.ranking.severity import SeverityScorer


# ---------------------------------------------------------------------------
# SeverityScorer
# ---------------------------------------------------------------------------

def test_default_severity_weights_are_plan_defaults() -> None:
    """Sanity-check the spec values from plan.md §2 Commit 02."""
    scorer = SeverityScorer(get_settings())
    assert scorer.score("ERROR") == 1.0
    assert scorer.score("WARN") == 0.7
    assert scorer.score("INFO") == 0.4
    assert scorer.score("DEBUG") == 0.2
    assert scorer.score("FATAL") == 1.0


def test_severity_case_insensitive() -> None:
    """Lowercased input must match the uppercased table."""
    scorer = SeverityScorer(get_settings())
    assert scorer.score("error") == scorer.score("ERROR")


def test_severity_warning_normalizes_to_warn() -> None:
    """``WARNING`` is a common alternate spelling — normalize it."""
    scorer = SeverityScorer(get_settings())
    assert scorer.score("WARNING") == scorer.score("WARN")
    assert scorer.score("warning") == scorer.score("WARN")


def test_severity_unknown_level_returns_zero() -> None:
    """Missing config keys are neutral rather than silently ERROR-weighted."""
    scorer = SeverityScorer(get_settings())
    assert scorer.score("WEIRD_LEVEL") == 0.0


def test_severity_empty_level_returns_zero() -> None:
    """Empty or missing level must not inherit a positive contribution."""
    scorer = SeverityScorer(get_settings())
    assert scorer.score("") == 0.0


# ---------------------------------------------------------------------------
# ServiceAuthorityScorer
# ---------------------------------------------------------------------------

def test_service_known_service_returns_table_value() -> None:
    scorer = ServiceAuthorityScorer(get_settings())
    assert scorer.score("payment") == 1.0
    assert scorer.score("auth") == 0.9


def test_service_unknown_service_returns_fallback() -> None:
    """An unrecognized service resolves to the ``unknown`` weight (0.5)."""
    scorer = ServiceAuthorityScorer(get_settings())
    assert scorer.score("random-microservice") == 0.5


def test_service_empty_string_returns_fallback() -> None:
    scorer = ServiceAuthorityScorer(get_settings())
    assert scorer.score("") == 0.5


def test_service_case_insensitive() -> None:
    scorer = ServiceAuthorityScorer(get_settings())
    assert scorer.score("PAYMENT") == scorer.score("payment")


def test_service_custom_fallback_respected() -> None:
    """If settings override ``unknown``, the fallback follows suit."""
    s = Settings(
        service_authority_weights={"payment": 1.0, "unknown": 0.1}
    )
    scorer = ServiceAuthorityScorer(s)
    assert scorer.score("mystery") == 0.1


# ---------------------------------------------------------------------------
# context.effective_weights / context.context_bonus
# ---------------------------------------------------------------------------

def test_effective_weights_incident_overrides_defaults() -> None:
    settings = get_settings()
    ew = effective_weights("incident", settings)
    # Incident table raises severity, drops tfidf — straight from config.
    assert ew["tfidf"] == settings.incident_ranking_weights["tfidf"]
    assert ew["severity"] == settings.incident_ranking_weights["severity"]
    assert ew["half_life_s"] == settings.temporal_half_life_incident_s
    assert ew["mode"] == "incident"


def test_effective_weights_none_uses_defaults() -> None:
    settings = get_settings()
    ew = effective_weights(None, settings)
    assert ew["tfidf"] == settings.ranking_weights["tfidf"]
    assert ew["half_life_s"] == settings.temporal_half_life_normal_s
    assert ew["mode"] is None


def test_effective_weights_unknown_mode_falls_back_to_defaults() -> None:
    settings = get_settings()
    ew = effective_weights("analysis", settings)
    assert ew["tfidf"] == settings.ranking_weights["tfidf"]
    assert ew["half_life_s"] == settings.temporal_half_life_normal_s
    # Mode echoed back so callers can still tag the response.
    assert ew["mode"] == "analysis"


def test_context_bonus_incident_on_error() -> None:
    assert context_bonus("incident", "ERROR") == 1.0
    assert context_bonus("incident", "FATAL") == 1.0


def test_context_bonus_incident_on_info_is_zero() -> None:
    assert context_bonus("incident", "INFO") == 0.0
    assert context_bonus("incident", "DEBUG") == 0.0


def test_context_bonus_no_mode_is_zero() -> None:
    assert context_bonus(None, "ERROR") == 0.0
    assert context_bonus("", "ERROR") == 0.0


def test_context_bonus_unknown_mode_is_zero() -> None:
    """Unknown modes shouldn't silently boost anything."""
    assert context_bonus("analysis", "ERROR") == 0.0
