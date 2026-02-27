"""Tests for the rule engine."""

from src.engine.rules import Rule, RuleEngine


# ---------------------------------------------------------------------------
# TestRule
# ---------------------------------------------------------------------------


class TestRule:
    """Unit tests for the Rule dataclass."""

    def test_match_all_matches_any_message(self) -> None:
        rule = Rule(name="catch_all", keywords=[], match_all=True)
        assert rule.matches("anything at all") is True
        assert rule.matches("") is True

    def test_keyword_match_case_insensitive(self) -> None:
        rule = Rule(name="errors", keywords=["error", "fatal"], match_all=False)
        assert rule.matches("An ERROR occurred") is True
        assert rule.matches("FATAL crash") is True
        assert rule.matches("error in module") is True

    def test_keyword_no_match(self) -> None:
        rule = Rule(name="errors", keywords=["error", "fatal"], match_all=False)
        assert rule.matches("All systems nominal") is False
        assert rule.matches("INFO startup complete") is False

    def test_empty_keywords_match_all_false_matches_nothing(self) -> None:
        rule = Rule(name="empty", keywords=[], match_all=False)
        assert rule.matches("anything") is False
        assert rule.matches("") is False


# ---------------------------------------------------------------------------
# TestRuleEngine
# ---------------------------------------------------------------------------


class TestRuleEngine:
    """Unit tests for the RuleEngine."""

    def _default_engine(self) -> RuleEngine:
        return RuleEngine.default()

    def test_evaluate_error_message(self) -> None:
        engine = self._default_engine()
        result = engine.evaluate("ERROR: something went wrong")
        assert result == ["system_info", "environment", "performance"]

    def test_evaluate_info_message(self) -> None:
        engine = self._default_engine()
        result = engine.evaluate("INFO: startup complete")
        assert result == ["system_info", "environment"]

    def test_evaluate_warning_message(self) -> None:
        engine = self._default_engine()
        result = engine.evaluate("WARNING: disk space low")
        assert result == ["system_info", "environment", "performance"]

    def test_evaluate_preserves_order_and_deduplicates(self) -> None:
        """Both critical_errors and default share system_info & environment;
        the result should list each collector exactly once in first-seen order."""
        engine = self._default_engine()
        result = engine.evaluate("fatal error detected")
        assert result == ["system_info", "environment", "performance"]
        # Verify no duplicates
        assert len(result) == len(set(result))

    def test_from_yaml_valid_data(self) -> None:
        data = {
            "rules": [
                {
                    "name": "custom",
                    "keywords": ["oops"],
                    "match_all": False,
                    "collectors": ["collector_a"],
                },
                {
                    "name": "fallback",
                    "keywords": [],
                    "match_all": True,
                    "collectors": ["collector_b"],
                },
            ]
        }
        engine = RuleEngine.from_yaml(data)
        assert len(engine._rules) == 2
        assert engine._rules[0].name == "custom"
        assert engine._rules[1].name == "fallback"

    def test_from_yaml_empty_dict_falls_back_to_default(self) -> None:
        engine = RuleEngine.from_yaml({})
        assert len(engine._rules) == 3
        assert engine._rules[0].name == "critical_errors"

    def test_default_returns_three_rules(self) -> None:
        engine = RuleEngine.default()
        assert len(engine._rules) == 3
        names = [r.name for r in engine._rules]
        assert names == ["critical_errors", "warnings", "default"]
