"""Unit tests for ``src.policy.loader.load_policy_set``.

Covers the three failure modes that produce ``PolicyLoadError`` (empty
file, malformed YAML, schema violation) plus the happy path that
round-trips ``config/retention_config.yaml`` into a ``PolicySet`` with
the expected policy count and names. A separate test confirms that
compliance violations propagate as ``ComplianceValidationError``
(not as ``PolicyLoadError``) so callers can distinguish "the file is
broken" from "the rules don't pass audit".
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src.compliance.validator import ComplianceValidationError
from src.policy.loader import PolicyLoadError, load_policy_set
from src.policy.schema import PolicySet

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_YAML = REPO_ROOT / "config" / "retention_config.yaml"


def test_load_valid_config_yaml_round_trips() -> None:
    """The demo config file parses, validates, and exposes 6 policies."""
    result = load_policy_set(CONFIG_YAML)

    assert isinstance(result, PolicySet)
    assert len(result.policies) == 6

    names = [p.name for p in result.policies]
    assert names == [
        "user_activity_gdpr",
        "payment_logs_sox",
        "health_records_hipaa",
        "card_data_pci",
        "ops_audit_soc2",
        "debug_logs",
    ]


def test_load_empty_file_raises(tmp_path: Path) -> None:
    """An empty YAML file does not parse to a mapping → PolicyLoadError."""
    empty = tmp_path / "empty.yaml"
    empty.write_text("", encoding="utf-8")

    with pytest.raises(PolicyLoadError, match="did not parse to a mapping"):
        load_policy_set(empty)


def test_load_malformed_yaml_raises(tmp_path: Path) -> None:
    """Syntactically invalid YAML raises PolicyLoadError (not yaml.YAMLError)."""
    bad = tmp_path / "bad.yaml"
    bad.write_text("policies: [unclosed", encoding="utf-8")

    with pytest.raises(PolicyLoadError, match="failed to parse"):
        load_policy_set(bad)


def test_load_invalid_schema_raises_PolicyLoadError(tmp_path: Path) -> None:
    """Pydantic ValidationError is re-raised as PolicyLoadError."""
    schema_bad = tmp_path / "schema_bad.yaml"
    schema_bad.write_text(
        "policies:\n"
        "  - name: x\n",  # missing selector + phases
        encoding="utf-8",
    )

    with pytest.raises(PolicyLoadError, match="policy schema invalid"):
        load_policy_set(schema_bad)


def test_load_invalid_compliance_raises_ComplianceValidationError(
    tmp_path: Path,
) -> None:
    """A schema-valid but compliance-bad policy raises the typed exception."""
    compliance_bad = tmp_path / "compliance_bad.yaml"
    compliance_bad.write_text(
        "policies:\n"
        "  - name: bad\n"
        "    selector: {}\n"
        "    compliance_tag: sox\n"
        "    immutable: false\n"  # SOX requires immutable=True
        "    phases:\n"
        "      - { after_days: 0,  action: promote, target_tier: hot }\n"
        "      - { after_days: 30, action: delete }\n",  # SOX min is 2555 d
        encoding="utf-8",
    )

    with pytest.raises(ComplianceValidationError) as exc_info:
        load_policy_set(compliance_bad)

    # Both violations should be present: SOX delete < 2555 d AND not immutable.
    assert len(exc_info.value.violations) == 2
    joined = "; ".join(exc_info.value.violations)
    assert "SOX" in joined
    assert "2555" in joined
    assert "immutable" in joined


def test_load_accepts_string_path(tmp_path: Path) -> None:
    """The loader accepts ``str`` as well as ``Path``."""
    f = tmp_path / "empty_policies.yaml"
    f.write_text("policies: []\n", encoding="utf-8")

    result = load_policy_set(str(f))

    assert isinstance(result, PolicySet)
    assert result.policies == []
