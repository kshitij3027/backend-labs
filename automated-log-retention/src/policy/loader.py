"""YAML → ``PolicySet`` loader with compliance validation.

Single public entry point: ``load_policy_set(path)``. The loader
performs three layered checks, each producing a distinct exception
type so callers can react appropriately:

  1. **YAML parse / shape.** Empty file or non-mapping top level →
     ``PolicyLoadError``. The service simply will not boot with a
     malformed config file.
  2. **Pydantic schema.** Wrong field types or missing required keys
     → ``PolicyLoadError`` wrapping the original ``ValidationError``
     so the operator sees the full failure path.
  3. **Compliance rules.** Tagged policies that violate per-framework
     minimums or immutability requirements → ``ComplianceValidationError``
     propagates **unwrapped**. Callers want the typed exception with
     its ``violations`` list, not a generic load-error string.

Called once at lifespan startup in ``main.py`` (wired in C12); any
exception here is fatal — better to crash on boot than to run with
non-compliant retention rules.
"""
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from src.compliance.validator import validate_policy_set
from src.policy.schema import PolicySet


class PolicyLoadError(ValueError):
    """Raised for YAML parse failures or Pydantic schema violations.

    Distinct from ``ComplianceValidationError`` so callers can decide
    whether the problem is "the file is broken" (this) or "the file is
    fine but its rules don't meet the framework" (that).
    """


def load_policy_set(path: Path | str) -> PolicySet:
    """Parse, validate, and return the ``PolicySet`` at ``path``.

    Raises:
        PolicyLoadError: file is empty, isn't valid YAML, isn't a
            mapping at the top level, or fails Pydantic schema.
        ComplianceValidationError: at least one tagged policy violates
            its framework's retention or immutability rules.
    """
    raw = Path(path).read_text(encoding="utf-8")

    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise PolicyLoadError(f"policy file {path} failed to parse: {e}") from e

    if data is None or not isinstance(data, dict):
        raise PolicyLoadError(
            f"policy file {path} did not parse to a mapping"
        )

    try:
        policy_set = PolicySet.model_validate(data)
    except ValidationError as e:
        raise PolicyLoadError(f"policy schema invalid: {e}") from e

    # Let ComplianceValidationError propagate untouched — callers want
    # the typed exception, not a generic load error.
    validate_policy_set(policy_set)

    return policy_set
