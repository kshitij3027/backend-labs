"""Pydantic v2 models for retention policies.

This module defines the static shape of a retention policy. No I/O —
parsing YAML lives in ``loader.py`` (C04); selecting which policy wins
for a given file lives in ``matcher.py`` (C05). The models here are
pure data containers plus two helpers on ``Selector``: ``matches`` and
``specificity``, both used by the matcher.

All four models are ``frozen=True`` and ``extra='forbid'`` so policies
load deterministically — a typo in the YAML fails loudly at startup
instead of silently becoming an unused field, and downstream code may
safely cache policy instances knowing they will never mutate.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Selector(BaseModel):
    """Picks which files a policy applies to.

    Each field is optional and may be either a single string (exact
    match) or a list of strings (OR match). A field left as ``None``
    means "do not constrain on this attribute" — an all-``None``
    selector is the wildcard that matches every file (used by debug
    policies).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    source: str | list[str] | None = None
    level: str | list[str] | None = None
    category: str | list[str] | None = None

    def matches(self, file: Any) -> bool:
        """True iff every non-None selector field matches ``file``.

        ``file`` may be any object with ``source``/``level``/``category``
        attributes (an ORM ``File``, a dataclass, a ``SimpleNamespace``,
        ...). Missing attributes (``None``) never satisfy a non-None
        selector — be explicit.
        """
        for field_name in ("source", "level", "category"):
            selector_value = getattr(self, field_name)
            if selector_value is None:
                continue
            file_value = getattr(file, field_name, None)
            if file_value is None:
                return False
            if isinstance(selector_value, list):
                if file_value not in selector_value:
                    return False
            else:
                if file_value != selector_value:
                    return False
        return True

    def specificity(self) -> int:
        """Count of non-``None`` selector fields. Higher = more specific."""
        return sum(
            1
            for field_name in ("source", "level", "category")
            if getattr(self, field_name) is not None
        )


class Phase(BaseModel):
    """One step in a policy's lifecycle.

    A phase fires when a file's age (computed from ``oldest_record_ts``)
    reaches ``after_days`` and the previous phase has already been
    applied. ``action`` decides what the applier does; ``target_tier``
    and ``compression_level`` are optional refinements (delete phases
    need neither; archive phases use both).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    after_days: int = Field(ge=0)
    action: Literal["promote", "compress", "archive", "delete"]
    target_tier: Literal["hot", "warm", "cold", "archive"] | None = None
    compression_level: int | None = None


class Policy(BaseModel):
    """A complete retention policy: who it applies to + what happens when.

    ``phases`` must be strictly ascending in ``after_days`` — duplicates
    are rejected because two phases firing on the same day would be
    ambiguous to the matcher. ``compliance_tag`` is consulted by the
    C04 validator to enforce per-framework minimum retention windows.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    selector: Selector
    priority: int = 0
    compliance_tag: Literal["gdpr", "sox", "hipaa", "pci_dss", "soc2"] | None = None
    immutable: bool = False
    phases: list[Phase] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_phase_ordering(self) -> "Policy":
        """Ensure phases are strictly ascending by ``after_days``."""
        for i in range(1, len(self.phases)):
            prev = self.phases[i - 1].after_days
            curr = self.phases[i].after_days
            if curr <= prev:
                raise ValueError(
                    f"Policy '{self.name}' phases must be strictly ascending by "
                    f"after_days; got {prev} then {curr} at index {i}"
                )
        return self


class PolicySet(BaseModel):
    """A collection of policies, typically loaded from one YAML file.

    An empty list is allowed at startup — the loader's validator will
    warn but not crash, so the service can boot with no policies and
    have them added later via config reload.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    policies: list[Policy] = Field(default_factory=list)
