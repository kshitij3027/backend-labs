"""Frozen pydantic models for redaction configuration.

Why these models are frozen
---------------------------
The :class:`ConfigurationManager` (see :mod:`src.config.manager`) implements
hot-reload by atomically rebinding a single ``RedactionConfig`` reference
under an ``RLock``. The atomic-rebind guarantee only buys "no torn reads";
it does NOT buy "no concurrent mutation of the object a reader is holding".

So we freeze the models. A reader thread that called ``manager.get()``
before a reload now holds a snapshot that nothing (not even the manager
itself) can mutate after the fact. Pair that with ``extra="forbid"`` and
the loader will also reject typos in operator-supplied config files at
validation time rather than silently dropping unknown keys.

Type-level enumeration via ``Literal``
--------------------------------------
``pattern_name``, ``strategy``, and ``compliance_tags`` are all closed sets
that the rest of the codebase relies on. We use ``typing.Literal`` rather
than ``str.Enum`` so pydantic emits validation errors that name the
offending value at the JSON schema layer (much cheaper for operators
debugging a config file than a stack trace from deeper code).
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class PatternRule(BaseModel):
    """One pattern → strategy mapping in a :class:`RedactionConfig`.

    Attributes
    ----------
    pattern_name : Literal[...]
        Canonical pattern identifier; must be one of the seven shapes the
        detection layer can emit (``ssn``, ``credit_card``, ``email``,
        ``us_phone``, ``mrn``, ``person``, ``org``).
    strategy : Literal[...]
        Which redaction transform to apply when this pattern fires. Maps
        directly to the four strategies in :mod:`src.redaction.strategies`.
    confidence_min : float
        Minimum confidence a detection must carry before the strategy is
        applied. Regex hits arrive at ``1.0``; NER hits at ``0.85``. The
        default of ``0.9`` therefore keeps every regex hit and lets the
        operator dial NER coverage in/out by raising or lowering the bar.
    compliance_tags : list[Literal["GDPR", "HIPAA", "PCI_DSS"]]
        Which regulatory regimes care about this pattern. The processor
        uses these to filter rules at runtime against
        ``RedactionConfig.active_compliance_sets``.
    """

    pattern_name: Literal["ssn", "credit_card", "email", "us_phone", "mrn", "person", "org"]
    strategy: Literal["mask", "partial", "hash", "tokenize"]
    confidence_min: float = Field(default=0.9, ge=0.0, le=1.0)
    compliance_tags: list[Literal["GDPR", "HIPAA", "PCI_DSS"]] = Field(default_factory=list)

    # ``frozen=True`` makes the model hashable + immutable, so a snapshot
    # handed out by ``ConfigurationManager.get()`` stays valid even after
    # the manager rebinds its internal reference to a new config. ``extra=
    # "forbid"`` makes operator typos a hard error rather than a silent
    # drop — important for a security-sensitive component.
    model_config = ConfigDict(extra="forbid", frozen=True)


class RedactionConfig(BaseModel):
    """The full declarative policy document for one redaction deployment.

    Attributes
    ----------
    version : str
        Schema version string. We bump this when making breaking changes
        to the on-disk JSON layout; consumers can refuse to load unknown
        versions in future commits without needing a new field.
    fields_to_redact : list[str]
        Which top-level fields of an incoming log entry the detection
        layer should scan. Defaults to the canonical trio called out in
        the project requirements (``message``, ``user_data``, ``details``).
    rules : dict[str, PatternRule]
        Map keyed by ``PatternRule.pattern_name`` for O(1) lookup at
        dispatch time. Storing the rules as a dict (vs a list) lets the
        processor avoid a linear scan on the hot path.
    audit_all_redactions : bool
        Whether to emit an audit record for every redaction. Defaults to
        ``True`` because the project's compliance posture requires a full
        trail; deployments that demote audit to "errors only" set this
        to ``False`` via the config file.
    active_compliance_sets : list[Literal[...]]
        Which compliance regimes are "in force" for this deployment.
        Rules whose ``compliance_tags`` don't intersect with this set are
        skipped at dispatch time — gives operators a single switch to
        toggle GDPR-only / HIPAA-only / PCI-only modes.
    """

    version: str = "1.0"
    fields_to_redact: list[str] = Field(
        default_factory=lambda: ["message", "user_data", "details"]
    )
    rules: dict[str, PatternRule]
    audit_all_redactions: bool = True
    active_compliance_sets: list[Literal["GDPR", "HIPAA", "PCI_DSS"]] = Field(
        default_factory=lambda: ["GDPR", "HIPAA", "PCI_DSS"]
    )

    # Same rationale as PatternRule: frozen for thread-safe snapshots,
    # extra="forbid" for operator-typo detection at load time.
    model_config = ConfigDict(extra="forbid", frozen=True)
