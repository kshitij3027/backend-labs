"""Constants-only compliance rule book.

This module is intentionally tiny: two dicts keyed by the same five
framework slugs used by ``Policy.compliance_tag``. The validator in
``compliance/validator.py`` consults these dicts at policy load time and
nothing else — there are no regulatory lookups at runtime, no network
calls, no third-party packages. If a regulation changes, edit the
constant and ship a new build; auditors can diff the change in git.

Why constants and not a class hierarchy: the rules are five integers
and five booleans. A class per framework would add ceremony without
adding behavior — each rule reduces to a numeric comparison or a
boolean check, both of which read more clearly inline.
"""
from __future__ import annotations

from typing import Final

# Minimum retention period (days) required by each compliance framework.
# GDPR's 1095 d (3 yr) is a conservative de-facto baseline for security logs
# (the regulation itself has no explicit minimum). The others are codified
# rules: SOX 7 yr (SEC 17 CFR 210.2-06), HIPAA 6 yr (45 CFR 164.316(b)(2)),
# PCI DSS 1 yr (Req. 10.5.1), SOC 2 1 yr (no formal minimum, but Type II
# window expectation).
MIN_RETENTION_DAYS: Final[dict[str, int]] = {
    "gdpr": 1095,
    "sox": 2555,
    "hipaa": 2190,
    "pci_dss": 365,
    "soc2": 365,
}

# Whether the framework requires immutable storage (WORM) for logs that
# land in the archive tier. SOC2 / GDPR allow mutable storage as long as
# tamper-evidence is provided elsewhere (the hash chain).
REQUIRES_IMMUTABLE: Final[dict[str, bool]] = {
    "gdpr": False,
    "sox": True,
    "hipaa": True,
    "pci_dss": True,
    "soc2": False,
}
