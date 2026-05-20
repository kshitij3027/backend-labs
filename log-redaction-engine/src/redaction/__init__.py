"""Redaction layer — strategies, token store, and salt loader.

This package owns the **transform** half of the engine. Once the detection
layer (``src.detection``) has produced a list of :class:`~src.detection.patterns.Detection`
spans, the redaction layer rewrites those spans according to one of four
strategies (mask / partial / hash / tokenize) and — for the reversible
``tokenize`` strategy — manages the in-memory bidirectional mapping behind
an RBAC + audit gate.

Sub-modules
-----------
* :mod:`src.redaction.salt`        — hex-decoded SHA-256 salt loader.
* :mod:`src.redaction.token_store` — thread-safe forward/reverse map.
* :mod:`src.redaction.strategies`  — ``Strategy`` Protocol + four concrete
  implementations + a name→instance registry.

Intentionally empty — exported symbols live in their concrete modules so
type-checkers can follow the import path back to the definition site.
"""
