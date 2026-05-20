"""Configuration layer — pydantic models, JSON loader, and hot-reload manager.

This package owns the **declarative policy** half of the engine. Everything
about *what* to redact (which patterns are active, which strategy each one
uses, which compliance regime is in force, which log fields to scan) lives
here as immutable pydantic models. The detection and redaction layers in
``src.detection`` and ``src.redaction`` consume these models to dispatch
the right transform per pattern.

Sub-modules
-----------
* :mod:`src.config.models`  — frozen ``RedactionConfig`` + ``PatternRule``.
* :mod:`src.config.loader`  — JSON-on-disk → validated pydantic objects.
* :mod:`src.config.manager` — ``ConfigurationManager`` with RLock-guarded
  atomic hot-reload semantics (old refs remain valid after reload).

Intentionally empty — exported symbols live in their concrete modules so
type-checkers can follow the import path back to the definition site.
"""
