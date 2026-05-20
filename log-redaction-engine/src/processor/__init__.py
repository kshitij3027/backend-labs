"""Processor subpackage: composes detection + strategies + config into a single pipeline.

C5 ships :class:`~src.processor.redaction_processor.RedactionProcessor`, the
in-process entry point used by the API layer (C7+). It depends on the C2
detector, C3 strategy registry, and C4 configuration manager but does not
import from the API or audit/stats layers — those are wired via optional
constructor arguments to keep this module decoupled.
"""
