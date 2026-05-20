"""Test fixtures for the redaction pipeline.

Each ``log_*.json`` file represents a realistic log entry the processor
might receive in a corresponding compliance scenario:

* ``log_pii.json``         — general-purpose PII (email + phone).
* ``log_phi.json``         — healthcare (MRN + SSN).
* ``log_pci.json``         — payment-card (credit card).
* ``log_mixed_batch.json`` — 10-entry batch spanning all patterns,
  including one plain entry to verify clean passthrough.

The fixtures live under ``tests/`` so pytest collection finds them but
they are NOT imported as Python modules — they're loaded via
``json.loads(Path(...).read_text())`` in the tests that exercise them.
This file exists only so the directory is a valid Python package, which
keeps editor/import-resolver tooling happy.
"""
