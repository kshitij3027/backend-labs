"""Report generation pipeline package.

This package owns the end-to-end pipeline that turns a freshly-inserted
``Report`` row into a signed, encrypted artefact on disk:

  * ``state_machine`` — the ``ReportState`` enum and the
    ``assert_transition`` guard that keeps the lifecycle legal
    (``PENDING -> AGGREGATING -> EXPORTING -> SIGNING -> COMPLETED``,
    with any state allowed to short-circuit to ``FAILED``).
  * ``aggregator`` — pulls the relevant ``LogEvent`` rows for the
    requested framework + window and shapes them into the canonical
    payload dict the exporters and signer both consume.
  * ``coordinator`` — orchestrates aggregate -> export -> sign ->
    encrypt -> persist, bounded by an ``asyncio.Semaphore`` so the
    background-task queue doesn't blow past the configured concurrency
    ceiling.

Exporters live in a sibling ``exporters`` sub-package and are wired
into the coordinator via a ``dict[str, Callable[[dict], bytes]]`` so
each format (PDF / CSV / JSON / XML) is independently swappable.
"""
from __future__ import annotations

__all__ = ["aggregator", "coordinator", "state_machine"]
