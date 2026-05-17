"""End-to-end log processing pipeline.

The processor layer is the seam between detection (C2), the keystore (C4),
and the eventual HTTP wiring (C7). It owns the in-process algorithm:

1. Detect sensitive leaves in a JSON-decoded log dict.
2. Encrypt only those leaves with the keystore's *active* DEK.
3. Splice the resulting :class:`~src.crypto.schema.EncryptedField` records
   back into a deep-cloned log and stamp a small ``_processing`` envelope
   so consumers (and the symmetric :meth:`LogProcessor.decrypt`) know
   which fields were transformed.

The parallel batch path is opt-in: :class:`ParallelEncryptor` falls back
to in-thread serial execution unless **both** field-count and total-byte
thresholds are exceeded. The threshold gate is intentional — for typical
small log entries the ``ThreadPoolExecutor`` submit overhead dwarfs the
~5-30 microsecond AES-GCM cost, so parallel only helps for fat batches.

Public surface:

* :class:`LogProcessor`     — encrypt/decrypt a single log dict.
* :class:`ParallelEncryptor` — threshold-gated thread-pool dispatcher.
* :class:`ProcessorError`   — raised on missing record_id, malformed
  encrypted-field payloads, etc.
"""
from __future__ import annotations

from .log_processor import LogProcessor, ProcessorError
from .parallel import ParallelEncryptor

__all__ = [
    "LogProcessor",
    "ParallelEncryptor",
    "ProcessorError",
]
