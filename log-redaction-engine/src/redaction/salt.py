"""Hex-decoded salt loader for the :class:`HashStrategy`.

The hash-based redaction strategy needs a 32-byte salt that is:

* **Deterministic** across restarts within a single deployment — so the
  same plaintext produces the same hash and downstream systems can
  correlate redacted records.
* **Unguessable** across deployments — so an attacker who learns the hash
  output for one deployment cannot replay it against another.

The salt is sourced from the ``REDACTION_HASH_SALT`` environment variable
(surfaced via :func:`src.settings.get_settings`) as a 64-character hex
string. We fail loudly at the first sign of misconfiguration rather than
silently degrading to a weak default — a wrong salt would silently break
hash correlation in production and an empty salt would silently leak that
fact to anyone with read access to the redacted logs.

Why a separate module
---------------------
The loader is tiny but the validation logic (hex-decode + length check)
is the kind of thing that earns its own unit tests. Keeping it isolated
from :mod:`src.redaction.strategies` means we can exercise the failure
paths without spinning up a strategy registry.
"""
from __future__ import annotations

import logging

from src.settings import get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# SHA-256 takes any key length, but 32 bytes (256 bits) matches the digest
# size and removes any "is this salt long enough?" debate. Documented here
# as a constant so the error message and the assertion don't drift.
_REQUIRED_SALT_BYTES = 32


def load_salt() -> bytes:
    """Return the 32-byte redaction salt, decoded from the configured hex string.

    Reads :attr:`Settings.REDACTION_HASH_SALT` via :func:`get_settings`,
    decodes it as hex, and verifies the result is exactly 32 bytes.

    Raises
    ------
    RuntimeError
        If the configured value is not valid hex, or if the decoded bytes
        are not exactly 32 long. The exception message includes only a
        truncated prefix of the offending value — operators frequently
        paste secrets into bug reports, and a full echo here would
        amplify that mistake.
    """
    # Single call to get_settings() — the LRU cache means this is cheap
    # but we still avoid calling it twice in case a future test rebinds
    # the singleton between the two reads.
    salt_hex = get_settings().REDACTION_HASH_SALT

    try:
        salt_bytes = bytes.fromhex(salt_hex)
    except ValueError as exc:
        # bytes.fromhex() raises ValueError for non-hex chars OR odd length;
        # both failure modes mean "this is not a hex string at all".
        # Truncate to 8 chars in the message so we don't echo the full secret
        # into logs / error responses. ``!r`` quotes the prefix so an operator
        # can see whether the offender contains whitespace / odd characters.
        raise RuntimeError(
            f"REDACTION_HASH_SALT must be hex; got first 8 chars: {salt_hex[:8]!r}..."
        ) from exc

    if len(salt_bytes) != _REQUIRED_SALT_BYTES:
        # Wrong length means the operator likely copy/pasted the wrong value
        # (e.g., a 16-byte token vs a 32-byte one). Tell them the expected
        # size in BOTH bytes and hex chars to remove ambiguity.
        raise RuntimeError(
            f"REDACTION_HASH_SALT must be 32 bytes (64 hex chars); "
            f"got {len(salt_bytes)} bytes"
        )

    logger.debug("loaded %d-byte redaction salt", len(salt_bytes))
    return salt_bytes
