"""Ed25519 sign/verify wrappers used to seal each audit record's self-hash.

The signer takes a base64-encoded 32-byte seed (the private key material) and
exposes deterministic signatures over the hex self-hash of a record. The
verifier accepts the matching 32-byte raw public key and returns a boolean,
turning ``InvalidSignature`` exceptions into a clean ``False`` result so
callers can branch without try/except.
"""

import base64

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat


class Ed25519Signer:
    """Sign self-hash strings with a base64-encoded 32-byte Ed25519 seed."""

    def __init__(self, seed_b64: str) -> None:
        seed = base64.b64decode(seed_b64)
        if len(seed) != 32:
            raise ValueError(
                f"Ed25519 seed must decode to exactly 32 bytes, got {len(seed)}"
            )
        self._private_key = Ed25519PrivateKey.from_private_bytes(seed)

    def sign(self, self_hash_hex: str) -> str:
        """Sign the UTF-8 encoded ``self_hash_hex`` and return base64 bytes."""
        signature = self._private_key.sign(self_hash_hex.encode("utf-8"))
        return base64.b64encode(signature).decode("ascii")

    def public_key_b64(self) -> str:
        """Return the raw 32-byte public key, base64-encoded."""
        raw_public = self._private_key.public_key().public_bytes(
            Encoding.Raw, PublicFormat.Raw
        )
        return base64.b64encode(raw_public).decode("ascii")


class Ed25519Verifier:
    """Verify base64 Ed25519 signatures against UTF-8 self-hash strings."""

    def __init__(self, public_key_b64: str) -> None:
        public_bytes = base64.b64decode(public_key_b64)
        if len(public_bytes) != 32:
            raise ValueError(
                f"Ed25519 public key must decode to exactly 32 bytes, got {len(public_bytes)}"
            )
        self._public_key = Ed25519PublicKey.from_public_bytes(public_bytes)

    def verify(self, signature_b64: str, self_hash_hex: str) -> bool:
        """Return True iff ``signature_b64`` is a valid signature over the hash."""
        signature_bytes = base64.b64decode(signature_b64)
        try:
            self._public_key.verify(signature_bytes, self_hash_hex.encode("utf-8"))
        except InvalidSignature:
            return False
        return True
