"""Batch splitter — splits oversized batches to fit within UDP datagram limits."""

import logging
from src.serializer import serialize_batch

logger = logging.getLogger(__name__)

# Maximum UDP payload size (65535 - 8 byte UDP header - 20 byte IP header)
MAX_UDP_PAYLOAD = 65507


def split_batch(entries: list[dict], compress: bool = True) -> list[bytes]:
    """Split a list of log-entry dicts into chunks that each fit in a UDP datagram.

    Uses a recursive binary-split approach: serialize the full batch, and if it
    exceeds *MAX_UDP_PAYLOAD*, split the entries list in half and recurse on
    each half.  Recursion depth is bounded by log2(len(entries)).

    Returns a list of serialized byte chunks, each <= MAX_UDP_PAYLOAD (unless a
    single entry already exceeds the limit, in which case it is returned as-is
    with a warning).
    """
    data = serialize_batch(entries, compress)

    if len(data) <= MAX_UDP_PAYLOAD:
        return [data]

    if len(entries) == 1:
        logger.warning(
            "Single log entry exceeds MAX_UDP_PAYLOAD (%d bytes > %d). "
            "Cannot split further; sending oversized datagram.",
            len(data),
            MAX_UDP_PAYLOAD,
        )
        return [data]

    mid = len(entries) // 2
    left = entries[:mid]
    right = entries[mid:]

    return split_batch(left, compress) + split_batch(right, compress)
