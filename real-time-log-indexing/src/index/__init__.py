"""Inverted-index package — tokenizer, segment, persistence, orchestrator.

Re-exports the top-level types callers most commonly want so they can
write ``from src.index import InvertedIndex`` rather than chasing the
concrete module path. Submodules remain importable under their own
names (``src.index.segment`` etc.) for anything that needs the finer
grained primitives.
"""

from src.index.inverted_index import InvertedIndex, SegmentMeta
from src.index.segment import Segment
from src.index.tokenizer import LogTokenizer

__all__ = [
    "InvertedIndex",
    "SegmentMeta",
    "Segment",
    "LogTokenizer",
]
