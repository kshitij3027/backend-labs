"""Index stats are returned as plain dicts for now — see :meth:`InvertedIndex.stats`.

Commit 09 will wrap these in the :class:`~src.models.StatsResponse` pydantic
model when the ``/api/search/stats`` endpoint lands. This module exists as
a forward-compatible seam so imports do not need to move once the wrapper
ships — downstream code that wants a typed projection can start importing
from ``src.index.stats`` today and the symbols will appear here in commit 09.
"""
