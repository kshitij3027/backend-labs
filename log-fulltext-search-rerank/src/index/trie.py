"""Character-level prefix trie for autocomplete suggestions.

Powers ``GET /api/search/suggestions``: given a prefix, return the
top-N tokens by frequency in under 5ms at 50k unique tokens. The
structure is deliberately simple — dict-of-dicts, no external library
— because the hot paths are walking ~10 characters of prefix and a
bounded DFS over the subtree beneath that node.

Design summary
--------------

* Each node is a plain ``dict``. Keys are single characters pointing
  to the child node (``dict``), plus one sentinel key ``"__END__"``
  whose value is an ``int`` holding the terminal-node frequency of
  the token that ends there. Using a sentinel key in the same dict
  (rather than a parallel "is_terminal" structure) keeps the memory
  footprint tight and the per-node allocation cost minimal.
* ``insert(token, freq=1)`` walks chars, creating nodes as needed,
  and bumps the terminal count at the end. ``_token_count`` increments
  only when a brand-new terminal is created so repeated inserts don't
  inflate the distinct-token count.
* ``suggest(prefix, limit)`` walks to the prefix node (missing →
  ``[]``), then DFS-collects ``(token, freq)`` for every terminal in
  the subtree, sorts by (``-freq``, token) for "most-popular first,
  alphabetical on ties", and returns the first ``limit`` token
  strings. Empty prefix returns ``[]`` per the spec — we intentionally
  do not fall through to "top-N globally".
* ``clear()`` resets the structure so the route can cheaply rebuild
  the trie when the inverted-index version changes.

Concurrency
-----------

The trie is populated lazily from the search route on an index-version
miss. All writes happen under the GIL via plain dict mutations. The
spec does not require concurrent writers, and reads during a write
would at worst observe an in-progress subtree — acceptable for an
advisory autocomplete surface.
"""

from __future__ import annotations


# Sentinel key used inside each trie node to record that this node is
# a terminal for some inserted token. Its value is the integer
# frequency contributed by the token(s) ending here. A multi-character
# name is used to keep it distinguishable from any real single-char
# key a caller might walk into.
_END_KEY = "__END__"


class PrefixTrie:
    """Character-level trie with per-terminal frequency counters.

    Supports ``insert`` to register tokens (optionally with a
    frequency weight — e.g. document frequency from the inverted
    index) and ``suggest`` to pull the top-N most-popular tokens that
    share a given prefix.
    """

    def __init__(self) -> None:
        # ``_root`` is the top of the trie. Every node follows the
        # same schema: char → child dict, plus an optional sentinel
        # ``_END_KEY`` pointing at the terminal frequency.
        self._root: dict = {}
        # Tracks the number of distinct tokens currently stored.
        # Incremented only on a newly-created terminal (see insert).
        self._token_count: int = 0

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def insert(self, token: str, freq: int = 1) -> None:
        """Register ``token`` in the trie with an optional frequency.

        ``freq`` defaults to 1, but callers that already know the
        document-frequency (e.g. the suggestions route, which reads
        it off the inverted index) can pass a larger count so the
        terminal reflects the token's true popularity.

        An empty token is a no-op — there is no meaningful way to
        represent it in a char-level trie and the spec explicitly
        treats empty prefixes as "no suggestions", not "all
        suggestions".
        """
        if not token:
            return

        # Walk chars, creating intermediate nodes when necessary. We
        # always create a fresh dict (never mutate an existing one)
        # the first time a char is seen at this depth.
        node = self._root
        for ch in token:
            child = node.get(ch)
            if child is None:
                child = {}
                node[ch] = child
            node = child

        # Terminal bookkeeping. If this terminal is brand new, the
        # distinct-token count goes up by one — repeated inserts of
        # the same token just bump the popularity counter.
        was_terminal = _END_KEY in node
        node[_END_KEY] = node.get(_END_KEY, 0) + freq
        if not was_terminal:
            self._token_count += 1

    def clear(self) -> None:
        """Drop all tokens and reset to the empty-trie state.

        The search route calls ``clear()`` before rebuilding the
        trie on an ``index.version`` mismatch so stale terminal
        frequencies do not linger across reindex cycles.
        """
        self._root = {}
        self._token_count = 0

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def suggest(self, prefix: str, limit: int = 10) -> list[str]:
        """Return up to ``limit`` tokens starting with ``prefix``.

        Ordering: primary key is frequency descending (most-popular
        first), secondary key is the token string ascending
        (alphabetical) to break ties deterministically.

        * An empty prefix returns ``[]`` — the spec requires that so
          the route does not accidentally dump the entire vocabulary
          to a client that sends no query.
        * A prefix that does not exist in the trie returns ``[]``.
        * ``limit`` is trusted — the route clamps it to ``[1, 100]``
          at the FastAPI layer, and an in-trie ``limit <= 0`` will
          naturally produce ``[]`` via slicing.
        """
        if not prefix or limit <= 0:
            return []

        # Step 1: walk to the prefix node. A missing char at any
        # depth means no tokens share this prefix, short-circuit.
        node = self._root
        for ch in prefix:
            child = node.get(ch)
            if child is None:
                return []
            node = child

        # Step 2: DFS from the prefix node, collecting every
        # terminal and its frequency. We track the current string by
        # threading a list that mirrors the path taken from the
        # prefix root — cheaper than repeatedly concatenating.
        results: list[tuple[str, int]] = []
        stack: list[tuple[dict, list[str]]] = [(node, list(prefix))]
        while stack:
            cur_node, cur_path = stack.pop()
            # Terminal? Record it.
            if _END_KEY in cur_node:
                results.append(("".join(cur_path), cur_node[_END_KEY]))
            # Push children. Iterate in reverse so the stack pops
            # children in ascending char order — purely cosmetic but
            # makes debugging output easier to read.
            for ch in sorted(cur_node.keys(), reverse=True):
                if ch == _END_KEY:
                    continue
                stack.append((cur_node[ch], cur_path + [ch]))

        if not results:
            return []

        # Step 3: sort by (-freq, token-asc) and trim.
        results.sort(key=lambda pair: (-pair[1], pair[0]))
        return [tok for tok, _ in results[:limit]]

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    @property
    def token_count(self) -> int:
        """Return the number of distinct tokens currently stored."""
        return self._token_count
