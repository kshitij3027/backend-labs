"""Text-embedding service (sentence-transformers ``all-MiniLM-L6-v2``).

This module is the single source of truth for turning incident text into dense
vectors. It is deliberately narrow in C4: it exposes the embedding + cache
*primitives* only. Wiring embeddings into ingestion (embed-on-ingest) is C5 and
K-NN retrieval is C6 — nothing here touches the database or the API.

Model loading
-------------
The model is loaded **lazily** as a per-process singleton (:func:`get_model`,
``@lru_cache``) — never at import time — so importing this module (and therefore
booting the app / answering ``/health``) stays fast and does not block on the
~90 MB model load. The weights are **baked into the Docker image** at build time
(see ``Dockerfile``: it runs ``SentenceTransformer('all-MiniLM-L6-v2')`` and sets
``HF_HOME`` / ``SENTENCE_TRANSFORMERS_HOME`` under ``/app/.cache``). We therefore
load from that on-disk cache and never trigger a network download at runtime.

Canonical document text
------------------------
:func:`build_incident_text` defines *what* gets embedded and is used for **both**
corpus incidents and query incidents, so the two are directly comparable in
vector space. Keep this function stable — changing it changes the meaning of
every stored vector.

Normalisation
-------------
All embeddings are **L2-normalised** (``normalize_embeddings=True``) and returned
as ``float32``. With unit vectors the dot product equals cosine similarity, which
is exactly what pgvector's ``vector_cosine_ops`` index (used for corpus KNN)
expects — so the same vectors serve indexing and in-process similarity without
rescaling.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import TYPE_CHECKING

import numpy as np

from src import observability
from src.clients import redis as redis_client
from src.config import get_settings

if TYPE_CHECKING:  # import only for type checkers; avoids the heavy import at runtime
    from sentence_transformers import SentenceTransformer

logger = observability.get_logger(__name__)


# --------------------------------------------------------------------------- #
# Model loading (lazy singleton, offline from the baked cache)
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def get_model() -> "SentenceTransformer":
    """Load and cache the sentence-transformers model (once per process).

    Lazy: the import and weight load happen on first call, not at module import,
    so app startup / ``/health`` are never blocked by the model load. The model
    name comes from ``settings.embedding_model`` (``all-MiniLM-L6-v2``) and is
    loaded from the cache baked into the image (``HF_HOME`` /
    ``SENTENCE_TRANSFORMERS_HOME`` are set in the Dockerfile), so no network
    fetch happens at runtime.
    """
    # Belt-and-braces: force HF/transformers into offline mode for this process so
    # a cache miss fails loudly instead of silently reaching out to the network.
    # The model is guaranteed present in the image, so this is safe.
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

    from sentence_transformers import SentenceTransformer

    model_name = get_settings().embedding_model
    logger.info("loading embedding model", model=model_name)
    return SentenceTransformer(model_name)


def _expected_dim() -> int:
    """Configured embedding dimensionality (``settings.embedding_dim`` → 384)."""
    return int(get_settings().embedding_dim)


def _validate_dim(vec: np.ndarray) -> None:
    """Raise if ``vec``'s trailing dimension is not the configured size."""
    expected = _expected_dim()
    actual = int(vec.shape[-1]) if vec.ndim else 0
    if actual != expected:
        raise ValueError(
            f"embedding dimension mismatch: expected {expected}, got {actual} "
            f"(model={get_settings().embedding_model!r})"
        )


# --------------------------------------------------------------------------- #
# Canonical document text
# --------------------------------------------------------------------------- #
def build_incident_text(
    title: str, description: str, tags: "list[str] | None"
) -> str:
    """Build the canonical document text embedded for an incident/query.

    Joins the title, description, and a normalised tag line into a single string.
    Used for **both** corpus incidents and query incidents so their embeddings
    are comparable. The format is intentionally simple and stable::

        <title>
        <description>
        tags: <tag1>, <tag2>, ...

    Empty/blank parts are dropped; tags are stripped, de-blanked, and joined with
    ", ". The tag line is omitted entirely when there are no usable tags.
    """
    parts: list[str] = []
    if title and title.strip():
        parts.append(title.strip())
    if description and description.strip():
        parts.append(description.strip())
    if tags:
        clean_tags = [t.strip() for t in tags if t and t.strip()]
        if clean_tags:
            parts.append("tags: " + ", ".join(clean_tags))
    return "\n".join(parts)


# --------------------------------------------------------------------------- #
# Encoding
# --------------------------------------------------------------------------- #
def embed_texts(texts: "list[str]") -> np.ndarray:
    """Batch-encode ``texts`` into an L2-normalised ``float32`` ``(n, 384)`` array.

    ``normalize_embeddings=True`` makes each row a unit vector, so the dot product
    equals cosine similarity (matching pgvector ``vector_cosine_ops``). An empty
    input yields an empty ``(0, dim)`` array. Raises :class:`ValueError` if the
    model returns an unexpected dimensionality.
    """
    if not texts:
        return np.empty((0, _expected_dim()), dtype=np.float32)

    model = get_model()
    vectors = model.encode(
        texts,
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=False,
    )
    arr = np.asarray(vectors, dtype=np.float32)
    if arr.ndim == 1:  # single row can come back 1-D; promote to (1, dim)
        arr = arr.reshape(1, -1)
    _validate_dim(arr)
    return arr


def embed_text(text: str) -> np.ndarray:
    """Encode a single ``text`` into an L2-normalised ``float32`` ``(384,)`` vector."""
    arr = embed_texts([text])
    return arr[0]


def embed_incident(
    title: str, description: str, tags: "list[str] | None"
) -> np.ndarray:
    """Embed a **corpus** incident: build its doc text, then encode it → ``(384,)``."""
    return embed_text(build_incident_text(title, description, tags))


def embed_query(
    title: str, description: str, tags: "list[str] | None"
) -> np.ndarray:
    """Embed a **query** incident → ``(384,)``.

    Identical in behaviour to :func:`embed_incident` (same doc text, same model,
    same normalisation) — exposed under its own name so query-side call sites read
    clearly and can diverge later if the query path ever needs different handling.
    """
    return embed_text(build_incident_text(title, description, tags))


def embed_text_cached(text: str) -> np.ndarray:
    """Return the embedding for ``text``, using the Redis cache as a read-through.

    On a cache hit the stored ``float32`` vector is returned directly. On a miss
    (or when Redis is unavailable — the cache degrades to ``None``) the embedding
    is computed via :func:`embed_text`, written back to the cache best-effort, and
    returned. Used by later commits for query embeddings, where repeated identical
    queries are common. The cache never affects correctness: a miss just recomputes.
    """
    cached = redis_client.cache_get_embedding(text)
    if cached is not None:
        # Guard against a stale/foreign entry of the wrong size (e.g. after a
        # model change): treat a dimension mismatch as a miss and recompute.
        if int(cached.shape[-1]) == _expected_dim():
            return cached
        logger.warning(
            "cached embedding has wrong dimension; recomputing",
            got=int(cached.shape[-1]),
            expected=_expected_dim(),
        )
    vec = embed_text(text)
    redis_client.cache_set_embedding(text, vec)
    return vec


__all__ = [
    "get_model",
    "build_incident_text",
    "embed_texts",
    "embed_text",
    "embed_incident",
    "embed_query",
    "embed_text_cached",
]
