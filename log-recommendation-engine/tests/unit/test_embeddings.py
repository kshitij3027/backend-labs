"""Unit tests for the C4 embedding service (:mod:`src.embeddings`).

These exercise the sentence-transformers ``all-MiniLM-L6-v2`` model, which is
**baked into the test image** (``HF_HOME`` / ``SENTENCE_TRANSFORMERS_HOME`` under
``/app/.cache``) and loaded offline — no network fetch. First test to touch the
model pays the ~90 MB load cost via the ``@lru_cache`` singleton; the rest are cheap.

Coverage:
  * ``embed_texts`` returns an ``(n, 384)`` float32 array whose every row is a unit
    vector (L2 norm ≈ 1.0) — proves ``normalize_embeddings=True`` and dtype/shape.
  * ``embed_text`` returns a single ``(384,)`` vector.
  * **Semantic sanity**: two similar incident texts score a higher dot product than
    a similar/dissimilar pair — proves the real model loaded and normalisation is
    meaningful (dot == cosine for unit vectors).
  * ``build_incident_text`` includes title + description, adds a ``tags:`` segment
    only when tags are present, and omits it when tags are empty/blank.
"""

from __future__ import annotations

import numpy as np

from src import embeddings


# --------------------------------------------------------------------------- #
# Shape / dtype / normalisation
# --------------------------------------------------------------------------- #
def test_embed_texts_shape_dtype_and_normalised() -> None:
    """Batch encode → (n, 384) float32, each row L2-normalised to ≈ 1.0."""
    texts = [
        "Database connection pool exhausted under load.",
        "Checkout service returns 500 on payment capture.",
        "Nightly ETL job silently drops late-arriving events.",
    ]
    arr = embeddings.embed_texts(texts)

    assert isinstance(arr, np.ndarray)
    assert arr.shape == (len(texts), 384)
    assert arr.dtype == np.float32

    norms = np.linalg.norm(arr, axis=1)
    # Every row must be a unit vector (normalize_embeddings=True).
    assert np.allclose(norms, 1.0, atol=1e-4), f"row norms not ~1.0: {norms}"


def test_embed_text_single_vector_shape() -> None:
    """A single text → a 1-D ``(384,)`` unit vector."""
    vec = embeddings.embed_text("Redis connection refused during cache warmup.")

    assert isinstance(vec, np.ndarray)
    assert vec.ndim == 1
    assert vec.shape == (384,)
    # dim-mismatch guard sanity: output length is exactly the configured dim.
    assert len(vec) == 384
    assert vec.dtype == np.float32
    assert np.isclose(np.linalg.norm(vec), 1.0, atol=1e-4)


def test_embed_texts_empty_returns_empty_matrix() -> None:
    """An empty batch yields an empty ``(0, 384)`` float32 matrix, not an error."""
    arr = embeddings.embed_texts([])
    assert arr.shape == (0, 384)
    assert arr.dtype == np.float32


# --------------------------------------------------------------------------- #
# Semantic sanity — proves the real model loaded and cosine is meaningful
# --------------------------------------------------------------------------- #
def test_semantic_similarity_ordering() -> None:
    """Similar incidents score higher than a similar/dissimilar pair.

    ``similar_a`` and ``similar_b`` both describe DB connection-pool timeouts;
    ``dissimilar`` is about a UI colour bug. Because every vector is unit-length,
    the dot product is the cosine similarity, so::

        dot(similar_a, similar_b) > dot(similar_a, dissimilar)

    This can only hold if the actual MiniLM weights loaded (a random/identity
    "model" would not separate these), which is the real point of the assertion.
    """
    similar_a = embeddings.embed_incident(
        title="Database connection pool exhausted",
        description=(
            "Requests started timing out while waiting to check out a Postgres "
            "connection; the pool was saturated during peak traffic."
        ),
        tags=["database", "timeout", "pool"],
    )
    similar_b = embeddings.embed_incident(
        title="Postgres connection pool timeouts under load",
        description=(
            "Service threads blocked waiting for a free DB connection and the "
            "connection pool ran out, causing request timeouts."
        ),
        tags=["postgres", "connection-pool", "latency"],
    )
    dissimilar = embeddings.embed_incident(
        title="Login button is the wrong colour",
        description="The primary CTA button renders green instead of the brand blue.",
        tags=["ui", "css"],
    )

    sim_pair = float(np.dot(similar_a, similar_b))
    dissim_pair = float(np.dot(similar_a, dissimilar))

    assert sim_pair > dissim_pair, (
        f"expected similar>dissimilar but got sim={sim_pair:.4f} "
        f"dissim={dissim_pair:.4f}"
    )


# --------------------------------------------------------------------------- #
# Canonical document text
# --------------------------------------------------------------------------- #
def test_build_incident_text_includes_title_description_and_tags() -> None:
    """With tags present, the doc text carries title, description, and a ``tags:`` line."""
    text = embeddings.build_incident_text(
        title="Kafka consumer lag spike",
        description="Lag grew to 2M messages after a rebalance storm.",
        tags=["kafka", "lag"],
    )
    assert "Kafka consumer lag spike" in text
    assert "Lag grew to 2M messages after a rebalance storm." in text
    assert "tags:" in text
    assert "kafka" in text and "lag" in text


def test_build_incident_text_omits_tags_segment_when_empty() -> None:
    """No usable tags → no ``tags:`` segment at all (None and blank-only both omit)."""
    text_none = embeddings.build_incident_text(
        title="Disk full on node-7",
        description="The data volume hit 100% and writes began failing.",
        tags=None,
    )
    assert "tags:" not in text_none
    assert "Disk full on node-7" in text_none
    assert "The data volume hit 100% and writes began failing." in text_none

    # Blank / whitespace-only tags are dropped, so the segment is still omitted.
    text_blank = embeddings.build_incident_text(
        title="Disk full on node-7",
        description="The data volume hit 100% and writes began failing.",
        tags=["", "   "],
    )
    assert "tags:" not in text_blank
