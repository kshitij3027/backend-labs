"""Unit tests for :mod:`src.features` (Commit 4).

These exercise the public feature-extraction API:
:func:`records_to_frame`, :class:`FeaturePipeline` and
:func:`build_feature_matrix`. The most important invariant verified throughout is
that the combined matrix is **non-negative** (``min() >= 0``) — required by
:class:`~sklearn.naive_bayes.MultinomialNB`, which rejects negative inputs — and
**fixed-width** across any input (a full batch, a single row, or a bare message).

The training corpus is a deterministic ``generate_logs(count=300, seed=42)`` batch
(module-scoped so the relatively expensive TF-IDF fit happens once).
"""

from __future__ import annotations

import numpy as np
import pytest
from scipy import sparse

from src.features import (
    DENSE_COLUMNS,
    METADATA_COLUMNS,
    TEMPORAL_COLUMNS,
    FeaturePipeline,
    build_feature_matrix,
    records_to_frame,
)
from src.log_generator import generate_logs


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def train_records() -> list[dict]:
    """A deterministic 300-record training batch (seed=42)."""
    return generate_logs(count=300, seed=42)


@pytest.fixture(scope="module")
def fitted(train_records):
    """A fitted ``(matrix, pipeline)`` pair built from ``train_records``.

    Module-scoped so the TF-IDF fit only runs once across the whole test module.
    """
    matrix, pipeline = build_feature_matrix(train_records)
    return matrix, pipeline


#: A bare, unlabeled message (no ``[LEVEL]`` bracket, mentions a DB keyword).
BARE_RECORD = {
    "raw_log": "Database connection failed with timeout error",
    "timestamp": "2026-06-21T00:00:00",
    "service": "",
    "severity": "",
    "category": "",
}


# ---------------------------------------------------------------------------
# 1. Shape & feature_dim composition.
# ---------------------------------------------------------------------------


def test_fit_transform_shape_and_dim(fitted):
    """fit_transform returns a CSR matrix of shape (300, tfidf_vocab + 20)."""
    matrix, pipeline = fitted
    assert sparse.issparse(matrix)
    assert sparse.isspmatrix_csr(matrix)
    assert matrix.shape[0] == 300
    assert matrix.shape[1] == pipeline.feature_dim
    # feature_dim == tfidf vocabulary size + 20 dense columns.
    n_tfidf = sum(1 for n in pipeline.get_feature_names() if n.startswith("tfidf__"))
    assert pipeline.feature_dim == n_tfidf + 20
    assert n_tfidf > 0  # the vocabulary is non-empty


# ---------------------------------------------------------------------------
# 2. Non-negativity (the MultinomialNB-critical invariant).
# ---------------------------------------------------------------------------


def test_non_negative_fit_transform(fitted):
    """The fit_transform matrix is entirely non-negative."""
    matrix, _ = fitted
    assert matrix.min() >= 0


def test_non_negative_transform_of_other_records(fitted):
    """transform() of a *different* batch is also entirely non-negative."""
    _, pipeline = fitted
    other = generate_logs(count=120, seed=7)  # disjoint seed from training
    out = pipeline.transform(other)
    assert sparse.isspmatrix_csr(out)
    assert out.min() >= 0


# ---------------------------------------------------------------------------
# 3. Fixed width across heterogeneous inputs.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("size", [1, 5, 50])
def test_transform_fixed_width_various_batch_sizes(fitted, size):
    """transform of size-1/5/50 batches all yield feature_dim columns."""
    _, pipeline = fitted
    recs = generate_logs(count=size, seed=99)[:size]
    out = pipeline.transform(recs)
    assert out.shape == (len(recs), pipeline.feature_dim)
    assert out.min() >= 0


def test_transform_fixed_width_single_bare_message(fitted):
    """A single bare-message dict transforms to the same fixed width."""
    _, pipeline = fitted
    out = pipeline.transform([BARE_RECORD])
    assert out.shape == (1, pipeline.feature_dim)
    assert out.min() >= 0


# ---------------------------------------------------------------------------
# 4. Feature names.
# ---------------------------------------------------------------------------


def test_feature_names_length_and_tail(fitted):
    """get_feature_names is feature_dim long; tail==DENSE_COLUMNS; tfidf prefix."""
    _, pipeline = fitted
    names = pipeline.get_feature_names()
    assert len(names) == pipeline.feature_dim
    assert names[-20:] == list(DENSE_COLUMNS)
    assert any(n.startswith("tfidf__") for n in names)


# ---------------------------------------------------------------------------
# 5. Bare message — width, non-negativity, and derived dense columns.
# ---------------------------------------------------------------------------


def test_bare_message_dense_columns(fitted):
    """Bare DB message: width/min via transform; level_*==0 & svc_database==1 via frame."""
    _, pipeline = fitted
    out = pipeline.transform([BARE_RECORD])
    assert out.shape[1] == pipeline.feature_dim
    assert out.min() >= 0

    frame = records_to_frame([BARE_RECORD])
    # No "[LEVEL]" bracket -> every level_* one-hot is 0.
    for col in [c for c in DENSE_COLUMNS if c.startswith("level_")]:
        assert frame[col].iloc[0] == 0.0
    # "Database" keyword -> svc_database lights up.
    assert frame["svc_database"].iloc[0] == 1.0


# ---------------------------------------------------------------------------
# 6. Dict missing timestamp AND service keys entirely.
# ---------------------------------------------------------------------------


def test_record_missing_keys_no_crash(fitted):
    """A dict with only raw_log (no timestamp/service) transforms at fixed width."""
    _, pipeline = fitted
    minimal = {"raw_log": "redis cache evicted keys"}
    out = pipeline.transform([minimal])
    assert out.shape == (1, pipeline.feature_dim)
    assert out.min() >= 0

    # records_to_frame should also tolerate the missing keys.
    frame = records_to_frame([minimal])
    assert list(frame.columns) == ["text", *DENSE_COLUMNS]
    # A cache keyword should light up svc_cache.
    assert frame["svc_cache"].iloc[0] == 1.0


# ---------------------------------------------------------------------------
# 7. Save / load round-trip.
# ---------------------------------------------------------------------------


def test_save_load_round_trip(fitted, tmp_path):
    """Reloaded pipeline matches feature_dim, names, and transform output."""
    _, pipeline = fitted
    path = tmp_path / "feature_pipeline.joblib"
    pipeline.save(str(path))
    assert path.exists()

    reloaded = FeaturePipeline.load(str(path))
    assert reloaded.feature_dim == pipeline.feature_dim
    assert reloaded.get_feature_names() == pipeline.get_feature_names()

    # Transform identical records through both; outputs must be element-wise equal.
    recs = generate_logs(count=20, seed=5)
    a = pipeline.transform(recs)
    b = reloaded.transform(recs)
    assert a.shape == b.shape
    assert np.allclose(a.toarray(), b.toarray())
    assert (a != b).nnz == 0


# ---------------------------------------------------------------------------
# 8. records_to_frame column layout.
# ---------------------------------------------------------------------------


def test_records_to_frame_columns(train_records):
    """The engineered frame's columns are exactly [text, *DENSE_COLUMNS]."""
    frame = records_to_frame(train_records[:10])
    assert list(frame.columns) == ["text", *DENSE_COLUMNS]
    assert len(frame) == 10


# ---------------------------------------------------------------------------
# 9. Error cases.
# ---------------------------------------------------------------------------


def test_fit_empty_raises_value_error():
    """Fitting on an empty record list raises ValueError."""
    pipeline = FeaturePipeline()
    with pytest.raises(ValueError):
        pipeline.fit([])


def test_records_to_frame_empty_raises_value_error():
    """records_to_frame on an empty iterable raises ValueError."""
    with pytest.raises(ValueError):
        records_to_frame([])


def test_transform_before_fit_raises_runtime_error():
    """Calling transform before fit raises RuntimeError."""
    pipeline = FeaturePipeline()
    with pytest.raises(RuntimeError):
        pipeline.transform([BARE_RECORD])


def test_get_feature_names_before_fit_raises_runtime_error():
    """Calling get_feature_names before fit raises RuntimeError."""
    pipeline = FeaturePipeline()
    with pytest.raises(RuntimeError):
        pipeline.get_feature_names()


# ---------------------------------------------------------------------------
# 10. DENSE_COLUMNS structural contract.
# ---------------------------------------------------------------------------


def test_dense_columns_structure():
    """DENSE_COLUMNS has length 20 and == TEMPORAL_COLUMNS + METADATA_COLUMNS."""
    assert len(DENSE_COLUMNS) == 20
    assert DENSE_COLUMNS == TEMPORAL_COLUMNS + METADATA_COLUMNS
    assert len(TEMPORAL_COLUMNS) == 5
    assert len(METADATA_COLUMNS) == 15
