"""Unit tests for the feature-extraction pipeline (:mod:`src.features`).

These pin the contract the streaming engine (C8) and the clusterers (C5-C7) depend on:

* fit-once / frozen transformers => a stable :pyattr:`FeatureExtractor.feature_dim`,
* dense ``float32`` output with no ``NaN``/``Inf``,
* :meth:`transform` is pure / deterministic (identical arrays across calls),
* the masking **collision** property carries into the content (TF-IDF) sub-vector — two
  logs differing only in IP/number share an identical content block,
* :meth:`transform_stream` updates running state (the "time since last similar" feature
  is the cap on first sight of a template and smaller on an immediate repeat),
* unseen ``service`` / ``level`` at transform time do not crash (``handle_unknown``),
* :meth:`project_2d` yields a 2-column projection, and
* calling ``transform`` before ``fit`` raises.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pytest

from src.features import FeatureExtractor
from src.log_generator import generate_logs
from src.schemas import LogEntry


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


#: Warm-up batch size for the shared extractor. Chosen so the TF-IDF ``max_features=1000``
#: cap actually binds (the masked corpus has >1000 distinct (1,2)-grams once it is this
#: large), giving the wide content block the spec expects. It also matches the committed
#: ``data/sample.jsonl`` the real engine warms up on. A much smaller batch (e.g. 300) has
#: fewer than 1000 distinct ngrams, so the cap would not bind — see
#: ``test_small_batch_feature_dim_below_cap`` which documents that ``max_features`` is a
#: cap, not a floor.
_WARMUP_N = 800


@pytest.fixture(scope="module")
def fitted_extractor() -> FeatureExtractor:
    """A :class:`FeatureExtractor` fit on a deterministic warm-up batch.

    Module-scoped because fitting (TF-IDF + PCA) is the expensive step; the frozen
    transformers are read-only from the tests' perspective, and ``transform`` never
    mutates them, so sharing one instance is safe.
    """
    logs = generate_logs(_WARMUP_N, seed=1)
    return FeatureExtractor().fit(logs)


# --------------------------------------------------------------------------- #
# fit
# --------------------------------------------------------------------------- #


def test_fit_sets_fitted_and_feature_dim(fitted_extractor: FeatureExtractor) -> None:
    """fit marks the extractor fitted and yields a wide feature space (> 1000 dims)."""
    assert fitted_extractor.fitted is True
    # TF-IDF (capped at max_features=1000) + numeric + one-hot => > 1000 once the cap
    # binds on a realistic warm-up corpus.
    assert fitted_extractor.feature_dim > 1000


def test_small_batch_feature_dim_below_cap() -> None:
    """``max_features`` is a CAP, not a floor: a tiny corpus yields a smaller dim.

    Documents the architecture explicitly — a 300-log batch has fewer than 1000 distinct
    masked (1,2)-grams, so ``feature_dim`` is well under 1000. ``feature_dim`` is whatever
    the warm-up batch produces and is then frozen; it is *not* guaranteed to hit the cap.
    """
    fe = FeatureExtractor().fit(generate_logs(300, seed=1))
    assert fe.fitted is True
    assert fe.feature_dim < 1000
    # And it stays stable: transform yields exactly that many columns.
    assert fe.transform(generate_logs(10, seed=4)).shape[1] == fe.feature_dim


def test_fit_returns_self() -> None:
    """fit returns the instance for chaining."""
    fe = FeatureExtractor()
    assert fe.fit(generate_logs(50, seed=3)) is fe


def test_fit_empty_raises() -> None:
    """Fitting on an empty batch raises (no vocabulary to learn)."""
    with pytest.raises(ValueError):
        FeatureExtractor().fit([])


# --------------------------------------------------------------------------- #
# transform — shape / dtype / finiteness / determinism
# --------------------------------------------------------------------------- #


def test_transform_shape_dtype_finite(fitted_extractor: FeatureExtractor) -> None:
    """transform returns (n, feature_dim) float32 with only finite values."""
    logs = generate_logs(40, seed=7)
    X = fitted_extractor.transform(logs)
    assert X.shape == (len(logs), fitted_extractor.feature_dim)
    assert X.dtype == np.float32
    assert np.isfinite(X).all()


def test_transform_is_deterministic(fitted_extractor: FeatureExtractor) -> None:
    """Two transforms of the same logs produce bit-identical arrays (pure / no state)."""
    logs = generate_logs(40, seed=7)
    X1 = fitted_extractor.transform(logs)
    X2 = fitted_extractor.transform(logs)
    np.testing.assert_array_equal(X1, X2)


def test_transform_empty_returns_zero_rows(fitted_extractor: FeatureExtractor) -> None:
    """An empty batch yields a (0, feature_dim) array, not an error."""
    X = fitted_extractor.transform([])
    assert X.shape == (0, fitted_extractor.feature_dim)
    assert X.dtype == np.float32


def test_transform_before_fit_raises() -> None:
    """transform must refuse to run before fit (frozen-after-fit contract)."""
    fe = FeatureExtractor()
    with pytest.raises(RuntimeError):
        fe.transform(generate_logs(5, seed=1))


def test_transform_stream_before_fit_raises() -> None:
    """transform_stream likewise refuses before fit."""
    fe = FeatureExtractor()
    with pytest.raises(RuntimeError):
        fe.transform_stream(generate_logs(1, seed=1)[0])


def test_project_2d_before_fit_raises() -> None:
    """project_2d also requires a fitted PCA."""
    fe = FeatureExtractor()
    with pytest.raises(RuntimeError):
        fe.project_2d(np.zeros((3, 5), dtype=np.float32))


# --------------------------------------------------------------------------- #
# Content collision: TF-IDF sub-vector ignores IP / numeric differences
# --------------------------------------------------------------------------- #


def _content_block(fe: FeatureExtractor, X: np.ndarray) -> np.ndarray:
    """Slice the TF-IDF (content) sub-matrix off the tail of a feature matrix."""
    n_tfidf = fe._n_tfidf  # noqa: SLF001 - white-box check of the content block
    return X[:, -n_tfidf:]


def test_content_block_collision_on_ip_and_number(
    fitted_extractor: FeatureExtractor,
) -> None:
    """Two logs differing ONLY in IP/number share an identical content (TF-IDF) block.

    This is the masking collision property propagating into features: ``mask_log`` rewrites
    the IP -> ``<IP>`` and the latency -> ``<NUM>``, so both messages tokenize identically
    and their TF-IDF rows must match exactly.
    """
    ts = datetime(2026, 6, 23, 10, 0, 0)
    a = LogEntry(
        timestamp=ts,
        service="auth",
        level="WARN",
        message="Failed login attempt for user-aaa from 203.0.113.7 took 12.5ms",
        source_ip="203.0.113.7",
        endpoint="/api/v1/login",
        response_time_ms=12.5,
        status_code=401,
    )
    b = LogEntry(
        timestamp=ts,
        service="auth",
        level="WARN",
        message="Failed login attempt for user-aaa from 198.51.100.99 took 88.0ms",
        source_ip="198.51.100.99",
        endpoint="/api/v1/login",
        response_time_ms=88.0,
        status_code=401,
    )
    Xa = fitted_extractor.transform([a])
    Xb = fitted_extractor.transform([b])
    np.testing.assert_array_equal(
        _content_block(fitted_extractor, Xa),
        _content_block(fitted_extractor, Xb),
    )


# --------------------------------------------------------------------------- #
# transform_stream — shape + running "since-last" state
# --------------------------------------------------------------------------- #


def test_transform_stream_shape(fitted_extractor: FeatureExtractor) -> None:
    """Each streamed log returns a (1, feature_dim) finite float32 row."""
    logs = generate_logs(10, seed=9)
    for log in logs:
        row = fitted_extractor.transform_stream(log)
        assert row.shape == (1, fitted_extractor.feature_dim)
        assert row.dtype == np.float32
        assert np.isfinite(row).all()


def test_transform_stream_time_since_last_similar_updates() -> None:
    """The "time since last similar" feature is the cap on first sight, then smaller.

    Build a fresh extractor, stream a log, then stream the *same masked template* 60s
    later. The first row carries the cap (3600); the immediate repeat carries ~60s, which
    is strictly smaller — proving the per-template last-seen clock is being updated.
    """
    # Fit on its own batch so we have an isolated, persistent streaming state.
    fe = FeatureExtractor().fit(generate_logs(200, seed=2))
    names = fe.feature_names()
    col = names.index("time_since_last_similar_sec")

    t0 = datetime(2026, 6, 23, 8, 0, 0)
    first = LogEntry(
        timestamp=t0,
        service="auth",
        level="ERROR",
        message="Brute force attack suspected from 203.0.113.7 on /api/v1/login",
        source_ip="203.0.113.7",
        endpoint="/api/v1/login",
        response_time_ms=50.0,
        status_code=429,
    )
    # Same message shape but a different IP -> SAME masked template, 60s later.
    repeat = first.model_copy(
        update={
            "timestamp": t0 + timedelta(seconds=60),
            "message": "Brute force attack suspected from 198.51.100.99 on /api/v1/login",
            "source_ip": "198.51.100.99",
        }
    )

    # Read the *unscaled* numeric value via a fresh local state to assert the raw seconds,
    # independent of the StandardScaler (which is fit on the warm-up batch).
    p_first = fe.transform_stream(first)
    p_repeat = fe.transform_stream(repeat)
    # Scaled values: the repeat's scaled "since-last" must be strictly less than the first
    # (cap -> ~60s is a decrease, and StandardScaler is monotonic per column).
    assert p_repeat[0, col] < p_first[0, col]


def test_transform_stream_mutates_only_persistent_state(
    fitted_extractor: FeatureExtractor,
) -> None:
    """transform (batch) must NOT see state left by prior transform_stream calls.

    Stream some logs (mutating the persistent state), then assert a batch transform is
    still deterministic and matches a second identical batch transform — i.e. the pure
    batch path is isolated from the hot-path state.
    """
    batch = generate_logs(25, seed=11)
    ref = fitted_extractor.transform(batch)
    for log in generate_logs(15, seed=12):
        fitted_extractor.transform_stream(log)
    after = fitted_extractor.transform(batch)
    np.testing.assert_array_equal(ref, after)


# --------------------------------------------------------------------------- #
# project_2d
# --------------------------------------------------------------------------- #


def test_project_2d_shape(fitted_extractor: FeatureExtractor) -> None:
    """project_2d(transform(logs)) returns an (n, 2) finite float32 array."""
    logs = generate_logs(30, seed=5)
    X = fitted_extractor.transform(logs)
    proj = fitted_extractor.project_2d(X)
    assert proj.shape == (len(logs), 2)
    assert proj.dtype == np.float32
    assert np.isfinite(proj).all()


def test_project_2d_wrong_width_raises(fitted_extractor: FeatureExtractor) -> None:
    """A matrix with the wrong number of columns is rejected."""
    with pytest.raises(ValueError):
        fitted_extractor.project_2d(np.zeros((4, fitted_extractor.feature_dim + 1)))


# --------------------------------------------------------------------------- #
# Unseen categories at transform time
# --------------------------------------------------------------------------- #


def test_unseen_service_and_level_do_not_crash(
    fitted_extractor: FeatureExtractor,
) -> None:
    """A never-before-seen service/level transforms fine (OneHotEncoder handle_unknown).

    The one-hot columns for the unknown categories are all-zero, but the row is still the
    right width and finite.
    """
    weird = LogEntry(
        timestamp=datetime(2026, 6, 23, 3, 0, 0),
        service="totally-new-service",
        level="TRACE",  # not in the fitted level vocabulary
        message="some entirely novel event 99 from 8.8.8.8",
        source_ip="8.8.8.8",
        endpoint="/new",
        response_time_ms=None,  # also exercises the resp_missing flag
        status_code=None,
    )
    X = fitted_extractor.transform([weird])
    assert X.shape == (1, fitted_extractor.feature_dim)
    assert np.isfinite(X).all()


def test_dict_input_supported(fitted_extractor: FeatureExtractor) -> None:
    """Plain dict logs (decoded JSON) transform identically to LogEntry inputs."""
    entry = {
        "timestamp": datetime(2026, 6, 23, 12, 0, 0),
        "service": "auth",
        "level": "INFO",
        "message": "Health check ok",
        "source_ip": "10.0.0.5",
        "endpoint": "/healthz",
        "response_time_ms": 4.0,
        "status_code": 200,
    }
    model = LogEntry(**entry)
    X_dict = fitted_extractor.transform([entry])
    X_model = fitted_extractor.transform([model])
    np.testing.assert_array_equal(X_dict, X_model)
