"""Base classifier factories for the ML Log Classifier (Commit 5).

This module is intentionally thin: it constructs the three individual,
**unfitted** scikit-learn classifiers named in the spec
(project requirements §2 — "Train three individual classifiers: Naive Bayes,
Random Forest, and Gradient Boosting") and exposes a couple of small,
JSON-friendly helpers. It does **not** build the soft-voting ensemble, train, or
persist anything — those concerns belong to ``src/ensemble.py`` (the next commit)
and ``src/trainer.py`` respectively.

All three estimators are configured to consume the **sparse, non-negative CSR
matrix** produced by :class:`src.features.FeaturePipeline`:

* :class:`~sklearn.naive_bayes.MultinomialNB` accepts sparse input and *requires*
  non-negative values — which the feature pipeline guarantees (TF-IDF is ``>= 0``
  and the dense block is min-max scaled into ``[0, 1]`` with ``clip=True``).
* :class:`~sklearn.ensemble.RandomForestClassifier` and
  :class:`~sklearn.ensemble.GradientBoostingClassifier` both accept sparse input
  too.

Why the param differences matter (sklearn API gotchas):

* ``MultinomialNB`` has **no** ``class_weight`` and **no** ``random_state``
  parameter — passing either raises ``TypeError``. We therefore construct it bare
  (``fit_prior=True`` is the default and is what we want).
* ``GradientBoostingClassifier`` does **not** support ``class_weight`` either;
  class balancing for GB is done via ``sample_weight`` at fit time elsewhere if
  needed, never here.
* Only ``RandomForestClassifier`` gets ``class_weight="balanced"`` (logs are
  naturally imbalanced — many ``INFO``, few ``CRITICAL``).

Every estimator built here uses default sklearn estimators and is therefore
picklable, so the downstream :class:`~sklearn.ensemble.VotingClassifier` and the
model registry can serialise them without special handling.
"""

from __future__ import annotations

from typing import Optional

from sklearn.base import ClassifierMixin
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.naive_bayes import MultinomialNB

from src.config import Settings, get_config

#: Stable, ordered short names for the three base classifiers. This order is the
#: contract the soft-voting ensemble relies on (see :func:`build_base_classifiers`),
#: and ``ensemble_weights`` in :class:`src.config.Settings` is aligned to it
#: (``[1, 2, 3]`` -> nb, rf, gb).
CLASSIFIER_NAMES: tuple[str, ...] = ("nb", "rf", "gb")


def _resolve(cfg: Optional[Settings]) -> Settings:
    """Return ``cfg`` if provided, else the process-wide configuration.

    Centralises the ``cfg or get_config()`` fallback so every factory resolves
    configuration identically.
    """
    return cfg if cfg is not None else get_config()


# ---------------------------------------------------------------------------
# Individual classifier factories.
# ---------------------------------------------------------------------------


def make_naive_bayes(cfg: Optional[Settings] = None) -> MultinomialNB:
    """Build an unfitted :class:`~sklearn.naive_bayes.MultinomialNB`.

    ``MultinomialNB`` is the correct Naive Bayes variant for sparse TF-IDF
    features (unlike ``GaussianNB``, which expects dense data). It is constructed
    with all defaults — notably ``fit_prior=True`` — and is *not* given
    ``class_weight`` or ``random_state`` because the estimator has **no such
    parameters** (passing them raises ``TypeError``). It requires non-negative
    input, which :class:`src.features.FeaturePipeline` guarantees.

    Args:
        cfg: Optional configuration. Accepted for a uniform factory signature;
            ``MultinomialNB`` has no configurable hyperparameters we tune here.

    Returns:
        An unfitted ``MultinomialNB`` ready to be composed into the ensemble.
    """
    # cfg is intentionally unused; resolved for signature symmetry / future use.
    _resolve(cfg)
    return MultinomialNB()


def make_random_forest(cfg: Optional[Settings] = None) -> RandomForestClassifier:
    """Build an unfitted :class:`~sklearn.ensemble.RandomForestClassifier`.

    Configured with:

    * ``n_estimators = cfg.rf_n_estimators`` (default 100),
    * ``class_weight = "balanced"`` — logs are imbalanced (many ``INFO``, few
      ``CRITICAL``); balanced weighting counteracts that,
    * ``random_state = cfg.random_seed`` for reproducibility,
    * ``n_jobs = -1`` to use all cores when fitting.

    Accepts the sparse CSR feature matrix natively.

    Args:
        cfg: Optional configuration; :func:`src.config.get_config` is used when
            omitted.

    Returns:
        An unfitted ``RandomForestClassifier``.
    """
    settings = _resolve(cfg)
    return RandomForestClassifier(
        n_estimators=int(settings.rf_n_estimators),
        class_weight="balanced",
        random_state=int(settings.random_seed),
        n_jobs=-1,
    )


def make_gradient_boosting(
    cfg: Optional[Settings] = None,
) -> GradientBoostingClassifier:
    """Build an unfitted :class:`~sklearn.ensemble.GradientBoostingClassifier`.

    Configured with:

    * ``n_estimators = cfg.gb_n_estimators`` (default 100),
    * ``random_state = cfg.random_seed`` for reproducibility.

    Deliberately **not** given ``class_weight``: ``GradientBoostingClassifier``
    does not support that parameter (it raises ``TypeError``). If class balancing
    is needed for GB, it is applied via ``sample_weight`` at fit time elsewhere —
    never here. Accepts the sparse CSR feature matrix natively.

    Args:
        cfg: Optional configuration; :func:`src.config.get_config` is used when
            omitted.

    Returns:
        An unfitted ``GradientBoostingClassifier``.
    """
    settings = _resolve(cfg)
    return GradientBoostingClassifier(
        n_estimators=int(settings.gb_n_estimators),
        random_state=int(settings.random_seed),
    )


def build_base_classifiers(
    cfg: Optional[Settings] = None,
) -> dict[str, ClassifierMixin]:
    """Build the three base classifiers as an ordered ``{name: estimator}`` dict.

    The mapping is ordered ``nb`` -> ``rf`` -> ``gb`` (matching
    :data:`CLASSIFIER_NAMES`). Python ``dict`` preserves insertion order, so this
    is exactly the named mapping the soft-voting ensemble consumes — typically as
    ``VotingClassifier(estimators=list(d.items()), ...)`` — and it aligns 1:1 with
    ``cfg.ensemble_weights`` (``[1, 2, 3]``).

    Args:
        cfg: Optional configuration; :func:`src.config.get_config` is used when
            omitted. Resolved **once** here and threaded into each factory so all
            three estimators share a single, consistent configuration.

    Returns:
        An ordered ``dict`` with keys ``"nb"``, ``"rf"``, ``"gb"`` mapping to fresh
        unfitted estimator instances.
    """
    settings = _resolve(cfg)
    return {
        "nb": make_naive_bayes(settings),
        "rf": make_random_forest(settings),
        "gb": make_gradient_boosting(settings),
    }


# ---------------------------------------------------------------------------
# Thin, reusable helpers (work with any fitted estimator).
# ---------------------------------------------------------------------------


def predict_with_confidence(estimator, X) -> tuple[list, list[float]]:
    """Predict labels and per-sample confidence for a fitted ``estimator``.

    Confidence is the maximum class probability for each sample
    (``predict_proba(X).max(axis=1)``), which is the soft-voting confidence the
    spec asks to surface alongside each prediction. Works for any fitted estimator
    exposing ``predict`` and ``predict_proba`` (``MultinomialNB``,
    ``RandomForestClassifier``, ``GradientBoostingClassifier``, and the
    ``VotingClassifier`` ensemble).

    Both returned values are native Python (JSON-friendly): labels are converted
    to native types via ``numpy``'s ``.tolist()`` and confidences are plain
    ``float``s — safe to serialise directly into an API response.

    Args:
        estimator: A fitted estimator with ``predict`` and ``predict_proba``.
        X: Feature matrix (the sparse CSR matrix from
            :class:`src.features.FeaturePipeline`, or any array-like the estimator
            accepts).

    Returns:
        A ``(labels, confidences)`` tuple of equal length, where ``labels`` is a
        ``list`` of native-typed predictions and ``confidences`` is a
        ``list[float]`` of the max class probabilities.
    """
    labels = estimator.predict(X)
    proba = estimator.predict_proba(X)
    confidences = proba.max(axis=1)
    # ``.tolist()`` converts numpy scalars to native Python types (str/int/float),
    # keeping the result JSON-serialisable without further coercion.
    return labels.tolist(), [float(c) for c in confidences.tolist()]


def evaluate(estimator, X, y) -> float:
    """Return the accuracy of a fitted ``estimator`` on ``(X, y)``.

    A convenience wrapper over :func:`sklearn.metrics.accuracy_score` for quick
    scoring in tests and the trainer.

    Args:
        estimator: A fitted estimator exposing ``predict``.
        X: Feature matrix accepted by ``estimator.predict``.
        y: Ground-truth labels aligned with the rows of ``X``.

    Returns:
        The classification accuracy as a Python ``float`` in ``[0, 1]``.
    """
    return float(accuracy_score(y, estimator.predict(X)))
