"""Soft-voting weighted ensembles + the public ``LogClassifier`` façade (Commit 6).

This module ties the feature pipeline (Commit 4) and the three base classifiers
(Commit 5) into the user-facing classifier the rest of the system serves. It does
**two** things and nothing more:

1. :func:`build_ensemble` — assemble a single
   :class:`~sklearn.ensemble.VotingClassifier` over the three base estimators
   (``nb`` / ``rf`` / ``gb``) using **soft** voting with the configured
   ``ensemble_weights`` (``[1, 2, 3]`` — least trust to Naive Bayes, most to
   Gradient Boosting). Soft voting averages per-class probabilities
   (``predict_proba``) weighted by those weights; all three estimators expose
   ``predict_proba``, which is the precondition soft voting requires.
2. :class:`LogClassifier` — a small façade that owns a
   :class:`src.features.FeaturePipeline` plus **two independent** ensembles, one
   for ``severity`` and one for ``category`` (the spec asks for *separate*
   severity and category predictions). It turns a bare raw-text log line into a
   structured ``{severity, category, confidence, ...}`` dict, classifies batches
   efficiently (a single transform + a single predict per axis), and persists /
   restores itself with :mod:`joblib`.

The whole object graph is picklable end-to-end: the feature pipeline references
the importable module-level :func:`src.preprocess.preprocess` (not a lambda) and
every estimator is a stock sklearn estimator, so :meth:`LogClassifier.save` /
:meth:`LogClassifier.load` just dump and load the three artifacts.

What is **out of scope** for this commit (and lives elsewhere): the trainer,
cross-validation, the model registry, the FastAPI app, multi-service hierarchical
classification, the adaptive retraining loop, and A/B serving.
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional, Sequence, Union

import joblib
from sklearn.ensemble import VotingClassifier

from src.classifiers import build_base_classifiers, predict_with_confidence
from src.config import Settings, get_config
from src.features import FeaturePipeline

#: Artifact filenames written under the model directory by
#: :meth:`LogClassifier.save` and read back by :meth:`LogClassifier.load`.
_FEATURE_PIPELINE_FILE = "feature_pipeline.joblib"
_SEVERITY_ENSEMBLE_FILE = "severity_ensemble.joblib"
_CATEGORY_ENSEMBLE_FILE = "category_ensemble.joblib"
_META_FILE = "meta.json"

#: A single record may be supplied as a full dict or as a bare ``str`` message.
RecordOrText = Union[str, dict[str, Any]]


def _resolve(cfg: Optional[Settings]) -> Settings:
    """Return ``cfg`` if provided, else the process-wide configuration."""
    return cfg if cfg is not None else get_config()


def build_ensemble(cfg: Optional[Settings] = None) -> VotingClassifier:
    """Build an **unfitted** soft-voting :class:`~sklearn.ensemble.VotingClassifier`.

    The ensemble is constructed directly from
    :func:`src.classifiers.build_base_classifiers`, so the estimator order and
    short names (``nb`` -> ``rf`` -> ``gb``, see
    :data:`src.classifiers.CLASSIFIER_NAMES`) are exactly the order that
    ``cfg.ensemble_weights`` (``[1, 2, 3]``) is aligned to.

    Construction (the contract downstream code and tests rely on):

    * ``estimators = list(build_base_classifiers(cfg).items())`` — ordered
      ``[("nb", NB), ("rf", RF), ("gb", GB)]``.
    * ``voting = "soft"`` — average **probabilities**, not hard label votes. Soft
      voting requires every estimator to expose ``predict_proba``; ``MultinomialNB``,
      ``RandomForestClassifier`` and ``GradientBoostingClassifier`` all do.
    * ``weights = list(cfg.ensemble_weights)`` — per-estimator weight on the
      averaged probabilities (``[1, 2, 3]`` by default).
    * ``n_jobs = -1`` — fit/predict the members in parallel across all cores.

    Args:
        cfg: Optional configuration; :func:`src.config.get_config` is used when
            omitted. Resolved once and threaded into ``build_base_classifiers`` so
            the members and the weights derive from a single, consistent config.

    Returns:
        An unfitted ``VotingClassifier`` ready to ``fit`` on a feature matrix and
        a label vector.
    """
    settings = _resolve(cfg)
    base = build_base_classifiers(settings)
    return VotingClassifier(
        estimators=list(base.items()),
        voting="soft",
        weights=list(settings.ensemble_weights),
        n_jobs=-1,
    )


def _as_record(item: RecordOrText) -> dict[str, Any]:
    """Normalise a batch item to a record dict the feature pipeline understands.

    Accepts either a bare ``str`` message (wrapped into a minimal record) or a
    full / partial record dict (only ``raw_log`` and ``timestamp`` are consulted
    downstream; all other keys are ignored by the feature pipeline). The returned
    dict always carries every schema key so the engineered frame stays rectangular.
    """
    if isinstance(item, str):
        raw_log = item
        timestamp = ""
    elif isinstance(item, dict):
        raw_log = item.get("raw_log", "")
        # Tolerate ``None`` timestamps by coercing to the neutral empty string the
        # feature pipeline treats as "no temporal info".
        timestamp = item.get("timestamp") or ""
    else:  # pragma: no cover - defensive; callers pass str or dict
        raise TypeError(
            f"classify input must be a str or dict, got {type(item).__name__}"
        )
    return {
        "raw_log": raw_log if raw_log is not None else "",
        "timestamp": timestamp,
        "service": "",
        "severity": "",
        "category": "",
    }


def _result_dict(
    severity: Any,
    category: Any,
    severity_conf: float,
    category_conf: float,
) -> dict[str, Any]:
    """Assemble the canonical, JSON-safe classification result dict.

    ``confidence`` is the mean of the two per-axis soft-voting confidences (the
    overall trust in the combined ``severity``/``category`` answer), rounded to 4
    decimals. All labels are native ``str`` and all numbers are native ``float``.
    """
    sev_conf = float(severity_conf)
    cat_conf = float(category_conf)
    overall = round((sev_conf + cat_conf) / 2.0, 4)
    return {
        "severity": str(severity),
        "category": str(category),
        "confidence": overall,
        "severity_confidence": sev_conf,
        "category_confidence": cat_conf,
    }


class LogClassifier:
    """Raw-text → ``{severity, category, confidence, ...}`` classification façade.

    Owns one :class:`src.features.FeaturePipeline` and **two** soft-voting
    ensembles — :attr:`severity_ensemble` and :attr:`category_ensemble` — fitted on
    the *same* feature matrix but against the severity and category label vectors
    respectively. After :meth:`fit` (or :meth:`load`) the instance can classify a
    single bare message (:meth:`classify`), a batch (:meth:`classify_batch`), or a
    full record dict (:meth:`classify_record`).

    Both ensembles share a single fitted feature representation, so a batch
    classification costs one ``transform`` plus one ``predict``/``predict_proba``
    per axis — no per-record Python loop over the model.

    Attributes:
        cfg: The :class:`src.config.Settings` driving the pipeline and ensembles.
        features: The (fitted after :meth:`fit`) feature pipeline.
        severity_ensemble: Soft-voting ensemble predicting the severity label.
        category_ensemble: Soft-voting ensemble predicting the category label.
        is_fitted: ``True`` once :meth:`fit`/:meth:`load` has completed.
        severity_classes_: Sorted severity classes the ensemble can emit
            (``None`` until fitted).
        category_classes_: Sorted category classes the ensemble can emit
            (``None`` until fitted).
    """

    def __init__(self, cfg: Optional[Settings] = None) -> None:
        """Create an unfitted classifier wiring a pipeline and two fresh ensembles.

        Args:
            cfg: Optional configuration; :func:`src.config.get_config` is used when
                omitted. Resolved once and reused for the pipeline and both
                ensembles so they share one configuration.
        """
        self.cfg: Settings = _resolve(cfg)
        self.features: FeaturePipeline = FeaturePipeline(self.cfg)
        self.severity_ensemble: VotingClassifier = build_ensemble(self.cfg)
        self.category_ensemble: VotingClassifier = build_ensemble(self.cfg)
        self.is_fitted: bool = False
        self.severity_classes_: Optional[list[str]] = None
        self.category_classes_: Optional[list[str]] = None

    # -- internal ----------------------------------------------------------

    def _ensure_fitted(self) -> None:
        """Raise a clear error if the classifier has not been trained/loaded yet."""
        if not self.is_fitted:
            raise RuntimeError(
                "LogClassifier is not fitted yet; call fit(records) or "
                "LogClassifier.load(dirpath) before classify()/classify_batch()"
            )

    # -- training ----------------------------------------------------------

    def fit(self, records: Sequence[dict[str, Any]]) -> "LogClassifier":
        """Fit the feature pipeline and both ensembles on labeled ``records``.

        Steps:

        1. ``X = self.features.fit_transform(records)`` — learn the TF-IDF
           vocabulary + dense scaler and produce the training matrix once.
        2. Fit :attr:`severity_ensemble` on ``(X, [r["severity"] for r in records])``.
        3. Fit :attr:`category_ensemble` on ``(X, [r["category"] for r in records])``.
        4. Cache the fitted class lists and flip :attr:`is_fitted`.

        Args:
            records: Non-empty sequence of labeled log dicts (each must carry
                ``raw_log``, ``severity`` and ``category``; ``timestamp`` is used
                for temporal features when present).

        Returns:
            ``self`` (for chaining).

        Raises:
            ValueError: if ``records`` is empty (propagated from the feature
                pipeline / label extraction).
        """
        if not records:
            raise ValueError("fit() requires at least one labeled record")

        X = self.features.fit_transform(records)
        y_severity = [r["severity"] for r in records]
        y_category = [r["category"] for r in records]

        self.severity_ensemble.fit(X, y_severity)
        self.category_ensemble.fit(X, y_category)

        # ``classes_`` are numpy arrays of (here) str labels; expose them as plain
        # sorted Python lists for JSON-friendly introspection and metadata.
        self.severity_classes_ = [str(c) for c in self.severity_ensemble.classes_]
        self.category_classes_ = [str(c) for c in self.category_ensemble.classes_]
        self.is_fitted = True
        return self

    # -- inference ---------------------------------------------------------

    def classify(self, raw_log: str, timestamp: Optional[str] = None) -> dict[str, Any]:
        """Classify a single raw log line into severity + category + confidence.

        Works on a **bare** message with no ``[LEVEL]`` bracket, no service word and
        no timestamp (e.g. ``"Database connection failed with timeout error"``): the
        feature pipeline derives every feature from the text alone and fixes the
        width, so a one-row matrix is produced and fed to both ensembles.

        Args:
            raw_log: The raw log line / message to classify.
            timestamp: Optional ISO-8601 timestamp for temporal features; omitted /
                ``None`` falls back to neutral temporal values.

        Returns:
            A dict with exactly these keys::

                {
                  "severity": <str>,               # severity_ensemble.predict
                  "category": <str>,               # category_ensemble.predict
                  "confidence": <float>,           # mean of the two, rounded to 4dp
                  "severity_confidence": <float>,  # severity max predict_proba
                  "category_confidence": <float>,  # category max predict_proba
                }

            All labels are native ``str`` and all numbers native ``float`` (JSON-safe).

        Raises:
            RuntimeError: if called before :meth:`fit`/:meth:`load`.
        """
        self._ensure_fitted()
        record = _as_record({"raw_log": raw_log, "timestamp": timestamp})
        X = self.features.transform([record])  # 1-row sparse matrix

        # ``predict_with_confidence`` returns (labels, confidences) as native lists.
        sev_labels, sev_conf = predict_with_confidence(self.severity_ensemble, X)
        cat_labels, cat_conf = predict_with_confidence(self.category_ensemble, X)

        return _result_dict(sev_labels[0], cat_labels[0], sev_conf[0], cat_conf[0])

    def classify_batch(self, records: Sequence[RecordOrText]) -> list[dict[str, Any]]:
        """Classify many logs at once, vectorized over a single feature matrix.

        Each item may be a full/partial record dict (``raw_log``/``timestamp``) or a
        bare ``str`` message — both are accepted and normalised. The matrix is built
        **once** for the whole batch and each ensemble runs a single
        ``predict``/``predict_proba``, so this is far cheaper than calling
        :meth:`classify` in a loop.

        Args:
            records: A sequence of record dicts and/or raw strings.

        Returns:
            A list of result dicts (same shape as :meth:`classify`), one per input,
            in input order. An empty input yields an empty list.

        Raises:
            RuntimeError: if called before :meth:`fit`/:meth:`load`.
        """
        self._ensure_fitted()
        if not records:
            return []

        normalised = [_as_record(item) for item in records]
        X = self.features.transform(normalised)

        sev_labels, sev_conf = predict_with_confidence(self.severity_ensemble, X)
        cat_labels, cat_conf = predict_with_confidence(self.category_ensemble, X)

        return [
            _result_dict(sev_labels[i], cat_labels[i], sev_conf[i], cat_conf[i])
            for i in range(len(normalised))
        ]

    def classify_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Classify a full record dict (convenience over :meth:`classify`).

        Reads ``raw_log`` and ``timestamp`` from ``record`` (any ground-truth label
        keys present are ignored) and returns the standard result dict.

        Args:
            record: A log record dict containing at least ``raw_log``.

        Returns:
            The standard classification result dict (see :meth:`classify`).

        Raises:
            RuntimeError: if called before :meth:`fit`/:meth:`load`.
        """
        self._ensure_fitted()
        rec = _as_record(record)
        return self.classify(rec["raw_log"], rec["timestamp"] or None)

    # -- introspection -----------------------------------------------------

    def feature_importance(self, top_n: int = 20) -> list[dict[str, Any]]:
        """Return the top engineered features by RandomForest importance.

        Reads ``feature_importances_`` off the **severity** ensemble's RandomForest
        member (``severity_ensemble.named_estimators_["rf"]``) and aligns those
        scores 1:1 with the feature pipeline's ordered :attr:`FeaturePipeline.feature_names_`.
        The pairs are sorted by importance descending and the first ``top_n`` are
        returned as plain JSON-safe dicts — exactly the shape the dashboard's
        feature-importance chart and ``GET /feature-importance`` consume.

        This is deliberately defensive: the method **never raises** for a model that
        is not yet fitted, is missing the RF member, exposes no importances, or whose
        importance vector length disagrees with the feature-name list. In any of those
        cases it simply returns an empty list, so the endpoint can degrade gracefully.

        Args:
            top_n: Maximum number of (feature, importance) pairs to return. Values
                ``<= 0`` yield an empty list.

        Returns:
            A list of ``{"name": <feature>, "importance": <float>}`` dicts, sorted by
            ``importance`` descending, of length ``min(top_n, n_features)`` (or empty
            when importances are unavailable).
        """
        if top_n <= 0 or not self.is_fitted:
            return []

        # The RandomForest member is the only base estimator exposing
        # ``feature_importances_``; pull it from the fitted severity ensemble.
        named = getattr(self.severity_ensemble, "named_estimators_", None)
        if not named or "rf" not in named:
            return []
        rf = named["rf"]
        importances = getattr(rf, "feature_importances_", None)
        if importances is None:
            return []

        names = getattr(self.features, "feature_names_", None)
        if not names:
            return []

        # Guard against any length mismatch (e.g. a partially-restored artifact):
        # zip to the shorter length so we never index past either sequence.
        n = min(len(names), len(importances))
        if n == 0:
            return []

        pairs = [
            {"name": str(names[i]), "importance": float(importances[i])}
            for i in range(n)
        ]
        pairs.sort(key=lambda d: d["importance"], reverse=True)
        return pairs[:top_n]

    # -- persistence -------------------------------------------------------

    def save(self, dirpath: str) -> None:
        """Persist the fitted classifier's three artifacts (+ metadata) to a directory.

        Writes, under ``dirpath`` (created if missing):

        * ``feature_pipeline.joblib`` — the fitted :class:`FeaturePipeline`.
        * ``severity_ensemble.joblib`` — the fitted severity ``VotingClassifier``.
        * ``category_ensemble.joblib`` — the fitted category ``VotingClassifier``.
        * ``meta.json`` — small human-readable metadata (class lists, sklearn/joblib
          versions when available).

        Args:
            dirpath: Destination directory.

        Raises:
            RuntimeError: if called before :meth:`fit`/:meth:`load` (nothing to save).
        """
        self._ensure_fitted()
        os.makedirs(dirpath, exist_ok=True)

        joblib.dump(self.features, os.path.join(dirpath, _FEATURE_PIPELINE_FILE))
        joblib.dump(
            self.severity_ensemble, os.path.join(dirpath, _SEVERITY_ENSEMBLE_FILE)
        )
        joblib.dump(
            self.category_ensemble, os.path.join(dirpath, _CATEGORY_ENSEMBLE_FILE)
        )

        meta: dict[str, Any] = {
            "severity_classes": list(self.severity_classes_ or []),
            "category_classes": list(self.category_classes_ or []),
            "ensemble_weights": list(self.cfg.ensemble_weights),
            "voting": "soft",
        }
        # Record library versions for diagnostics; never fail the save if a version
        # string is somehow unavailable.
        try:  # pragma: no cover - trivial best-effort metadata
            import sklearn

            meta["sklearn_version"] = sklearn.__version__
        except Exception:
            pass
        try:  # pragma: no cover - trivial best-effort metadata
            meta["joblib_version"] = joblib.__version__
        except Exception:
            pass

        with open(os.path.join(dirpath, _META_FILE), "w", encoding="utf-8") as fh:
            json.dump(meta, fh, indent=2, sort_keys=True)

    @classmethod
    def load(cls, dirpath: str, cfg: Optional[Settings] = None) -> "LogClassifier":
        """Reconstruct a fitted :class:`LogClassifier` from a :meth:`save` directory.

        Loads the three joblib artifacts, attaches them to a fresh instance, and
        restores the cached class lists from ``meta.json`` (falling back to the
        ensembles' own ``classes_`` if the metadata file is missing/unreadable).

        Args:
            dirpath: Directory previously written by :meth:`save`.
            cfg: Optional configuration for the rebuilt instance;
                :func:`src.config.get_config` is used when omitted. (The persisted
                fitted artifacts are authoritative — ``cfg`` only seeds defaults.)

        Returns:
            A fitted ``LogClassifier`` ready to classify.

        Raises:
            FileNotFoundError: if any required artifact is missing from ``dirpath``.
            TypeError: if the feature-pipeline artifact is not a ``FeaturePipeline``.
        """
        feature_path = os.path.join(dirpath, _FEATURE_PIPELINE_FILE)
        severity_path = os.path.join(dirpath, _SEVERITY_ENSEMBLE_FILE)
        category_path = os.path.join(dirpath, _CATEGORY_ENSEMBLE_FILE)
        for path in (feature_path, severity_path, category_path):
            if not os.path.isfile(path):
                raise FileNotFoundError(f"missing model artifact: {path}")

        instance = cls(cfg)
        instance.features = FeaturePipeline.load(feature_path)
        instance.severity_ensemble = joblib.load(severity_path)
        instance.category_ensemble = joblib.load(category_path)

        # Prefer the persisted metadata; fall back to the live estimators' classes_.
        meta: dict[str, Any] = {}
        meta_path = os.path.join(dirpath, _META_FILE)
        if os.path.isfile(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as fh:
                    loaded = json.load(fh)
                if isinstance(loaded, dict):
                    meta = loaded
            except (OSError, ValueError):
                meta = {}

        instance.severity_classes_ = [
            str(c)
            for c in meta.get(
                "severity_classes", list(instance.severity_ensemble.classes_)
            )
        ]
        instance.category_classes_ = [
            str(c)
            for c in meta.get(
                "category_classes", list(instance.category_ensemble.classes_)
            )
        ]
        instance.is_fitted = True
        return instance
