"""Hierarchical multi-service classifier + cross-service anomaly voting (Commit 11).

This module implements **Feature Area A** of the spec: handle logs from the three
distinct services (``web`` / ``database`` / ``cache``), train a *separate* model
per service, classify **hierarchically** (first decide the service, then apply
that service's own severity model), and surface a cross-service **anomaly score**
derived from ensemble voting.

It deliberately does **not** re-implement features or ensembles — it composes the
existing building blocks:

* one :class:`src.features.FeaturePipeline` (shared, fit on *all* records) so the
  whole hierarchy speaks a single, frozen feature representation;
* :func:`src.ensemble.build_ensemble` for every sub-model (the same soft-voting
  ``VotingClassifier`` the base :class:`src.ensemble.LogClassifier` uses).

The object graph is:

    MultiServiceClassifier
    ├── features            : FeaturePipeline                 (shared)
    ├── service_ensemble    : VotingClassifier  -> service    (web/database/cache)
    ├── severity_by_service : {service: VotingClassifier}     -> severity (per svc)
    └── category_ensemble   : VotingClassifier  -> category   (global; see below)

Why severity is per-service but category is global
--------------------------------------------------
The spec calls out *service-specific severity* models ("first determine service
type, then apply a service-specific severity model"). It does **not** ask for a
service-specific category model, and categories (``SYSTEM`` / ``AUTH`` / ...) are
shared semantics across services, so a single global category ensemble is the
faithful, lower-variance choice. Splitting category per-service would also starve
some (service, category) cells of training data.

Degenerate per-service severity
--------------------------------
If a particular service's training subset happens to contain only **one** severity
class, a ``VotingClassifier`` fit on it would only ever predict that one label
(useless and brittle). For any such service we transparently fall back to a
**global** severity ensemble (fit on all records) for that service's slot, so the
hierarchy still routes through a real, multi-class severity model.

Cross-service anomaly voting
----------------------------
At inference time **every** per-service severity model votes on the single record.
``severity_agreement`` is the fraction of services whose top severity equals the
modal severity. The anomaly score combines two independent "this log is weird"
signals — an ambiguous service prediction *and/or* disagreement among the
per-service severity models::

    anomaly_score = clip(0.5 * (1 - service_confidence)
                         + 0.5 * (1 - severity_agreement), 0, 1)

(rounded to 4dp). It is high when the service is uncertain **and/or** the
per-service severity models disagree about the record — exactly the cross-service
ensemble-voting anomaly signal the spec asks for. It is always in ``[0, 1]``.

Everything is picklable end-to-end (the feature pipeline references the importable
:func:`src.preprocess.preprocess`, every estimator is stock sklearn), so
:meth:`MultiServiceClassifier.save` / :meth:`load` just dump and load the
artifacts plus a small ``meta.json``.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from typing import Any, Optional, Sequence, Union

import joblib
from sklearn.ensemble import VotingClassifier

from src.classifiers import predict_with_confidence
from src.config import Settings, get_config
from src.ensemble import build_ensemble
from src.features import FeaturePipeline
from src.log_generator import SERVICES

#: A single classify input may be a full/partial record dict or a bare message.
RecordOrText = Union[str, dict[str, Any]]

#: Artifact filenames written under the model directory by :meth:`save` and read
#: back by :meth:`load`. Per-service severity ensembles are written as
#: ``severity_<service>.joblib``.
_FEATURE_PIPELINE_FILE = "feature_pipeline.joblib"
_SERVICE_ENSEMBLE_FILE = "service_ensemble.joblib"
_CATEGORY_ENSEMBLE_FILE = "category_ensemble.joblib"
_GLOBAL_SEVERITY_FILE = "severity_global.joblib"
_META_FILE = "meta.json"


def _severity_artifact_name(service: str) -> str:
    """Filename for a per-service severity ensemble (``severity_web.joblib``)."""
    return f"severity_{service}.joblib"


def _resolve(cfg: Optional[Settings]) -> Settings:
    """Return ``cfg`` if provided, else the process-wide configuration."""
    return cfg if cfg is not None else get_config()


def _as_record(item: RecordOrText) -> dict[str, Any]:
    """Normalise a classify input to a record dict the feature pipeline understands.

    Accepts either a bare ``str`` message (wrapped into a minimal record) or a full
    / partial record dict. Only ``raw_log`` and ``timestamp`` are consulted by the
    feature pipeline; any ground-truth label keys present are ignored so features
    are identical at train and inference time. The returned dict always carries the
    canonical schema keys so the engineered frame stays rectangular.
    """
    if isinstance(item, str):
        raw_log: Any = item
        timestamp: Any = ""
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


class MultiServiceClassifier:
    """Hierarchical service→severity classifier with cross-service anomaly voting.

    Owns one shared :class:`src.features.FeaturePipeline` plus several soft-voting
    ensembles:

    * :attr:`service_ensemble` predicts the *service* (``web`` / ``database`` /
      ``cache``);
    * :attr:`severity_by_service` maps each service to its **own** severity
      ensemble (the hierarchy: service first, then that service's severity model);
    * :attr:`category_ensemble` predicts the (global) category.

    After :meth:`fit` (or :meth:`load`) the instance can classify a single record
    (:meth:`classify`), a batch (:meth:`classify_batch`), and persist / restore
    itself (:meth:`save` / :meth:`load`).

    Attributes:
        cfg: The :class:`src.config.Settings` driving the pipeline and ensembles.
        features: The (fitted after :meth:`fit`) shared feature pipeline.
        service_ensemble: Soft-voting ensemble predicting the service label.
        severity_by_service: ``{service: VotingClassifier}`` — a severity ensemble
            per service (a degenerate single-class subset falls back to the global
            severity ensemble for that service).
        category_ensemble: Global soft-voting ensemble predicting the category.
        global_severity_ensemble: Severity ensemble fit on *all* records; used as
            the fallback for any service whose subset is single-class.
        is_fitted: ``True`` once :meth:`fit`/:meth:`load` has completed.
        services_: Sorted list of services the service ensemble can emit.
        service_classes_: Same as :attr:`services_` (the service ensemble classes).
        severity_classes_by_service_: ``{service: [severity, ...]}`` — the severity
            classes each per-service model can emit.
        category_classes_: Sorted category classes.
    """

    def __init__(self, cfg: Optional[Settings] = None) -> None:
        """Create an unfitted multi-service classifier.

        Args:
            cfg: Optional configuration; :func:`src.config.get_config` is used when
                omitted. Resolved once and reused for the pipeline and every
                ensemble so they share one configuration.
        """
        self.cfg: Settings = _resolve(cfg)
        self.features: FeaturePipeline = FeaturePipeline(self.cfg)
        self.service_ensemble: VotingClassifier = build_ensemble(self.cfg)
        self.category_ensemble: VotingClassifier = build_ensemble(self.cfg)
        self.global_severity_ensemble: VotingClassifier = build_ensemble(self.cfg)
        self.severity_by_service: dict[str, VotingClassifier] = {}

        self.is_fitted: bool = False
        self.services_: Optional[list[str]] = None
        self.service_classes_: Optional[list[str]] = None
        self.severity_classes_by_service_: Optional[dict[str, list[str]]] = None
        self.category_classes_: Optional[list[str]] = None

    # -- internal ----------------------------------------------------------

    def _ensure_fitted(self) -> None:
        """Raise a clear error if the classifier has not been trained/loaded yet."""
        if not self.is_fitted:
            raise RuntimeError(
                "MultiServiceClassifier is not fitted yet; call fit(records) or "
                "MultiServiceClassifier.load(dirpath) before classify()/classify_batch()"
            )

    @staticmethod
    def _classes_of(estimator: VotingClassifier) -> list[str]:
        """Return a fitted estimator's class labels as plain ``str`` list."""
        return [str(c) for c in estimator.classes_]

    # -- training ----------------------------------------------------------

    def fit(self, records: Sequence[dict[str, Any]]) -> "MultiServiceClassifier":
        """Fit the shared pipeline and the full service→severity hierarchy.

        Steps:

        1. ``X = self.features.fit_transform(records)`` — learn the TF-IDF
           vocabulary + dense scaler and produce the training matrix **once**; every
           sub-model is fit on (a row-slice of) this same matrix.
        2. Fit :attr:`service_ensemble` on ``(X, services)`` (predicts the service).
        3. Fit :attr:`global_severity_ensemble` on ``(X, severities)`` — the
           fallback used for any degenerate per-service subset.
        4. For each service in :data:`src.log_generator.SERVICES`, slice that
           service's rows and fit :attr:`severity_by_service`\\ ``[service]`` on its
           ``(X_subset, severity_subset)``. A subset with **fewer than two** severity
           classes (or no rows) falls back to :attr:`global_severity_ensemble`.
        5. Fit the global :attr:`category_ensemble` on ``(X, categories)``.
        6. Cache the class lists and flip :attr:`is_fitted`.

        Args:
            records: Non-empty sequence of labeled log dicts (each must carry
                ``raw_log``, ``service``, ``severity`` and ``category``;
                ``timestamp`` is used for temporal features when present).

        Returns:
            ``self`` (for chaining).

        Raises:
            ValueError: if ``records`` is empty.
        """
        if not records:
            raise ValueError("fit() requires at least one labeled record")

        # 1) One shared feature representation for the whole hierarchy.
        X = self.features.fit_transform(records)
        services = [str(r["service"]) for r in records]
        severities = [str(r["severity"]) for r in records]
        categories = [str(r["category"]) for r in records]

        # 2) Top of the hierarchy: which service produced this log.
        self.service_ensemble.fit(X, services)

        # 3) Global severity ensemble (also the per-service fallback for any
        #    service whose own subset is single-class).
        self.global_severity_ensemble.fit(X, severities)

        # 4) Per-service severity ensembles. Iterate the canonical service list so
        #    the mapping is stable regardless of label ordering in ``records``.
        self.severity_by_service = {}
        severity_classes_by_service: dict[str, list[str]] = {}
        for service in SERVICES:
            rows = [i for i, s in enumerate(services) if s == service]
            subset_severities = [severities[i] for i in rows]
            distinct = set(subset_severities)
            if len(rows) >= 2 and len(distinct) >= 2:
                # Healthy multi-class subset -> a real per-service severity model.
                model = build_ensemble(self.cfg)
                # Row-slice the shared sparse matrix (CSR supports fancy row indexing).
                model.fit(X[rows], subset_severities)
                self.severity_by_service[service] = model
                severity_classes_by_service[service] = self._classes_of(model)
            else:
                # Degenerate (single-class or empty) -> use the global severity model
                # so this service still routes through a multi-class severity model.
                self.severity_by_service[service] = self.global_severity_ensemble
                severity_classes_by_service[service] = self._classes_of(
                    self.global_severity_ensemble
                )

        # 5) Global category ensemble (category is shared across services).
        self.category_ensemble.fit(X, categories)

        # 6) Cache introspection metadata as plain Python (JSON-friendly).
        self.service_classes_ = self._classes_of(self.service_ensemble)
        self.services_ = list(self.service_classes_)
        self.severity_classes_by_service_ = severity_classes_by_service
        self.category_classes_ = self._classes_of(self.category_ensemble)
        self.is_fitted = True
        return self

    # -- inference ---------------------------------------------------------

    def _severity_model_for(self, service: str) -> VotingClassifier:
        """Return the severity ensemble to apply for a predicted ``service``.

        Falls back to the global severity ensemble for an unknown service (should
        not happen for in-vocabulary predictions, but keeps inference total).
        """
        return self.severity_by_service.get(service, self.global_severity_ensemble)

    @staticmethod
    def _anomaly_score(service_confidence: float, severity_agreement: float) -> float:
        """Combine service ambiguity + per-service severity disagreement into [0, 1].

        ``anomaly_score = clip(0.5 * (1 - service_confidence)
                               + 0.5 * (1 - severity_agreement), 0, 1)`` rounded to
        4dp. High when the service is uncertain **and/or** the per-service severity
        models disagree about the record.
        """
        raw = 0.5 * (1.0 - service_confidence) + 0.5 * (1.0 - severity_agreement)
        clipped = min(1.0, max(0.0, raw))
        return round(clipped, 4)

    def _result(
        self,
        service: str,
        service_conf: float,
        severity: str,
        severity_conf: float,
        category: str,
        category_conf: float,
        severity_agreement: float,
    ) -> dict[str, Any]:
        """Assemble the canonical 8-key, JSON-safe classification result dict.

        ``confidence`` is the mean of the three per-axis confidences (rounded 4dp);
        ``anomaly_score`` is :meth:`_anomaly_score`. All labels are native ``str``,
        all numbers native ``float``.
        """
        svc_conf = float(service_conf)
        sev_conf = float(severity_conf)
        cat_conf = float(category_conf)
        overall = round((svc_conf + sev_conf + cat_conf) / 3.0, 4)
        return {
            "service": str(service),
            "service_confidence": svc_conf,
            "severity": str(severity),
            "severity_confidence": sev_conf,
            "category": str(category),
            "category_confidence": cat_conf,
            "confidence": overall,
            "anomaly_score": self._anomaly_score(svc_conf, severity_agreement),
        }

    def classify(
        self, raw_log: RecordOrText, timestamp: Optional[str] = None
    ) -> dict[str, Any]:
        """Classify a single log HIERARCHICALLY (service → its severity model).

        Flow:

        1. Transform the single record into a one-row matrix ``x``.
        2. Predict ``service`` + ``service_confidence`` from
           :attr:`service_ensemble`.
        3. Apply the **service-specific** severity ensemble
           (:attr:`severity_by_service`\\ ``[service]``) to get ``severity`` +
           ``severity_confidence`` — the hierarchy routes through the predicted
           service's own model.
        4. Predict ``category`` + ``category_confidence`` from the global
           :attr:`category_ensemble`.
        5. **Cross-service anomaly voting**: run *every* per-service severity model
           on ``x``, take each one's top severity, and set ``severity_agreement`` to
           the fraction of services predicting the modal severity. Derive
           ``anomaly_score`` from service ambiguity and this disagreement.

        Args:
            raw_log: The raw log line to classify (a bare ``str``), or a record dict
                carrying ``raw_log`` / ``timestamp``.
            timestamp: Optional ISO-8601 timestamp for temporal features (ignored
                when ``raw_log`` is itself a dict that carries its own timestamp).

        Returns:
            A dict with exactly these keys (native types, JSON-safe)::

                {
                  "service": <str>,                # service_ensemble.predict
                  "service_confidence": <float>,   # service max predict_proba
                  "severity": <str>,               # per-service severity model
                  "severity_confidence": <float>,  # severity max predict_proba
                  "category": <str>,               # global category_ensemble
                  "category_confidence": <float>,  # category max predict_proba
                  "confidence": <float>,           # mean of the 3 above, 4dp
                  "anomaly_score": <float>,        # cross-service vote, [0,1], 4dp
                }

        Raises:
            RuntimeError: if called before :meth:`fit`/:meth:`load`.
        """
        self._ensure_fitted()
        if isinstance(raw_log, dict):
            record = _as_record(raw_log)
        else:
            record = _as_record({"raw_log": raw_log, "timestamp": timestamp})
        X = self.features.transform([record])  # 1-row sparse matrix

        # 2) Service (top of the hierarchy).
        svc_labels, svc_conf = predict_with_confidence(self.service_ensemble, X)
        service = str(svc_labels[0])

        # 3) Service-specific severity (the hierarchy step).
        severity_model = self._severity_model_for(service)
        sev_labels, sev_conf = predict_with_confidence(severity_model, X)
        severity = str(sev_labels[0])

        # 4) Global category.
        cat_labels, cat_conf = predict_with_confidence(self.category_ensemble, X)
        category = str(cat_labels[0])

        # 5) Cross-service anomaly voting over EVERY per-service severity model.
        agreement = self._severity_agreement(X)

        return self._result(
            service,
            svc_conf[0],
            severity,
            sev_conf[0],
            category,
            cat_conf[0],
            agreement,
        )

    def _severity_agreement(self, X) -> float:
        """Fraction of per-service severity models agreeing on the modal severity.

        Runs each distinct per-service severity ensemble on the single-row matrix
        ``X`` and tallies its top severity; returns ``count(modal) / num_services``.
        With three services this is one of ``{1/3, 2/3, 1.0}`` — full agreement (all
        three predict the same severity) yields ``1.0`` (no anomaly from this term).
        """
        votes: list[str] = []
        for service in SERVICES:
            model = self._severity_model_for(service)
            labels, _ = predict_with_confidence(model, X)
            votes.append(str(labels[0]))
        if not votes:  # pragma: no cover - SERVICES is never empty
            return 1.0
        modal_count = Counter(votes).most_common(1)[0][1]
        return modal_count / len(votes)

    def classify_batch(
        self, records: Sequence[RecordOrText]
    ) -> list[dict[str, Any]]:
        """Classify many logs at once, vectorized over a single feature matrix.

        Each item may be a full/partial record dict (``raw_log`` / ``timestamp``) or
        a bare ``str`` message — both are accepted and normalised. The matrix is
        built **once** for the whole batch and each ensemble runs a single
        ``predict``/``predict_proba`` over all rows (including every per-service
        severity model for the cross-service anomaly vote), so this is far cheaper
        than calling :meth:`classify` in a loop.

        Args:
            records: A sequence of record dicts and/or raw strings.

        Returns:
            A list of result dicts (same 8-key shape as :meth:`classify`), one per
            input, in input order. An empty input yields an empty list.

        Raises:
            RuntimeError: if called before :meth:`fit`/:meth:`load`.
        """
        self._ensure_fitted()
        if not records:
            return []

        normalised = [_as_record(item) for item in records]
        X = self.features.transform(normalised)
        n = len(normalised)

        # Service + category once for the whole batch.
        svc_labels, svc_conf = predict_with_confidence(self.service_ensemble, X)
        cat_labels, cat_conf = predict_with_confidence(self.category_ensemble, X)

        # Every distinct per-service severity model predicts the whole batch once.
        # ``per_service_preds[service]`` is a (labels, confidences) pair aligned to
        # the batch rows; used both for routing (step 3) and the anomaly vote.
        per_service_preds: dict[str, tuple[list, list[float]]] = {}
        for service in SERVICES:
            model = self._severity_model_for(service)
            per_service_preds[service] = predict_with_confidence(model, X)

        results: list[dict[str, Any]] = []
        for i in range(n):
            service = str(svc_labels[i])
            # Route severity through the PREDICTED service's model for this row.
            routing_service = service if service in per_service_preds else SERVICES[0]
            sev_labels_i, sev_conf_i = per_service_preds[routing_service]
            severity = str(sev_labels_i[i])

            # Cross-service agreement for row i across all per-service models.
            votes = [str(per_service_preds[s][0][i]) for s in SERVICES]
            modal_count = Counter(votes).most_common(1)[0][1]
            agreement = modal_count / len(votes)

            results.append(
                self._result(
                    service,
                    svc_conf[i],
                    severity,
                    sev_conf_i[i],
                    str(cat_labels[i]),
                    cat_conf[i],
                    agreement,
                )
            )
        return results

    # -- persistence -------------------------------------------------------

    def save(self, dirpath: str) -> None:
        """Persist the fitted classifier's artifacts (+ metadata) to a directory.

        Writes, under ``dirpath`` (created if missing):

        * ``feature_pipeline.joblib`` — the shared fitted :class:`FeaturePipeline`.
        * ``service_ensemble.joblib`` — the fitted service ``VotingClassifier``.
        * ``severity_global.joblib`` — the global/fallback severity ensemble.
        * ``severity_<service>.joblib`` — each per-service severity ensemble (a
          service that fell back to the global model is recorded as such in the
          metadata and not double-written).
        * ``category_ensemble.joblib`` — the global category ensemble.
        * ``meta.json`` — services, per-service severity classes, category classes,
          and which services use the global-severity fallback.

        Args:
            dirpath: Destination directory.

        Raises:
            RuntimeError: if called before :meth:`fit`/:meth:`load`.
        """
        self._ensure_fitted()
        os.makedirs(dirpath, exist_ok=True)

        joblib.dump(self.features, os.path.join(dirpath, _FEATURE_PIPELINE_FILE))
        joblib.dump(
            self.service_ensemble, os.path.join(dirpath, _SERVICE_ENSEMBLE_FILE)
        )
        joblib.dump(
            self.category_ensemble, os.path.join(dirpath, _CATEGORY_ENSEMBLE_FILE)
        )
        joblib.dump(
            self.global_severity_ensemble,
            os.path.join(dirpath, _GLOBAL_SEVERITY_FILE),
        )

        # Per-service severity models. A service whose model IS the global ensemble
        # (degenerate fallback) is not re-dumped; it is restored from the global file.
        fallback_services: list[str] = []
        for service in SERVICES:
            model = self.severity_by_service.get(service)
            if model is None or model is self.global_severity_ensemble:
                fallback_services.append(service)
                continue
            joblib.dump(
                model, os.path.join(dirpath, _severity_artifact_name(service))
            )

        meta: dict[str, Any] = {
            "services": list(self.services_ or []),
            "service_classes": list(self.service_classes_ or []),
            "severity_classes_by_service": {
                k: list(v) for k, v in (self.severity_classes_by_service_ or {}).items()
            },
            "category_classes": list(self.category_classes_ or []),
            "fallback_services": fallback_services,
            "ensemble_weights": list(self.cfg.ensemble_weights),
            "voting": "soft",
        }
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
    def load(
        cls, dirpath: str, cfg: Optional[Settings] = None
    ) -> "MultiServiceClassifier":
        """Reconstruct a fitted :class:`MultiServiceClassifier` from :meth:`save`.

        Loads the shared feature pipeline, the service / category / global-severity
        ensembles, and each per-service severity ensemble (services recorded as
        fallbacks are wired to the loaded global-severity ensemble). Class-list
        metadata is restored from ``meta.json`` (falling back to the live estimators'
        ``classes_`` when the file is missing/unreadable).

        Args:
            dirpath: Directory previously written by :meth:`save`.
            cfg: Optional configuration for the rebuilt instance;
                :func:`src.config.get_config` is used when omitted. (The persisted
                fitted artifacts are authoritative — ``cfg`` only seeds defaults.)

        Returns:
            A fitted ``MultiServiceClassifier`` ready to classify.

        Raises:
            FileNotFoundError: if any required core artifact is missing from
                ``dirpath``.
            TypeError: if the feature-pipeline artifact is not a ``FeaturePipeline``.
        """
        feature_path = os.path.join(dirpath, _FEATURE_PIPELINE_FILE)
        service_path = os.path.join(dirpath, _SERVICE_ENSEMBLE_FILE)
        category_path = os.path.join(dirpath, _CATEGORY_ENSEMBLE_FILE)
        global_sev_path = os.path.join(dirpath, _GLOBAL_SEVERITY_FILE)
        for path in (feature_path, service_path, category_path, global_sev_path):
            if not os.path.isfile(path):
                raise FileNotFoundError(f"missing model artifact: {path}")

        instance = cls(cfg)
        instance.features = FeaturePipeline.load(feature_path)
        instance.service_ensemble = joblib.load(service_path)
        instance.category_ensemble = joblib.load(category_path)
        instance.global_severity_ensemble = joblib.load(global_sev_path)

        # Restore metadata (best-effort).
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

        fallback_services = set(meta.get("fallback_services", []))

        # Restore per-service severity models: load a dedicated artifact when present,
        # else wire the service to the global-severity ensemble.
        instance.severity_by_service = {}
        severity_classes_by_service: dict[str, list[str]] = {}
        for service in SERVICES:
            artifact = os.path.join(dirpath, _severity_artifact_name(service))
            if service not in fallback_services and os.path.isfile(artifact):
                model = joblib.load(artifact)
            else:
                model = instance.global_severity_ensemble
            instance.severity_by_service[service] = model
            severity_classes_by_service[service] = cls._classes_of(model)

        # Prefer persisted class metadata; fall back to the live estimators.
        instance.service_classes_ = [
            str(c)
            for c in meta.get(
                "service_classes", list(instance.service_ensemble.classes_)
            )
        ]
        instance.services_ = [
            str(s) for s in meta.get("services", instance.service_classes_)
        ]
        instance.severity_classes_by_service_ = {
            k: [str(c) for c in v]
            for k, v in meta.get(
                "severity_classes_by_service", severity_classes_by_service
            ).items()
        } or severity_classes_by_service
        instance.category_classes_ = [
            str(c)
            for c in meta.get(
                "category_classes", list(instance.category_ensemble.classes_)
            )
        ]
        instance.is_fitted = True
        return instance
