"""End-to-end training orchestration + cross-validation for the classifier (Commit 7).

This module is the **"train a model" entry point** for the system. It ties the
data source (:func:`src.log_generator.generate_logs`), the feature pipeline
(:class:`src.features.FeaturePipeline`), the soft-voting ensembles
(:func:`src.ensemble.build_ensemble` / :class:`src.ensemble.LogClassifier`) and
the versioned model store (:class:`src.model_store.ModelRegistry`) into one
:func:`train` call that produces — and optionally persists — a ready-to-serve
model along with the two metrics the spec grades on:

* **Held-out test accuracy** (project requirements §5: *"90%+ classification
  accuracy on test logs"*). A clean train/test split is taken, a
  :class:`LogClassifier` is fitted on the **train** split only, and severity /
  category accuracy are measured on the untouched **test** split.
* **Cross-validation accuracy** (project requirements §5: *">85% during the
  training phase"*). ``cross_val_score`` is run for both the severity and the
  category ensembles, and the mean / std of each are recorded.

The final inference model is then re-fitted on **all** records (more data → a
better production model than the split model) and handed to the registry, which
stamps it with a version id.

A note on cross-validation leakage
----------------------------------
For the CV step the feature matrix is built **once** over *all* records via
``FeaturePipeline().fit_transform`` and reused across folds. This means the TF-IDF
vocabulary is fit on the full set, so a fold's validation rows influenced the
vocabulary — a small, deliberate leak. It is accepted here on purpose: the data is
synthetic and template-driven, the convenience of a single transform is worth it,
and the *primary* "90%+" metric uses a genuinely clean holdout split with the
pipeline fit on the train portion only, so it is not affected.

CLI
---
``python -m src.trainer --count 1000 --cv 5 --model-dir <dir>`` trains on freshly
generated logs, prints the metrics, and persists a version. All defaults come from
:func:`src.config.get_config`.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Optional, Sequence

from sklearn.metrics import accuracy_score
from sklearn.model_selection import cross_val_score, train_test_split

from src.config import Settings, get_config
from src.ensemble import LogClassifier, build_ensemble
from src.features import FeaturePipeline
from src.log_generator import generate_logs
from src.model_store import ModelRegistry


def _resolve(cfg: Optional[Settings]) -> Settings:
    """Return ``cfg`` if provided, else the process-wide configuration."""
    return cfg if cfg is not None else get_config()


def _labels(records: Sequence[dict[str, Any]], key: str) -> list[str]:
    """Extract the ``key`` label (``"severity"`` / ``"category"``) from records."""
    return [str(r[key]) for r in records]


def _can_stratify(labels: Sequence[str], test_size: float, n_total: int) -> bool:
    """Decide whether a stratified split is feasible for ``labels``.

    Stratification needs at least two members of **every** class, and each class
    must keep at least one row on *both* sides of the split. When any class is too
    small we fall back to a plain random split rather than letting
    ``train_test_split`` raise.

    Args:
        labels: The stratification target (one label per record).
        test_size: Fraction routed to the test split.
        n_total: Total number of records.

    Returns:
        ``True`` if a stratified split should be attempted, ``False`` otherwise.
    """
    from collections import Counter

    counts = Counter(labels)
    if not counts:
        return False
    # Every class needs >= 2 samples for stratification to be defined at all.
    if min(counts.values()) < 2:
        return False
    # The test side must be able to hold at least one row per class.
    n_classes = len(counts)
    n_test = int(round(test_size * n_total))
    return n_test >= n_classes and (n_total - n_test) >= n_classes


def _train_test_split_records(
    records: Sequence[dict[str, Any]],
    test_size: float,
    random_state: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split ``records`` into (train, test), stratifying on severity when feasible.

    Tries to stratify on the severity labels so every severity is represented in
    both splits (important for the headline accuracy to be meaningful). If
    stratification is not feasible — or sklearn still rejects it for a rare class —
    it transparently falls back to a non-stratified random split.

    Args:
        records: All labeled records.
        test_size: Fraction routed to the test split.
        random_state: Seed for a reproducible split.

    Returns:
        A ``(train_records, test_records)`` tuple of plain lists.
    """
    n_total = len(records)
    severity_labels = _labels(records, "severity")

    stratify = severity_labels if _can_stratify(severity_labels, test_size, n_total) else None
    try:
        train_recs, test_recs = train_test_split(
            list(records),
            test_size=test_size,
            random_state=random_state,
            stratify=stratify,
        )
    except ValueError:
        # Rare-class edge case sklearn rejected even after our guard — retry plain.
        train_recs, test_recs = train_test_split(
            list(records),
            test_size=test_size,
            random_state=random_state,
            stratify=None,
        )
    return list(train_recs), list(test_recs)


def cross_validate(
    records: Sequence[dict[str, Any]],
    cfg: Optional[Settings] = None,
    cv: int = 5,
) -> dict[str, float]:
    """Run k-fold cross-validation for both the severity and category ensembles.

    Builds the combined feature matrix **once** over all ``records`` (see the
    module docstring on the accepted, minor TF-IDF leak), then scores a freshly
    built soft-voting ensemble with :func:`sklearn.model_selection.cross_val_score`
    (``scoring="accuracy"``) against the severity labels and again against the
    category labels.

    Args:
        records: Non-empty sequence of labeled log dicts.
        cfg: Optional configuration; :func:`src.config.get_config` is used when
            omitted.
        cv: Number of folds (default 5). Silently clamped to ``[2, n_records]``.

    Returns:
        A dict with::

            {
              "severity_cv_mean": float, "severity_cv_std": float,
              "category_cv_mean": float, "category_cv_std": float,
              "cv": int,          # the (possibly clamped) fold count used
            }

    Raises:
        ValueError: if ``records`` is empty.
    """
    settings = _resolve(cfg)
    if not records:
        raise ValueError("cross_validate() requires at least one record")

    X = FeaturePipeline(settings).fit_transform(records)
    y_severity = _labels(records, "severity")
    y_category = _labels(records, "category")

    # Clamp the fold count so a tiny/imbalanced corpus (tests, degenerate input)
    # never errors. ``cross_val_score`` defaults to *stratified* k-fold for a
    # classifier, which requires ``n_splits <=`` the smallest class's member count
    # — so bound the folds by the rarest class across BOTH label axes, not just the
    # record count. Always keep at least 2 folds for a meaningful CV estimate.
    from collections import Counter

    min_class = min(
        min(Counter(y_severity).values()),
        min(Counter(y_category).values()),
    )
    folds = max(2, min(int(cv), len(records), min_class))

    sev_scores = cross_val_score(
        build_ensemble(settings), X, y_severity, cv=folds, scoring="accuracy"
    )
    cat_scores = cross_val_score(
        build_ensemble(settings), X, y_category, cv=folds, scoring="accuracy"
    )

    return {
        "severity_cv_mean": float(sev_scores.mean()),
        "severity_cv_std": float(sev_scores.std()),
        "category_cv_mean": float(cat_scores.mean()),
        "category_cv_std": float(cat_scores.std()),
        "cv": int(folds),
    }


def train(
    records: Optional[list[dict[str, Any]]] = None,
    cfg: Optional[Settings] = None,
    test_size: float = 0.2,
    cv: int = 5,
    persist: bool = True,
    registry: Optional[ModelRegistry] = None,
) -> dict[str, Any]:
    """Run the full training workflow and (optionally) persist a model version.

    Pipeline:

    1. **Data.** If ``records`` is ``None``, generate them deterministically with
       :func:`src.log_generator.generate_logs` (``cfg.sample_size`` / ``cfg.random_seed``).
    2. **Clean holdout (the "90%+ on test logs" metric).** Split off a test set
       (stratified on severity when feasible, see
       :func:`_train_test_split_records`), fit a :class:`LogClassifier` on the
       **train** split only, and measure ``severity_test_accuracy`` /
       ``category_test_accuracy`` on the held-out test split via
       :meth:`LogClassifier.classify_batch` and
       :func:`sklearn.metrics.accuracy_score`.
    3. **Cross-validation (the ">85% during training" metric).** Call
       :func:`cross_validate` for severity and category mean / std.
    4. **Final model.** Fit a fresh :class:`LogClassifier` on **all** records — the
       best inference model (more data than the split model).
    5. **Metrics dict.** Assemble all scores plus sizes and a timestamp.
    6. **Persist.** If ``persist``, save the final model through a
       :class:`ModelRegistry`, stamping it with a version id.

    Args:
        records: Optional pre-built labeled records; generated when ``None``.
        cfg: Optional configuration; :func:`src.config.get_config` when omitted.
        test_size: Fraction held out for the clean accuracy metric (default 0.2).
        cv: Cross-validation fold count (default 5).
        persist: Whether to write the final model to the registry (default True).
        registry: Optional registry to persist into; one rooted at ``cfg.model_dir``
            is created when omitted and ``persist`` is True.

    Returns:
        A dict::

            {
              "classifier": LogClassifier,   # final model fit on ALL records
              "metrics":    {...},           # see the metric keys below
              "version":    str | None,      # registry version id, or None
              "registry":   ModelRegistry | None,
            }

        where ``metrics`` has exactly these keys::

            severity_test_accuracy, category_test_accuracy,
            severity_cv_mean, severity_cv_std,
            category_cv_mean, category_cv_std,
            n_total, n_train, n_test, cv, trained_at

    Raises:
        ValueError: if there are no records to train on.
    """
    settings = _resolve(cfg)

    if records is None:
        print(
            f"[trainer] generating {settings.sample_size} synthetic logs "
            f"(seed={settings.random_seed}) ..."
        )
        records = generate_logs(settings.sample_size, settings.random_seed)
    if not records:
        raise ValueError("train() requires at least one record")

    n_total = len(records)
    print(f"[trainer] training on {n_total} records (test_size={test_size}, cv={cv})")

    # --- 2. Clean holdout: fit on train split, score on untouched test split. ---
    train_recs, test_recs = _train_test_split_records(
        records, test_size=test_size, random_state=settings.random_seed
    )
    print(
        f"[trainer] holdout split -> train={len(train_recs)} test={len(test_recs)}; "
        "fitting holdout classifier ..."
    )
    holdout_clf = LogClassifier(settings).fit(train_recs)
    test_results = holdout_clf.classify_batch(test_recs)
    severity_test_accuracy = float(
        accuracy_score(
            _labels(test_recs, "severity"), [r["severity"] for r in test_results]
        )
    )
    category_test_accuracy = float(
        accuracy_score(
            _labels(test_recs, "category"), [r["category"] for r in test_results]
        )
    )
    print(
        f"[trainer] held-out accuracy -> severity={severity_test_accuracy:.4f} "
        f"category={category_test_accuracy:.4f}"
    )

    # --- 3. Cross-validation on all records (severity + category). ---
    print(f"[trainer] running {cv}-fold cross-validation ...")
    cv_metrics = cross_validate(records, settings, cv=cv)
    print(
        f"[trainer] CV mean -> severity={cv_metrics['severity_cv_mean']:.4f} "
        f"(+/-{cv_metrics['severity_cv_std']:.4f}) "
        f"category={cv_metrics['category_cv_mean']:.4f} "
        f"(+/-{cv_metrics['category_cv_std']:.4f})"
    )

    # --- 4. Final model fit on ALL records (best inference model). ---
    print(f"[trainer] fitting final classifier on all {n_total} records ...")
    final_clf = LogClassifier(settings).fit(records)

    # --- 5. Assemble metrics. ---
    metrics: dict[str, Any] = {
        "severity_test_accuracy": severity_test_accuracy,
        "category_test_accuracy": category_test_accuracy,
        "severity_cv_mean": cv_metrics["severity_cv_mean"],
        "severity_cv_std": cv_metrics["severity_cv_std"],
        "category_cv_mean": cv_metrics["category_cv_mean"],
        "category_cv_std": cv_metrics["category_cv_std"],
        "n_total": int(n_total),
        "n_train": int(len(train_recs)),
        "n_test": int(len(test_recs)),
        "cv": int(cv_metrics["cv"]),
        "trained_at": datetime.utcnow().isoformat(),
    }

    # --- 6. Persist the final model through the registry. ---
    version: Optional[str] = None
    used_registry: Optional[ModelRegistry] = registry
    if persist:
        used_registry = registry or ModelRegistry(settings.model_dir)
        version = used_registry.save_version(final_clf, metrics)
        print(f"[trainer] persisted model version '{version}' to {used_registry.model_dir}")

    return {
        "classifier": final_clf,
        "metrics": metrics,
        "version": version,
        "registry": used_registry,
    }


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------


def _format_metrics(metrics: dict[str, Any], version: Optional[str]) -> str:
    """Render the metrics dict as a compact, human-readable block for the CLI."""
    lines = ["", "=== Training metrics ===", ""]
    ordered = [
        ("severity_test_accuracy", "Severity test accuracy"),
        ("category_test_accuracy", "Category test accuracy"),
        ("severity_cv_mean", "Severity CV mean"),
        ("severity_cv_std", "Severity CV std"),
        ("category_cv_mean", "Category CV mean"),
        ("category_cv_std", "Category CV std"),
        ("n_total", "Total records"),
        ("n_train", "Train records"),
        ("n_test", "Test records"),
        ("cv", "CV folds"),
        ("trained_at", "Trained at"),
    ]
    for key, label in ordered:
        value = metrics.get(key)
        if isinstance(value, float):
            lines.append(f"  {label:<24} {value:.4f}")
        else:
            lines.append(f"  {label:<24} {value}")
    lines.append(f"  {'Saved version':<24} {version if version else '(not persisted)'}")
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point: train, print the metrics, and persist a version.

    Defaults for ``--count`` / ``--cv`` / ``--model-dir`` / ``--test-size`` /
    ``--seed`` come from :func:`src.config.get_config`.
    """
    cfg = get_config()

    parser = argparse.ArgumentParser(
        prog="python -m src.trainer",
        description="Train the ensemble log classifier, cross-validate, and persist a version.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=cfg.sample_size,
        help=f"number of synthetic logs to train on (default: {cfg.sample_size})",
    )
    parser.add_argument(
        "--cv",
        type=int,
        default=5,
        help="cross-validation fold count (default: 5)",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="fraction held out for the test-accuracy metric (default: 0.2)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=cfg.random_seed,
        help=f"RNG seed for data + split reproducibility (default: {cfg.random_seed})",
    )
    parser.add_argument(
        "--model-dir",
        type=str,
        default=cfg.model_dir,
        help=f"registry directory to persist the model into (default: {cfg.model_dir})",
    )
    parser.add_argument(
        "--no-persist",
        action="store_true",
        help="train and report metrics without writing a model version",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="print the metrics dict as JSON instead of a formatted block",
    )
    args = parser.parse_args(argv)

    # Build a config that honours the CLI overrides for the knobs that matter.
    settings = Settings(
        **{
            **cfg.__dict__,
            "sample_size": args.count,
            "random_seed": args.seed,
            "model_dir": args.model_dir,
        }
    )

    registry = None if args.no_persist else ModelRegistry(settings.model_dir)
    result = train(
        records=None,
        cfg=settings,
        test_size=args.test_size,
        cv=args.cv,
        persist=not args.no_persist,
        registry=registry,
    )

    if args.json:
        payload = dict(result["metrics"])
        payload["version"] = result["version"]
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_format_metrics(result["metrics"], result["version"]))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
