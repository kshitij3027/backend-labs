"""Intent-classification analyzer — TF-IDF + LogisticRegression over the synthetic corpus.

A log line's **intent** (``authentication``, ``deployment``, ``error_report``, ...) is the
coarse "what is this line about" label the dashboard groups and trends on. There is no real
labeled ops-log dataset here, so — exactly as the NER layer is *measured* against it — this
classifier is *trained* on the deterministic, balanced, labeled corpus manufactured by
:mod:`src.generators`. That corpus is plentiful (hundreds of lines per intent) and carries
the realism noise (timestamp/level prefixes, synonym swaps, casing jitter) the model must be
robust to, so a classic linear text classifier learns the template phrasings very well.

**The model** is one :class:`sklearn.pipeline.Pipeline`::

    TfidfVectorizer(ngram_range=(1, 2), sublinear_tf=True, min_df=2)
        -> LogisticRegression(max_iter=1000, class_weight="balanced")

* ``ngram_range=(1, 2)`` captures the discriminative *phrases* ("health check passed",
  "deployment of", "high memory usage") not just bag-of-words.
* ``sublinear_tf`` dampens repeated tokens; ``min_df=2`` drops the once-only surface noise
  (a specific IP / host / user id appears at most a handful of times and carries no intent).
* ``class_weight="balanced"`` keeps every intent weighted equally even though the corpus is
  already balanced — a cheap guard against any split skew.

**Confidence & the reject bucket.** ``LogisticRegression`` exposes a native
:meth:`predict_proba`; the confidence of a prediction is simply the maximum class
probability in ``[0, 1]``. When that maximum falls **below** :data:`DEFAULT_THRESHOLD` the
line is too ambiguous / out-of-distribution to trust, so the analyzer returns the
:data:`OTHER_LABEL` (``"other"``) reject bucket instead of a real intent — but it keeps the
*real* max probability as the reported confidence (the ``"other"`` verdict is about the
label, not the number). Empty input short-circuits to ``("other", 0.0)``.

**Persistence.** The **whole pipeline** is persisted with :mod:`joblib` (never the
vectorizer and the model separately — a split fit is a classic source of train/serve skew).
The artifact is **built into the Docker image at build time** by ``scripts/train_intent.py``
and lands at :data:`DEFAULT_ARTIFACT_PATH`; it is gitignored (``*.joblib``) and never
committed — it is reproduced deterministically from the seeded corpus on every image build.
:meth:`IntentAnalyzer.load_or_train` is what the C7 engine calls at startup: load the baked
artifact if present, else train on the fly (training is fast, keeping unit tests hermetic).

Everything here is deterministic: the corpus is seeded, TF-IDF is deterministic, and the
``lbfgs`` LogisticRegression solver is deterministic (a fixed ``random_state`` is set anyway,
so the fit never depends on any global RNG or the wall clock).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

from src.generators import LogSample, produce_corpus

if TYPE_CHECKING:  # annotation-only imports (never needed at runtime)
    from collections.abc import Iterable, Sequence

    import numpy as np

#: Where the baked pipeline lives inside the image / repo — computed relative to this file
#: so it is import-location independent (``src/nlp/artifacts/intent.joblib``). The directory
#: is created on demand by :meth:`IntentAnalyzer.save`; the artifact itself is gitignored.
DEFAULT_ARTIFACT_PATH: Path = Path(__file__).parent / "artifacts" / "intent.joblib"

#: The label returned when the top class probability is below the confidence threshold — the
#: "I am not sure this is any known intent" reject bucket. Deliberately NOT a member of
#: ``generators.INTENTS`` so callers can tell a real intent from a low-confidence fallback.
OTHER_LABEL: str = "other"

#: Default low-confidence floor. A max ``predict_proba`` below this maps to :data:`OTHER_LABEL`.
#: Tunable per-instance (the C7 engine reads it from settings and passes it in).
DEFAULT_THRESHOLD: float = 0.45


def _build_pipeline() -> Pipeline:
    """Construct a fresh, unfitted TF-IDF + LogisticRegression pipeline (the research design).

    Returned unfitted so every :meth:`IntentAnalyzer.train` starts from a clean estimator.
    ``random_state`` is fixed for determinism even though ``lbfgs`` is already deterministic.
    """
    return Pipeline(
        [
            ("tfidf", TfidfVectorizer(ngram_range=(1, 2), sublinear_tf=True, min_df=2)),
            (
                "clf",
                LogisticRegression(
                    max_iter=1000,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )


def _to_xy(
    samples: "Iterable[LogSample] | tuple[Sequence[str], Sequence[str]]",
) -> tuple[list[str], list[str]]:
    """Normalise training input to parallel ``(texts, labels)`` lists.

    Accepts either an iterable of :class:`~src.generators.LogSample` (``.message`` /
    ``.intent`` are read off each) or a ready-made ``(texts, labels)`` two-tuple. A two-tuple
    whose first element is itself a :class:`LogSample` is treated as the iterable form (so a
    two-element sequence of samples is never mis-read as ``(texts, labels)``).
    """
    if (
        isinstance(samples, tuple)
        and len(samples) == 2
        and not isinstance(samples[0], LogSample)
    ):
        texts, labels = samples
        return list(texts), list(labels)

    texts, labels = [], []
    for sample in samples:  # type: ignore[union-attr]
        texts.append(sample.message)
        labels.append(sample.intent)
    return texts, labels


class IntentAnalyzer:
    """Classify a log line's intent and report a calibrated confidence.

    Wraps a (possibly pre-fit) sklearn :class:`Pipeline` plus the low-confidence
    :attr:`threshold`. Construct empty and :meth:`train`, or :meth:`load` / :meth:`load_or_train`
    a baked artifact. :meth:`predict` returns ``(label, confidence)`` where ``label`` is one
    of ``generators.INTENTS`` or :data:`OTHER_LABEL` and ``confidence`` is the real max class
    probability in ``[0, 1]``.
    """

    def __init__(self, pipeline: Pipeline | None = None, threshold: float = DEFAULT_THRESHOLD) -> None:
        """Hold an optional already-fitted ``pipeline`` and the reject ``threshold``.

        Args:
            pipeline: A fitted sklearn ``Pipeline`` to serve from, or ``None`` to construct an
                untrained analyzer (call :meth:`train` before predicting).
            threshold: Max-probability floor below which a prediction maps to
                :data:`OTHER_LABEL`. The confidence value returned is unaffected.
        """
        self.pipeline = pipeline
        self.threshold = threshold

    # -- training ----------------------------------------------------------------------
    def train(
        self,
        samples: "Iterable[LogSample] | tuple[Sequence[str], Sequence[str]]",
    ) -> "IntentAnalyzer":
        """Fit a fresh pipeline on ``samples`` and adopt it; returns ``self`` for chaining.

        Args:
            samples: An iterable of :class:`~src.generators.LogSample` (labels read from
                ``.intent``) or a ``(texts, labels)`` two-tuple.

        Returns:
            ``self``, now trained.

        Raises:
            ValueError: If ``samples`` yields no training rows.
        """
        texts, labels = _to_xy(samples)
        if not texts:
            raise ValueError("cannot train IntentAnalyzer on an empty sample set")
        pipeline = _build_pipeline()
        pipeline.fit(texts, labels)
        self.pipeline = pipeline
        return self

    # -- inference ---------------------------------------------------------------------
    def predict(self, text: str) -> tuple[str, float]:
        """Return ``(label, confidence)`` for one line.

        ``confidence`` is the maximum class probability in ``[0, 1]``; when it is below
        :attr:`threshold` the ``label`` is :data:`OTHER_LABEL` (but ``confidence`` still holds
        the real maximum). Empty / whitespace-only input returns ``(OTHER_LABEL, 0.0)``.

        Raises:
            RuntimeError: If the analyzer has not been trained or loaded.
        """
        self._require_ready()
        if not text or not text.strip():
            return (OTHER_LABEL, 0.0)
        proba = self.pipeline.predict_proba([text])[0]  # type: ignore[union-attr]
        return self._decide(proba, self._classes())

    def predict_batch(self, texts: list[str]) -> list[tuple[str, float]]:
        """Vectorized :meth:`predict` over many lines; order-preserving and empty-safe.

        Calls ``predict_proba`` **once** on the non-empty subset (the speed win), then
        reassembles results in input order. Each element is identical to what :meth:`predict`
        would return for that line — empty / whitespace items yield ``(OTHER_LABEL, 0.0)``.

        Raises:
            RuntimeError: If the analyzer has not been trained or loaded.
        """
        self._require_ready()
        results: list[tuple[str, float]] = [(OTHER_LABEL, 0.0)] * len(texts)
        idxs = [i for i, t in enumerate(texts) if t and t.strip()]
        if not idxs:
            return results
        proba = self.pipeline.predict_proba([texts[i] for i in idxs])  # type: ignore[union-attr]
        classes = self._classes()
        for row, i in zip(proba, idxs):
            results[i] = self._decide(row, classes)
        return results

    # -- persistence -------------------------------------------------------------------
    def save(self, path: str | Path = DEFAULT_ARTIFACT_PATH) -> None:
        """``joblib.dump`` the *whole* fitted pipeline to ``path`` (parent dir created).

        Persisting the whole pipeline (vectorizer + model together) is deliberate: dumping
        the two separately is a classic source of train/serve vocabulary skew.

        Raises:
            RuntimeError: If there is nothing trained to save.
        """
        self._require_ready()
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self.pipeline, path)

    @classmethod
    def load(cls, path: str | Path = DEFAULT_ARTIFACT_PATH, threshold: float = DEFAULT_THRESHOLD) -> "IntentAnalyzer":
        """Load a pipeline persisted by :meth:`save` and wrap it at ``threshold``."""
        pipeline = joblib.load(Path(path))
        return cls(pipeline=pipeline, threshold=threshold)

    @classmethod
    def load_or_train(
        cls,
        path: str | Path = DEFAULT_ARTIFACT_PATH,
        n_per_intent: int = 200,
        seed: int = 42,
        threshold: float = DEFAULT_THRESHOLD,
    ) -> "IntentAnalyzer":
        """Return a ready analyzer: load the baked artifact if it exists, else train fresh.

        This is the C7 engine's startup hook. In the built image the artifact exists (baked by
        ``scripts/train_intent.py``) so this is a fast load. Off-image (unit tests, a bare
        checkout) the artifact is absent, so it trains from a seeded
        :func:`~src.generators.produce_corpus` — training is fast and, crucially, this path
        does **not** write the artifact, keeping tests hermetic (no filesystem side effects).

        Args:
            path: Artifact location to try loading from.
            n_per_intent: Corpus size per intent when training is needed.
            seed: Corpus seed when training is needed (determinism).
            threshold: Confidence floor for the returned analyzer.
        """
        path = Path(path)
        if path.exists():
            return cls.load(path, threshold=threshold)
        analyzer = cls(threshold=threshold)
        analyzer.train(produce_corpus(n_per_intent=n_per_intent, seed=seed))
        return analyzer

    # -- internals ---------------------------------------------------------------------
    def _require_ready(self) -> None:
        """Raise a clear error if no pipeline has been trained or loaded yet."""
        if self.pipeline is None:
            raise RuntimeError(
                "IntentAnalyzer has no model; call train(), load(), or load_or_train() "
                "before predicting."
            )

    def _classes(self) -> "np.ndarray":
        """The fitted classifier's class labels (from the final pipeline step)."""
        return self.pipeline.steps[-1][1].classes_  # type: ignore[union-attr]

    def _decide(self, proba_row: "np.ndarray", classes: "np.ndarray") -> tuple[str, float]:
        """Turn one ``predict_proba`` row into ``(label, confidence)`` applying the threshold."""
        best = int(proba_row.argmax())
        confidence = float(proba_row[best])
        label = str(classes[best]) if confidence >= self.threshold else OTHER_LABEL
        return (label, confidence)
