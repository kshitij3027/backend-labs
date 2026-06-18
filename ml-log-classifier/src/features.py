"""Feature extraction & combiner for the ML Log Classifier (Commit 4).

This module turns a batch of raw log records into ONE fixed-length, **non-negative**
sparse feature matrix that is safe to feed to every model in the ensemble —
:class:`~sklearn.naive_bayes.MultinomialNB` (which *rejects* negative inputs),
:class:`~sklearn.ensemble.RandomForestClassifier` and
:class:`~sklearn.ensemble.GradientBoostingClassifier`.

Three feature channels are combined declaratively with a
:class:`sklearn.compose.ColumnTransformer` over a small pandas
:class:`~pandas.DataFrame`:

1. **Text (TF-IDF).** The ``raw_log`` string is vectorized with
   :class:`~sklearn.feature_extraction.text.TfidfVectorizer`, whose
   ``preprocessor`` is :func:`src.preprocess.preprocess` (strips timestamps / IPs /
   UUIDs / latencies / opaque ``key=<id>`` tokens, lowercases, drops stopwords).
   Uni- and bi-grams up to ``cfg.tfidf_ngram_max`` are kept, capped at
   ``cfg.tfidf_max_features``. TF-IDF weights are always ``>= 0``.
2. **Temporal.** Numeric columns derived from the ISO ``timestamp``: hour of day,
   day of week, a weekend flag, and a *cyclical* encoding of the hour. The
   cyclical pair is shifted into ``[0, 1]`` as ``(sin + 1) / 2`` / ``(cos + 1) / 2``
   so it can never be negative.
3. **Metadata.** Numeric / binary columns derived **heuristically from the text of
   ``raw_log``** (never from the ground-truth labels) so the exact same features
   can be computed at inference time on a bare, unlabeled message: a one-hot of the
   ``[LEVEL]`` bracket token, a one-hot of detected service keywords, presence
   flags for request-ids / IPs, and a handful of shape statistics (length, token
   count, digit / upper-case ratios, punctuation count).

Why this guarantees a non-negative, fixed-width matrix
------------------------------------------------------
* All TF-IDF values are non-negative by construction.
* Every dense (temporal + metadata) column is passed through a single
  :class:`~sklearn.preprocessing.MinMaxScaler` fit with ``clip=True``. Min-max
  scaling maps the *training* range to ``[0, 1]``; ``clip=True`` is **mandatory**
  because it also clamps out-of-range inputs at transform time (e.g. a very short
  bare message whose ``msg_len`` is below anything seen in training) back into
  ``[0, 1]`` instead of letting it go negative. ``StandardScaler`` would produce
  negative values and is deliberately *not* used.
* The set of dense columns is fixed (see :data:`DENSE_COLUMNS`) and the TF-IDF
  vocabulary is frozen at ``fit`` time, so ``transform`` of *any* records — a full
  batch or a single bare-message dict — yields exactly ``feature_dim`` columns.

Public API
----------
* :func:`records_to_frame` — records → engineered :class:`~pandas.DataFrame`.
* :func:`build_column_transformer` — assemble the (unfitted) ``ColumnTransformer``.
* :class:`FeaturePipeline` — the public façade: ``fit`` / ``transform`` /
  ``fit_transform`` / ``get_feature_names`` / ``save`` / ``load``.
* :func:`build_feature_matrix` — one-call fit + transform convenience.

Everything is picklable end-to-end with :mod:`joblib`: the vectorizer references the
*module-level* function :func:`src.preprocess.preprocess` (importable, hence
picklable) rather than a lambda or closure.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Optional, Sequence

import joblib
import pandas as pd
from scipy import sparse
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import MinMaxScaler

from src.config import Settings, get_config
from src.preprocess import IPV4_RE, IPV6_RE, UUID_RE, preprocess, tokenize

# ---------------------------------------------------------------------------
# Column layout. The ORDER here is load-bearing: it fixes the column order of
# the dense block (and therefore the tail of the combined matrix and the tail of
# ``feature_names_``). It must stay in sync with what :func:`records_to_frame`
# produces and what :func:`build_column_transformer` hands to the scaler.
# ---------------------------------------------------------------------------

#: Temporal numeric columns derived from ``timestamp``.
TEMPORAL_COLUMNS: tuple[str, ...] = (
    "hour",          # 0-23
    "day_of_week",   # 0 (Mon) .. 6 (Sun)
    "is_weekend",    # 0 / 1
    "hour_sin",      # (sin(2*pi*hour/24) + 1) / 2   -> [0, 1]
    "hour_cos",      # (cos(2*pi*hour/24) + 1) / 2   -> [0, 1]
)

#: Recognised structured log levels (the ``[LEVEL]`` bracket token), one-hot
#: encoded. Mirrors ``src.log_generator.SEVERITIES``.
_LEVELS: tuple[str, ...] = ("DEBUG", "INFO", "WARN", "ERROR", "CRITICAL")

#: Service buckets inferred from keywords in the text, one-hot encoded. Mirrors
#: ``src.log_generator.SERVICES``.
_SERVICES: tuple[str, ...] = ("web", "database", "cache")

#: One-hot ``level_*`` columns. All zero when no ``[LEVEL]`` bracket is present.
LEVEL_COLUMNS: tuple[str, ...] = tuple(f"level_{lvl}" for lvl in _LEVELS)

#: One-hot ``svc_*`` columns. All zero when no service keyword matches.
SERVICE_COLUMNS: tuple[str, ...] = tuple(f"svc_{svc}" for svc in _SERVICES)

#: Remaining heuristic metadata (presence flags + text-shape statistics).
META_STAT_COLUMNS: tuple[str, ...] = (
    "has_request_id",  # req_id= / conn_id= / a UUID present
    "has_ip",          # an IPv4 or IPv6 address present
    "msg_len",         # len(raw_log)
    "token_count",     # len(tokenize(raw_log))
    "digit_ratio",     # digits / len(raw_log)   in [0, 1]
    "upper_ratio",     # uppercase / len(raw_log) in [0, 1]
    "punct_count",     # count of punctuation characters
)

#: Full metadata block in order.
METADATA_COLUMNS: tuple[str, ...] = (
    LEVEL_COLUMNS + SERVICE_COLUMNS + META_STAT_COLUMNS
)

#: All dense numeric columns the :class:`~sklearn.preprocessing.MinMaxScaler`
#: operates on, in their fixed order (temporal first, then metadata).
DENSE_COLUMNS: tuple[str, ...] = TEMPORAL_COLUMNS + METADATA_COLUMNS

#: Name of the single text column the TF-IDF vectorizer consumes.
TEXT_COLUMN: str = "text"

# ---------------------------------------------------------------------------
# Heuristic extraction regexes (text -> metadata). We reuse the noise regexes
# from :mod:`src.preprocess` where they apply (UUID / IPv4 / IPv6) and add a few
# tiny ones for the bracketed level and the ``*_id=`` request markers.
# ---------------------------------------------------------------------------

#: A leading ``[LEVEL]`` bracket token, case-insensitive (``[ERROR]``). Only the
#: first bracketed token is consulted.
_LEVEL_BRACKET_RE = re.compile(r"\[\s*([A-Za-z]+)\s*\]")

#: Request-identifier markers: ``req_id=`` / ``conn_id=`` / ``request_id=`` /
#: ``session=`` style ``*_id=`` keys (value irrelevant — we only flag presence).
_REQUEST_ID_KEY_RE = re.compile(
    r"\b(?:req_id|conn_id|request_id|requestid|reqid|connid|correlation_id|trace_id)\s*=",
    re.IGNORECASE,
)

#: Any character we count as "punctuation" for the ``punct_count`` shape feature.
_PUNCT_RE = re.compile(r"""[\[\](){}<>:;,.\-_/\\=+*?!@#$%^&|~`'"]""")

#: Service-keyword cues. Order does not matter; a record can light up more than
#: one ``svc_*`` column (e.g. a message mentioning both nginx and redis), which is
#: fine — the heuristic is a soft signal, not a hard label.
_SERVICE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "web": ("nginx", "http", "https", "upstream", "gateway", "request", "tls", "dns"),
    "database": ("sql", "query", "postgres", "postgresql", "db", "database",
                 "deadlock", "transaction", "txn", "index", "shard"),
    "cache": ("redis", "cache", "evict", "eviction", "memcached", "keyspace"),
}

#: Word-boundary matchers compiled once per (service -> keyword) so a substring
#: like ``db`` in ``"dbus"`` does not falsely trip the database bucket.
_SERVICE_KEYWORD_RE: dict[str, re.Pattern[str]] = {
    svc: re.compile(
        r"\b(?:" + "|".join(re.escape(k) for k in keywords) + r")\b",
        re.IGNORECASE,
    )
    for svc, keywords in _SERVICE_KEYWORDS.items()
}


# ---------------------------------------------------------------------------
# Per-record feature derivation helpers.
# ---------------------------------------------------------------------------


def _coerce_text(value: Any) -> str:
    """Coerce a ``raw_log`` value to ``str``; ``None`` becomes ``""``."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _parse_timestamp(value: Any) -> Optional[pd.Timestamp]:
    """Parse an ISO-8601 ``timestamp`` to a :class:`pandas.Timestamp`.

    Tolerant by design: a missing, empty, or unparseable value yields ``None`` so
    the caller can fall back to neutral temporal features (``hour=0`` etc.). The
    ``Z``/offset suffix emitted by the generator is handled by pandas; any tz info
    is dropped (we only care about wall-clock hour / weekday).
    """
    if value is None or value == "":
        return None
    try:
        ts = pd.to_datetime(value, utc=False, errors="raise")
    except (ValueError, TypeError, OverflowError):
        return None
    if ts is None or pd.isna(ts):
        return None
    # Normalise away any timezone so ``.hour`` / ``.dayofweek`` are wall-clock.
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_localize(None)
    return ts


def _temporal_features(value: Any) -> dict[str, float]:
    """Derive the temporal feature dict from a raw ``timestamp`` value.

    Falls back to ``hour=0`` (midnight, a weekday) when the timestamp is missing
    or unparseable, which keeps the feature width fixed and the values valid.
    """
    ts = _parse_timestamp(value)
    if ts is None:
        hour = 0
        dow = 0
    else:
        hour = int(ts.hour)
        dow = int(ts.dayofweek)  # Monday=0 .. Sunday=6

    import math

    angle = 2.0 * math.pi * (hour / 24.0)
    return {
        "hour": float(hour),
        "day_of_week": float(dow),
        "is_weekend": 1.0 if dow >= 5 else 0.0,
        # Shift sin/cos from [-1, 1] into [0, 1] so the dense block is already
        # non-negative even before MinMaxScaler (defence in depth).
        "hour_sin": (math.sin(angle) + 1.0) / 2.0,
        "hour_cos": (math.cos(angle) + 1.0) / 2.0,
    }


def _level_one_hot(text: str) -> dict[str, float]:
    """One-hot the ``[LEVEL]`` bracket token; all zeros when absent.

    A bare message with no ``[...]`` bracket (e.g. a raw user-supplied line)
    correctly yields every ``level_*`` column ``= 0``.
    """
    onehot = {col: 0.0 for col in LEVEL_COLUMNS}
    match = _LEVEL_BRACKET_RE.search(text)
    if match:
        token = match.group(1).upper()
        col = f"level_{token}"
        if col in onehot:
            onehot[col] = 1.0
    return onehot


def _service_one_hot(text: str) -> dict[str, float]:
    """One-hot detected service keywords; all zeros when nothing matches.

    More than one ``svc_*`` may be set if the text mentions keywords from several
    services — that is acceptable (these are soft cues, not exclusive labels).
    """
    onehot = {col: 0.0 for col in SERVICE_COLUMNS}
    for svc, pattern in _SERVICE_KEYWORD_RE.items():
        if pattern.search(text):
            onehot[f"svc_{svc}"] = 1.0
    return onehot


def _shape_features(text: str) -> dict[str, float]:
    """Compute presence flags and text-shape statistics from ``text``.

    All ratios are normalised by ``len(text)`` and therefore already live in
    ``[0, 1]``; counts (``msg_len`` / ``token_count`` / ``punct_count``) are raw
    magnitudes that the downstream :class:`MinMaxScaler` rescales.
    """
    length = len(text)
    has_request_id = bool(_REQUEST_ID_KEY_RE.search(text)) or bool(UUID_RE.search(text))
    has_ip = bool(IPV4_RE.search(text)) or bool(IPV6_RE.search(text))

    if length:
        digits = sum(ch.isdigit() for ch in text)
        uppers = sum(ch.isupper() for ch in text)
        digit_ratio = digits / length
        upper_ratio = uppers / length
    else:
        digit_ratio = 0.0
        upper_ratio = 0.0

    return {
        "has_request_id": 1.0 if has_request_id else 0.0,
        "has_ip": 1.0 if has_ip else 0.0,
        "msg_len": float(length),
        "token_count": float(len(tokenize(text))),
        "digit_ratio": float(digit_ratio),
        "upper_ratio": float(upper_ratio),
        "punct_count": float(len(_PUNCT_RE.findall(text))),
    }


def _record_to_row(record: dict[str, Any]) -> dict[str, Any]:
    """Build one fully-populated feature row (all columns present) for a record.

    The row always contains :data:`TEXT_COLUMN` plus every column in
    :data:`DENSE_COLUMNS`, regardless of which keys the input dict happened to
    carry — guaranteeing a rectangular frame and a fixed feature width.
    """
    raw_log = _coerce_text(record.get("raw_log"))

    row: dict[str, Any] = {TEXT_COLUMN: raw_log}
    row.update(_temporal_features(record.get("timestamp")))
    row.update(_level_one_hot(raw_log))
    row.update(_service_one_hot(raw_log))
    row.update(_shape_features(raw_log))
    return row


# ---------------------------------------------------------------------------
# Public: records -> DataFrame.
# ---------------------------------------------------------------------------


def records_to_frame(records: Iterable[dict[str, Any]]) -> pd.DataFrame:
    """Engineer a :class:`pandas.DataFrame` (one row per record) from log dicts.

    Each row exposes the single text column (:data:`TEXT_COLUMN`, the raw log line
    that the vectorizer will preprocess) plus every temporal and metadata column
    in :data:`DENSE_COLUMNS`. The frame's columns are ordered ``[text, *DENSE_COLUMNS]``
    and are produced even for records missing ``timestamp`` / ``service`` / etc.
    (those keys are derived from the text or defaulted), so a bare message dict
    such as::

        {"raw_log": "Database connection failed with timeout error",
         "timestamp": "2026-06-21T00:00:00", "service": "", "severity": "",
         "category": ""}

    yields a valid single-row frame with the same columns as a full batch.

    Args:
        records: An iterable of log dicts. Only ``raw_log`` and ``timestamp`` are
            consulted; all other keys (including the ground-truth labels) are
            ignored so the features are identical at train and inference time.

    Returns:
        A DataFrame with columns ``[TEXT_COLUMN, *DENSE_COLUMNS]``.

    Raises:
        ValueError: if ``records`` is empty (an empty frame has no rows to fit /
            transform and almost always signals a caller bug).
    """
    rows = [_record_to_row(rec) for rec in records]
    if not rows:
        raise ValueError(
            "records_to_frame received no records; expected at least one log dict"
        )
    columns = [TEXT_COLUMN, *DENSE_COLUMNS]
    frame = pd.DataFrame(rows, columns=columns)
    # Guarantee numeric dtype for the dense block (one-hot/flag floats included);
    # any unexpected NaN becomes 0.0 so the scaler never sees missing values.
    frame[list(DENSE_COLUMNS)] = (
        frame[list(DENSE_COLUMNS)].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    )
    frame[TEXT_COLUMN] = frame[TEXT_COLUMN].astype(str)
    return frame


# ---------------------------------------------------------------------------
# Public: the ColumnTransformer.
# ---------------------------------------------------------------------------


def build_column_transformer(cfg: Settings) -> ColumnTransformer:
    """Assemble the (unfitted) :class:`~sklearn.compose.ColumnTransformer`.

    Two branches over the engineered frame:

    * ``"tfidf"`` — :class:`~sklearn.feature_extraction.text.TfidfVectorizer` on
      :data:`TEXT_COLUMN`. ``preprocessor=preprocess`` (a module-level, picklable
      function) does cleaning + tokenisation + stopword removal, so
      ``lowercase=False`` (``preprocess`` already lowercases) and a permissive
      ``token_pattern`` simply splits on word boundaries. ``ngram_range`` and
      ``max_features`` come from ``cfg``. Output is sparse and non-negative.
    * ``"dense"`` — a single :class:`~sklearn.preprocessing.MinMaxScaler` with
      **``clip=True``** over every column in :data:`DENSE_COLUMNS`. ``clip=True``
      is mandatory: it keeps transformed values inside ``[0, 1]`` even for inputs
      outside the training range, which is what makes the *combined* matrix
      reliably non-negative for :class:`~sklearn.naive_bayes.MultinomialNB`.

    ``remainder="drop"`` (the text/dense columns are the only inputs) and the
    default ``sparse_threshold`` are kept so the combined output stays a sparse
    matrix given the large TF-IDF block.

    Args:
        cfg: Resolved :class:`src.config.Settings` (uses ``tfidf_max_features`` and
            ``tfidf_ngram_max``).

    Returns:
        An unfitted ``ColumnTransformer`` ready to ``fit`` on a frame from
        :func:`records_to_frame`.
    """
    vectorizer = TfidfVectorizer(
        preprocessor=preprocess,          # module-level fn -> picklable
        token_pattern=r"(?u)\b\w+\b",
        ngram_range=(1, int(cfg.tfidf_ngram_max)),
        max_features=int(cfg.tfidf_max_features),
        lowercase=False,                  # preprocess() already lowercases
    )
    scaler = MinMaxScaler(clip=True)      # clip=True -> stays in [0, 1]

    return ColumnTransformer(
        transformers=[
            ("tfidf", vectorizer, TEXT_COLUMN),
            ("dense", scaler, list(DENSE_COLUMNS)),
        ],
        remainder="drop",
    )


# ---------------------------------------------------------------------------
# Public façade.
# ---------------------------------------------------------------------------


class FeaturePipeline:
    """Fit/transform façade that combines TF-IDF + temporal + metadata features.

    Wraps a :class:`~sklearn.compose.ColumnTransformer` (built by
    :func:`build_column_transformer`) and the small DataFrame-engineering step
    (:func:`records_to_frame`) behind a stable, picklable interface. After
    :meth:`fit`, the produced matrix has a frozen width (:attr:`feature_dim`) and a
    matching ordered list of names (:attr:`feature_names_`), so :meth:`transform`
    of *any* records — including a single bare message — returns the same number of
    columns and is guaranteed non-negative.

    Attributes:
        cfg: The :class:`src.config.Settings` used to build the transformer.
        column_transformer: The underlying (fitted after :meth:`fit`) transformer.
        feature_dim: Total number of output columns (``None`` until fitted).
        feature_names_: Ordered output feature names, length == ``feature_dim``
            (``None`` until fitted). TF-IDF names are prefixed ``tfidf__`` and the
            dense names are the raw :data:`DENSE_COLUMNS`.
    """

    def __init__(self, cfg: Optional[Settings] = None) -> None:
        """Create an unfitted pipeline.

        Args:
            cfg: Optional configuration; :func:`src.config.get_config` is used when
                omitted.
        """
        self.cfg: Settings = cfg if cfg is not None else get_config()
        self.column_transformer: ColumnTransformer = build_column_transformer(self.cfg)
        self.feature_dim: Optional[int] = None
        self.feature_names_: Optional[list[str]] = None

    # -- internal ----------------------------------------------------------

    def _ensure_fitted(self) -> None:
        """Raise a clear error if the pipeline has not been fitted yet."""
        if self.feature_dim is None or self.feature_names_ is None:
            raise RuntimeError(
                "FeaturePipeline is not fitted yet; call fit()/fit_transform() first"
            )

    def _compute_feature_names(self) -> list[str]:
        """Build the ordered output names after the transformer is fitted.

        TF-IDF vocabulary names (from the fitted vectorizer's
        ``get_feature_names_out``) are prefixed ``tfidf__``; the dense names are
        the literal :data:`DENSE_COLUMNS`. The result lines up 1:1 with the matrix
        columns and is used later for feature-importance visualisation.
        """
        vectorizer: TfidfVectorizer = self.column_transformer.named_transformers_["tfidf"]
        tfidf_names = [f"tfidf__{name}" for name in vectorizer.get_feature_names_out()]
        return tfidf_names + list(DENSE_COLUMNS)

    # -- public ------------------------------------------------------------

    def fit(self, records: Sequence[dict[str, Any]]) -> "FeaturePipeline":
        """Fit the underlying transformer on ``records`` and freeze the width.

        Builds the engineered frame, fits the ``ColumnTransformer`` (learning the
        TF-IDF vocabulary and the MinMax ranges of the dense columns), then records
        :attr:`feature_dim` and :attr:`feature_names_`.

        Args:
            records: Non-empty sequence of log dicts to learn from.

        Returns:
            ``self`` (for chaining).

        Raises:
            ValueError: if ``records`` is empty (propagated from
                :func:`records_to_frame`).
        """
        frame = records_to_frame(records)
        matrix = self.column_transformer.fit_transform(frame)
        self.feature_names_ = self._compute_feature_names()
        self.feature_dim = matrix.shape[1]
        return self

    def transform(self, records: Sequence[dict[str, Any]]):
        """Transform ``records`` into a non-negative CSR feature matrix.

        Args:
            records: Non-empty sequence of log dicts (may differ from the fit set;
                each is reduced to the same fixed columns).

        Returns:
            A :class:`scipy.sparse.csr_matrix` of shape
            ``(len(records), feature_dim)``. Every entry is ``>= 0``.

        Raises:
            RuntimeError: if called before :meth:`fit`.
            ValueError: if ``records`` is empty.
        """
        self._ensure_fitted()
        frame = records_to_frame(records)
        matrix = self.column_transformer.transform(frame)
        return sparse.csr_matrix(matrix)

    def fit_transform(self, records: Sequence[dict[str, Any]]):
        """Fit on ``records`` and return their transformed CSR matrix.

        Equivalent to :meth:`fit` followed by :meth:`transform`, but reuses the
        single ``fit_transform`` pass of the underlying transformer.

        Args:
            records: Non-empty sequence of log dicts.

        Returns:
            A non-negative :class:`scipy.sparse.csr_matrix` of shape
            ``(len(records), feature_dim)``.
        """
        frame = records_to_frame(records)
        matrix = self.column_transformer.fit_transform(frame)
        self.feature_names_ = self._compute_feature_names()
        self.feature_dim = matrix.shape[1]
        return sparse.csr_matrix(matrix)

    def get_feature_names(self) -> list[str]:
        """Return the ordered output feature names (length == :attr:`feature_dim`).

        Raises:
            RuntimeError: if called before :meth:`fit`.
        """
        self._ensure_fitted()
        assert self.feature_names_ is not None  # for type-checkers; guarded above
        return list(self.feature_names_)

    def save(self, path: str) -> None:
        """Persist the fitted pipeline to ``path`` via :mod:`joblib`.

        The whole object (fitted ``ColumnTransformer`` + ``cfg`` + cached
        ``feature_dim`` / ``feature_names_``) is pickled. It is fully picklable
        because the vectorizer references the importable module-level
        :func:`src.preprocess.preprocess` rather than a lambda/closure.

        Args:
            path: Destination file path.

        Raises:
            RuntimeError: if called before :meth:`fit`.
        """
        self._ensure_fitted()
        joblib.dump(self, path)

    @classmethod
    def load(cls, path: str) -> "FeaturePipeline":
        """Load a pipeline previously written by :meth:`save`.

        Args:
            path: Source file path.

        Returns:
            The restored :class:`FeaturePipeline`.

        Raises:
            TypeError: if the pickle does not contain a :class:`FeaturePipeline`.
        """
        obj = joblib.load(path)
        if not isinstance(obj, cls):
            raise TypeError(
                f"loaded object is not a FeaturePipeline (got {type(obj).__name__})"
            )
        return obj


# ---------------------------------------------------------------------------
# Convenience one-call helper.
# ---------------------------------------------------------------------------


def build_feature_matrix(
    records: Sequence[dict[str, Any]],
    cfg: Optional[Settings] = None,
):
    """Fit a fresh :class:`FeaturePipeline` on ``records`` and transform them.

    A thin convenience wrapper around ``FeaturePipeline(cfg).fit_transform(records)``
    for call sites (tests, the trainer) that just want a matrix and the fitted
    pipeline in one step.

    Args:
        records: Non-empty sequence of log dicts.
        cfg: Optional configuration; :func:`src.config.get_config` is used when
            omitted.

    Returns:
        A ``(matrix, pipeline)`` tuple where ``matrix`` is a non-negative
        :class:`scipy.sparse.csr_matrix` of shape ``(len(records), feature_dim)``
        and ``pipeline`` is the fitted :class:`FeaturePipeline`.
    """
    pipeline = FeaturePipeline(cfg)
    matrix = pipeline.fit_transform(records)
    return matrix, pipeline
