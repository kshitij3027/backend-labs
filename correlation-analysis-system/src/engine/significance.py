"""Statistical significance helpers for the metric-based detector.

Pure numpy/scipy numerics with **no engine imports** — and the project's only
scipy import site (engine modules stay scipy-free);
:mod:`src.engine.metric` turns these numbers into Correlations.

The centrepiece is Benjamini-Hochberg false-discovery-rate control
(:func:`benjamini_hochberg` / :func:`bh_adjusted`), the false-positive filter
called out in the plan's research: the metric detector tests ~19 series pairs
every 2-second cycle — thousands of hypothesis tests per hour — so emitting on
raw per-pair p < 0.05 would flood the dashboard with coincidences (the classic
multiple-testing problem). BH (Benjamini & Hochberg 1995) instead bounds the
EXPECTED FRACTION of false discoveries among each cycle's emissions at q:
sort the m p-values ascending, find the largest k with

    p_(k) <= (k / m) * q

and keep exactly the hypotheses with p <= p_(k).

The remaining helpers are the method-by-data-shape toolbox from the plan:
Pearson (linear) vs Spearman (monotone) keeping whichever fits better,
time-lagged cross-correlation for lead-lag propagation, histogram mutual
information for nonlinear dependence, and Jaccard overlap for binary
error-presence co-occurrence.
"""

from __future__ import annotations

from collections.abc import Iterator

import numpy as np
from scipy import stats as _stats

#: Minimum finite sample pairs before any correlation statistic is attempted.
_MIN_PAIRS = 10


# --- Multiple-testing control (BH-FDR) --------------------------------------------
def benjamini_hochberg(pvals: np.ndarray, q: float = 0.05) -> np.ndarray:
    """BH step-up keep-mask at FDR level ``q``, in the ORIGINAL input order.

    Classic Benjamini-Hochberg (1995): with p-values sorted ascending
    p_(1) <= ... <= p_(m), find the largest k such that
    ``p_(k) <= (k / m) * q`` and keep exactly the hypotheses with p <= p_(k)
    (step-up: everything at or below the winning rank is kept, even rows whose
    own per-rank test failed). Returns a bool array aligned with ``pvals`` —
    all False when no k qualifies, empty for empty input. Non-finite p-values
    are treated as 1.0 (never kept).
    """
    p = np.asarray(pvals, dtype=float)
    if p.size == 0:
        return np.zeros(0, dtype=bool)
    clean = np.where(np.isfinite(p), p, 1.0)
    m = clean.size
    ranked = np.sort(clean)
    passing = np.nonzero(ranked <= (np.arange(1, m + 1) / m) * q)[0]
    if passing.size == 0:
        return np.zeros(m, dtype=bool)
    return clean <= ranked[passing[-1]]


def bh_adjusted(pvals: np.ndarray) -> np.ndarray:
    """BH step-up adjusted p-values (q-values), in the ORIGINAL input order.

    For the sorted p-values, ``p_adj_(i) = min over j >= i of (m / j) * p_(j)``
    clipped to 1 — the smallest FDR level at which hypothesis i would still be
    kept, so ``bh_adjusted(p) <= q`` reproduces :func:`benjamini_hochberg`'s
    keep-mask. Monotone non-decreasing along the sorted order by construction.
    Non-finite p-values are treated as 1.0.
    """
    p = np.asarray(pvals, dtype=float)
    if p.size == 0:
        return np.zeros(0)
    clean = np.where(np.isfinite(p), p, 1.0)
    m = clean.size
    order = np.argsort(clean, kind="stable")
    scaled = clean[order] * (m / np.arange(1, m + 1))
    adjusted = np.minimum(np.minimum.accumulate(scaled[::-1])[::-1], 1.0)
    out = np.empty(m)
    out[order] = adjusted
    return out


# --- Correlation statistics ---------------------------------------------------------
def lagged_xcorr(
    a: np.ndarray, b: np.ndarray, max_lag: int = 10
) -> tuple[int, float, float]:
    """Best time-lagged Pearson cross-correlation over lags -max_lag..+max_lag.

    A POSITIVE lag L pairs ``a[t]`` with ``b[t + L]`` — "b lags a by L
    seconds", i.e. a leads and is the candidate cause of b. Negative lags test
    the reverse direction (a lags b). Each lag's overlapping samples are
    pairwise finite-masked and scored with ``scipy.stats.pearsonr``; lags with
    fewer than 10 finite pairs or zero variance on either side are skipped.

    Returns ``(best_lag, r_at_best, p_at_best)`` where "best" maximizes |r|
    (ties keep the smaller |lag|, positive direction first), or
    ``(0, nan, 1.0)`` when no lag is computable at all.
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    best: tuple[int, float, float] | None = None
    for lag in _lags_by_distance(max_lag):
        x, y = _finite_xy(*_shifted(a, b, lag))
        if x.size < _MIN_PAIRS or _constant(x) or _constant(y):
            continue
        r, p = _stat_pair(_stats.pearsonr, x, y)
        if not (np.isfinite(r) and np.isfinite(p)):
            continue
        if best is None or abs(r) > abs(best[1]):
            best = (lag, r, p)
    return best if best is not None else (0, float("nan"), 1.0)


def mutual_information(a: np.ndarray, b: np.ndarray, bins: int = 8) -> float:
    """Normalized histogram mutual information in [0, 1].

    Pairwise finite-masked samples are binned by ``np.histogram2d`` into a
    ``bins x bins`` joint distribution P, then (natural log)

        MI = sum_ij p_ij * log(p_ij / (p_i. * p_.j))

    normalized by ``min(H(a), H(b))`` so 1.0 means one series pins down the
    other's bin exactly — this catches dependence Pearson is blind to (e.g.
    y = x^2). Returns 0.0 for anything degenerate: fewer than 10 finite pairs,
    zero variance on either side, or zero marginal entropy.
    """
    x, y = _finite_xy(np.asarray(a, dtype=float), np.asarray(b, dtype=float))
    if x.size < _MIN_PAIRS or _constant(x) or _constant(y):
        return 0.0
    joint, _, _ = np.histogram2d(x, y, bins=bins)
    total = joint.sum()
    if total <= 0.0:
        return 0.0
    pxy = joint / total
    px = pxy.sum(axis=1)
    py = pxy.sum(axis=0)
    nonzero = pxy > 0.0
    mi = float(np.sum(pxy[nonzero] * np.log(pxy[nonzero] / np.outer(px, py)[nonzero])))
    h_x = float(-np.sum(px[px > 0.0] * np.log(px[px > 0.0])))
    h_y = float(-np.sum(py[py > 0.0] * np.log(py[py > 0.0])))
    h_min = min(h_x, h_y)
    if h_min <= 0.0:
        return 0.0
    return min(1.0, max(0.0, mi / h_min))


def jaccard(a_bin: np.ndarray, b_bin: np.ndarray) -> tuple[float, int]:
    """(Jaccard overlap, union size) of two binary series.

    Samples are pairwise finite-masked, then binarized at > 0.5:
    ``J = |a AND b| / |a OR b|``. Returns ``(0.0, 0)`` when the union is empty
    — no activity on either side is no evidence of anything.
    """
    x, y = _finite_xy(np.asarray(a_bin, dtype=float), np.asarray(b_bin, dtype=float))
    a_on = x > 0.5
    b_on = y > 0.5
    union = int(np.count_nonzero(a_on | b_on))
    if union == 0:
        return 0.0, 0
    return int(np.count_nonzero(a_on & b_on)) / union, union


def pearson_or_spearman(
    a: np.ndarray, b: np.ndarray, min_n: int = 10
) -> tuple[str, float, float, int] | None:
    """The better of Pearson (linear) and Spearman (monotone) for one pair.

    Pairwise finite-masks the samples, computes BOTH ``scipy.stats.pearsonr``
    and ``spearmanr``, and returns ``(method, r, p, n)`` for whichever
    achieved the smaller p-value (ties prefer "pearson"). None when the pair
    is untestable: fewer than ``min_n`` finite pairs, zero variance on either
    side, or neither statistic finite.
    """
    x, y = _finite_xy(np.asarray(a, dtype=float), np.asarray(b, dtype=float))
    n = int(x.size)
    if n < min_n or _constant(x) or _constant(y):
        return None
    options = [
        ("pearson", *_stat_pair(_stats.pearsonr, x, y)),
        ("spearman", *_stat_pair(_stats.spearmanr, x, y)),
    ]
    usable = [
        (method, r, p) for method, r, p in options if np.isfinite(r) and np.isfinite(p)
    ]
    if not usable:
        return None
    method, r, p = min(usable, key=lambda option: option[2])
    return method, r, p, n


# --- Internals ----------------------------------------------------------------------
def _lags_by_distance(max_lag: int) -> Iterator[int]:
    """0, +1, -1, +2, -2, ... — so |r| ties resolve to the smallest shift."""
    yield 0
    for lag in range(1, max_lag + 1):
        yield lag
        yield -lag


def _shifted(a: np.ndarray, b: np.ndarray, lag: int) -> tuple[np.ndarray, np.ndarray]:
    """Overlapping views pairing ``a[t]`` with ``b[t + lag]`` (empty if |lag| too big)."""
    if lag > 0:
        return a[:-lag], b[lag:]
    if lag < 0:
        return a[-lag:], b[:lag]
    return a, b


def _finite_xy(a: np.ndarray, b: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Pairwise finite-masked copies of ``a``/``b`` (a pair with any NaN/inf drops)."""
    mask = np.isfinite(a) & np.isfinite(b)
    return a[mask], b[mask]


def _constant(x: np.ndarray) -> bool:
    """True when ``x`` has no variance (correlation is undefined on it)."""
    return x.size == 0 or bool(np.all(x == x[0]))


def _stat_pair(func, x: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    """(statistic, p) from a scipy correlation callable; (nan, nan) on failure."""
    try:
        result = func(x, y)
        return float(result[0]), float(result[1])
    except Exception:  # noqa: BLE001 — degenerate input; the caller skips it
        return float("nan"), float("nan")
