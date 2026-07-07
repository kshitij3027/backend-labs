"""Unit tests for the significance helpers (BH-FDR, TLCC, MI, Jaccard).

Benjamini-Hochberg is pinned against two published worked examples with
hand-verified outcomes, plus hand-computed adjusted p-values. The correlation
statistics run on seeded synthetic data with margins wide enough that the
assertions follow from construction, not seed luck (e.g. a planted 3-second
lead-lag recovers r ~ 0.999 while every other lag stays at white-noise level).
"""

import numpy as np
import pytest

from src.engine.significance import (
    benjamini_hochberg,
    bh_adjusted,
    jaccard,
    lagged_xcorr,
    mutual_information,
    pearson_or_spearman,
)

#: Benjamini & Hochberg (1995), section 3.1: m=15 p-values at q=0.05 — the
#: paper rejects exactly the smallest four (largest k with p_(k) <= k*0.05/15
#: is k=4: 0.0095 <= 0.01333, while k=5: 0.0201 > 0.01667).
BH1995_PVALS = np.array(
    [
        0.0001, 0.0004, 0.0019, 0.0095, 0.0201, 0.0278, 0.0298, 0.0344,
        0.0459, 0.3240, 0.4262, 0.5719, 0.6528, 0.7590, 1.0000,
    ]
)

#: McDonald's Handbook of Biological Statistics worked vector (m=24, q=0.25).
#: Largest k with p_(k) <= (k/24)*0.25: k=6 (0.060 <= 0.0625; k=7 fails with
#: 0.074 > 0.07292), so the keep-mask is every p <= 0.06 — six hypotheses,
#: including 0.039 whose OWN rank test fails (the step-up property).
HANDBOOK_PVALS = np.array(
    [
        0.001, 0.008, 0.039, 0.041, 0.042, 0.06, 0.074, 0.205, 0.212, 0.216,
        0.222, 0.251, 0.269, 0.275, 0.34, 0.341, 0.384, 0.569, 0.594, 0.696,
        0.762, 0.94, 0.942, 0.975,
    ]
)


# --- benjamini_hochberg -------------------------------------------------------------
def test_bh_1995_paper_example_keeps_exactly_first_four():
    mask = benjamini_hochberg(BH1995_PVALS, q=0.05)
    assert mask.dtype == bool
    assert mask.tolist() == [True] * 4 + [False] * 11


def test_bh_handbook_vector_keeps_every_p_up_to_006():
    mask = benjamini_hochberg(HANDBOOK_PVALS, q=0.25)
    assert mask.tolist() == [True] * 6 + [False] * 18


def test_bh_scrambled_order_preserves_mask_correctness():
    rng = np.random.default_rng(1)
    perm = rng.permutation(HANDBOOK_PVALS.size)
    mask = benjamini_hochberg(HANDBOOK_PVALS[perm], q=0.25)
    assert int(mask.sum()) == 6
    kept = sorted(HANDBOOK_PVALS[perm][mask].tolist())
    assert kept == [0.001, 0.008, 0.039, 0.041, 0.042, 0.06]


def test_bh_empty_input_yields_empty_mask():
    mask = benjamini_hochberg(np.array([]), q=0.05)
    assert mask.size == 0
    assert mask.dtype == bool


def test_bh_all_high_pvalues_keep_nothing():
    assert not benjamini_hochberg(np.array([0.2, 0.5, 0.9]), q=0.05).any()


def test_bh_single_pvalue_compares_against_q_directly():
    assert benjamini_hochberg(np.array([0.04]), q=0.05).tolist() == [True]
    assert benjamini_hochberg(np.array([0.06]), q=0.05).tolist() == [False]


# --- bh_adjusted --------------------------------------------------------------------
def test_bh_adjusted_matches_hand_computed_values():
    # p_adj_(i) = min over j >= i of (24/j) * p_(j), e.g. the head of the
    # handbook vector: 24*0.001, 12*0.008, then min(8*0.039, 6*0.041,
    # 4.8*0.042, ...) = 4.8*0.042 = 0.2016 shared by ranks 3-5, then
    # 4*0.06 = 0.24 and (24/7)*0.074 = 0.253714...
    adjusted = bh_adjusted(HANDBOOK_PVALS)
    assert adjusted[0] == pytest.approx(0.024, abs=1e-9)
    assert adjusted[1] == pytest.approx(0.096, abs=1e-9)
    assert adjusted[2] == pytest.approx(0.2016, abs=1e-9)
    assert adjusted[3] == pytest.approx(0.2016, abs=1e-9)
    assert adjusted[4] == pytest.approx(0.2016, abs=1e-9)
    assert adjusted[5] == pytest.approx(0.24, abs=1e-9)
    assert adjusted[6] == pytest.approx(24 / 7 * 0.074, abs=1e-9)


def test_bh_adjusted_is_monotone_capped_and_above_raw_min():
    adjusted = bh_adjusted(HANDBOOK_PVALS)  # input already sorted ascending
    assert np.all(np.diff(adjusted) >= -1e-12)  # monotone non-decreasing
    assert np.all(adjusted <= 1.0)
    assert adjusted.min() > HANDBOOK_PVALS.min()
    assert bh_adjusted(np.array([])).size == 0


def test_bh_adjusted_threshold_reproduces_keep_mask():
    # keep(p, q) is exactly p_adj <= q — on both vectors, in scrambled order.
    rng = np.random.default_rng(2)
    for pvals, q in ((HANDBOOK_PVALS, 0.25), (BH1995_PVALS, 0.05)):
        shuffled = pvals[rng.permutation(pvals.size)]
        assert np.array_equal(
            benjamini_hochberg(shuffled, q=q), bh_adjusted(shuffled) <= q
        )


# --- lagged_xcorr -------------------------------------------------------------------
def test_lagged_xcorr_recovers_planted_three_second_lead():
    rng = np.random.default_rng(3)
    x = rng.normal(0.0, 1.0, 60)
    # y[t] = x[t-3] + tiny noise, built by concatenation (no roll wraparound):
    # b lags a by 3 seconds, so the detector-facing convention reports +3.
    y = np.concatenate([rng.normal(0.0, 1.0, 3), x[:-3]]) + rng.normal(0.0, 0.05, 60)
    lag, r, p = lagged_xcorr(x, y)
    assert lag == 3
    assert r > 0.9
    assert p < 1e-6


def test_lagged_xcorr_reports_negative_lag_when_roles_swap():
    rng = np.random.default_rng(3)
    x = rng.normal(0.0, 1.0, 60)
    y = np.concatenate([rng.normal(0.0, 1.0, 3), x[:-3]]) + rng.normal(0.0, 0.05, 60)
    lag, r, _ = lagged_xcorr(y, x)  # now the first series is the lagging one
    assert lag == -3
    assert r > 0.9


def test_lagged_xcorr_independent_noise_finds_nothing_strong():
    rng = np.random.default_rng(8)
    lag, r, p = lagged_xcorr(rng.normal(0.0, 1.0, 60), rng.normal(0.0, 1.0, 60))
    assert -10 <= lag <= 10
    assert abs(r) < 0.6 or p > 0.01


def test_lagged_xcorr_degenerate_inputs_return_sentinel():
    rng = np.random.default_rng(4)
    # Zero variance on one side: no lag is computable.
    lag, r, p = lagged_xcorr(np.full(60, 3.14), rng.normal(0.0, 1.0, 60))
    assert (lag, p) == (0, 1.0) and np.isnan(r)
    # Too short for 10 finite pairs at any lag.
    lag, r, p = lagged_xcorr(np.arange(5.0), np.arange(5.0))
    assert (lag, p) == (0, 1.0) and np.isnan(r)


# --- mutual_information -------------------------------------------------------------
def test_mutual_information_detects_nonlinear_dependence():
    rng = np.random.default_rng(5)
    x = rng.uniform(-1.0, 1.0, 200)
    assert mutual_information(x, x**2) > 0.4  # Pearson-blind (r ~ 0), MI is not


def test_mutual_information_near_zero_for_independent_noise():
    rng = np.random.default_rng(6)
    a = rng.uniform(-1.0, 1.0, 200)
    b = rng.uniform(-1.0, 1.0, 200)
    assert mutual_information(a, b) < 0.2


def test_mutual_information_zero_for_degenerate_input():
    rng = np.random.default_rng(7)
    assert mutual_information(np.full(50, 1.0), rng.normal(0.0, 1.0, 50)) == 0.0
    assert mutual_information(np.arange(5.0), np.arange(5.0)) == 0.0  # n < 10


# --- jaccard ------------------------------------------------------------------------
def test_jaccard_hand_built_overlap():
    a = np.array([1.0, 1.0, 0.0, 0.0, 1.0])
    b = np.array([1.0, 0.0, 0.0, 1.0, 1.0])
    # intersection {0, 4} = 2, union {0, 1, 3, 4} = 4.
    assert jaccard(a, b) == (0.5, 4)


def test_jaccard_disjoint_and_empty_union():
    overlap, union = jaccard(np.array([1.0, 1.0, 0.0]), np.array([0.0, 0.0, 1.0]))
    assert overlap == 0.0 and union == 3
    assert jaccard(np.zeros(6), np.zeros(6)) == (0.0, 0)


def test_jaccard_masks_non_finite_pairs():
    a = np.array([1.0, np.nan, 1.0])
    b = np.array([1.0, 1.0, np.nan])
    assert jaccard(a, b) == (1.0, 1)  # only index 0 survives the pairwise mask


# --- pearson_or_spearman ------------------------------------------------------------
def test_monotone_nonlinear_relationship_prefers_spearman():
    rng = np.random.default_rng(11)
    x = rng.uniform(-1.0, 1.0, 100)
    y = x**3 + rng.normal(0.0, 0.01, 100)  # monotone but strongly nonlinear
    result = pearson_or_spearman(x, y)
    assert result is not None
    method, r, p, n = result
    assert method == "spearman"  # ranks are near-perfect; raw r is only ~0.92
    assert r > 0.95
    assert p < 1e-6
    assert n == 100


def test_linear_relationship_scores_high_either_way():
    rng = np.random.default_rng(12)
    x = rng.uniform(-1.0, 1.0, 100)
    y = 2.0 * x + rng.normal(0.0, 0.05, 100)
    result = pearson_or_spearman(x, y)
    assert result is not None
    method, r, p, n = result
    assert method in ("pearson", "spearman")
    assert abs(r) > 0.95
    assert n == 100


def test_constant_or_short_input_returns_none():
    rng = np.random.default_rng(13)
    assert pearson_or_spearman(np.full(60, 2.0), rng.normal(0.0, 1.0, 60)) is None
    assert pearson_or_spearman(np.arange(5.0), np.arange(5.0)) is None  # n < 10


def test_nan_pairs_are_masked_pairwise():
    rng = np.random.default_rng(14)
    x = rng.uniform(0.0, 1.0, 60)
    y = 2.0 * x + rng.normal(0.0, 0.05, 60)
    x[5] = np.nan
    y[10] = np.nan
    result = pearson_or_spearman(x, y)
    assert result is not None
    _, r, _, n = result
    assert n == 58  # two distinct positions dropped
    assert abs(r) > 0.95
