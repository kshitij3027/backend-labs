import pytest
from src.metrics_core import PressureFuser, PressureMetrics


def _m(qdr=0.0, lag=0.0, cpu=0.0, mem=0.0, ts=0.0):
    return PressureMetrics(queue_depth_ratio=qdr, normalized_lag=lag, cpu=cpu, mem=mem, ts=ts)


def test_idle_score_is_zero():
    f = PressureFuser(alpha=0.3, history_size=10)
    assert f.fuse(_m()) == pytest.approx(0.0)


def test_saturated_all_dims_score_reaches_high():
    f = PressureFuser(alpha=0.3, history_size=10)
    last = 0.0
    for i in range(20):
        last = f.fuse(_m(qdr=1.0, lag=1.0, cpu=1.0, mem=1.0, ts=float(i)))
    assert last >= 0.9


def test_ewma_converges_within_ten_samples():
    f = PressureFuser(alpha=0.3, history_size=20)
    for i in range(10):
        score = f.fuse(_m(qdr=0.5, lag=0.5, cpu=0.5, mem=0.5, ts=float(i)))
    assert abs(score - 0.5) <= 0.05


def test_max_branch_dominates_when_one_dim_is_high():
    f = PressureFuser(alpha=1.0, history_size=10)
    score = f.fuse(_m(qdr=0.0, lag=0.0, cpu=0.9, mem=0.0, ts=0.0))
    assert score == pytest.approx(0.9)


def test_history_bounded():
    f = PressureFuser(alpha=0.3, history_size=5)
    for i in range(20):
        f.fuse(_m(qdr=0.1, ts=float(i)))
    hist = f.history()
    assert len(hist) == 5
    assert hist[0][0] == 15.0


def test_alpha_setter_takes_effect_next_fuse():
    f = PressureFuser(alpha=0.1, history_size=10)
    f.fuse(_m(qdr=1.0, ts=0.0))
    s1 = f.last_score
    f.alpha = 1.0
    f.fuse(_m(qdr=1.0, ts=1.0))
    s2 = f.last_score
    assert s2 > s1
    assert s2 == pytest.approx(1.0)
