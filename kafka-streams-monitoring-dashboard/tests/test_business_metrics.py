"""Tests for BusinessMetricsTracker."""

import threading

import pytest

from src.business_metrics import BusinessMetricsTracker


@pytest.fixture
def tracker():
    return BusinessMetricsTracker(max_users=100)


# ── API version tracking ────────────────────────────────────────

def test_track_api_version_v1(tracker):
    tracker.track_api_version("/api/v1/users")
    metrics = tracker.get_business_metrics()
    assert metrics["api_versions"]["v1"] == 1


def test_track_api_version_v2(tracker):
    tracker.track_api_version("/api/v2/orders")
    metrics = tracker.get_business_metrics()
    assert metrics["api_versions"]["v2"] == 1


def test_track_api_version_unversioned(tracker):
    tracker.track_api_version("/checkout")
    metrics = tracker.get_business_metrics()
    assert metrics["api_versions"]["unversioned"] == 1


def test_track_api_version_none(tracker):
    # Should not crash
    tracker.track_api_version(None)
    metrics = tracker.get_business_metrics()
    assert metrics["api_versions"] == {}


def test_track_multiple_versions(tracker):
    tracker.track_api_version("/api/v1/users")
    tracker.track_api_version("/api/v1/products")
    tracker.track_api_version("/api/v2/orders")
    tracker.track_api_version("/checkout")

    metrics = tracker.get_business_metrics()
    assert metrics["api_versions"]["v1"] == 2
    assert metrics["api_versions"]["v2"] == 1
    assert metrics["api_versions"]["unversioned"] == 1


# ── Payment funnel tracking ─────────────────────────────────────

def test_track_payment_funnel(tracker):
    tracker.track_payment_funnel("/products", "page_view")
    tracker.track_payment_funnel("/cart", "add_to_cart")
    tracker.track_payment_funnel("/checkout", "")
    tracker.track_payment_funnel("/payment", "")
    tracker.track_payment_funnel("", "purchase")

    metrics = tracker.get_business_metrics()
    funnel = metrics["funnel"]
    assert funnel["browse"] == 1
    assert funnel["add_to_cart"] == 1
    assert funnel["checkout"] == 1
    assert funnel["payment"] == 1
    assert funnel["confirmation"] == 1


# ── Auth tracking ────────────────────────────────────────────────

def test_track_auth_success(tracker):
    tracker.track_auth_event("login", success=True)
    metrics = tracker.get_business_metrics()
    assert metrics["auth"]["success"] == 1
    assert metrics["auth"]["failure"] == 0


def test_track_auth_failure(tracker):
    tracker.track_auth_event("login", success=False)
    metrics = tracker.get_business_metrics()
    assert metrics["auth"]["success"] == 0
    assert metrics["auth"]["failure"] == 1


def test_auth_failure_rate(tracker):
    tracker.track_auth_event("login", success=True)
    tracker.track_auth_event("login", success=True)
    tracker.track_auth_event("login", success=True)
    tracker.track_auth_event("login", success=False)

    metrics = tracker.get_business_metrics()
    assert metrics["auth"]["failure_rate"] == 25.0


def test_auth_ignores_irrelevant_actions(tracker):
    tracker.track_auth_event("page_view", success=True)
    tracker.track_auth_event("purchase", success=False)
    metrics = tracker.get_business_metrics()
    assert metrics["auth"]["success"] == 0
    assert metrics["auth"]["failure"] == 0


# ── Structure & thread safety ────────────────────────────────────

def test_get_business_metrics_structure(tracker):
    metrics = tracker.get_business_metrics()
    assert "api_versions" in metrics
    assert "funnel" in metrics
    assert "auth" in metrics
    assert "success" in metrics["auth"]
    assert "failure" in metrics["auth"]
    assert "failure_rate" in metrics["auth"]


def test_funnel_user_eviction(tracker):
    """When max_users is exceeded, oldest entries are evicted."""
    tracker._max_users = 5
    for i in range(10):
        tracker.track_payment_funnel(f"/products", "page_view")
    metrics = tracker.get_business_metrics()
    assert metrics["funnel"]["browse"] == 10


def test_infer_funnel_stage_pay_action(tracker):
    """Action 'pay' maps to payment stage."""
    tracker.track_payment_funnel("", "pay")
    metrics = tracker.get_business_metrics()
    assert metrics["funnel"]["payment"] == 1


def test_infer_funnel_stage_no_match(tracker):
    """Empty path and empty action returns None (no funnel stage)."""
    tracker.track_payment_funnel("", "")
    metrics = tracker.get_business_metrics()
    # All stages should remain at 0
    assert all(v == 0 for v in metrics["funnel"].values())


def test_auth_signup_success(tracker):
    tracker.track_auth_event("signup", success=True)
    metrics = tracker.get_business_metrics()
    assert metrics["auth"]["success"] == 1


def test_auth_signup_failure(tracker):
    tracker.track_auth_event("signup", success=False)
    metrics = tracker.get_business_metrics()
    assert metrics["auth"]["failure"] == 1


def test_auth_logout_ignored(tracker):
    """Logout action is in the accepted set but doesn't count as success/failure."""
    tracker.track_auth_event("logout", success=True)
    metrics = tracker.get_business_metrics()
    assert metrics["auth"]["success"] == 0
    assert metrics["auth"]["failure"] == 0


def test_thread_safety(tracker):
    """Concurrent access should not crash."""
    errors = []

    def worker():
        try:
            for _ in range(200):
                tracker.track_api_version("/api/v1/test")
                tracker.track_payment_funnel("/cart", "add_to_cart")
                tracker.track_auth_event("login", success=True)
                tracker.get_business_metrics()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
