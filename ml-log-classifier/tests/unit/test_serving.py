"""Unit tests for :mod:`src.serving` (Commit 13 — A/B serving + graceful fallback).

Exercises :class:`src.serving.ABRouter` directly against a **real**
:class:`src.model_store.ModelRegistry` holding two tiny, real
:class:`src.ensemble.LogClassifier` versions (``v1`` / ``v2``):

* default A/B config from the registry (A = current/champion, B = latest),
* deterministic, split-respecting :meth:`~ABRouter.assign` (seeded RNG; the
  0.0 / 1.0 / 0.3 corner + fractional cases),
* the three serving keys added to :meth:`~ABRouter.classify` on the happy path,
* **graceful fallback** when the assigned version's cached classifier is poisoned
  (A answers when B is broken), and the per-version metric bookkeeping that goes
  with it,
* the all-versions-broken case raising ``RuntimeError`` (the API maps to 503),
* :meth:`~ABRouter.promote` repointing both the registry's ``current`` and group A,
* the :meth:`~ABRouter.metrics` / :meth:`~ABRouter.models` introspection shapes,
* and ``configure`` rejecting an unknown version id.

Two tiny dual-ensemble classifiers (RF=4 + GB=4 on 60 deterministic records each)
are fitted **once** per module and saved as ``v1`` / ``v2`` into a shared temp
registry — the router under test never trains, it only routes/loads, so two cheap
real models are enough to cover every path while keeping the suite fast.
"""

from __future__ import annotations

import pytest

from src.config import Settings
from src.ensemble import LogClassifier
from src.log_generator import generate_logs
from src.model_store import ModelRegistry
from src.serving import ABRouter

#: Canonical line used to confirm a router can actually classify (spec headline).
CANONICAL_INPUT = "Database connection failed with timeout error"

#: The five keys every underlying classification result carries.
BASE_RESULT_KEYS = {
    "severity",
    "category",
    "confidence",
    "severity_confidence",
    "category_confidence",
}

#: The three serving keys :meth:`ABRouter.classify` adds on top of the base five.
SERVING_KEYS = {"model_version", "ab_group", "fallback_used"}


class _BoomClassifier:
    """A stub classifier whose :meth:`classify` always raises (poisons a cache slot).

    Dropping one of these into ``router._cache[<vid>]`` simulates a version whose
    on-disk artifacts are mid-swap / corrupt: the cached object exists (so it is a
    candidate) but blows up the moment the router tries to serve from it, which is
    exactly the condition graceful fallback must paper over.
    """

    def classify(self, *args, **kwargs):  # noqa: D401 - intentionally explosive
        raise RuntimeError("boom: this version cannot serve")


@pytest.fixture(scope="module")
def two_version_registry(tmp_path_factory) -> ModelRegistry:
    """A real registry with two tiny, real versions ``v1`` and ``v2``.

    ``v1`` is fit on ``generate_logs(60, 42)`` and ``v2`` on ``generate_logs(60, 7)``
    — different seeds so the two models are genuinely distinct — each with a 4-tree
    RF + 4-stage GB ensemble (fast). ``save_version`` mints ``v1`` then ``v2`` and
    leaves ``v2`` as ``current`` (the registry's default ``make_current=True``).
    Module-scoped so the two real fits happen exactly once.
    """
    model_dir = tmp_path_factory.mktemp("ab_models")
    cfg = Settings(rf_n_estimators=4, gb_n_estimators=4, model_dir=str(model_dir))
    registry = ModelRegistry(str(model_dir))

    clf_v1 = LogClassifier(cfg).fit(generate_logs(60, 42))
    registry.save_version(clf_v1, {"severity_test_accuracy": 0.9})  # -> v1 (current)
    clf_v2 = LogClassifier(cfg).fit(generate_logs(60, 7))
    registry.save_version(clf_v2, {"severity_test_accuracy": 0.92})  # -> v2 (current)
    return registry


@pytest.fixture
def router(two_version_registry) -> ABRouter:
    """A seeded :class:`ABRouter` over the two-version registry, defaults loaded.

    Seeded (``seed=1234``) so :meth:`~ABRouter.assign` is deterministic, and
    pre-configured via :meth:`~ABRouter.set_default_from_registry` so A = current
    (``v2``) and B = latest (``v2``); individual tests reconfigure as needed.
    Function-scoped so each test gets a fresh router (clean cache + metrics) over
    the shared (module-scoped) registry.
    """
    r = ABRouter(two_version_registry, split_b=0.5, seed=1234)
    r.set_default_from_registry()
    return r


# --------------------------------------------------------------------------- #
# set_default_from_registry — A = champion (current), B = challenger (latest)
# --------------------------------------------------------------------------- #


def test_set_default_from_registry_a_is_current_b_is_latest(two_version_registry):
    """Defaults bind A to the registry's ``current`` and B to its ``latest``."""
    reg = two_version_registry
    r = ABRouter(reg, seed=1)
    r.set_default_from_registry()

    assert r.a_version == reg.current_version  # champion == current (v2 here)
    assert r.b_version == reg.latest()  # challenger == latest (v2 here)
    # With two saves and the second left current, both resolve to v2.
    assert r.a_version == "v2"
    assert r.b_version == "v2"


# --------------------------------------------------------------------------- #
# assign — deterministic, split-respecting routing
# --------------------------------------------------------------------------- #


def test_assign_is_deterministic_for_a_given_seed(two_version_registry):
    """Two routers with the same seed produce an identical assignment stream."""
    reg = two_version_registry
    r1 = ABRouter(reg, split_b=0.5, seed=99)
    r2 = ABRouter(reg, split_b=0.5, seed=99)
    seq1 = [r1.assign() for _ in range(50)]
    seq2 = [r2.assign() for _ in range(50)]
    assert seq1 == seq2
    # Sanity: a 0.5 split with this seed actually exercises both arms.
    assert set(seq1) == {"A", "B"}


def test_assign_observed_b_fraction_matches_split(two_version_registry):
    """Over 2000 draws at split_b=0.3 the observed B-fraction is ~0.3 (±0.05)."""
    r = ABRouter(two_version_registry, split_b=0.3, seed=2026)
    n = 2000
    b = sum(1 for _ in range(n) if r.assign() == "B")
    frac = b / n
    assert 0.25 <= frac <= 0.35, f"observed B-fraction {frac:.3f} not within 0.3±0.05"


def test_assign_split_zero_is_always_a(two_version_registry):
    """split_b == 0.0 routes every request to group A."""
    r = ABRouter(two_version_registry, split_b=0.0, seed=7)
    assert all(r.assign() == "A" for _ in range(500))


def test_assign_split_one_is_always_b(two_version_registry):
    """split_b == 1.0 routes every request to group B."""
    r = ABRouter(two_version_registry, split_b=1.0, seed=7)
    assert all(r.assign() == "B" for _ in range(500))


# --------------------------------------------------------------------------- #
# classify — happy path adds the three serving keys
# --------------------------------------------------------------------------- #


def test_classify_happy_path_adds_serving_keys(router):
    """A served request returns the 5 base keys + the 3 serving keys, no fallback."""
    # Distinct A and B so model_version is unambiguous; force a 50/50 mix isn't
    # needed — just confirm whichever version served is one of the configured ids.
    router.configure(a_version="v1", b_version="v2", split_b=0.5)
    result = router.classify(CANONICAL_INPUT)

    assert BASE_RESULT_KEYS.issubset(result), f"missing base keys: {sorted(result)}"
    assert SERVING_KEYS.issubset(result), f"missing serving keys: {sorted(result)}"
    assert result["model_version"] in {"v1", "v2"}
    assert result["ab_group"] in {"A", "B"}
    assert result["fallback_used"] is False
    assert 0.0 <= float(result["confidence"]) <= 1.0


# --------------------------------------------------------------------------- #
# classify — graceful fallback when the assigned version is poisoned
# --------------------------------------------------------------------------- #


def test_classify_falls_back_to_a_when_b_is_poisoned(router):
    """Forcing B and poisoning v2's cache makes the router fall back to A (v1)."""
    # Pin every request to B (=v2) so the assigned version is deterministic.
    router.configure(a_version="v1", b_version="v2", split_b=1.0)
    # Poison the assigned (B=v2) classifier so serving it raises.
    router._cache["v2"] = _BoomClassifier()

    result = router.classify(CANONICAL_INPUT)

    # Still a valid classification — fallback answered.
    assert BASE_RESULT_KEYS.issubset(result), f"missing base keys: {sorted(result)}"
    assert result["fallback_used"] is True
    assert result["ab_group"] == "B"  # request was *assigned* to B ...
    assert result["model_version"] == "v1"  # ... but A (v1) actually served.

    # Metrics: v2 booked an error + a fallback; v1 served at least one request.
    metrics = router.metrics()["per_version"]
    assert metrics["v2"]["errors"] >= 1
    assert metrics["v2"]["fallbacks"] >= 1
    assert metrics["v1"]["requests"] >= 1


def test_classify_falls_back_when_assigned_slot_is_none(router):
    """A ``None`` cache slot for the assigned version also triggers graceful fallback."""
    router.configure(a_version="v1", b_version="v2", split_b=1.0)
    router._cache["v2"] = None  # assigned version has no usable classifier

    result = router.classify(CANONICAL_INPUT)

    assert BASE_RESULT_KEYS.issubset(result)
    assert result["fallback_used"] is True
    assert result["model_version"] == "v1"


# --------------------------------------------------------------------------- #
# classify — nothing can serve -> RuntimeError (API maps to 503)
# --------------------------------------------------------------------------- #


def test_classify_raises_when_all_versions_broken(router):
    """When both A and B (and the champion) are poisoned, classify raises RuntimeError."""
    router.configure(a_version="v1", b_version="v2", split_b=0.5)
    # Poison every candidate the router could reach: both groups and the champion.
    router._cache["v1"] = _BoomClassifier()
    router._cache["v2"] = _BoomClassifier()

    with pytest.raises(RuntimeError):
        router.classify(CANONICAL_INPUT)


# --------------------------------------------------------------------------- #
# promote — repoints registry.current AND group A
# --------------------------------------------------------------------------- #


def test_promote_updates_registry_current_and_group_a(router, two_version_registry):
    """Promoting v1 makes it the registry's current version and the A/B champion."""
    router.configure(a_version="v2", b_version="v2", split_b=0.5)
    assert router.promote("v1") == "v1"

    assert two_version_registry.current_version == "v1"
    assert router.a_version == "v1"
    # Restore current to v2 so the module-scoped registry is left as other tests
    # (and the default fixture) expect.
    two_version_registry.set_current("v2")


# --------------------------------------------------------------------------- #
# metrics / models — per-version counts + annotations
# --------------------------------------------------------------------------- #


def test_metrics_counts_increment_with_served_requests(router):
    """Per-version request counts grow as traffic is served through the router."""
    router.configure(a_version="v1", b_version="v1", split_b=0.0)  # pin everything to A=v1
    before = router.metrics()["per_version"].get("v1", {}).get("requests", 0)

    for _ in range(5):
        router.classify(CANONICAL_INPUT)

    after = router.metrics()["per_version"]["v1"]["requests"]
    assert after == before + 5, f"v1 requests went {before} -> {after} (expected +5)"


def test_metrics_top_level_shape(router):
    """``metrics()`` exposes the A/B config plus a per-version mapping."""
    router.configure(a_version="v1", b_version="v2", split_b=0.4)
    m = router.metrics()
    assert m["a_version"] == "v1"
    assert m["b_version"] == "v2"
    assert m["split_b"] == pytest.approx(0.4)
    assert isinstance(m["per_version"], dict)


def test_models_annotates_champion_group_and_metrics(router, two_version_registry):
    """``models()`` annotates each version with is_champion / ab_group / serving_metrics."""
    router.configure(a_version="v1", b_version="v2", split_b=0.5)
    # Serve a couple of A-only requests so v1 has non-zero serving metrics.
    router.configure(split_b=0.0)  # pin to A=v1
    router.classify(CANONICAL_INPUT)
    router.classify(CANONICAL_INPUT)

    entries = router.models()
    by_id = {e["version"]: e for e in entries}
    assert {"v1", "v2"}.issubset(by_id)

    current = two_version_registry.current_version
    for vid, entry in by_id.items():
        assert "is_champion" in entry
        assert "ab_group" in entry
        assert "serving_metrics" in entry
        assert entry["is_champion"] == (vid == current)
        # serving_metrics always carries the public per-version shape.
        assert set(entry["serving_metrics"]) == {
            "requests",
            "errors",
            "fallbacks",
            "avg_confidence",
            "last_used",
        }

    assert by_id["v1"]["ab_group"] == "A"
    assert by_id["v2"]["ab_group"] == "B"
    assert by_id["v1"]["serving_metrics"]["requests"] >= 2


# --------------------------------------------------------------------------- #
# configure — unknown version is rejected
# --------------------------------------------------------------------------- #


def test_configure_unknown_version_raises(router):
    """``configure`` with a version id the registry does not know raises (KeyError)."""
    with pytest.raises((KeyError, ValueError)):
        router.configure(a_version="v999")
