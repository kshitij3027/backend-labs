"""Unit tests for :mod:`src.model_store` (Commit 7).

Covers the versioned :class:`src.model_store.ModelRegistry`: empty-state
defaults, monotonic ``v<N>`` version minting, the headline-accuracy index entry,
load/round-trip of a fitted :class:`src.ensemble.LogClassifier`, the
``current`` pointer (``get_current`` / ``set_current``), ``KeyError`` on unknown
ids, on-disk persistence via ``metadata.json``, and the ``make_current=False``
opt-out.

A single tiny dual-ensemble classifier is fitted **once** per module (10-tree RF
+ 10-stage GB on 60 deterministic records) and reused across every test — the
registry under test never trains, it only stores/loads, so one cheap real model
is enough to exercise every persistence path while keeping the suite fast.
"""

from __future__ import annotations

import pytest

from src.config import Settings
from src.ensemble import LogClassifier
from src.log_generator import generate_logs
from src.model_store import ModelRegistry

#: Canonical line used to confirm a loaded model can actually classify.
CANONICAL_INPUT = "Database connection failed with timeout error"

#: Exact key set a classification result dict exposes (see src.ensemble).
RESULT_KEYS = {
    "severity",
    "category",
    "confidence",
    "severity_confidence",
    "category_confidence",
}


@pytest.fixture(scope="module")
def clf() -> LogClassifier:
    """A tiny :class:`LogClassifier` fitted once (10-tree RF + 10-stage GB, 60 recs).

    The registry only persists/loads classifiers, so a single cheap-but-real
    fitted model is sufficient for every test in this module. Module scope keeps
    the (still real) dual-ensemble fit to exactly one occurrence.
    """
    cfg = Settings(rf_n_estimators=10, gb_n_estimators=10)
    return LogClassifier(cfg).fit(generate_logs(60, 42))


def _assert_classifies(model: LogClassifier) -> None:
    """Assert ``model`` classifies the canonical line into the 5-key result dict."""
    out = model.classify(CANONICAL_INPUT)
    assert set(out.keys()) == RESULT_KEYS, f"unexpected keys: {sorted(out)}"


def test_empty_registry_defaults(tmp_path) -> None:
    """A fresh registry reports no models, no current, and an empty version list."""
    reg = ModelRegistry(str(tmp_path))
    assert reg.has_models() is False
    assert reg.current_version is None
    assert reg.list_versions() == []
    assert reg.latest() is None
    assert reg.get_current() is None


def test_first_save_returns_v1_and_becomes_current(tmp_path, clf) -> None:
    """The first ``save_version`` mints ``v1``, sets it current, and records accuracy."""
    reg = ModelRegistry(str(tmp_path))
    version = reg.save_version(clf, {"severity_test_accuracy": 0.9})

    assert version == "v1"
    assert reg.has_models() is True
    assert reg.current_version == "v1"
    assert reg.latest() == "v1"

    versions = reg.list_versions()
    assert len(versions) == 1
    assert versions[0]["version"] == "v1"
    assert versions[0]["accuracy"] == pytest.approx(0.9)


def test_second_save_returns_v2_in_numeric_order(tmp_path, clf) -> None:
    """A second save mints ``v2``, repoints current, and lists in numeric order."""
    reg = ModelRegistry(str(tmp_path))
    reg.save_version(clf, {"severity_test_accuracy": 0.9})
    version2 = reg.save_version(clf, {"severity_test_accuracy": 0.95})

    assert version2 == "v2"
    assert reg.current_version == "v2"
    assert reg.latest() == "v2"

    versions = reg.list_versions()
    assert len(versions) == 2
    assert [v["version"] for v in versions] == ["v1", "v2"]


def test_load_version_returns_working_classifier(tmp_path, clf) -> None:
    """``load_version`` reconstructs a fitted classifier that can classify."""
    reg = ModelRegistry(str(tmp_path))
    reg.save_version(clf, {"severity_test_accuracy": 0.9})

    loaded = reg.load_version("v1")
    assert isinstance(loaded, LogClassifier)
    _assert_classifies(loaded)


def test_get_current_returns_id_and_classifier(tmp_path, clf) -> None:
    """``get_current`` returns ``(current_id, classifier)`` and the model classifies."""
    reg = ModelRegistry(str(tmp_path))
    reg.save_version(clf, {"severity_test_accuracy": 0.9})
    reg.save_version(clf, {"severity_test_accuracy": 0.95})

    current = reg.get_current()
    assert current is not None
    version_id, model = current
    assert version_id == "v2"
    assert isinstance(model, LogClassifier)
    _assert_classifies(model)


def test_set_current_switches_and_unknown_ids_raise(tmp_path, clf) -> None:
    """``set_current`` repoints current; unknown ids raise ``KeyError`` on both paths."""
    reg = ModelRegistry(str(tmp_path))
    reg.save_version(clf, {"severity_test_accuracy": 0.9})
    reg.save_version(clf, {"severity_test_accuracy": 0.95})
    assert reg.current_version == "v2"

    reg.set_current("v1")
    assert reg.current_version == "v1"
    # The switch is durable: a re-opened registry reads back v1 as current.
    assert ModelRegistry(str(tmp_path)).current_version == "v1"

    with pytest.raises(KeyError):
        reg.set_current("v99")
    with pytest.raises(KeyError):
        reg.load_version("v99")


def test_metadata_persists_across_instances(tmp_path, clf) -> None:
    """A new registry over the same dir re-reads metadata.json (versions + current)."""
    reg = ModelRegistry(str(tmp_path))
    reg.save_version(clf, {"severity_test_accuracy": 0.9})
    reg.save_version(clf, {"severity_test_accuracy": 0.95})
    reg.set_current("v1")

    reopened = ModelRegistry(str(tmp_path))
    assert reopened.has_models() is True
    assert reopened.current_version == "v1"
    assert [v["version"] for v in reopened.list_versions()] == ["v1", "v2"]
    # And the persisted (not cached) model still loads + classifies.
    _assert_classifies(reopened.load_version("v2"))


def test_save_without_make_current_keeps_pointer(tmp_path, clf) -> None:
    """``make_current=False`` stores the version but leaves ``current`` unchanged."""
    reg = ModelRegistry(str(tmp_path))
    reg.save_version(clf, {"severity_test_accuracy": 0.9})  # v1 -> current
    version2 = reg.save_version(
        clf, {"severity_test_accuracy": 0.95}, make_current=False
    )  # v2, but not current

    assert version2 == "v2"
    assert reg.current_version == "v1"  # unchanged
    assert reg.latest() == "v2"  # latest tracks the counter, not current
    assert [v["version"] for v in reg.list_versions()] == ["v1", "v2"]
