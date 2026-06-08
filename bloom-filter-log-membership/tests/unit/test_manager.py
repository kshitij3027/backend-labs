"""Unit tests for src.manager — routing, generations, persistence, threading.

Covers the FilterManager contract end to end: per-log-type routing isolation,
sizing pulled from settings, the exact spec confidence strings, metrics
recording, the two-generation rotation semantics (old keys answerable for
exactly ONE rotation, forgotten after two), clock-driven ``rotate_if_due``,
save_all/load_all roundtrips including the previous generation, config-
mismatch and corruption rejection on load, and a threaded smoke test over a
single shared filter.

Settings are constructed directly with tiny capacities (explicit kwargs beat
env vars in pydantic-settings, so these tests are immune to ambient config).
Rotation tests inject a fake clock (``clock = lambda: clock_now[0]``) so
generation aging is deterministic and instant.

Confidence assertions deliberately use the string literals (not the module
constants) — the spec wording itself is the contract under test.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from src.manager import FilterManager
from src.settings import Settings

#: Every env var the C7 settings extension added — deleted in the defaults
#: test so ambient configuration cannot mask a wrong default.
NEW_ENV_VARS = (
    "ERROR_LOGS_CAPACITY",
    "ERROR_LOGS_FP_RATE",
    "ACCESS_LOGS_CAPACITY",
    "ACCESS_LOGS_FP_RATE",
    "SECURITY_LOGS_CAPACITY",
    "SECURITY_LOGS_FP_RATE",
    "SBF_GROWTH_FACTOR",
    "SBF_TIGHTENING_RATIO",
    "SNAPSHOT_INTERVAL_SECONDS",
    "ROTATION_MAX_AGE_SECONDS",
    "ROTATION_CHECK_INTERVAL_SECONDS",
)

ALL_NAMES = ("error_logs", "access_logs", "security_logs")


def make_settings(**overrides: object) -> Settings:
    """Settings with tiny per-type capacities so tests run in microseconds.

    All six sizing fields are passed explicitly (deterministic regardless of
    environment); anything else can be overridden per test.
    """
    fields: dict[str, object] = dict(
        error_logs_capacity=200,
        error_logs_fp_rate=0.01,
        access_logs_capacity=300,
        access_logs_fp_rate=0.05,
        security_logs_capacity=100,
        security_logs_fp_rate=0.001,
    )
    fields.update(overrides)
    return Settings(**fields)  # type: ignore[arg-type]


# ---------------------------------------------------------------------- #
# settings extension                                                     #
# ---------------------------------------------------------------------- #


def test_settings_new_field_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """The C7 settings fields carry the spec defaults."""
    for var in NEW_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    s = Settings()
    assert s.error_logs_capacity == 1_000_000
    assert s.error_logs_fp_rate == 0.01
    assert s.access_logs_capacity == 5_000_000
    assert s.access_logs_fp_rate == 0.05
    assert s.security_logs_capacity == 100_000
    assert s.security_logs_fp_rate == 0.001
    assert s.sbf_growth_factor == 2
    assert s.sbf_tightening_ratio == 0.85
    assert s.snapshot_interval_seconds == 30.0
    assert s.rotation_max_age_seconds == 86_400.0
    assert s.rotation_check_interval_seconds == 60.0


def test_settings_filter_configs_mapping() -> None:
    """filter_configs() maps each filter name to its (capacity, fp) pair."""
    s = Settings(
        error_logs_capacity=11,
        error_logs_fp_rate=0.5,
        access_logs_capacity=22,
        access_logs_fp_rate=0.25,
        security_logs_capacity=33,
        security_logs_fp_rate=0.125,
    )
    assert s.filter_configs() == {
        "error_logs": (11, 0.5),
        "access_logs": (22, 0.25),
        "security_logs": (33, 0.125),
    }


# ---------------------------------------------------------------------- #
# routing and sizing                                                     #
# ---------------------------------------------------------------------- #


def test_names_lists_all_three_filters() -> None:
    manager = FilterManager(make_settings())
    assert manager.names == ALL_NAMES


def test_routing_isolation_between_log_types() -> None:
    """A key added to one filter is invisible to the other two."""
    manager = FilterManager(make_settings())
    added, duration_ms = manager.add("error_logs", "ERR-2026-001")
    assert added is True
    assert duration_ms >= 0.0

    assert manager.query("error_logs", "ERR-2026-001")[0] is True
    # The sibling filters are empty (all bits 0), so their "no" is exact.
    assert manager.query("access_logs", "ERR-2026-001")[0] is False
    assert manager.query("security_logs", "ERR-2026-001")[0] is False


def test_per_type_sizing_matches_settings() -> None:
    """Each filter is built from its own settings entry, slice 0 included."""
    settings = make_settings()
    manager = FilterManager(settings)
    for name, (capacity, fp_rate) in settings.filter_configs().items():
        sbf = manager.get(name).current
        assert sbf.initial_capacity == capacity
        assert sbf.target_fp_rate == fp_rate
        assert sbf.growth == settings.sbf_growth_factor
        assert sbf.tightening == settings.sbf_tightening_ratio
        # Slice 0 carries the configured capacity and the SBF-budgeted FP
        # share: target * (1 - tightening) * tightening**0.
        slice0 = sbf.slices[0]
        assert slice0.expected_items == capacity
        assert slice0.fp_rate == pytest.approx(
            fp_rate * (1.0 - settings.sbf_tightening_ratio)
        )


def test_unknown_filter_name_raises_key_error() -> None:
    """get/add/query all KeyError on a name outside the routing map."""
    manager = FilterManager(make_settings())
    with pytest.raises(KeyError):
        manager.get("debug_logs")
    with pytest.raises(KeyError):
        manager.add("debug_logs", "key")
    with pytest.raises(KeyError):
        manager.query("debug_logs", "key")


# ---------------------------------------------------------------------- #
# confidence strings and metrics                                         #
# ---------------------------------------------------------------------- #


def test_confidence_strings_are_exact_spec_wording() -> None:
    manager = FilterManager(make_settings())
    manager.add("security_logs", "login-failed-10.0.0.7")

    might, confidence, _ = manager.query("security_logs", "login-failed-10.0.0.7")
    assert might is True
    assert confidence == "probably_exists"

    might, confidence, _ = manager.query("security_logs", "never-added")
    assert might is False
    assert confidence == "definitely_not_exist"


def test_metrics_recorded_per_filter() -> None:
    """N adds + M queries show up exactly in that filter's metrics ledger."""
    manager = FilterManager(make_settings())
    for i in range(7):
        manager.add("error_logs", f"metric-{i}")

    outcomes = [manager.query("error_logs", f"metric-{i}")[0] for i in range(5)]
    outcomes += [manager.query("error_logs", f"absent-{i}")[0] for i in range(4)]
    # Present keys are guaranteed positive (zero false negatives).
    assert all(outcomes[:5])

    snap = manager.metrics.snapshot()["error_logs"]
    assert snap["adds_total"] == 7
    assert snap["queries_total"] == 9
    # Capture-and-compare: positives/negatives mirror the answers observed.
    assert snap["positives"] == sum(outcomes)
    assert snap["negatives"] == 9 - sum(outcomes)

    # Untouched filters recorded nothing.
    assert "access_logs" not in manager.metrics.snapshot() or (
        manager.metrics.snapshot()["access_logs"]["queries_total"] == 0
    )


# ---------------------------------------------------------------------- #
# rotation generations                                                   #
# ---------------------------------------------------------------------- #


def test_rotation_keeps_old_generation_answerable_for_one_period() -> None:
    """Rotate once: old keys still True via previous; new adds hit current.

    Rotate twice: the original generation is dropped and its keys answer
    False again — the documented two-generation memory bound. This is the
    expected behavior, not a bug: "recently seen" dedup only needs one full
    period of lookback, and full history lives in the C10 sqlite tier.
    """
    manager = FilterManager(make_settings())
    mf = manager.get("error_logs")
    gen1 = [f"gen1-{i}" for i in range(20)]
    for key in gen1:
        manager.add("error_logs", key)

    manager.rotate("error_logs")
    assert mf.rotations == 1
    assert mf.current.count == 0
    assert mf.previous is not None
    assert mf.previous.count == 20

    # Zero false negatives across the rotation boundary: every pre-rotation
    # key is still answered positively, served by the previous generation.
    for key in gen1:
        might, confidence, _ = manager.query("error_logs", key)
        assert might is True
        assert confidence == "probably_exists"

    # New adds land in current only; previous is frozen.
    gen2 = [f"gen2-{i}" for i in range(10)]
    for key in gen2:
        manager.add("error_logs", key)
    assert mf.current.count == 10
    assert mf.previous.count == 20

    # Second rotation: gen2 becomes previous, gen1 is forgotten entirely.
    manager.rotate("error_logs")
    assert mf.rotations == 2
    assert mf.previous.count == 10
    for key in gen1:
        might, confidence, _ = manager.query("error_logs", key)
        assert might is False
        assert confidence == "definitely_not_exist"
    for key in gen2:
        assert manager.query("error_logs", key)[0] is True


def test_rotate_if_due_uses_injected_clock() -> None:
    clock_now = [1000.0]
    settings = make_settings(rotation_max_age_seconds=100.0)
    manager = FilterManager(settings, clock=lambda: clock_now[0])

    # Not due one tick before the age limit.
    clock_now[0] = 1099.999
    assert manager.rotate_if_due() == []
    assert all(manager.get(n).rotations == 0 for n in ALL_NAMES)

    # Due exactly at the limit: every filter rotates and restarts its age.
    clock_now[0] = 1100.0
    rotated = manager.rotate_if_due()
    assert sorted(rotated) == sorted(ALL_NAMES)
    for name in ALL_NAMES:
        mf = manager.get(name)
        assert mf.rotations == 1
        assert mf.created_at == 1100.0

    # created_at was refreshed: no immediate re-rotation on the next checks.
    assert manager.rotate_if_due() == []
    clock_now[0] = 1199.0
    assert manager.rotate_if_due() == []
    clock_now[0] = 1200.0
    assert sorted(manager.rotate_if_due()) == sorted(ALL_NAMES)


def test_rotation_disabled_when_max_age_is_zero() -> None:
    clock_now = [0.0]
    manager = FilterManager(
        make_settings(rotation_max_age_seconds=0.0), clock=lambda: clock_now[0]
    )
    for advance in (1.0, 86_400.0, 1e12):
        clock_now[0] = advance
        assert manager.rotate_if_due() == []
    assert all(manager.get(n).rotations == 0 for n in ALL_NAMES)


# ---------------------------------------------------------------------- #
# persistence: save_all / load_all                                       #
# ---------------------------------------------------------------------- #


def test_save_all_load_all_roundtrip(tmp_path: Path) -> None:
    """A fresh manager restored from disk answers exactly like the source."""
    settings = make_settings()
    source = FilterManager(settings)
    keys = {name: [f"{name}-key-{i}" for i in range(25)] for name in ALL_NAMES}
    for name, name_keys in keys.items():
        for key in name_keys:
            source.add(name, key)

    source.save_all(tmp_path)
    for name in ALL_NAMES:
        assert (tmp_path / f"{name}.bloom").exists()
        # Never rotated -> no previous generation -> no .prev snapshot.
        assert not (tmp_path / f"{name}.bloom.prev").exists()
    assert not list(tmp_path.glob("*.tmp")), "write_atomic left a temp file"

    restored = FilterManager(settings)
    results = restored.load_all(tmp_path)
    assert results == {name: True for name in ALL_NAMES}
    for name, name_keys in keys.items():
        assert restored.get(name).current.count == source.get(name).current.count
        for key in name_keys:
            assert restored.query(name, key)[0] is True


def test_save_load_preserves_previous_generation(tmp_path: Path) -> None:
    """After a rotation, both generations roundtrip through disk."""
    settings = make_settings()
    source = FilterManager(settings)
    gen1 = [f"old-{i}" for i in range(15)]
    for key in gen1:
        source.add("error_logs", key)
    source.rotate("error_logs")
    gen2 = [f"new-{i}" for i in range(10)]
    for key in gen2:
        source.add("error_logs", key)

    source.save_all(tmp_path)
    assert (tmp_path / "error_logs.bloom").exists()
    assert (tmp_path / "error_logs.bloom.prev").exists()

    restored = FilterManager(settings)
    assert restored.load_all(tmp_path)["error_logs"] is True
    mf = restored.get("error_logs")
    assert mf.current.count == 10
    assert mf.previous is not None
    assert mf.previous.count == 15
    # Old-generation keys are still answerable after the restart.
    for key in gen1 + gen2:
        assert restored.query("error_logs", key)[0] is True


def test_load_all_rejects_config_mismatch(tmp_path: Path) -> None:
    """A snapshot built under different sizing is refused, filter stays fresh."""
    source = FilterManager(make_settings(error_logs_capacity=200))
    source.add("error_logs", "sticky-key")
    source.add("access_logs", "access-key")
    source.save_all(tmp_path)

    changed = FilterManager(make_settings(error_logs_capacity=300))
    results = changed.load_all(tmp_path)
    assert results["error_logs"] is False  # capacity changed -> rejected
    assert results["access_logs"] is True  # unchanged config still loads

    mf = changed.get("error_logs")
    assert mf.current.count == 0
    assert mf.previous is None
    assert changed.query("error_logs", "sticky-key")[0] is False
    assert changed.query("access_logs", "access-key")[0] is True


def test_load_all_survives_corrupt_snapshot(tmp_path: Path) -> None:
    """Garbage bytes on disk mean a fresh filter, never an exception."""
    (tmp_path / "error_logs.bloom").write_bytes(b"this is not a bloom snapshot")
    manager = FilterManager(make_settings())

    results = manager.load_all(tmp_path)
    assert results["error_logs"] is False

    # The fresh filter is fully usable after the rejected load.
    manager.add("error_logs", "after-corruption")
    assert manager.query("error_logs", "after-corruption")[0] is True


# ---------------------------------------------------------------------- #
# stats                                                                  #
# ---------------------------------------------------------------------- #


def test_stats_merges_filter_generation_and_ops_views() -> None:
    clock_now = [500.0]
    manager = FilterManager(make_settings(), clock=lambda: clock_now[0])
    manager.add("error_logs", "stat-1")
    manager.query("error_logs", "stat-1")
    clock_now[0] = 530.0
    manager.rotate("error_logs")  # created_at -> 530.0
    manager.add("error_logs", "stat-2")
    clock_now[0] = 542.5

    stats = manager.stats()
    assert set(stats) == set(ALL_NAMES)

    es = stats["error_logs"]
    assert es["name"] == "error_logs"
    assert es["count"] == 1  # current generation only
    assert es["previous_count"] == 1
    assert es["rotations"] == 1
    assert es["created_at"] == 530.0
    assert es["generation_age_seconds"] == pytest.approx(12.5)
    mf = manager.get("error_logs")
    assert mf.previous is not None
    assert es["memory_bytes_total"] == (
        mf.current.memory_bytes + mf.previous.memory_bytes
    )
    assert es["ops"]["adds_total"] == 2
    assert es["ops"]["queries_total"] == 1

    # Untouched filters still expose a complete, zeroed ops block.
    untouched = stats["access_logs"]
    assert untouched["previous_count"] == 0
    assert untouched["rotations"] == 0
    assert untouched["memory_bytes_total"] == untouched["memory_bytes"]
    assert untouched["ops"]["adds_total"] == 0


# ---------------------------------------------------------------------- #
# threading                                                              #
# ---------------------------------------------------------------------- #


def test_threaded_smoke_mixed_add_query_on_one_filter() -> None:
    """8 threads x 500 mixed ops on one filter: no errors, exact metrics.

    Capacity is 200, so 2000 distinct adds also force concurrent slice
    growth under the lock — the exact production contention pattern.
    """
    threads = 8
    ops_per_thread = 500  # alternating add/query -> 250 adds + 250 queries
    manager = FilterManager(make_settings())

    def worker(tid: int) -> list[str]:
        added: list[str] = []
        for i in range(ops_per_thread):
            key = f"thread{tid}-key{i // 2}"
            if i % 2 == 0:
                manager.add("error_logs", key)
                added.append(key)
            else:
                # Queries its own just-added key: must never be a miss.
                assert manager.query("error_logs", key)[0] is True
        return added

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker, tid) for tid in range(threads)]
        added_keys = [key for future in futures for key in future.result()]

    total_adds = threads * (ops_per_thread // 2)
    assert len(added_keys) == total_adds

    # Every added key remains queryable afterwards (zero false negatives).
    for key in added_keys:
        assert manager.query("error_logs", key)[0] is True

    # Dedup semantics bound the distinct count by the keys added.
    assert manager.get("error_logs").current.count <= total_adds

    # Metrics totals are exact: 2000 adds + 2000 worker queries (the 4000
    # threaded ops), plus the 2000 verification queries just issued above.
    snap = manager.metrics.snapshot()["error_logs"]
    assert snap["adds_total"] == total_adds
    assert snap["queries_total"] == total_adds + len(added_keys)
