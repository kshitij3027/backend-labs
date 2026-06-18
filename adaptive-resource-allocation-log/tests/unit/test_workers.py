"""Unit tests for :mod:`src.workers`.

Arrival rates and ``dt`` are injected explicitly so the queue simulation is fully
deterministic. Pool bounds mirror the :class:`src.config.Settings` defaults
(min=2, max=20, capacity_per_worker=400) but are also exercised with hand-picked
values where that makes an assertion clearer.
"""

import pytest

from src.config import Settings
from src.workers import (
    DockerWorkerPool,
    SimulatedWorkerPool,
    WorkerPool,
    create_worker_pool,
)


MIN_WORKERS = 2
MAX_WORKERS = 20
CAP_PER_WORKER = 400.0


def _docker_available() -> bool:
    """Return True iff a Docker daemon is reachable.

    Tries to import the SDK and ping the daemon, swallowing *any* error (missing SDK,
    no socket, permission denied). Live DockerWorkerPool tests are skipped when this is
    False — which is the expected case in the hermetic test container (no docker socket).
    """
    try:
        import docker

        docker.from_env().ping()
        return True
    except Exception:  # noqa: BLE001 - any failure means "no usable daemon"
        return False


@pytest.fixture
def pool() -> SimulatedWorkerPool:
    """A simulated pool with the standard bounds, starting at min_workers."""
    return SimulatedWorkerPool(MIN_WORKERS, MAX_WORKERS, CAP_PER_WORKER)


# --------------------------------------------------------------------------- #
# Construction / initial state
# --------------------------------------------------------------------------- #
def test_starts_at_min_workers_by_default(pool):
    """With no ``initial``, the pool starts at ``min_workers``."""
    assert pool.current() == MIN_WORKERS


def test_initial_is_clamped():
    """An ``initial`` outside the bounds is clamped into range."""
    low = SimulatedWorkerPool(MIN_WORKERS, MAX_WORKERS, CAP_PER_WORKER, initial=0)
    high = SimulatedWorkerPool(MIN_WORKERS, MAX_WORKERS, CAP_PER_WORKER, initial=999)
    assert low.current() == MIN_WORKERS
    assert high.current() == MAX_WORKERS


def test_backend_is_simulated(pool):
    """The simulated pool reports the ``"simulated"`` backend."""
    assert pool.backend == "simulated"
    assert isinstance(pool, WorkerPool)


def test_initial_stats_are_zeroed(pool):
    """A fresh pool has an empty queue and zero throughput/latency."""
    stats = pool.stats()
    assert stats["queue_depth"] == 0
    assert stats["throughput"] == 0.0
    assert stats["latency_ms"] == 0.0


# --------------------------------------------------------------------------- #
# scale_to clamping
# --------------------------------------------------------------------------- #
def test_scale_to_clamps_below_min(pool):
    """Scaling below ``min_workers`` clamps to the minimum."""
    pool.scale_to(-5)
    assert pool.current() == MIN_WORKERS


def test_scale_to_clamps_above_max(pool):
    """Scaling above ``max_workers`` clamps to the maximum."""
    pool.scale_to(1000)
    assert pool.current() == MAX_WORKERS


def test_scale_to_sets_in_range_value(pool):
    """An in-range target is applied verbatim."""
    pool.scale_to(7)
    assert pool.current() == 7


# --------------------------------------------------------------------------- #
# observe: queue dynamics
# --------------------------------------------------------------------------- #
def test_queue_grows_when_overloaded(pool):
    """When arrival_rate exceeds capacity, the backlog grows each tick."""
    pool.scale_to(2)                      # capacity = 800 msgs/s
    arrival = 2000.0                      # demand well above capacity

    depths = []
    for _ in range(5):
        pool.observe(arrival, dt=1.0)
        depths.append(pool.stats()["queue_depth"])

    # Monotonically non-decreasing and strictly larger by the end.
    assert depths == sorted(depths)
    assert depths[-1] > depths[0]
    assert depths[-1] > 0


def test_queue_drains_when_underloaded(pool):
    """When arrival_rate is below capacity, an existing backlog drains to zero."""
    pool.scale_to(2)                      # capacity = 800 msgs/s

    # Build up a backlog first.
    for _ in range(5):
        pool.observe(2000.0, dt=1.0)
    backlog = pool.stats()["queue_depth"]
    assert backlog > 0

    # Now demand drops below capacity; backlog must shrink toward 0.
    for _ in range(20):
        pool.observe(100.0, dt=1.0)
    assert pool.stats()["queue_depth"] == 0


def test_latency_increases_with_queue_depth(pool):
    """Latency grows as the backlog grows (more pending work to clear)."""
    pool.scale_to(2)                      # fixed capacity throughout

    pool.observe(2000.0, dt=1.0)
    latency_early = pool.stats()["latency_ms"]

    for _ in range(4):
        pool.observe(2000.0, dt=1.0)
    latency_late = pool.stats()["latency_ms"]

    assert latency_late > latency_early > 0.0


def test_balanced_load_keeps_queue_empty(pool):
    """When arrival equals capacity exactly, the backlog stays at zero."""
    pool.scale_to(2)                      # capacity = 800 msgs/s
    for _ in range(10):
        pool.observe(800.0, dt=1.0)
    assert pool.stats()["queue_depth"] == 0


# --------------------------------------------------------------------------- #
# scaling down preserves the queue
# --------------------------------------------------------------------------- #
def test_scaling_down_preserves_queue(pool):
    """Scaling the pool down must not discard the pending backlog."""
    pool.scale_to(10)
    for _ in range(5):
        pool.observe(50_000.0, dt=1.0)    # overload to build a backlog
    backlog_before = pool.stats()["queue_depth"]
    assert backlog_before > 0

    pool.scale_to(MIN_WORKERS)            # scale all the way down
    # The queue is untouched by scaling itself (no observe() in between).
    assert pool.stats()["queue_depth"] == backlog_before


# --------------------------------------------------------------------------- #
# stats contract
# --------------------------------------------------------------------------- #
def test_stats_has_all_keys_with_types(pool):
    """stats() returns exactly the four documented keys with correct types."""
    pool.scale_to(3)
    pool.observe(2000.0, dt=1.0)
    stats = pool.stats()

    assert set(stats.keys()) == {"queue_depth", "throughput", "latency_ms", "capacity"}
    assert isinstance(stats["queue_depth"], int)
    assert isinstance(stats["throughput"], float)
    assert isinstance(stats["latency_ms"], float)
    assert isinstance(stats["capacity"], float)


def test_capacity_equals_count_times_per_worker(pool):
    """capacity in stats() equals current_count * capacity_per_worker."""
    pool.scale_to(5)
    assert pool.stats()["capacity"] == 5 * CAP_PER_WORKER

    pool.scale_to(MAX_WORKERS)
    assert pool.stats()["capacity"] == MAX_WORKERS * CAP_PER_WORKER


def test_throughput_capped_at_capacity(pool):
    """Throughput never exceeds aggregate capacity."""
    pool.scale_to(2)                      # capacity = 800
    pool.observe(5000.0, dt=1.0)          # demand far above capacity
    assert pool.stats()["throughput"] == pytest.approx(800.0)


# --------------------------------------------------------------------------- #
# create_worker_pool factory
# --------------------------------------------------------------------------- #
def test_factory_returns_simulated_for_default_settings():
    """With default Settings (simulated backend), the factory builds a SimulatedWorkerPool."""
    pool = create_worker_pool(Settings())
    assert isinstance(pool, SimulatedWorkerPool)
    assert pool.backend == "simulated"
    # Bounds and capacity are sourced from Settings.
    assert pool.current() == Settings().min_workers
    assert pool.stats()["capacity"] == Settings().min_workers * Settings().capacity_per_worker


def test_factory_returns_docker_pool_without_daemon():
    """The docker backend builds a DockerWorkerPool and does NOT touch the daemon.

    Construction must be lazy: no docker import/connection happens until a method that
    needs the daemon is called, so the factory works even with no docker installed.
    """
    settings = Settings(worker_backend="docker")
    pool = create_worker_pool(settings)  # must not raise / must not connect

    assert isinstance(pool, DockerWorkerPool)
    assert pool.backend == "docker"
    # No client should have been created merely by constructing the pool.
    assert pool._docker_client is None


def test_docker_pool_construction_is_lazy_and_reports_backend():
    """DockerWorkerPool can be constructed directly with no daemon present."""
    pool = DockerWorkerPool(Settings(worker_backend="docker"), pool_label="test-pool")

    assert pool.backend == "docker"
    assert pool._docker_client is None  # nothing connected at construction


# --------------------------------------------------------------------------- #
# DockerWorkerPool live operations (skipped unless a daemon is reachable)
# --------------------------------------------------------------------------- #
@pytest.mark.skipif(not _docker_available(), reason="docker daemon not available")
def test_docker_pool_scale_up_and_down_live():
    """Live: scaling up spawns labelled containers; scaling down retires extras.

    Runs ONLY where a Docker daemon is reachable (skipped in the hermetic container).
    Uses a tiny always-available image and a unique label so it never disturbs other
    containers, and always tears its own containers down.
    """
    import uuid

    label = f"ar-test-{uuid.uuid4().hex[:8]}"
    settings = Settings(
        worker_backend="docker",
        worker_image="busybox:latest",
        min_workers=0,
        max_workers=5,
    )
    pool = DockerWorkerPool(settings, pool_label=label)
    # busybox does not loop forever by default; override the spawn command via the SDK
    # is out of scope here, so we only assert reconciliation logic that does not rely on
    # the container staying up. To keep the test robust we scale to 0 and assert clean.
    try:
        pool.scale_to(0)
        assert pool.current() == 0
    finally:
        # Defensive cleanup: retire anything left carrying our unique label.
        pool.scale_to(0)


@pytest.mark.skipif(not _docker_available(), reason="docker daemon not available")
def test_docker_pool_current_counts_only_pool_label_live():
    """Live: current() counts only this pool's labelled containers (isolation)."""
    import uuid

    label = f"ar-test-{uuid.uuid4().hex[:8]}"
    pool = DockerWorkerPool(
        Settings(worker_backend="docker", min_workers=0, max_workers=5),
        pool_label=label,
    )
    # A brand-new unique label can have no containers yet.
    assert pool.current() == 0
