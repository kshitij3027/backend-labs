"""Worker-pool abstractions modelling *capacity*.

Where :mod:`src.load_model` models incoming *demand*, this module models the
*capacity* that serves it. A :class:`WorkerPool` exposes a uniform interface the
orchestrator uses to read pool state, scale the worker count, and (for simulated
backends) advance an internal queue simulation each tick.

Two concrete realities are envisaged:

* :class:`SimulatedWorkerPool` — a pure in-process model. It tracks a worker count
  and a floating-point backlog, draining the backlog at ``count * capacity_per_worker``
  messages/second. Used for fast, deterministic tests and for running the system
  without any container runtime.
* ``DockerWorkerPool`` — a real backend that launches worker containers and *measures*
  throughput/latency rather than simulating them. It is intentionally **not** in this
  commit; :func:`create_worker_pool` raises :class:`NotImplementedError` for it.

The :meth:`WorkerPool.stats` contract returns a dict with exactly four keys::

    {
        "queue_depth": int,    # pending backlog, messages
        "throughput":  float,  # messages/second currently being served
        "latency_ms":  float,  # estimated time to clear the backlog, milliseconds
        "capacity":    float,  # count * capacity_per_worker, messages/second
    }
"""

from abc import ABC, abstractmethod


# Sentinel latency reported when there is zero capacity but pending work — avoids a
# divide-by-zero while still signalling "effectively unbounded" to the orchestrator.
_NO_CAPACITY_LATENCY_MS = 1e6


class WorkerPool(ABC):
    """Abstract interface for a pool of workers serving the message backlog.

    Subclasses model capacity either by simulation (:class:`SimulatedWorkerPool`) or
    by managing and measuring a real backend. The orchestrator interacts only through
    this interface.
    """

    @abstractmethod
    def scale_to(self, n: int) -> None:
        """Scale the pool to ``n`` workers, clamped to the pool's ``[min, max]``.

        Scaling *down* must not discard pending work — implementations preserve any
        existing backlog so the system keeps its "zero message loss" guarantee.
        """
        raise NotImplementedError

    @abstractmethod
    def current(self) -> int:
        """Return the current number of workers in the pool."""
        raise NotImplementedError

    @abstractmethod
    def stats(self) -> dict:
        """Return pool telemetry.

        Returns:
            A dict with keys ``queue_depth`` (int), ``throughput`` (float),
            ``latency_ms`` (float) and ``capacity`` (float). See the module docstring
            for the precise contract.
        """
        raise NotImplementedError

    def observe(self, arrival_rate: float, dt: float) -> None:
        """Advance the pool's internal model by ``dt`` seconds at ``arrival_rate``.

        The orchestrator calls this once per tick. Simulated backends use it to
        advance their queue simulation; real backends measure live metrics instead and
        leave this as a no-op (the default implementation here).

        Args:
            arrival_rate: Incoming demand in messages/second over the interval.
            dt: Length of the interval in seconds.
        """
        # Default: real backends measure rather than simulate — nothing to do.
        return None

    @property
    @abstractmethod
    def backend(self) -> str:
        """Short identifier for the backend, e.g. ``"simulated"`` or ``"docker"``."""
        raise NotImplementedError


class SimulatedWorkerPool(WorkerPool):
    """In-process queue simulation of a worker pool.

    Models a single FIFO backlog drained by ``count`` workers, each able to process
    ``capacity_per_worker`` messages/second. The backlog is a float so fractional
    messages accumulate smoothly across ticks; :meth:`stats` reports the integer part.

    Args:
        min_workers: Lower bound on the worker count (inclusive).
        max_workers: Upper bound on the worker count (inclusive).
        capacity_per_worker: Messages/second a single worker can process.
        initial: Starting worker count; defaults to ``min_workers``. Clamped into
            ``[min_workers, max_workers]``.
    """

    def __init__(
        self,
        min_workers: int,
        max_workers: int,
        capacity_per_worker: float,
        initial: int | None = None,
    ) -> None:
        self._min_workers = int(min_workers)
        self._max_workers = int(max_workers)
        self._capacity_per_worker = float(capacity_per_worker)

        start = self._min_workers if initial is None else int(initial)
        self._count = self._clamp_count(start)

        # Simulation state.
        self._queue: float = 0.0       # pending backlog in messages (float for smoothness)
        self._throughput: float = 0.0   # messages/second served on the last observe()
        self._latency_ms: float = 0.0    # estimated backlog-clear time on the last observe()

    # -- helpers -----------------------------------------------------------------

    def _clamp_count(self, n: int) -> int:
        """Clamp ``n`` into the configured ``[min_workers, max_workers]`` range."""
        if n < self._min_workers:
            return self._min_workers
        if n > self._max_workers:
            return self._max_workers
        return n

    def _capacity(self) -> float:
        """Current aggregate capacity in messages/second."""
        return self._count * self._capacity_per_worker

    # -- WorkerPool interface ----------------------------------------------------

    def scale_to(self, n: int) -> None:
        """Set the worker count to ``n`` clamped to ``[min_workers, max_workers]``.

        The pending backlog (``_queue``) is deliberately left untouched so scaling
        down never drops in-flight work.
        """
        self._count = self._clamp_count(n)

    def current(self) -> int:
        """Return the current (clamped) worker count."""
        return self._count

    def observe(self, arrival_rate: float, dt: float) -> None:
        """Advance the queue simulation by ``dt`` seconds.

        The net change in backlog over the interval is ``(arrival_rate - capacity)*dt``;
        the backlog is floored at zero. Throughput is whatever the pool actually serves
        (capped at capacity, but at least the inflow when the pool keeps up), and
        latency is the time to clear the current backlog at full capacity.

        Args:
            arrival_rate: Incoming demand in messages/second over the interval.
            dt: Interval length in seconds.
        """
        capacity = self._capacity()
        net = (arrival_rate - capacity) * dt
        self._queue = max(0.0, self._queue + net)

        # We serve at capacity whenever there is a backlog to drain or demand exceeds
        # capacity; otherwise we only serve the (smaller) incoming rate.
        if capacity > 0.0:
            self._throughput = min(max(arrival_rate, 0.0), capacity)
            if self._queue > 0.0:
                self._throughput = capacity
            self._latency_ms = (self._queue / capacity) * 1000.0
        else:
            # No workers / no capacity: nothing is served and any backlog is unbounded.
            self._throughput = 0.0
            self._latency_ms = _NO_CAPACITY_LATENCY_MS if self._queue > 0.0 else 0.0

    def stats(self) -> dict:
        """Return the four-key telemetry dict for the current simulation state."""
        return {
            "queue_depth": int(self._queue),
            "throughput": float(self._throughput),
            "latency_ms": float(self._latency_ms),
            "capacity": float(self._capacity()),
        }

    @property
    def backend(self) -> str:
        """Backend identifier — always ``"simulated"`` for this class."""
        return "simulated"


def create_worker_pool(config) -> WorkerPool:
    """Construct the worker pool selected by ``config.worker_backend``.

    Args:
        config: A :class:`src.config.Settings` (or any object exposing
            ``worker_backend``, ``min_workers``, ``max_workers`` and
            ``capacity_per_worker``).

    Returns:
        A :class:`SimulatedWorkerPool` for the ``"simulated"`` backend.

    Raises:
        NotImplementedError: If ``worker_backend == "docker"``. The real
            ``DockerWorkerPool`` is added in a later commit.
    """
    if config.worker_backend == "docker":
        raise NotImplementedError("docker backend is added in a later commit")
    return SimulatedWorkerPool(
        config.min_workers,
        config.max_workers,
        config.capacity_per_worker,
    )
