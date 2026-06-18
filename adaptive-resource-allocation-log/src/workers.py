"""Worker-pool abstractions modelling *capacity*.

Where :mod:`src.load_model` models incoming *demand*, this module models the
*capacity* that serves it. A :class:`WorkerPool` exposes a uniform interface the
orchestrator uses to read pool state, scale the worker count, and (for simulated
backends) advance an internal queue simulation each tick.

Two concrete realities are provided:

* :class:`SimulatedWorkerPool` — a pure in-process model. It tracks a worker count
  and a floating-point backlog, draining the backlog at ``count * capacity_per_worker``
  messages/second. Used for fast, deterministic tests and for running the system
  without any container runtime.
* :class:`DockerWorkerPool` — a real backend that launches worker *containers* and
  *measures* their count rather than simulating a queue. It is **opt-in** (selected
  by ``worker_backend == "docker"``, i.e. ``USE_DOCKER=1``) and connects to the Docker
  daemon **lazily** so importing this module and constructing the factory never
  requires the ``docker`` SDK or a running daemon.

The :meth:`WorkerPool.stats` contract returns a dict with exactly four keys::

    {
        "queue_depth": int,    # pending backlog, messages
        "throughput":  float,  # messages/second currently being served
        "latency_ms":  float,  # estimated time to clear the backlog, milliseconds
        "capacity":    float,  # count * capacity_per_worker, messages/second
    }

.. warning::
   The Docker backend talks to the host Docker daemon (typically via the mounted
   ``/var/run/docker.sock``). **Mounting the Docker socket is root-equivalent** —
   a container that can reach it can control the host's Docker. For that reason the
   Docker backend is strictly opt-in via ``USE_DOCKER=1``; the default
   :class:`SimulatedWorkerPool` requires no daemon, keeping unit tests and the
   end-to-end flow fully hermetic.
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


# Label key/value used to tag (and later discover) the worker containers this pool
# owns. Reconciliation only ever touches containers carrying this exact label, so a
# DockerWorkerPool never disturbs unrelated containers on the host.
_POOL_LABEL_KEY = "ar.worker.pool"

# Unix socket the Docker daemon listens on by default. ``docker.from_env()`` is tried
# first (honours DOCKER_HOST etc.); this is the explicit fallback.
_DOCKER_SOCKET_URL = "unix:///var/run/docker.sock"


class DockerWorkerPool(WorkerPool):
    """Real worker pool backed by Docker containers.

    Each "worker" is a detached container started from ``config.worker_image`` and
    tagged with the label ``ar.worker.pool=<pool_label>``. :meth:`scale_to` reconciles
    the number of running, so-labelled containers toward the requested count: spawning
    new ones when short and **gracefully** stopping (``stop(timeout=10)``) then removing
    the *newest* extras when over, so in-flight work has a chance to drain — preserving
    the system's zero-message-loss intent.

    Unlike :class:`SimulatedWorkerPool` this backend does **not** model a queue: it
    measures the live container count and derives capacity from it. The per-container
    queue/throughput/latency are not introspected here, so :meth:`stats` reports
    best-effort placeholders for those (see :meth:`stats`).

    The Docker SDK is imported and the client constructed **lazily** on first use (see
    :meth:`_client`). Constructing this object therefore performs no I/O and needs no
    running daemon — only the methods that actually touch Docker do.

    Args:
        config: A :class:`src.config.Settings` (or any object exposing
            ``worker_image``, ``min_workers``, ``max_workers`` and
            ``capacity_per_worker``).
        pool_label: Value for the ``ar.worker.pool`` label that identifies this pool's
            containers. Defaults to ``"adaptive-worker"``.
    """

    # Resource caps applied to each spawned worker container. Conservative defaults so
    # a worker cannot starve the host; both are accepted directly by the Docker SDK.
    _MEM_LIMIT = "256m"
    _NANO_CPUS = 500_000_000  # 0.5 CPU (Docker expresses CPU as billionths of a core)

    def __init__(self, config, pool_label: str = "adaptive-worker") -> None:
        self._config = config
        self._pool_label = str(pool_label)
        self._min_workers = int(config.min_workers)
        self._max_workers = int(config.max_workers)
        self._capacity_per_worker = float(config.capacity_per_worker)
        self._image = str(config.worker_image)

        # Cached Docker client, created on first use by _client(). Kept None here so
        # construction never imports the SDK or touches the daemon.
        self._docker_client = None

        # Last count we know about. Seeded with the clamped minimum and refreshed by
        # current()/scale_to(); used as a defensive fallback when the daemon is
        # unreachable so the orchestrator still gets a sane number.
        self._last_known = self._clamp_count(self._min_workers)

    # -- lazy docker plumbing ----------------------------------------------------

    def _client(self):
        """Return a cached Docker client, creating it on first use.

        ``docker`` is imported here (not at module import) so the module stays
        importable without the SDK installed. ``docker.from_env()`` is tried first so
        ``DOCKER_HOST`` and friends are honoured; on failure we fall back to the
        conventional unix socket.

        Raises:
            RuntimeError: If the ``docker`` SDK is not installed.
            docker.errors.DockerException: If a client cannot be created at all.
        """
        if self._docker_client is not None:
            return self._docker_client

        try:
            import docker  # imported lazily — see module docstring
        except ImportError as exc:  # pragma: no cover - exercised only without the SDK
            raise RuntimeError(
                "the 'docker' SDK is required for the docker worker backend "
                "(install docker==7.1.0 or run with the default simulated backend)"
            ) from exc

        try:
            client = docker.from_env()
        except docker.errors.DockerException:
            # from_env() can fail if DOCKER_HOST is unset/odd; try the default socket.
            client = docker.DockerClient(base_url=_DOCKER_SOCKET_URL)

        self._docker_client = client
        return client

    def _label_filter(self) -> dict:
        """Docker ``filters`` dict selecting *this* pool's containers."""
        return {"label": f"{_POOL_LABEL_KEY}={self._pool_label}"}

    def _running_workers(self) -> list:
        """Return this pool's running worker containers, oldest first.

        Containers are ordered by creation time so :meth:`scale_to` can remove the
        *newest* extras (least likely to hold long-running in-flight work) when
        scaling down. Returns an empty list if the daemon is unreachable.
        """
        try:
            client = self._client()
            containers = client.containers.list(
                filters={**self._label_filter(), "status": "running"}
            )
        except Exception:  # noqa: BLE001 - any docker/SDK error => "none observable"
            return []

        def _created(container) -> str:
            # 'Created' is an ISO-8601 string; lexical sort matches chronological order.
            return str(container.attrs.get("Created", ""))

        return sorted(containers, key=_created)

    # -- helpers -----------------------------------------------------------------

    def _clamp_count(self, n: int) -> int:
        """Clamp ``n`` into the configured ``[min_workers, max_workers]`` range."""
        if n < self._min_workers:
            return self._min_workers
        if n > self._max_workers:
            return self._max_workers
        return n

    def _spawn(self, count: int) -> None:
        """Start ``count`` detached worker containers tagged with the pool label."""
        client = self._client()
        for _ in range(count):
            client.containers.run(
                self._image,
                detach=True,
                labels={_POOL_LABEL_KEY: self._pool_label},
                mem_limit=self._MEM_LIMIT,
                nano_cpus=self._NANO_CPUS,
            )

    def _retire(self, containers: list) -> None:
        """Gracefully stop then remove the given containers (drain in-flight work)."""
        for container in containers:
            try:
                # stop() sends SIGTERM and waits up to `timeout` before SIGKILL, so a
                # worker can finish/checkpoint current work instead of losing it.
                container.stop(timeout=10)
                container.remove()
            except Exception:  # noqa: BLE001 - already gone / racing removal is fine
                continue

    # -- WorkerPool interface ----------------------------------------------------

    def scale_to(self, n: int) -> None:
        """Reconcile the running worker containers toward ``n`` (clamped to bounds).

        When short, new containers are spawned; when over, the *newest* extras are
        gracefully stopped (``stop(timeout=10)``) and removed so in-flight work drains.
        If the daemon is unreachable the request is recorded as the last-known count
        and otherwise treated as a no-op (no exception escapes).

        Args:
            n: Desired worker count; clamped into ``[min_workers, max_workers]``.
        """
        target = self._clamp_count(int(n))
        self._last_known = target

        try:
            running = self._running_workers()
            have = len(running)
            if have < target:
                self._spawn(target - have)
            elif have > target:
                # Remove the newest extras (end of the oldest-first list).
                self._retire(running[target:])
        except Exception:  # noqa: BLE001 - best-effort; never crash the control loop
            return None
        return None

    def current(self) -> int:
        """Return the count of running, pool-labelled containers.

        Counted live from the daemon. If Docker is unavailable, fall back defensively
        to the last-known (clamped) count so the orchestrator always gets a sane value
        rather than an exception.
        """
        try:
            count = len(self._running_workers())
            self._last_known = count
            return count
        except Exception:  # noqa: BLE001 - daemon unreachable => last-known fallback
            return self._last_known

    def stats(self) -> dict:
        """Return the four-key telemetry dict for the live container count.

        ``capacity`` is derived as ``current() * capacity_per_worker``. The real
        per-container queue/throughput/latency are **not** introspected by this
        backend, so they are reported best-effort: ``queue_depth`` and ``latency_ms``
        are ``0`` and ``throughput`` equals the current capacity (an optimistic
        "fully served" assumption). These placeholders keep the :meth:`WorkerPool.stats`
        contract intact while making clear the real backend measures count, not queue.
        """
        capacity = float(self.current()) * self._capacity_per_worker
        return {
            "queue_depth": 0,
            "throughput": float(capacity),
            "latency_ms": 0.0,
            "capacity": float(capacity),
        }

    def observe(self, arrival_rate: float, dt: float) -> None:
        """No-op: the real backend *measures* live state instead of simulating it."""
        return None

    @property
    def backend(self) -> str:
        """Backend identifier — always ``"docker"`` for this class."""
        return "docker"


def create_worker_pool(config) -> WorkerPool:
    """Construct the worker pool selected by ``config.worker_backend``.

    Args:
        config: A :class:`src.config.Settings` (or any object exposing
            ``worker_backend``, ``min_workers``, ``max_workers`` and
            ``capacity_per_worker``).

    Returns:
        A :class:`DockerWorkerPool` when ``worker_backend == "docker"`` (constructed
        without connecting to the daemon — the connection is lazy), otherwise a
        :class:`SimulatedWorkerPool`.
    """
    if config.worker_backend == "docker":
        # No daemon connection happens here: DockerWorkerPool connects lazily on first
        # use, so the factory is safe to call even where Docker is unavailable.
        return DockerWorkerPool(config)
    return SimulatedWorkerPool(
        config.min_workers,
        config.max_workers,
        config.capacity_per_worker,
    )
