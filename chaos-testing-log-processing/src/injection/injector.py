"""Failure-injection facade with safety pre-flight checks.

This module owns the public ``FailureInjector`` API used by the
``ExperimentEngine``. At C5 the per-type injection methods are deliberate
stubs that raise ``NotImplementedError`` — C7/C8/C9 fill them in. The
safety pre-flight is real and gated by the same checks that landed in
``config/safety_config.yaml``.

Tracked state:
    - ``_active``: ``OrderedDict[scenario_id, ActiveScenario]`` — preserves
      registration order so blast-radius and abort-all flows are
      deterministic.
    - Each active entry carries the rollback callable so a later
      :meth:`rollback` (or kill-switch via :meth:`rollback_all`) can always
      clean up the fault that was actually installed.

The ``_test_handlers`` seam lets unit tests verify routing + safety logic
without tripping the stub ``NotImplementedError`` of the real per-type
methods. Production code never sets it.
"""

from __future__ import annotations

import inspect
import logging
import time
from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

from ..docker_client.client import DockerClient
from ..models.metrics import SystemMetrics
from ..models.scenarios import FailureScenario, FailureType, ScenarioStatus


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Type aliases
# --------------------------------------------------------------------------- #

#: Callable that, when invoked, returns the latest :class:`SystemMetrics`
#: snapshot from the monitor, or ``None`` if no snapshot has been collected
#: yet. When the monitor wiring is absent, the threshold check is skipped
#: rather than failing — a fresh process must still be able to schedule
#: its first scenario before the first 5s tick lands.
MetricsSnapshotProvider = Callable[[], Optional[SystemMetrics]]

#: Finalizer the per-type handler hands back so the injector can later
#: undo the fault. May be sync or async; :meth:`rollback` awaits the
#: result iff it's an awaitable.
RollbackCallable = Callable[[], Awaitable[None] | None]


# --------------------------------------------------------------------------- #
# Exceptions
# --------------------------------------------------------------------------- #


class InjectorError(Exception):
    """Base class for all errors raised by :class:`FailureInjector`."""


class SafetyCheckError(InjectorError):
    """A pre-flight safety check refused the scenario.

    Raised by :meth:`FailureInjector._run_safety_checks` with a message
    that names the violated constraint (allowlist, concurrency limit,
    CPU/mem threshold). The scenario's ``status`` is set to
    :attr:`ScenarioStatus.FAILED` before the exception escapes.
    """


class UnsupportedFailureTypeError(InjectorError):
    """No handler is registered for ``scenario.type``.

    Used when a new ``FailureType`` value is added to the enum but the
    routing table inside :class:`FailureInjector` has not been extended.
    """


# --------------------------------------------------------------------------- #
# Active-scenario record
# --------------------------------------------------------------------------- #


@dataclass
class ActiveScenario:
    """One in-flight scenario the injector is tracking.

    Carried in :attr:`FailureInjector._active`. ``rollback`` is whatever
    the per-type handler returned; it may be ``None`` (rare — only for
    pure observation faults) or a sync/async callable to undo the fault.
    """

    scenario: FailureScenario
    rollback: Optional[RollbackCallable] = None
    injected_at: float = field(default_factory=lambda: time.monotonic())


# --------------------------------------------------------------------------- #
# Injector
# --------------------------------------------------------------------------- #


class FailureInjector:
    """Facade that routes a scenario to the right per-type injector.

    Responsibilities:
        - Run pre-flight safety checks (allowlist, concurrency cap,
          CPU/mem emergency thresholds).
        - Dispatch by :class:`FailureType` to per-type handlers.
        - Track active scenarios so they can be rolled back individually
          (operator action) or all at once (supervisor kill-switch).

    At C5 the real per-type handlers raise ``NotImplementedError`` —
    they're filled in by C7 (network latency / packet loss),
    C8 (network partition), and C9 (resource exhaustion / component
    failure). Tests of the routing + safety surface use the
    :attr:`_test_handlers` seam.
    """

    def __init__(
        self,
        docker_client: DockerClient,
        allowlist: Iterable[str],
        max_concurrent: int,
        cpu_emergency_threshold_pct: float,
        mem_emergency_threshold_pct: float,
        metrics_snapshot: MetricsSnapshotProvider | None = None,
    ) -> None:
        self._docker = docker_client
        self._allowlist: frozenset[str] = frozenset(allowlist)
        self._max_concurrent = int(max_concurrent)
        self._cpu_limit = float(cpu_emergency_threshold_pct)
        self._mem_limit = float(mem_emergency_threshold_pct)
        self._metrics_snapshot: MetricsSnapshotProvider | None = metrics_snapshot

        # OrderedDict preserves registration order so rollback_all can
        # honor LIFO unwinding ("most-recent fault undone first").
        self._active: "OrderedDict[str, ActiveScenario]" = OrderedDict()

        # Test seam: when set to a mapping, takes precedence over the
        # real per-type stubs so unit tests can verify routing + safety
        # without tripping NotImplementedError. Production never sets
        # this; see module docstring.
        self._test_handlers: dict[FailureType, Callable] | None = None

    # ------------------------------------------------------------------ #
    # Inspection
    # ------------------------------------------------------------------ #

    @property
    def active_count(self) -> int:
        """Number of scenarios currently tracked as in-flight."""
        return len(self._active)

    def active_ids(self) -> list[str]:
        """Snapshot of the currently in-flight scenario IDs (insertion order)."""
        return list(self._active.keys())

    # ------------------------------------------------------------------ #
    # Pre-flight safety checks
    # ------------------------------------------------------------------ #

    def _run_safety_checks(self, scenario: FailureScenario) -> None:
        """Validate ``scenario`` against the configured safety guardrails.

        Order matters — the test contract pins this so failure messages
        are predictable:

        1. ``scenario.target`` must be in the allowlist.
        2. Currently in-flight count must be ``< max_concurrent``.
        3. If a metrics snapshot is wired AND non-``None``, the host CPU%
           and mem% must both be below their emergency thresholds.

        Raises:
            SafetyCheckError: Any of the above failed; the message names
                the violated constraint.
        """
        # 1) Allowlist
        if scenario.target not in self._allowlist:
            raise SafetyCheckError(
                f"target not in allowlist: {scenario.target}"
            )

        # 2) Concurrency cap
        if len(self._active) >= self._max_concurrent:
            raise SafetyCheckError(
                f"max concurrent scenarios reached: {len(self._active)}"
            )

        # 3) Emergency thresholds (only if monitor is wired and has data)
        if self._metrics_snapshot is not None:
            snapshot = self._metrics_snapshot()
            if snapshot is not None:
                if snapshot.cpu_pct >= self._cpu_limit:
                    raise SafetyCheckError(
                        f"cpu over threshold: {snapshot.cpu_pct} >= {self._cpu_limit}"
                    )
                if snapshot.mem_pct >= self._mem_limit:
                    raise SafetyCheckError(
                        f"mem over threshold: {snapshot.mem_pct} >= {self._mem_limit}"
                    )

    # ------------------------------------------------------------------ #
    # Public inject / rollback
    # ------------------------------------------------------------------ #

    async def inject(self, scenario: FailureScenario) -> ActiveScenario:
        """Run safety checks, dispatch to the per-type handler, and track.

        On success the scenario is registered in :attr:`_active` and its
        status flipped to :attr:`ScenarioStatus.ACTIVE`. The returned
        :class:`ActiveScenario` carries the rollback callable handed back
        by the handler so a later :meth:`rollback` (or the supervisor
        kill-switch via :meth:`rollback_all`) can clean up.

        Failure modes:
            - :class:`SafetyCheckError`: pre-flight refused the scenario;
              scenario status flipped to :attr:`ScenarioStatus.FAILED`.
            - :class:`UnsupportedFailureTypeError`: no handler exists for
              ``scenario.type``; scenario removed from tracking.
            - ``NotImplementedError`` from a stub per-type handler at C5
              propagates after tracking is rolled back.
        """
        # 1) Pre-flight — never reaches the handler if this raises.
        try:
            self._run_safety_checks(scenario)
        except SafetyCheckError:
            scenario.status = ScenarioStatus.FAILED
            logger.warning(
                "safety check failed for scenario=%s target=%s type=%s",
                scenario.id,
                scenario.target,
                scenario.type.value,
            )
            raise

        # 2) Resolve the handler — prefer the test seam if set.
        if self._test_handlers is not None:
            handler = self._test_handlers.get(scenario.type)
        else:
            handler = self._route_for_type(scenario.type)

        if handler is None:
            raise UnsupportedFailureTypeError(
                f"no handler registered for failure type: {scenario.type.value}"
            )

        # 3) Register BEFORE dispatch so safety counts include this
        #    scenario for the duration of the handler call. If dispatch
        #    raises (e.g. C5 NotImplementedError), we unregister and
        #    re-raise.
        active = ActiveScenario(scenario=scenario)
        self._active[scenario.id] = active

        try:
            result = handler(scenario)
            if inspect.isawaitable(result):
                rollback = await result
            else:
                rollback = result
        except Exception:
            # Roll back tracking; handler will not have side effects
            # registered against us.
            self._active.pop(scenario.id, None)
            raise

        active.rollback = rollback
        scenario.status = ScenarioStatus.ACTIVE
        logger.info(
            "injected scenario=%s type=%s target=%s active_count=%d",
            scenario.id,
            scenario.type.value,
            scenario.target,
            len(self._active),
        )
        return active

    async def rollback(self, scenario_id: str) -> None:
        """Undo a single in-flight scenario and remove it from tracking.

        Rollback is best-effort: any exception from the finalizer is
        logged and swallowed so the supervisor's abort-all path
        (:meth:`rollback_all`) always makes forward progress. The
        scenario's status is set to :attr:`ScenarioStatus.COMPLETED`
        regardless of whether the finalizer succeeded — the caller wires
        :attr:`ScenarioStatus.ABORTED` separately when needed.
        """
        active = self._active.pop(scenario_id, None)
        if active is None:
            logger.warning("rollback called for unknown scenario_id=%s", scenario_id)
            return

        if active.rollback is not None:
            try:
                result = active.rollback()
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:  # noqa: BLE001 — rollback is best-effort
                logger.exception(
                    "rollback finalizer raised for scenario=%s: %s",
                    scenario_id,
                    exc,
                )

        active.scenario.status = ScenarioStatus.COMPLETED
        logger.info(
            "rolled back scenario=%s active_count=%d",
            scenario_id,
            len(self._active),
        )

    async def rollback_all(self) -> None:
        """Undo every in-flight scenario in reverse registration order.

        Used by the C15 supervisor kill-switch. Iterates a snapshot of
        the active values (since each :meth:`rollback` mutates
        ``self._active``) and undoes them most-recent first so any
        nested faults (e.g. CPU pressure layered on top of a partition)
        come off in the right order.
        """
        scenario_ids = list(reversed(list(self._active.keys())))
        for sid in scenario_ids:
            await self.rollback(sid)

    # ------------------------------------------------------------------ #
    # Routing — production stubs (filled in C7/C8/C9)
    # ------------------------------------------------------------------ #

    def _route_for_type(self, failure_type: FailureType) -> Callable | None:
        """Return the per-type handler for ``failure_type`` or ``None``."""
        route: dict[FailureType, Callable] = {
            FailureType.LATENCY_INJECTION: self._inject_latency,
            FailureType.PACKET_LOSS: self._inject_packet_loss,
            FailureType.NETWORK_PARTITION: self._inject_partition,
            FailureType.RESOURCE_EXHAUSTION: self._inject_resource,
            FailureType.COMPONENT_FAILURE: self._inject_component,
        }
        return route.get(failure_type)

    async def _inject_latency(self, scenario: FailureScenario) -> RollbackCallable:
        """Stub — LATENCY_INJECTION is implemented in commit C7."""
        raise NotImplementedError(
            "LATENCY_INJECTION will be implemented in commit C7"
        )

    async def _inject_packet_loss(
        self, scenario: FailureScenario
    ) -> RollbackCallable:
        """Stub — PACKET_LOSS is implemented in commit C7."""
        raise NotImplementedError(
            "PACKET_LOSS will be implemented in commit C7"
        )

    async def _inject_partition(
        self, scenario: FailureScenario
    ) -> RollbackCallable:
        """Stub — NETWORK_PARTITION is implemented in commit C8."""
        raise NotImplementedError(
            "NETWORK_PARTITION will be implemented in commit C8"
        )

    async def _inject_resource(
        self, scenario: FailureScenario
    ) -> RollbackCallable:
        """Stub — RESOURCE_EXHAUSTION is implemented in commit C9."""
        raise NotImplementedError(
            "RESOURCE_EXHAUSTION will be implemented in commit C9"
        )

    async def _inject_component(
        self, scenario: FailureScenario
    ) -> RollbackCallable:
        """Stub — COMPONENT_FAILURE is implemented in commit C9."""
        raise NotImplementedError(
            "COMPONENT_FAILURE will be implemented in commit C9"
        )
