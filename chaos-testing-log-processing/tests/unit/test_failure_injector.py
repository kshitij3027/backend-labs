"""Unit tests for C5 — DockerClient + FailureInjector surface.

Covers the two C5 deliverables under ``src/docker_client/client.py`` and
``src/injection/injector.py``. The injector's per-type handlers are stubs
at C5 (they raise NotImplementedError pointing to C7/C8/C9); routing +
safety + rollback are real, and that's what we exercise here.

The success-criteria test from ``project_requirements.md`` §5 is named
exactly ``test_safety_checks`` and bundles every safety-check branch.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
from unittest.mock import AsyncMock, MagicMock, call

from src.docker_client.client import (
    DockerClient,
    NotAllowlistedError,
    TargetNotFoundError,
)
from src.injection.injector import (
    FailureInjector,
    SafetyCheckError,
    UnsupportedFailureTypeError,
)
from src.models.metrics import SystemMetrics
from src.models.scenarios import FailureScenario, FailureType, ScenarioStatus


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


ALLOWLIST = ("log-producer", "log-consumer", "redis")


def make_scenario(
    *,
    target: str = "log-producer",
    failure_type: FailureType = FailureType.LATENCY_INJECTION,
    duration: int = 60,
    severity: int = 2,
    parameters: dict[str, Any] | None = None,
) -> FailureScenario:
    """Build a fresh, valid FailureScenario for the given knobs."""
    return FailureScenario(
        type=failure_type,
        target=target,
        parameters=parameters or {"latency_ms": 100},
        duration=duration,
        severity=severity,
    )


def make_metrics(*, cpu_pct: float = 10.0, mem_pct: float = 20.0) -> SystemMetrics:
    """Build a minimal SystemMetrics snapshot."""
    return SystemMetrics(
        timestamp=datetime.now(timezone.utc),
        cpu_pct=cpu_pct,
        mem_pct=mem_pct,
        disk_pct=30.0,
    )


def make_injector(
    *,
    docker_client: Any = None,
    allowlist: tuple[str, ...] = ALLOWLIST,
    max_concurrent: int = 3,
    cpu_emergency_threshold_pct: float = 90.0,
    mem_emergency_threshold_pct: float = 90.0,
    metrics_snapshot=None,
) -> FailureInjector:
    """Build a FailureInjector with sane test defaults."""
    return FailureInjector(
        docker_client=docker_client or MagicMock(name="docker_client"),
        allowlist=allowlist,
        max_concurrent=max_concurrent,
        cpu_emergency_threshold_pct=cpu_emergency_threshold_pct,
        mem_emergency_threshold_pct=mem_emergency_threshold_pct,
        metrics_snapshot=metrics_snapshot,
    )


def noop_handler_factory(rollback_callable=None):
    """Return a sync handler that registers and returns ``rollback_callable``.

    Used to fill ``_test_handlers`` so ``inject`` can succeed without
    tripping the real C5 NotImplementedError stubs.
    """

    def _handler(scenario: FailureScenario):
        return rollback_callable

    return _handler


# ===========================================================================
# A. Safety checks — the success-criteria test
# ===========================================================================


class TestSafetyChecks:
    """Cohesive coverage of every branch in ``_run_safety_checks``.

    ``test_safety_checks`` (the success-criteria entry point) thin-wraps
    a sequence of sub-cases that each exercise one branch in turn so a
    single failure narrows the cause.
    """

    @pytest.mark.asyncio
    async def test_safety_checks(self) -> None:
        """Success-criteria test (project_requirements.md §5).

        Bundles every safety-check branch into one cohesive test so that
        running just this name covers all four guardrails.
        """
        await self._target_not_in_allowlist_raises()
        await self._happy_path_with_no_metrics_provider()
        await self._max_concurrent_enforced()
        await self._cpu_over_threshold_raises()
        await self._mem_over_threshold_raises()
        await self._metrics_provider_returning_none_skips_threshold()
        await self._order_allowlist_before_threshold()

    # ----- branch 1: allowlist refusal -------------------------------------

    async def _target_not_in_allowlist_raises(self) -> None:
        injector = make_injector()
        scenario = make_scenario(target="unauthorized-container")

        with pytest.raises(SafetyCheckError) as excinfo:
            await injector.inject(scenario)

        assert "allowlist" in str(excinfo.value).lower()
        assert scenario.status == ScenarioStatus.FAILED
        assert injector.active_count == 0

    # ----- branch 2: happy path with no metrics wiring ---------------------

    async def _happy_path_with_no_metrics_provider(self) -> None:
        injector = make_injector()
        rollback_sentinel = MagicMock(name="rollback_sentinel")
        injector._test_handlers = {
            FailureType.LATENCY_INJECTION: noop_handler_factory(rollback_sentinel),
        }
        scenario = make_scenario()

        active = await injector.inject(scenario)

        assert scenario.status == ScenarioStatus.ACTIVE
        assert injector.active_count == 1
        assert active.rollback is rollback_sentinel

    # ----- branch 3: concurrency cap ---------------------------------------

    async def _max_concurrent_enforced(self) -> None:
        injector = make_injector(max_concurrent=3)
        injector._test_handlers = {
            FailureType.LATENCY_INJECTION: noop_handler_factory(MagicMock()),
        }

        # Prime: inject 3 successful scenarios.
        primed = []
        for _ in range(3):
            scenario = make_scenario()
            await injector.inject(scenario)
            primed.append(scenario)
        assert injector.active_count == 3

        # 4th should be refused with a message naming the constraint.
        fourth = make_scenario()
        with pytest.raises(SafetyCheckError) as excinfo:
            await injector.inject(fourth)

        assert "concurrent" in str(excinfo.value).lower()
        assert fourth.status == ScenarioStatus.FAILED
        # Primed scenarios remain in flight.
        assert injector.active_count == 3

    # ----- branch 4: CPU over threshold ------------------------------------

    async def _cpu_over_threshold_raises(self) -> None:
        snapshot = make_metrics(cpu_pct=95.0, mem_pct=10.0)
        provider = MagicMock(return_value=snapshot)
        injector = make_injector(
            cpu_emergency_threshold_pct=90.0,
            mem_emergency_threshold_pct=90.0,
            metrics_snapshot=provider,
        )
        injector._test_handlers = {
            FailureType.LATENCY_INJECTION: noop_handler_factory(MagicMock()),
        }
        scenario = make_scenario()

        with pytest.raises(SafetyCheckError) as excinfo:
            await injector.inject(scenario)

        assert "cpu" in str(excinfo.value).lower()
        assert scenario.status == ScenarioStatus.FAILED
        provider.assert_called()

    # ----- branch 5: mem over threshold ------------------------------------

    async def _mem_over_threshold_raises(self) -> None:
        snapshot = make_metrics(cpu_pct=10.0, mem_pct=95.0)
        provider = MagicMock(return_value=snapshot)
        injector = make_injector(
            cpu_emergency_threshold_pct=90.0,
            mem_emergency_threshold_pct=90.0,
            metrics_snapshot=provider,
        )
        injector._test_handlers = {
            FailureType.LATENCY_INJECTION: noop_handler_factory(MagicMock()),
        }
        scenario = make_scenario()

        with pytest.raises(SafetyCheckError) as excinfo:
            await injector.inject(scenario)

        assert "mem" in str(excinfo.value).lower()
        assert scenario.status == ScenarioStatus.FAILED

    # ----- branch 6: provider returns None ---------------------------------

    async def _metrics_provider_returning_none_skips_threshold(self) -> None:
        provider = MagicMock(return_value=None)
        injector = make_injector(
            cpu_emergency_threshold_pct=90.0,
            mem_emergency_threshold_pct=90.0,
            metrics_snapshot=provider,
        )
        injector._test_handlers = {
            FailureType.LATENCY_INJECTION: noop_handler_factory(MagicMock()),
        }
        scenario = make_scenario()

        active = await injector.inject(scenario)

        assert scenario.status == ScenarioStatus.ACTIVE
        assert injector.active_count == 1
        assert active is not None
        provider.assert_called()

    # ----- branch 7: ordering — allowlist before threshold -----------------

    async def _order_allowlist_before_threshold(self) -> None:
        """Allowlist failure beats threshold failure when both would fire."""
        snapshot = make_metrics(cpu_pct=95.0, mem_pct=95.0)
        provider = MagicMock(return_value=snapshot)
        injector = make_injector(
            cpu_emergency_threshold_pct=90.0,
            mem_emergency_threshold_pct=90.0,
            metrics_snapshot=provider,
        )
        # Target is NOT in allowlist AND metrics are over threshold.
        scenario = make_scenario(target="not-in-allowlist")

        with pytest.raises(SafetyCheckError) as excinfo:
            await injector.inject(scenario)

        msg = str(excinfo.value).lower()
        assert "allowlist" in msg
        # Should NOT report cpu/mem — allowlist check came first and short-circuited.
        assert "cpu" not in msg
        assert "mem" not in msg


# ===========================================================================
# B. Routing / dispatch
# ===========================================================================


class TestRouting:
    """The per-FailureType dispatch table fires the correct handler."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure_type",
        [
            FailureType.LATENCY_INJECTION,
            FailureType.PACKET_LOSS,
            FailureType.NETWORK_PARTITION,
            FailureType.RESOURCE_EXHAUSTION,
            FailureType.COMPONENT_FAILURE,
        ],
    )
    async def test_each_failure_type_routes_to_seam_handler(
        self, failure_type: FailureType
    ) -> None:
        """``_test_handlers`` seam is consulted and the right handler runs."""
        injector = make_injector()
        rollback_sentinel = MagicMock(name=f"rollback-{failure_type.value}")
        # Build a handler-per-type dict, but only one handler will actually fire
        # since each scenario only carries one type.
        handlers: dict[FailureType, MagicMock] = {
            ft: MagicMock(return_value=rollback_sentinel if ft == failure_type else None)
            for ft in FailureType
        }
        injector._test_handlers = handlers  # type: ignore[assignment]

        scenario = make_scenario(failure_type=failure_type)
        active = await injector.inject(scenario)

        # Exactly the routed handler was called.
        handlers[failure_type].assert_called_once_with(scenario)
        # No other handler was invoked.
        for other_type, other_handler in handlers.items():
            if other_type is failure_type:
                continue
            other_handler.assert_not_called()

        assert scenario.status == ScenarioStatus.ACTIVE
        assert active.rollback is rollback_sentinel

    # --- NotImplementedError surfaces for every per-type stub ------------- #

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure_type, milestone",
        [
            (FailureType.LATENCY_INJECTION, "C7"),
            (FailureType.PACKET_LOSS, "C7"),
            (FailureType.NETWORK_PARTITION, "C8"),
            (FailureType.RESOURCE_EXHAUSTION, "C9"),
            (FailureType.COMPONENT_FAILURE, "C9"),
        ],
    )
    async def test_stub_raises_not_implemented_with_milestone(
        self, failure_type: FailureType, milestone: str
    ) -> None:
        """Without ``_test_handlers``, the stubs cite the future commit."""
        injector = make_injector()
        scenario = make_scenario(failure_type=failure_type)

        with pytest.raises(NotImplementedError) as excinfo:
            await injector.inject(scenario)

        assert milestone in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_not_implemented_clears_active_registration(self) -> None:
        """A failing handler must unregister so concurrency counts stay honest."""
        injector = make_injector()
        scenario = make_scenario(failure_type=FailureType.LATENCY_INJECTION)

        with pytest.raises(NotImplementedError):
            await injector.inject(scenario)

        assert injector.active_count == 0
        assert scenario.id not in injector.active_ids()


# ===========================================================================
# C. Rollback
# ===========================================================================


class TestRollback:
    """Single + bulk rollback semantics."""

    @pytest.mark.asyncio
    async def test_rollback_invokes_async_finalizer(self) -> None:
        injector = make_injector()
        async_finalizer = AsyncMock(name="async_finalizer")
        injector._test_handlers = {
            FailureType.LATENCY_INJECTION: noop_handler_factory(async_finalizer),
        }

        scenario = make_scenario()
        await injector.inject(scenario)

        await injector.rollback(scenario.id)

        async_finalizer.assert_awaited_once()
        assert injector.active_count == 0
        assert scenario.status == ScenarioStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_rollback_invokes_sync_finalizer(self) -> None:
        injector = make_injector()
        sync_finalizer = MagicMock(name="sync_finalizer", return_value=None)
        injector._test_handlers = {
            FailureType.LATENCY_INJECTION: noop_handler_factory(sync_finalizer),
        }

        scenario = make_scenario()
        await injector.inject(scenario)

        await injector.rollback(scenario.id)

        sync_finalizer.assert_called_once()
        assert injector.active_count == 0
        assert scenario.status == ScenarioStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_rollback_on_unknown_id_is_noop(self) -> None:
        injector = make_injector()
        # Should not raise.
        await injector.rollback("does-not-exist")
        assert injector.active_count == 0

    @pytest.mark.asyncio
    async def test_rollback_swallows_finalizer_exception(self) -> None:
        injector = make_injector()
        raising_finalizer = MagicMock(side_effect=RuntimeError("kaboom"))
        injector._test_handlers = {
            FailureType.LATENCY_INJECTION: noop_handler_factory(raising_finalizer),
        }
        scenario = make_scenario()
        await injector.inject(scenario)

        # No exception escapes despite the finalizer raising.
        await injector.rollback(scenario.id)

        raising_finalizer.assert_called_once()
        assert injector.active_count == 0

    @pytest.mark.asyncio
    async def test_rollback_all_unwinds_lifo(self) -> None:
        """rollback_all calls finalizers in reverse registration order."""
        injector = make_injector(max_concurrent=10)

        # Parent mock so we can compare ``mock_calls`` ordering across mocks.
        parent = MagicMock(name="parent")
        fin_a = MagicMock(name="fin_a", side_effect=lambda: parent.fin_a())
        fin_b = MagicMock(name="fin_b", side_effect=lambda: parent.fin_b())
        fin_c = MagicMock(name="fin_c", side_effect=lambda: parent.fin_c())

        scenario_a = make_scenario()
        scenario_b = make_scenario()
        scenario_c = make_scenario()

        # Per-scenario handler dict — for each inject, swap in the right rollback.
        injector._test_handlers = {
            FailureType.LATENCY_INJECTION: noop_handler_factory(fin_a),
        }
        await injector.inject(scenario_a)

        injector._test_handlers = {
            FailureType.LATENCY_INJECTION: noop_handler_factory(fin_b),
        }
        await injector.inject(scenario_b)

        injector._test_handlers = {
            FailureType.LATENCY_INJECTION: noop_handler_factory(fin_c),
        }
        await injector.inject(scenario_c)

        assert injector.active_count == 3

        await injector.rollback_all()

        assert injector.active_count == 0
        # LIFO: c was registered last, so it must be called first.
        assert parent.mock_calls == [call.fin_c(), call.fin_b(), call.fin_a()]


# ===========================================================================
# D. DockerClient surface
# ===========================================================================


class TestDockerClient:
    """Allowlist + label enforcement and method routing."""

    def _make(self, allowlist: tuple[str, ...] = ALLOWLIST):
        """Construct a DockerClient with a MagicMock injected docker SDK."""
        sdk = MagicMock(name="docker_sdk")
        client = DockerClient(allowlist=allowlist, client=sdk)
        return client, sdk

    def _make_container(self, name: str, *, label_value: str | None = "true"):
        """Build a MagicMock container with optional chaos.target label."""
        container = MagicMock(name=f"container-{name}")
        container.name = name
        if label_value is None:
            container.labels = {}
        else:
            container.labels = {"chaos.target": label_value}
        return container

    # ----- allowlist enforcement -----------------------------------------

    def test_get_target_rejects_unlisted_name_without_calling_sdk(self) -> None:
        client, sdk = self._make()

        with pytest.raises(NotAllowlistedError):
            client.get_target("rogue-container")

        sdk.containers.get.assert_not_called()

    def test_get_target_rejects_when_label_missing(self) -> None:
        """Defense in depth: allowlisted name but no chaos.target=true label."""
        client, sdk = self._make()
        container = self._make_container("log-producer", label_value=None)
        sdk.containers.get.return_value = container

        with pytest.raises(NotAllowlistedError):
            client.get_target("log-producer")

        sdk.containers.get.assert_called_once_with("log-producer")

    def test_get_target_returns_container_on_pass(self) -> None:
        client, sdk = self._make()
        container = self._make_container("log-producer")
        sdk.containers.get.return_value = container

        result = client.get_target("log-producer")

        assert result is container
        sdk.containers.get.assert_called_once_with("log-producer")

    def test_get_target_translates_notfound_to_target_not_found_error(self) -> None:
        from docker.errors import NotFound

        client, sdk = self._make()
        sdk.containers.get.side_effect = NotFound("missing")

        with pytest.raises(TargetNotFoundError):
            client.get_target("log-producer")

    # ----- mutating methods route through get_target ----------------------

    def test_exec_goes_through_get_target(self) -> None:
        client, sdk = self._make()
        container = self._make_container("log-producer")
        container.exec_run.return_value = (0, b"ok\n")
        sdk.containers.get.return_value = container

        exit_code, output = client.exec("log-producer", ["echo", "hi"])

        assert exit_code == 0
        assert output == b"ok\n"
        sdk.containers.get.assert_called_once_with("log-producer")
        container.exec_run.assert_called_once()

    def test_pause_goes_through_get_target(self) -> None:
        client, sdk = self._make()
        container = self._make_container("log-producer")
        sdk.containers.get.return_value = container

        client.pause("log-producer")

        sdk.containers.get.assert_called_once_with("log-producer")
        container.pause.assert_called_once()

    def test_unpause_goes_through_get_target(self) -> None:
        client, sdk = self._make()
        container = self._make_container("log-producer")
        sdk.containers.get.return_value = container

        client.unpause("log-producer")

        sdk.containers.get.assert_called_once_with("log-producer")
        container.unpause.assert_called_once()

    def test_kill_goes_through_get_target(self) -> None:
        client, sdk = self._make()
        container = self._make_container("log-producer")
        sdk.containers.get.return_value = container

        client.kill("log-producer", signal="SIGTERM")

        sdk.containers.get.assert_called_once_with("log-producer")
        container.kill.assert_called_once_with(signal="SIGTERM")

    def test_restart_goes_through_get_target(self) -> None:
        client, sdk = self._make()
        container = self._make_container("log-producer")
        sdk.containers.get.return_value = container

        client.restart("log-producer", timeout=10)

        sdk.containers.get.assert_called_once_with("log-producer")
        container.restart.assert_called_once_with(timeout=10)

    # ----- network surgery ------------------------------------------------

    def test_disconnect_network_returns_aliases_and_ipv4(self) -> None:
        client, sdk = self._make()
        container = self._make_container("log-producer")
        # Pre-populate container.attrs so the wrapper can recover metadata.
        container.attrs = {
            "NetworkSettings": {
                "Networks": {
                    "chaos-net": {
                        "Aliases": ["log-producer", "producer"],
                        "IPAMConfig": {"IPv4Address": "172.18.0.5"},
                    }
                }
            }
        }
        sdk.containers.get.return_value = container

        network = MagicMock(name="network-chaos-net")
        sdk.networks.get.return_value = network

        result = client.disconnect_network("log-producer", "chaos-net")

        assert "aliases" in result
        assert "ipv4" in result
        assert result["aliases"] == ["log-producer", "producer"]
        assert result["ipv4"] == "172.18.0.5"
        network.disconnect.assert_called_once_with(container, force=True)

    def test_disconnect_network_handles_missing_network_metadata(self) -> None:
        """When the container has no record for the network, return empty defaults."""
        client, sdk = self._make()
        container = self._make_container("log-producer")
        container.attrs = {"NetworkSettings": {"Networks": {}}}
        sdk.containers.get.return_value = container
        network = MagicMock(name="network")
        sdk.networks.get.return_value = network

        result = client.disconnect_network("log-producer", "chaos-net")

        assert result["aliases"] == []
        assert result["ipv4"] is None
        network.disconnect.assert_called_once_with(container, force=True)

    def test_connect_network_passes_aliases_and_ipv4(self) -> None:
        client, sdk = self._make()
        container = self._make_container("log-producer")
        sdk.containers.get.return_value = container
        network = MagicMock(name="network")
        sdk.networks.get.return_value = network

        client.connect_network(
            "log-producer",
            "chaos-net",
            aliases=["log-producer", "producer"],
            ipv4="172.18.0.5",
        )

        network.connect.assert_called_once_with(
            container,
            aliases=["log-producer", "producer"],
            ipv4_address="172.18.0.5",
        )

    def test_connect_network_with_no_extras_passes_none(self) -> None:
        client, sdk = self._make()
        container = self._make_container("log-producer")
        sdk.containers.get.return_value = container
        network = MagicMock(name="network")
        sdk.networks.get.return_value = network

        client.connect_network("log-producer", "chaos-net")

        network.connect.assert_called_once_with(
            container, aliases=None, ipv4_address=None
        )

    # ----- listing --------------------------------------------------------

    def test_list_chaos_targets_intersects_label_and_allowlist(self) -> None:
        client, sdk = self._make(allowlist=("log-producer", "log-consumer"))
        # SDK returns three labeled containers; only the two in the allowlist
        # should make it through.
        c1 = self._make_container("log-producer")
        c2 = self._make_container("log-consumer")
        c3 = self._make_container("rogue-target")  # labeled but not allowlisted
        sdk.containers.list.return_value = [c1, c2, c3]

        result = client.list_chaos_targets()

        names = {c.name for c in result}
        assert names == {"log-producer", "log-consumer"}
        sdk.containers.list.assert_called_once_with(
            filters={"label": "chaos.target=true"}
        )

    # ----- allowlist property + close ------------------------------------

    def test_allowlist_property_is_frozenset(self) -> None:
        client, _ = self._make()
        assert client.allowlist == frozenset(ALLOWLIST)
        assert isinstance(client.allowlist, frozenset)

    def test_close_swallows_errors(self) -> None:
        sdk = MagicMock(name="sdk")
        sdk.close.side_effect = OSError("broken pipe")
        client = DockerClient(allowlist=ALLOWLIST, client=sdk)
        # Should not raise.
        client.close()
        sdk.close.assert_called_once()
