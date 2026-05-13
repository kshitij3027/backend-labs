"""Unit tests for the C9 resource-injection primitives.

Exercises ``src/injection/resource.py`` with a mocked ``DockerClient``.
We assert the exact ``sh -c nohup stress-ng ...`` argv passed to
``client.exec``, the input-validation branches, and that ``rollback``
is best-effort / idempotent (pkill exit codes 0 / 1 / other all swallowed).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.injection.resource import (
    ResourceInjectionError,
    inject_cpu_pressure,
    inject_memory_pressure,
    rollback,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def make_client(*results: tuple[int, bytes]) -> MagicMock:
    """Return a MagicMock DockerClient whose ``exec`` returns each tuple in turn."""
    client = MagicMock(name="docker_client")
    if len(results) == 1:
        client.exec.return_value = results[0]
    else:
        client.exec.side_effect = list(results)
    return client


# --------------------------------------------------------------------------- #
# inject_cpu_pressure
# --------------------------------------------------------------------------- #


class TestInjectCPUPressure:
    """stress-ng CPU pressure launch."""

    def test_happy_path_defaults(self) -> None:
        client = make_client((0, b"12345"))

        inject_cpu_pressure(client, "log-consumer")

        client.exec.assert_called_once()
        args, kwargs = client.exec.call_args
        container_arg, argv = args
        assert container_arg == "log-consumer"
        # sh -c "nohup stress-ng ... &"
        assert argv[0] == "sh"
        assert argv[1] == "-c"
        # Default knobs: cores=1, load_pct=100, duration_s=30
        assert "stress-ng --cpu 1 --cpu-load 100 --timeout 30s" in argv[2]
        assert kwargs == {"user": "root"}

    def test_custom_params_propagate_to_argv(self) -> None:
        client = make_client((0, b"9999"))

        inject_cpu_pressure(
            client, "log-consumer", cores=2, load_pct=80, duration_s=10
        )

        client.exec.assert_called_once()
        args, kwargs = client.exec.call_args
        container_arg, argv = args
        assert container_arg == "log-consumer"
        assert argv[0] == "sh"
        assert argv[1] == "-c"
        assert "stress-ng --cpu 2 --cpu-load 80 --timeout 10s" in argv[2]
        assert kwargs == {"user": "root"}

    def test_cores_zero_raises_without_exec(self) -> None:
        client = make_client((0, b""))

        with pytest.raises(ResourceInjectionError):
            inject_cpu_pressure(client, "log-consumer", cores=0)

        client.exec.assert_not_called()

    def test_load_pct_zero_raises_without_exec(self) -> None:
        client = make_client((0, b""))

        with pytest.raises(ResourceInjectionError):
            inject_cpu_pressure(client, "log-consumer", load_pct=0)

        client.exec.assert_not_called()

    def test_load_pct_over_hundred_raises_without_exec(self) -> None:
        client = make_client((0, b""))

        with pytest.raises(ResourceInjectionError):
            inject_cpu_pressure(client, "log-consumer", load_pct=101)

        client.exec.assert_not_called()

    def test_duration_zero_raises_without_exec(self) -> None:
        client = make_client((0, b""))

        with pytest.raises(ResourceInjectionError):
            inject_cpu_pressure(client, "log-consumer", duration_s=0)

        client.exec.assert_not_called()

    def test_non_zero_exit_raises_with_stderr_in_message(self) -> None:
        client = make_client((127, b"stress-ng: command not found"))

        with pytest.raises(ResourceInjectionError) as excinfo:
            inject_cpu_pressure(client, "log-consumer")

        # The decoded stderr should be visible in the error message.
        assert "stress-ng: command not found" in str(excinfo.value)


# --------------------------------------------------------------------------- #
# inject_memory_pressure
# --------------------------------------------------------------------------- #


class TestInjectMemoryPressure:
    """stress-ng VM (memory) pressure launch."""

    def test_happy_path_defaults(self) -> None:
        client = make_client((0, b"23456"))

        inject_memory_pressure(client, "log-consumer")

        client.exec.assert_called_once()
        args, kwargs = client.exec.call_args
        container_arg, argv = args
        assert container_arg == "log-consumer"
        assert argv[0] == "sh"
        assert argv[1] == "-c"
        # Default knobs: workers=1, bytes_per_worker="256M", duration_s=30
        assert "stress-ng --vm 1 --vm-bytes 256M --timeout 30s" in argv[2]
        assert kwargs == {"user": "root"}

    def test_custom_params_propagate_to_argv(self) -> None:
        client = make_client((0, b"34567"))

        inject_memory_pressure(
            client,
            "log-consumer",
            bytes_per_worker="512M",
            workers=2,
            duration_s=15,
        )

        client.exec.assert_called_once()
        args, kwargs = client.exec.call_args
        container_arg, argv = args
        assert container_arg == "log-consumer"
        assert "stress-ng --vm 2 --vm-bytes 512M --timeout 15s" in argv[2]
        assert kwargs == {"user": "root"}

    def test_workers_zero_raises_without_exec(self) -> None:
        client = make_client((0, b""))

        with pytest.raises(ResourceInjectionError):
            inject_memory_pressure(client, "log-consumer", workers=0)

        client.exec.assert_not_called()

    def test_duration_zero_raises_without_exec(self) -> None:
        client = make_client((0, b""))

        with pytest.raises(ResourceInjectionError):
            inject_memory_pressure(client, "log-consumer", duration_s=0)

        client.exec.assert_not_called()

    def test_non_zero_exit_raises_with_stderr_in_message(self) -> None:
        client = make_client((1, b"cannot allocate memory"))

        with pytest.raises(ResourceInjectionError) as excinfo:
            inject_memory_pressure(client, "log-consumer")

        assert "cannot allocate memory" in str(excinfo.value)


# --------------------------------------------------------------------------- #
# rollback
# --------------------------------------------------------------------------- #


class TestRollback:
    """``rollback`` is best-effort / idempotent: never raises."""

    def test_happy_path_calls_pkill(self) -> None:
        client = make_client((0, b""))

        # Must not raise.
        rollback(client, "log-consumer")

        client.exec.assert_called_once_with(
            "log-consumer",
            ["pkill", "-f", "stress-ng"],
            user="root",
        )

    def test_no_match_exit_one_is_silent(self) -> None:
        """``pkill`` exit code 1 (no processes matched) -> no raise."""
        client = make_client((1, b""))

        # Must not raise.
        rollback(client, "log-consumer")

        client.exec.assert_called_once_with(
            "log-consumer",
            ["pkill", "-f", "stress-ng"],
            user="root",
        )

    def test_other_non_zero_exit_is_swallowed(self) -> None:
        """Best-effort: any other non-zero exit logs but does not raise."""
        client = make_client((2, b"some other failure"))

        # Must not raise.
        rollback(client, "log-consumer")

        client.exec.assert_called_once()
