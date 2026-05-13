"""Unit tests for the C7 network-injection primitives.

Exercises ``src/injection/network.py`` directly with a mocked
``DockerClient``. We assert the exact ``tc`` argv passed to
``client.exec``, the "File exists" retry-with-``change`` branch, the
input-validation branches, and that ``rollback`` is best-effort/
idempotent.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from src.injection.network import (
    NetworkInjectionError,
    inject_latency,
    inject_packet_loss,
    inject_partition,
    rollback,
    rollback_partition,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def make_client(*results: tuple[int, bytes]) -> MagicMock:
    """Return a MagicMock DockerClient whose ``exec`` returns each tuple in turn.

    Each entry of ``results`` is a ``(exit_code, stderr_bytes)`` pair
    the next ``.exec`` call will return. Useful for arranging the
    add-then-change retry sequence.
    """
    client = MagicMock(name="docker_client")
    if len(results) == 1:
        client.exec.return_value = results[0]
    else:
        client.exec.side_effect = list(results)
    return client


# --------------------------------------------------------------------------- #
# inject_latency
# --------------------------------------------------------------------------- #


class TestInjectLatency:
    """tc-netem ``delay`` injection."""

    def test_happy_path_no_jitter(self) -> None:
        client = make_client((0, b""))

        inject_latency(client, "log-producer", 200)

        client.exec.assert_called_once_with(
            "log-producer",
            ["tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", "200ms"],
            user="root",
        )

    def test_happy_path_with_jitter_appends_jitter_arg(self) -> None:
        client = make_client((0, b""))

        inject_latency(client, "log-producer", 200, jitter_ms=50)

        client.exec.assert_called_once()
        args, kwargs = client.exec.call_args
        container_arg, argv = args
        assert container_arg == "log-producer"
        # Jitter arg comes immediately after the latency arg.
        assert "200ms" in argv
        assert "50ms" in argv
        assert argv.index("50ms") == argv.index("200ms") + 1
        assert kwargs == {"user": "root"}

    def test_file_exists_retries_with_change(self) -> None:
        """First ``add`` reports File exists; we retry with ``change`` and succeed."""
        client = make_client(
            (2, b"RTNETLINK answers: File exists"),
            (0, b""),
        )

        inject_latency(client, "log-producer", 200)

        assert client.exec.call_count == 2
        # Second call must use ``change`` instead of ``add``.
        second_args, second_kwargs = client.exec.call_args_list[1]
        _, second_argv = second_args
        assert "qdisc" in second_argv
        assert "change" in second_argv
        assert "add" not in second_argv
        assert second_kwargs == {"user": "root"}

    def test_invalid_latency_zero_raises_without_exec(self) -> None:
        client = make_client((0, b""))

        with pytest.raises(NetworkInjectionError):
            inject_latency(client, "log-producer", 0)

        client.exec.assert_not_called()

    def test_invalid_latency_negative_raises_without_exec(self) -> None:
        client = make_client((0, b""))

        with pytest.raises(NetworkInjectionError):
            inject_latency(client, "log-producer", -5)

        client.exec.assert_not_called()

    def test_non_benign_error_raises_without_retry(self) -> None:
        client = make_client((1, b"some other error"))

        with pytest.raises(NetworkInjectionError):
            inject_latency(client, "log-producer", 200)

        # Exactly one call: we did NOT retry for a non-"File exists" error.
        assert client.exec.call_count == 1


# --------------------------------------------------------------------------- #
# inject_packet_loss
# --------------------------------------------------------------------------- #


class TestInjectPacketLoss:
    """tc-netem ``loss`` injection."""

    def test_happy_path(self) -> None:
        client = make_client((0, b""))

        inject_packet_loss(client, "log-producer", 10.0)

        client.exec.assert_called_once_with(
            "log-producer",
            ["tc", "qdisc", "add", "dev", "eth0", "root", "netem", "loss", "10.0%"],
            user="root",
        )

    def test_loss_zero_raises_without_exec(self) -> None:
        client = make_client((0, b""))

        with pytest.raises(NetworkInjectionError):
            inject_packet_loss(client, "log-producer", 0)

        client.exec.assert_not_called()

    def test_loss_over_hundred_raises_without_exec(self) -> None:
        client = make_client((0, b""))

        with pytest.raises(NetworkInjectionError):
            inject_packet_loss(client, "log-producer", 101)

        client.exec.assert_not_called()

    def test_file_exists_retries_with_change(self) -> None:
        client = make_client(
            (2, b"RTNETLINK answers: File exists"),
            (0, b""),
        )

        inject_packet_loss(client, "log-producer", 25.0)

        assert client.exec.call_count == 2
        second_args, second_kwargs = client.exec.call_args_list[1]
        _, second_argv = second_args
        assert "qdisc" in second_argv
        assert "change" in second_argv
        assert "add" not in second_argv
        assert second_kwargs == {"user": "root"}


# --------------------------------------------------------------------------- #
# rollback
# --------------------------------------------------------------------------- #


class TestRollback:
    """``rollback`` is best-effort / idempotent: never raises."""

    def test_happy_path_calls_qdisc_del(self) -> None:
        client = make_client((0, b""))

        # Must not raise.
        rollback(client, "log-producer")

        client.exec.assert_called_once_with(
            "log-producer",
            ["tc", "qdisc", "del", "dev", "eth0", "root"],
            user="root",
        )

    def test_already_absent_is_silent(self) -> None:
        """``RTNETLINK answers: No such file or directory`` -> no raise."""
        client = make_client(
            (2, b"RTNETLINK answers: No such file or directory"),
        )

        # Must not raise.
        rollback(client, "log-producer")

        client.exec.assert_called_once()

    def test_other_non_zero_is_swallowed(self) -> None:
        """Best-effort: any other non-zero exit logs but does not raise."""
        client = make_client((1, b"some other failure mode"))

        # Must not raise.
        rollback(client, "log-producer")

        client.exec.assert_called_once()


# --------------------------------------------------------------------------- #
# inject_partition / rollback_partition
# --------------------------------------------------------------------------- #


class TestInjectPartition:
    """``inject_partition`` defers to ``DockerClient.disconnect_network``."""

    def test_happy_path_returns_state_and_delegates(self) -> None:
        """Returns the dict from ``disconnect_network`` verbatim and calls it once."""
        client = MagicMock(name="docker_client")
        expected_state = {"aliases": ["log-consumer"], "ipv4": None}
        client.disconnect_network.return_value = expected_state

        result = inject_partition(client, "log-consumer", "chaos-net")

        assert result == expected_state
        client.disconnect_network.assert_called_once_with(
            "log-consumer", "chaos-net"
        )


class TestRollbackPartition:
    """``rollback_partition`` is best-effort: never re-raises into the engine."""

    def test_happy_path_restores_aliases_and_ipv4(self) -> None:
        client = MagicMock(name="docker_client")
        state = {"aliases": ["log-consumer"], "ipv4": "172.20.0.5"}

        rollback_partition(client, "log-consumer", "chaos-net", state)

        client.connect_network.assert_called_once_with(
            "log-consumer",
            "chaos-net",
            aliases=["log-consumer"],
            ipv4="172.20.0.5",
        )

    def test_state_none_passes_none_defaults(self) -> None:
        """Defensive: ``state=None`` -> both kwargs default to None."""
        client = MagicMock(name="docker_client")

        rollback_partition(client, "log-consumer", "chaos-net", None)

        client.connect_network.assert_called_once_with(
            "log-consumer",
            "chaos-net",
            aliases=None,
            ipv4=None,
        )

    def test_swallows_connect_network_exception(self) -> None:
        """Best-effort: ``client.connect_network`` raising MUST NOT propagate."""
        client = MagicMock(name="docker_client")
        client.connect_network.side_effect = RuntimeError("network gone")
        state = {"aliases": ["log-consumer"], "ipv4": "172.20.0.5"}

        # Must not raise.
        rollback_partition(client, "log-consumer", "chaos-net", state)

        client.connect_network.assert_called_once()
