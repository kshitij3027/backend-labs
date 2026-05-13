"""Unit tests for C16 — Prometheus ``/metrics`` endpoint + request-id middleware.

Drives a fresh ``app = create_app()`` per test using
``httpx.AsyncClient + ASGITransport`` without entering the lifespan. The
metric singletons in :mod:`src.observability.prom` are module-level (so
state accumulates across tests in the same process); to keep assertions
robust we snapshot the relevant counter/histogram value *before* the
increment and assert that the post-increment value is greater-than-or-
equal-to ``before + expected_delta``. Histogram bucket assertions only
require the bucket count to be at least 1 — accepts ``"1"`` and
``"1.0"`` text encodings.
"""

from __future__ import annotations

import re

from httpx import ASGITransport, AsyncClient
from prometheus_client import CONTENT_TYPE_LATEST

from src.main import create_app
from src.observability.prom import (
    EMERGENCY_STOPS_TOTAL,
    EXPERIMENTS_TOTAL,
    FAULTS_ACTIVE,
    INJECTION_LATENCY_SECONDS,
    RECOVERY_DURATION_SECONDS,
    RECOVERY_FAILURES_TOTAL,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _client(app) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _scrape(client: AsyncClient) -> str:
    resp = await client.get("/metrics")
    assert resp.status_code == 200, resp.text
    # The Prometheus content type starts with text/plain (the full token is
    # ``text/plain; version=0.0.4; charset=utf-8``-ish).
    ctype = resp.headers.get("content-type", "")
    assert ctype.startswith("text/plain") or ctype == CONTENT_TYPE_LATEST, ctype
    return resp.content.decode("utf-8")


def _value_for_metric(body: str, metric_line_prefix: str) -> float:
    """Extract the float value from the first ``^<prefix>`` non-HELP/TYPE line.

    ``metric_line_prefix`` is the full metric+labelset prefix up to (but not
    including) the trailing whitespace before the numeric value — e.g.
    ``chaos_experiments_total{verdict="completed"}`` or just
    ``chaos_faults_active``. Returns ``0.0`` if no matching sample is found
    (which mirrors Prometheus client-lib semantics where unobserved labels
    don't emit a sample at all).
    """
    pattern = re.compile(
        r"^" + re.escape(metric_line_prefix) + r"\s+([0-9eE+\-.]+)\s*$",
        re.MULTILINE,
    )
    match = pattern.search(body)
    if match is None:
        return 0.0
    return float(match.group(1))


# --------------------------------------------------------------------------- #
# /metrics endpoint shape
# --------------------------------------------------------------------------- #


class TestMetricsEndpointShape:
    async def test_metrics_endpoint_returns_200_and_prometheus_content_type(
        self,
    ) -> None:
        app = create_app()
        async with _client(app) as client:
            resp = await client.get("/metrics")
        assert resp.status_code == 200
        ctype = resp.headers.get("content-type", "")
        # CONTENT_TYPE_LATEST is "text/plain; version=...; charset=utf-8".
        assert ctype.startswith("text/plain") or ctype == CONTENT_TYPE_LATEST, (
            f"unexpected content-type: {ctype!r}"
        )

    async def test_metrics_body_contains_all_six_metric_names(self) -> None:
        # HELP/TYPE pre-registration means the metric names show up even
        # before any sample has been observed.
        app = create_app()
        async with _client(app) as client:
            body = await _scrape(client)

        expected = [
            "chaos_experiments_total",
            "chaos_faults_active",
            "chaos_recovery_failures_total",
            "chaos_injection_latency_seconds",
            "chaos_recovery_duration_seconds",
            "chaos_emergency_stops_total",
        ]
        for name in expected:
            assert name in body, f"missing metric {name!r} in /metrics body"


# --------------------------------------------------------------------------- #
# Increments via the module-level singletons
# --------------------------------------------------------------------------- #


class TestMetricIncrements:
    async def test_experiments_total_counter_increments(self) -> None:
        app = create_app()
        async with _client(app) as client:
            before_body = await _scrape(client)
            before = _value_for_metric(
                before_body, 'chaos_experiments_total{verdict="completed"}'
            )

            EXPERIMENTS_TOTAL.labels(verdict="completed").inc()

            after_body = await _scrape(client)
            after = _value_for_metric(
                after_body, 'chaos_experiments_total{verdict="completed"}'
            )

        assert after >= before + 1.0, (
            f"counter did not increment: before={before} after={after}"
        )
        # Also assert the actual line is emitted (matches both `1` and `1.0`).
        assert re.search(
            r'^chaos_experiments_total\{verdict="completed"\}\s+[0-9.]+\s*$',
            after_body,
            re.MULTILINE,
        ), "no chaos_experiments_total{verdict=\"completed\"} sample line found"

    async def test_recovery_failures_total_counter_increments_by_n(self) -> None:
        app = create_app()
        async with _client(app) as client:
            before_body = await _scrape(client)
            before = _value_for_metric(
                before_body,
                'chaos_recovery_failures_total{test_name="HealthProbeTest"}',
            )

            RECOVERY_FAILURES_TOTAL.labels(test_name="HealthProbeTest").inc(2)

            after_body = await _scrape(client)
            after = _value_for_metric(
                after_body,
                'chaos_recovery_failures_total{test_name="HealthProbeTest"}',
            )

        assert after >= before + 2.0, (
            f"counter did not increment by 2: before={before} after={after}"
        )
        assert re.search(
            r'^chaos_recovery_failures_total\{test_name="HealthProbeTest"\}\s+[0-9.]+\s*$',
            after_body,
            re.MULTILINE,
        ), "no chaos_recovery_failures_total{test_name=\"HealthProbeTest\"} sample found"

    async def test_faults_active_gauge_sets_to_value(self) -> None:
        app = create_app()
        # Gauge.set replaces the value (it's not an increment), so an exact
        # equality assertion is safe here.
        FAULTS_ACTIVE.set(3)
        async with _client(app) as client:
            body = await _scrape(client)
        line = re.search(
            r"^chaos_faults_active\s+([0-9.]+)\s*$", body, re.MULTILINE
        )
        assert line is not None, "no chaos_faults_active sample line found"
        assert float(line.group(1)) == 3.0
        # Reset so we don't poison sibling tests that might observe this gauge.
        FAULTS_ACTIVE.set(0)

    async def test_injection_latency_histogram_observes_sample(self) -> None:
        app = create_app()
        async with _client(app) as client:
            before_body = await _scrape(client)
            before_count = _value_for_metric(
                before_body,
                'chaos_injection_latency_seconds_count{type="latency_injection"}',
            )

            INJECTION_LATENCY_SECONDS.labels(type="latency_injection").observe(0.05)

            after_body = await _scrape(client)
            after_count = _value_for_metric(
                after_body,
                'chaos_injection_latency_seconds_count{type="latency_injection"}',
            )

        assert after_count >= before_count + 1.0, (
            f"count did not increase: before={before_count} after={after_count}"
        )
        # A bucket line with le="0.1" should exist for our labelset; the
        # value can be at-least-1 (we observed 0.05 which is below 0.1).
        # prometheus_client emits labels in alphabetical order
        # (le="0.1",type="latency_injection") but we match either order so
        # the test isn't tied to client-lib internals.
        bucket_pattern = re.compile(
            r'^chaos_injection_latency_seconds_bucket\{'
            r'(?:le="0\.1",type="latency_injection"'
            r'|type="latency_injection",le="0\.1")'
            r"\}\s+([0-9.]+)\s*$",
            re.MULTILINE,
        )
        match = bucket_pattern.search(after_body)
        assert match is not None, (
            "no chaos_injection_latency_seconds_bucket{...,le=\"0.1\"} line "
            'for type="latency_injection" found in body'
        )
        assert float(match.group(1)) >= 1.0

    async def test_recovery_duration_histogram_count_increments(self) -> None:
        app = create_app()
        async with _client(app) as client:
            before_body = await _scrape(client)
            before_count = _value_for_metric(
                before_body, "chaos_recovery_duration_seconds_count"
            )

            RECOVERY_DURATION_SECONDS.observe(0.5)

            after_body = await _scrape(client)
            after_count = _value_for_metric(
                after_body, "chaos_recovery_duration_seconds_count"
            )

        assert after_count >= before_count + 1.0, (
            f"count did not increase: before={before_count} after={after_count}"
        )
        assert re.search(
            r"^chaos_recovery_duration_seconds_count\s+[0-9.]+\s*$",
            after_body,
            re.MULTILINE,
        ), "no chaos_recovery_duration_seconds_count line found"

    async def test_emergency_stops_total_counter_increments(self) -> None:
        app = create_app()
        async with _client(app) as client:
            before_body = await _scrape(client)
            before = _value_for_metric(before_body, "chaos_emergency_stops_total")

            EMERGENCY_STOPS_TOTAL.inc()

            after_body = await _scrape(client)
            after = _value_for_metric(after_body, "chaos_emergency_stops_total")

        assert after >= before + 1.0, (
            f"counter did not increment: before={before} after={after}"
        )
        assert re.search(
            r"^chaos_emergency_stops_total\s+[0-9.]+\s*$",
            after_body,
            re.MULTILINE,
        ), "no chaos_emergency_stops_total line found"


# --------------------------------------------------------------------------- #
# request_id_middleware
# --------------------------------------------------------------------------- #


_HEX_32 = re.compile(r"^[0-9a-f]{32}$")


class TestRequestIdMiddleware:
    async def test_health_response_carries_generated_request_id(self) -> None:
        # /health doesn't need lifespan state (it tolerates missing attrs),
        # so we can hit it directly without any state injection.
        app = create_app()
        async with _client(app) as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        rid = resp.headers.get("X-Request-Id") or resp.headers.get("x-request-id")
        assert rid is not None, "X-Request-Id header missing from response"
        assert _HEX_32.match(rid), (
            f"X-Request-Id is not a 32-char lowercase hex string: {rid!r}"
        )

    async def test_health_response_echoes_inbound_request_id(self) -> None:
        app = create_app()
        # Choose a value that's clearly distinguishable from a uuid4 hex.
        provided = "trace-id-abc-123"
        async with _client(app) as client:
            resp = await client.get(
                "/health", headers={"X-Request-Id": provided}
            )
        assert resp.status_code == 200
        rid = resp.headers.get("X-Request-Id") or resp.headers.get("x-request-id")
        assert rid == provided, (
            f"expected response X-Request-Id={provided!r}, got {rid!r}"
        )
