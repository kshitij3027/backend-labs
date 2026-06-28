"""Black-box end-to-end verifier for the Predictive Log Analytics Engine (C13).

Runs **inside Docker** (the profile-gated ``e2e`` service) against the *live* API
over HTTP — no in-process imports of the app, no DB/Redis access. It exercises the
full data flow the system promises:

    ingest synthetic metrics (POST /metrics)
        -> compute an on-demand forecast (GET /forecast/{steps})
        -> read the structured forecast surface (ensemble + individual_forecasts +
           confidence array + alert_level) and assert it matches §8
        -> confirm the supporting endpoints (/health, /metrics, /models) are healthy

It is deliberately self-seeding: a fresh stack has no metrics, so the script first
generates a few hundred points per metric via :mod:`src.generator` (available in the
test image) and POSTs them to ``/metrics``. This guarantees a forecast can be
computed without depending on the Celery worker/Beat having run.

Configuration (all via env, with sensible defaults):

* ``API_BASE_URL``     base URL of the live API (default ``http://api:8000``).
* ``E2E_READY_TIMEOUT`` seconds to wait for ``/health`` to come up (default 60).
* ``E2E_METRIC``       which metric to forecast (default ``response_time``).
* ``E2E_STEPS``        forecast horizon in steps for the on-demand check (default 12).
* ``E2E_SEED_POINTS``  synthetic points to POST per metric (default 400).

Exit code: ``0`` when every check passes; non-zero with a clear ``FAIL:`` message
on the first failed assertion, so ``make e2e`` fails loudly.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

from src.generator import METRIC_NAMES, generate_series

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("API_BASE_URL", "http://api:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("E2E_READY_TIMEOUT", "60"))
METRIC = os.environ.get("E2E_METRIC", "response_time")
STEPS = int(os.environ.get("E2E_STEPS", "12"))
SEED_POINTS = int(os.environ.get("E2E_SEED_POINTS", "400"))

VALID_ALERT_LEVELS = {"high", "medium", "low"}

# Per-check result accumulator; printed as a summary at the end.
_RESULTS: list[tuple[str, bool, str]] = []


def _record(name: str, ok: bool, detail: str = "") -> None:
    _RESULTS.append((name, ok, detail))
    flag = "PASS" if ok else "FAIL"
    line = f"[{flag}] {name}"
    if detail:
        line += f" — {detail}"
    print(line, flush=True)


class CheckError(AssertionError):
    """Raised to fail the verifier with a clear message."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CheckError(message)


# --------------------------------------------------------------------------- #
# Readiness
# --------------------------------------------------------------------------- #
def wait_for_health(client: httpx.Client) -> None:
    """Poll ``GET /health`` until it returns 200, up to ``READY_TIMEOUT`` seconds."""
    deadline = time.time() + READY_TIMEOUT
    last_err = "no response"
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            resp = client.get("/health", timeout=5.0)
            if resp.status_code == 200:
                _record(
                    "health endpoint ready",
                    True,
                    f"200 after {attempt} attempt(s)",
                )
                return
            last_err = f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001 - service may still be starting
            last_err = type(exc).__name__
        time.sleep(2.0)
    raise CheckError(
        f"/health not ready after {READY_TIMEOUT:.0f}s (last: {last_err})"
    )


# --------------------------------------------------------------------------- #
# Seeding (self-contained; uses src.generator + POST /metrics)
# --------------------------------------------------------------------------- #
def seed_metrics(client: httpx.Client) -> None:
    """Ensure metrics exist by POSTing synthetic points for every metric family.

    Generates ``SEED_POINTS`` points per metric ending at *now* (5-min spacing)
    and ingests them via the public ``POST /metrics`` endpoint. This makes the
    verifier independent of the seed CLI and the background worker.
    """
    interval = 300
    end = datetime.now(timezone.utc)
    start = end - timedelta(seconds=interval * (SEED_POINTS + 1))

    total = 0
    for name in METRIC_NAMES:
        series = generate_series(
            metric_name=name,
            start=start,
            end=end,
            interval_seconds=interval,
            seed=1234,
        )
        # Keep only the most recent SEED_POINTS points.
        series = series[-SEED_POINTS:]
        payload = {
            "points": [
                {
                    "metric_name": p.metric_name,
                    "timestamp": p.timestamp.isoformat(),
                    "value": p.value,
                }
                for p in series
            ]
        }
        resp = client.post("/metrics", json=payload, timeout=30.0)
        _require(
            resp.status_code == 201,
            f"POST /metrics for {name!r} returned {resp.status_code}: {resp.text[:200]}",
        )
        body = resp.json()
        ingested = int(body.get("ingested", 0))
        _require(ingested > 0, f"POST /metrics for {name!r} ingested 0 rows")
        total += ingested
    _record("seed metrics (POST /metrics)", True, f"ingested {total} points across {len(METRIC_NAMES)} metrics")


def verify_metric_readback(client: httpx.Client) -> None:
    """Confirm the ingested points are queryable via GET /metrics/{name}."""
    resp = client.get(f"/metrics/{METRIC}", params={"limit": 50}, timeout=15.0)
    _require(resp.status_code == 200, f"GET /metrics/{METRIC} returned {resp.status_code}")
    body = resp.json()
    _require(int(body.get("count", 0)) > 0, f"no points read back for {METRIC!r}")
    _record("metric read-back (GET /metrics/{name})", True, f"count={body['count']}")


# --------------------------------------------------------------------------- #
# Forecast (on-demand compute -> structured §8 payload)
# --------------------------------------------------------------------------- #
def _assert_forecast_shape(body: dict, *, expect_steps: int, source: str) -> None:
    """Assert ``body`` is a valid §8 ForecastResponse with ``expect_steps`` steps."""
    _require("ensemble_prediction" in body, f"{source}: missing ensemble_prediction")
    ens = body["ensemble_prediction"]
    _require(isinstance(ens, list), f"{source}: ensemble_prediction not a list")
    _require(
        len(ens) == expect_steps,
        f"{source}: ensemble_prediction len {len(ens)} != {expect_steps}",
    )
    _require(
        all(isinstance(v, (int, float)) for v in ens),
        f"{source}: ensemble_prediction has non-numeric values",
    )

    conf_arr = body.get("ensemble_confidence")
    _require(isinstance(conf_arr, list), f"{source}: ensemble_confidence not a list")
    _require(
        len(conf_arr) == expect_steps,
        f"{source}: ensemble_confidence len {len(conf_arr)} != {expect_steps}",
    )

    individual = body.get("individual_forecasts")
    _require(isinstance(individual, dict), f"{source}: individual_forecasts not a dict")
    _require(len(individual) >= 1, f"{source}: no individual_forecasts present")

    alert = body.get("alert_level")
    _require(
        alert in VALID_ALERT_LEVELS,
        f"{source}: alert_level {alert!r} not in {sorted(VALID_ALERT_LEVELS)}",
    )

    conf = body.get("confidence")
    _require(
        isinstance(conf, (int, float)) and 0.0 <= float(conf) <= 1.0,
        f"{source}: scalar confidence {conf!r} not in [0, 1]",
    )


def verify_on_demand_forecast(client: httpx.Client) -> None:
    """Compute a forecast via GET /forecast/{steps} and assert the §8 contract."""
    resp = client.get(
        f"/forecast/{STEPS}", params={"metric": METRIC}, timeout=60.0
    )
    _require(
        resp.status_code == 200,
        f"GET /forecast/{STEPS} returned {resp.status_code}: {resp.text[:200]}",
    )
    body = resp.json()
    _assert_forecast_shape(body, expect_steps=STEPS, source=f"/forecast/{STEPS}")
    _record(
        "on-demand forecast (GET /forecast/{steps})",
        True,
        f"steps={STEPS} alert={body['alert_level']} confidence={float(body['confidence']):.3f} "
        f"models={list(body['individual_forecasts'].keys())}",
    )


def verify_predictions_endpoint(client: httpx.Client) -> None:
    """Confirm GET /predictions serves a structured forecast for the metric.

    The scheduled worker may not have run, so /predictions can legitimately 404
    on a fresh stack. We accept either: a 200 with a valid §8 payload (cache/DB
    hit), or a 404 (no scheduled forecast yet) — the on-demand path already proved
    the compute pipeline works. A 5xx or malformed 200 is a failure.
    """
    resp = client.get("/predictions", params={"metric": METRIC}, timeout=30.0)
    if resp.status_code == 200:
        body = resp.json()
        steps = int(body.get("horizon_steps", len(body.get("ensemble_prediction", []))))
        _require(steps >= 1, "/predictions: horizon_steps < 1")
        _assert_forecast_shape(body, expect_steps=steps, source="/predictions")
        _record(
            "latest forecast (GET /predictions)",
            True,
            f"served {steps}-step forecast (cached={body.get('cached')})",
        )
    elif resp.status_code == 404:
        _record(
            "latest forecast (GET /predictions)",
            True,
            "404 (no scheduled forecast yet — on-demand path already verified)",
        )
    else:
        raise CheckError(
            f"GET /predictions returned {resp.status_code}: {resp.text[:200]}"
        )


# --------------------------------------------------------------------------- #
# Supporting surface
# --------------------------------------------------------------------------- #
def verify_health(client: httpx.Client) -> None:
    """GET /health is 200 and reports DB + Redis up."""
    resp = client.get("/health", timeout=15.0)
    _require(resp.status_code == 200, f"GET /health returned {resp.status_code}")
    body = resp.json()
    subs = body.get("subsystems", {})
    _require(bool(subs.get("database")), "health: database subsystem not up")
    _require(bool(subs.get("redis")), "health: redis subsystem not up")
    _record(
        "health subsystems (db + redis)",
        True,
        f"status={body.get('status')} db={subs.get('database')} redis={subs.get('redis')}",
    )


def verify_app_metrics(client: httpx.Client) -> None:
    """GET /metrics (application metrics JSON) is 200 and well-formed."""
    resp = client.get("/metrics", timeout=15.0)
    _require(resp.status_code == 200, f"GET /metrics returned {resp.status_code}")
    body = resp.json()
    _require("resource_usage" in body, "/metrics: missing resource_usage")
    _require("processing_times" in body, "/metrics: missing processing_times")
    _record("application metrics (GET /metrics)", True, "JSON well-formed")


def verify_models(client: httpx.Client) -> None:
    """GET /models is 200 and returns the ensemble roster shape."""
    resp = client.get("/models", timeout=15.0)
    _require(resp.status_code == 200, f"GET /models returned {resp.status_code}")
    body = resp.json()
    _require("models" in body and "count" in body, "/models: missing roster fields")
    _record(
        "ensemble roster (GET /models)",
        True,
        f"count={body.get('count')} deployed={body.get('deployed_count')}",
    )


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
def run() -> int:
    print(f"== E2E verifier against {BASE_URL} ==", flush=True)
    with httpx.Client(base_url=BASE_URL) as client:
        wait_for_health(client)
        seed_metrics(client)
        verify_metric_readback(client)
        verify_on_demand_forecast(client)
        verify_predictions_endpoint(client)
        verify_health(client)
        verify_app_metrics(client)
        verify_models(client)
    return 0


def main() -> int:
    try:
        run()
    except CheckError as exc:
        print(f"\nFAIL: {exc}", file=sys.stderr, flush=True)
        _summary()
        return 1
    except Exception as exc:  # noqa: BLE001 - any unexpected error is a failure
        print(f"\nFAIL: unexpected error: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        _summary()
        return 2
    _summary()
    print("\nE2E PASS: all checks succeeded.", flush=True)
    return 0


def _summary() -> None:
    passed = sum(1 for _, ok, _ in _RESULTS if ok)
    total = len(_RESULTS)
    print(f"\n--- E2E summary: {passed}/{total} checks passed ---", flush=True)


if __name__ == "__main__":
    sys.exit(main())
