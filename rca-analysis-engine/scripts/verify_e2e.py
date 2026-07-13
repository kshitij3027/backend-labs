"""Black-box end-to-end verifier for the RCA Analysis Engine (C10).

Runs **inside Docker** (the profile-gated ``e2e`` compose service) against the LIVE
backend over HTTP + WebSocket only — it never reaches into the analyzer's process. It
MAY import :mod:`src.generators` for **ground truth** (the verifier runs in the tester
image, which ships ``src/``): every incident it posts is a deterministic, seedable
scenario whose injected root cause is known, so correctness can be graded against a
label rather than guessed. The first failing check prints a loud ``FAIL`` line and exits
non-zero immediately, so ``make e2e`` propagates it.

The 11 checks, in order:

 1. ``/api/health`` returns EXACTLY ``{"status": "healthy", "analyzer_ready": true}``.
 2. ``POST /api/analyze-incident`` -> 200 with a structurally valid ``IncidentReport``
    (incident_id, non-empty timeline, root_causes, causal_graph.nodes, impact_analysis).
 3. The timeline is chronologically ordered and its first ``relative_time`` is ``T+0:00``.
 4. Root-cause accuracy >= ``MIN_ACCURACY``: over ``ACCURACY_SAMPLES`` varied scenarios,
    the fraction whose injected ``root_cause_event_id`` lands in the returned top-3
    root_causes (top-1 rate also reported); expected ~1.0.
 5. Analysis latency < ``MAX_ANALYSIS_SECONDS``: time one POST of a moderately-sized
    incident; the measured seconds are reported.
 6. Impact / blast-radius present and non-trivial on a cascade (blast_radius >= 1,
    affected_services non-empty).
 7. WebSocket push: connect ``ws://.../ws``, POST a fresh incident, and assert an
    ``{"type": "incident_update"}`` frame for it arrives within ``E2E_WS_TIMEOUT``.
 8. ``GET /api/incidents`` grows across two reads; ``GET /api/incidents/{id}`` returns a
    posted report; an unknown id -> 404.
 9. ``GET /api/incidents/{id}/report`` -> non-empty markdown + recovery_points +
    classifications (every event classified exactly once).
10. Multi-hypothesis: a report carries >= 2 ``hypotheses`` with independent confidences.
11. ``GET /api/calibration`` returns the stats shape (unfitted initially); after enough
    ``feedback`` posts of incidents with known ground truth it flips to ``fitted: true``
    with numeric raw + calibrated Brier scores (both reported).

Environment knobs (all optional, ``${VAR:-default}`` in compose):

* ``TARGET_URL``            backend base URL (default ``http://backend:8000``)
* ``E2E_READY_TIMEOUT``     seconds to wait for /api/health (default 90)
* ``MIN_ACCURACY``          root-cause top-3 accuracy gate (default 0.85)
* ``ACCURACY_SAMPLES``      number of scenarios for the accuracy check (default 20)
* ``MAX_ANALYSIS_SECONDS``  single-analyze latency ceiling, seconds (default 30)
* ``E2E_LATENCY_EVENTS``    size of the moderately-sized incident timed in check 5 (default 300)
* ``E2E_WS_TIMEOUT``        seconds to wait for the WebSocket frame (default 10)
* ``E2E_FEEDBACK_INCIDENTS`` max incidents fed back to fit the calibrator (default 8)

Exit code: 0 with ``E2E PASSED (11/11)`` only when every check holds; 1 on the first
breach.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections.abc import Callable, Sequence
from typing import Any

import httpx
import websockets
from dateutil import parser as date_parser

from src.generators import generate_events, generate_incident
from src.models import LogEvent

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("TARGET_URL", "http://backend:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("E2E_READY_TIMEOUT", "90"))
MIN_ACCURACY = float(os.environ.get("MIN_ACCURACY", "0.85"))
ACCURACY_SAMPLES = int(os.environ.get("ACCURACY_SAMPLES", "20"))
MAX_ANALYSIS_SECONDS = float(os.environ.get("MAX_ANALYSIS_SECONDS", "30"))
LATENCY_EVENTS = int(os.environ.get("E2E_LATENCY_EVENTS", "300"))
WS_TIMEOUT = float(os.environ.get("E2E_WS_TIMEOUT", "10"))
FEEDBACK_INCIDENTS = int(os.environ.get("E2E_FEEDBACK_INCIDENTS", "8"))

TOTAL_CHECKS = 11

#: How many top-ranked root causes count as a "hit" for the accuracy check.
_TOP_K = 3

#: The spec-frozen /api/health body — asserted verbatim in check 1.
_HEALTH_EXACT = {"status": "healthy", "analyzer_ready": True}

#: The keys every IncidentReport must carry (checked structurally in check 2).
_REPORT_KEYS = frozenset(
    {"incident_id", "timestamp", "timeline", "root_causes", "impact_analysis"}
)


class CheckFailure(AssertionError):
    """Raised inside a check to fail it with a single clear detail line."""


# --------------------------------------------------------------------------- #
# HTTP plumbing
# --------------------------------------------------------------------------- #
CLIENT = httpx.Client(base_url=BASE_URL, timeout=60.0)


def _ws_url() -> str:
    """Derive the ``ws(s)://.../ws`` URL from the HTTP base URL."""
    return "ws" + BASE_URL[len("http"):] + "/ws"


def _events_json(events: Sequence[LogEvent]) -> list[dict]:
    """Serialize LogEvents to the JSON array shape POSTed to ``/api/analyze-incident``."""
    return [event.model_dump(mode="json") for event in events]


def get_json(path: str, params: dict[str, Any] | None = None) -> tuple[int, Any]:
    """GET ``path`` and return (status_code, parsed JSON body or None)."""
    try:
        resp = CLIENT.get(path, params=params)
    except Exception as exc:  # noqa: BLE001 — network failure = check failure
        raise CheckFailure(f"GET {path} raised {type(exc).__name__}: {exc}") from exc
    try:
        body = resp.json()
    except ValueError:
        body = None
    return resp.status_code, body


def api(path: str, params: dict[str, Any] | None = None) -> Any:
    """GET ``path`` expecting HTTP 200 JSON; anything else fails the check."""
    status, body = get_json(path, params)
    if status != 200 or body is None:
        raise CheckFailure(f"GET {path} -> HTTP {status} (expected 200 with a JSON body)")
    return body


def post_report(events: Sequence[LogEvent]) -> dict[str, Any]:
    """POST a batch of events to ``/api/analyze-incident``; return the report (asserts 200)."""
    try:
        resp = CLIENT.post("/api/analyze-incident", json=_events_json(events))
    except Exception as exc:  # noqa: BLE001 — network failure = check failure
        raise CheckFailure(
            f"POST /api/analyze-incident raised {type(exc).__name__}: {exc}"
        ) from exc
    if resp.status_code != 200:
        raise CheckFailure(
            f"POST /api/analyze-incident -> HTTP {resp.status_code} (expected 200): "
            f"{resp.text[:200]}"
        )
    return resp.json()


def wait_ready(timeout: float = READY_TIMEOUT) -> None:
    """Poll GET /api/health until it answers 200, or exit 1 at the timeout."""
    print(f"[e2e] waiting for {BASE_URL}/api/health (up to {timeout:.0f}s)...", flush=True)
    deadline = time.time() + timeout
    last = "no response"
    while time.time() < deadline:
        try:
            resp = CLIENT.get("/api/health", timeout=5.0)
            if resp.status_code == 200:
                print("[e2e] backend is ready", flush=True)
                return
            last = f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001 — the service may still be starting
            last = type(exc).__name__
        time.sleep(2.0)
    print(f"FAIL bootstrap: /api/health not ready after {timeout:.0f}s (last: {last})", flush=True)
    sys.exit(1)


# --------------------------------------------------------------------------- #
# Check runner
# --------------------------------------------------------------------------- #
_counter = 0


def check(name: str, fn: Callable[[], str]) -> None:
    """Run one check; print PASS with evidence, or FAIL + exit 1 immediately."""
    global _counter
    _counter += 1
    prefix = f"[{_counter:2d}/{TOTAL_CHECKS}]"
    try:
        evidence = fn()
    except CheckFailure as exc:
        print(f"{prefix} FAIL {name}: {exc}", flush=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 — an unexpected error is still a failure
        print(f"{prefix} FAIL {name}: unexpected {type(exc).__name__}: {exc}", flush=True)
        sys.exit(1)
    print(f"{prefix} PASS {name} ({evidence})", flush=True)


# --------------------------------------------------------------------------- #
# Structural validation helpers
# --------------------------------------------------------------------------- #
def _require_valid_report(report: dict[str, Any]) -> None:
    """Fail unless ``report`` has the core IncidentReport shape with populated content."""
    missing = _REPORT_KEYS - set(report)
    if missing:
        raise CheckFailure(f"report missing keys {sorted(missing)}")
    if not isinstance(report.get("incident_id"), str) or not report["incident_id"]:
        raise CheckFailure(f"incident_id is {report.get('incident_id')!r} (want a non-empty str)")
    if not report.get("timeline"):
        raise CheckFailure("timeline is empty (want the reconstructed incident timeline)")
    if not report.get("root_causes"):
        raise CheckFailure("root_causes is empty (want ranked candidates)")
    graph = report.get("causal_graph") or {}
    if not graph.get("nodes"):
        raise CheckFailure("causal_graph.nodes is empty (want one node per event)")
    if not isinstance(report.get("impact_analysis"), dict):
        raise CheckFailure(f"impact_analysis is {report.get('impact_analysis')!r} (want an object)")


# --------------------------------------------------------------------------- #
# The 11 checks, in run order
# --------------------------------------------------------------------------- #
def check_health() -> str:
    """1. /api/health answers 200 with the EXACT spec-frozen body — the two keys, nothing more."""
    status, body = get_json("/api/health")
    if status != 200:
        raise CheckFailure(f"/api/health -> HTTP {status} (want 200)")
    if body != _HEALTH_EXACT:
        raise CheckFailure(f"body is {body!r} (want exactly {_HEALTH_EXACT!r})")
    return 'exact {"status":"healthy","analyzer_ready":true}'


def check_analyze_returns_valid_report() -> str:
    """2. Analyzing a generated scenario returns a structurally complete report."""
    scenario = generate_incident(seed=101)
    report = post_report(scenario.events)
    _require_valid_report(report)
    return (
        f"incident {report['incident_id']}: {len(report['timeline'])} timeline entries, "
        f"{len(report['root_causes'])} root causes, "
        f"{len(report['causal_graph']['nodes'])} graph nodes"
    )


def check_timeline_ordered() -> str:
    """3. The timeline is chronologically non-decreasing and starts at T+0:00."""
    report = post_report(generate_incident(seed=102).events)
    timeline = report["timeline"]
    if timeline[0].get("relative_time") != "T+0:00":
        raise CheckFailure(
            f"first entry relative_time is {timeline[0].get('relative_time')!r} (want 'T+0:00')"
        )
    stamps = [date_parser.isoparse(entry["timestamp"]) for entry in timeline]
    for earlier, later in zip(stamps, stamps[1:]):
        if later < earlier:
            raise CheckFailure(f"timeline not chronological: {later} precedes {earlier}")
    seqs = [entry["sequence_id"] for entry in timeline]
    if seqs != sorted(seqs):
        raise CheckFailure(f"sequence_ids not ascending: {seqs}")
    return f"{len(timeline)} entries chronological; first 'T+0:00'; seq ids ascending"


def check_root_cause_accuracy() -> str:
    """4. The injected root cause lands in the top-3 for at least MIN_ACCURACY of scenarios."""
    hits_top3 = 0
    hits_top1 = 0
    for i in range(ACCURACY_SAMPLES):
        scenario = generate_incident(seed=1000 + i)
        report = post_report(scenario.events)
        ranked = [rc["event_id"] for rc in report["root_causes"]]
        truth = scenario.root_cause_event_id
        if truth in ranked[:_TOP_K]:
            hits_top3 += 1
        if ranked and ranked[0] == truth:
            hits_top1 += 1
    accuracy = hits_top3 / ACCURACY_SAMPLES
    top1_rate = hits_top1 / ACCURACY_SAMPLES
    if accuracy < MIN_ACCURACY:
        raise CheckFailure(
            f"top-3 accuracy {accuracy:.3f} < gate {MIN_ACCURACY} over n={ACCURACY_SAMPLES} "
            f"(top-1 {top1_rate:.3f})"
        )
    return (
        f"top-3 accuracy {accuracy:.3f} >= {MIN_ACCURACY} (top-1 {top1_rate:.3f}) "
        f"over n={ACCURACY_SAMPLES}"
    )


def check_analysis_latency() -> str:
    """5. A single analyze of a moderately-sized incident completes well under the ceiling."""
    events = generate_events(LATENCY_EVENTS, seed=2000)
    t0 = time.perf_counter()
    report = post_report(events)
    elapsed = time.perf_counter() - t0
    _require_valid_report(report)
    if elapsed >= MAX_ANALYSIS_SECONDS:
        raise CheckFailure(
            f"analyze of {LATENCY_EVENTS} events took {elapsed:.2f}s >= gate "
            f"{MAX_ANALYSIS_SECONDS:.0f}s"
        )
    return f"{LATENCY_EVENTS} events analyzed in {elapsed:.2f}s < {MAX_ANALYSIS_SECONDS:.0f}s"


def check_impact_present() -> str:
    """6. A cascade produces a non-trivial blast radius and affected-services set."""
    report = post_report(generate_incident(seed=103).events)
    impact = report["impact_analysis"]
    blast_radius = impact.get("blast_radius", 0)
    affected = impact.get("affected_services") or []
    if blast_radius < 1:
        raise CheckFailure(f"blast_radius {blast_radius} < 1 on a cascade (want downstream reach)")
    if not affected:
        raise CheckFailure("affected_services empty on a cascade (want the impacted services)")
    return f"blast_radius {blast_radius}; {len(affected)} affected services {affected}"


def _ws_probe() -> str:
    """Connect /ws, POST a fresh incident, and wait for its incident_update frame."""
    scenario = generate_incident(seed=104)
    payload = _events_json(scenario.events)

    async def run() -> str:
        async with websockets.connect(_ws_url(), open_timeout=WS_TIMEOUT) as ws:
            # The socket is broadcast-eligible only after the server's connect() finishes
            # registering it; a brief pause closes the accept()->register micro-window so
            # the POST that follows is guaranteed to fan out to this client.
            await asyncio.sleep(0.3)
            async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as ac:
                resp = await ac.post("/api/analyze-incident", json=payload)
                if resp.status_code != 200:
                    raise CheckFailure(f"POST during WS probe -> HTTP {resp.status_code}")
                posted_id = resp.json()["incident_id"]

            deadline = time.time() + WS_TIMEOUT
            while time.time() < deadline:
                remaining = max(0.1, deadline - time.time())
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                frame = json.loads(raw)
                if (
                    frame.get("type") == "incident_update"
                    and (frame.get("data") or {}).get("incident_id") == posted_id
                ):
                    return posted_id
            raise CheckFailure(
                f"no incident_update frame for {posted_id} within {WS_TIMEOUT:.0f}s"
            )

    posted_id = asyncio.run(run())
    return f"received incident_update for {posted_id} over the WebSocket"


def check_websocket_push() -> str:
    """7. A new analysis is pushed to a connected WebSocket client in real time."""
    return _ws_probe()


def check_incidents_history() -> str:
    """8. History grows across reads; get-by-id round-trips; an unknown id -> 404."""
    before = api("/api/incidents")
    if not isinstance(before, list):
        raise CheckFailure(f"/api/incidents returned {type(before).__name__} (want a list)")
    report = post_report(generate_incident(seed=105).events)
    posted_id = report["incident_id"]
    after = api("/api/incidents")
    if len(after) <= len(before):
        raise CheckFailure(
            f"history did not grow ({len(before)} -> {len(after)}) after a new analysis"
        )
    if after[0]["incident_id"] != posted_id:
        raise CheckFailure(
            f"newest-first ordering broken: head is {after[0]['incident_id']!r}, "
            f"posted {posted_id!r}"
        )
    fetched = api(f"/api/incidents/{posted_id}")
    if fetched["incident_id"] != posted_id:
        raise CheckFailure(f"GET /api/incidents/{posted_id} returned a different report")
    status, _ = get_json("/api/incidents/does-not-exist")
    if status != 404:
        raise CheckFailure(f"unknown incident -> HTTP {status} (want 404)")
    return f"history {len(before)} -> {len(after)}; get-by-id round-trips; unknown -> 404"


def check_report_export() -> str:
    """9. The post-mortem export returns non-empty markdown + recovery points + classifications."""
    scenario = generate_incident(seed=106)
    report = post_report(scenario.events)
    incident_id = report["incident_id"]
    body = api(f"/api/incidents/{incident_id}/report")
    markdown = body.get("markdown")
    if not isinstance(markdown, str) or not markdown.strip():
        raise CheckFailure("markdown is empty (want a rendered post-mortem)")
    if incident_id not in markdown:
        raise CheckFailure("markdown does not reference the incident id")
    if not body.get("recovery_points"):
        raise CheckFailure("recovery_points empty (want interior choke points for a cascade)")
    classifications = body.get("classifications") or {}
    if not classifications:
        raise CheckFailure("classifications empty (want one class per event)")
    event_ids = {event["event_id"] for event in report["events"]}
    if set(classifications) != event_ids:
        raise CheckFailure("classifications do not cover exactly the incident's events")
    return (
        f"{len(markdown)} chars of markdown; {len(body['recovery_points'])} recovery points; "
        f"{len(classifications)} events classified"
    )


def check_multi_hypothesis() -> str:
    """10. A report retains >= 2 concurrent hypotheses with independent confidences."""
    report = post_report(generate_incident(seed=107).events)
    hypotheses = report.get("hypotheses") or []
    if len(hypotheses) < 2:
        raise CheckFailure(f"only {len(hypotheses)} hypotheses (want >= 2 concurrent explanations)")
    confidences = [hyp.get("confidence") for hyp in hypotheses]
    for conf in confidences:
        if not isinstance(conf, (int, float)) or not 0.0 <= float(conf) <= 1.0:
            raise CheckFailure(f"hypothesis confidence {conf!r} outside [0, 1]")
    total = sum(float(conf) for conf in confidences)
    # Independent (not a normalized-to-1 distribution): either the mass differs from 1 or
    # the confidences are not all identical. Both hold for real anomaly-seeded PPR output.
    independent = abs(total - 1.0) > 1e-6 or len(set(round(float(c), 6) for c in confidences)) > 1
    if not independent:
        raise CheckFailure(f"hypothesis confidences look normalized-to-1: {confidences}")
    return f"{len(hypotheses)} hypotheses, independent confidences (sum {total:.3f})"


def check_calibration() -> str:
    """11. Calibration reports the stats shape and fits once enough outcomes are fed back."""
    initial = api("/api/calibration")
    required = {"method", "n_samples", "fitted", "brier_raw", "brier_calibrated", "reliability_bins"}
    missing = required - set(initial)
    if missing:
        raise CheckFailure(f"/api/calibration missing keys {sorted(missing)}")

    # Feed back incidents with KNOWN ground truth until the calibrator fits. Each feedback
    # records one sample per ranked candidate (the true root positive, the rest negatives),
    # so a few cascades exceed calibration_min_samples with both classes present.
    final: dict[str, Any] = initial
    fed = 0
    for i in range(FEEDBACK_INCIDENTS):
        scenario = generate_incident(seed=3000 + i)
        report = post_report(scenario.events)
        resp = CLIENT.post(
            f"/api/incidents/{report['incident_id']}/feedback",
            json={"true_root_cause_event_id": scenario.root_cause_event_id},
        )
        if resp.status_code != 200:
            raise CheckFailure(
                f"feedback POST -> HTTP {resp.status_code} (want 200): {resp.text[:200]}"
            )
        fed += 1
        final = resp.json()
        if final.get("fitted"):
            break

    if not final.get("fitted"):
        raise CheckFailure(
            f"calibrator still unfitted after {fed} feedback incidents "
            f"(n_samples {final.get('n_samples')})"
        )
    brier_raw = final.get("brier_raw")
    brier_cal = final.get("brier_calibrated")
    for label, value in (("brier_raw", brier_raw), ("brier_calibrated", brier_cal)):
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise CheckFailure(f"{label} is {value!r} (want a number once fitted)")
    return (
        f"fitted after {fed} incidents (n_samples {final.get('n_samples')}); "
        f"brier_raw {brier_raw:.4f}, brier_calibrated {brier_cal:.4f}"
    )


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
def main() -> None:
    print(f"[e2e] == RCA Analysis Engine black-box verifier vs {BASE_URL} ==", flush=True)
    print(
        f"[e2e] gates: root-cause top-3 accuracy >= {MIN_ACCURACY} over {ACCURACY_SAMPLES} "
        f"scenarios; analysis latency < {MAX_ANALYSIS_SECONDS:.0f}s",
        flush=True,
    )
    wait_ready()

    check("health contract verbatim", check_health)
    check("analyze returns a valid report", check_analyze_returns_valid_report)
    check("timeline chronological + T+0:00", check_timeline_ordered)
    check("root-cause accuracy gate", check_root_cause_accuracy)
    check("analysis latency gate", check_analysis_latency)
    check("impact / blast-radius present", check_impact_present)
    check("websocket push on new analysis", check_websocket_push)
    check("incidents history + get-by-id + 404", check_incidents_history)
    check("post-mortem report export", check_report_export)
    check("multi-hypothesis retained", check_multi_hypothesis)
    check("calibration shape + fit on feedback", check_calibration)

    print(f"E2E PASSED ({TOTAL_CHECKS}/{TOTAL_CHECKS})", flush=True)


if __name__ == "__main__":
    main()
