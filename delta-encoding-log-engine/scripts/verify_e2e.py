"""Containerized black-box end-to-end verifier for the delta-encoding log engine.

Run by the compose ``e2e`` profile service (``Dockerfile.test``), this script drives
the LIVE ``app`` container over HTTP + WebSocket — reaching it by **service name**
(``http://app:8080`` via ``APP_URL``), never ``localhost`` — and asserts the end-to-end
behaviour the in-process tests cannot: that a real generate → compress → reconstruct →
stats flow holds together against a running uvicorn server on the compose network.

This is a **true black box**: it never imports ``app.*``. Fidelity is judged by
**canonical JSON** of the entries — ``json.dumps(x, sort_keys=True,
separators=(",",":"))`` — which is the engine's own equality contract (see
*plan.md → Fidelity contract*). The originally-generated ``logs`` are captured from the
``POST /api/generate`` response and every reconstructed entry is compared against them
canonically, so a single field-diff/apply regression is a hard FAIL.

Unlike a fail-fast script, **every check RUNS** regardless of earlier failures: each
prints ``PASS: <name> — <detail>`` or ``FAIL: <name> — <reason>``, the failures are
collected, and the process exits 1 if any check failed (0 only on a clean sweep). Each
check is wrapped so an unexpected exception becomes a labelled FAIL rather than a crash.

A per-run **nonce** seeds the generated batch (so the data is reproducible and distinct
from whatever ``make up`` may already hold), and the suite resets the engine at the very
end so reruns start clean.

Checks, in order (each a hard gate):

 1.  health          — ``GET /health`` → ``{"status":"healthy"}`` (the 30×1s poll).
 2.  fidelity        — generate 1000 → compress → (a) ``POST /api/reconstruct {}`` whole
                       batch canonical-equals the generated logs element-wise, and (b)
                       sampled ``GET /api/logs/{i}`` (0, 1, a keyframe, K±1, mid, last)
                       ``.entry`` canonical-equals ``generated[i]``.
 3.  reduction       — ``GET /api/stats`` → ``storage.delta_reduction >= 60``; the three
                       reductions + ``storage_savings_percent`` present.
 4.  latency         — ~200 random ``GET /api/logs/{i}`` then ``performance
                       .reconstruct_p99_ms < 100`` (p50/p99 reported).
 5.  dashboard_page  — ``GET /`` → 200 with the title, ``chart-reduction`` canvas, and
                       the vendored ``/static/chart.umd.min.js`` reference.
 6.  dashboard_static— ``/static/chart.umd.min.js`` 200 and >100 KB; ``/static/dashboard
                       .js`` 200 non-empty.
 7.  ws_tick         — ``WS /ws`` first tick within ~12s: ``type=="tick"``, ``error``
                       null, ``stats.storage`` present (a healthy tick).
 8.  errors_zero     — (run last of the assertions) ``system.errors == 0`` — the whole
                       flow produced zero internal 500s.
 9.  reset           — ``POST /api/reset`` ok, then ``storage.count == 0``.

Final line: ``E2E: <n_pass>/<n_total> checks passed``; exit 0 only on a full sweep.
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import sys
import time
from collections.abc import Callable
from uuid import uuid4

import httpx
import websockets

# Talk to the app by service name inside the compose network (env-overridable for
# local runs, e.g. APP_URL=http://localhost:8080 against `make up`).
APP_URL = os.environ.get("APP_URL", "http://app:8080").rstrip("/")

#: Per-run nonce. Seeds the generated batch (reproducible + distinct from any data a
#: warm `make up` already holds) and labels this run in the header line.
NONCE = uuid4().hex[:8]
#: A deterministic-but-per-run integer seed for /api/generate, derived from the nonce.
SEED = int(NONCE, 16) % 2_000_000_000

#: Fidelity batch size (matches the plan: generate 1000 → compress → reconstruct).
BATCH = 1000
#: Default keyframe interval the app ships with (KEYFRAME_INTERVAL=100); the sampled
#: indices probe the K±1 boundary around it. Only used to pick interesting indices —
#: the reduction/latency gates do not depend on it.
KEYFRAME_INTERVAL = 100

#: Latency-gate workload: number of random single-entry reconstructs to drive before
#: reading the p99 (enough samples for a stable percentile).
LATENCY_PROBES = 200

#: Default per-request timeout. Hot endpoints answer in milliseconds; the generous
#: budget only ever matters when something is genuinely wrong.
_TIMEOUT = 30.0

# Gates.
MIN_DELTA_REDUCTION = 60.0  # the headline ≥60% storage-reduction claim
MAX_RECONSTRUCT_P99_MS = 100.0  # the <100ms reconstruction-latency criterion


def _canon(obj: object) -> str:
    """The engine's canonical-JSON form — the basis of the fidelity contract.

    ``sort_keys`` canonicalizes (recursively) nested key order and the compact
    separators drop incidental whitespace, so two entries are canon-equal iff they are
    equal as JSON objects regardless of how they were serialized on the wire.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


class Fail(AssertionError):
    """One check's failure, carrying the detail for its FAIL line."""


def _require(condition: bool, detail: str) -> None:
    """Raise :class:`Fail` with ``detail`` unless ``condition`` holds."""
    if not condition:
        raise Fail(detail)


def _wait_healthy(client: httpx.Client) -> int:
    """Poll ``GET /health`` up to 30×1s; return the winning attempt number.

    compose's ``depends_on: condition: service_healthy`` means attempt 1 should win —
    the retry budget only matters when this script is run by hand against a stack that
    is still starting.
    """
    last_error = "no attempt made"
    for attempt in range(1, 31):
        try:
            resp = client.get(f"{APP_URL}/health", timeout=5.0)
            if resp.status_code == 200 and resp.json().get("status") == "healthy":
                return attempt
            last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except httpx.HTTPError as exc:
            last_error = repr(exc)
        time.sleep(1)
    raise Fail(f"{APP_URL}/health never healthy in 30 attempts (last: {last_error})")


# --------------------------------------------------------------------- #
# state shared between checks                                           #
# --------------------------------------------------------------------- #
#: The batch returned by /api/generate in check 2 — the fidelity oracle every
#: reconstruction is compared against. Populated by ``check_fidelity``.
GENERATED: list[dict] = []


# --------------------------------------------------------------------- #
# checks (each returns the PASS-line detail or raises Fail)             #
# --------------------------------------------------------------------- #


def check_health(client: httpx.Client) -> str:
    """``GET /health`` is healthy (the startup poll)."""
    attempt = _wait_healthy(client)
    return f"{APP_URL}/health healthy (attempt {attempt})"


def check_fidelity(client: httpx.Client) -> str:
    """Generate → compress → reconstruct must be byte-for-byte lossless (canonical JSON).

    Generates ``BATCH`` seeded entries, captures them as the oracle, compresses the
    server's pending batch, then proves fidelity two ways: the whole-batch reconstruct
    canon-equals the oracle element-wise, and a spread of single-entry random-access
    reconstructs (``GET /api/logs/{i}``) canon-equal their oracle entry — sampling the
    ends, a keyframe, and the keyframe±1 boundary where delta-replay bugs hide.
    """
    # Generate a reproducible batch and capture it as the fidelity oracle.
    gen = client.post(
        f"{APP_URL}/api/generate", json={"count": BATCH, "seed": SEED}
    )
    _require(gen.status_code == 200, f"/api/generate HTTP {gen.status_code}: {gen.text[:200]}")
    body = gen.json()
    logs = body.get("logs")
    _require(
        isinstance(logs, list) and len(logs) == BATCH,
        f"/api/generate returned {len(logs) if isinstance(logs, list) else logs!r} "
        f"logs, want {BATCH}",
    )
    GENERATED.clear()
    GENERATED.extend(logs)

    # Compress the just-generated batch (server-held).
    comp = client.post(f"{APP_URL}/api/compress", json={"use_generated": True})
    _require(comp.status_code == 200, f"/api/compress HTTP {comp.status_code}: {comp.text[:200]}")

    # (a) Whole-batch reconstruct must canon-equal the oracle element-wise.
    rec = client.post(f"{APP_URL}/api/reconstruct", json={})
    _require(rec.status_code == 200, f"/api/reconstruct HTTP {rec.status_code}: {rec.text[:200]}")
    rec_logs = rec.json().get("logs")
    _require(
        isinstance(rec_logs, list) and len(rec_logs) == BATCH,
        f"reconstruct returned {len(rec_logs) if isinstance(rec_logs, list) else rec_logs!r} "
        f"logs, want {BATCH}",
    )
    mismatches = [
        i
        for i, (got, want) in enumerate(zip(rec_logs, logs))
        if _canon(got) != _canon(want)
    ]
    _require(
        not mismatches,
        f"whole-batch reconstruct differs from generated at "
        f"{len(mismatches)} index(es), first few: {mismatches[:5]}",
    )

    # (b) Sampled single-entry random access. Includes the ends, a keyframe index, and
    # the keyframe±1 boundary (default interval 100) where reconstruction bugs surface.
    last = BATCH - 1
    sample_indices = sorted(
        {
            0,
            1,
            KEYFRAME_INTERVAL,  # a keyframe boundary (100)
            KEYFRAME_INTERVAL - 1,  # 99 — the entry just before a keyframe
            KEYFRAME_INTERVAL + 1,  # 101 — the entry just after a keyframe
            BATCH // 2,  # the middle
            last,  # the very last entry
        }
    )
    for i in sample_indices:
        one = client.get(f"{APP_URL}/api/logs/{i}")
        _require(one.status_code == 200, f"/api/logs/{i} HTTP {one.status_code}: {one.text[:200]}")
        entry = one.json().get("entry")
        _require(
            _canon(entry) == _canon(logs[i]),
            f"/api/logs/{i}.entry differs from generated[{i}] (canonical mismatch)",
        )

    return (
        f"generate {BATCH} → compress → reconstruct lossless: whole batch + "
        f"{len(sample_indices)} sampled indices {sample_indices} all canon-equal"
    )


def check_reduction(client: httpx.Client) -> str:
    """``storage.delta_reduction >= 60`` and the full 3-number breakdown is present."""
    stats = client.get(f"{APP_URL}/api/stats")
    _require(stats.status_code == 200, f"/api/stats HTTP {stats.status_code}")
    storage = stats.json().get("storage", {})

    for key in (
        "delta_reduction",
        "gzip_raw_reduction",
        "delta_plus_gzip_reduction",
        "storage_savings_percent",
    ):
        _require(
            isinstance(storage.get(key), (int, float)),
            f"storage.{key} missing or not numeric: {storage.get(key)!r}",
        )

    delta = storage["delta_reduction"]
    _require(
        delta >= MIN_DELTA_REDUCTION,
        f"storage.delta_reduction {delta} < {MIN_DELTA_REDUCTION} (the ≥60% gate)",
    )
    return (
        f"delta_reduction={delta}% (gate ≥{MIN_DELTA_REDUCTION}); "
        f"gzip_raw={storage['gzip_raw_reduction']}%, "
        f"delta+gzip={storage['delta_plus_gzip_reduction']}%, "
        f"storage_savings_percent={storage['storage_savings_percent']}%"
    )


def check_latency(client: httpx.Client) -> str:
    """Drive random single-entry reconstructs, then assert the p99 latency gate.

    ``GET /api/logs/{i}`` is exactly the path the ``reconstruct`` op times per-entry, so
    this fills the latency window with realistic random-access samples and then reads
    ``performance.reconstruct_p99_ms`` back off ``/api/stats``.
    """
    rng = random.Random(SEED)
    for _ in range(LATENCY_PROBES):
        i = rng.randrange(BATCH)
        resp = client.get(f"{APP_URL}/api/logs/{i}")
        _require(resp.status_code == 200, f"/api/logs/{i} HTTP {resp.status_code} during latency drive")

    perf = client.get(f"{APP_URL}/api/stats").json().get("performance", {})
    p99 = perf.get("reconstruct_p99_ms")
    p50 = perf.get("reconstruct_p50_ms")
    _require(
        isinstance(p99, (int, float)),
        f"performance.reconstruct_p99_ms missing/not numeric: {p99!r}",
    )
    _require(
        p99 < MAX_RECONSTRUCT_P99_MS,
        f"reconstruct_p99_ms {p99} not < {MAX_RECONSTRUCT_P99_MS}",
    )
    return (
        f"{LATENCY_PROBES} random reconstructs: p50={p50}ms, p99={p99}ms "
        f"(gate <{MAX_RECONSTRUCT_P99_MS}ms)"
    )


def check_dashboard_page(client: httpx.Client) -> str:
    """``GET /`` serves the page with the title, the reduction canvas, and Chart.js."""
    resp = client.get(f"{APP_URL}/")
    _require(resp.status_code == 200, f"GET / HTTP {resp.status_code}")
    html = resp.text
    for marker in (
        "Delta Encoding Log Engine",
        "chart-reduction",
        "/static/chart.umd.min.js",
    ):
        _require(marker in html, f"page missing {marker!r}")
    return "page serves with title, chart-reduction canvas, vendored Chart.js"


def check_dashboard_static(client: httpx.Client) -> str:
    """The vendored Chart.js and the dashboard JS both serve with real content (no CDN)."""
    chart = client.get(f"{APP_URL}/static/chart.umd.min.js")
    _require(chart.status_code == 200, f"chart.umd.min.js HTTP {chart.status_code}")
    size = len(chart.content)
    _require(size > 100_000, f"chart.umd.min.js only {size} bytes (>100KB expected)")

    js = client.get(f"{APP_URL}/static/dashboard.js")
    _require(js.status_code == 200, f"dashboard.js HTTP {js.status_code}")
    js_size = len(js.content)
    _require(js_size > 0, "dashboard.js is empty")
    return f"chart.umd.min.js {size} bytes, dashboard.js {js_size} bytes (both served, no CDN)"


async def _read_first_tick() -> dict:
    """Connect to the app's WS and return its first (immediate) tick payload."""
    ws_url = APP_URL.replace("https://", "wss://").replace("http://", "ws://")
    async with websockets.connect(f"{ws_url}/ws", open_timeout=10) as ws:
        raw = await asyncio.wait_for(ws.recv(), timeout=12)
    return json.loads(raw)


def check_ws_tick(client: httpx.Client) -> str:
    """The ``/ws`` contract: an immediate, error-free tick carrying live storage stats."""
    tick = asyncio.run(_read_first_tick())
    _require(tick.get("type") == "tick", f"tick.type {tick.get('type')!r}")
    _require(tick.get("error") is None, f"tick.error {tick.get('error')!r} (want null)")
    stats = tick.get("stats")
    _require(isinstance(stats, dict), f"tick.stats is {type(stats).__name__}, want dict")
    _require("storage" in stats, f"tick.stats missing 'storage': keys={sorted(stats)}")
    return "first tick within 12s, type=tick, error=null, stats.storage present"


def check_errors_zero(client: httpx.Client) -> str:
    """The reliability gate: the whole flow produced zero internal errors.

    Run as the last assertion so it covers every preceding request — a 500 anywhere
    above would have bumped ``system.errors`` and tripped this.
    """
    system = client.get(f"{APP_URL}/api/stats").json().get("system", {})
    errors = system.get("errors")
    _require(errors == 0, f"system.errors == {errors!r}, want 0 (an internal failure occurred)")
    return f"system.errors == 0 (zero internal 500s across the whole flow)"


def check_reset(client: httpx.Client) -> str:
    """``POST /api/reset`` clears the engine: stats read back as empty (count 0)."""
    resp = client.post(f"{APP_URL}/api/reset")
    _require(resp.status_code == 200, f"/api/reset HTTP {resp.status_code}")
    body = resp.json()
    _require(body.get("status") == "reset", f"/api/reset body {body}")

    storage = client.get(f"{APP_URL}/api/stats").json().get("storage", {})
    count = storage.get("count")
    _require(count == 0, f"storage.count {count!r} after reset, want 0")
    return "reset → storage.count == 0 (engine back to empty)"


# --------------------------------------------------------------------- #
# runner                                                                #
# --------------------------------------------------------------------- #

CHECKS: tuple[tuple[str, Callable[[httpx.Client], str]], ...] = (
    ("health", check_health),
    ("fidelity", check_fidelity),
    ("reduction", check_reduction),
    ("latency", check_latency),
    ("dashboard_page", check_dashboard_page),
    ("dashboard_static", check_dashboard_static),
    ("ws_tick", check_ws_tick),
    ("errors_zero", check_errors_zero),
    ("reset", check_reset),
)


def main() -> int:
    """Run every check in order, report PASS/FAIL per check, summarize, return exit code."""
    print(f"E2E target: {APP_URL}  (nonce={NONCE}, seed={SEED})")
    failures: list[str] = []
    with httpx.Client(timeout=_TIMEOUT) as client:
        for name, fn in CHECKS:
            try:
                detail = fn(client)
                print(f"PASS: {name} — {detail}", flush=True)
            except Fail as exc:
                failures.append(name)
                print(f"FAIL: {name} — {exc}", flush=True)
            except Exception as exc:  # noqa: BLE001 — a crash is a labelled FAIL, not a stack trace
                failures.append(name)
                print(
                    f"FAIL: {name} — unexpected {type(exc).__name__}: {exc}",
                    flush=True,
                )

    n_total = len(CHECKS)
    n_pass = n_total - len(failures)
    print(f"E2E: {n_pass}/{n_total} checks passed")
    if failures:
        print(f"E2E: FAILED — {', '.join(failures)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
