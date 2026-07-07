"""Synthetic e-commerce log generator: 5 sources, coherent checkout journeys,
rotating incident scenarios, and ground truth for the E2E accuracy check.

Behavior per 1-second ``generate(now)`` tick:

1. **Journeys** — spawn ``Poisson(events_per_second * 0.8 / 7)`` checkout
   journeys. Each journey emits ~6-7 lines over its life, hop-scheduled across
   0-4 s: web request -> api validate -> db query -> inventory reserve ->
   payment -> web checkout response. All hops share one ``corr_<8hex>`` +
   ``user_<1..500>`` pair, and a :class:`JourneyRecord` lands in
   :attr:`LogGenerator.journeys` at spawn (ground truth).
2. **Noise** — ~20% of the per-tick budget as background lines with NO
   correlation/user ids (web healthchecks, db ``SELECT 1``, api metrics pings,
   inventory stock syncs), so id-based detectors have something to ignore.
3. **Scenarios** — a deterministic rotation on the scenario clock (epoch = first
   ``generate`` call): each ``scenario_period_seconds`` (45 s) slot advances
   through [DB_POOL_SATURATION, quiet, PAYMENT_SLOWDOWN, quiet,
   INVENTORY_TIMEOUTS, quiet], active only for the slot's first
   ``scenario_duration_seconds`` (20 s). While active, symptoms genuinely
   co-move (e.g. db pool exhaustion + web 5xx) so the target correlations are
   real, not coincidental.
4. **Drain** — every pending line whose due time has arrived is returned as
   ``(SourceType, raw_line)``; each line's embedded timestamp IS its due time.

Determinism: all randomness flows through the injected ``random.Random`` and all
time comes from the explicit ``now`` argument — no wall-clock reads — so seeded
tests replay identical traffic. Line formats are produced exclusively by the
``_fmt_*`` helpers below, the single encoding source of truth matched by
:mod:`src.parsers` (which owns the shared timestamp constants).

The hot path stays allocation-light: pending lines live in one heapq of plain
tuples, and lines are formatted exactly once, at schedule time.
"""

from __future__ import annotations

import heapq
import json
import math
import random
from collections import deque
from datetime import datetime

from src.config import Settings
from src.models import CHECKOUT_FAILED, JourneyRecord, ScenarioKind, SourceType
from src.parsers import MONTH_ABBR, SIM_TZ, SIM_TZ_ISO, SIM_TZ_NAME, SIM_TZ_NGINX

#: One raw generated line: (source it came from, the unparsed line text).
RawLine = tuple[SourceType, str]

#: Scenario rotation: one entry per scenario-period slot (None = quiet slot).
_SCENARIO_CYCLE: tuple[ScenarioKind | None, ...] = (
    ScenarioKind.DB_POOL_SATURATION,
    None,
    ScenarioKind.PAYMENT_SLOWDOWN,
    None,
    ScenarioKind.INVENTORY_TIMEOUTS,
    None,
)

#: Base seconds between consecutive journey hops (jittered x0.8-1.5 per hop, so
#: offsets stay strictly increasing and the whole journey spans < 4 s).
_HOP_GAPS = (0.2, 0.3, 0.5, 0.8, 0.7)
#: Spec sizing constant: a journey emits ~7 lines over its life.
_LINES_PER_JOURNEY = 7.0
_JOURNEY_BUDGET_FRACTION = 0.8
_NOISE_BUDGET_FRACTION = 0.2

_DB_POOL_SIZE = 20


# --- Timestamp encoders (inverse of src.parsers extraction) -------------------
def _ts_web(ts: float) -> str:
    """Epoch seconds -> nginx $time_local style ``08/Jul/2026:10:00:00.123 -0700``."""
    dt = datetime.fromtimestamp(ts, tz=SIM_TZ)
    return (
        f"{dt.day:02d}/{MONTH_ABBR[dt.month - 1]}/{dt.year:04d}"
        f":{dt:%H:%M:%S}.{dt.microsecond // 1000:03d} {SIM_TZ_NGINX}"
    )


def _ts_db(ts: float) -> str:
    """Epoch seconds -> postgresql style ``2026-07-08 10:00:00.123 PDT``."""
    dt = datetime.fromtimestamp(ts, tz=SIM_TZ)
    return f"{dt:%Y-%m-%d %H:%M:%S}.{dt.microsecond // 1000:03d} {SIM_TZ_NAME}"


def _ts_iso(ts: float) -> str:
    """Epoch seconds -> ISO-8601 ``2026-07-08T10:00:00.123-07:00``."""
    dt = datetime.fromtimestamp(ts, tz=SIM_TZ)
    return f"{dt:%Y-%m-%dT%H:%M:%S}.{dt.microsecond // 1000:03d}{SIM_TZ_ISO}"


# --- Line formatters (single source of truth for the wire formats) ------------
def _fmt_web(
    ts: float,
    method: str,
    path: str,
    status: int,
    *,
    ip: str = "192.168.1.10",
    nbytes: int = 512,
    ua: str = "Mozilla/5.0",
    corr: str | None = None,
    user: str | None = None,
    latency_ms: float | None = None,
) -> str:
    parts = [f'{ip} - - [{_ts_web(ts)}] "{method} {path} HTTP/1.1" {status} {nbytes} "-" "{ua}"']
    if corr is not None:
        parts.append(f"corr={corr}")
    if user is not None:
        parts.append(f"user={user}")
    if latency_ms is not None:
        parts.append(f"latency_ms={latency_ms:.1f}")
    return " ".join(parts)


def _db_meta(corr: str | None, user: str | None, pool_in_use: int, pool_size: int) -> str:
    parts = []
    if corr is not None:
        parts.append(f"corr={corr}")
    if user is not None:
        parts.append(f"user={user}")
    parts.append(f"pool={pool_in_use}/{pool_size}")
    return " ".join(parts)


def _fmt_db_log(
    ts: float,
    pid: int,
    duration_ms: float,
    statement: str,
    *,
    corr: str | None = None,
    user: str | None = None,
    pool_in_use: int = 3,
    pool_size: int = _DB_POOL_SIZE,
) -> str:
    meta = _db_meta(corr, user, pool_in_use, pool_size)
    return (
        f"{_ts_db(ts)} [{pid}] LOG:  duration: {duration_ms:.3f} ms"
        f"  statement: {statement} /* {meta} */"
    )


def _fmt_db_error(
    ts: float,
    pid: int,
    message: str,
    *,
    corr: str | None = None,
    user: str | None = None,
    pool_in_use: int = 3,
    pool_size: int = _DB_POOL_SIZE,
) -> str:
    meta = _db_meta(corr, user, pool_in_use, pool_size)
    return f"{_ts_db(ts)} [{pid}] ERROR:  {message} /* {meta} */"


def _fmt_db_fatal_pool(ts: float, pid: int, *, pool_size: int = _DB_POOL_SIZE) -> str:
    return (
        f"{_ts_db(ts)} [{pid}] FATAL:  connection pool exhausted"
        f" /* pool={pool_size}/{pool_size} */"
    )


def _fmt_api(
    ts: float,
    level: str,
    message: str,
    endpoint: str,
    status: int,
    latency_ms: float,
    *,
    corr: str | None = None,
    user: str | None = None,
    error_code: str | None = None,
) -> str:
    obj: dict[str, object] = {
        "ts": _ts_iso(ts),
        "level": level,
        "service": "api-service",
        "message": message,
    }
    if corr is not None:
        obj["correlation_id"] = corr
    if user is not None:
        obj["user_id"] = user
    obj["endpoint"] = endpoint
    obj["status"] = status
    obj["latency_ms"] = round(latency_ms, 1)
    if error_code is not None:
        obj["error_code"] = error_code
    return json.dumps(obj)


def _fmt_payment(
    ts: float,
    level: str,
    event: str,
    corr: str,
    user: str,
    amount: float,
    latency_ms: float,
    status: str,
) -> str:
    return (
        f"ts={_ts_iso(ts)} level={level} event={event} corr={corr} user={user}"
        f" amount={amount:.2f} latency_ms={latency_ms:.1f} status={status}"
    )


def _fmt_inventory(
    ts: float,
    op: str,
    sku: str,
    qty: int,
    status: str,
    latency_ms: float,
    *,
    corr: str | None = None,
    user: str | None = None,
) -> str:
    parts = [f"[{_ts_iso(ts)}] INVENTORY {op} sku={sku} qty={qty} status={status}"]
    if corr is not None:
        parts.append(f"corr={corr}")
    if user is not None:
        parts.append(f"user={user}")
    parts.append(f"latency_ms={latency_ms:.1f}")
    return " ".join(parts)


class LogGenerator:
    """Deterministic multi-source e-commerce log simulator (see module docstring)."""

    def __init__(self, settings: Settings, rng: random.Random | None = None) -> None:
        self.settings = settings
        self.rng = rng or random.Random()
        #: Ground truth: one record per spawned journey (bounded; the E2E
        #: accuracy check reads this via the debug API in C8).
        self.journeys: deque[JourneyRecord] = deque(maxlen=2000)
        #: Pending-line schedule: heap of (due_ts, seq, source, formatted line).
        #: `seq` is an insertion counter that breaks due_ts ties so heapq never
        #: compares enums/strings and equal-time lines keep insertion order.
        self._pending: list[tuple[float, int, SourceType, str]] = []
        self._seq = 0
        #: Diagnostic counters: user ids drawn / journeys spawned so far.
        self._user_seq = 0
        self._journey_seq = 0
        #: Scenario clock base — set by the first generate() call.
        self._epoch: float | None = None

        eps = float(settings.events_per_second)
        self._journey_rate = eps * _JOURNEY_BUDGET_FRACTION / _LINES_PER_JOURNEY
        self._noise_rate = eps * _NOISE_BUDGET_FRACTION

    # --- Scenario clock -------------------------------------------------------
    def active_scenario(self, now: float) -> ScenarioKind | None:
        """The scenario active at ``now``, or None (quiet slot / cooldown / no epoch).

        Deterministic rotation: slot = (elapsed // period) mod len(cycle); the
        slot's scenario is active only for the first `scenario_duration_seconds`
        of the slot.
        """
        if self._epoch is None:
            return None
        elapsed = now - self._epoch
        if elapsed < 0:
            return None
        period = float(self.settings.scenario_period_seconds)
        slot = int(elapsed // period) % len(_SCENARIO_CYCLE)
        if (elapsed % period) < self.settings.scenario_duration_seconds:
            return _SCENARIO_CYCLE[slot]
        return None

    # --- Main tick -------------------------------------------------------------
    def generate(self, now: float) -> list[RawLine]:
        """Advance the simulation to ``now`` and return every line now due."""
        if self._epoch is None:
            self._epoch = now
        scenario = self.active_scenario(now)

        # Guaranteed per-tick scenario bursts (no ids — infrastructure-level noise
        # that makes each incident's signature unmissable within one tick).
        if scenario is ScenarioKind.DB_POOL_SATURATION:
            for _ in range(self.rng.randint(2, 4)):
                self._push(now, SourceType.DATABASE, _fmt_db_fatal_pool(now, self._pid()))
        elif scenario is ScenarioKind.INVENTORY_TIMEOUTS:
            for _ in range(self.rng.randint(2, 4)):
                self._push(
                    now,
                    SourceType.INVENTORY,
                    _fmt_inventory(now, "sync", self._sku(), self.rng.randint(1, 50),
                                   "timeout", self.rng.uniform(5, 25) * 10.0),
                )

        for _ in range(self._poisson(self._noise_rate)):
            self._push_noise(now, scenario)
        for _ in range(self._poisson(self._journey_rate)):
            self._spawn_journey(now, scenario)

        # Drain everything due. Lines were formatted at schedule time with their
        # due_ts embedded, so this loop is pure heap pops + tuple appends.
        out: list[RawLine] = []
        pending = self._pending
        while pending and pending[0][0] <= now:
            _, _, src, line = heapq.heappop(pending)
            out.append((src, line))
        return out

    # --- Journey simulation -----------------------------------------------------
    def _spawn_journey(self, now: float, scenario: ScenarioKind | None) -> None:
        """Schedule one checkout journey's hop lines and record its ground truth.

        Scenario effects are decided at spawn time (the incident the user hit),
        even though the affected lines drain over the next few seconds.
        """
        rng = self.rng
        self._journey_seq += 1
        self._user_seq += 1
        corr = f"corr_{rng.getrandbits(32):08x}"
        user_n = rng.randint(1, 500)
        user = f"user_{user_n}"

        # Strictly increasing hop times: base gaps jittered x0.8-1.5.
        ts = [now]
        for gap in _HOP_GAPS:
            ts.append(ts[-1] + gap * rng.uniform(0.8, 1.5))

        db_saturated = scenario is ScenarioKind.DB_POOL_SATURATION
        pay_slow = scenario is ScenarioKind.PAYMENT_SLOWDOWN
        inv_affected = scenario is ScenarioKind.INVENTORY_TIMEOUTS and rng.random() < 0.5
        web_fail = db_saturated and rng.random() < 0.4
        pay_timeout = pay_slow and rng.random() < 0.25
        abandoned = pay_slow and rng.random() < 0.35

        sources: list[str] = []

        def push(t: float, src: SourceType, line: str) -> None:
            sources.append(src.value)
            self._push(t, src, line)

        # Hop 0 — web request (~1% background 4xx/5xx keeps error detectors honest
        # outside scenarios too).
        req_status = 200 if rng.random() >= 0.01 else rng.choice((404, 500, 502))
        push(ts[0], SourceType.WEB, _fmt_web(
            ts[0], "POST", "/api/checkout/start", req_status,
            ip=f"192.168.1.{rng.randint(2, 250)}", nbytes=rng.randint(300, 4000),
            corr=corr, user=user, latency_ms=rng.uniform(20, 90),
        ))
        # Hop 1 — api validate (latency x3 while the db pool is saturated).
        push(ts[1], SourceType.API_SERVICE, _fmt_api(
            ts[1], "INFO", "checkout step completed", "/checkout/validate", 200,
            rng.uniform(15, 60) * (3.0 if db_saturated else 1.0),
            corr=corr, user=user,
        ))
        # Hop 2 — db query (saturation: pool pegged at 20/20, duration x5).
        push(ts[2], SourceType.DATABASE, _fmt_db_log(
            ts[2], self._pid(),
            rng.uniform(2.0, 20.0) * (5.0 if db_saturated else 1.0),
            f"SELECT * FROM orders WHERE user_id={user_n}",
            corr=corr, user=user,
            pool_in_use=_DB_POOL_SIZE if db_saturated else rng.randint(1, 8),
        ))
        # Hop 3 — inventory reserve (timeout + latency x10 when affected).
        push(ts[3], SourceType.INVENTORY, _fmt_inventory(
            ts[3], "reserve", self._sku(), rng.randint(1, 3),
            "timeout" if inv_affected else "ok",
            rng.uniform(5, 25) * (10.0 if inv_affected else 1.0),
            corr=corr, user=user,
        ))
        # Hop 4 — payment (slowdown: latency x8; some timeouts; ~2% declines).
        if pay_timeout:
            pay_status, pay_level = "timeout", "ERROR"
        elif rng.random() < 0.02:
            pay_status, pay_level = "declined", "WARN"
        else:
            pay_status, pay_level = "success", "INFO"
        push(ts[4], SourceType.PAYMENT, _fmt_payment(
            ts[4], pay_level, "payment_processed", corr, user,
            rng.uniform(9.99, 299.99),
            rng.uniform(150, 320) * (8.0 if pay_slow else 1.0),
            pay_status,
        ))
        # Hop 5 — checkout outcome: abandonment / inventory-driven failure /
        # pool-driven 5xx / normal completion.
        if abandoned:
            push(ts[5], SourceType.WEB, _fmt_web(
                ts[5], "POST", "/api/cart/abandon", 200,
                nbytes=rng.randint(100, 300), corr=corr, user=user,
                latency_ms=rng.uniform(5, 20),
            ))
        elif inv_affected:
            push(ts[5], SourceType.API_SERVICE, _fmt_api(
                ts[5], "ERROR", "checkout failed: inventory reservation timed out",
                "/checkout/complete", 500, rng.uniform(900, 1500),
                corr=corr, user=user, error_code=CHECKOUT_FAILED,
            ))
            push(ts[5] + 0.05, SourceType.WEB, _fmt_web(
                ts[5] + 0.05, "POST", "/api/checkout/complete", 500,
                nbytes=rng.randint(100, 300), corr=corr, user=user,
                latency_ms=rng.uniform(900, 1500),
            ))
        elif web_fail:
            push(ts[5], SourceType.WEB, _fmt_web(
                ts[5], "POST", "/api/checkout/complete", rng.choice((500, 502, 503)),
                nbytes=rng.randint(100, 300), corr=corr, user=user,
                latency_ms=rng.uniform(400, 900),
            ))
        else:
            push(ts[5], SourceType.WEB, _fmt_web(
                ts[5], "POST", "/api/checkout/complete", 200,
                nbytes=rng.randint(200, 900), corr=corr, user=user,
                latency_ms=rng.uniform(150, 450),
            ))

        self.journeys.append(JourneyRecord(
            correlation_id=corr,
            user_id=user,
            sources=sources,
            started_at=ts[0],
            completed_at=ts[5] + 0.05 if inv_affected else ts[5],
            abandoned=abandoned,
        ))

    # --- Background noise ---------------------------------------------------------
    def _push_noise(self, now: float, scenario: ScenarioKind | None) -> None:
        """One id-less background line (healthchecks, SELECT 1, pings, syncs)."""
        rng = self.rng
        roll = rng.random()
        if roll < 0.40:
            self._push(now, SourceType.WEB, _fmt_web(
                now, "GET", "/health", 200,
                ip=f"10.0.0.{rng.randint(2, 9)}", nbytes=15, ua="kube-probe/1.29",
            ))
        elif roll < 0.65:
            saturated = scenario is ScenarioKind.DB_POOL_SATURATION
            pool = _DB_POOL_SIZE if saturated else rng.randint(1, 6)
            if rng.random() < 0.03:  # occasional plain query errors (background)
                self._push(now, SourceType.DATABASE, _fmt_db_error(
                    now, self._pid(), "deadlock detected", pool_in_use=pool,
                ))
            else:
                stmt = rng.choice((
                    "SELECT 1",
                    "SELECT count(*) FROM sessions",
                    "SELECT * FROM cache_entries LIMIT 10",
                ))
                self._push(now, SourceType.DATABASE, _fmt_db_log(
                    now, self._pid(), rng.uniform(0.1, 2.5), stmt, pool_in_use=pool,
                ))
        elif roll < 0.85:
            self._push(now, SourceType.API_SERVICE, _fmt_api(
                now, "INFO", "metrics ping", "/internal/metrics", 200,
                rng.uniform(0.5, 4.0),
            ))
        else:
            self._push(now, SourceType.INVENTORY, _fmt_inventory(
                now, "sync", self._sku(), rng.randint(5, 80), "ok",
                rng.uniform(2, 15),
            ))

    # --- Internals ------------------------------------------------------------------
    def _push(self, due_ts: float, source: SourceType, line: str) -> None:
        self._seq += 1
        heapq.heappush(self._pending, (due_ts, self._seq, source, line))

    def _poisson(self, lam: float) -> int:
        """Knuth Poisson sampler driven by self.rng (lam is small: <= ~2*eps/7)."""
        if lam <= 0.0:
            return 0
        threshold = math.exp(-lam)
        k = 0
        p = 1.0
        while True:
            p *= self.rng.random()
            if p <= threshold:
                return k
            k += 1

    def _pid(self) -> int:
        return self.rng.randint(8000, 8999)

    def _sku(self) -> str:
        return f"SKU-{self.rng.randint(1000, 9999)}"
