"""Per-source log parsers: raw line -> standardized :class:`src.models.LogEvent`.

Contract (consumed by the C3 collector)::

    parse_line(source, line, ingested_at) -> LogEvent | None

- **Never raises** — hostile/garbage/empty input yields ``None`` (a blanket
  guard in :func:`parse_line` backs up the per-parser checks).
- The event ``timestamp`` is extracted from the line itself; when the embedded
  timestamp is missing or malformed the ``ingested_at`` argument is used as the
  documented fallback. Parsers never consult the wall clock.
- The wire formats are produced by :mod:`src.generators` — its formatter
  helpers and the regexes here are the two halves of one contract. The shared
  timestamp constants (``SIM_TZ*``, ``MONTH_ABBR``) live HERE and are imported
  by the generator so both sides agree exactly (and stay locale-independent:
  no ``%b`` strftime/strptime anywhere).

Formats handled:

- WEB (nginx combined + optional ``corr=/user=/latency_ms=`` trailer)
- DATABASE (postgresql-style ``LOG/ERROR/FATAL`` lines with an optional
  ``/* corr=... user=... pool=a/b */`` comment)
- API_SERVICE (one JSON object per line)
- PAYMENT (logfmt ``k=v`` pairs)
- INVENTORY (``[iso-ts] INVENTORY <op> k=v ...`` bracket format)
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from src.models import (
    CART_ABANDONED,
    DB_POOL_EXHAUSTED,
    DB_QUERY_ERROR,
    INVENTORY_TIMEOUT,
    PAYMENT_DECLINED,
    PAYMENT_TIMEOUT,
    LogEvent,
    SourceType,
)

# --- Shared wire-format constants (imported by src.generators) ---------------
#: The simulation's fixed wall-clock zone. Every generated line embeds this
#: offset, so parsing is deterministic regardless of host/container TZ.
SIM_TZ = timezone(timedelta(hours=-7))
SIM_TZ_NAME = "PDT"  # postgresql-style zone abbreviation
SIM_TZ_ISO = "-07:00"  # ISO-8601 offset suffix
SIM_TZ_NGINX = "-0700"  # nginx $time_local offset
#: Locale-independent English month abbreviations (index = month - 1).
MONTH_ABBR = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

_MONTH_NUM = {abbr: i + 1 for i, abbr in enumerate(MONTH_ABBR)}
#: Zone abbreviation -> UTC offset hours for the postgresql format. The
#: generator only emits PDT; unknown abbreviations fall back to the sim zone.
_TZ_ABBR_HOURS = {"PDT": -7, "PST": -8, "UTC": 0, "GMT": 0}

#: Per-source `service` names stamped on parsed events.
SERVICE_BY_SOURCE = {
    SourceType.WEB: "nginx",
    SourceType.DATABASE: "postgresql",
    SourceType.API_SERVICE: "api-service",
    SourceType.PAYMENT: "payment-service",
    SourceType.INVENTORY: "inventory-service",
}

# --- Compiled line regexes ----------------------------------------------------
#: Generic `key=value` pairs (web trailer, db comment, payment/inventory bodies).
_KV_RE = re.compile(r"(\w+)=(\S+)")

_WEB_RE = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ \[(?P<ts>[^\]]+)\] '
    r'"(?P<method>[A-Z]+) (?P<path>\S+) HTTP/[0-9.]+" '
    r'(?P<status>\d{3}) (?P<bytes>\d+|-) '
    r'"[^"]*" "[^"]*"'
    r'(?P<trailer>.*)$'
)
_NGINX_TS_RE = re.compile(
    r"^(\d{1,2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})"
    r"(?:\.(\d{1,6}))?\s+([+-])(\d{2})(\d{2})$"
)

_DB_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?) (?P<tz>[A-Z]{2,5}) "
    r"\[(?P<pid>\d+)\] (?P<sev>[A-Z]+):\s+(?P<body>.*?)"
    r"(?:\s*/\*\s*(?P<meta>.*?)\s*\*/)?\s*$"
)
_DB_DURATION_RE = re.compile(r"^duration:\s*([0-9.]+)\s*ms\s+statement:\s*(.*)$")

_INV_RE = re.compile(r"^\[(?P<ts>[^\]]+)\]\s+INVENTORY\s+(?P<op>\w+)\s+(?P<rest>.*)$")


def _new_event_id() -> str:
    """Unique event id (uuid4 hex — uniqueness matters, determinism does not)."""
    return uuid.uuid4().hex


def _to_float(raw: object) -> float | None:
    """float(raw) or None — never raises."""
    try:
        return float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


# --- Timestamp extraction (cheap; None on any malformation) -------------------
def _parse_nginx_ts(raw: str) -> float | None:
    """``08/Jul/2026:10:00:00.123 -0700`` -> epoch seconds."""
    m = _NGINX_TS_RE.match(raw)
    if m is None:
        return None
    day, mon, year, hh, mm, ss, frac, sign, oh, om = m.groups()
    month = _MONTH_NUM.get(mon.title())
    if month is None:
        return None
    micro = int((frac or "0").ljust(6, "0")[:6])
    offset = timedelta(hours=int(oh), minutes=int(om))
    if sign == "-":
        offset = -offset
    try:
        dt = datetime(int(year), month, int(day), int(hh), int(mm), int(ss), micro,
                      tzinfo=timezone(offset))
    except ValueError:
        return None
    return dt.timestamp()


def _parse_db_ts(raw: str, tz_abbr: str) -> float | None:
    """``2026-07-08 10:00:00.123`` + ``PDT`` -> epoch seconds."""
    fmt = "%Y-%m-%d %H:%M:%S.%f" if "." in raw else "%Y-%m-%d %H:%M:%S"
    try:
        naive = datetime.strptime(raw, fmt)
    except ValueError:
        return None
    hours = _TZ_ABBR_HOURS.get(tz_abbr, -7)
    return naive.replace(tzinfo=timezone(timedelta(hours=hours))).timestamp()


def _parse_iso_ts(raw: object) -> float | None:
    """ISO-8601 string -> epoch seconds; naive values assume the sim zone."""
    if not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SIM_TZ)
    return dt.timestamp()


# --- Per-source parsers --------------------------------------------------------
def _parse_web(line: str, ingested_at: float) -> LogEvent | None:
    """nginx combined format, optionally trailed by corr=/user=/latency_ms=."""
    m = _WEB_RE.match(line)
    if m is None:
        return None
    ts = _parse_nginx_ts(m["ts"])
    if ts is None:
        ts = ingested_at
    status = int(m["status"])
    path = m["path"]
    kvs = dict(_KV_RE.findall(m["trailer"]))

    # Level + error-code taxonomy: 5xx -> ERROR, 4xx -> WARN, and the cart
    # abandonment endpoint is a semantic WARN even though it returns 200.
    if path == "/api/cart/abandon":
        level: str = "WARN"
        error_code: str | None = CART_ABANDONED
    elif status >= 500:
        level, error_code = "ERROR", f"HTTP_{status}"
    elif status >= 400:
        level, error_code = "WARN", None
    else:
        level, error_code = "INFO", None

    metrics: dict[str, float] = {"status": float(status)}
    if m["bytes"] != "-":
        metrics["bytes"] = float(m["bytes"])
    latency = _to_float(kvs.get("latency_ms"))
    if latency is not None:
        metrics["latency_ms"] = latency

    return LogEvent(
        id=_new_event_id(),
        timestamp=ts,
        source=SourceType.WEB,
        service=SERVICE_BY_SOURCE[SourceType.WEB],
        level=level,
        message=f"{m['method']} {path} -> {status}",
        correlation_id=kvs.get("corr"),
        user_id=kvs.get("user"),
        error_code=error_code,
        metrics=metrics,
        raw=line,
    )


def _parse_db(line: str, ingested_at: float) -> LogEvent | None:
    """postgresql-style lines with an optional ``/* k=v ... */`` metadata comment."""
    m = _DB_RE.match(line)
    if m is None:
        return None
    ts = _parse_db_ts(m["ts"], m["tz"])
    if ts is None:
        ts = ingested_at
    sev = m["sev"]
    body = m["body"].strip()
    meta = dict(_KV_RE.findall(m["meta"])) if m["meta"] else {}

    metrics: dict[str, float] = {}
    pool = meta.get("pool")
    if pool is not None and "/" in pool:
        in_use_raw, _, size_raw = pool.partition("/")
        in_use, size = _to_float(in_use_raw), _to_float(size_raw)
        if in_use is not None and size is not None:
            metrics["pool_in_use"] = in_use
            metrics["pool_size"] = size

    # `LOG:  duration: X ms  statement: <sql>` lines yield latency + statement.
    message = body
    dm = _DB_DURATION_RE.match(body)
    if dm is not None:
        duration = _to_float(dm.group(1))
        if duration is not None:
            metrics["latency_ms"] = duration
        message = dm.group(2).strip()

    if sev == "FATAL" and "connection pool exhausted" in body:
        level: str = "ERROR"
        error_code: str | None = DB_POOL_EXHAUSTED
    elif sev in ("ERROR", "FATAL", "PANIC"):
        level, error_code = "ERROR", DB_QUERY_ERROR
    elif sev == "WARNING":
        level, error_code = "WARN", None
    else:  # LOG / INFO / DEBUG / NOTICE ...
        level, error_code = "INFO", None

    return LogEvent(
        id=_new_event_id(),
        timestamp=ts,
        source=SourceType.DATABASE,
        service=SERVICE_BY_SOURCE[SourceType.DATABASE],
        level=level,
        message=message,
        correlation_id=meta.get("corr"),
        user_id=meta.get("user"),
        error_code=error_code,
        metrics=metrics,
        raw=line,
    )


def _parse_api(line: str, ingested_at: float) -> LogEvent | None:
    """One JSON object per line, emitted by the api-service."""
    stripped = line.strip()
    if not stripped.startswith("{") or not stripped.endswith("}"):
        return None
    try:
        obj = json.loads(stripped)
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None

    ts = _parse_iso_ts(obj.get("ts"))
    if ts is None:
        ts = ingested_at
    # Level field is taken verbatim (upper-cased; WARNING normalized to WARN).
    level = str(obj.get("level") or "INFO").upper()
    level = {"WARNING": "WARN"}.get(level, level)

    status = _to_float(obj.get("status"))
    error_code = obj.get("error_code")
    if error_code is not None:
        error_code = str(error_code)
    elif status is not None and status >= 500:
        error_code = f"HTTP_{int(status)}"

    metrics: dict[str, float] = {}
    if status is not None:
        metrics["status"] = status
    latency = _to_float(obj.get("latency_ms"))
    if latency is not None:
        metrics["latency_ms"] = latency

    corr = obj.get("correlation_id")
    user = obj.get("user_id")
    return LogEvent(
        id=_new_event_id(),
        timestamp=ts,
        source=SourceType.API_SERVICE,
        service=str(obj.get("service") or SERVICE_BY_SOURCE[SourceType.API_SERVICE]),
        level=level,
        message=str(obj.get("message") or obj.get("endpoint") or "api event"),
        correlation_id=str(corr) if corr is not None else None,
        user_id=str(user) if user is not None else None,
        error_code=error_code,
        metrics=metrics,
        raw=line,
    )


def _parse_payment(line: str, ingested_at: float) -> LogEvent | None:
    """logfmt ``k=v`` payment lines; `ts` and `event` keys are mandatory."""
    kvs = dict(_KV_RE.findall(line))
    if "ts" not in kvs or "event" not in kvs:
        return None
    ts = _parse_iso_ts(kvs["ts"])
    if ts is None:
        ts = ingested_at

    status = kvs.get("status", "")
    if status == "timeout":
        level: str = "ERROR"
        error_code: str | None = PAYMENT_TIMEOUT
    elif status == "declined":
        level, error_code = "WARN", PAYMENT_DECLINED
    else:
        level = kvs.get("level", "INFO").upper()
        level = {"WARNING": "WARN"}.get(level, level)
        error_code = None

    metrics: dict[str, float] = {}
    for key in ("amount", "latency_ms"):
        val = _to_float(kvs.get(key))
        if val is not None:
            metrics[key] = val

    return LogEvent(
        id=_new_event_id(),
        timestamp=ts,
        source=SourceType.PAYMENT,
        service=SERVICE_BY_SOURCE[SourceType.PAYMENT],
        level=level,
        message=f"{kvs['event']} status={status or 'unknown'}",
        correlation_id=kvs.get("corr"),
        user_id=kvs.get("user"),
        error_code=error_code,
        metrics=metrics,
        raw=line,
    )


def _parse_inventory(line: str, ingested_at: float) -> LogEvent | None:
    """``[iso-ts] INVENTORY <op> sku=... qty=... status=... ...`` bracket lines."""
    m = _INV_RE.match(line)
    if m is None:
        return None
    ts = _parse_iso_ts(m["ts"])
    if ts is None:
        ts = ingested_at
    kvs = dict(_KV_RE.findall(m["rest"]))

    status = kvs.get("status", "ok")
    if status == "timeout":
        level: str = "ERROR"
        error_code: str | None = INVENTORY_TIMEOUT
    else:
        level, error_code = "INFO", None

    metrics: dict[str, float] = {}
    latency = _to_float(kvs.get("latency_ms"))
    if latency is not None:
        metrics["latency_ms"] = latency

    return LogEvent(
        id=_new_event_id(),
        timestamp=ts,
        source=SourceType.INVENTORY,
        service=SERVICE_BY_SOURCE[SourceType.INVENTORY],
        level=level,
        message=f"{m['op']} {kvs.get('sku', '?')} status={status}",
        correlation_id=kvs.get("corr"),
        user_id=kvs.get("user"),
        error_code=error_code,
        metrics=metrics,
        raw=line,
    )


#: Dispatch table — the collector looks parsers up by source.
PARSERS: dict[SourceType, Callable[[str, float], LogEvent | None]] = {
    SourceType.WEB: _parse_web,
    SourceType.DATABASE: _parse_db,
    SourceType.API_SERVICE: _parse_api,
    SourceType.PAYMENT: _parse_payment,
    SourceType.INVENTORY: _parse_inventory,
}


def parse_line(source: SourceType, line: str, ingested_at: float) -> LogEvent | None:
    """Parse one raw line from ``source`` into a LogEvent (None if unparseable).

    This is the pipeline's hot path (1000+ lines/sec bursts), so failure handling
    is a cheap None return; the blanket except is the never-raise safety net for
    input the per-parser guards did not anticipate.
    """
    parser = PARSERS.get(source)
    if parser is None or not line or line.isspace():
        return None
    try:
        return parser(line, ingested_at)
    except Exception:  # noqa: BLE001 — contract: never raise on hostile input
        return None
