"""Unit tests for src.parsers — per-source round-trips, error taxonomy, garbage.

Expected epoch values are computed from the same fixed-offset instants embedded
in the hand-written lines, so the assertions are exact (to the millisecond the
formats carry) and independent of host/container timezone.
"""

from datetime import datetime, timedelta, timezone

import pytest

from src import models
from src.models import SourceType
from src.parsers import parse_line

#: The simulation's fixed zone (-07:00) — matches every hand-written line below.
TZ = timezone(timedelta(hours=-7))
#: Epoch for 2026-07-08T10:00:00.123-07:00, the instant most sample lines use.
TS_EPOCH = datetime(2026, 7, 8, 10, 0, 0, 123000, tzinfo=TZ).timestamp()

INGESTED_AT = 1_750_000_000.0

ALL_SOURCES = list(SourceType)


# --- WEB (nginx combined + trailer) -------------------------------------------
WEB_HAPPY = (
    '192.168.1.10 - - [08/Jul/2026:10:00:00.123 -0700] "GET /api/products HTTP/1.1" '
    '200 5321 "-" "Mozilla/5.0" corr=corr_ab12cd34 user=user_42 latency_ms=45.2'
)


def test_web_happy_path_round_trip():
    ev = parse_line(SourceType.WEB, WEB_HAPPY, INGESTED_AT)
    assert ev is not None
    assert ev.id
    assert ev.timestamp == pytest.approx(TS_EPOCH, abs=1e-3)
    assert ev.source is SourceType.WEB
    assert ev.service == "nginx"
    assert ev.level == "INFO"
    assert ev.message == "GET /api/products -> 200"
    assert ev.correlation_id == "corr_ab12cd34"
    assert ev.user_id == "user_42"
    assert ev.error_code is None
    assert ev.metrics["status"] == 200.0
    assert ev.metrics["bytes"] == 5321.0
    assert ev.metrics["latency_ms"] == pytest.approx(45.2)
    assert ev.raw == WEB_HAPPY


@pytest.mark.parametrize(
    ("status", "expected_code"),
    [(500, models.HTTP_500), (502, models.HTTP_502), (503, models.HTTP_503)],
)
def test_web_5xx_maps_to_error_code(status, expected_code):
    line = (
        f'192.168.1.11 - - [08/Jul/2026:10:00:01.000 -0700] '
        f'"POST /api/checkout/complete HTTP/1.1" {status} 210 "-" "Mozilla/5.0" '
        f'corr=corr_ab12cd34 user=user_42 latency_ms=812.0'
    )
    ev = parse_line(SourceType.WEB, line, INGESTED_AT)
    assert ev is not None
    assert ev.level == "ERROR"
    assert ev.error_code == expected_code
    assert ev.metrics["status"] == float(status)


def test_web_4xx_is_warn_without_error_code():
    line = (
        '192.168.1.12 - - [08/Jul/2026:10:00:01.000 -0700] '
        '"GET /api/products/9 HTTP/1.1" 404 120 "-" "Mozilla/5.0" '
        'corr=corr_ab12cd34 user=user_42 latency_ms=8.1'
    )
    ev = parse_line(SourceType.WEB, line, INGESTED_AT)
    assert ev is not None
    assert ev.level == "WARN"
    assert ev.error_code is None


def test_web_cart_abandon_is_semantic_warn():
    line = (
        '192.168.1.13 - - [08/Jul/2026:10:00:02.500 -0700] '
        '"POST /api/cart/abandon HTTP/1.1" 200 150 "-" "Mozilla/5.0" '
        'corr=corr_ab12cd34 user=user_42 latency_ms=9.4'
    )
    ev = parse_line(SourceType.WEB, line, INGESTED_AT)
    assert ev is not None
    assert ev.level == "WARN"
    assert ev.error_code == models.CART_ABANDONED
    assert ev.metrics["status"] == 200.0


def test_web_healthcheck_without_trailer_has_no_ids():
    line = (
        '10.0.0.5 - - [08/Jul/2026:10:00:00.123 -0700] '
        '"GET /health HTTP/1.1" 200 15 "-" "kube-probe/1.29"'
    )
    ev = parse_line(SourceType.WEB, line, INGESTED_AT)
    assert ev is not None
    assert ev.correlation_id is None
    assert ev.user_id is None
    assert "latency_ms" not in ev.metrics
    assert ev.metrics["status"] == 200.0


def test_web_bad_timestamp_falls_back_to_ingested_at():
    line = (
        '10.0.0.5 - - [08/Xxx/2026:10:00:00.123 -0700] '
        '"GET /health HTTP/1.1" 200 15 "-" "kube-probe/1.29"'
    )
    ev = parse_line(SourceType.WEB, line, 55.5)
    assert ev is not None
    assert ev.timestamp == 55.5


# --- DATABASE (postgresql) -----------------------------------------------------
DB_HAPPY = (
    "2026-07-08 10:00:00.123 PDT [8341] LOG:  duration: 5.234 ms  "
    "statement: SELECT * FROM orders WHERE user_id=42 "
    "/* corr=corr_ab12cd34 user=user_42 pool=3/20 */"
)


def test_db_happy_path_round_trip():
    ev = parse_line(SourceType.DATABASE, DB_HAPPY, INGESTED_AT)
    assert ev is not None
    assert ev.timestamp == pytest.approx(TS_EPOCH, abs=1e-3)
    assert ev.source is SourceType.DATABASE
    assert ev.service == "postgresql"
    assert ev.level == "INFO"
    assert ev.message == "SELECT * FROM orders WHERE user_id=42"
    assert ev.correlation_id == "corr_ab12cd34"
    assert ev.user_id == "user_42"
    assert ev.error_code is None
    assert ev.metrics["latency_ms"] == pytest.approx(5.234)
    assert ev.metrics["pool_in_use"] == 3.0
    assert ev.metrics["pool_size"] == 20.0
    assert ev.raw == DB_HAPPY


def test_db_fatal_pool_exhausted():
    line = "2026-07-08 10:00:01.500 PDT [8341] FATAL:  connection pool exhausted /* pool=20/20 */"
    ev = parse_line(SourceType.DATABASE, line, INGESTED_AT)
    assert ev is not None
    assert ev.level == "ERROR"
    assert ev.error_code == models.DB_POOL_EXHAUSTED
    assert ev.metrics["pool_in_use"] == 20.0
    assert ev.metrics["pool_size"] == 20.0
    assert ev.correlation_id is None
    assert "connection pool exhausted" in ev.message


def test_db_error_maps_to_query_error():
    line = (
        "2026-07-08 10:00:02.000 PDT [8355] ERROR:  deadlock detected "
        "/* corr=corr_ab12cd34 user=user_42 pool=4/20 */"
    )
    ev = parse_line(SourceType.DATABASE, line, INGESTED_AT)
    assert ev is not None
    assert ev.level == "ERROR"
    assert ev.error_code == models.DB_QUERY_ERROR
    assert ev.correlation_id == "corr_ab12cd34"
    assert ev.metrics["pool_in_use"] == 4.0


# --- API_SERVICE (JSON) ----------------------------------------------------------
API_HAPPY = (
    '{"ts": "2026-07-08T10:00:00.123-07:00", "level": "INFO", "service": "api-service", '
    '"message": "checkout step completed", "correlation_id": "corr_ab12cd34", '
    '"user_id": "user_42", "endpoint": "/checkout/validate", "status": 200, '
    '"latency_ms": 33.1}'
)


def test_api_happy_path_round_trip():
    ev = parse_line(SourceType.API_SERVICE, API_HAPPY, INGESTED_AT)
    assert ev is not None
    assert ev.timestamp == pytest.approx(TS_EPOCH, abs=1e-3)
    assert ev.source is SourceType.API_SERVICE
    assert ev.service == "api-service"
    assert ev.level == "INFO"
    assert ev.message == "checkout step completed"
    assert ev.correlation_id == "corr_ab12cd34"
    assert ev.user_id == "user_42"
    assert ev.error_code is None
    assert ev.metrics["status"] == 200.0
    assert ev.metrics["latency_ms"] == pytest.approx(33.1)
    assert ev.raw == API_HAPPY


def test_api_checkout_failed_error_code_field_wins():
    line = (
        '{"ts": "2026-07-08T10:00:02.700-07:00", "level": "ERROR", "service": "api-service", '
        '"message": "checkout failed: inventory reservation timed out", '
        '"correlation_id": "corr_ab12cd34", "user_id": "user_42", '
        '"endpoint": "/checkout/complete", "status": 500, "latency_ms": 1201.0, '
        '"error_code": "CHECKOUT_FAILED"}'
    )
    ev = parse_line(SourceType.API_SERVICE, line, INGESTED_AT)
    assert ev is not None
    assert ev.level == "ERROR"
    assert ev.error_code == models.CHECKOUT_FAILED
    assert ev.metrics["status"] == 500.0


def test_api_5xx_without_error_code_field_derives_http_code():
    line = (
        '{"ts": "2026-07-08T10:00:03.000-07:00", "level": "ERROR", "service": "api-service", '
        '"message": "upstream exploded", "endpoint": "/checkout/validate", '
        '"status": 500, "latency_ms": 88.0}'
    )
    ev = parse_line(SourceType.API_SERVICE, line, INGESTED_AT)
    assert ev is not None
    assert ev.error_code == models.HTTP_500
    assert ev.correlation_id is None


# --- PAYMENT (logfmt) --------------------------------------------------------------
PAYMENT_HAPPY = (
    "ts=2026-07-08T10:00:00.123-07:00 level=INFO event=payment_processed "
    "corr=corr_ab12cd34 user=user_42 amount=49.99 latency_ms=231.4 status=success"
)


def test_payment_happy_path_round_trip():
    ev = parse_line(SourceType.PAYMENT, PAYMENT_HAPPY, INGESTED_AT)
    assert ev is not None
    assert ev.timestamp == pytest.approx(TS_EPOCH, abs=1e-3)
    assert ev.source is SourceType.PAYMENT
    assert ev.service == "payment-service"
    assert ev.level == "INFO"
    assert ev.message == "payment_processed status=success"
    assert ev.correlation_id == "corr_ab12cd34"
    assert ev.user_id == "user_42"
    assert ev.error_code is None
    assert ev.metrics["amount"] == pytest.approx(49.99)
    assert ev.metrics["latency_ms"] == pytest.approx(231.4)
    assert ev.raw == PAYMENT_HAPPY


def test_payment_timeout():
    line = (
        "ts=2026-07-08T10:00:05.000-07:00 level=ERROR event=payment_processed "
        "corr=corr_ab12cd34 user=user_42 amount=12.50 latency_ms=2100.0 status=timeout"
    )
    ev = parse_line(SourceType.PAYMENT, line, INGESTED_AT)
    assert ev is not None
    assert ev.level == "ERROR"
    assert ev.error_code == models.PAYMENT_TIMEOUT
    assert ev.metrics["latency_ms"] == pytest.approx(2100.0)


def test_payment_declined():
    line = (
        "ts=2026-07-08T10:00:06.000-07:00 level=WARN event=payment_processed "
        "corr=corr_ab12cd34 user=user_42 amount=99.00 latency_ms=250.0 status=declined"
    )
    ev = parse_line(SourceType.PAYMENT, line, INGESTED_AT)
    assert ev is not None
    assert ev.level == "WARN"
    assert ev.error_code == models.PAYMENT_DECLINED


# --- INVENTORY (bracket) --------------------------------------------------------------
INVENTORY_HAPPY = (
    "[2026-07-08T10:00:00.123-07:00] INVENTORY reserve sku=SKU-1042 qty=1 "
    "status=ok corr=corr_ab12cd34 user=user_42 latency_ms=12.5"
)


def test_inventory_happy_path_round_trip():
    ev = parse_line(SourceType.INVENTORY, INVENTORY_HAPPY, INGESTED_AT)
    assert ev is not None
    assert ev.timestamp == pytest.approx(TS_EPOCH, abs=1e-3)
    assert ev.source is SourceType.INVENTORY
    assert ev.service == "inventory-service"
    assert ev.level == "INFO"
    assert ev.message == "reserve SKU-1042 status=ok"
    assert ev.correlation_id == "corr_ab12cd34"
    assert ev.user_id == "user_42"
    assert ev.error_code is None
    assert ev.metrics["latency_ms"] == pytest.approx(12.5)
    assert ev.raw == INVENTORY_HAPPY


def test_inventory_timeout():
    line = (
        "[2026-07-08T10:00:07.000-07:00] INVENTORY reserve sku=SKU-9 qty=2 "
        "status=timeout corr=corr_ab12cd34 user=user_42 latency_ms=180.0"
    )
    ev = parse_line(SourceType.INVENTORY, line, INGESTED_AT)
    assert ev is not None
    assert ev.level == "ERROR"
    assert ev.error_code == models.INVENTORY_TIMEOUT
    assert ev.metrics["latency_ms"] == pytest.approx(180.0)


# --- Garbage / never-raise contract -----------------------------------------------------
@pytest.mark.parametrize("source", ALL_SOURCES)
def test_garbage_returns_none_and_never_raises(source):
    assert parse_line(source, "%%% not a log", 123.0) is None


@pytest.mark.parametrize("source", ALL_SOURCES)
def test_empty_line_returns_none(source):
    assert parse_line(source, "", 123.0) is None


@pytest.mark.parametrize("source", ALL_SOURCES)
def test_whitespace_line_returns_none(source):
    assert parse_line(source, "   \t  ", 123.0) is None
