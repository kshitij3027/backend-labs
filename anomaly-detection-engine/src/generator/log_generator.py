"""Synthetic log generator with configurable anomaly injection."""
from __future__ import annotations

import random
from datetime import datetime, timezone
from enum import Enum
from typing import Generator

from src.models import LogEntry


class AnomalyType(Enum):
    """Types of anomalies that can be injected into generated logs."""

    SLOW_RESPONSE = "slow_response"
    UNUSUAL_PAYLOAD = "unusual_payload"
    SUSPICIOUS_AGENT = "suspicious_agent"
    BAD_STATUS = "bad_status"


class LogGenerator:
    """Generates synthetic HTTP log entries with configurable anomaly injection.

    Uses a seeded random.Random instance for full reproducibility.
    """

    NORMAL_USER_AGENTS: list[str] = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 OPR/106.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Vivaldi/6.5",
    ]

    SUSPICIOUS_USER_AGENTS: list[str] = [
        "bot",
        "curl/7.68.0",
        "python-requests/2.28.1",
        "Go-http-client/1.1",
        "sqlmap/1.7",
        "Nikto/2.1.6",
        "masscan/1.3",
        "a]b$c%d^e&f*g(h)i+j=k{l}m|n;o:p'q<r>s,t.u/v?w!x@y#z" * 5,
        "x",
        "",
    ]

    NORMAL_PATHS: list[str] = [
        "/api/users",
        "/api/users/profile",
        "/api/data",
        "/api/data/export",
        "/api/orders",
        "/api/orders/history",
        "/api/products",
        "/api/products/search",
        "/api/auth/login",
        "/api/auth/logout",
        "/api/auth/refresh",
        "/dashboard",
        "/dashboard/analytics",
        "/dashboard/settings",
        "/login",
        "/register",
        "/health",
        "/api/notifications",
        "/api/reports",
        "/static/css/main.css",
        "/static/js/app.js",
        "/static/images/logo.png",
        "/api/inventory",
        "/api/payments",
    ]

    IP_SUBNETS: list[str] = [
        "192.168.1",
        "192.168.2",
        "10.0.0",
        "10.0.1",
        "10.1.0",
        "172.16.0",
        "172.16.1",
        "172.17.0",
    ]

    # Weighted status codes for normal traffic
    _NORMAL_STATUS_CODES: list[int] = [200] * 70 + [201] * 10 + [301] * 5 + [304] * 5 + [404] * 8 + [500] * 2
    _NORMAL_METHODS: list[str] = ["GET"] * 70 + ["POST"] * 20 + ["PUT"] * 5 + ["DELETE"] * 5
    _BAD_STATUS_CODES: list[int] = [500, 502, 503, 429]

    def __init__(self, anomaly_rate: float = 0.05, seed: int = 42) -> None:
        self.anomaly_rate = anomaly_rate
        self.seed = seed
        self._rng = random.Random(seed)

    def _clamp(self, value: float, lo: float, hi: float) -> float:
        """Clamp a value between lo and hi."""
        return max(lo, min(hi, value))

    def _random_ip(self) -> str:
        """Generate a random private IP address."""
        subnet = self._rng.choice(self.IP_SUBNETS)
        return f"{subnet}.{self._rng.randint(1, 254)}"

    def _generate_normal(self) -> LogEntry:
        """Generate a normal (non-anomalous) log entry."""
        response_time = self._clamp(self._rng.gauss(200, 50), 50, 500)
        bytes_sent = int(self._clamp(self._rng.gauss(5000, 2000), 100, 15000))
        session_duration = self._clamp(self._rng.gauss(300, 100), 30, 900)
        page_views = int(self._clamp(self._rng.gauss(5, 2), 1, 20))

        return LogEntry(
            timestamp=datetime.now(timezone.utc),
            ip=self._random_ip(),
            method=self._rng.choice(self._NORMAL_METHODS),
            path=self._rng.choice(self.NORMAL_PATHS),
            status_code=self._rng.choice(self._NORMAL_STATUS_CODES),
            response_time=round(response_time, 2),
            bytes_sent=bytes_sent,
            user_agent=self._rng.choice(self.NORMAL_USER_AGENTS),
            session_duration=round(session_duration, 2),
            page_views=page_views,
            _is_anomaly=False,
            _anomaly_type="",
        )

    def _generate_anomalous(self) -> LogEntry:
        """Generate an anomalous log entry with a randomly chosen anomaly type."""
        anomaly_type = self._rng.choice(list(AnomalyType))
        entry = self._generate_normal()

        if anomaly_type == AnomalyType.SLOW_RESPONSE:
            response_time = round(self._rng.uniform(3000, 15000), 2)
            return LogEntry(
                timestamp=entry.timestamp,
                ip=entry.ip,
                method=entry.method,
                path=entry.path,
                status_code=entry.status_code,
                response_time=response_time,
                bytes_sent=entry.bytes_sent,
                user_agent=entry.user_agent,
                session_duration=entry.session_duration,
                page_views=entry.page_views,
                _is_anomaly=True,
                _anomaly_type=anomaly_type.value,
            )

        if anomaly_type == AnomalyType.UNUSUAL_PAYLOAD:
            bytes_sent = self._rng.randint(50000, 200000)
            return LogEntry(
                timestamp=entry.timestamp,
                ip=entry.ip,
                method=entry.method,
                path=entry.path,
                status_code=entry.status_code,
                response_time=entry.response_time,
                bytes_sent=bytes_sent,
                user_agent=entry.user_agent,
                session_duration=entry.session_duration,
                page_views=entry.page_views,
                _is_anomaly=True,
                _anomaly_type=anomaly_type.value,
            )

        if anomaly_type == AnomalyType.SUSPICIOUS_AGENT:
            user_agent = self._rng.choice(self.SUSPICIOUS_USER_AGENTS)
            return LogEntry(
                timestamp=entry.timestamp,
                ip=entry.ip,
                method=entry.method,
                path=entry.path,
                status_code=entry.status_code,
                response_time=entry.response_time,
                bytes_sent=entry.bytes_sent,
                user_agent=user_agent,
                session_duration=entry.session_duration,
                page_views=entry.page_views,
                _is_anomaly=True,
                _anomaly_type=anomaly_type.value,
            )

        # BAD_STATUS
        status_code = self._rng.choice(self._BAD_STATUS_CODES)
        # Slightly abnormal response time for bad status entries
        response_time = round(self._clamp(self._rng.gauss(800, 300), 200, 2000), 2)
        return LogEntry(
            timestamp=entry.timestamp,
            ip=entry.ip,
            method=entry.method,
            path=entry.path,
            status_code=status_code,
            response_time=response_time,
            bytes_sent=entry.bytes_sent,
            user_agent=entry.user_agent,
            session_duration=entry.session_duration,
            page_views=entry.page_views,
            _is_anomaly=True,
            _anomaly_type=anomaly_type.value,
        )

    def generate(self) -> LogEntry:
        """Produce a single log entry, possibly anomalous based on anomaly_rate."""
        if self._rng.random() < self.anomaly_rate:
            return self._generate_anomalous()
        return self._generate_normal()

    def generate_batch(self, n: int) -> list[LogEntry]:
        """Generate a batch of n log entries."""
        return [self.generate() for _ in range(n)]

    def generate_stream(self, count: int) -> Generator[LogEntry, None, None]:
        """Yield log entries one at a time."""
        for _ in range(count):
            yield self.generate()
