import time


class Metrics:
    def __init__(self):
        self.latencies: list[float] = []  # milliseconds
        self.total_sent: int = 0
        self.total_success: int = 0
        self.total_errors: int = 0
        self.total_bytes: int = 0
        self.start_time: float = 0.0
        self.end_time: float = 0.0

    def start(self):
        self.start_time = time.monotonic()

    def stop(self):
        self.end_time = time.monotonic()

    def record(self, latency_ms: float, success: bool, bytes_sent: int = 0):
        self.latencies.append(latency_ms)
        self.total_sent += 1
        self.total_bytes += bytes_sent
        if success:
            self.total_success += 1
        else:
            self.total_errors += 1

    def summary(self) -> dict:
        duration = max(
            self.end_time - self.start_time, 0.001
        )

        if not self.latencies:
            return {
                "total_sent": 0,
                "total_success": 0,
                "total_errors": 0,
                "error_rate": 0.0,
                "actual_rps": 0.0,
                "duration_secs": duration,
                "latency_avg_ms": 0.0,
                "latency_min_ms": 0.0,
                "latency_max_ms": 0.0,
                "latency_p50_ms": 0.0,
                "latency_p95_ms": 0.0,
                "latency_p99_ms": 0.0,
                "total_bytes": 0,
            }

        sorted_lat = sorted(self.latencies)

        def percentile(data, p):
            k = (len(data) - 1) * (p / 100.0)
            f = int(k)
            c = f + 1
            if c >= len(data):
                return data[-1]
            return data[f] + (k - f) * (data[c] - data[f])

        return {
            "total_sent": self.total_sent,
            "total_success": self.total_success,
            "total_errors": self.total_errors,
            "error_rate": self.total_errors / max(self.total_sent, 1),
            "actual_rps": self.total_sent / duration,
            "duration_secs": round(duration, 3),
            "latency_avg_ms": round(
                sum(self.latencies) / len(self.latencies), 3
            ),
            "latency_min_ms": round(min(self.latencies), 3),
            "latency_max_ms": round(max(self.latencies), 3),
            "latency_p50_ms": round(percentile(sorted_lat, 50), 3),
            "latency_p95_ms": round(percentile(sorted_lat, 95), 3),
            "latency_p99_ms": round(percentile(sorted_lat, 99), 3),
            "total_bytes": self.total_bytes,
        }
