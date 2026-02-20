import json
import os
import time
from datetime import datetime, timezone

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


class ResourceMonitor:
    """Samples CPU and memory usage at regular intervals."""

    def __init__(self, interval: float = 1.0):
        self.interval = interval
        self.cpu_samples: list[float] = []
        self.memory_samples: list[float] = []  # in MB
        self._running = False

    def sample(self):
        """Take a single resource sample."""
        if HAS_PSUTIL:
            self.cpu_samples.append(psutil.cpu_percent(interval=None))
            mem = psutil.virtual_memory()
            self.memory_samples.append(mem.used / (1024 * 1024))
        else:
            self.cpu_samples.append(0.0)
            self.memory_samples.append(0.0)

    def summary(self) -> dict:
        if not self.cpu_samples:
            return {
                "avg_cpu_percent": 0,
                "peak_cpu_percent": 0,
                "avg_memory_mb": 0,
                "peak_memory_mb": 0,
            }
        return {
            "avg_cpu_percent": round(
                sum(self.cpu_samples) / len(self.cpu_samples), 1
            ),
            "peak_cpu_percent": round(max(self.cpu_samples), 1),
            "avg_memory_mb": round(
                sum(self.memory_samples) / len(self.memory_samples), 1
            ),
            "peak_memory_mb": round(max(self.memory_samples), 1),
        }


class BenchmarkReporter:
    """Generates JSON benchmark reports."""

    def __init__(self, output_dir: str = "."):
        self.output_dir = output_dir
        self.tests: list[dict] = []

    def add_test(self, name: str, config: dict, results: dict, resources: dict):
        self.tests.append({
            "name": name,
            "config": config,
            "results": results,
            "resources": resources,
        })

    def generate_report(
        self, target_rps: int = 1000, target_error_rate: float = 0.01
    ) -> str:
        """Generate and save the benchmark report. Returns the file path."""
        # Find the best RPS achieved
        best_rps = max(
            (t["results"].get("actual_rps", 0) for t in self.tests), default=0
        )
        worst_error_rate = max(
            (t["results"].get("error_rate", 0) for t in self.tests), default=0
        )

        system_info = {"cpu_count": 0, "memory_mb": 0}
        if HAS_PSUTIL:
            system_info = {
                "cpu_count": psutil.cpu_count(),
                "memory_mb": round(psutil.virtual_memory().total / (1024 * 1024)),
            }

        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "system": system_info,
            "tests": self.tests,
            "verification": {
                "target_rps": target_rps,
                "achieved": best_rps >= target_rps,
                "best_rps": round(best_rps),
                "target_error_rate": target_error_rate,
                "achieved_error_rate": round(worst_error_rate, 4),
                "pass": best_rps >= target_rps and worst_error_rate <= target_error_rate,
            },
        }

        os.makedirs(self.output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_report_{timestamp}.json"
        filepath = os.path.join(self.output_dir, filename)

        with open(filepath, "w") as f:
            json.dump(report, f, indent=2)

        print(f"Benchmark report saved to: {filepath}")
        return filepath
