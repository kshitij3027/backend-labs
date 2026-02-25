"""Thread-safe metrics tracking for validation and registration."""
import threading
from datetime import datetime, timezone


class MetricsTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._total_validations = 0
        self._successful_validations = 0
        self._failed_validations = 0
        self._total_registrations = 0
        self._per_subject = {}  # subject -> {validations, successes, failures}
        self._started_at = datetime.now(timezone.utc).isoformat()

    def record_validation(self, subject, valid):
        with self._lock:
            self._total_validations += 1
            if valid:
                self._successful_validations += 1
            else:
                self._failed_validations += 1

            if subject not in self._per_subject:
                self._per_subject[subject] = {"validations": 0, "successes": 0, "failures": 0}
            self._per_subject[subject]["validations"] += 1
            if valid:
                self._per_subject[subject]["successes"] += 1
            else:
                self._per_subject[subject]["failures"] += 1

    def record_registration(self):
        with self._lock:
            self._total_registrations += 1

    def get_metrics(self):
        with self._lock:
            success_rate = 0.0
            if self._total_validations > 0:
                success_rate = round(self._successful_validations / self._total_validations * 100, 2)
            return {
                "total_validations": self._total_validations,
                "successful_validations": self._successful_validations,
                "failed_validations": self._failed_validations,
                "success_rate": success_rate,
                "total_registrations": self._total_registrations,
                "per_subject": dict(self._per_subject),
                "started_at": self._started_at,
            }
