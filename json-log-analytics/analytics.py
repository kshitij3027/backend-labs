from collections import defaultdict, deque
from threading import Lock
from datetime import datetime, timezone


class AnalyticsEngine:
    def __init__(self, max_buckets=60):
        self._lock = Lock()
        self._max_buckets = max_buckets
        self._buckets = defaultdict(lambda: {
            "total": 0,
            "errors": 0,
            "services": defaultdict(int),
            "processing_times": [],
            "users": defaultdict(int),
        })
        self._bucket_order = deque()
        self._service_last_seen = {}

    def _bucket_key(self, timestamp):
        return datetime.fromisoformat(timestamp).strftime("%Y-%m-%dT%H:%M")

    def record(self, log_entry):
        timestamp = log_entry["timestamp"]
        level = log_entry.get("level", "INFO")
        service = log_entry.get("service", "unknown")
        key = self._bucket_key(timestamp)

        with self._lock:
            if key not in self._buckets:
                self._bucket_order.append(key)
                # Access the key to create the default entry
                _ = self._buckets[key]
                while len(self._bucket_order) > self._max_buckets:
                    old_key = self._bucket_order.popleft()
                    del self._buckets[old_key]

            bucket = self._buckets[key]
            bucket["total"] += 1
            bucket["services"][service] += 1

            if level in ("ERROR", "CRITICAL"):
                bucket["errors"] += 1

            metadata = log_entry.get("metadata", {})
            if metadata and "processing_time_ms" in metadata:
                bucket["processing_times"].append(metadata["processing_time_ms"])

            user_id = log_entry.get("user_id")
            if user_id:
                bucket["users"][user_id] += 1

            self._service_last_seen[service] = datetime.fromisoformat(timestamp)

    def get_time_series(self, minutes=10):
        with self._lock:
            keys = list(self._bucket_order)[-minutes:]
            return [
                {"time": k, "total": self._buckets[k]["total"], "errors": self._buckets[k]["errors"]}
                for k in keys
            ]

    def get_service_stats(self):
        with self._lock:
            totals = defaultdict(int)
            for key in self._bucket_order:
                bucket = self._buckets[key]
                for service, count in bucket["services"].items():
                    totals[service] += count
            return {service: {"total": count} for service, count in totals.items()}

    def get_error_rate(self, minutes=5):
        with self._lock:
            keys = list(self._bucket_order)[-minutes:]
            total_logs = sum(self._buckets[k]["total"] for k in keys)
            total_errors = sum(self._buckets[k]["errors"] for k in keys)
            if total_logs == 0:
                return 0.0
            return total_errors / total_logs

    def get_error_trends(self):
        with self._lock:
            result = []
            for key in self._bucket_order:
                bucket = self._buckets[key]
                total = bucket["total"]
                error_rate = bucket["errors"] / total if total > 0 else 0.0
                result.append({"time": key, "error_rate": error_rate})
            return result

    def get_service_health(self):
        now = datetime.now(timezone.utc)
        with self._lock:
            # Aggregate log counts per service
            service_counts = defaultdict(int)
            for key in self._bucket_order:
                bucket = self._buckets[key]
                for service, count in bucket["services"].items():
                    service_counts[service] += count

            result = {}
            for service, last_seen in self._service_last_seen.items():
                # Make last_seen offset-aware if it is naive
                if last_seen.tzinfo is None:
                    last_seen_aware = last_seen.replace(tzinfo=timezone.utc)
                else:
                    last_seen_aware = last_seen
                delta = (now - last_seen_aware).total_seconds()
                if delta > 120:
                    status = "down"
                elif delta > 60:
                    status = "warning"
                else:
                    status = "healthy"
                result[service] = {
                    "status": status,
                    "last_seen": last_seen.isoformat(),
                    "log_count": service_counts.get(service, 0),
                }
            return result

    def get_most_active_services(self, limit=5):
        with self._lock:
            totals = defaultdict(int)
            for key in self._bucket_order:
                bucket = self._buckets[key]
                for service, count in bucket["services"].items():
                    totals[service] += count
            sorted_services = sorted(totals.items(), key=lambda x: x[1], reverse=True)
            return [{"service": name, "count": count} for name, count in sorted_services[:limit]]

    def get_user_activity(self, limit=10):
        with self._lock:
            totals = defaultdict(int)
            for key in self._bucket_order:
                bucket = self._buckets[key]
                for user_id, count in bucket["users"].items():
                    totals[user_id] += count
            sorted_users = sorted(totals.items(), key=lambda x: x[1], reverse=True)
            return [{"user_id": uid, "count": count} for uid, count in sorted_users[:limit]]

    def get_summary(self):
        with self._lock:
            total_logs = sum(self._buckets[k]["total"] for k in self._bucket_order)
            total_errors = sum(self._buckets[k]["errors"] for k in self._bucket_order)
            error_rate = total_errors / total_logs if total_logs > 0 else 0.0

            all_services = set()
            for key in self._bucket_order:
                all_services.update(self._buckets[key]["services"].keys())

            if self._bucket_order:
                time_range = {
                    "first": self._bucket_order[0],
                    "last": self._bucket_order[-1],
                }
            else:
                time_range = None

            return {
                "total_logs": total_logs,
                "total_errors": total_errors,
                "error_rate": error_rate,
                "active_services": len(all_services),
                "time_range": time_range,
            }
