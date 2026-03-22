"""Thread-safe metrics store backed by a bounded deque."""

import threading
import time
from collections import deque


class MetricsStore:
    """Stores streaming events and provides windowed aggregations."""

    def __init__(self, max_length=1000):
        self._lock = threading.Lock()
        self._events = deque(maxlen=max_length)
        self._max_length = max_length

    def add_event(self, topic, data):
        """Add an event from a specific topic. data must be a dict."""
        event = {
            "topic": topic,
            "timestamp": data.get("timestamp", time.time()),
            "data": data,
        }
        with self._lock:
            self._events.append(event)

    def get_windowed_metrics(self, window_seconds=60):
        """Return aggregated metrics for events within the time window."""
        now = time.time()
        cutoff = now - window_seconds

        with self._lock:
            window_events = [e for e in self._events if e["timestamp"] >= cutoff]

        total = len(window_events)
        per_topic = {}
        error_count = 0
        response_times = []

        for event in window_events:
            topic = event["topic"]
            per_topic[topic] = per_topic.get(topic, 0) + 1

            if topic == "error-events":
                error_count += 1

            rt = event["data"].get("response_time")
            if rt is not None:
                response_times.append(float(rt))

        error_rate = (error_count / total * 100) if total > 0 else 0.0
        avg_rt = sum(response_times) / len(response_times) if response_times else 0.0

        sorted_rt = sorted(response_times)
        p95_rt = (
            sorted_rt[int(len(sorted_rt) * 0.95)]
            if len(sorted_rt) >= 20
            else (sorted_rt[-1] if sorted_rt else 0.0)
        )

        events_per_sec = total / window_seconds if window_seconds > 0 else 0.0

        return {
            "total_events": total,
            "per_topic_counts": per_topic,
            "error_rate": round(error_rate, 2),
            "avg_response_time": round(avg_rt, 2),
            "p95_response_time": round(p95_rt, 2),
            "events_per_second": round(events_per_sec, 2),
            "window_seconds": window_seconds,
            "timestamp": now,
        }

    def get_historical(self, points=30, bucket_seconds=10):
        """Return time-bucketed data for Chart.js charts."""
        now = time.time()

        with self._lock:
            all_events = list(self._events)

        buckets = []
        for i in range(points):
            bucket_end = now - (i * bucket_seconds)
            bucket_start = bucket_end - bucket_seconds
            bucket_events = [
                e
                for e in all_events
                if bucket_start <= e["timestamp"] < bucket_end
            ]

            error_count = sum(
                1 for e in bucket_events if e["topic"] == "error-events"
            )
            total = len(bucket_events)
            response_times = [
                e["data"].get("response_time", 0)
                for e in bucket_events
                if e["data"].get("response_time") is not None
            ]

            buckets.append(
                {
                    "timestamp": bucket_end,
                    "events": total,
                    "error_rate": round(
                        (error_count / total * 100) if total > 0 else 0, 2
                    ),
                    "avg_response_time": round(
                        sum(response_times) / len(response_times)
                        if response_times
                        else 0,
                        2,
                    ),
                }
            )

        buckets.reverse()  # oldest first for Chart.js

        return {
            "labels": [b["timestamp"] for b in buckets],
            "events": [b["events"] for b in buckets],
            "error_rate": [b["error_rate"] for b in buckets],
            "response_times": [b["avg_response_time"] for b in buckets],
        }
