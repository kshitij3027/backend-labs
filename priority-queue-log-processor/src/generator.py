"""Synthetic log message generator for testing and demonstration."""

import logging
import random
import threading
import time

from src.classifier import MessageClassifier
from src.config import Settings
from src.metrics import MetricsTracker
from src.models import LogMessage, Priority
from src.priority_queue import ThreadSafePriorityQueue

logger = logging.getLogger(__name__)


class SyntheticLogGenerator:
    """Generates realistic log messages with configurable distribution.

    Default distribution: ~5% CRITICAL, 15% HIGH, 30% MEDIUM, 50% LOW.
    Messages are created with priority LOW by default -- actual classification
    is performed by the MessageClassifier.
    """

    TEMPLATES: dict[str, list[str]] = {
        "CRITICAL": [
            "Payment processing failed for transaction TXN-{id}",
            "Security breach detected from IP {ip}",
            "System down: primary database unreachable",
            "Data corruption detected in shard-{id}",
            "Database connection failed on node db-{id}",
        ],
        "HIGH": [
            "High latency on /api/users: {n}ms response time",
            "Memory usage at {n}% on worker-{id}",
            "Connection timeout to redis-cluster-{id}",
            "Service unavailable: payment-gateway-{id}",
            "CPU usage exceeds threshold at {n}%",
        ],
        "MEDIUM": [
            "User error: invalid email format for user-{id}",
            "Validation failure on signup form field 'phone'",
            "Authentication failed for user admin-{id}",
            "Rate limit exceeded for client 10.0.0.{n}",
            "Login failed: incorrect password for user-{id}",
        ],
        "LOW": [
            "User {id} logged in successfully",
            "Health check passed for service-{id}",
            "Cache refreshed for module config-{id}",
            "Scheduled backup completed for db-{id}",
            "Session created for user-{id}",
        ],
    }

    # Weighted distribution: CRITICAL 5%, HIGH 15%, MEDIUM 30%, LOW 50%
    CATEGORIES = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    WEIGHTS = [5, 15, 30, 50]

    def _fill_template(self, template: str) -> str:
        """Replace template placeholders with random values."""
        result = template
        result = result.replace("{id}", str(random.randint(1000, 9999)))
        result = result.replace("{n}", str(random.randint(80, 999)))
        result = result.replace(
            "{ip}",
            f"{random.randint(1, 255)}.{random.randint(0, 255)}."
            f"{random.randint(0, 255)}.{random.randint(1, 254)}",
        )
        return result

    def generate(self, count: int = 1) -> list[LogMessage]:
        """Generate *count* log messages with realistic distribution.

        Messages are returned with priority set to LOW by default.
        Classification should be performed separately via MessageClassifier.
        """
        messages: list[LogMessage] = []
        categories = random.choices(self.CATEGORIES, weights=self.WEIGHTS, k=count)

        for category in categories:
            template = random.choice(self.TEMPLATES[category])
            text = self._fill_template(template)
            msg = LogMessage(
                source="generator",
                message=text,
                priority=Priority.LOW,
            )
            messages.append(msg)

        return messages

    def start_continuous(
        self,
        queue: ThreadSafePriorityQueue,
        classifier: MessageClassifier,
        metrics: MetricsTracker,
        rate: float,
        stop_event: threading.Event,
    ) -> None:
        """Continuously generate, classify, and enqueue messages.

        Runs at *rate* messages per second until *stop_event* is set.
        If rate is 0 or negative, returns immediately.
        """
        if rate <= 0:
            logger.info("[generator] Rate is 0 -- generation disabled")
            return

        interval = 1.0 / rate

        while not stop_event.is_set():
            msgs = self.generate(1)
            msg = classifier.classify_message(msgs[0])

            pushed = queue.push(msg)
            if pushed:
                metrics.record_enqueued(msg.priority)
            else:
                metrics.record_dropped(msg.priority)

            stop_event.wait(interval)

    def start(
        self,
        queue: ThreadSafePriorityQueue,
        classifier: MessageClassifier,
        metrics: MetricsTracker,
        settings: Settings,
        stop_event: threading.Event,
    ) -> threading.Thread:
        """Launch a daemon thread that generates messages continuously.

        Returns the thread handle.
        """
        t = threading.Thread(
            target=self.start_continuous,
            args=(queue, classifier, metrics, settings.generator_rate, stop_event),
            name="log-generator",
            daemon=True,
        )
        t.start()
        return t
