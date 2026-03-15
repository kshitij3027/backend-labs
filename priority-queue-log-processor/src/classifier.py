"""Log message classifier that assigns priority based on content patterns."""

import re

from src.models import LogMessage, Priority


class MessageClassifier:
    """Classifies log messages into priority levels using regex patterns."""

    def __init__(self) -> None:
        self._patterns: list[tuple[re.Pattern, Priority]] = []

        # CRITICAL patterns
        critical_patterns = [
            r"payment.*(fail|error|decline)",
            r"security.*(breach|violation|intrusion)",
            r"system.*(down|crash|unresponsive)",
            r"data.*(loss|corrupt)",
            r"database.*(fail|crash|corrupt)",
        ]
        for pattern in critical_patterns:
            self._patterns.append((re.compile(pattern, re.IGNORECASE), Priority.CRITICAL))

        # HIGH patterns
        high_patterns = [
            r"(high|elevated).*(latency|response.time)",
            r"(memory|cpu|disk).*(high|full|exceed|threshold)",
            r"(timeout|connection.refused)",
            r"(service|node).*(unavailable|degraded)",
        ]
        for pattern in high_patterns:
            self._patterns.append((re.compile(pattern, re.IGNORECASE), Priority.HIGH))

        # MEDIUM patterns
        medium_patterns = [
            r"user.*(error|invalid|denied)",
            r"validation.*(fail|error)",
            r"(auth|login).*(fail|invalid)",
            r"(rate.limit|throttl)",
        ]
        for pattern in medium_patterns:
            self._patterns.append((re.compile(pattern, re.IGNORECASE), Priority.MEDIUM))

    def classify(self, message: str) -> Priority:
        """Classify a raw message string into a priority level.

        Iterates through patterns in order (CRITICAL, HIGH, MEDIUM).
        Returns the first match's priority, or LOW as the default.
        """
        for pattern, priority in self._patterns:
            if pattern.search(message):
                return priority
        return Priority.LOW

    def classify_message(self, log_message: LogMessage) -> LogMessage:
        """Classify a LogMessage, setting its priority fields.

        Sets both priority and original_priority to the classified value.
        Returns the mutated LogMessage.
        """
        priority = self.classify(log_message.message)
        log_message.priority = priority
        log_message.original_priority = priority
        return log_message
