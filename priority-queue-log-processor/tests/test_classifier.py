"""Tests for the MessageClassifier."""

from src.classifier import MessageClassifier
from src.models import LogMessage, Priority


# ── CRITICAL ────────────────────────────────────────────────────────

class TestCriticalClassification:
    def test_critical_payment_failure(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Payment processing failed for order 123") == Priority.CRITICAL

    def test_critical_security_breach(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Security breach detected in auth module") == Priority.CRITICAL

    def test_critical_system_down(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("System down: main database unreachable") == Priority.CRITICAL

    def test_critical_data_loss(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Data loss detected in storage cluster") == Priority.CRITICAL

    def test_critical_database_failure(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Database connection failed") == Priority.CRITICAL


# ── HIGH ────────────────────────────────────────────────────────────

class TestHighClassification:
    def test_high_latency(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("High latency detected on API gateway: 5000ms") == Priority.HIGH

    def test_high_memory(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Memory usage exceeds threshold at 95%") == Priority.HIGH

    def test_high_timeout(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Connection refused by upstream service") == Priority.HIGH

    def test_high_service_unavailable(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Service unavailable: payment-gateway") == Priority.HIGH


# ── MEDIUM ──────────────────────────────────────────────────────────

class TestMediumClassification:
    def test_medium_user_error(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("User error: invalid email format") == Priority.MEDIUM

    def test_medium_validation(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Validation failure on signup form") == Priority.MEDIUM

    def test_medium_auth_fail(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Authentication failed for user admin") == Priority.MEDIUM

    def test_medium_rate_limit(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Rate limit exceeded for client 10.0.0.1") == Priority.MEDIUM


# ── LOW (default) ──────────────────────────────────────────────────

class TestLowClassification:
    def test_low_default(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Application started successfully") == Priority.LOW

    def test_low_normal(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("Health check passed") == Priority.LOW


# ── Edge cases ─────────────────────────────────────────────────────

class TestClassifierEdgeCases:
    def test_case_insensitive(self, classifier: MessageClassifier) -> None:
        assert classifier.classify("PAYMENT FAILED for ORDER-456") == Priority.CRITICAL

    def test_classify_message(self, classifier: MessageClassifier) -> None:
        log_msg = LogMessage(message="Payment processing failed for order 789")
        result = classifier.classify_message(log_msg)

        assert result is log_msg
        assert result.priority == Priority.CRITICAL
        assert result.original_priority == Priority.CRITICAL
