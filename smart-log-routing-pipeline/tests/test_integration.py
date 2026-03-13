"""Integration tests for smart log routing pipeline (require live RabbitMQ).

These tests verify that messages are correctly routed through direct, topic,
and fanout exchanges to their bound queues using a real RabbitMQ instance.
"""

import os
import time

import pytest
import requests

from src.config import Config
from src.models.log_message import LogMessage
from src.producer import LogProducer
from src.setup import RabbitMQSetup

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def config():
    """Create a Config instance (picks up RABBITMQ_HOST from env)."""
    return Config()


@pytest.fixture(scope="module")
def management_url(config):
    """Return the base URL for the RabbitMQ management API.

    Uses RABBITMQ_HOST env var directly (consistent with network_mode sharing).
    """
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    return f"http://{host}:{config.management_port}"


@pytest.fixture(scope="module")
def management_auth(config):
    """Return (username, password) tuple for management API auth."""
    return (config.username, config.password)


@pytest.fixture(scope="module")
def topology(config):
    """Ensure all exchanges and queues are declared before tests run."""
    setup = RabbitMQSetup(config)
    setup.setup_all()


@pytest.fixture(scope="module")
def producer(config, topology):
    """Create and connect a LogProducer (quiet mode to suppress output)."""
    p = LogProducer(config=config, quiet=True)
    p.connect()
    yield p
    p.close()


def purge_queue(management_url, auth, queue_name):
    """Delete all messages from a queue via the management HTTP API."""
    url = f"{management_url}/api/queues/%2f/{queue_name}/contents"
    requests.delete(url, auth=auth, timeout=5)


def get_queue_message_count(management_url, auth, queue_name):
    """Return the number of messages currently in a queue."""
    url = f"{management_url}/api/queues/%2f/{queue_name}"
    resp = requests.get(url, auth=auth, timeout=5)
    resp.raise_for_status()
    return resp.json().get("messages", 0)


def wait_for_queue_messages(management_url, auth, queue_name, expected=1, timeout=10):
    """Poll until queue has at least expected messages, returns final count."""
    for _ in range(timeout):
        time.sleep(1)
        count = get_queue_message_count(management_url, auth, queue_name)
        if count >= expected:
            return count
    return get_queue_message_count(management_url, auth, queue_name)


def get_messages_from_queue(management_url, auth, queue_name, count=1):
    """Fetch messages from a queue via management API (non-destructive peek).

    Uses ackmode=ack_requeue_true so messages stay in the queue.
    """
    url = f"{management_url}/api/queues/%2f/{queue_name}/get"
    payload = {
        "count": count,
        "ackmode": "ack_requeue_true",
        "encoding": "auto",
        "truncate": 50000,
    }
    resp = requests.post(url, json=payload, auth=auth, timeout=5)
    resp.raise_for_status()
    return resp.json()


class TestPublishConsumeDirect:
    """Verify direct exchange routes messages to the correct queue by level."""

    def test_error_message_reaches_error_queue(
        self, management_url, management_auth, producer
    ):
        purge_queue(management_url, management_auth, "error_logs")

        msg = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="api",
            component="gateway",
            level="error",
            message="Test error for direct routing",
            metadata={"test": True},
        )
        producer.publish_to_direct(msg)

        count = wait_for_queue_messages(
            management_url, management_auth, "error_logs"
        )
        assert count >= 1, f"Expected >= 1 message in error_logs, got {count}"

    def test_warning_message_reaches_warning_queue(
        self, management_url, management_auth, producer
    ):
        purge_queue(management_url, management_auth, "warning_logs")

        msg = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="api",
            component="gateway",
            level="warning",
            message="Test warning for direct routing",
            metadata={"test": True},
        )
        producer.publish_to_direct(msg)

        count = wait_for_queue_messages(
            management_url, management_auth, "warning_logs"
        )
        assert count >= 1, f"Expected >= 1 message in warning_logs, got {count}"


class TestPublishConsumeTopic:
    """Verify topic exchange routes messages based on hierarchical routing key."""

    def test_database_message_reaches_database_queue(
        self, management_url, management_auth, producer
    ):
        purge_queue(management_url, management_auth, "database_logs")

        msg = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="database",
            component="postgres",
            level="error",
            message="Test database error for topic routing",
            metadata={"test": True},
        )
        producer.publish_to_topic(msg)

        count = wait_for_queue_messages(
            management_url, management_auth, "database_logs"
        )
        assert count >= 1, f"Expected >= 1 message in database_logs, got {count}"

    def test_security_message_reaches_security_queue(
        self, management_url, management_auth, producer
    ):
        purge_queue(management_url, management_auth, "security_logs")

        msg = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="security",
            component="firewall",
            level="warning",
            message="Test security warning for topic routing",
            metadata={"test": True},
        )
        producer.publish_to_topic(msg)

        count = wait_for_queue_messages(
            management_url, management_auth, "security_logs"
        )
        assert count >= 1, f"Expected >= 1 message in security_logs, got {count}"


class TestPublishConsumeFanout:
    """Verify fanout exchange broadcasts to all bound queues."""

    def test_fanout_reaches_both_audit_and_all_logs(
        self, management_url, management_auth, producer
    ):
        purge_queue(management_url, management_auth, "audit_logs")
        purge_queue(management_url, management_auth, "all_logs")

        msg = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="payment",
            component="processor",
            level="info",
            message="Test info for fanout routing",
            metadata={"test": True},
        )
        producer.publish_to_fanout(msg)

        wait_for_queue_messages(
            management_url, management_auth, "audit_logs"
        )
        audit_count = get_queue_message_count(
            management_url, management_auth, "audit_logs"
        )
        all_count = get_queue_message_count(
            management_url, management_auth, "all_logs"
        )
        assert audit_count >= 1, (
            f"Expected >= 1 message in audit_logs, got {audit_count}"
        )
        assert all_count >= 1, (
            f"Expected >= 1 message in all_logs, got {all_count}"
        )


class TestMessagePersistence:
    """Verify that published messages use delivery_mode=2 (persistent)."""

    def test_delivery_mode_is_persistent(
        self, management_url, management_auth, producer
    ):
        purge_queue(management_url, management_auth, "error_logs")

        msg = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="api",
            component="gateway",
            level="error",
            message="Test persistence check",
            metadata={"test": True},
        )
        producer.publish_to_direct(msg)

        wait_for_queue_messages(
            management_url, management_auth, "error_logs"
        )
        messages = get_messages_from_queue(
            management_url, management_auth, "error_logs", count=1
        )
        assert len(messages) >= 1, "No messages retrieved from error_logs"

        props = messages[0].get("properties", {})
        assert props.get("delivery_mode") == 2, (
            f"Expected delivery_mode=2, got {props.get('delivery_mode')}"
        )
