"""Integration tests that run against a live RabbitMQ instance.

These tests require a running RabbitMQ server with the management plugin enabled.
They are marked with @pytest.mark.integration and skipped unless explicitly selected.

In Docker, set RABBITMQ_HOST=rabbitmq (handled by docker-compose).
"""

import json
import os
import time

import pytest
import requests

from src.config import Config
from src.connection import RabbitMQConnection
from src.health_checker import HealthChecker
from src.publisher import LogPublisher
from src.setup import RabbitMQSetup

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
MGMT_URL = f"http://{RABBITMQ_HOST}:15672/api"
AUTH = ("guest", "guest")


def purge_queue(queue_name):
    """Delete all messages from a queue via the Management API."""
    url = f"{MGMT_URL}/queues/%2f/{queue_name}/contents"
    requests.delete(url, auth=AUTH, timeout=5)


def get_queue_info(queue_name):
    """Fetch queue details from the Management API."""
    url = f"{MGMT_URL}/queues/%2f/{queue_name}"
    resp = requests.get(url, auth=AUTH, timeout=5)
    return resp


def get_messages(queue_name, count=1):
    """Consume messages from a queue via the Management API."""
    url = f"{MGMT_URL}/queues/%2f/{queue_name}/get"
    body = {"count": count, "ackmode": "ack_requeue_false", "encoding": "auto"}
    resp = requests.post(url, json=body, auth=AUTH, timeout=5)
    resp.raise_for_status()
    return resp.json()


@pytest.fixture
def setup_rabbitmq():
    """Set up the full RabbitMQ topology before each integration test."""
    config = Config()
    setup = RabbitMQSetup(config)
    setup.setup_all()

    # Purge all queues to avoid interference
    queue_names = [q["name"] for q in config.get_queue_configs()]
    queue_names.append(config.get_dlx_config()["queue"])
    for name in queue_names:
        purge_queue(name)

    yield config

    # Cleanup: purge queues again
    for name in queue_names:
        purge_queue(name)


@pytest.mark.integration
class TestQueueCreationAndBinding:
    """Verify that setup_all creates the correct RabbitMQ topology."""

    def test_queue_creation_and_binding(self, setup_rabbitmq):
        """Verify exchanges, queues, DLX, and bindings all exist."""
        config = setup_rabbitmq

        # Verify main exchange 'logs' exists
        resp = requests.get(
            f"{MGMT_URL}/exchanges/%2f/logs", auth=AUTH, timeout=5
        )
        assert resp.status_code == 200, "Exchange 'logs' should exist"

        # Verify DLX exchange 'logs_dlx' exists
        resp = requests.get(
            f"{MGMT_URL}/exchanges/%2f/logs_dlx", auth=AUTH, timeout=5
        )
        assert resp.status_code == 200, "DLX exchange 'logs_dlx' should exist"

        # Verify all 3 main queues exist
        for q in config.get_queue_configs():
            resp = get_queue_info(q["name"])
            assert resp.status_code == 200, f"Queue '{q['name']}' should exist"

        # Verify dead_letter_queue exists
        dlx_config = config.get_dlx_config()
        resp = get_queue_info(dlx_config["queue"])
        assert resp.status_code == 200, "dead_letter_queue should exist"

        # Verify bindings for each queue
        for q in config.get_queue_configs():
            url = f"{MGMT_URL}/queues/%2f/{q['name']}/bindings"
            resp = requests.get(url, auth=AUTH, timeout=5)
            assert resp.status_code == 200
            bindings = resp.json()
            routing_keys = [b["routing_key"] for b in bindings]
            assert q["routing_key"] in routing_keys, (
                f"Queue '{q['name']}' should be bound with routing key '{q['routing_key']}'"
            )


@pytest.mark.integration
class TestMessageRoutingByLogLevel:
    """Verify that messages route to the correct queues by log level."""

    def test_message_routing_by_log_level(self, setup_rabbitmq):
        """Publish one message per level and verify each lands in the correct queue."""
        config = setup_rabbitmq
        publisher = LogPublisher(config)

        # Publish one message per level
        publisher.publish("info", "web", "info message")
        publisher.publish("error", "api", "error message")
        publisher.publish("debug", "worker", "debug message")

        # Allow time for messages to be routed and stats to update
        time.sleep(1.5)

        # Verify message counts via Management API
        for queue_name, expected_level, expected_msg in [
            ("log_messages", "info", "info message"),
            ("error_messages", "error", "error message"),
            ("debug_messages", "debug", "debug message"),
        ]:
            resp = get_queue_info(queue_name)
            assert resp.status_code == 200
            data = resp.json()
            assert data.get("messages", 0) >= 1, (
                f"Queue '{queue_name}' should have at least 1 message, got: {data}"
            )

            # Consume and verify content
            messages = get_messages(queue_name, count=1)
            assert len(messages) >= 1
            body = json.loads(messages[0]["payload"])
            assert body["level"] == expected_level
            assert body["message"] == expected_msg


@pytest.mark.integration
class TestMessagePersistenceAndDurability:
    """Verify that queues are durable and messages are persistent."""

    def test_message_persistence_and_durability(self, setup_rabbitmq):
        """Check queue durable=true and that published messages exist."""
        config = setup_rabbitmq

        # Verify all queues are durable
        for q in config.get_queue_configs():
            resp = get_queue_info(q["name"])
            assert resp.status_code == 200
            data = resp.json()
            assert data["durable"] is True, (
                f"Queue '{q['name']}' should be durable"
            )

        # Publish a message and verify it exists in the queue
        publisher = LogPublisher(config)
        publisher.publish("info", "web", "persistence test")

        time.sleep(1.5)

        resp = get_queue_info("log_messages")
        data = resp.json()
        assert data.get("messages", 0) >= 1, (
            f"log_messages should have at least 1 message after publishing, got: {data}"
        )


@pytest.mark.integration
class TestHealthCheckReturnsHealthy:
    """Verify that the HealthChecker reports healthy on a live system."""

    def test_health_check_returns_healthy(self, setup_rabbitmq):
        """All health checks should pass against a running, configured RabbitMQ."""
        config = setup_rabbitmq
        checker = HealthChecker(config)
        report = checker.run_health_check()

        assert report["overall"] == "healthy"
        assert report["connection"]["status"] == "healthy"
        assert report["management_api"]["status"] == "healthy"
        assert report["queues"]["status"] == "healthy"
