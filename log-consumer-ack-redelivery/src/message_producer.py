"""Click CLI producer for sending test log messages to RabbitMQ."""

import json
import random
import uuid
from datetime import datetime, timezone

import click
import pika

from src.config import get_settings

SERVICES = [
    "auth-api",
    "payment-gateway",
    "user-service",
    "order-processor",
    "notification-hub",
]
LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]
MESSAGES = [
    "Request processed successfully",
    "Database query completed",
    "Cache miss for key",
    "Connection timeout exceeded",
    "Rate limit threshold reached",
    "Authentication token validated",
    "Background job completed",
    "Configuration reloaded",
]


@click.group()
def cli():
    """Log message producer for testing the consumer."""
    pass


@cli.command()
@click.option("--count", default=10, help="Number of messages to send")
@click.option("--host", default=None, help="RabbitMQ host (default: from config)")
@click.option("--port", default=None, type=int, help="RabbitMQ port")
@click.option("--queue", default=None, help="Target queue")
@click.option("--simulate-failures", is_flag=True, help="Include fatal messages")
def send(count, host, port, queue, simulate_failures):
    """Send test log messages to RabbitMQ."""
    settings = get_settings()
    rmq_host = host or settings.RABBITMQ_HOST
    rmq_port = port or settings.RABBITMQ_PORT
    target_queue = queue or settings.MAIN_QUEUE

    # Connect to RabbitMQ
    credentials = pika.PlainCredentials(settings.RABBITMQ_USER, settings.RABBITMQ_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rmq_host, port=rmq_port, credentials=credentials
        )
    )
    channel = connection.channel()

    # Send messages
    for i in range(count):
        msg = {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": random.choice(LEVELS),
            "service": random.choice(SERVICES),
            "message": random.choice(MESSAGES),
            "metadata": {"index": i, "batch_size": count},
        }

        # If simulate_failures, mark ~10% as fatal
        if simulate_failures and random.random() < 0.1:
            msg["fatal"] = True
            msg["message"] = "FATAL: unrecoverable error"

        channel.basic_publish(
            exchange="",
            routing_key=target_queue,
            body=json.dumps(msg),
            properties=pika.BasicProperties(
                delivery_mode=2, content_type="application/json"
            ),
        )
        click.echo(
            f"[{i + 1}/{count}] Sent {msg['id'][:8]}... ({msg['level']}) - {msg['service']}"
        )

    connection.close()
    click.echo(f"\nDone! Sent {count} messages to {target_queue}")


if __name__ == "__main__":
    cli()
