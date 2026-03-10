"""RabbitMQ topology setup: exchanges, queues, and bindings."""


def setup_topology(channel, config):
    """Declare exchanges, queues, and bindings for the log pipeline.

    Creates:
    - Topic exchange 'logs' (durable)
    - Direct DLX exchange 'logs_dlx' (durable)
    - DLQ queue 'log_queue_dlq' bound to DLX
    - Main queue 'log_queue' (durable) with dead-letter routing to DLX
    - Binding from main queue to exchange with routing key 'log.#'
    """
    exchange_cfg = config.exchange
    queue_cfg = config.queue
    dl_cfg = config.dead_letter

    # Declare the main topic exchange
    channel.exchange_declare(
        exchange=exchange_cfg["name"],
        exchange_type=exchange_cfg["type"],
        durable=exchange_cfg["durable"],
    )

    # Declare the dead-letter exchange (direct type)
    channel.exchange_declare(
        exchange=dl_cfg["exchange"],
        exchange_type="direct",
        durable=True,
    )

    # Declare the dead-letter queue and bind it to the DLX
    channel.queue_declare(queue=dl_cfg["queue"], durable=True)
    channel.queue_bind(
        queue=dl_cfg["queue"],
        exchange=dl_cfg["exchange"],
        routing_key=dl_cfg["queue"],
    )

    # Declare the main queue with dead-letter exchange argument
    channel.queue_declare(
        queue=queue_cfg["name"],
        durable=queue_cfg["durable"],
        arguments={
            "x-dead-letter-exchange": dl_cfg["exchange"],
        },
    )

    # Bind the main queue to the topic exchange
    channel.queue_bind(
        queue=queue_cfg["name"],
        exchange=exchange_cfg["name"],
        routing_key=queue_cfg["routing_key"],
    )
