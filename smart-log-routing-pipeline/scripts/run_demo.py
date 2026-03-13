"""Demo orchestrator: runs producer and all consumers with live stats reporting."""

import argparse
import sys
import threading
import time

from colorama import Fore, Style, init

from scripts.wait_for_rabbitmq import wait_for_rabbitmq
from src.consumers.audit_consumer import AuditConsumer
from src.consumers.database_consumer import DatabaseConsumer
from src.consumers.error_consumer import ErrorConsumer
from src.consumers.security_consumer import SecurityConsumer
from src.models.log_message import LogMessage
from src.producer import LogProducer
from src.setup import RabbitMQSetup

init(autoreset=True)

# Shared stop event for graceful shutdown
stop_event = threading.Event()


def run_producer(producer, rate):
    """Generate and publish random log messages until stop_event is set.

    Args:
        producer: A connected LogProducer instance.
        rate: Messages per second.
    """
    interval = 1.0 / rate
    while not stop_event.is_set():
        try:
            message = LogMessage.generate_random()
            producer.publish_to_all(message)
            stop_event.wait(timeout=interval)
        except Exception as exc:
            print(f"{Fore.RED}Producer error: {exc}{Style.RESET_ALL}")
            break


def run_consumer(consumer):
    """Connect and start consuming until stop_event triggers shutdown.

    Args:
        consumer: A BaseConsumer subclass instance (not yet connected).
    """
    try:
        consumer.connect()
        consumer._channel.queue_declare(queue=consumer.queue_name, durable=True)
        consumer._channel.basic_consume(
            queue=consumer.queue_name,
            on_message_callback=consumer._on_message,
            auto_ack=False,
        )
        # Poll with short timeouts so we can check stop_event
        while not stop_event.is_set():
            consumer._conn_manager._connection.process_data_events(
                time_limit=1
            )
    except Exception as exc:
        if not stop_event.is_set():
            print(
                f"{Fore.RED}Consumer '{consumer.queue_name}' error: {exc}{Style.RESET_ALL}"
            )


def print_header(rate):
    """Print a colorama-styled header banner."""
    print(f"\n{Fore.CYAN}{'=' * 60}")
    print(f"  Smart Log Routing Pipeline - Demo")
    print(f"  Rate: {rate} messages/sec | Press Ctrl+C to stop")
    print(f"{'=' * 60}{Style.RESET_ALL}\n")


def print_stats(producer, consumers, elapsed):
    """Print a formatted stats table for producer and all consumers.

    Args:
        producer: LogProducer instance with .message_count.
        consumers: List of BaseConsumer instances.
        elapsed: Seconds since demo started.
    """
    print(f"\n{Fore.CYAN}{'=' * 60}")
    print(f"  Stats Report  |  Elapsed: {elapsed:.0f}s")
    print(f"{'=' * 60}{Style.RESET_ALL}")
    print(
        f"  {Fore.WHITE}{'Component':<25} {'Processed':>10} {'Errors':>8} "
        f"{'Rate (msg/s)':>14}{Style.RESET_ALL}"
    )
    print(f"  {'-' * 57}")

    # Producer row
    prod_rate = producer.message_count / elapsed if elapsed > 0 else 0
    print(
        f"  {Fore.GREEN}{'Producer':<25} {producer.message_count:>10} "
        f"{'N/A':>8} {prod_rate:>14.2f}{Style.RESET_ALL}"
    )

    # Consumer rows
    for consumer in consumers:
        stats = consumer.get_stats()
        color = Fore.YELLOW if stats["errors"] > 0 else Fore.WHITE
        print(
            f"  {color}{consumer.queue_name:<25} {stats['processed']:>10} "
            f"{stats['errors']:>8} {stats['messages_per_sec']:>14.2f}"
            f"{Style.RESET_ALL}"
        )

    print(f"  {Fore.CYAN}{'=' * 60}{Style.RESET_ALL}\n")


def main():
    parser = argparse.ArgumentParser(description="Smart Log Routing Pipeline Demo")
    parser.add_argument(
        "--rate", type=int, default=20, help="Messages per second (default: 20)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Duration in seconds (0 = run until Ctrl+C, default: 0)",
    )
    args = parser.parse_args()

    # Wait for RabbitMQ to be ready
    if not wait_for_rabbitmq():
        print("RabbitMQ is not available. Aborting demo.")
        sys.exit(1)

    # Set up topology
    print(f"\n{Fore.CYAN}Setting up RabbitMQ topology...{Style.RESET_ALL}")
    RabbitMQSetup().setup_all()

    print_header(args.rate)

    # Create producer
    producer = LogProducer()
    producer.connect()

    # Create consumers
    consumers = [
        ErrorConsumer(),
        SecurityConsumer(),
        DatabaseConsumer(),
        AuditConsumer(),
    ]

    # Start consumer threads
    threads = []
    for consumer in consumers:
        t = threading.Thread(target=run_consumer, args=(consumer,), daemon=True)
        t.start()
        threads.append(t)
        print(f"  {Fore.GREEN}Started consumer: {consumer.queue_name}{Style.RESET_ALL}")

    # Start producer thread
    producer_thread = threading.Thread(
        target=run_producer, args=(producer, args.rate), daemon=True
    )
    producer_thread.start()
    threads.append(producer_thread)
    print(f"  {Fore.GREEN}Started producer (rate={args.rate}/s){Style.RESET_ALL}")

    # Main loop: report stats every 5 seconds
    start_time = time.time()
    try:
        while True:
            time.sleep(5)
            elapsed = time.time() - start_time
            print_stats(producer, consumers, elapsed)
            if args.duration > 0 and elapsed >= args.duration:
                print(f"{Fore.YELLOW}Duration reached. Stopping...{Style.RESET_ALL}")
                break
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Shutting down gracefully...{Style.RESET_ALL}")

    # Signal all threads to stop
    stop_event.set()

    # Wait for threads to finish
    for t in threads:
        t.join(timeout=5)

    # Final stats
    elapsed = time.time() - start_time
    print_stats(producer, consumers, elapsed)

    # Cleanup
    producer.close()
    for consumer in consumers:
        try:
            consumer.close()
        except Exception:
            pass

    print(f"{Fore.GREEN}Demo complete.{Style.RESET_ALL}")


if __name__ == "__main__":
    main()
