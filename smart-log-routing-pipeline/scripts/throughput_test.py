"""Throughput benchmark for smart log routing pipeline.

Publishes 5000 messages to each exchange type and measures the rate.
Asserts that the combined rate exceeds 1000 messages per second.
"""

import sys
import time

from colorama import Fore, Style, init

from src.config import Config
from src.models.log_message import LogMessage
from src.producer import LogProducer
from src.setup import RabbitMQSetup

init(autoreset=True)

NUM_MESSAGES = 5000
MIN_RATE = 1000  # messages per second


def run_exchange_test(producer, exchange_type, publish_fn):
    """Publish NUM_MESSAGES via publish_fn, return (duration, rate).

    Args:
        producer: LogProducer instance (already connected).
        exchange_type: Label for display (e.g. "Direct").
        publish_fn: Callable that accepts a LogMessage.

    Returns:
        Tuple of (duration_seconds, messages_per_second).
    """
    messages = [LogMessage.generate_random() for _ in range(NUM_MESSAGES)]

    start = time.perf_counter()
    for msg in messages:
        publish_fn(msg)
    end = time.perf_counter()

    duration = end - start
    rate = NUM_MESSAGES / duration if duration > 0 else float("inf")
    return duration, rate


def main():
    """Run throughput benchmarks across all exchange types and print results."""
    config = Config()

    # Ensure topology is set up
    print(f"{Fore.CYAN}=== Setting up RabbitMQ topology ==={Style.RESET_ALL}")
    setup = RabbitMQSetup(config)
    setup.setup_all()

    # Create a single producer with quiet mode to suppress per-message output
    producer = LogProducer(config=config, quiet=True)
    producer.connect()

    results = []

    try:
        print(f"\n{Fore.CYAN}=== Throughput Benchmark ==={Style.RESET_ALL}")
        print(f"  Publishing {NUM_MESSAGES} messages per exchange type...\n")

        # Direct exchange test
        print(f"  Testing {Fore.GREEN}Direct{Style.RESET_ALL} exchange...")
        dur, rate = run_exchange_test(
            producer, "Direct", producer.publish_to_direct
        )
        results.append(("Direct", NUM_MESSAGES, dur, rate))
        print(f"    Done: {rate:.0f} msg/s\n")

        # Topic exchange test
        print(f"  Testing {Fore.CYAN}Topic{Style.RESET_ALL} exchange...")
        dur, rate = run_exchange_test(
            producer, "Topic", producer.publish_to_topic
        )
        results.append(("Topic", NUM_MESSAGES, dur, rate))
        print(f"    Done: {rate:.0f} msg/s\n")

        # Fanout exchange test
        print(f"  Testing {Fore.YELLOW}Fanout{Style.RESET_ALL} exchange...")
        dur, rate = run_exchange_test(
            producer, "Fanout", producer.publish_to_fanout
        )
        results.append(("Fanout", NUM_MESSAGES, dur, rate))
        print(f"    Done: {rate:.0f} msg/s\n")

    finally:
        producer.close()

    # Calculate combined totals
    total_messages = sum(r[1] for r in results)
    total_duration = sum(r[2] for r in results)
    combined_rate = total_messages / total_duration if total_duration > 0 else 0

    # Print results table
    header = (
        f"{'Exchange Type':<15}|  {'Messages':<10}|  {'Duration':<10}|  {'Rate (msg/s)'}"
    )
    separator = "\u2500" * 58

    print(f"\n{Fore.CYAN}{header}{Style.RESET_ALL}")
    print(separator)
    for name, count, dur, rate in results:
        print(f"{name:<15}|  {count:<10}|  {dur:.2f}s{'':<5}|  {rate:.0f}")
    print(separator)
    print(
        f"{'Combined':<15}|  {total_messages:<10}|  "
        f"{total_duration:.2f}s{'':<5}|  {combined_rate:.0f}"
    )

    # Pass/fail check
    print()
    if combined_rate > MIN_RATE:
        print(
            f"{Fore.GREEN}PASS: Combined throughput {combined_rate:.0f} msg/s "
            f"> {MIN_RATE} msg/s threshold{Style.RESET_ALL}"
        )
    else:
        print(
            f"{Fore.RED}FAIL: Combined throughput {combined_rate:.0f} msg/s "
            f"< {MIN_RATE} msg/s threshold{Style.RESET_ALL}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
