"""Multi-service simulation: generates logs with weighted severity distributions."""

import argparse
import random
import sys
import time

from colorama import Fore, Style, init

from scripts.wait_for_rabbitmq import wait_for_rabbitmq
from src.models.log_message import LogMessage, MESSAGE_TEMPLATES
from src.producer import LogProducer
from src.setup import RabbitMQSetup

init(autoreset=True)

# Service definitions with weighted severity distributions
SERVICE_PROFILES = {
    "user": {
        "weights": {"info": 60, "warning": 20, "error": 15, "critical": 5},
        "components": ["auth", "profile", "session", "registration"],
    },
    "database": {
        "weights": {"info": 40, "warning": 30, "error": 20, "critical": 10},
        "components": ["postgres", "mysql", "redis", "mongo"],
    },
    "api-gateway": {
        "weights": {"info": 50, "warning": 25, "error": 20, "critical": 5},
        "components": ["gateway", "rest", "graphql", "webhook"],
    },
    "security": {
        "weights": {"info": 30, "warning": 25, "error": 25, "critical": 20},
        "components": ["firewall", "ids", "auth", "scanner"],
    },
    "payment": {
        "weights": {"info": 45, "warning": 25, "error": 20, "critical": 10},
        "components": ["processor", "validator", "gateway", "ledger"],
    },
}


def pick_level(weights):
    """Choose a severity level based on weighted distribution.

    Args:
        weights: Dict mapping level names to integer weights (must sum to 100).

    Returns:
        A severity level string.
    """
    levels = list(weights.keys())
    w = list(weights.values())
    return random.choices(levels, weights=w, k=1)[0]


def generate_service_message(service_name, profile):
    """Create a LogMessage for a given service using its profile.

    Args:
        service_name: The service identifier.
        profile: Dict with 'weights' and 'components' keys.

    Returns:
        A LogMessage instance.
    """
    level = pick_level(profile["weights"])
    component = random.choice(profile["components"])

    # Use matching message templates; fall back to debug templates if level not in templates
    templates = MESSAGE_TEMPLATES.get(level, MESSAGE_TEMPLATES["info"])
    message_text = random.choice(templates)

    from datetime import datetime, timezone
    import uuid

    return LogMessage(
        timestamp=datetime.now(timezone.utc).isoformat(),
        service=service_name,
        component=component,
        level=level,
        message=message_text,
        metadata={
            "source_ip": f"10.0.{random.randint(0, 255)}.{random.randint(1, 254)}",
            "request_id": str(uuid.uuid4()),
            "simulation": True,
        },
    )


def print_sim_header(rate, duration):
    """Print a colorama-styled simulation banner."""
    print(f"\n{Fore.CYAN}{'=' * 60}")
    print(f"  Multi-Service Log Simulation")
    print(f"  Rate: {rate} msg/s | Duration: {duration}s")
    print(f"  Services: {', '.join(SERVICE_PROFILES.keys())}")
    print(f"{'=' * 60}{Style.RESET_ALL}\n")


def print_final_stats(per_service_stats, total_published, elapsed):
    """Print per-service statistics summary.

    Args:
        per_service_stats: Dict mapping service name to per-level counts.
        total_published: Total number of messages published.
        elapsed: Elapsed time in seconds.
    """
    print(f"\n{Fore.CYAN}{'=' * 60}")
    print(f"  Simulation Complete")
    print(f"  Total messages: {total_published} | Elapsed: {elapsed:.1f}s")
    print(f"  Throughput: {total_published / elapsed:.1f} msg/s")
    print(f"{'=' * 60}{Style.RESET_ALL}")

    print(
        f"\n  {Fore.WHITE}{'Service':<15} {'info':>8} {'warning':>8} "
        f"{'error':>8} {'critical':>10} {'Total':>8}{Style.RESET_ALL}"
    )
    print(f"  {'-' * 57}")

    for service_name, levels in sorted(per_service_stats.items()):
        total = sum(levels.values())
        print(
            f"  {service_name:<15} {levels.get('info', 0):>8} "
            f"{levels.get('warning', 0):>8} {levels.get('error', 0):>8} "
            f"{levels.get('critical', 0):>10} {total:>8}"
        )

    print(f"  {'-' * 57}")

    # Grand totals per level
    grand = {}
    for levels in per_service_stats.values():
        for level, count in levels.items():
            grand[level] = grand.get(level, 0) + count

    grand_total = sum(grand.values())
    print(
        f"  {Fore.GREEN}{'TOTAL':<15} {grand.get('info', 0):>8} "
        f"{grand.get('warning', 0):>8} {grand.get('error', 0):>8} "
        f"{grand.get('critical', 0):>10} {grand_total:>8}{Style.RESET_ALL}"
    )
    print()


def main():
    parser = argparse.ArgumentParser(description="Multi-Service Log Simulation")
    parser.add_argument(
        "--rate", type=int, default=20, help="Messages per second (default: 20)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Duration in seconds (default: 30)",
    )
    args = parser.parse_args()

    # Wait for RabbitMQ
    if not wait_for_rabbitmq():
        print("RabbitMQ is not available. Aborting simulation.")
        sys.exit(1)

    # Set up topology
    print(f"\n{Fore.CYAN}Setting up RabbitMQ topology...{Style.RESET_ALL}")
    RabbitMQSetup().setup_all()

    print_sim_header(args.rate, args.duration)

    # Connect producer
    producer = LogProducer()
    producer.connect()

    # Track per-service, per-level stats
    per_service_stats = {
        name: {"info": 0, "warning": 0, "error": 0, "critical": 0}
        for name in SERVICE_PROFILES
    }

    service_names = list(SERVICE_PROFILES.keys())
    interval = 1.0 / args.rate
    total_published = 0
    start_time = time.time()

    try:
        while True:
            elapsed = time.time() - start_time
            if elapsed >= args.duration:
                break

            # Pick a random service for each message
            service_name = random.choice(service_names)
            profile = SERVICE_PROFILES[service_name]

            message = generate_service_message(service_name, profile)
            producer.publish_to_all(message)
            total_published += 1

            # Track stats (publish_to_all publishes 3 copies, but we count logical messages)
            per_service_stats[service_name][message.level] += 1

            # Progress indicator every 100 messages
            if total_published % 100 == 0:
                print(
                    f"  {Fore.WHITE}[{elapsed:.1f}s] "
                    f"Published {total_published} messages...{Style.RESET_ALL}"
                )

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Simulation interrupted.{Style.RESET_ALL}")

    elapsed = time.time() - start_time

    # Cleanup
    producer.close()

    # Print final statistics
    print_final_stats(per_service_stats, total_published, elapsed)


if __name__ == "__main__":
    main()
