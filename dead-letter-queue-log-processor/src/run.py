"""Main orchestrator for the Dead Letter Queue Log Processor."""

import asyncio
import signal
import sys

from colorama import Fore, Style, init as colorama_init

from src.config import load_config
from src.redis_client import RedisClient
from src.producer import MessageProducer
from src.processor import MessageProcessor
from src.retry_scheduler import RetryScheduler
from src.classifier import FailureClassifier
from src.dashboard import Dashboard
from src.stats import StatsTracker


async def main():
    colorama_init()
    settings = load_config()

    # Print startup banner
    print(f"{Fore.CYAN}{'='*60}")
    print(f"  Dead Letter Queue Log Processor")
    print(f"  Dashboard: http://localhost:{settings.dashboard_port}")
    print(f"{'='*60}{Style.RESET_ALL}")

    # Initialize Redis
    redis_client = RedisClient(settings)
    await redis_client.connect()
    print(f"{Fore.GREEN}[ok] Connected to Redis{Style.RESET_ALL}")

    # Initialize components
    stats = StatsTracker(redis_client, settings)
    classifier = FailureClassifier()
    producer = MessageProducer(redis_client, settings)
    processor = MessageProcessor(
        redis_client, settings, classifier, stats_tracker=stats
    )
    scheduler = RetryScheduler(redis_client, settings)
    dashboard = Dashboard(redis_client, settings)

    # Stop event for graceful shutdown
    stop_event = asyncio.Event()

    def handle_shutdown(sig, frame):
        print(
            f"\n{Fore.YELLOW}Received {signal.Signals(sig).name}, "
            f"shutting down...{Style.RESET_ALL}"
        )
        stop_event.set()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print(f"{Fore.GREEN}[ok] All components initialized{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Starting processing pipeline...{Style.RESET_ALL}\n")

    # Run all components concurrently
    try:
        await asyncio.gather(
            producer.produce_continuous(stop_event),
            processor.run(stop_event),
            scheduler.run(stop_event),
            dashboard.start(stop_event),
        )
    except Exception as e:
        print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
    finally:
        await redis_client.close()
        print(f"\n{Fore.GREEN}[ok] Shutdown complete{Style.RESET_ALL}")


if __name__ == "__main__":
    asyncio.run(main())
