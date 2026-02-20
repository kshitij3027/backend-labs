import argparse
import asyncio

from generator.config import GeneratorConfig
from generator.load_generator import LoadGenerator


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load generator for TCP log server"
    )
    parser.add_argument(
        "total_logs",
        type=int,
        nargs="?",
        default=None,
        help="Total number of log messages to send",
    )
    parser.add_argument(
        "duration",
        type=int,
        nargs="?",
        default=None,
        help="Max duration in seconds",
    )
    parser.add_argument(
        "concurrency",
        type=int,
        nargs="?",
        default=None,
        help="Number of concurrent workers",
    )
    parser.add_argument("--host", default=None, help="Server host")
    parser.add_argument(
        "--port", type=int, default=None, help="Server port"
    )
    parser.add_argument(
        "--no-tls", action="store_true", help="Disable TLS"
    )
    parser.add_argument(
        "--compress", action="store_true", help="Enable gzip compression"
    )
    parser.add_argument(
        "--batch-size", type=int, default=None, help="Batch size"
    )
    parser.add_argument(
        "--target-rps",
        type=int,
        default=None,
        help="Target RPS (0=unlimited)",
    )
    return parser.parse_args()


async def main():
    args = parse_args()

    # Start with env-based config, then override with CLI args
    config = GeneratorConfig.from_env()

    overrides = {}
    if args.total_logs is not None:
        overrides["TOTAL_LOGS"] = args.total_logs
    if args.duration is not None:
        overrides["DURATION_SECS"] = args.duration
    if args.concurrency is not None:
        overrides["CONCURRENCY"] = args.concurrency
    if args.host is not None:
        overrides["SERVER_HOST"] = args.host
    if args.port is not None:
        overrides["SERVER_PORT"] = args.port
    if args.no_tls:
        overrides["ENABLE_TLS"] = False
    if args.compress:
        overrides["COMPRESS"] = True
    if args.batch_size is not None:
        overrides["BATCH_SIZE"] = args.batch_size
    if args.target_rps is not None:
        overrides["TARGET_RPS"] = args.target_rps

    if overrides:
        # Create new config with overrides (frozen dataclass, so reconstruct)
        from dataclasses import asdict

        d = asdict(config)
        d.update(overrides)
        config = GeneratorConfig(**d)

    print("Load Generator Config:")
    print(f"  Server:      {config.SERVER_HOST}:{config.SERVER_PORT}")
    print(f"  Total Logs:  {config.TOTAL_LOGS}")
    print(f"  Duration:    {config.DURATION_SECS}s")
    print(f"  Concurrency: {config.CONCURRENCY}")
    print(f"  TLS:         {config.ENABLE_TLS}")
    print(f"  Compress:    {config.COMPRESS}")
    print()

    generator = LoadGenerator(config)
    await generator.run()


if __name__ == "__main__":
    asyncio.run(main())
