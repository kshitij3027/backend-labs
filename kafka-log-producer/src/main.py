"""CLI entry point for the Kafka Log Producer."""

import signal
import sys
import time

import typer
import uvicorn
from rich.console import Console
from rich.table import Table

from src.config import Config
from src.dashboard import create_app
from src.log_generator import LogGenerator
from src.producer import KafkaLogProducer

app = typer.Typer(help="Kafka Log Producer — demo, server, and performance testing.")
console = Console()


@app.command()
def server(
    config_path: str = typer.Option("config/producer_config.yaml", help="Path to YAML config"),
):
    """Start the FastAPI dashboard server."""
    config = Config(config_path)
    console.print(f"[bold green]Starting server on port {config.dashboard_port}...[/]")
    fastapi_app = create_app(config)
    uvicorn.run(fastapi_app, host="0.0.0.0", port=config.dashboard_port, log_level="info")


@app.command()
def demo(
    count: int = typer.Option(100, help="Number of logs to send"),
    rate: float = typer.Option(10.0, help="Messages per second"),
    config_path: str = typer.Option("config/producer_config.yaml", help="Path to YAML config"),
):
    """Send sample logs at a configurable rate."""
    config = Config(config_path)
    producer = KafkaLogProducer(config)
    generator = LogGenerator()

    # Graceful shutdown on SIGINT
    shutdown = False

    def handle_signal(sig, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, handle_signal)

    console.print(f"[bold]Sending {count} logs at {rate} msg/s...[/]")

    sent = 0
    failed = 0
    start_time = time.time()
    interval = 1.0 / rate if rate > 0 else 0

    for i in range(count):
        if shutdown:
            console.print("\n[yellow]Interrupted — flushing...[/]")
            break
        entry = generator.generate_one()
        try:
            producer.send_log(entry)
            sent += 1
        except Exception:
            failed += 1

        if interval > 0 and (i + 1) % max(1, int(rate)) == 0:
            elapsed = time.time() - start_time
            expected = (i + 1) / rate
            sleep_time = expected - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

        if (i + 1) % 10 == 0 or (i + 1) == count:
            console.print(f"  Progress: {i+1}/{count} sent={sent} failed={failed}", end="\r")

    producer.flush()
    producer.close()
    elapsed = time.time() - start_time

    # Summary table
    console.print()
    table = Table(title="Demo Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Total Sent", str(sent))
    table.add_row("Total Failed", str(failed))
    table.add_row("Duration", f"{elapsed:.1f}s")
    table.add_row("Throughput", f"{sent/elapsed:.0f} msg/s" if elapsed > 0 else "N/A")
    console.print(table)


@app.command()
def performance(
    duration: int = typer.Argument(60, help="Test duration in seconds"),
    rate: int = typer.Argument(1000, help="Target messages per second"),
    config_path: str = typer.Option("config/producer_config.yaml", help="Path to YAML config"),
):
    """Run a sustained throughput test."""
    config = Config(config_path)
    producer = KafkaLogProducer(config)
    generator = LogGenerator()

    console.print(f"[bold]Performance test: {duration}s at {rate} msg/s target[/]")

    shutdown = False

    def handle_signal(sig, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, handle_signal)

    total_sent = 0
    total_failed = 0
    start = time.time()

    while time.time() - start < duration and not shutdown:
        batch_start = time.time()
        entries = generator.generate_batch(rate)

        for entry in entries:
            producer.send_log(entry)
            total_sent += 1

        # Pace to 1-second batches
        batch_elapsed = time.time() - batch_start
        if batch_elapsed < 1.0:
            time.sleep(1.0 - batch_elapsed)

        elapsed = time.time() - start
        actual_rate = total_sent / elapsed if elapsed > 0 else 0
        console.print(f"  [{elapsed:.0f}s] sent={total_sent} rate={actual_rate:.0f}/s", end="\r")

    remaining = producer.flush(30)
    producer.close()

    elapsed = time.time() - start
    stats = producer.stats
    total_failed = stats.get("total_failed", 0)
    actual_throughput = total_sent / elapsed if elapsed > 0 else 0

    console.print()
    table = Table(title="Performance Results")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Duration", f"{elapsed:.1f}s")
    table.add_row("Total Sent", str(total_sent))
    table.add_row("Total Failed", str(total_failed))
    table.add_row("Throughput", f"{actual_throughput:.0f} msg/s")
    table.add_row("Target Rate", f"{rate} msg/s")
    table.add_row("Unflushed", str(remaining))

    passed = actual_throughput >= 1000 and total_failed == 0
    status = "[bold green]PASS[/]" if passed else "[bold red]FAIL[/]"
    table.add_row("Result", status)
    console.print(table)

    if not passed:
        sys.exit(1)


if __name__ == "__main__":
    app()
