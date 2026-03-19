"""Rich terminal dashboard for CLI mode monitoring."""
import time
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from src.monitoring.metrics import MetricsCollector


class CLIDashboard:
    """Displays real-time consumer group metrics in the terminal using Rich."""

    def __init__(self, metrics: MetricsCollector, producer_stats_fn=None) -> None:
        self._metrics = metrics
        self._producer_stats_fn = producer_stats_fn
        self._console = Console()
        self._prev_consumed = 0

    def _build_summary_table(self, snap: dict, producer_stats: dict) -> Table:
        table = Table(title="System Summary", expand=True)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")

        table.add_row("Total Produced", str(producer_stats.get("produced", 0)))
        table.add_row("Total Consumed", str(snap["total_consumed"]))
        table.add_row("Total Errors", str(snap["total_errors"]))
        table.add_row("Uptime", f"{snap['uptime_seconds']:.0f}s")

        mps = snap["total_consumed"] - self._prev_consumed
        self._prev_consumed = snap["total_consumed"]
        table.add_row("Throughput", f"{mps} msg/s")

        return table

    def _build_partition_table(self, snap: dict) -> Table:
        table = Table(title="Partition Distribution", expand=True)
        table.add_column("Partition", style="cyan")
        table.add_column("Messages", justify="right")
        table.add_column("Consumer", style="yellow")
        table.add_column("Lag", justify="right")

        consumer_map = {}
        for cid, info in snap.get("per_consumer", {}).items():
            for p in info.get("partitions", []):
                consumer_map[p] = cid

        for i in range(6):
            count = snap.get("per_partition", {}).get(i, 0)
            consumer = consumer_map.get(i, "-")
            lag = snap.get("lag", {}).get(i, 0)
            table.add_row(f"P-{i}", str(count), consumer, str(lag))

        return table

    def _build_consumer_table(self, snap: dict) -> Table:
        table = Table(title="Consumer Status", expand=True)
        table.add_column("Consumer", style="cyan")
        table.add_column("Consumed", justify="right")
        table.add_column("Errors", justify="right")
        table.add_column("Partitions", style="yellow")

        for cid, info in sorted(snap.get("per_consumer", {}).items()):
            parts = ", ".join(str(p) for p in info.get("partitions", []))
            table.add_row(cid, str(info["consumed"]), str(info["errors"]), parts)

        return table

    def _build_rebalance_table(self, snap: dict) -> Table:
        table = Table(title="Recent Rebalance Events", expand=True)
        table.add_column("Time", style="cyan")
        table.add_column("Type", style="yellow")
        table.add_column("Consumer")
        table.add_column("Partitions")

        for evt in snap.get("rebalance_events", [])[-5:]:
            t = time.strftime("%H:%M:%S", time.localtime(evt["timestamp"]))
            parts = ", ".join(str(p) for p in evt.get("partitions", []))
            table.add_row(t, evt["type"], evt.get("consumer_id", "-"), parts)

        return table

    def build_display(self) -> Layout:
        """Build the full dashboard layout."""
        snap = self._metrics.snapshot()
        producer_stats = self._producer_stats_fn() if self._producer_stats_fn else {}

        layout = Layout()
        layout.split_column(
            Layout(name="top", size=10),
            Layout(name="middle", size=12),
            Layout(name="bottom", size=10),
        )

        layout["top"].split_row(
            Layout(Panel(self._build_summary_table(snap, producer_stats))),
            Layout(Panel(self._build_consumer_table(snap))),
        )
        layout["middle"].update(Panel(self._build_partition_table(snap)))
        layout["bottom"].update(Panel(self._build_rebalance_table(snap)))

        return layout

    def run(self, shutdown_event) -> None:
        """Run the live dashboard until shutdown."""
        with Live(self.build_display(), console=self._console, refresh_per_second=1, screen=True) as live:
            while not shutdown_event.is_set():
                live.update(self.build_display())
                shutdown_event.wait(1.0)
