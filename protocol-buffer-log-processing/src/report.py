"""Generate a formatted benchmark report comparing JSON and Protocol Buffers."""

from __future__ import annotations

from src.benchmark import BenchmarkResult
from src.config import Config


def _fmt_bytes(n: int) -> str:
    """Format byte count as a human-readable string (B / KB / MB / GB)."""
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f} GB"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f} MB"
    if n >= 1_000:
        return f"{n / 1_000:.2f} KB"
    return f"{n} B"


def _section_header(title: str, width: int = 72) -> str:
    """Return a box-drawn section header."""
    top = f"+{'-' * (width - 2)}+"
    mid = f"| {title:<{width - 4}} |"
    bot = f"+{'-' * (width - 2)}+"
    return f"{top}\n{mid}\n{bot}"


def generate_report(
    results: dict[str, BenchmarkResult],
    config: Config,
    num_entries: int = 100,
) -> str:
    """Build a multi-section benchmark report.

    Sections:
    1. Speed Comparison — timing table
    2. Size Comparison — byte sizes, ratio, savings
    3. Real-World Projections — daily storage and time savings
    4. High-Scale Impact — per-second overhead at high rate
    5. Summary line

    Args:
        results: Dict returned by :func:`~src.benchmark.run_all_benchmarks`.
        config: Application :class:`~src.config.Config` instance.
        num_entries: Number of log entries in the benchmark batch.

    Returns:
        A formatted multi-line string ready for printing.
    """
    lines: list[str] = []
    w = 72

    # Convenience aliases
    js = results["json_serialize"]
    jd = results["json_deserialize"]
    ps = results["protobuf_serialize"]
    pd = results["protobuf_deserialize"]

    # =======================================================================
    # Header
    # =======================================================================
    lines.append("")
    lines.append("=" * w)
    lines.append("  PROTOCOL BUFFERS vs JSON  --  BENCHMARK REPORT")
    lines.append("=" * w)
    lines.append(f"  Log entries benchmarked : {js.iterations} iterations")
    lines.append("")

    # =======================================================================
    # Section 1: Speed Comparison
    # =======================================================================
    lines.append(_section_header("SECTION 1: SPEED COMPARISON", w))
    lines.append("")

    hdr = (
        f"  {'Operation':<26} {'Mean (ms)':>10} {'StdDev':>10}"
        f" {'Min (ms)':>10} {'Max (ms)':>10}"
    )
    lines.append(hdr)
    lines.append(f"  {'-' * 66}")

    for label, res in [
        ("JSON serialize", js),
        ("JSON deserialize", jd),
        ("Protobuf serialize", ps),
        ("Protobuf deserialize", pd),
    ]:
        row = (
            f"  {label:<26} {res.mean_ms:>10.4f} {res.stddev_ms:>10.4f}"
            f" {res.min_ms:>10.4f} {res.max_ms:>10.4f}"
        )
        lines.append(row)

    lines.append("")

    # Speed ratios
    ser_ratio = js.mean_ms / ps.mean_ms if ps.mean_ms > 0 else float("inf")
    deser_ratio = jd.mean_ms / pd.mean_ms if pd.mean_ms > 0 else float("inf")
    lines.append(f"  Serialize speed-up   : Protobuf is {ser_ratio:.2f}x faster")
    lines.append(f"  Deserialize speed-up : Protobuf is {deser_ratio:.2f}x faster")
    lines.append("")

    # =======================================================================
    # Section 2: Size Comparison
    # =======================================================================
    lines.append(_section_header("SECTION 2: SIZE COMPARISON", w))
    lines.append("")

    json_size = js.total_bytes
    proto_size = ps.total_bytes
    size_ratio = json_size / proto_size if proto_size > 0 else float("inf")
    savings_pct = (1 - proto_size / json_size) * 100 if json_size > 0 else 0.0

    lines.append(f"  JSON size     : {_fmt_bytes(json_size):>12}  ({json_size:,} bytes)")
    lines.append(f"  Protobuf size : {_fmt_bytes(proto_size):>12}  ({proto_size:,} bytes)")
    lines.append(f"  Size ratio    : {size_ratio:.2f}x smaller with Protobuf")
    lines.append(f"  Savings       : {savings_pct:.1f}%")
    lines.append("")

    # =======================================================================
    # Section 3: Real-World Projections
    # =======================================================================
    lines.append(_section_header("SECTION 3: REAL-WORLD PROJECTIONS", w))
    lines.append(f"  Based on DAILY_LOG_VOLUME = {config.DAILY_LOG_VOLUME:,} logs/day")
    lines.append("")

    # Storage savings — scale per-entry sizes from the benchmark batch
    entry_count = max(1, num_entries)
    json_per_entry = json_size / entry_count
    proto_per_entry = proto_size / entry_count

    daily_json_bytes = json_per_entry * config.DAILY_LOG_VOLUME
    daily_proto_bytes = proto_per_entry * config.DAILY_LOG_VOLUME
    daily_savings_bytes = daily_json_bytes - daily_proto_bytes

    lines.append(f"  Daily JSON storage     : {_fmt_bytes(int(daily_json_bytes))}")
    lines.append(f"  Daily Protobuf storage : {_fmt_bytes(int(daily_proto_bytes))}")
    lines.append(f"  Daily savings          : {_fmt_bytes(int(daily_savings_bytes))}")
    lines.append("")

    # Serialization time savings (extrapolated)
    json_time_per_entry_ms = js.mean_ms / entry_count
    proto_time_per_entry_ms = ps.mean_ms / entry_count
    daily_json_time_s = json_time_per_entry_ms * config.DAILY_LOG_VOLUME / 1000.0
    daily_proto_time_s = proto_time_per_entry_ms * config.DAILY_LOG_VOLUME / 1000.0
    daily_time_savings_s = daily_json_time_s - daily_proto_time_s

    lines.append(f"  Daily JSON serialization time     : {daily_json_time_s:,.1f} s")
    lines.append(f"  Daily Protobuf serialization time : {daily_proto_time_s:,.1f} s")
    lines.append(f"  Daily time savings                : {daily_time_savings_s:,.1f} s")
    lines.append("")

    # =======================================================================
    # Section 4: High-Scale Impact
    # =======================================================================
    lines.append(_section_header("SECTION 4: HIGH-SCALE IMPACT", w))
    lines.append(f"  Based on HIGH_SCALE_RATE = {config.HIGH_SCALE_RATE:,} logs/sec")
    lines.append("")

    json_overhead_ms = json_time_per_entry_ms * config.HIGH_SCALE_RATE
    proto_overhead_ms = proto_time_per_entry_ms * config.HIGH_SCALE_RATE
    json_overhead_pct = (json_overhead_ms / 1000.0) * 100  # % of one second
    proto_overhead_pct = (proto_overhead_ms / 1000.0) * 100

    lines.append(
        f"  JSON   : {json_overhead_ms:,.2f} ms/s serialization overhead "
        f"({json_overhead_pct:.1f}% of wall-clock)"
    )
    lines.append(
        f"  Protobuf : {proto_overhead_ms:,.2f} ms/s serialization overhead "
        f"({proto_overhead_pct:.1f}% of wall-clock)"
    )
    lines.append("")

    bytes_per_sec_json = (json_size / entry_count) * config.HIGH_SCALE_RATE
    bytes_per_sec_proto = (proto_size / entry_count) * config.HIGH_SCALE_RATE
    lines.append(f"  JSON throughput     : {_fmt_bytes(int(bytes_per_sec_json))}/s")
    lines.append(f"  Protobuf throughput : {_fmt_bytes(int(bytes_per_sec_proto))}/s")
    lines.append("")

    # =======================================================================
    # Summary
    # =======================================================================
    lines.append("=" * w)
    overall_speed = (ser_ratio + deser_ratio) / 2
    lines.append(
        f"  SUMMARY: Protobuf is {overall_speed:.1f}x faster and "
        f"{size_ratio:.1f}x smaller than JSON"
    )
    lines.append("=" * w)
    lines.append("")

    return "\n".join(lines)
