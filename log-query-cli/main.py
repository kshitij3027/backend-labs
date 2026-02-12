"""log-query-cli — parse, filter, and search application log files."""

import sys
from argparse import ArgumentParser
from itertools import islice

from src.filters import build_filter_chain
from src.formatter import get_formatter
from src.parser import parse_line
from src.reader import expand_paths, read_multiple, tail_file


def build_parser() -> ArgumentParser:
    """Build the CLI argument parser."""
    parser = ArgumentParser(
        prog="log-query",
        description="Parse, filter, and search application log files.",
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="Log file path(s) or glob pattern(s)",
    )
    parser.add_argument(
        "--level",
        help="Filter by log level (e.g. ERROR, WARN, INFO, DEBUG)",
    )
    parser.add_argument(
        "--search",
        help="Filter by keyword in message (case-insensitive)",
    )
    parser.add_argument(
        "--date",
        help="Filter by date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--time-range",
        help="Filter by time range (HH:MM-HH:MM, inclusive)",
    )
    parser.add_argument(
        "--lines",
        type=int,
        help="Limit output to N entries",
    )
    parser.add_argument(
        "--output",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--color",
        action="store_true",
        help="Colorize output by log level (ANSI)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show statistics instead of log entries",
    )
    parser.add_argument(
        "--tail",
        action="store_true",
        help="Follow a log file for new entries (like tail -f)",
    )
    return parser


def run_pipeline(args):
    """Assemble and execute the generator pipeline."""
    # Validate incompatible combos
    if args.tail and args.stats:
        print("Error: --tail and --stats cannot be used together", file=sys.stderr)
        sys.exit(1)

    if args.tail and len(args.files) > 1:
        print("Error: --tail requires a single file", file=sys.stderr)
        sys.exit(1)

    # Build filter
    filter_fn = build_filter_chain(args)

    # Build formatter
    formatter = get_formatter(output_format=args.output, color=args.color)

    if args.tail:
        # Tail mode: single file, stream forever
        paths = expand_paths(args.files)
        lines = tail_file(paths[0])
    else:
        # Normal mode: expand paths, read all files
        paths = expand_paths(args.files)
        lines = read_multiple(paths)

    # Parse
    entries = (parse_line(line, source_file=path) for line, path in lines)

    # Skip unparseable
    entries = (e for e in entries if e is not None)

    # Filter
    entries = (e for e in entries if filter_fn(e))

    # Stats mode
    if args.stats:
        from src.stats import compute_stats, format_stats_text, format_stats_json
        stats = compute_stats(entries)
        if args.output == "json":
            print(format_stats_json(stats))
        else:
            print(format_stats_text(stats))
        return

    # Limit
    if args.lines:
        entries = islice(entries, args.lines)

    # Output
    for entry in entries:
        print(formatter(entry))


def main():
    parser = build_parser()
    args = parser.parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
    except BrokenPipeError:
        sys.exit(0)
