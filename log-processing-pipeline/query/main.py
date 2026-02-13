"""Query CLI — search and display stored log entries."""

import argparse
import sys

from shared.config_loader import load_yaml
from query.src.config import QueryConfig
from query.src.searcher import search_by_pattern, search_by_index
from query.src.formatter import get_formatter


def main() -> None:
    parser = argparse.ArgumentParser(description="Query the log storage")
    parser.add_argument("--pattern", help="Regex pattern to search for")
    parser.add_argument("--index-type", help="Index type: level or date")
    parser.add_argument("--index-value", help="Index value: e.g. ERROR, 2026-02-13")
    parser.add_argument("--output", choices=["text", "json"], default="text",
                        help="Output format (default: text)")
    parser.add_argument("--lines", type=int, default=50, help="Max results (default: 50)")
    parser.add_argument("--storage-dir", help="Override storage directory")
    args = parser.parse_args()

    cfg = QueryConfig.from_dict(load_yaml()["query"])
    storage_dir = args.storage_dir or cfg.storage_dir

    if args.index_type and args.index_value:
        results = list(search_by_index(storage_dir, args.index_type,
                                       args.index_value, args.lines))
    elif args.pattern:
        results = list(search_by_pattern(storage_dir, args.pattern, args.lines))
    else:
        print("Error: provide --pattern or --index-type + --index-value", file=sys.stderr)
        sys.exit(1)

    formatter = get_formatter(args.output)
    output = formatter(results)
    if output:
        print(output)
    print(f"\n--- {len(results)} result(s) ---", file=sys.stderr)


if __name__ == "__main__":
    main()
