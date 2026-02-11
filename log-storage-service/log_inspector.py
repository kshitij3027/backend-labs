"""CLI log inspector — list, read, and search log files."""

import argparse
import os
import sys

from src.inspector import list_log_files, read_file, search_files


def main():
    parser = argparse.ArgumentParser(description="Inspect stored log files")
    parser.add_argument("--log-dir", default=os.environ.get("LOG_DIR", "./logs"),
                        help="Directory containing log files")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list", action="store_true", help="List all log files")
    group.add_argument("--read", metavar="FILENAME", help="Read a specific log file")
    group.add_argument("--search", metavar="TEXT", help="Search text across all log files")
    args = parser.parse_args()

    if args.list:
        files = list_log_files(args.log_dir)
        if not files:
            print("No log files found.")
            return
        for name in files:
            path = os.path.join(args.log_dir, name)
            size = os.path.getsize(path)
            if size < 1024:
                size_str = f"{size} B"
            elif size < 1024 * 1024:
                size_str = f"{size / 1024:.1f} KB"
            else:
                size_str = f"{size / (1024 * 1024):.1f} MB"
            print(f"  {name}  ({size_str})")

    elif args.read:
        try:
            content = read_file(args.log_dir, args.read)
            sys.stdout.write(content)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    elif args.search:
        results = search_files(args.log_dir, args.search)
        if not results:
            print(f"No matches found for '{args.search}'.")
            return
        for filename, line_num, line in results:
            print(f"  [{filename}:{line_num}] {line}")


if __name__ == "__main__":
    main()
