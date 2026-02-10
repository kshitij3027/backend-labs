#!/usr/bin/env python3
"""One-shot demo — parses hardcoded sample lines and optionally a file."""

import argparse
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.parsers import parse_line
from src.models import entry_to_dict

SAMPLE_LINES = [
    '192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326',
    '93.180.71.3 - - [17/May/2015:08:05:32 +0000] "GET /downloads/product_1 HTTP/1.1" 304 0 "-" "Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.21)"',
    '{"timestamp": "2024-01-15T10:30:00Z", "level": "INFO", "message": "Application started", "service": "auth-service"}',
    '<13>Jan  5 14:30:01 myhost sshd[12345]: Accepted publickey for user from 192.168.1.50 port 22',
    'this is not a valid log line at all',
]


def demo_hardcoded():
    """Parse hardcoded sample lines and print results."""
    print("=" * 60)
    print("Log Parsing Service — Demo")
    print("=" * 60)

    success = 0
    failure = 0

    for line in SAMPLE_LINES:
        entry = parse_line(line)
        d = entry_to_dict(entry)
        label = f"[{entry.source_format.upper()}]"
        status = "OK" if entry.parsed else "FAIL"
        print(f"\n--- {label} {status} ---")
        print(json.dumps(d, indent=2))
        if entry.parsed:
            success += 1
        else:
            failure += 1

    print("\n" + "=" * 60)
    print(f"Summary: {success} parsed, {failure} failed, {len(SAMPLE_LINES)} total")
    print("=" * 60)


def demo_file(filepath: str):
    """Parse all lines in a file and print results."""
    print(f"\n{'=' * 60}")
    print(f"Parsing file: {filepath}")
    print("=" * 60)

    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    success = 0
    failure = 0
    format_counts: dict[str, int] = {}

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        entry = parse_line(stripped)
        d = entry_to_dict(entry)
        label = f"[{entry.source_format.upper()}]"
        status = "OK" if entry.parsed else "FAIL"
        print(f"\n--- {label} {status} ---")
        print(json.dumps(d, indent=2))
        if entry.parsed:
            success += 1
        else:
            failure += 1
        format_counts[entry.source_format] = format_counts.get(entry.source_format, 0) + 1

    print(f"\n{'=' * 60}")
    print(f"File summary: {success} parsed, {failure} failed, {success + failure} total")
    print(f"Formats: {format_counts}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Log Parsing Demo")
    parser.add_argument("--file", "-f", help="Path to a log file to parse")
    args = parser.parse_args()

    demo_hardcoded()

    if args.file:
        demo_file(args.file)


if __name__ == "__main__":
    main()
