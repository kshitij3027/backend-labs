"""CLI wrapper for log generation."""

import argparse
import os
import sys

sys.path.insert(0, "/app")  # for Docker

from src.generator import generate_apache_logs, generate_json_logs


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample log data for MapReduce analytics"
    )
    parser.add_argument("--output-dir", default="/data")
    parser.add_argument("--json-count", type=int, default=10_000)
    parser.add_argument("--apache-count", type=int, default=5_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    json_path = f"{args.output_dir}/sample-logs.jsonl"
    apache_path = f"{args.output_dir}/sample-apache.log"

    print(f"Generating {args.json_count} JSON log lines...")
    json_stats = generate_json_logs(json_path, args.json_count, args.seed)
    print(f"  -> {json_path} ({json_stats['total_lines']} lines)")
    print(f"  Level distribution: {json_stats['level_counts']}")

    print(f"Generating {args.apache_count} Apache log lines...")
    apache_stats = generate_apache_logs(
        apache_path, args.apache_count, args.seed
    )
    print(f"  -> {apache_path} ({apache_stats['total_lines']} lines)")
    print(f"  Method distribution: {apache_stats['method_counts']}")

    print("Done!")


if __name__ == "__main__":
    main()
