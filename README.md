# Backend Labs

A collection of independent mini-projects built during a backend engineering learning journey.

## Projects

| Project | Description | Tech |
|---|---|---|
| [configurable-log-generator](./configurable-log-generator/) | Continuously generates realistic, timestamped application logs at a configurable rate with burst mode and state machine patterns | Python 3.12, Docker |
| [log-capture-service](./log-capture-service/) | Real-time log file watcher that parses text/JSON logs, applies regex filtering and tagging, and writes structured JSON batches | Python 3.12, watchdog, Docker |
| [log-parsing-service](./log-parsing-service/) | Transforms raw log lines (Apache, Nginx, JSON, Syslog) into normalized structured JSON with auto-format detection and aggregate statistics | Python 3.12, watchdog, Docker |
| [log-storage-service](./log-storage-service/) | File-based log storage engine with automatic rotation, compression, and purging based on configurable policies, plus a CLI log inspector | Python 3.12, Docker Compose |
| [log-query-cli](./log-query-cli/) | CLI tool that parses, filters, and searches log files using a memory-efficient generator pipeline with text/JSON/color output and statistics | Python 3.12, Docker |
