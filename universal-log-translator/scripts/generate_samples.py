"""Generate sample log files in all supported formats.

Run inside Docker where proto-compiled code is available:
    python scripts/generate_samples.py
"""
import io
import json
import os

import fastavro


SAMPLE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sample_logs")


def generate_json_sample() -> None:
    """Generate sample_logs/sample.json."""
    data = {
        "timestamp": "2024-01-15T10:30:00",
        "level": "INFO",
        "message": "Application started successfully",
        "source": "app-server",
        "hostname": "web-01",
        "service": "api-gateway",
    }
    path = os.path.join(SAMPLE_DIR, "sample.json")
    with open(path, "w") as f:
        json.dump(data, f)
    print(f"Generated: {path}")


def generate_text_sample() -> None:
    """Generate sample_logs/sample.txt (RFC 5424 syslog)."""
    line = "<165>1 2024-01-15T10:30:00.000Z web-01 api-gateway 1234 - - Application started successfully"
    path = os.path.join(SAMPLE_DIR, "sample.txt")
    with open(path, "w") as f:
        f.write(line + "\n")
    print(f"Generated: {path}")


def generate_protobuf_sample() -> None:
    """Generate sample_logs/sample.pb."""
    from src.generated import log_entry_pb2

    entry = log_entry_pb2.LogEntry()
    entry.timestamp = "2024-01-15T10:30:00"
    entry.level = log_entry_pb2.LOG_LEVEL_INFO
    entry.message = "Application started successfully"
    entry.source = "app-server"
    entry.hostname = "web-01"
    entry.service = "api-gateway"

    path = os.path.join(SAMPLE_DIR, "sample.pb")
    with open(path, "wb") as f:
        f.write(entry.SerializeToString())
    print(f"Generated: {path}")


def generate_avro_sample() -> None:
    """Generate sample_logs/sample.avro (Avro OCF)."""
    schema = {
        "type": "record",
        "name": "LogEntry",
        "namespace": "com.logtranslator",
        "fields": [
            {"name": "timestamp", "type": "string"},
            {"name": "level", "type": "string"},
            {"name": "message", "type": "string"},
            {"name": "source", "type": ["null", "string"], "default": None},
            {"name": "hostname", "type": ["null", "string"], "default": None},
            {"name": "service", "type": ["null", "string"], "default": None},
            {
                "name": "metadata",
                "type": {"type": "map", "values": "string"},
                "default": {},
            },
        ],
    }
    parsed = fastavro.parse_schema(schema)
    record = {
        "timestamp": "2024-01-15T10:30:00",
        "level": "INFO",
        "message": "Application started successfully",
        "source": "app-server",
        "hostname": "web-01",
        "service": "api-gateway",
        "metadata": {},
    }

    path = os.path.join(SAMPLE_DIR, "sample.avro")
    with open(path, "wb") as f:
        fastavro.writer(f, parsed, [record])
    print(f"Generated: {path}")


def main() -> None:
    """Generate all sample files."""
    os.makedirs(SAMPLE_DIR, exist_ok=True)

    generate_json_sample()
    generate_text_sample()
    generate_protobuf_sample()
    generate_avro_sample()

    print(f"\nAll sample files generated in {SAMPLE_DIR}/")


if __name__ == "__main__":
    main()
