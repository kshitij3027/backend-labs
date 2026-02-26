"""Streaming pipeline for log processing."""
import os
from typing import Generator, Optional, TextIO
from src.detection import FormatDetectionEngine
from src.validators import validate_parsed_log
from src.formatters import get_formatter
from src.metrics import ProcessingMetrics
from src.models import ParsedLog


CHUNK_SIZE = 8192  # 8KB chunked reads


def stream_lines(filepath: str) -> Generator[str, None, None]:
    """
    Stream lines from a file using chunked reads.

    Yields one line at a time, memory-efficient for large files.
    """
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        remainder = ""
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                if remainder:
                    yield remainder
                break
            text = remainder + chunk
            lines = text.split("\n")
            remainder = lines[-1]
            for line in lines[:-1]:
                yield line


def process_file(
    filepath: str,
    output_format: str = "json",
    engine: Optional[FormatDetectionEngine] = None,
) -> Generator[tuple, None, None]:
    """
    Process a log file through the full pipeline.

    Yields (formatted_output, parsed_log) tuples for each successfully parsed line.
    Also yields a final metrics summary as ("__metrics__", metrics_dict).

    Pipeline: stream -> detect -> parse -> validate -> format
    """
    if engine is None:
        engine = FormatDetectionEngine()

    formatter = get_formatter(output_format)
    metrics = ProcessingMetrics()

    for line in stream_lines(filepath):
        line = line.strip()
        if not line:
            metrics.record_skip()
            continue

        parsed = engine.parse_line(line)
        if parsed is None:
            metrics.record_failure()
            continue

        is_valid, errors = validate_parsed_log(parsed)
        if not is_valid:
            metrics.record_failure()
            continue

        formatted = formatter(parsed)
        metrics.record_success(parsed.source_format)
        yield (formatted, parsed)

    metrics.finish()
    yield ("__metrics__", metrics.to_dict())
