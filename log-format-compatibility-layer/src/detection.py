"""Format detection engine for log lines."""
from typing import List, Optional, Tuple
from src.adapters import AdapterRegistry
from src.adapters.base import LogFormatAdapter
from src.adapters.json_adapter import JsonLogAdapter
from src.adapters.syslog_rfc5424 import SyslogRFC5424Adapter
from src.adapters.syslog_rfc3164 import SyslogRFC3164Adapter
from src.adapters.journald import JournaldAdapter
from src.models import ParsedLog


class FormatDetectionEngine:
    """Engine for detecting and parsing log formats.

    Detection order (cheapest first):
    JSON → RFC 5424 → RFC 3164 → Journald
    """

    def __init__(self):
        self.registry = AdapterRegistry()
        # Register adapters in detection order (cheapest first)
        self.registry.register(JsonLogAdapter())
        self.registry.register(SyslogRFC5424Adapter())
        self.registry.register(SyslogRFC3164Adapter())
        self.registry.register(JournaldAdapter())

    def detect_line(self, line: str) -> Optional[Tuple[str, float]]:
        """
        Detect the format of a single log line.

        Returns (format_name, confidence) or None if unrecognized.
        """
        result = self.registry.detect(line)
        if result is None:
            return None
        adapter, confidence = result
        return (adapter.format_name, confidence)

    def parse_line(self, line: str) -> Optional[ParsedLog]:
        """
        Detect and parse a single log line.

        Returns ParsedLog or None if unrecognized.
        """
        return self.registry.detect_and_parse(line)

    def detect_batch(self, lines: List[str], sample_size: int = 0) -> dict:
        """
        Detect formats in a batch of lines.

        If sample_size > 0, only sample that many lines for detection.
        Returns a dict with format distribution and stats.
        """
        if sample_size > 0 and sample_size < len(lines):
            # Sample evenly spaced lines
            step = len(lines) // sample_size
            sampled = [lines[i] for i in range(0, len(lines), step)][:sample_size]
        else:
            sampled = lines

        format_counts = {}
        total = 0
        detected = 0

        for line in sampled:
            line = line.strip()
            if not line:
                continue
            total += 1
            result = self.detect_line(line)
            if result:
                fmt, confidence = result
                if fmt not in format_counts:
                    format_counts[fmt] = {"count": 0, "total_confidence": 0.0}
                format_counts[fmt]["count"] += 1
                format_counts[fmt]["total_confidence"] += confidence
                detected += 1

        # Calculate averages
        for fmt in format_counts:
            count = format_counts[fmt]["count"]
            format_counts[fmt]["avg_confidence"] = (
                format_counts[fmt]["total_confidence"] / count if count > 0 else 0.0
            )

        return {
            "total_lines": total,
            "detected_lines": detected,
            "detection_rate": detected / total if total > 0 else 0.0,
            "formats": format_counts,
        }
