"""Auto-detects log format and parses a line.

Detection order (same as sibling log-parsing-service/src/parsers.py):
  1. Starts with '{' -> try JSON parser
  2. Starts with '<' + digit -> try syslog parser
  3. Try nginx parser (more specific — has referer/user_agent)
  4. Try apache parser (less specific)
  5. Return unparsed fallback
"""

from parser.src.json_parser import parse_json_line
from parser.src.syslog_parser import parse_syslog_line
from parser.src.nginx_parser import parse_nginx_line
from parser.src.apache_parser import parse_apache_line


def parse_line(line: str) -> dict | None:
    """Auto-detect format and parse a single log line."""
    if not line:
        return None

    # JSON detection
    if line.startswith("{"):
        result = parse_json_line(line)
        if result:
            return result

    # Syslog detection
    if line.startswith("<") and len(line) > 1 and line[1].isdigit():
        result = parse_syslog_line(line)
        if result:
            return result

    # Nginx (more specific, has referer/user_agent)
    result = parse_nginx_line(line)
    if result:
        return result

    # Apache (less specific)
    result = parse_apache_line(line)
    if result:
        return result

    # Fallback
    return {"raw": line, "parsed": False, "level": "UNKNOWN"}
