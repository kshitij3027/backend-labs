"""Configuration constants for the log format compatibility layer."""

# Syslog facility codes (RFC 5424)
FACILITY_MAP = {
    0: "kern",
    1: "user",
    2: "mail",
    3: "daemon",
    4: "auth",
    5: "syslog",
    6: "lpr",
    7: "news",
    8: "uucp",
    9: "cron",
    10: "authpriv",
    11: "ftp",
    12: "ntp",
    13: "security",
    14: "console",
    15: "solaris-cron",
    16: "local0",
    17: "local1",
    18: "local2",
    19: "local3",
    20: "local4",
    21: "local5",
    22: "local6",
    23: "local7",
}

# Syslog severity levels (RFC 5424)
SEVERITY_MAP = {
    0: "emergency",
    1: "alert",
    2: "critical",
    3: "error",
    4: "warning",
    5: "notice",
    6: "informational",
    7: "debug",
}

# Default output directory for processed logs
DEFAULT_OUTPUT_DIR = "output"

# Default directory for sample log files
DEFAULT_LOG_DIR = "logs/samples"

# Minimum confidence score to consider a parse result valid
CONFIDENCE_THRESHOLD = 0.5

# Confidence score above which a parse is considered highly reliable
HIGH_CONFIDENCE_THRESHOLD = 0.9
