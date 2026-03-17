"""Log message models for Kafka consumer."""
import json
import time
from pydantic import BaseModel, Field

class BaseLogMessage(BaseModel):
    """Common fields for all log messages."""
    timestamp: float = Field(default_factory=time.time)
    log_type: str = ""
    topic: str = ""

class WebAccessLog(BaseLogMessage):
    """Web server access log entry."""
    log_type: str = "web_access"
    endpoint: str = ""
    method: str = "GET"
    status_code: int = 200
    response_time_ms: float = 0.0
    source_ip: str = ""
    geo: str = "internal"

class AppLog(BaseLogMessage):
    """Application log entry."""
    log_type: str = "app_log"
    service: str = ""
    component: str = ""
    level: str = "INFO"
    message: str = ""

class ErrorLog(BaseLogMessage):
    """Error log entry."""
    log_type: str = "error_log"
    error_type: str = ""
    stack_trace: str = ""
    endpoint: str = ""
    severity: str = "ERROR"
    message: str = ""

# Union type for convenience
LogMessage = WebAccessLog | AppLog | ErrorLog

def parse_log_message(value: bytes, topic: str = "") -> LogMessage | None:
    """Parse a Kafka message value into the appropriate log model.

    Returns None if parsing fails.
    """
    try:
        data = json.loads(value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    if topic:
        data["topic"] = topic

    log_type = data.get("log_type", "")

    # Route to correct model based on log_type or topic
    if log_type == "web_access" or topic == "web-logs":
        return WebAccessLog(**data)
    elif log_type == "error_log" or topic == "error-logs":
        return ErrorLog(**data)
    elif log_type == "app_log" or topic == "app-logs":
        return AppLog(**data)

    # Fallback: try to infer from fields
    if "status_code" in data or "response_time_ms" in data:
        return WebAccessLog(**data)
    elif "stack_trace" in data or "severity" in data:
        return ErrorLog(**data)
    else:
        return AppLog(**data)

def log_to_json(log: LogMessage) -> str:
    """Serialize a log message to JSON string."""
    return log.model_dump_json()
