"""Realistic message pools per log level."""

import random

INFO_MESSAGES = [
    "User login successful",
    "Request processed successfully",
    "Database query completed",
    "Cache hit for session data",
    "Health check passed",
    "Configuration loaded",
    "Connection pool initialized",
    "Order placed successfully",
    "Email notification sent",
    "File upload completed",
    "Payment processed",
    "Inventory updated",
    "User profile retrieved",
    "Search query executed",
    "Session token refreshed",
]

WARNING_MESSAGES = [
    "High memory usage detected",
    "Slow database query detected",
    "Rate limit approaching threshold",
    "Deprecated API endpoint called",
    "Connection pool running low",
    "Retry attempt for failed request",
    "Cache miss for frequently accessed key",
    "Disk usage above 80%",
    "Response time exceeded threshold",
    "Authentication token expiring soon",
]

ERROR_MESSAGES = [
    "Database connection failed",
    "Unhandled exception in request handler",
    "Payment gateway timeout",
    "Failed to send email notification",
    "Disk write error",
    "Service dependency unavailable",
    "Authentication failed: invalid credentials",
    "Request payload too large",
    "Circuit breaker triggered",
    "Out of memory error",
]

DEBUG_MESSAGES = [
    "Entering function process_request",
    "Variable state: user_id=12345, active=True",
    "SQL query: SELECT * FROM users WHERE id=?",
    "HTTP headers: Content-Type=application/json",
    "Cache key generated: user_session_abc123",
    "Thread pool size: 4 active, 6 idle",
    "Garbage collection triggered",
    "Request body parsed in 2ms",
    "Middleware chain: auth -> validate -> process",
    "Environment: production, region: us-east-1",
]

_POOLS = {
    "INFO": INFO_MESSAGES,
    "WARNING": WARNING_MESSAGES,
    "ERROR": ERROR_MESSAGES,
    "DEBUG": DEBUG_MESSAGES,
}


def get_random_message(level: str) -> str:
    return random.choice(_POOLS[level])
