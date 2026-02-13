"""Shared data pools used by all log formatters."""

IP_POOL = [
    "192.168.1.1", "10.0.0.42", "172.16.0.5", "203.0.113.7",
    "198.51.100.23", "192.0.2.88", "10.10.10.10", "172.31.255.1",
]

PATH_POOL = [
    "/", "/index.html", "/api/users", "/api/orders", "/api/products",
    "/api/health", "/login", "/logout", "/dashboard", "/static/app.js",
    "/images/logo.png", "/api/search?q=test", "/docs", "/api/v2/items",
]

METHOD_POOL = ["GET", "POST", "PUT", "DELETE", "PATCH"]
METHOD_WEIGHTS = [60, 15, 10, 10, 5]

STATUS_POOL = [200, 200, 200, 201, 204, 301, 302, 400, 401, 403, 404, 404, 500, 502, 503]

USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1",
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "curl/7.68.0",
    "python-requests/2.31.0",
]

REFERER_POOL = [
    "-",
    "https://google.com/",
    "https://example.com/page",
    "https://github.com/",
    "-",
]

SYSLOG_HOSTS = ["webserver01", "appserver02", "dbhost03", "cache01", "worker05"]
SYSLOG_TAGS = ["sshd", "cron", "kernel", "app", "nginx", "systemd"]

SERVICE_POOL = ["user-api", "order-service", "payment-gateway", "auth-service", "search-api"]
MESSAGE_POOL = [
    "Request processed successfully",
    "Database query completed",
    "Cache miss, fetching from origin",
    "User authentication successful",
    "Rate limit check passed",
    "Connection pool exhausted",
    "Timeout waiting for upstream",
    "Invalid request payload",
    "Permission denied for resource",
    "Health check passed",
]
