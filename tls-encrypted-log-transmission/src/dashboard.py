"""HTTP stats dashboard for the TLS log server."""

import json
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

logger = logging.getLogger(__name__)


class DashboardHandler(BaseHTTPRequestHandler):
    """Handles HTTP requests for the stats dashboard."""

    metrics = None

    def do_GET(self):
        if self.path == "/api/stats":
            self._serve_stats()
        elif self.path == "/":
            self._serve_html()
        else:
            self.send_error(404)

    def _serve_stats(self):
        stats = self.metrics.snapshot() if self.metrics else {}
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(stats).encode("utf-8"))

    def _serve_html(self):
        html = """<!DOCTYPE html>
<html>
<head>
    <title>TLS Log Server Dashboard</title>
    <style>
        body { font-family: monospace; background: #1a1a2e; color: #e0e0e0; padding: 20px; }
        h1 { color: #00d4ff; }
        .stats { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 15px; }
        .stat { background: #16213e; border: 1px solid #0f3460; border-radius: 8px; padding: 15px; }
        .stat .label { color: #a0a0a0; font-size: 12px; text-transform: uppercase; }
        .stat .value { color: #00d4ff; font-size: 24px; font-weight: bold; margin-top: 5px; }
        .footer { margin-top: 20px; color: #666; font-size: 12px; }
    </style>
</head>
<body>
    <h1>TLS Log Server</h1>
    <div class="stats" id="stats"></div>
    <div class="footer">Refreshes every 2 seconds</div>
    <script>
        const labels = {
            logs_received: "Logs Received",
            bytes_compressed: "Bytes (Compressed)",
            bytes_decompressed: "Bytes (Decompressed)",
            compression_ratio: "Compression Ratio",
            total_connections: "Total Connections",
            active_connections: "Active Connections",
            elapsed_seconds: "Uptime (seconds)",
            throughput_logs_per_sec: "Throughput (logs/sec)"
        };
        function update() {
            fetch("/api/stats")
                .then(r => r.json())
                .then(data => {
                    const el = document.getElementById("stats");
                    el.innerHTML = "";
                    for (const [key, val] of Object.entries(data)) {
                        const label = labels[key] || key;
                        const formatted = typeof val === "number" && !Number.isInteger(val) ? val.toFixed(2) : val;
                        el.innerHTML += '<div class="stat"><div class="label">' + label + '</div><div class="value">' + formatted + '</div></div>';
                    }
                })
                .catch(() => {});
        }
        update();
        setInterval(update, 2000);
    </script>
</body>
</html>"""
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def log_message(self, format, *args):
        pass


class DashboardServer:
    """Runs the dashboard HTTP server in a daemon thread."""

    def __init__(self, port: int = 8080, metrics=None):
        self._port = port
        DashboardHandler.metrics = metrics
        self._httpd = None
        self._thread = None

    def start(self):
        self._httpd = HTTPServer(("0.0.0.0", self._port), DashboardHandler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()
        logger.info("Dashboard started on http://0.0.0.0:%d", self._port)
        print(f"[SERVER] Dashboard available at http://0.0.0.0:{self._port}")

    def stop(self):
        if self._httpd:
            self._httpd.shutdown()
