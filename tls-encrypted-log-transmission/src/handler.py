"""Per-client connection handler — receive compressed logs, send acks."""

import gzip
import json
import logging
import threading
from datetime import datetime, timezone

from src.protocol import encode_frame, decode_frame_header, recv_exact

logger = logging.getLogger(__name__)

_log_writer = None
_metrics = None


def set_log_writer(writer):
    """Set the module-level log writer for all handlers."""
    global _log_writer
    _log_writer = writer


def set_metrics(metrics):
    """Set the module-level metrics tracker for all handlers."""
    global _metrics
    _metrics = metrics


def handle_client(conn, addr, config, shutdown_event: threading.Event):
    """Handle a single TLS client connection.

    Protocol:
    - Receive: [4-byte BE length][gzip-compressed JSON log entry]
    - Send ack: [4-byte BE length][JSON {"status":"ok"}] (uncompressed)
    """
    client_id = f"{addr[0]}:{addr[1]}"
    logs_received = 0

    if _metrics:
        _metrics.record_connection()

    try:
        conn.settimeout(30.0)

        while not shutdown_event.is_set():
            try:
                header = recv_exact(conn, 4)
            except ConnectionError:
                break

            payload_len = decode_frame_header(header)
            compressed_data = recv_exact(conn, payload_len)

            raw_json = gzip.decompress(compressed_data)
            log_entry = json.loads(raw_json.decode("utf-8"))
            logs_received += 1

            if _metrics:
                _metrics.record_log(len(compressed_data), len(raw_json))

            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            level = log_entry.get("level", "?")
            message = log_entry.get("message", "")
            print(f"[{now}] [{level}] {message}")

            if _log_writer:
                _log_writer.write(log_entry)

            ack = json.dumps({"status": "ok"}).encode("utf-8")
            conn.sendall(encode_frame(ack))

    except Exception as e:
        logger.error("Error handling client %s: %s", client_id, e)
    finally:
        if _metrics:
            _metrics.record_disconnection()
        conn.close()
        logger.info("Client %s disconnected (%d logs received)", client_id, logs_received)
        print(f"[SERVER] Client {client_id} disconnected ({logs_received} logs received)")
