"""TLS client — connect, compress, send logs, receive acks."""

import gzip
import json
import logging
import socket
import ssl
import time

from src.config import ClientConfig
from src.tls_context import create_client_context_unverified
from src.protocol import encode_frame, decode_frame_header, recv_exact

logger = logging.getLogger(__name__)


class TLSLogClient:
    """Client that sends gzip-compressed log entries over TLS."""

    def __init__(self, config: ClientConfig):
        self._config = config
        self._ssl_ctx = create_client_context_unverified()
        self._sock = None
        self._total_raw = 0
        self._total_compressed = 0
        self._logs_sent = 0

    def connect(self):
        """Establish a TLS connection to the server."""
        raw_sock = socket.create_connection(
            (self._config.host, self._config.port), timeout=10
        )
        self._sock = self._ssl_ctx.wrap_socket(
            raw_sock, server_hostname=self._config.host
        )
        print(f"[CLIENT] TLS connection established — {self._sock.version()}")
        logger.info("Connected to %s:%d", self._config.host, self._config.port)

    def connect_with_retry(self):
        """Connect with exponential backoff retry."""
        delay = self._config.retry_base_delay
        for attempt in range(1, self._config.retry_attempts + 1):
            try:
                self.connect()
                return
            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                if attempt == self._config.retry_attempts:
                    raise
                print(f"[CLIENT] Connection attempt {attempt} failed: {e}")
                print(f"[CLIENT] Retrying in {delay:.1f}s...")
                time.sleep(delay)
                delay *= 2

    def send_log(self, log_entry: dict):
        """Compress and send a log entry, wait for ack."""
        raw = json.dumps(log_entry).encode("utf-8")
        compressed = gzip.compress(raw)

        self._total_raw += len(raw)
        self._total_compressed += len(compressed)
        self._logs_sent += 1

        frame = encode_frame(compressed)
        self._sock.sendall(frame)

        # Receive ack
        ack_header = recv_exact(self._sock, 4)
        ack_len = decode_frame_header(ack_header)
        ack_data = recv_exact(self._sock, ack_len)
        ack = json.loads(ack_data.decode("utf-8"))

        if ack.get("status") != "ok":
            raise RuntimeError(f"Unexpected ack: {ack}")

    def print_stats(self):
        """Print compression statistics."""
        if self._total_raw > 0:
            ratio = self._total_raw / self._total_compressed
            saved = (1 - self._total_compressed / self._total_raw) * 100
            print(f"\n[CLIENT] Compression Stats:")
            print(f"  Raw bytes:        {self._total_raw:,}")
            print(f"  Compressed bytes: {self._total_compressed:,}")
            print(f"  Ratio:            {ratio:.2f}x")
            print(f"  Space saved:      {saved:.1f}%")
            print(f"  Logs sent:        {self._logs_sent}")

    def close(self):
        """Close the TLS connection."""
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
