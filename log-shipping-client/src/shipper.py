"""Basic single-threaded log shipper."""

import logging
import threading

from src.compressor import compress_payload
from src.config import Config
from src.file_reader import read_batch, FileTailer
from src.formatter import parse_log_line, format_ndjson
from src.tcp_client import TCPClient

logger = logging.getLogger(__name__)


class LogShipper:
    """Reads log lines from a file, formats as NDJSON, and ships over TCP."""

    def __init__(self, config: Config, shutdown_event: threading.Event):
        self._config = config
        self._shutdown = shutdown_event
        self._client = TCPClient(config.server_host, config.server_port, shutdown_event)
        self._sent = 0
        self._failed = 0

    @property
    def sent(self) -> int:
        return self._sent

    @property
    def failed(self) -> int:
        return self._failed

    def run(self):
        """Connect and ship logs based on mode (batch or continuous)."""
        if not self._client.connect_with_backoff(max_attempts=5):
            logger.error("Could not connect to server, aborting")
            return

        try:
            if self._config.batch_mode:
                self._run_batch()
            else:
                self._run_continuous()
        finally:
            self._client.close()
            logger.info("Shipper finished: sent=%d, failed=%d", self._sent, self._failed)

    def _run_batch(self):
        """Read all lines from file and send them in batches."""
        lines = read_batch(self._config.log_file)
        batch: list[bytes] = []
        for line in lines:
            if self._shutdown.is_set():
                break
            entry = parse_log_line(line)
            if entry is None:
                logger.debug("Skipping unparseable line: %s", line[:100])
                continue
            batch.append(format_ndjson(entry))
            if len(batch) >= self._config.batch_size:
                self._flush_batch(batch)
                batch = []
        if batch:
            self._flush_batch(batch)

    def _run_continuous(self):
        """Tail the file and send new lines in batches."""
        self._batch_buffer: list[bytes] = []
        tailer = FileTailer(
            self._config.log_file,
            self._shutdown,
            callback=self._buffer_line,
            poll_interval=self._config.poll_interval,
        )
        tailer.run()
        if self._batch_buffer:
            self._flush_batch(self._batch_buffer)
            self._batch_buffer = []

    def _buffer_line(self, raw: str):
        """Parse and buffer a line, flushing when batch is full."""
        entry = parse_log_line(raw)
        if entry is None:
            logger.debug("Skipping unparseable line: %s", raw[:100])
            return
        self._batch_buffer.append(format_ndjson(entry))
        if len(self._batch_buffer) >= self._config.batch_size:
            self._flush_batch(self._batch_buffer)
            self._batch_buffer = []

    def _flush_batch(self, batch: list[bytes]):
        """Concatenate batch, optionally compress, send, and read acks."""
        if not batch:
            return
        payload = b"".join(batch)
        if self._config.compress:
            payload = compress_payload(payload)
        if not self._client.send(payload):
            self._failed += len(batch)
            return
        for _ in range(len(batch)):
            result = self._client.recv_line()
            if result and result.get("status") == "ok":
                self._sent += 1
            else:
                self._failed += 1
