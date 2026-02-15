"""Tests for file_reader module."""

import os
import threading
import time

from src.file_reader import read_batch, FileTailer


class TestReadBatch:
    def test_reads_all_lines(self, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("line one\nline two\nline three\n")
        lines = read_batch(str(f))
        assert lines == ["line one", "line two", "line three"]

    def test_skips_empty_lines(self, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("line one\n\n  \nline two\n")
        lines = read_batch(str(f))
        assert lines == ["line one", "line two"]

    def test_strips_whitespace(self, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("  padded  \n\tindented\t\n")
        lines = read_batch(str(f))
        assert lines == ["padded", "indented"]

    def test_empty_file(self, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("")
        lines = read_batch(str(f))
        assert lines == []


class TestFileTailer:
    def test_detects_appended_lines(self, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("existing line\n")

        received = []
        shutdown = threading.Event()
        tailer = FileTailer(str(f), shutdown, callback=received.append, poll_interval=0.05)

        t = threading.Thread(target=tailer.run, daemon=True)
        t.start()

        # Give tailer time to open and seek to end
        time.sleep(0.15)

        # Append new lines
        with open(str(f), "a") as fh:
            fh.write("new line 1\n")
            fh.write("new line 2\n")
            fh.flush()

        time.sleep(0.3)
        shutdown.set()
        t.join(timeout=2)

        assert "new line 1" in received
        assert "new line 2" in received
        # Should NOT include the existing line (tailer seeks to end)
        assert "existing line" not in received

    def test_file_not_exists_initially(self, tmp_path):
        f = tmp_path / "delayed.log"
        received = []
        shutdown = threading.Event()
        tailer = FileTailer(str(f), shutdown, callback=received.append, poll_interval=0.05)

        t = threading.Thread(target=tailer.run, daemon=True)
        t.start()

        time.sleep(0.2)
        # Create file empty, let tailer open and seek to end
        f.write_text("")

        time.sleep(0.2)
        # Now append a new line
        with open(str(f), "a") as fh:
            fh.write("appeared!\n")
            fh.flush()

        time.sleep(0.3)
        shutdown.set()
        t.join(timeout=2)

        assert "appeared!" in received

    def test_truncation_handling(self, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("original\n")

        received = []
        shutdown = threading.Event()
        tailer = FileTailer(str(f), shutdown, callback=received.append, poll_interval=0.05)

        t = threading.Thread(target=tailer.run, daemon=True)
        t.start()

        time.sleep(0.15)

        # Append a line
        with open(str(f), "a") as fh:
            fh.write("before truncation\n")
            fh.flush()

        time.sleep(0.2)

        # Truncate and write new content
        with open(str(f), "w") as fh:
            fh.write("after truncation\n")
            fh.flush()

        time.sleep(0.3)
        shutdown.set()
        t.join(timeout=2)

        assert "before truncation" in received
        assert "after truncation" in received

    def test_shutdown_responsiveness(self, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("")

        shutdown = threading.Event()
        tailer = FileTailer(str(f), shutdown, poll_interval=0.05)

        t = threading.Thread(target=tailer.run, daemon=True)
        t.start()

        time.sleep(0.1)
        shutdown.set()
        t.join(timeout=1)

        assert not t.is_alive()
