"""Tests for FallbackStorage — local JSONL file fallback."""

import json

from src.fallback_storage import FallbackStorage


class TestFallbackStorage:

    def test_write_creates_file(self, tmp_path):
        storage = FallbackStorage(storage_dir=str(tmp_path / "fb"))
        storage.write([{"msg": "hello"}])
        assert (tmp_path / "fb" / "fallback.jsonl").exists()

    def test_write_content(self, tmp_path):
        storage = FallbackStorage(storage_dir=str(tmp_path / "fb"))
        entries = [{"level": "INFO", "msg": "one"}, {"level": "ERROR", "msg": "two"}]
        storage.write(entries)

        lines = (tmp_path / "fb" / "fallback.jsonl").read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0]) == entries[0]
        assert json.loads(lines[1]) == entries[1]

    def test_append_works(self, tmp_path):
        storage = FallbackStorage(storage_dir=str(tmp_path / "fb"))
        batch1 = [{"id": 1}]
        batch2 = [{"id": 2}, {"id": 3}]
        storage.write(batch1)
        storage.write(batch2)

        lines = (tmp_path / "fb" / "fallback.jsonl").read_text().strip().split("\n")
        assert len(lines) == 3
        assert json.loads(lines[0]) == {"id": 1}
        assert json.loads(lines[1]) == {"id": 2}
        assert json.loads(lines[2]) == {"id": 3}

    def test_drain_reads_and_deletes(self, tmp_path):
        storage = FallbackStorage(storage_dir=str(tmp_path / "fb"))
        entries = [{"a": 1}, {"b": 2}, {"c": 3}]
        storage.write(entries)

        received = []
        def callback(chunk):
            received.extend(chunk)

        total = storage.drain(callback, chunk_size=100)
        assert total == 3
        assert received == entries
        assert not (tmp_path / "fb" / "fallback.jsonl").exists()

    def test_drain_chunks(self, tmp_path):
        storage = FallbackStorage(storage_dir=str(tmp_path / "fb"))
        entries = [{"i": i} for i in range(5)]
        storage.write(entries)

        call_count = 0
        chunk_sizes = []
        def callback(chunk):
            nonlocal call_count
            call_count += 1
            chunk_sizes.append(len(chunk))

        total = storage.drain(callback, chunk_size=2)
        assert total == 5
        assert call_count == 3
        assert chunk_sizes == [2, 2, 1]

    def test_drain_missing_file(self, tmp_path):
        storage = FallbackStorage(storage_dir=str(tmp_path / "fb"))
        # Don't write anything — file doesn't exist
        total = storage.drain(lambda chunk: None)
        assert total == 0

    def test_has_data_true(self, tmp_path):
        storage = FallbackStorage(storage_dir=str(tmp_path / "fb"))
        storage.write([{"x": 1}])
        assert storage.has_data() is True

    def test_has_data_false(self, tmp_path):
        storage = FallbackStorage(storage_dir=str(tmp_path / "fb"))
        assert storage.has_data() is False

    def test_has_data_after_drain(self, tmp_path):
        storage = FallbackStorage(storage_dir=str(tmp_path / "fb"))
        storage.write([{"x": 1}])
        storage.drain(lambda chunk: None)
        assert storage.has_data() is False
