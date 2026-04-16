"""Persist and restore the inverted index using orjson + gzip."""

import gzip
import os
import tempfile

import orjson

from backend.index import InvertedIndex
from backend.models import DocumentMeta


class IndexPersistence:
    """Persist and restore the inverted index using orjson + gzip."""

    def __init__(self, storage_dir: str = "./storage"):
        self._storage_dir = storage_dir
        self._index_file = os.path.join(storage_dir, "index.json.gz")

    def save(self, index: InvertedIndex) -> None:
        """Serialize the index to disk with gzip compression.

        Uses atomic write (write to temp file, then rename) to prevent
        corruption.
        """
        os.makedirs(self._storage_dir, exist_ok=True)

        # Serialize all index data
        data = {
            "postings": {
                term: [(doc_id, positions) for doc_id, positions in posting_list]
                for term, posting_list in index._postings.items()
            },
            "documents": {
                str(doc_id): {
                    "doc_id": doc.doc_id,
                    "message": doc.message,
                    "timestamp": doc.timestamp,
                    "service": doc.service,
                    "level": doc.level,
                    "term_count": doc.term_count,
                }
                for doc_id, doc in index._documents.items()
            },
            "term_frequencies": index._term_frequencies,
            "next_doc_id": index._next_doc_id,
        }

        serialized = orjson.dumps(data)
        compressed = gzip.compress(serialized)

        # Atomic write: write to temp, then rename
        fd, tmp_path = tempfile.mkstemp(dir=self._storage_dir, suffix=".tmp")
        try:
            os.write(fd, compressed)
            os.close(fd)
            os.replace(tmp_path, self._index_file)
        except Exception:
            try:
                os.close(fd)
            except OSError:
                pass
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def load(self, index: InvertedIndex) -> bool:
        """Load a persisted index from disk.

        Returns True if loaded, False if file not found.
        """
        if not os.path.exists(self._index_file):
            return False

        with open(self._index_file, "rb") as f:
            compressed = f.read()

        decompressed = gzip.decompress(compressed)
        data = orjson.loads(decompressed)

        # Restore postings
        index._postings = {
            term: [(entry[0], entry[1]) for entry in posting_list]
            for term, posting_list in data["postings"].items()
        }

        # Restore documents
        index._documents = {
            int(doc_id): DocumentMeta(**doc_data)
            for doc_id, doc_data in data["documents"].items()
        }

        # Restore term frequencies and counter
        index._term_frequencies = data["term_frequencies"]
        index._next_doc_id = data["next_doc_id"]

        return True

    def get_file_size(self) -> int:
        """Return the size of the persisted index file in bytes."""
        if os.path.exists(self._index_file):
            return os.path.getsize(self._index_file)
        return 0

    def exists(self) -> bool:
        """Check if a persisted index exists on disk."""
        return os.path.exists(self._index_file)
