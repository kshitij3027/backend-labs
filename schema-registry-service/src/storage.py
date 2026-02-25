"""Thread-safe JSON file storage with atomic writes."""
import json
import os
import tempfile
import threading

class FileStorage:
    def __init__(self, path="data/registry.json"):
        self._path = path
        self._lock = threading.Lock()
        self._state = self._load()

    def _load(self):
        if os.path.exists(self._path):
            with open(self._path, "r") as f:
                return json.load(f)
        return {"next_id": 1, "schemas": {}, "subjects": {}}

    def get_state(self):
        with self._lock:
            return self._state.copy()

    def set_state(self, state):
        with self._lock:
            self._state = state
            self._save()

    def _save(self):
        """Atomic write: write to temp file then os.replace()."""
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            dir=os.path.dirname(self._path) or ".",
            suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(self._state, f, indent=2)
            os.replace(tmp_path, self._path)
        except:
            os.unlink(tmp_path)
            raise
