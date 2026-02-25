"""Flask application factory for Schema Registry Service."""
import os
from flask import Flask, jsonify

from src.storage import FileStorage


def create_app(storage_path=None):
    """Create and configure the Flask application.

    Args:
        storage_path: Optional path for the storage file. Defaults to data/registry.json.
    """
    template_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates"
    )
    app = Flask(__name__, template_folder=template_dir)

    path = storage_path or os.environ.get("STORAGE_PATH", "data/registry.json")
    storage = FileStorage(path)

    @app.route("/health")
    def health():
        state = storage.get_state()
        schema_count = len(state.get("schemas", {}))
        subject_count = len(state.get("subjects", {}))
        return jsonify({
            "status": "healthy",
            "schema_count": schema_count,
            "subject_count": subject_count,
        })

    # Store references on app for use by other modules/phases
    app.storage = storage

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=8080, debug=False)
