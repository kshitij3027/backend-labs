"""Flask web interface for querying the log storage."""

import json

from flask import Flask, request, render_template, jsonify

from query.src.searcher import search_by_pattern, search_by_index
from query.src.formatter import format_text


def create_app(storage_dir: str) -> Flask:
    app = Flask(__name__, template_folder="../templates")
    app.config["STORAGE_DIR"] = storage_dir

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/search")
    def search():
        storage = app.config["STORAGE_DIR"]
        pattern = request.args.get("pattern", "")
        index_type = request.args.get("index_type", "")
        index_value = request.args.get("index_value", "")
        lines = int(request.args.get("lines", 50))
        fmt = request.args.get("format", "text")

        results = _run_query(storage, pattern, index_type, index_value, lines)

        if fmt == "json":
            return jsonify(results=results, count=len(results))

        return render_template("index.html", results=results, count=len(results),
                               pattern=pattern, index_type=index_type,
                               index_value=index_value, lines=lines)

    @app.route("/api/search")
    def api_search():
        storage = app.config["STORAGE_DIR"]
        pattern = request.args.get("pattern", "")
        lines = int(request.args.get("lines", 50))
        results = list(search_by_pattern(storage, pattern, lines)) if pattern else []
        return jsonify(results=results, count=len(results))

    @app.route("/api/index")
    def api_index():
        storage = app.config["STORAGE_DIR"]
        idx_type = request.args.get("type", "")
        idx_value = request.args.get("value", "")
        lines = int(request.args.get("lines", 50))
        if idx_type and idx_value:
            results = list(search_by_index(storage, idx_type, idx_value, lines))
        else:
            results = []
        return jsonify(results=results, count=len(results))

    @app.route("/health")
    def health():
        return jsonify(status="ok")

    return app


def _run_query(storage: str, pattern: str, index_type: str,
               index_value: str, lines: int) -> list[dict]:
    if index_type and index_value:
        return list(search_by_index(storage, index_type, index_value, lines))
    elif pattern:
        return list(search_by_pattern(storage, pattern, lines))
    return []
