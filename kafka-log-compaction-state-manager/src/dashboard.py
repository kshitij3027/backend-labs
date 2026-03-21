"""Flask dashboard for the Kafka log compaction state manager."""

import json
import logging

from flask import Flask, render_template, jsonify

from src.config import Settings
from src.consumer import StateConsumer
from src.monitor import CompactionMonitor

logger = logging.getLogger(__name__)


def create_app(
    config: Settings, consumer: StateConsumer, monitor: CompactionMonitor
) -> Flask:
    """Create and configure the Flask dashboard application."""
    app = Flask(__name__, template_folder="../templates")

    app.config["APP_CONFIG"] = config
    app.config["CONSUMER"] = consumer
    app.config["MONITOR"] = monitor

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/health")
    def health():
        return jsonify({"status": "healthy", "service": "kafka-log-compaction-state-manager"}), 200

    @app.route("/api/stats")
    def stats():
        consumer_stats = consumer.get_stats()

        compaction_metrics = {}
        try:
            compaction_metrics = monitor.get_compaction_metrics()
        except Exception:
            logger.warning("Failed to retrieve compaction metrics", exc_info=True)

        topic_config = {}
        try:
            topic_config = monitor.get_topic_config()
        except Exception:
            logger.warning("Failed to retrieve topic config", exc_info=True)

        return jsonify({
            "active_profiles": consumer_stats.get("active_profiles", 0),
            "total_consumed": consumer_stats.get("total_consumed", 0),
            "tombstones_processed": consumer_stats.get("tombstones_processed", 0),
            "updates_by_type": consumer_stats.get("updates_by_type", {}),
            "compaction_metrics": {
                "total_messages": compaction_metrics.get("total_messages", 0),
                "unique_keys": compaction_metrics.get("unique_keys", 0),
                "compaction_ratio": compaction_metrics.get("compaction_ratio", 0.0),
                "estimated_storage_saved_bytes": compaction_metrics.get("estimated_storage_saved_bytes", 0),
                "messages_per_second": compaction_metrics.get("messages_per_second", 0.0),
            },
            "topic_config": topic_config,
        })

    @app.route("/api/profiles")
    def profiles():
        active = consumer.get_active_profiles()
        return jsonify([profile.model_dump() for profile in active.values()])

    @app.route("/api/profiles/<user_id>")
    def profile_by_id(user_id: str):
        active = consumer.get_active_profiles()
        if user_id not in active:
            return jsonify({"error": "Profile not found"}), 404
        return jsonify(active[user_id].model_dump())

    return app
