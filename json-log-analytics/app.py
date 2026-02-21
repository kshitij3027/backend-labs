import os
from flask import Flask, request, jsonify, render_template
from apscheduler.schedulers.background import BackgroundScheduler

from config import Config
from validator import LogValidator
from log_store import LogStore
from analytics import AnalyticsEngine
from alerting import AlertManager, ConsoleAlertHandler
from simulator import generate_log, generate_batch


def create_app(config=None):
    """Flask application factory."""
    app = Flask(__name__)

    # Initialize components
    if config is None:
        config = Config(os.environ.get("CONFIG_PATH", "config.yaml"))

    validator = LogValidator(config["schema"]["path"])
    store = LogStore(max_size=config["storage"]["max_logs"])
    analytics = AnalyticsEngine(max_buckets=config["analytics"]["max_buckets"])
    alert_manager = AlertManager(analytics, config={"alerting": config["alerting"]})
    alert_manager.add_handler(ConsoleAlertHandler())

    # Store components on app for access in tests
    app.config["components"] = {
        "config": config,
        "validator": validator,
        "store": store,
        "analytics": analytics,
        "alert_manager": alert_manager,
    }

    # APScheduler for periodic service-down checks
    scheduler = BackgroundScheduler()
    scheduler.add_job(alert_manager.check_service_down, "interval", seconds=30)
    scheduler.start()

    # Make sure scheduler shuts down with the app
    import atexit
    atexit.register(scheduler.shutdown)

    # --- Routes ---

    @app.route("/health")
    def health():
        return jsonify({
            "status": "healthy",
            "total_logs": store.total_count,
            "current_stored": store.current_size,
        })

    @app.route("/api/logs", methods=["POST"])
    def ingest_log():
        log_entry = request.get_json(force=True)

        is_valid, errors = validator.validate(log_entry)
        if not is_valid:
            return jsonify({"status": "invalid", "errors": errors}), 400

        store.add(log_entry)
        analytics.record(log_entry)

        # Check error rate and high volume on each ingest
        alert_manager.check_error_rate()
        alert_manager.check_high_volume()

        return jsonify({"status": "accepted"}), 201

    @app.route("/api/advanced-dashboard-data")
    def dashboard_data():
        summary = analytics.get_summary()
        return jsonify({
            "summary": summary,
            "time_series": analytics.get_time_series(minutes=30),
            "error_trends": analytics.get_error_trends(),
            "service_health": analytics.get_service_health(),
            "most_active_services": analytics.get_most_active_services(),
            "user_activity": analytics.get_user_activity(),
            "recent_logs": store.get_recent(20),
            "active_alerts": alert_manager.get_active_alerts(),
            "validation_stats": validator.get_stats(),
        })

    @app.route("/api/simulate-logs", methods=["POST"])
    def simulate_logs():
        data = request.get_json(force=True)
        count = min(data.get("count", 10), 1000)  # Cap at 1000
        logs = generate_batch(count=count, minutes_ago=5)

        accepted = 0
        for log in logs:
            is_valid, _ = validator.validate(log)
            if is_valid:
                store.add(log)
                analytics.record(log)
                accepted += 1

        return jsonify({"status": "simulated", "requested": count, "accepted": accepted})

    @app.route("/api/simulate-errors", methods=["POST"])
    def simulate_errors():
        data = request.get_json(force=True)
        count = min(data.get("count", 10), 500)
        error_rate = data.get("error_rate", 0.8)
        service = data.get("service", None)

        logs = generate_batch(count=count, error_rate=error_rate, service=service, minutes_ago=2)

        accepted = 0
        for log in logs:
            is_valid, _ = validator.validate(log)
            if is_valid:
                store.add(log)
                analytics.record(log)
                accepted += 1

        # Run alert checks after bulk error injection
        alert_manager.check_error_rate()
        alert_manager.check_high_volume()

        return jsonify({"status": "simulated", "requested": count, "accepted": accepted})

    @app.route("/api/time-series")
    def time_series():
        minutes = request.args.get("minutes", 30, type=int)
        return jsonify(analytics.get_time_series(minutes=minutes))

    @app.route("/api/validation-stats")
    def validation_stats():
        return jsonify(validator.get_stats())

    @app.route("/")
    def dashboard():
        return render_template("dashboard.html")

    return app


# For gunicorn: `gunicorn 'app:create_app()'`
if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=True)
