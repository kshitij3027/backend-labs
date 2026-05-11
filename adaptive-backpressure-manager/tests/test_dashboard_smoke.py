def test_dashboard_layout_contains_required_ids():
    from src.dashboard.app import build_app

    app = build_app(base_url="http://localhost:8000")
    rendered = str(app.layout)
    assert "state-banner" in rendered
    assert "pressure-chart" in rendered
    assert "queue-chart" in rendered
    assert "throttle-chart" in rendered
    assert "resource-gauges" in rendered
    assert "manual-submit" in rendered
    assert "lt-start" in rendered
    assert "lt-spike-now" in rendered


def test_dashboard_app_title():
    from src.dashboard.app import build_app

    app = build_app()
    assert app.title == "Adaptive Backpressure Manager"
