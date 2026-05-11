import os

from src.dashboard.app import build_app


if __name__ == "__main__":
    base = os.environ.get("APP_BASE_URL", "http://localhost:8000")
    port = int(os.environ.get("PORT", "8050"))
    app = build_app(base_url=base)
    app.run(host="0.0.0.0", port=port, debug=False)
