"""Entry point for the Kafka Log Streaming Dashboard."""

import uvicorn

from src.config import load_config
from src.dashboard import create_app

settings = load_config()
app = create_app(settings)

if __name__ == "__main__":
    uvicorn.run(app, host=settings.dashboard_host, port=settings.dashboard_port)
