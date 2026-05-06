"""Entry point: ``python -m src`` launches the FastAPI app via uvicorn."""
import os
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "src.api.app:create_app",
        factory=True,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        log_level=os.getenv("UVICORN_LOG_LEVEL", "info").lower(),
    )
