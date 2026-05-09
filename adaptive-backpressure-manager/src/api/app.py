from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI(title="Adaptive Backpressure Manager")

    @app.get("/system/health")
    def health() -> dict:
        return {"status": "ok"}

    return app
