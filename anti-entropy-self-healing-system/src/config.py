import os


class Config:
    NODE_HOST = os.getenv("NODE_HOST", "0.0.0.0")
    NODE_PORT = int(os.getenv("NODE_PORT", "8001"))
    COORDINATOR_HOST = os.getenv("COORDINATOR_HOST", "0.0.0.0")
    COORDINATOR_PORT = int(os.getenv("COORDINATOR_PORT", "5000"))
    SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "30"))
    NODE_URLS = os.getenv("NODE_URLS", "http://node-a:8001,http://node-b:8001,http://node-c:8001").split(",")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
