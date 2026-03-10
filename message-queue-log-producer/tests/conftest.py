import pytest


@pytest.fixture
def config_file(tmp_path):
    """Create a temporary config.yaml for testing."""
    config_content = """
rabbitmq:
  host: localhost
  port: 5672
  user: guest
  password: guest
  heartbeat: 600
  blocked_connection_timeout: 300

exchange:
  name: logs
  type: topic
  durable: true

queue:
  name: log_queue
  durable: true
  routing_key: "log.#"

dead_letter:
  exchange: logs_dlx
  queue: log_queue_dlq

batch:
  max_size: 100
  flush_interval: 2.0

circuit_breaker:
  failure_threshold: 5
  recovery_timeout: 30

queue_maxsize: 10000
http_port: 8080
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_content)
    return str(config_path)


@pytest.fixture
def config(config_file):
    """Create a Config instance from temp file."""
    from src.config import Config

    return Config(config_path=config_file)


@pytest.fixture
def app(config):
    """Create Flask test app."""
    from src.app import create_app

    application = create_app(config=config)
    application.config["TESTING"] = True
    return application


@pytest.fixture
def client(app):
    """Create Flask test client."""
    return app.test_client()
