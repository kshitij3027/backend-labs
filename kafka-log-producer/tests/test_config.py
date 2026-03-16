"""Tests for src.config — YAML loading, env overrides, confluent-kafka key mapping."""

from src.config import Config


class TestConfigLoadsYaml:
    """Verify that default YAML values are surfaced correctly."""

    def test_config_loads_yaml(self, config: Config) -> None:
        assert config.bootstrap_servers == "localhost:9092"
        assert config.prometheus_port == 8000
        assert config.dashboard_port == 8080
        assert config.ws_interval == 2
        assert config.fallback_path == "/tmp/kafka_fallback.jsonl"


class TestConfigEnvOverride:
    """Environment variables must override YAML defaults."""

    def test_config_env_override(self, tmp_path, monkeypatch) -> None:
        # Write a minimal YAML so the loader doesn't fail
        yaml_content = (
            "kafka:\n"
            '  bootstrap_servers: "original:9092"\n'
            "  acks: '1'\n"
            "  retries: 3\n"
            "  batch_size: 100\n"
            "  linger_ms: 1\n"
            '  compression_type: "none"\n'
            "  enable_idempotence: false\n"
            "prometheus:\n"
            "  port: 8000\n"
            "dashboard:\n"
            "  port: 8080\n"
            "  ws_interval: 2\n"
            "fallback:\n"
            '  storage_path: "/tmp/fb.jsonl"\n'
        )
        cfg_file = tmp_path / "cfg.yaml"
        cfg_file.write_text(yaml_content)

        monkeypatch.setenv("BOOTSTRAP_SERVERS", "kafka-cluster:9093")
        monkeypatch.setenv("KAFKA_BATCH_SIZE", "32768")
        monkeypatch.setenv("PROMETHEUS_PORT", "9090")

        cfg = Config(config_path=str(cfg_file))

        assert cfg.bootstrap_servers == "kafka-cluster:9093"
        assert cfg.kafka_config["batch.size"] == 32768
        assert cfg.prometheus_port == 9090


class TestKafkaConfigFormat:
    """kafka_config must use confluent-kafka style dotted keys."""

    def test_kafka_config_format(self, config: Config) -> None:
        kc = config.kafka_config
        assert "bootstrap.servers" in kc
        assert "batch.size" in kc
        assert "linger.ms" in kc
        assert "compression.type" in kc
        assert "enable.idempotence" in kc
        assert kc["bootstrap.servers"] == "localhost:9092"
        assert kc["acks"] == "all"
