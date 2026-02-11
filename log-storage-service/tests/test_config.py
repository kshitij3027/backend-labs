"""Tests for the configuration module."""

import os
import unittest

from src.config import Config, load_config, _parse_bool


class TestParseBool(unittest.TestCase):
    def test_true_values(self):
        for val in ("true", "True", "TRUE", "1", "yes", "YES", " true "):
            self.assertTrue(_parse_bool(val), f"Expected True for {val!r}")

    def test_false_values(self):
        for val in ("false", "False", "0", "no", "NO", "", "anything"):
            self.assertFalse(_parse_bool(val), f"Expected False for {val!r}")


class TestConfigDefaults(unittest.TestCase):
    def test_default_values(self):
        cfg = Config()
        self.assertEqual(cfg.log_dir, "./logs")
        self.assertEqual(cfg.log_filename, "application.log")
        self.assertEqual(cfg.max_file_size_bytes, 10 * 1024 * 1024)
        self.assertEqual(cfg.rotation_interval_seconds, 3600)
        self.assertEqual(cfg.max_file_count, 10)
        self.assertEqual(cfg.max_age_days, 7)
        self.assertTrue(cfg.compression_enabled)

    def test_frozen(self):
        cfg = Config()
        with self.assertRaises(AttributeError):
            cfg.log_dir = "/tmp"


class TestLoadConfig(unittest.TestCase):
    def setUp(self):
        self._orig_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._orig_env)

    def test_defaults_without_env(self):
        for key in ("LOG_DIR", "LOG_FILENAME", "MAX_FILE_SIZE_BYTES",
                     "MAX_FILE_SIZE_MB", "ROTATION_INTERVAL_SECONDS",
                     "MAX_FILE_COUNT", "MAX_AGE_DAYS", "COMPRESSION_ENABLED"):
            os.environ.pop(key, None)
        cfg = load_config()
        self.assertEqual(cfg.log_dir, "./logs")
        self.assertEqual(cfg.max_file_size_bytes, 10 * 1024 * 1024)
        self.assertTrue(cfg.compression_enabled)

    def test_env_var_overrides(self):
        os.environ["LOG_DIR"] = "/var/log/app"
        os.environ["LOG_FILENAME"] = "custom.log"
        os.environ["ROTATION_INTERVAL_SECONDS"] = "120"
        os.environ["MAX_FILE_COUNT"] = "5"
        os.environ["MAX_AGE_DAYS"] = "3"
        os.environ["COMPRESSION_ENABLED"] = "false"
        os.environ["MAX_FILE_SIZE_MB"] = "20"
        cfg = load_config()
        self.assertEqual(cfg.log_dir, "/var/log/app")
        self.assertEqual(cfg.log_filename, "custom.log")
        self.assertEqual(cfg.max_file_size_bytes, 20 * 1024 * 1024)
        self.assertEqual(cfg.rotation_interval_seconds, 120)
        self.assertEqual(cfg.max_file_count, 5)
        self.assertEqual(cfg.max_age_days, 3)
        self.assertFalse(cfg.compression_enabled)

    def test_max_file_size_bytes_precedence(self):
        os.environ["MAX_FILE_SIZE_BYTES"] = "2048"
        os.environ["MAX_FILE_SIZE_MB"] = "50"
        cfg = load_config()
        self.assertEqual(cfg.max_file_size_bytes, 2048)

    def test_max_file_size_mb_only(self):
        os.environ.pop("MAX_FILE_SIZE_BYTES", None)
        os.environ["MAX_FILE_SIZE_MB"] = "5"
        cfg = load_config()
        self.assertEqual(cfg.max_file_size_bytes, 5 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
