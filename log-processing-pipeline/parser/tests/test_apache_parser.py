"""Tests for the Apache log parser."""

import unittest

from parser.src.apache_parser import parse_apache_line


class TestApacheParser(unittest.TestCase):
    def test_valid_line(self):
        line = '192.168.1.1 - - [13/Feb/2026:06:50:53 +0000] "GET /api/users HTTP/1.1" 200 1234'
        result = parse_apache_line(line)
        self.assertIsNotNone(result)
        self.assertEqual(result["remote_host"], "192.168.1.1")
        self.assertEqual(result["method"], "GET")
        self.assertEqual(result["path"], "/api/users")
        self.assertEqual(result["protocol"], "HTTP/1.1")
        self.assertEqual(result["status_code"], 200)
        self.assertEqual(result["body_bytes"], 1234)
        self.assertEqual(result["level"], "INFO")

    def test_level_derivation(self):
        cases = [
            (200, "INFO"), (201, "INFO"), (301, "INFO"), (302, "INFO"),
            (400, "WARNING"), (401, "WARNING"), (404, "WARNING"),
            (500, "ERROR"), (502, "ERROR"), (503, "ERROR"),
        ]
        for status, expected_level in cases:
            line = f'10.0.0.1 - - [01/Jan/2026:00:00:00 +0000] "GET / HTTP/1.1" {status} 100'
            result = parse_apache_line(line)
            self.assertEqual(result["level"], expected_level,
                             f"Status {status} should give level {expected_level}")

    def test_invalid_line_returns_none(self):
        self.assertIsNone(parse_apache_line("not a log line"))
        self.assertIsNone(parse_apache_line(""))
        self.assertIsNone(parse_apache_line("just random text"))

    def test_dash_size(self):
        line = '10.0.0.1 - - [01/Jan/2026:00:00:00 +0000] "GET / HTTP/1.1" 200 -'
        result = parse_apache_line(line)
        self.assertIsNotNone(result)
        self.assertEqual(result["body_bytes"], 0)

    def test_raw_field_preserved(self):
        line = '10.0.0.1 - - [01/Jan/2026:00:00:00 +0000] "GET / HTTP/1.1" 200 100'
        result = parse_apache_line(line)
        self.assertEqual(result["raw"], line)


if __name__ == "__main__":
    unittest.main()
