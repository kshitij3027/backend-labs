"""Tests for the Apache log formatter."""

import re
import unittest

from generator.src.apache_formatter import generate_apache_line

_APACHE_RE = re.compile(
    r'^(?P<host>\S+) \S+ \S+ '
    r'\[(?P<time>[^\]]+)\] '
    r'"(?P<request>[^"]*)" '
    r'(?P<status>\d{3}|-) '
    r'(?P<size>\d+|-)$'
)


class TestApacheFormatter(unittest.TestCase):
    def test_generated_lines_match_regex(self):
        for _ in range(200):
            line = generate_apache_line()
            self.assertRegex(line, _APACHE_RE, f"Line didn't match: {line}")

    def test_status_codes_are_valid(self):
        statuses = set()
        for _ in range(500):
            line = generate_apache_line()
            m = _APACHE_RE.match(line)
            self.assertIsNotNone(m)
            statuses.add(int(m.group("status")))
        # Should have a mix of 2xx, 3xx, 4xx, 5xx
        self.assertTrue(any(200 <= s < 300 for s in statuses))
        self.assertTrue(any(400 <= s < 500 for s in statuses))
        self.assertTrue(any(500 <= s < 600 for s in statuses))

    def test_request_has_method_path_protocol(self):
        for _ in range(50):
            line = generate_apache_line()
            m = _APACHE_RE.match(line)
            request = m.group("request")
            parts = request.split(" ")
            self.assertEqual(len(parts), 3)
            self.assertIn(parts[0], ["GET", "POST", "PUT", "DELETE", "PATCH"])
            self.assertEqual(parts[2], "HTTP/1.1")


if __name__ == "__main__":
    unittest.main()
