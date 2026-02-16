"""Tests for the error tracker."""

from src.error_tracker import ErrorTracker


class TestErrorTracker:
    def test_add_and_get(self):
        tracker = ErrorTracker(max_size=100)
        tracker.add({"message": "err1"})
        tracker.add({"message": "err2"})

        recent = tracker.get_recent(10)
        assert len(recent) == 2
        assert recent[0]["message"] == "err1"
        assert recent[1]["message"] == "err2"

    def test_evicts_oldest_at_capacity(self):
        tracker = ErrorTracker(max_size=3)
        for i in range(5):
            tracker.add({"seq": i})

        recent = tracker.get_recent(10)
        assert len(recent) == 3
        assert recent[0]["seq"] == 2
        assert recent[1]["seq"] == 3
        assert recent[2]["seq"] == 4

    def test_get_recent_limits_count(self):
        tracker = ErrorTracker(max_size=100)
        for i in range(10):
            tracker.add({"seq": i})

        recent = tracker.get_recent(3)
        assert len(recent) == 3
        assert recent[0]["seq"] == 7

    def test_count_property(self):
        tracker = ErrorTracker(max_size=100)
        assert tracker.count == 0
        tracker.add({"msg": "x"})
        assert tracker.count == 1

    def test_empty_tracker(self):
        tracker = ErrorTracker()
        assert tracker.get_recent(10) == []
        assert tracker.count == 0
