"""Business metrics tracking: API version usage, payment funnel, auth patterns."""

import re
import threading
import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)


class BusinessMetricsTracker:
    def __init__(self, max_users=10000):
        self._lock = threading.Lock()
        self._api_versions = {}  # {"v1": count, "v2": count, "unversioned": count}
        self._funnel_stages = ["browse", "add_to_cart", "checkout", "payment", "confirmation"]
        self._funnel_users = OrderedDict()  # {user_id: last_stage} bounded
        self._funnel_counts = {stage: 0 for stage in self._funnel_stages}
        self._auth_success = 0
        self._auth_failure = 0
        self._max_users = max_users
        self._api_version_pattern = re.compile(r'/api/v(\d+)/')

    def track_api_version(self, path):
        """Extract and track API version from request path."""
        if not path:
            return
        with self._lock:
            match = self._api_version_pattern.search(path)
            if match:
                version = f"v{match.group(1)}"
            else:
                version = "unversioned"
            self._api_versions[version] = self._api_versions.get(version, 0) + 1

    def track_payment_funnel(self, path, action):
        """Track user progression through payment funnel stages."""
        stage = self._infer_funnel_stage(path, action)
        if not stage:
            return
        with self._lock:
            self._funnel_counts[stage] = self._funnel_counts.get(stage, 0) + 1
            # Evict oldest if over limit
            if len(self._funnel_users) >= self._max_users:
                self._funnel_users.popitem(last=False)

    def _infer_funnel_stage(self, path, action):
        """Map path/action to funnel stage."""
        if not path and not action:
            return None
        path = (path or "").lower()
        action = (action or "").lower()

        if action == "purchase" or "confirmation" in path:
            return "confirmation"
        elif "payment" in path or "pay" in action:
            return "payment"
        elif "checkout" in path:
            return "checkout"
        elif "cart" in path or action == "add_to_cart":
            return "add_to_cart"
        elif action == "page_view" or path:
            return "browse"
        return None

    def track_auth_event(self, action, success=True):
        """Track authentication success/failure."""
        if action not in ("login", "signup", "logout"):
            return
        with self._lock:
            if action in ("login", "signup"):
                if success:
                    self._auth_success += 1
                else:
                    self._auth_failure += 1

    def get_business_metrics(self):
        with self._lock:
            total_auth = self._auth_success + self._auth_failure
            return {
                "api_versions": dict(self._api_versions),
                "funnel": dict(self._funnel_counts),
                "auth": {
                    "success": self._auth_success,
                    "failure": self._auth_failure,
                    "failure_rate": round(
                        (self._auth_failure / total_auth * 100) if total_auth > 0 else 0, 2
                    ),
                },
            }
