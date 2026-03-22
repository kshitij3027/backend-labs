"""Lightweight IP-to-region mapping using deterministic hash -- no external database."""

import hashlib
import threading
import logging

logger = logging.getLogger(__name__)

# Predefined regions for demo purposes
REGIONS = [
    {"country": "US", "region": "North America", "city": "New York", "lat": 40.71, "lon": -74.01},
    {"country": "US", "region": "North America", "city": "San Francisco", "lat": 37.77, "lon": -122.42},
    {"country": "UK", "region": "Europe", "city": "London", "lat": 51.51, "lon": -0.13},
    {"country": "DE", "region": "Europe", "city": "Berlin", "lat": 52.52, "lon": 13.41},
    {"country": "JP", "region": "Asia Pacific", "city": "Tokyo", "lat": 35.68, "lon": 139.69},
    {"country": "AU", "region": "Asia Pacific", "city": "Sydney", "lat": -33.87, "lon": 151.21},
    {"country": "BR", "region": "South America", "city": "Sao Paulo", "lat": -23.55, "lon": -46.63},
    {"country": "IN", "region": "Asia Pacific", "city": "Mumbai", "lat": 19.08, "lon": 72.88},
    {"country": "CA", "region": "North America", "city": "Toronto", "lat": 43.65, "lon": -79.38},
    {"country": "SG", "region": "Asia Pacific", "city": "Singapore", "lat": 1.35, "lon": 103.82},
]


class GeoAnalyzer:
    def __init__(self):
        self._lock = threading.Lock()
        self._cache = {}  # {ip: region_dict}
        self._traffic_by_region = {}  # {region_name: count}
        self._latency_by_region = {}  # {region_name: [response_times]}

    def analyze_ip(self, ip_address):
        """Map IP to a geographic region using deterministic hash."""
        if not ip_address:
            return None

        with self._lock:
            if ip_address in self._cache:
                region = self._cache[ip_address]
                self._traffic_by_region[region["region"]] = (
                    self._traffic_by_region.get(region["region"], 0) + 1
                )
                return region

        # Check for private/internal IPs
        if ip_address.startswith(("10.", "172.16.", "192.168.", "127.")):
            region = {"country": "Internal", "region": "Internal", "city": "Local", "lat": 0, "lon": 0}
        else:
            # Deterministic hash to region
            idx = int(hashlib.md5(ip_address.encode()).hexdigest(), 16) % len(REGIONS)
            region = REGIONS[idx].copy()

        with self._lock:
            self._cache[ip_address] = region
            self._traffic_by_region[region["region"]] = (
                self._traffic_by_region.get(region["region"], 0) + 1
            )

        return region

    def record_latency(self, ip_address, response_time):
        """Record response time for a geographic region."""
        region = self.analyze_ip(ip_address)
        if region and response_time is not None:
            region_name = region["region"]
            with self._lock:
                if region_name not in self._latency_by_region:
                    self._latency_by_region[region_name] = []
                # Keep bounded
                latencies = self._latency_by_region[region_name]
                if len(latencies) >= 1000:
                    latencies.pop(0)
                latencies.append(float(response_time))

    def get_geo_metrics(self):
        with self._lock:
            latency_summary = {}
            for region, times in self._latency_by_region.items():
                if times:
                    latency_summary[region] = {
                        "avg": round(sum(times) / len(times), 2),
                        "count": len(times),
                    }

            return {
                "traffic_by_region": dict(self._traffic_by_region),
                "latency_by_region": latency_summary,
            }
