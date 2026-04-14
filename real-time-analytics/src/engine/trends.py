"""Trend analysis for metric data using linear regression."""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from src.models import MetricPoint


def calculate_trend(data_points: list[MetricPoint], window_minutes: float = 5.0) -> dict:
    """Calculate trend direction using linear regression over the given window.

    Returns dict with: direction, slope, r_squared, change_rate, current_value, data_points_count
    """
    if len(data_points) < 2:
        return {
            "direction": "insufficient_data",
            "slope": 0.0,
            "r_squared": 0.0,
            "change_rate": 0.0,
            "current_value": data_points[0].value if data_points else 0.0,
            "data_points_count": len(data_points),
        }

    # Filter to window
    now = data_points[-1].timestamp  # most recent point
    cutoff = now - (window_minutes * 60)
    windowed = [p for p in data_points if p.timestamp >= cutoff]

    if len(windowed) < 2:
        return {
            "direction": "insufficient_data",
            "slope": 0.0,
            "r_squared": 0.0,
            "change_rate": 0.0,
            "current_value": data_points[-1].value,
            "data_points_count": len(windowed),
        }

    timestamps = np.array([p.timestamp for p in windowed])
    values = np.array([p.value for p in windowed])

    # Normalize timestamps to start at 0
    t_normalized = timestamps - timestamps[0]

    result = linregress(t_normalized, values)
    slope = float(result.slope)
    r_squared = float(result.rvalue ** 2)

    mean_val = float(np.mean(values))
    # Determine direction based on slope relative to mean
    if mean_val == 0:
        direction = "stable"
    elif abs(slope) > 0.01 * abs(mean_val):
        direction = "increasing" if slope > 0 else "decreasing"
    else:
        direction = "stable"

    change_rate = (slope / abs(mean_val) * 100) if mean_val != 0 else 0.0

    return {
        "direction": direction,
        "slope": round(slope, 6),
        "r_squared": round(r_squared, 4),
        "change_rate": round(change_rate, 2),
        "current_value": float(values[-1]),
        "data_points_count": len(windowed),
    }
