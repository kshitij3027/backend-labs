# Implementation Guide

## Architecture Overview

```
                    ┌─────────────────┐
                    │   config.yaml   │
                    │  (window types, │
                    │   Redis, API)   │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   AppConfig     │  ← also reads env vars
                    └────────┬────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
 ┌────────▼────────┐ ┌──────▼───────┐ ┌────────▼────────┐
 │  WindowManager  │ │  Aggregator  │ │  WindowRotator  │
 │  (assignment,   │ │  (counters,  │ │  (lifecycle     │
 │   alignment)    │ │   metrics)   │ │   transitions)  │
 └────────┬────────┘ └──────┬───────┘ └────────┬────────┘
          │                  │                  │
          └──────────────────┼──────────────────┘
                             │
                    ┌────────▼────────┐
                    │     Redis       │
                    │  (hashes, sets, │
                    │   TTL expiry)   │
                    └─────────────────┘
```

### Component Responsibilities

| Component | File | Role |
|-----------|------|------|
| `AppConfig` | `src/config.py` | Loads YAML config, merges environment overrides |
| `TimestampParser` | `src/timestamp_parser.py` | Parses ISO 8601, Unix epoch, Apache, syslog formats into UTC datetimes |
| `WindowManager` | `src/window_manager.py` | Aligns timestamps to window boundaries, routes events, creates windows in Redis |
| `Aggregator` | `src/aggregator.py` | Atomic counter updates (HINCRBY), JSON field updates (WATCH/MULTI), metric reads |
| `WindowRotator` | `src/window_rotator.py` | Background task: transitions windows active -> grace -> closed, cleans expired keys |
| `ConnectionManager` | `src/websocket.py` | WebSocket connection tracking, broadcast with dead-connection pruning |
| `api` | `src/api.py` | FastAPI app: ingest, query, replay, dashboard, WebSocket, scheduler setup |

### Request Flow (Ingest)

1. `POST /api/v1/logs` receives a `LogEvent`
2. `TimestampParser.parse()` converts the timestamp to a UTC datetime
3. `WindowManager.assign_event()` iterates all configured window types:
   - Aligns timestamp to window boundary via floor division
   - Checks window state (active / grace / closed)
   - Creates the Redis hash if new, adds to active sorted set
4. `Aggregator.record_event()` updates counters:
   - `HINCRBY` for `count`, `error_count` (atomic, no locking needed)
   - `HINCRBYFLOAT` for `total_response_time`
   - `_json_field_incr()` for `levels` and `services` (WATCH/MULTI)
   - If `order_id` present, also updates e-commerce fields
5. Response returned with accepted/rejected/late counts

## Redis Data Model

### Window Hash

Each window is a single Redis hash. Key format: `window:{type}:{size}:{start_ts}`

Example: `window:5m:300:1711267200`

| Field | Type | Description |
|-------|------|-------------|
| `count` | int | Total events in window |
| `error_count` | int | Events with level ERROR or CRITICAL |
| `total_response_time` | float | Sum of response_time values |
| `sum_response_time_sq` | float | Sum of squared response times (for variance) |
| `start_ts` | int | Window start (unix epoch) |
| `end_ts` | int | Window end (unix epoch) |
| `window_type` | str | e.g. "5m", "1h", "1d" |
| `status` | str | "active", "grace", or "closed" |
| `levels` | JSON str | `{"ERROR": 5, "INFO": 42, ...}` |
| `services` | JSON str | `{"auth-service": 10, ...}` |
| `order_count` | int | Number of order events (e-commerce) |
| `total_revenue` | float | Sum of order values (e-commerce) |
| `order_statuses` | JSON str | `{"placed": 3, "cancelled": 1}` (e-commerce) |

### Active Window Sorted Set

Key: `windows:active:{type}` (e.g. `windows:active:5m`)

Members are window hash keys, scored by `start_ts`. Used by:
- Query endpoints to enumerate active windows
- `WindowRotator` to find windows needing state transitions
- Cleanup task to remove references to TTL-expired hashes

### TTL Strategy

Each window hash gets a TTL of `size_seconds + grace_period_seconds + retention_seconds`. For a 5-minute window with 60s grace and 3600s retention, TTL = 4260 seconds. Redis automatically evicts the hash after this period. The cleanup task periodically removes orphaned entries from sorted sets where the hash has already expired.

## Window Lifecycle

```
          ┌────────────┐
          │   ACTIVE    │  now < end_ts
          │  (accepting │
          │   events)   │
          └──────┬──────┘
                 │  now >= end_ts
          ┌──────▼──────┐
          │   GRACE     │  end_ts <= now < end_ts + grace_period
          │  (late OK)  │
          └──────┬──────┘
                 │  now >= end_ts + grace_period
          ┌──────▼──────┐
          │   CLOSED    │  read-only, retained until TTL
          │  (no new    │
          │   events)   │
          └──────┬──────┘
                 │  TTL expires
                 ▼
              [deleted by Redis]
```

The `WindowRotator.check_windows()` method runs every `lifecycle.check_interval` seconds (default: 10s) and transitions windows by updating the `status` field in the hash. Closed windows are removed from the active sorted set but their data remains queryable via the history endpoint until Redis TTL removes them.

## Extending the System

### Adding a New Window Type

Edit `config.yaml` and add an entry under `window_types`:

```yaml
window_types:
  - name: "15m"
    size_seconds: 900
    grace_period_seconds: 120
    retention_seconds: 7200
```

No code changes are needed. The system dynamically reads all window types from config at startup. Every ingested event is routed to all configured window types.

### Adding New Aggregate Fields

1. Add the field to `LogEvent` in `src/models.py` (optional field)
2. Update `Aggregator.record_event()` to track the new field in the hash
3. Update `Aggregator.get_window_metrics()` (or add a new method) to read it back
4. Optionally add a new query endpoint in `src/api.py`

### Adding New Timestamp Formats

Add a regex and parse method in `src/timestamp_parser.py`, then call it from `TimestampParser.parse()` before the ISO fallback.

## Performance Characteristics

### Atomic Operations

Most counter updates (`count`, `error_count`, `total_response_time`) use `HINCRBY`/`HINCRBYFLOAT` which are O(1) and lock-free in Redis. JSON field updates (`levels`, `services`) use optimistic locking and may retry on contention, but conflicts are rare at normal throughput since each window's hash is a distinct key.

### Memory Usage

Each window hash stores a fixed set of fields plus two JSON blobs that grow with the number of distinct log levels and service names. For typical deployments (5-10 services, 5 log levels), a window hash is under 1KB. With 5 window types and windows retained for up to 7 days, peak key count stays in the low thousands.

### Throughput

The load test (`make loadtest`) sends sustained traffic and reports events/second. The primary bottleneck is the JSON field optimistic locking; under high contention on a single window, retries increase. In practice, the 5-minute window rotation means contention is spread across multiple keys.

### Dashboard Latency

The WebSocket broadcast loop runs every `dashboard.refresh_interval` seconds (default: 5s). It reads all active windows for all types and sends a single JSON payload. Dashboard latency is bounded by the refresh interval plus Redis read time.
