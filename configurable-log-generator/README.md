# Configurable Log Generator

A long-lived Python process that continuously produces realistic, timestamped application logs at a configurable rate with multiple output formats. Designed to feed downstream log collectors, analyzers, and monitoring pipelines.

## Tech Stack

- **Language**: Python 3.12 (standard library only — zero external dependencies)
- **Containerization**: Docker + Docker Compose

## Features

- Generates logs at a configurable rate (default: 10 logs/sec)
- Four log levels with configurable probability distribution (INFO 70%, WARNING 20%, ERROR 5%, DEBUG 5%)
- Three output formats: plain text, JSON, CSV
- Dual output: file + optional console simultaneously
- **Burst mode**: probabilistic rate spikes (5-10x) simulating traffic surges
- **State machine patterns**: realistic multi-step sequences
  - User sessions: login → browse → purchase → logout
  - API requests: request → auth → process → query → response
  - Error recovery: warning → degraded → error → circuit breaker → recovery
- Custom fields per entry: service_name, user_id, request_id, duration_ms
- Graceful shutdown via SIGINT/SIGTERM
- All settings configurable via environment variables

## How to Run

### Docker Compose (recommended)

```bash
# Start the generator
docker-compose up --build -d

# Watch the logs in real-time
docker-compose logs -f log-generator

# Stop
docker-compose down
```

Generated logs appear in `./logs/app.log` on the host.

### Customize via environment

Copy `.env.example` to `.env`, edit values, then update `docker-compose.yml` to use `env_file: .env` instead of `.env.example`.

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `LOG_RATE` | `10` | Logs generated per second |
| `LOG_FORMAT` | `text` | Output format: `text`, `json`, or `csv` |
| `OUTPUT_FILE` | `logs/app.log` | Path to the log file inside the container |
| `CONSOLE_OUTPUT` | `true` | Also write logs to stdout |
| `LOG_DISTRIBUTION` | `INFO:0.70,WARNING:0.20,ERROR:0.05,DEBUG:0.05` | Level weights |
| `ENABLE_BURSTS` | `true` | Enable probabilistic rate spikes |
| `BURST_FREQUENCY` | `0.05` | Chance of burst per second (0.0-1.0) |
| `BURST_MULTIPLIER` | `5` | Base multiplier during bursts (actual: 1x-2x this value) |
| `BURST_DURATION` | `3` | Burst duration in seconds |
| `ENABLE_PATTERNS` | `true` | Enable state machine log sequences |

## Sample Output

**Text format:**
```
2025-05-14 10:23:45 | INFO    | abc-1234 | user-service | user-67890 | req-xyz789 | 142ms | User login successful
```

**JSON format:**
```json
{"timestamp": "2025-05-14T10:23:45", "level": "INFO", "id": "abc-1234", "service": "user-service", "user_id": "user-67890", "request_id": "req-xyz789", "duration_ms": 142, "message": "User login successful"}
```

**CSV format** (with header row):
```
timestamp,level,id,service,user_id,request_id,duration_ms,message
2025-05-14T10:23:45,INFO,abc-1234,user-service,user-67890,req-xyz789,142,User login successful
```

## Project Structure

```
src/
├── main.py          # Entry point, main loop, signal handling
├── config.py        # Environment variable loading + Config dataclass
├── models.py        # LogEntry dataclass + ID generators
├── formatters.py    # Text, JSON, CSV formatters
├── output.py        # Thread-safe dual writer (file + console)
├── messages.py      # Realistic message pools per log level
├── burst.py         # BurstController for rate spikes
└── patterns.py      # State machine patterns + session tracker
```

## What I Learned

- **Rate limiting with sleep**: generating logs at a precise per-second rate using a budget-based loop with `time.sleep()` for spacing
- **State machines for realistic data**: modeling multi-step processes (user sessions, API requests, error recovery) as ordered step sequences that progress over time
- **Probabilistic burst simulation**: using random rolls each second to trigger temporary rate multipliers, mimicking real-world traffic spikes
- **Dual output with thread safety**: writing to both a file and stdout simultaneously using a `threading.Lock` to protect shared file handles
- **CSV edge cases**: using `csv.writer` with `io.StringIO` to handle proper escaping (messages with commas, quotes) rather than naive string concatenation
- **Clean Docker containerization**: running a Python process with zero dependencies in an Alpine image, using volume mounts for log file visibility on the host
- **Signal handling in containers**: catching SIGTERM from `docker stop` for graceful shutdown and clean file handle closure
