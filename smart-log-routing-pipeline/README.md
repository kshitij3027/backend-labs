# Smart Log Routing Pipeline

A RabbitMQ-based log routing system that directs log messages to specialized processing pipelines using direct, topic, and fanout exchange patterns.

## Architecture

```
                          +------------------+
                          |   Log Producer   |
                          | (generates logs) |
                          +--------+---------+
                                   |
                    +--------------+--------------+
                    |              |              |
              +-----v----+  +-----v----+  +------v-----+
              |  Direct   |  |  Topic   |  |  Fanout    |
              | Exchange  |  | Exchange |  |  Exchange  |
              |logs_direct|  |logs_topic|  |logs_fanout |
              +--+--+--+--+  +--+--+---+  +---+----+---+
                 |  |  |        |  |  |        |    |
        +--------+  |  +--+    |  |  +--+     |    +--------+
        |           |     |    |  |     |      |             |
   +----v---+ +----v--+ +-v---++ |  +---v--+ +-v--------+ +-v------+
   | error  | |warning| |crit.|| |  |api   | | audit    | | all    |
   | _logs  | |_logs  | |_logs|| |  |_logs | | _logs    | | _logs  |
   +----+---+ +-------+ +-----+| |  +------+ +----+-----+ +---+---+
        |                  +----v-+---+             |           |
        |                  | database |             |           |
        |                  | _logs    |             |           |
        |                  +--+---+---+             |           |
        |                     |   |                 |           |
        |              +------v-+ |                 |           |
        |              |security| |                 |           |
        |              |_logs   | |                 |           |
        |              +---+----+ |                 |           |
        |                  |      |                 |           |
   +----v------+     +----v----+ +v-----------+ +--v-----------v--+
   |  Error    |     |Security |  |Database   | |  Audit          |
   |  Consumer |     |Consumer |  |Consumer   | |  Consumer       |
   | (incidents|     |(threats)|  |(perf      | | (compliance     |
   |  mgmt)   |     |         |  | analysis) | |  logging)       |
   +-----------+     +---------+  +-----------+ +-----------------+

                          +------------------+
                          | Flask Dashboard  |
                          |  (localhost:5555)|
                          | Real-time stats  |
                          | via WebSocket    |
                          +------------------+
```

## Tech Stack

- **Python 3.11** -- application runtime
- **RabbitMQ 3.13** -- message broker with management plugin
- **Flask + Flask-SocketIO** -- real-time monitoring dashboard
- **Pika** -- Python AMQP client for RabbitMQ
- **Docker & Docker Compose** -- containerized deployment
- **pytest** -- unit and integration testing

## How to Run

```bash
make build        # Build Docker images
make run          # Start RabbitMQ
make test         # Run 65 unit + integration tests
make e2e          # Run 18 E2E verification checks
make throughput   # Validate 1000+ msg/s throughput
make dashboard    # Start real-time dashboard on :5555
docker compose up -d  # Start full stack (all services)
make demo         # Run demo orchestrator
make sim          # Run multi-service simulation
make clean        # Tear down everything
```

RabbitMQ management UI is available at `http://localhost:15672` (guest/guest).

## Project Structure

```
smart-log-routing-pipeline/
├── config/
│   └── routing_config.yaml      # Exchange, queue, and binding definitions
├── logs/                        # Runtime log output directory
├── scripts/
│   ├── multi_service_sim.py     # Multi-service architecture simulator
│   ├── run_demo.py              # Demo orchestrator script
│   ├── throughput_test.py       # Throughput benchmark (1000+ msg/s)
│   ├── verify_e2e.py            # End-to-end verification (18 checks)
│   └── wait_for_rabbitmq.py     # Startup health check helper
├── src/
│   ├── config.py                # Configuration loader (YAML + env vars)
│   ├── connection.py            # RabbitMQ connection manager with retries
│   ├── producer.py              # Log producer (publishes to all exchanges)
│   ├── setup.py                 # Exchange and queue declaration/binding
│   ├── consumers/
│   │   ├── base_consumer.py     # Abstract base consumer with ack/prefetch
│   │   ├── audit_consumer.py    # Fanout consumer for compliance logging
│   │   ├── database_consumer.py # Topic consumer for DB performance analysis
│   │   ├── error_consumer.py    # Direct consumer for incident management
│   │   └── security_consumer.py # Topic consumer for threat analysis
│   ├── dashboard/
│   │   ├── app.py               # Flask + SocketIO dashboard server
│   │   └── stats_collector.py   # RabbitMQ management API stats poller
│   └── models/
│       └── log_message.py       # Structured log message model
├── tests/
│   ├── conftest.py              # Shared fixtures and mocks
│   ├── test_config.py           # Configuration loading tests
│   ├── test_connection.py       # Connection manager tests
│   ├── test_consumers.py        # Consumer processing logic tests
│   ├── test_dashboard.py        # Dashboard endpoint tests
│   ├── test_integration.py      # Integration tests (live RabbitMQ)
│   ├── test_models.py           # Log message model tests
│   ├── test_producer.py         # Producer publishing tests
│   └── test_setup.py           # Exchange/queue setup tests
├── web/
│   └── templates/
│       └── index.html           # Dashboard UI template
├── docker-compose.yml           # Service definitions (9 services)
├── Dockerfile                   # Application image
├── Dockerfile.test              # Test runner image
├── Makefile                     # Build/run/test shortcuts
├── pytest.ini                   # pytest configuration
├── requirements.txt             # Python dependencies
└── .env.example                 # Environment variable template
```

## Exchange Types & Routing

| Exchange | Type | Routing Pattern | Target Queues |
|---|---|---|---|
| `logs_direct` | Direct | Exact routing key match | `error_logs`, `warning_logs`, `critical_logs` |
| `logs_topic` | Topic | Wildcard pattern matching (`*`, `#`) | `database_logs`, `security_logs`, `api_logs` |
| `logs_fanout` | Fanout | Broadcast to all bound queues | `audit_logs`, `all_logs` |

## Queue Configuration

| Queue | Exchange | Routing Key | Purpose |
|---|---|---|---|
| `error_logs` | `logs_direct` | `error` | Error-level logs for incident management |
| `warning_logs` | `logs_direct` | `warning` | Warning-level logs for monitoring |
| `critical_logs` | `logs_direct` | `critical` | Critical-level logs for immediate alerts |
| `database_logs` | `logs_topic` | `database.#` | All database service logs (any component/level) |
| `security_logs` | `logs_topic` | `security.#` | All security service logs (any component/level) |
| `api_logs` | `logs_topic` | `api.*.error` | API service error logs only |
| `audit_logs` | `logs_fanout` | _(none)_ | Compliance audit trail (receives all messages) |
| `all_logs` | `logs_fanout` | _(none)_ | Complete log archive (receives all messages) |

## Configuration

All settings are loaded from `config/routing_config.yaml` and can be overridden with environment variables:

| Variable | Default | Description |
|---|---|---|
| `RABBITMQ_HOST` | `localhost` | RabbitMQ server hostname |
| `RABBITMQ_PORT` | `5672` | AMQP protocol port |
| `RABBITMQ_USER` | `guest` | RabbitMQ username |
| `RABBITMQ_PASS` | `guest` | RabbitMQ password |
| `DASHBOARD_PORT` | `5555` | Flask dashboard port |

## Dashboard

The Flask + SocketIO dashboard runs at `http://localhost:5555` and provides real-time monitoring of all exchanges, queues, and message flow via WebSocket push updates.

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Real-time monitoring UI with live message feed |
| `/api/stats` | GET | Queue statistics as JSON |
| `/health` | GET | Health check endpoint |

Start it with:

```bash
make dashboard
```

## Key Concepts Learned

- **Direct exchange**: exact routing key matching for severity-based routing (error, warning, critical)
- **Topic exchange**: wildcard pattern matching (`*` matches one word, `#` matches zero or more) for hierarchical service routing
- **Fanout exchange**: broadcast to all bound queues for audit/logging regardless of routing key
- **Manual acknowledgment** (`basic_ack`) for reliable message processing -- messages are not lost if a consumer crashes
- **QoS prefetch** for consumer flow control -- prevents a single consumer from being overwhelmed
- **RabbitMQ management API** for monitoring queue depths, message rates, and connection health
- **Real-time WebSocket updates** with Flask-SocketIO for live dashboard without polling
- **Docker Compose service orchestration** with health checks and dependency ordering
