## 1. Project Overview

**Project Name:** Smart Log Routing with Exchange Types

**One-Line Description:** A RabbitMQ-based log routing system that directs log messages to specialized processing pipelines using direct, topic, and fanout exchange patterns.

**How It Runs:** Long-lived process — a RabbitMQ broker runs in Docker, a producer generates log messages continuously, multiple specialized consumers run as separate processes consuming from their respective queues, and an optional Flask web dashboard provides real-time monitoring.

---

## 2. Core Requirements

* [ ] Set up a RabbitMQ instance via Docker Compose with management plugin enabled (ports 5672 and 15672)
* [ ] Implement a **Direct Exchange** (`logs_direct`) that routes messages using exact routing key matches
* [ ] Implement a **Topic Exchange** (`logs_topic`) that routes messages using wildcard pattern matching (e.g., `database.*.error`, `*.*.warning`)
* [ ] Implement a **Fanout Exchange** (`logs_fanout`) that broadcasts messages to all bound queues
* [ ] All three exchanges must be declared as `durable=True`
* [ ] Define queue bindings that map exchanges to queues using routing patterns or exact keys
* [ ] Implement a **Log Producer** that generates structured log messages with fields: `timestamp`, `service`, `component`, `level`, `routing_key`, and `metadata` (containing `source_ip` and `request_id`)
* [ ] Routing keys must follow the hierarchical format `{service}.{component}.{level}`
* [ ] Producer must be able to publish to all three exchange types (direct, topic, fanout)
* [ ] Implement **Specialized Consumers** that process messages differently based on log type — error logs go to incident management processing, security logs go to threat analysis processing, database logs go to performance analysis processing
* [ ] Consumers must acknowledge messages after processing (`basic_ack`)
* [ ] Implement a **real-time web dashboard** (Flask + SocketIO) that displays routed messages and live statistics
* [ ] The dashboard must emit log updates and stats to connected clients via WebSocket
* [ ] System must handle **1000+ messages/second** throughput
* [ ] Project structure must follow: `src/`, `tests/`, `config/`, `logs/`, `web/`, `scripts/` directories

---

## 3. Extended Requirements (Homework / Enhancements)

The article does not include an explicit homework or enhancements section. However, these are implied enhancements from the narrative:

**Feature Area A: Routing Pattern Flexibility**
* [ ] Support routing patterns that grow with the service architecture (e.g., adding new services without reconfiguring existing consumers)
* [ ] Design routing keys that accommodate future service additions without breaking existing bindings

**Feature Area B: Load and Scale Testing**
* [ ] Test with hundreds of messages per second, then scale to thousands
* [ ] Monitor routing performance and queue depths under load

**Feature Area C: Dashboard Stats**
* [ ] Track and display per-queue message counts in real time
* [ ] Show routing distribution across exchange types

---

## 4. Bonus / Stretch Goals

* [ ] Add Redis integration for caching routing statistics or message deduplication (Redis is listed as a dependency but not deeply specified in the article)
* [ ] Add colorized console output for different log types using `colorama`
* [ ] Simulate realistic multi-service architectures (user service, database service, API gateway, security service) producing diverse log streams simultaneously

---

## 5. Success Criteria

* [ ] All tests pass (article specifies 12/12)
* [ ] RabbitMQ is running and accessible on `localhost:5672`
* [ ] RabbitMQ management UI accessible on `localhost:15672`
* [ ] Web dashboard accessible at `http://localhost:5000`
* [ ] Console shows routed messages categorized by type with correct emoji prefixes
* [ ] Real-time statistics update on the dashboard
* [ ] Direct exchange correctly delivers to exact routing key match only
* [ ] Topic exchange correctly matches wildcard patterns (e.g., `database.*.error` matches `database.postgres.error` and `database.mysql.error`)
* [ ] Fanout exchange delivers messages to all bound queues
* [ ] Demo output matches expected pattern:
```
🎯 Direct: database.postgres.error
🏷️  Topic: api.gateway.info  
📢 Fanout: Critical security message
✅ [DATABASE-PROCESSOR] Processing message #1
🚨 ERROR PROCESSING: Sending to incident management
```

---

## 6. Technical Needs

| Category | Details |
|---|---|
| **Language / Runtime** | Python 3 |
| **External Libraries** | `pika==1.3.2`, `flask==3.0.3`, `pytest==8.2.0`, `redis==5.0.4`, `flask-socketio==5.3.6`, `colorama==0.4.6`, `requests==2.31.0` |
| **Infrastructure** | Docker, Docker Compose, RabbitMQ 3.12 with management plugin |
| **Already Have** | Python, Docker Desktop |
| **Need to Install** | All pip dependencies via `requirements.txt` (RabbitMQ runs in Docker, so no separate install needed) |

---

## 7. Configurable Parameters

| Parameter | Default | Configured Via |
|---|---|---|
| RabbitMQ host | `localhost` | Config file or env var |
| RabbitMQ port | `5672` | Docker Compose / env var |
| RabbitMQ management port | `15672` | Docker Compose / env var |
| RabbitMQ username | `guest` | Docker Compose env / config |
| RabbitMQ password | `guest` | Docker Compose env / config |
| Web dashboard port | `5000` | Config file or env var |
| Direct exchange name | `logs_direct` | Code constant / config |
| Topic exchange name | `logs_topic` | Code constant / config |
| Fanout exchange name | `logs_fanout` | Code constant / config |
| Routing key format | `{service}.{component}.{level}` | Convention (not configurable) |
| Message throughput target | `1000+ msg/sec` | Producer rate control |

---

## 8. Input / Output Spec

**Inputs:** The producer generates synthetic log messages internally — no external file or stdin input. It simulates logs from services like `database`, `api`, `security`, etc. with components and severity levels.

**Outputs:**
- Console output showing routed messages with type-specific prefixes (emoji-tagged)
- WebSocket events pushed to the Flask dashboard (`log_update` events with message, stats, and queue info)
- RabbitMQ queues holding messages for each consumer type

**Output Format(s):** JSON (message bodies in queues), plain text (console), WebSocket JSON events (dashboard)

**Sample Output:**
```
🎯 Direct: database.postgres.error
🏷️  Topic: api.gateway.info  
📢 Fanout: Critical security message
✅ [DATABASE-PROCESSOR] Processing message #1
🚨 ERROR PROCESSING: Sending to incident management
🔒 SECURITY PROCESSING: Analyzing for threats
💾 DATABASE PROCESSING: Performance analysis
```