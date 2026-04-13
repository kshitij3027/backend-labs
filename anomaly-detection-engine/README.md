# Anomaly Detection Engine

A multi-algorithm anomaly detection system that processes real-time log streams using Z-score, Isolation Forest, and Temporal Pattern Analysis with ensemble decision-making and a live monitoring dashboard.

## Tech Stack

- **Language:** Python 3.11+
- **Web Framework:** Flask + Flask-SocketIO
- **Real-time:** WebSockets (via Socket.IO)
- **ML/Stats:** scikit-learn, NumPy, SciPy
- **Dashboard:** HTML/CSS/JS with Chart.js and Socket.IO client
- **Containerization:** Docker / Docker Compose

## Architecture

```
Log Source(s)
     │
     ▼
┌─────────────────────────────────┐
│        Log Ingestion Layer      │
│   (streaming parser & buffer)   │
└──────────────┬──────────────────┘
               │
     ┌─────────┼─────────┐
     ▼         ▼         ▼
┌─────────┐┌─────────┐┌──────────────┐
│ Z-Score ││Isolation││  Temporal    │
│Detector ││ Forest  ││  Pattern     │
│         ││Detector ││  Analyzer    │
└────┬────┘└────┬────┘└──────┬───────┘
     │          │            │
     ▼          ▼            ▼
┌─────────────────────────────────┐
│      Ensemble Decision Maker    │
│  (weighted voting / threshold)  │
└──────────────┬──────────────────┘
               │
     ┌─────────┴─────────┐
     ▼                   ▼
┌──────────┐     ┌──────────────┐
│ Alert    │     │  Dashboard   │
│ Manager  │     │ (WebSocket)  │
└──────────┘     └──────────────┘
```

## Detection Algorithms

### 1. Z-Score Detector
Statistical method that flags log entries whose numeric features (e.g., response time, payload size) deviate beyond a configurable number of standard deviations from the rolling mean.

### 2. Isolation Forest Detector
Tree-based unsupervised ML algorithm that isolates anomalies by randomly partitioning feature space. Anomalous points require fewer splits to isolate and receive lower anomaly scores.

### 3. Temporal Pattern Analyzer
Detects anomalies based on time-series patterns — sudden bursts in log frequency, unusual timing gaps, or deviations from learned periodic behavior (e.g., expected request cadence).

### Ensemble Decision Maker
Combines scores from all three detectors using weighted voting. A log entry is flagged as anomalous only when the ensemble confidence exceeds a configurable threshold, reducing false positives.

## How to Run

### With Docker (recommended)

```bash
docker-compose up --build
```

The dashboard will be available at `http://localhost:5000`.

### Without Docker

```bash
cd anomaly-detection-engine
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open `http://localhost:5000` in your browser.

## Configuration

Environment variables (see `.env.example`):

| Variable | Description | Default |
|---|---|---|
| `FLASK_PORT` | Dashboard server port | `5000` |
| `ZSCORE_THRESHOLD` | Z-score sigma threshold | `3.0` |
| `IFOREST_CONTAMINATION` | Isolation Forest expected anomaly ratio | `0.05` |
| `ENSEMBLE_THRESHOLD` | Minimum ensemble confidence to flag | `0.6` |
| `LOG_BUFFER_SIZE` | Rolling window size for statistics | `1000` |
| `SIMULATED_EPS` | Simulated log events per second | `10` |

## Dashboard Features

- Real-time anomaly feed with severity coloring
- Per-algorithm score breakdown for each flagged event
- Time-series chart of log volume and anomaly rate
- Ensemble confidence distribution histogram
- Algorithm agreement heatmap

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Live monitoring dashboard |
| `GET` | `/api/stats` | Current detection statistics |
| `GET` | `/api/anomalies` | Recent anomalies (paginated) |
| `POST` | `/api/logs` | Ingest a batch of log entries |
| `WebSocket` | `/ws` | Real-time anomaly push channel |

## What I Learned

- Implementing and tuning multiple anomaly detection algorithms on streaming data
- Ensemble methods for reducing false positives in anomaly detection
- Real-time data pipelines with Flask-SocketIO and WebSockets
- Rolling statistics and online learning for unbounded streams
- Building live monitoring dashboards with server-push updates
