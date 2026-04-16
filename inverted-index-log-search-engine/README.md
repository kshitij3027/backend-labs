# Inverted Index Log Search Engine

A high-performance inverted index system with specialized log tokenization, a RESTful search API, and a React-based search interface that enables sub-100ms full-text search across log entries.

## Tech Stack

- **Backend**: Python 3.12, FastAPI, Uvicorn
- **Frontend**: React (Vite)
- **Search Engine**: Custom inverted index with log-aware tokenization
- **Containerization**: Docker, Docker Compose

## Architecture

```
┌─────────────────┐       ┌─────────────────────────────────────┐
│  React Frontend │──────▶│         FastAPI Backend              │
│   (port 3000)   │  API  │          (port 8000)                │
└─────────────────┘       │                                     │
                          │  ┌─────────────┐  ┌──────────────┐  │
                          │  │  Tokenizer   │  │   Inverted   │  │
                          │  │  (log-aware) │─▶│    Index     │  │
                          │  └─────────────┘  └──────────────┘  │
                          │                                     │
                          │  ┌─────────────────────────────┐    │
                          │  │   Search API (REST)          │    │
                          │  │   - Full-text search         │    │
                          │  │   - Filtered queries         │    │
                          │  │   - Index management         │    │
                          │  └─────────────────────────────┘    │
                          └─────────────────────────────────────┘
```

## How It Runs

Long-lived server with API:
- **FastAPI backend** serves search/indexing endpoints on port **8000**
- **React frontend** runs on port **3000**
- Both are containerized via **Docker Compose**

## Features

- **Log-Aware Tokenization**: Parses timestamps, log levels, IP addresses, paths, and error codes as distinct tokens
- **Inverted Index**: In-memory inverted index with positional data for phrase queries
- **Sub-100ms Search**: Optimized posting list intersection for fast full-text search
- **RESTful API**: Endpoints for indexing log entries, searching, and index management
- **React Search UI**: Real-time search interface with syntax highlighting and faceted filtering
- **Bulk Indexing**: Batch import of log files with progress tracking

## API Endpoints

| Method | Endpoint               | Description                          |
|--------|------------------------|--------------------------------------|
| POST   | `/api/index`           | Index a single log entry             |
| POST   | `/api/index/bulk`      | Bulk index log entries from file      |
| GET    | `/api/search`          | Full-text search with query params    |
| GET    | `/api/search/advanced` | Advanced search with filters          |
| GET    | `/api/stats`           | Index statistics                      |
| DELETE | `/api/index`           | Clear the index                       |

## How to Run

### With Docker Compose

```bash
docker-compose up --build
```

- Backend: http://localhost:8000
- Frontend: http://localhost:3000
- API Docs: http://localhost:8000/docs

### Without Docker

**Backend:**
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

**Frontend:**
```bash
cd frontend
npm install
npm run dev
```

## What I Learned

- Building an inverted index from scratch with positional indexing
- Log-specific tokenization strategies (timestamps, IPs, log levels, stack traces)
- Posting list intersection algorithms and their performance characteristics
- Optimizing search latency to sub-100ms for large log datasets
- Connecting a FastAPI backend to a React frontend with real-time search UX
