# Compliance Reporting Engine

A service that aggregates log data and generates standardized, cryptographically-signed compliance reports (SOX, HIPAA, PCI-DSS, GDPR) in multiple export formats.

## Overview

The Compliance Reporting Engine ingests log events from upstream sources, applies framework-specific evidence rules, and produces tamper-evident reports suitable for auditor review. Each report is digitally signed so its integrity can be verified independently after download.

### How It Runs

- **Long-lived FastAPI server** exposing a REST API for on-demand report generation, status polling, and file downloads.
- **React dashboard frontend** for browsing past reports, kicking off new runs, and viewing audit trails.
- **Background task workers** generate reports asynchronously so HTTP requests stay non-blocking.
- **Scheduled jobs** trigger automated periodic report runs (daily/weekly/monthly) for each enabled compliance framework.

## Tech Stack

- **Language:** Python 3.11+
- **API Framework:** FastAPI
- **ASGI Server:** Uvicorn
- **Frontend:** React (separate package, not part of this Python requirements file)
- **Background Tasks:** FastAPI BackgroundTasks / APScheduler
- **Scheduler:** APScheduler (cron-style triggers)
- **Database:** PostgreSQL (via SQLAlchemy + Alembic for migrations)
- **Cryptographic Signing:** `cryptography` (Ed25519 / RSA-PSS for report signatures)
- **Export Formats:** PDF (ReportLab), XLSX (openpyxl), CSV (pandas), JSON
- **Data Aggregation:** pandas
- **Validation:** Pydantic v2
- **Testing:** pytest, pytest-asyncio, httpx

## Compliance Frameworks Supported

| Framework | Scope | Typical Evidence |
| --------- | ----- | ---------------- |
| **SOX**   | Financial controls | Access changes to financial systems, segregation-of-duty violations |
| **HIPAA** | Protected Health Information | PHI access logs, encryption-at-rest checks, breach notifications |
| **PCI-DSS** | Payment card data | Cardholder-data access, key rotation events, network segmentation |
| **GDPR**  | Personal data of EU residents | Consent records, data-subject requests, cross-border transfers |

## Planned Endpoints (subject to refinement)

| Method | Path | Purpose |
| ------ | ---- | ------- |
| `POST` | `/reports` | Submit a new report generation request |
| `GET`  | `/reports` | List reports (filter by framework, status, date range) |
| `GET`  | `/reports/{id}` | Get report metadata and status |
| `GET`  | `/reports/{id}/download` | Download the signed report file |
| `GET`  | `/reports/{id}/signature` | Fetch the detached signature + verification key reference |
| `POST` | `/schedules` | Create a recurring scheduled report job |
| `GET`  | `/schedules` | List active schedules |
| `DELETE` | `/schedules/{id}` | Cancel a scheduled job |
| `GET`  | `/health` | Service liveness/readiness |

## Export Formats

- **PDF** — auditor-friendly human-readable report
- **XLSX** — tabular evidence for spreadsheet review
- **CSV** — raw evidence rows for ingestion into other tools
- **JSON** — machine-readable structured output

Each export includes a detached signature file (`.sig`) so the report bytes can be verified without re-downloading the file.

## How to Run

_Not yet implemented._ This scaffold currently contains only project requirements, dependency manifest, and ignore rules. Implementation will follow once the design is approved.

## What I Learned

_To be filled in as the project evolves._

## Status

Scaffolding only — README, `requirements.txt`, and `.gitignore`. No application code yet.
