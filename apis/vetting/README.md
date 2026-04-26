# KLAIRE Vetting API — Setup & Deployment Guide

FastAPI microservice for Clearline HMO PA (Prior Authorization) vetting.

## Quick Start

```bash
# From the repo root
pip install -r requirements.txt

# Set required environment variables (or add to .streamlit/secrets.toml)
export MOTHERDUCK_TOKEN=your_motherduck_token
export ANTHROPIC_API_KEY=your_anthropic_key
export MONGO_URI=mongodb://localhost:27017   # or Atlas URI

# Run
uvicorn apis.vetting.main:app --host 0.0.0.0 --port 8000 --reload
```

API docs available at `http://localhost:8000/docs` once running.

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `MOTHERDUCK_TOKEN` | Yes | MotherDuck cloud DuckDB token. Falls back to `.streamlit/secrets.toml` key `MOTHERDUCK_TOKEN` |
| `ANTHROPIC_API_KEY` | Yes | Anthropic Claude API key |
| `MONGO_URI` | No | MongoDB connection string. Defaults to `mongodb://localhost:27017` |

## Static Data Files (must be in repo root)

The following files are loaded at startup and must be present:

| File | Purpose |
|---|---|
| `KLAIRE AGENT_CONSULATATION.csv` | Master consultation code list (code, name, type) |
| `Private Capitation List.xlsx` | Capitated procedure codes |
| `cba-capitation-details-report (38).xlsx` | Capitated enrollee list with attached providers |

## BNF Chroma DB

The BNF 80 vector database (`bnf_chroma_db/`) must be present in the repo root.
It is a local ChromaDB persistent store with collection `bnf_80`.
Used by `bnf_client.py` as supplementary drug dosing context in Rule 9.
If absent, the system degrades gracefully — Rule 9 continues without BNF context.

## Source Files

| File | Role |
|---|---|
| `main.py` | FastAPI app, all endpoints, startup |
| `klaire_consultation.py` | GP + specialist consultation engine (5 rules) |
| `klaire_pa.py` | PA validation engine (16 rules, parallel execution) |
| `klaire_admission.py` | Admission pre-check engine (severity, readmission, duration) |
| `comprehensive.py` | ComprehensiveVettingEngine — bulk PA, learning tables |
| `thirty_day.py` | 30-day duplicate detection |
| `clinical_necessity.py` | ClinicalNecessityEngine — AI clinical appropriateness |
| `drug_apis.py` | RxNorm, RxClass, WHO EML, OpenFDA utilities |
| `bnf_client.py` | BNF 80 Chroma DB query client |
| `mongo_db.py` | MongoDB read/write helpers |

## MongoDB Collections (auto-created on first run)

- `PROCEDURE_MASTER` — procedure codes with branch (NO-AUTH/PRE-AUTH), class, age/gender rules
- `DIAGNOSIS_MASTER` — diagnosis codes with age/gender eligibility
- `PROCEDURE_DIAGNOSIS_COMP` — known compatible procedure+diagnosis pairs
- `ai_human_procedure_diagnosis` — learning table: procedure/diagnosis compatibility
- `ai_specialist_diagnosis` — learning table: specialist/diagnosis compatibility
- `klaire_review_queue` — agent review queue (PENDING_REVIEW items)
- `vetting_queue` — legacy PA queue

## Key API Endpoints

### No-Auth Flow
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/klaire/providers` | Provider dropdown |
| GET | `/api/v1/klaire/procedures` | Procedure dropdown (includes `branch` field) |
| GET | `/api/v1/klaire/consultation-codes` | Consultation code dropdown |
| GET | `/api/v1/klaire/tariff` | Contracted tariff price lookup |
| GET | `/api/v1/klaire/search-diagnoses` | Live diagnosis search |
| GET | `/api/v1/klaire/is-capitated` | Capitation check |
| POST | `/api/v1/klaire/consult` | Submit consultation (GP or specialist) |
| POST | `/api/v1/klaire/pa` | Submit PA items (multiple procedures) |

### Agent Review
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/klaire/reviews` | Fetch pending review queue |
| POST | `/api/v1/klaire/review/{id}` | Submit agent decision |

### Pre-Auth (Admission)
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/klaire/admission-codes` | ADM01/02/03 code list |
| POST | `/api/v1/klaire/admission` | Submit admission request |

Full API reference: see `KLAIRE_NO_AUTH_REFERENCE.md` in repo root.
