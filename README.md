# 🏛️ JudiQ AI — Section 138 NI Act Litigation Intelligence Platform

[![Build Status](https://img.shields.io/badge/Tests-1%2C307%20Passing-brightgreen.svg)](file:///backend/tests)
[![Accuracy](https://img.shields.io/badge/Benchmark%20Accuracy-100%25-blue.svg)](file:///backend/tests/test_cheque_bounce_hard_cases.py)
[![Python Version](https://img.shields.io/badge/Python-3.12%2B-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/Backend-FastAPI-009688.svg)](https://fastapi.tiangolo.com)
[![Docker](https://img.shields.io/badge/Container-Non--Root%20Hardened-blue.svg)](file:///Dockerfile)
![License](https://img.shields.io/badge/License-Enterprise%20Proprietary-gold.svg)

> **JudiQ AI** is a deterministic legal intelligence platform purpose-built for Section 138 of the Negotiable Instruments Act (Cheque Bounce) litigation. It automates fatal defect analysis, limitation audits, statutory notice validation, courtroom probability scoring, verified precedent matching, and legal document drafting.

---

## 📑 Core Section 138 Framework & Capabilities

| Module | Statutory Framework | Key Capabilities |
| :--- | :--- | :--- |
| **Cheque Presentation** | NI Act Section 138(a) | Mandatory 3-month cheque presentation check from date of drawal; validity extension audit. |
| **Demand Notice Audit** | NI Act Section 138(b) | Strict 30-day statutory notice dispatch calculation from bank dishonour memo receipt. |
| **Cure Window & Cause of Action** | NI Act Section 138(c) | 15-day statutory payment cure window tracking; limitation accrual date calculation. |
| **Filing Limitation & Condonation** | NI Act Section 142(1)(b) | 30-day court complaint filing window audit; Section 142(1)(b) proviso condonation requirement detection. |
| **Vicarious Corporate Liability** | NI Act Section 141 | Strict averment tests for directors, managing directors, and authorized signatories (*SMS Pharmaceuticals* / *Sunita Palita* benchmarks). |
| **Financial Capacity Defense** | Indian Evidence Act / BSA | *Basalingappa* (2019) challenge matrix against complainant financial solvency and ITR proof. |

---

## 🚀 Quick Start (Local Development)

### 1. Prerequisites

- Python 3.10+ (Recommended: Python 3.12)
- Git

### 2. Installation

```bash
# Clone the repository
git clone https://bitbucket.org/judiqai/judiq.git
cd judiq-ai

# Install backend dependencies
pip install -r backend/requirements.txt
```

### 3. Start Local Unified Server

```bash
cd backend
python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

### 4. Access the Application

- **Frontend Web App:** [http://localhost:8000/](http://localhost:8000/) *(or [http://127.0.0.1:8000/](http://127.0.0.1:8000/))*
- **API Documentation (Swagger UI):** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Health & Readiness Checks:**
  - Health: [http://localhost:8000/health](http://localhost:8000/health)
  - Readiness Probe: [http://localhost:8000/ready](http://localhost:8000/ready)
  - Liveness Probe: [http://localhost:8000/live](http://localhost:8000/live)
  - Prometheus Metrics: [http://localhost:8000/metrics](http://localhost:8000/metrics)

---

## 🧪 Automated Testing & Empirical Benchmarks

The active test suite covers 1,307 verified unit, integration, and benchmark tests:

```bash
# Run entire active test suite
python -m pytest backend/tests

# Run hard cases and S.141/142 director liability benchmarks
python -m pytest backend/tests/test_cheque_bounce_hard_cases.py -v
python -m pytest backend/tests/test_s141_s142_ultra_hard_cases.py -v
```

**Result:** `1307 passed (100% pass rate, 0 failures)`.

---

## 🏛️ Comprehensive Architecture & Engineering Guide

For an in-depth, file-by-file breakdown of the backend engines, data flow sequences, frontend SPA modules, token systems, and junior developer onboarding guides, please refer to:

👉 [**ARCHITECTURE.md**](file:///c:/Users/Atharva/OneDrive/Desktop/judiq-ai/ARCHITECTURE.md)

---

## 📁 Repository Structure

```text
judiq-ai/
├── backend/               # FastAPI Backend Engines & Precedent Registries
│   ├── core/              # BaseDomainEngine ABC & Case Registry
│   ├── cheque_bounce/     # Section 138 specialized litigation engine
│   ├── tests/             # 1,307 automated benchmark test cases
│   ├── main.py            # FastAPI entry point & unified static file mount
│   ├── documents.py       # PDF & Word court document endpoints
│   └── session.py         # Database manager & analytics store
│
├── frontend/              # Institutional Legal-Tech Frontend SPA
│   ├── js/                # Main controller & client modules (charts, simulator, dock)
│   ├── api.js             # Fetch client with auto JWT injection
│   ├── config.js          # Multi-domain wizard definitions & role mappings
│   ├── draft_templates.js # Legal draft templates (Section 138, Notices, Condonations)
│   ├── renderer.js        # SVG score gauge & strategy results renderer
│   ├── styles.css         # Obsidian dark theme, tokens & glassmorphism
│   ├── wizard.js          # Dynamic intake wizard state machine
│   └── index.html         # Master application shell
│
├── ARCHITECTURE.md        # Complete technical and architectural manual
└── README.md              # Platform overview & quick start
```
