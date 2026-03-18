# 📊 Stock Analyzer

[![FastAPI](https://img.shields.io/badge/FastAPI-production--grade-success?style=flat-square&logo=fastapi)](https://fastapi.tiangolo.com/) [![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=flat-square&logo=python)](https://www.python.org/) [![Docker](https://img.shields.io/badge/Docker-Ready-informational?style=flat-square&logo=docker)](https://www.docker.com/)

> 🚀 **Production-grade FastAPI stock analyzer for IDX (Indonesia Stock Exchange) data**

---

## 🎯 Table of Contents
- [🏃 Quick Start](#-quick-start)
- [🐳 Docker Setup](#-docker-setup)
- [⚙️ Schedulers & Data Collection](#️-schedulers--data-collection)
- [📈 Financial Reports](#-financial-reports)
- [📝 Database Schema](#-database-schema)

---

## 🏃 Quick Start

### Run Locally
Get up and running in minutes:

```bash
# Install dependencies
pip install -r requirements.txt

# Start the development server
uvicorn app.main:app --reload
```

✨ The API will be available at `http://localhost:8000`

### 📮 Postman (Import API Collection via URL)

1. Start the API server:
   ```bash
   uvicorn app.main:app --reload
   ```
2. In Postman: **Import → Link**
3. Use this URL:
   ```
   http://localhost:8000/openapi.json
   ```

---

## 🐳 Docker Setup

Build and run with Docker Compose:

```bash
docker compose up --build
```

---

## ⚙️ Schedulers & Data Collection

### 📅 IDX Pull Scheduler (Daily)
Set up automated daily data collection:

```bash
# Run once now, then every day at 19:00
python scripts/schedule_collect_idx_to_db.py --time 19:00 --run-now

# Only schedule (without immediate run)
python scripts/schedule_collect_idx_to_db.py --time 19:00
```

### 🏢 Collect IDX Company Profiles (Manual)

**Basic usage:**
```bash
python scripts/collect_company_profiles.py
```

**With optional flags:**
```bash
python scripts/collect_company_profiles.py \
  --emiten-type s \
  --start 0 \
  --length 9999 \
  --output data/company_profiles.json
```

> 📌 **Default Behavior:** Data is upserted to DB via `stock.upsert_company_profiles`
> 
> 💡 **Skip DB:** Use `--skip-db` flag to write only to file (requires `--output`)

---

## 📈 Financial Reports

### 📊 Collect Financial Reports (Manual)

**Collect for all stocks across years:**
```bash
python scripts/collect_financial_reports.py --year-start 2010 --year-end 2026
```

**Collect for specific stock:**
```bash
python scripts/collect_financial_reports.py \
  --stock-code BBCA \
  --year-start 2018 \
  --year-end 2026 \
  --output-dir data/financial_reports
```

**Skip JSON output (DB only):**
```bash
python scripts/collect_financial_reports.py \
  --stock-code BBCA \
  --year-start 2018 \
  --year-end 2026 \
  --no-output
```

### 📥 Download Financial Report Attachments

Cache attachments locally and save paths to database:

```bash
python scripts/collect_financial_reports.py \
  --stock-code BBCA \
  --year-start 2018 \
  --year-end 2026 \
  --download-attachments \
  --attachments-dir data/financial_report_attachments \
  --skip-existing
```

> 💾 Downloaded files are stored on disk and local paths are saved to `stock.financial_report_attachment.local_path`

---

## 📝 Database Schema

### 📋 Financial Report Storage

The system uses two main tables for financial reporting:

| Table | Purpose |
|-------|---------|
| **`stock.financial_report`** | Summary per emiten/tahun/periode/report_type |
| **`stock.financial_report_attachment`** | Detail file lampiran (PDF/XLSX/ZIP, etc.) |

---

## 🌟 Key Features

✅ Real-time IDX stock data collection  
✅ Automated daily scheduling  
✅ Financial report aggregation  
✅ Attachment file management  
✅ Database integration  
✅ Production-ready FastAPI framework

---

## 📧 Support & Contributing

Have questions or issues? Feel free to open an issue on GitHub!

**Happy analyzing! 📊✨**
