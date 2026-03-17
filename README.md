Production-grade FastAPI stock analyzer

Run locally:

pip install -r requirements.txt
uvicorn app.main:app --reload

Run docker:

docker compose up --build

Run IDX pull scheduler (daily):

python scripts/schedule_collect_idx_to_db.py --time 19:00 --run-now

Collect IDX company profiles (manual):

python scripts/collect_company_profiles.py

Optional flags:

python scripts/collect_company_profiles.py --emiten-type s --start 0 --length 9999 --output data/company_profiles.json

Notes:
- Default behavior: upsert to DB via stock.upsert_company_profiles.
- Use --skip-db to skip DB and only write file (requires --output).

Collect IDX financial reports (manual, per emiten per year):

python scripts/collect_financial_reports.py --year-start 2010 --year-end 2026

Optional flags:

python scripts/collect_financial_reports.py --stock-code BBCA --year-start 2018 --year-end 2026 --output-dir data/financial_reports

Skip writing JSON output (DB upsert only):

python scripts/collect_financial_reports.py --stock-code BBCA --year-start 2018 --year-end 2026 --no-output

Financial report storage schema:
- stock.financial_report: summary per emiten/tahun/periode/report_type
- stock.financial_report_attachment: detail file lampiran (PDF/XLSX/ZIP, dll)

Download financial report attachments (cache to disk, end-to-end in collector):

python scripts/collect_financial_reports.py --stock-code BBCA --year-start 2018 --year-end 2026 --download-attachments --attachments-dir data/financial_report_attachments --skip-existing

Notes:
- Downloaded files are stored on disk and local path is saved to stock.financial_report_attachment.local_path.

Scheduler options:

- Run once now, then continue every day at 19:00:
  `python scripts/schedule_collect_idx_to_db.py --time 19:00 --run-now`
- Run only on daily schedule (without immediate first run):
  `python scripts/schedule_collect_idx_to_db.py --time 19:00`
