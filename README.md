Production-grade FastAPI stock analyzer

Run locally:

pip install -r requirements.txt
uvicorn app.main:app --reload

Run docker:

docker compose up --build

Run IDX pull scheduler (daily):

python scripts/schedule_collect_idx_to_db.py --time 19:00 --run-now

Scheduler options:

- Run once now, then continue every day at 19:00:
  `python scripts/schedule_collect_idx_to_db.py --time 19:00 --run-now`
- Run only on daily schedule (without immediate first run):
  `python scripts/schedule_collect_idx_to_db.py --time 19:00`
