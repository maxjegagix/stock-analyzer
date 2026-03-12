import argparse
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings

WORKER_PATH = PROJECT_ROOT / "app" / "workers" / "pull-data" / "collect_idx_to_db.py"

DB_CONFIG = {
    "host": settings.DB_HOST,
    "port": settings.DB_PORT,
    "dbname": settings.DB_NAME,
    "user": settings.DB_USER,
    "password": settings.DB_PASSWORD,
}


def get_max_trade_date():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(trade_date) FROM stock.summary;")
            return cursor.fetchone()[0]
    finally:
        conn.close()


def run_collect_job():
    today = date.today()
    max_trade_date = get_max_trade_date()

    start_date = max_trade_date or today
    end_date = today

    if start_date > end_date:
        print(
            f"[SKIP] start_date ({start_date}) lebih besar dari end_date ({end_date})",
            flush=True,
        )
        return

    print(
        f"[RUN] collect_idx_to_db.py --start {start_date} --end {end_date}",
        flush=True,
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = (
        str(PROJECT_ROOT)
        if not env.get("PYTHONPATH")
        else f"{PROJECT_ROOT}:{env['PYTHONPATH']}"
    )
    result = subprocess.run(
        [
            sys.executable,
            str(WORKER_PATH),
            "--start",
            start_date.isoformat(),
            "--end",
            end_date.isoformat(),
        ],
        env=env,
    )

    if result.returncode != 0:
        print(f"[ERROR] job gagal dengan exit code {result.returncode}", flush=True)
    else:
        print("[DONE] job selesai", flush=True)


def parse_time(value):
    try:
        return datetime.strptime(value, "%H:%M").time()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Format waktu harus HH:MM") from exc


def seconds_until_next_run(run_time):
    now = datetime.now()
    next_run = datetime.combine(now.date(), run_time)
    if next_run <= now:
        next_run += timedelta(days=1)
    wait_seconds = (next_run - now).total_seconds()
    return wait_seconds, next_run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--time",
        type=parse_time,
        default=parse_time("19:00"),
        help="Jam run harian (HH:MM), default 19:00",
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Jalankan sekali saat startup, lalu lanjut schedule harian",
    )
    args = parser.parse_args()

    print(f"[SCHEDULER] worker={WORKER_PATH}", flush=True)
    print(
        f"[DB] connect host={DB_CONFIG['host']} port={DB_CONFIG['port']} db={DB_CONFIG['dbname']}",
        flush=True,
    )

    if args.run_now:
        run_collect_job()

    while True:
        wait_seconds, next_run = seconds_until_next_run(args.time)
        print(
            f"[WAIT] next_run={next_run.strftime('%Y-%m-%d %H:%M:%S')} "
            f"(in {int(wait_seconds)}s)",
            flush=True,
        )
        time.sleep(wait_seconds)
        run_collect_job()


if __name__ == "__main__":
    main()
