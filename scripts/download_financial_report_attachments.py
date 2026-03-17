import argparse
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings

BASE_URL = "https://www.idx.co.id"


def connect_db():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def build_url(file_path: str) -> str:
    if not file_path:
        return ""
    if file_path.startswith("http://") or file_path.startswith("https://"):
        return file_path
    return urllib.parse.urljoin(BASE_URL, file_path)


def sanitize_filename(name: str) -> str:
    if not name:
        return "file"
    name = name.replace("/", "_").replace("\\", "_")
    return name.strip() or "file"


def ensure_unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    for i in range(1, 1000):
        candidate = path.with_name(f"{stem}_{i}{suffix}")
        if not candidate.exists():
            return candidate
    return path


def download_file(url: str, dest: Path, timeout: int = 60) -> None:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
            "Accept": "*/*",
            "Referer": "https://www.idx.co.id/",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as fh:
            fh.write(response.read())


def fetch_attachments(conn, stock_code: str | None, year: int | None, limit: int | None):
    query = """
        SELECT stock_code, report_year, report_period, report_type, file_id,
               file_name, file_path, file_type, local_path
        FROM stock.financial_report_attachment
        WHERE file_path IS NOT NULL AND file_path <> ''
    """
    params = []
    if stock_code:
        query += " AND stock_code = %s"
        params.append(stock_code)
    if year:
        query += " AND report_year = %s"
        params.append(year)
    query += " ORDER BY stock_code, report_year, file_id"
    if limit:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()


def update_local_path(conn, key_tuple, local_path: str):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE stock.financial_report_attachment
            SET local_path = %s, downloaded_at = now()
            WHERE stock_code = %s AND report_year = %s AND report_period = %s
              AND report_type = %s AND file_id = %s
            """,
            (local_path, *key_tuple),
        )
    conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/financial_report_attachments")
    parser.add_argument("--stock-code", default=None)
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--skip-existing", action="store_true", help="Skip if local_path exists")
    parser.add_argument("--sleep", type=float, default=1.0)
    args = parser.parse_args()

    try:
        conn = connect_db()
    except Exception as exc:
        print(f"[DB ERROR] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    output_dir = Path(args.output_dir)

    try:
        rows = fetch_attachments(conn, args.stock_code, args.year, args.limit)
    except Exception as exc:
        print(f"[DB ERROR] {type(exc).__name__}: {exc}", file=sys.stderr)
        conn.close()
        return 1

    for row in rows:
        (
            stock_code,
            report_year,
            report_period,
            report_type,
            file_id,
            file_name,
            file_path,
            file_type,
            local_path,
        ) = row

        if args.skip_existing and local_path:
            if Path(local_path).exists():
                continue

        url = build_url(file_path)
        if not url:
            print(f"[SKIP] {stock_code} {report_year} {file_id} empty url", flush=True)
            continue

        filename = sanitize_filename(file_name or file_id)
        if file_type and not filename.lower().endswith(file_type.lower()):
            filename += file_type

        dest = output_dir / stock_code / str(report_year) / filename
        dest = ensure_unique_path(dest)

        print(f"[GET] {url} -> {dest}", flush=True)
        try:
            download_file(url, dest)
        except urllib.error.HTTPError as exc:
            print(f"[ERROR] {stock_code} {report_year} {file_id} HTTP {exc.code}", file=sys.stderr)
            time.sleep(args.sleep)
            continue
        except Exception as exc:
            print(f"[ERROR] {stock_code} {report_year} {file_id} {type(exc).__name__}: {exc}", file=sys.stderr)
            time.sleep(args.sleep)
            continue

        try:
            update_local_path(
                conn,
                (stock_code, report_year, report_period, report_type, file_id),
                str(dest),
            )
        except Exception as exc:
            conn.rollback()
            print(f"[DB ERROR] {stock_code} {report_year} {file_id} {type(exc).__name__}: {exc}", file=sys.stderr)

        time.sleep(args.sleep)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
