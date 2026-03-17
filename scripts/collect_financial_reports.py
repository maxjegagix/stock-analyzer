import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings

ENDPOINT = "https://www.idx.co.id/primary/ListedCompany/GetFinancialReport"
BASE_URL = "https://www.idx.co.id"

DEFAULT_HEADERS = [
    "accept: application/json, text/plain, */*",
    "accept-language: en-US,en;q=0.9",
    "priority: u=1, i",
    "referer: https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan/",
    'sec-ch-ua: "Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    "sec-ch-ua-mobile: ?0",
    'sec-ch-ua-platform: "Linux"',
    "sec-fetch-dest: empty",
    "sec-fetch-mode: cors",
    "sec-fetch-site: same-origin",
    "user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
]


def build_url(stock_code: str, year: int, periode: str, report_type: str) -> str:
    return (
        f"{ENDPOINT}?periode={periode}&year={year}"
        f"&indexFrom=0&pageSize=1000&reportType={report_type}&kodeEmiten={stock_code}"
    )


def fetch_json(url: str) -> dict:
    cmd = ["curl", url]
    for header in DEFAULT_HEADERS:
        cmd.extend(["-H", header])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "curl failed")

    payload = result.stdout.strip()
    if not payload:
        raise RuntimeError("empty response")

    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON: {exc}") from exc


def connect_db():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def get_stock_codes(conn) -> list:
    with conn.cursor() as cursor:
        cursor.execute("SELECT stock_code FROM stock.company_profile ORDER BY stock_code")
        return [row[0] for row in cursor.fetchall()]


def upsert_to_db(conn, payload: dict) -> int:
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT stock.upsert_financial_reports_payload(%s::jsonb)",
            (json.dumps(payload),),
        )
        inserted = cursor.fetchone()[0]
    conn.commit()
    return inserted or 0


def build_url(stock_code: str, year: int, periode: str, report_type: str) -> str:
    return (
        f"{ENDPOINT}?periode={periode}&year={year}"
        f"&indexFrom=0&pageSize=1000&reportType={report_type}&kodeEmiten={stock_code}"
    )


def build_attachment_url(file_path: str) -> str:
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


def download_attachments(conn, payload: dict, output_dir: Path, skip_existing: bool, sleep_seconds: float):
    results = payload.get("Results")
    if not isinstance(results, list):
        return

    search = payload.get("Search") or {}
    fallback_year = search.get("Year")
    fallback_period = search.get("Periode")
    fallback_type = search.get("ReportType")

    for rec in results:
        attachments = rec.get("Attachments")
        if not isinstance(attachments, list):
            continue

        stock_code = rec.get("KodeEmiten") or ""
        report_year = rec.get("Report_Year") or fallback_year
        report_period = rec.get("Report_Period") or fallback_period or "unknown"
        report_type = rec.get("Report_Type") or fallback_type or "unknown"

        try:
            report_year_int = int(report_year) if report_year else None
        except ValueError:
            report_year_int = None

        for att in attachments:
            file_id = att.get("File_ID") or ""
            file_name = att.get("File_Name") or file_id
            file_path = att.get("File_Path") or ""
            file_type = att.get("File_Type") or ""

            url = build_attachment_url(file_path)
            if not url:
                print(f"[SKIP] {stock_code} {report_year} {file_id} empty url", flush=True)
                continue

            filename = sanitize_filename(file_name)
            if file_type and not filename.lower().endswith(file_type.lower()):
                filename += file_type

            if report_year_int is None:
                report_year_dir = "unknown_year"
            else:
                report_year_dir = str(report_year_int)

            dest = output_dir / stock_code / report_year_dir / filename
            dest = ensure_unique_path(dest)

            if skip_existing and dest.exists():
                continue

            print(f"[GET] {url} -> {dest}", flush=True)
            try:
                download_file(url, dest)
            except urllib.error.HTTPError as exc:
                print(f"[ERROR] {stock_code} {report_year} {file_id} HTTP {exc.code}", file=sys.stderr)
                time.sleep(sleep_seconds)
                continue
            except Exception as exc:
                print(f"[ERROR] {stock_code} {report_year} {file_id} {type(exc).__name__}: {exc}", file=sys.stderr)
                time.sleep(sleep_seconds)
                continue

            try:
                update_local_path(
                    conn,
                    (stock_code, report_year_int, report_period, report_type, file_id),
                    str(dest),
                )
            except Exception as exc:
                conn.rollback()
                print(f"[DB ERROR] {stock_code} {report_year} {file_id} {type(exc).__name__}: {exc}", file=sys.stderr)

            time.sleep(sleep_seconds)


def write_output(output_dir: Path, stock_code: str, year: int, payload: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"financial_report_{stock_code}_{year}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year-start", type=int, default=2010)
    parser.add_argument("--year-end", type=int, default=datetime.now().year)
    parser.add_argument("--periode", default="audit")
    parser.add_argument("--report-type", default="rdf")
    parser.add_argument("--stock-code", default=None, help="Jika diisi, hanya satu emiten")
    parser.add_argument("--sleep", type=float, default=2.0)
    parser.add_argument("--skip-db", action="store_true")
    parser.add_argument("--output-dir", default=None, help="Simpan response JSON per emiten per tahun")
    parser.add_argument("--no-output", action="store_true", help="Jangan tulis file output")
    parser.add_argument("--download-attachments", action="store_true", help="Download file lampiran")
    parser.add_argument(
        "--attachments-dir",
        default="data/financial_report_attachments",
        help="Folder output untuk lampiran",
    )
    parser.add_argument("--skip-existing", action="store_true", help="Skip file lampiran yang sudah ada")
    args = parser.parse_args()

    try:
        conn = connect_db()
    except Exception as exc:
        print(f"[DB ERROR] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    try:
        if args.stock_code:
            stock_codes = [args.stock_code]
        else:
            stock_codes = get_stock_codes(conn)
    except Exception as exc:
        print(f"[DB ERROR] {type(exc).__name__}: {exc}", file=sys.stderr)
        conn.close()
        return 1

    output_dir = None if args.no_output else (Path(args.output_dir) if args.output_dir else None)
    attachments_dir = Path(args.attachments_dir)

    for stock_code in stock_codes:
        for year in range(args.year_start, args.year_end + 1):
            url = build_url(stock_code, year, args.periode, args.report_type)
            print(f"[FETCH] {stock_code} {year} {url}", flush=True)

            try:
                payload = fetch_json(url)
            except RuntimeError as exc:
                print(f"[ERROR] {stock_code} {year} {exc}", file=sys.stderr)
                time.sleep(args.sleep)
                continue

            rows = payload.get("Results")
            if isinstance(rows, list):
                print(f"[INFO] results={len(rows)}", flush=True)
            else:
                print("[INFO] results=unknown", flush=True)

            if not args.skip_db:
                try:
                    inserted = upsert_to_db(conn, payload)
                    print(f"[UPSERT] results={inserted}", flush=True)
                except Exception as exc:
                    conn.rollback()
                    print(f"[DB ERROR] {stock_code} {year} {type(exc).__name__}: {exc}", file=sys.stderr)
                    time.sleep(args.sleep)
                    continue

            if output_dir:
                write_output(output_dir, stock_code, year, payload)

            if args.download_attachments:
                try:
                    download_attachments(
                        conn,
                        payload,
                        attachments_dir,
                        args.skip_existing,
                        args.sleep,
                    )
                except Exception as exc:
                    print(f"[ATTACH ERROR] {stock_code} {year} {type(exc).__name__}: {exc}", file=sys.stderr)

            time.sleep(args.sleep)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
