import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings

ENDPOINT = "https://www.idx.co.id/primary/ListedCompany/GetCompanyProfiles"

DEFAULT_HEADERS = [
    "accept: application/json, text/plain, */*",
    "accept-language: en-US,en;q=0.9",
    "priority: u=1, i",
    "referer: https://www.idx.co.id/id/perusahaan-tercatat/profil-perusahaan-tercatat/",
    'sec-ch-ua: "Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    "sec-ch-ua-mobile: ?0",
    'sec-ch-ua-platform: "Linux"',
    "sec-fetch-dest: empty",
    "sec-fetch-mode: cors",
    "sec-fetch-site: same-origin",
    "user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
]


def build_url(emiten_type: str, start: int, length: int) -> str:
    return f"{ENDPOINT}?emitenType={emiten_type}&start={start}&length={length}"


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


def write_output(output_path: Path, payload: dict) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def upsert_to_db(rows: list) -> int:
    if not rows:
        return 0

    conn = psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT stock.upsert_company_profiles(%s::jsonb)",
                (json.dumps(rows),),
            )
            inserted = cursor.fetchone()[0]
        conn.commit()
        return inserted or 0
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emiten-type", default="s", help="Default s (saham)")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--length", type=int, default=9999)
    parser.add_argument(
        "--output",
        default=None,
        help="Output path (default: data/company_profiles_YYYYMMDD.json)",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Skip upsert to database (only write file if --output set)",
    )
    args = parser.parse_args()

    url = build_url(args.emiten_type, args.start, args.length)
    print(f"[FETCH] {url}", flush=True)

    try:
        payload = fetch_json(url)
    except RuntimeError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    data_rows = payload.get("data")
    if isinstance(data_rows, list):
        print(f"[INFO] rows={len(data_rows)}", flush=True)
    else:
        print("[INFO] rows=unknown", flush=True)

    if not args.skip_db:
        try:
            inserted = upsert_to_db(data_rows if isinstance(data_rows, list) else [])
            print(f"[UPSERT] rows={inserted}", flush=True)
        except Exception as exc:
            print(f"[DB ERROR] {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1

    if args.output:
        output_path = Path(args.output)
        write_output(output_path, payload)
        print(f"[DONE] saved to {output_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
