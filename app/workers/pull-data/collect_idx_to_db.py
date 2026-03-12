import argparse
import json
import subprocess
import time
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from app.core.config import settings

ENDPOINT = "https://www.idx.co.id/primary/TradingSummary/GetStockSummary"

DB_CONFIG = {
    "host": settings.DB_HOST,
    "port": settings.DB_PORT,
    "dbname": settings.DB_NAME,
    "user": settings.DB_USER,
    "password": settings.DB_PASSWORD,
}


def fetch_one_day(date_str):
    cmd = [
        "curl",
        f"{ENDPOINT}?length=9999&start=0&date={date_str}",
        "-H",
        "accept: application/json, text/plain, */*",
        "-H",
        "accept-language: en-US,en;q=0.9",
        "-H",
        "priority: u=1, i",
        "-H",
        "referer: https://www.idx.co.id/id/data-pasar/ringkasan-perdagangan/ringkasan-saham/",
        "-H",
        'sec-ch-ua: "Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "-H",
        "sec-ch-ua-mobile: ?0",
        "-H",
        'sec-ch-ua-platform: "Linux"',
        "-H",
        "sec-fetch-dest: empty",
        "-H",
        "sec-fetch-mode: cors",
        "-H",
        "sec-fetch-site: same-origin",
        "-H",
        "user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"[ERROR] {date_str} curl failed")
            print(result.stderr)
            return None

        json_string = result.stdout.strip()
        if not json_string:
            print(f"[EMPTY] {date_str}")
            return None

        data = json.loads(json_string)
        rows = data.get("data", [])
        if not rows:
            print(f"[EMPTY DATA] {date_str}")
            return None

        return rows
    except Exception as e:
        print(f"[ERROR] {date_str} {e}")
        return None


def upsert_rows(rows, conn):
    df = pd.DataFrame(rows)

    df.rename(
        columns={
            "IDStockSummary": "id_stock_summary",
            "Date": "trade_date",
            "StockCode": "stock_code",
            "StockName": "stock_name",
            "Remarks": "remarks",
            "Previous": "previous",
            "OpenPrice": "open_price",
            "FirstTrade": "first_trade",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Change": "change",
            "Volume": "volume",
            "Value": "value",
            "Frequency": "frequency",
            "IndexIndividual": "index_individual",
            "Offer": "offer",
            "OfferVolume": "offer_volume",
            "Bid": "bid",
            "BidVolume": "bid_volume",
            "ListedShares": "listed_shares",
            "TradebleShares": "tradeable_shares",
            "WeightForIndex": "weight_for_index",
            "ForeignSell": "foreign_sell",
            "ForeignBuy": "foreign_buy",
            "DelistingDate": "delisting_date",
            "NonRegularVolume": "non_regular_volume",
            "NonRegularValue": "non_regular_value",
            "NonRegularFrequency": "non_regular_frequency",
            "persen": "persen",
            "percentage": "percentage",
        },
        inplace=True,
    )

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    if "delisting_date" in df.columns:
        df["delisting_date"] = pd.to_datetime(df["delisting_date"], errors="coerce").dt.date

    df = df.astype(object).where(pd.notnull(df), None)

    sql = """
    INSERT INTO stock.summary (
        trade_date, stock_code,
        id_stock_summary, stock_name, remarks,
        previous, open_price, first_trade, high, low, close, change,
        volume, value, frequency,
        index_individual,
        offer, offer_volume, bid, bid_volume,
        listed_shares, tradeable_shares, weight_for_index,
        foreign_sell, foreign_buy,
        delisting_date,
        non_regular_volume, non_regular_value, non_regular_frequency,
        persen, percentage
    )
    VALUES (
        %(trade_date)s, %(stock_code)s,
        %(id_stock_summary)s, %(stock_name)s, %(remarks)s,
        %(previous)s, %(open_price)s, %(first_trade)s, %(high)s, %(low)s, %(close)s, %(change)s,
        %(volume)s, %(value)s, %(frequency)s,
        %(index_individual)s,
        %(offer)s, %(offer_volume)s, %(bid)s, %(bid_volume)s,
        %(listed_shares)s, %(tradeable_shares)s, %(weight_for_index)s,
        %(foreign_sell)s, %(foreign_buy)s,
        %(delisting_date)s,
        %(non_regular_volume)s, %(non_regular_value)s, %(non_regular_frequency)s,
        %(persen)s, %(percentage)s
    )
    ON CONFLICT (trade_date, stock_code)
    DO UPDATE SET
        id_stock_summary = EXCLUDED.id_stock_summary,
        stock_name = EXCLUDED.stock_name,
        remarks = EXCLUDED.remarks,
        previous = EXCLUDED.previous,
        open_price = EXCLUDED.open_price,
        first_trade = EXCLUDED.first_trade,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        change = EXCLUDED.change,
        volume = EXCLUDED.volume,
        value = EXCLUDED.value,
        frequency = EXCLUDED.frequency,
        index_individual = EXCLUDED.index_individual,
        offer = EXCLUDED.offer,
        offer_volume = EXCLUDED.offer_volume,
        bid = EXCLUDED.bid,
        bid_volume = EXCLUDED.bid_volume,
        listed_shares = EXCLUDED.listed_shares,
        tradeable_shares = EXCLUDED.tradeable_shares,
        weight_for_index = EXCLUDED.weight_for_index,
        foreign_sell = EXCLUDED.foreign_sell,
        foreign_buy = EXCLUDED.foreign_buy,
        delisting_date = EXCLUDED.delisting_date,
        non_regular_volume = EXCLUDED.non_regular_volume,
        non_regular_value = EXCLUDED.non_regular_value,
        non_regular_frequency = EXCLUDED.non_regular_frequency,
        persen = EXCLUDED.persen,
        percentage = EXCLUDED.percentage;
    """

    records = df.to_dict(orient="records")
    cursor = conn.cursor()
    for row in records:
        cursor.execute(sql, row)

    conn.commit()
    cursor.close()
    print(f"[UPSERT] {len(records)} rows")


def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    print(f"[START] range={start}..{end}", flush=True)
    print(
        f"[DB] connect host={DB_CONFIG['host']} port={DB_CONFIG['port']} db={DB_CONFIG['dbname']} user={DB_CONFIG['user']}",
        flush=True,
    )

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"[DB ERROR] {type(e).__name__}: {e}", flush=True)
        print("[ABORT] gagal konek ke PostgreSQL", flush=True)
        return

    try:
        for day in daterange(start, end):
            date_str = day.strftime("%Y%m%d")
            print(f"[FETCH] {date_str}", flush=True)

            rows = fetch_one_day(date_str)
            if rows:
                upsert_rows(rows, conn)
            else:
                print(f"[SKIP] {date_str} no data", flush=True)

            time.sleep(4)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
