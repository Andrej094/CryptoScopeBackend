import os
import sqlite3
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Thread

DB_PATH = "Program\\crypto.db"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

# Global HTTP session
session = requests.Session()
session.headers.update(HEADERS)


# ---------- DB helpers ----------

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            symbol TEXT NOT NULL,
            date   TEXT NOT NULL,
            open   REAL,
            high   REAL,
            low    REAL,
            close  REAL,
            volume REAL,
            PRIMARY KEY(symbol, date)
        );
        """
    )
    conn.commit()
    conn.close()


def db_writer(queue: Queue):

    conn = sqlite3.connect(DB_PATH, timeout=60)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()

    total_written = 0

    try:
        while True:
            item = queue.get()
            if item is None:
                break

            rows = item  # list of tuples (symbol, date, open, high, low, close, volume)
            if not rows:
                continue

            try:
                cur.executemany(
                    """
                    INSERT OR IGNORE INTO prices
                    (symbol, date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                    """,
                    rows,
                )
                conn.commit()
                total_written += len(rows)
            except Exception as e:
                print(f"[ERROR] DB insert batch failed: {e}")

    finally:
        conn.close()
        print(f"Total rows written in this run: {total_written}")


# ---------- Date helpers ----------

def _normalize_last_date(raw_last_date):

    if pd.isna(raw_last_date):
        return None
    s = str(raw_last_date).strip()
    if not s or s.lower() == "none":
        return None
    try:
        dt = pd.to_datetime(s).date()
        return dt
    except Exception:
        return None


def _compute_start_date(last_date: date | None, years_back: int = 10) -> date:

    today = date.today()
    n_years_ago = today - timedelta(days=365 * years_back)

    if last_date is None:
        start = n_years_ago
    else:
        start = max(last_date + timedelta(days=1), n_years_ago)

    if start >= today:
        return today
    return start


# ---------- Yahoo fetch ----------

def yahoo_fetch_range_rows(symbol: str, start_dt: date, end_dt: date) -> list[tuple]:

    if start_dt >= end_dt:
        return []

    period1 = int(datetime(start_dt.year, start_dt.month, start_dt.day).timestamp())
    period2 = int(datetime(end_dt.year, end_dt.month, end_dt.day).timestamp())

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "interval": "1d",
        "period1": period1,
        "period2": period2,
    }

    try:
        resp = session.get(url, params=params, timeout=10)
        data = resp.json()
    except Exception as e:
        print(f"[ERROR] Request failed for {symbol}: {e}")
        return []

    try:
        result = data["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        quote = result["indicators"]["quote"][0]
    except Exception:
        return []

    opens = quote.get("open", []) or []
    highs = quote.get("high", []) or []
    lows = quote.get("low", []) or []
    closes = quote.get("close", []) or []
    volumes = quote.get("volume", []) or []

    rows: list[tuple] = []
    for t, o, h, l, c, v in zip(timestamps, opens, highs, lows, closes, volumes):
        if o is None or h is None or l is None or c is None:
            continue
        dt_str = datetime.utcfromtimestamp(t).date().isoformat()
        rows.append(
            (
                symbol,
                dt_str,
                float(o),
                float(h),
                float(l),
                float(c),
                float(v) if v is not None else 0.0,
            )
        )

    return rows



def fetch_worker(symbol: str, raw_last_date):

    last_dt = _normalize_last_date(raw_last_date)
    start_dt = _compute_start_date(last_dt, years_back=10)
    today = date.today()
    end_dt = today + timedelta(days=1)  # exclusive

    if start_dt >= end_dt:
        return symbol, []

    rows = yahoo_fetch_range_rows(symbol, start_dt, end_dt)
    return symbol, rows


# ---------- Orchestrator ----------

def update_data(download_plan_csv: str = "Program\\download_plan.csv", workers: int | None = None):

    if not os.path.exists(download_plan_csv):
        raise FileNotFoundError(f"{download_plan_csv} not found. Run Filter 2 first.")

    plan = pd.read_csv(download_plan_csv)
    if "symbol" not in plan.columns:
        raise ValueError("download_plan.csv must contain a 'symbol' column.")

    ensure_db()

    symbols = plan["symbol"].astype(str).tolist()
    last_dates = plan["last_date"].tolist()

    if not symbols:
        print("No symbols in download_plan.csv – nothing to do.")
        return

    if workers is None:
        cpu = os.cpu_count() or 4
        workers = cpu * 4

    workers = min(workers, len(symbols))

    print(
        f"\nFilter 3: Starting Yahoo downloads (incremental, up to 10 years) "
        f"with {workers} fetch threads for {len(symbols)} symbols...\n"
    )

    q: Queue = Queue(maxsize=workers * 4)

    writer_thread = Thread(target=db_writer, args=(q,), daemon=True)
    writer_thread.start()

    total_rows = 0
    completed = 0

    # Fetch in parallel
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(fetch_worker, sym, last_date)
            for sym, last_date in zip(symbols, last_dates)
        ]

        for fut in as_completed(futures):
            try:
                sym, rows = fut.result()
                n_rows = len(rows)
                total_rows += n_rows
                completed += 1

                if n_rows > 0:
                    q.put(rows)  # send batch to writer

                if completed % 200 == 0 or completed == len(symbols):
                    print(
                        f"   Progress: {completed}/{len(symbols)} symbols done "
                        f"(total new rows fetched: {total_rows})"
                    )

            except Exception as e:
                print(f"Fetch worker crashed: {e}")

    q.put(None)
    writer_thread.join()

    print(f"\nFilter 3 complete. Total new rows fetched (and sent to DB): {total_rows}")


if __name__ == "__main__":
    update_data()
