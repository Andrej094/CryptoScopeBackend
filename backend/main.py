from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import pandas as pd

from filter1_scrape_symbols import get_symbols
from filter2_check_existing_data import get_existing_status
from filter3_download_missing import update_data

DB_PATH = "Program/crypto.db"
SYMBOLS_CSV = "Program/symbols.csv"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_connection():
    """
    Open a SQLite connection.
    Separate connection per request is fine for SQLite.
    """
    conn = sqlite3.connect(DB_PATH)
    return conn


def load_latest_and_change_all():
    """
    Load latest price/volume AND 24h change for ALL symbols
    with ONE scan over the prices table.

    Returns:
        latest:  { 'BTC-USD': {'price': 12345.0, 'vol': '12.3M'}, ... }
        changes: { 'BTC-USD': 1.23, ... }  # percentage
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
                SELECT symbol, date, close, volume
                FROM prices
                ORDER BY symbol ASC, date DESC
                """)

    latest = {}
    changes = {}

    current_symbol = None
    last_close = None

    for symbol, date, close, volume in cur.fetchall():
        if symbol != current_symbol:
            current_symbol = symbol
            last_close = None

        if symbol not in latest:
            latest[symbol] = {
                "price": float(close),
                "vol": f"{round(volume / 1_000_000, 2)}M"
            }
            last_close = float(close)

        elif symbol not in changes and last_close is not None:
            prev_close = float(close)
            if prev_close != 0:
                changes[symbol] = round(((last_close - prev_close) / prev_close) * 100, 2)

    conn.close()
    return latest, changes


@app.get("/api/symbols")
def api_symbols():
    """
    Returns list of symbols with:
        id, rank, symbol, name, price, vol, change
    All price/volume/change values precomputed in ONE DB scan.
    """
    df = pd.read_csv(SYMBOLS_CSV)

    df["id"] = range(1, len(df) + 1)
    df["rank"] = df["id"]
    df["name"] = df["symbol"].str.split("-").str[0]

    latest, changes = load_latest_and_change_all()

    def get_price(sym):
        info = latest.get(sym)
        return info["price"] if info else 0.0

    def get_vol(sym):
        info = latest.get(sym)
        return info["vol"] if info else "0"

    def get_change(sym):
        return changes.get(sym, 0.0)

    df["price"] = df["symbol"].apply(get_price)
    df["vol"] = df["symbol"].apply(get_vol)
    df["change"] = df["symbol"].apply(get_change)

    return df.to_dict(orient="records")


@app.get("/api/prices/{symbol}")
def api_prices(symbol: str):
    """
    Returns full OHLCV history for a single symbol.
    This is only called when user opens the chart,
    so one query per click is totally fine.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
                SELECT date, open, high, low, close, volume
                FROM prices
                WHERE symbol = ?
                ORDER BY date ASC
                """, (symbol,))
    rows = cur.fetchall()
    conn.close()

    return [
        {
            "date": r[0],
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "volume": r[5],
        }
        for r in rows
    ]


@app.post("/api/etl/run")
def api_run_etl():
    """
    Runs your full ETL pipeline:
    1. Fetch symbols
    2. Check existing data
    3. Download missing candles
    """
    get_symbols(limit=1000)
    get_existing_status()
    update_data()
    return {"status": "completed"}
