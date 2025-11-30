import os
import sqlite3
import pandas as pd

DB_PATH = "Program\\crypto.db"


def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
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


def get_existing_status(
    symbols_csv: str = "Program\\symbols.csv", output_csv: str = "Program\\download_plan.csv"
) -> pd.DataFrame:

    if not os.path.exists(symbols_csv):
        raise FileNotFoundError(f"{symbols_csv} not found. Run Filter 1 first.")

    ensure_db()

    symbols_df = pd.read_csv(symbols_csv)
    symbols = symbols_df["symbol"].astype(str).tolist()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    plan_rows = []
    for sym in symbols:
        cur.execute(
            "SELECT MAX(date) FROM prices WHERE symbol = ?;",
            (sym,),
        )
        row = cur.fetchone()
        last_date = row[0] if row and row[0] is not None else pd.NA
        plan_rows.append({"symbol": sym, "last_date": last_date})

    conn.close()

    plan_df = pd.DataFrame(plan_rows)
    plan_df.to_csv(output_csv, index=False)
    print(plan_df.head())
    return plan_df


if __name__ == "__main__":
    get_existing_status()
