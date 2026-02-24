import requests
import sqlite3
import pandas as pd

# Example: Open source API (sample - replace with real API as needed)
API_URL = "https://api.coindesk.com/v1/bpi/currentprice.json"

def fetch_data():
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()

def transform_data(raw):
    # Example transformation: flatten and select fields
    bpi = raw["bpi"]
    records = [
        {"currency": k, "rate": v["rate_float"]} for k, v in bpi.items()
    ]
    return pd.DataFrame(records)

def setup_db():
    conn = sqlite3.connect("data_pipeline.db")
    cur = conn.cursor()
    # Bronze: raw data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_json TEXT
        )
    """)
    # Silver: cleaned/transformed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            currency TEXT,
            rate REAL
        )
    """)
    # Gold: analytics/final
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            currency TEXT,
            rate REAL,
            rate_category TEXT
        )
    """)
    conn.commit()
    return conn

def load_bronze(conn, raw):
    cur = conn.cursor()
    cur.execute("INSERT INTO bronze (raw_json) VALUES (?)", (str(raw),))
    conn.commit()

def load_silver(conn, df):
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute("INSERT INTO silver (currency, rate) VALUES (?, ?)", (row["currency"], row["rate"]))
    conn.commit()

def load_gold(conn, df):
    cur = conn.cursor()
    for _, row in df.iterrows():
        category = "high" if row["rate"] > 30000 else "low"
        cur.execute("INSERT INTO gold (currency, rate, rate_category) VALUES (?, ?, ?)", (row["currency"], row["rate"], category))
    conn.commit()

def main():
    raw = fetch_data()
    df = transform_data(raw)
    conn = setup_db()
    load_bronze(conn, raw)
    load_silver(conn, df)
    load_gold(conn, df)
    conn.close()

if __name__ == "__main__":
    main()
