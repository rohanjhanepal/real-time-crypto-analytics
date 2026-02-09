import sqlite3
import logging
from typing import Optional

import redis
import pandas as pd

log = logging.getLogger("storage")

def init_sqlite(path: str) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            symbol TEXT NOT NULL,
            t_start_ms INTEGER NOT NULL,
            t_end_ms INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY(symbol, t_start_ms)
        );
        """)
        con.commit()
    finally:
        con.close()

def insert_candle(path: str, row: dict) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute("""
        INSERT OR REPLACE INTO candles(symbol,t_start_ms,t_end_ms,open,high,low,close,volume)
        VALUES(?,?,?,?,?,?,?,?)
        """, (
            row["symbol"], row["t_start_ms"], row["t_end_ms"],
            row["open"], row["high"], row["low"], row["close"], row["volume"]
        ))
        con.commit()
    finally:
        con.close()

def read_candles(path: str, symbol: str, limit: int = 500) -> pd.DataFrame:
    con = sqlite3.connect(path)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM candles WHERE symbol=? ORDER BY t_start_ms DESC LIMIT ?",
            con, params=(symbol, limit)
        )
        if df.empty:
            return df
        df = df.sort_values("t_start_ms")
        return df
    finally:
        con.close()

def write_latest(r: redis.Redis, key: str, mapping: dict) -> None:
    r.hset(key, mapping=mapping)

def read_latest(r: redis.Redis, key: str) -> Optional[dict]:
    m = r.hgetall(key)
    return m or None
