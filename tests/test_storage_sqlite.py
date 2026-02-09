import os
import sqlite3
import tempfile

from src.storage import init_sqlite, insert_candle, read_candles


def test_sqlite_insert_and_read():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        init_sqlite(path)

        insert_candle(path, {
            "symbol": "btcusdt",
            "t_start_ms": 1000,
            "t_end_ms": 6000,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
        })

        df = read_candles(path, "btcusdt", limit=10)
        assert len(df) == 1
        row = df.iloc[0]
        assert row["symbol"] == "btcusdt"
        assert int(row["t_start_ms"]) == 1000
        assert float(row["open"]) == 1.0
        assert float(row["high"]) == 2.0
        assert float(row["low"]) == 0.5
        assert float(row["close"]) == 1.5
        assert float(row["volume"]) == 10.0
