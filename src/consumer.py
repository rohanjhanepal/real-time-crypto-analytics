import time
import logging
from collections import defaultdict
from dataclasses import dataclass

import redis
import pandas as pd

from src.common import Config, setup_logging
from src.storage import init_sqlite, insert_candle, write_latest
from src.indicators import rsi, macd, bollinger, sma, ema

log = logging.getLogger("consumer")

@dataclass
class Candle:
    t_start_ms: int
    t_end_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float

def redis_client(cfg: Config) -> redis.Redis:
    return redis.Redis(
        host=cfg.redis_host,
        port=cfg.redis_port,
        password=cfg.redis_password or None,
        decode_responses=True,
    )

def stream_key(cfg: Config, symbol: str) -> str:
    return f"{cfg.stream_prefix}{symbol}"

def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    '''
    Create a consumer group on a stream if it doesnt exist.
    If it already exists, Redis throws BUSYGROUP and we ignore it.
    '''
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            return
        raise

def floor_bucket(ts_ms: int, bucket_ms: int) -> int:
    '''
    consumer takes trades and buckets them into fixed time windows.
    This function computes bucket for trades.
    '''
    return (ts_ms // bucket_ms) * bucket_ms

def compute_and_cache(cfg: Config, r: redis.Redis, symbol: str) -> None:
    # Read recent candles from SQLite for indicator computation 
    import sqlite3
    con = sqlite3.connect(cfg.sqlite_path)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM candles WHERE symbol=? ORDER BY t_start_ms DESC LIMIT 300",
            con, params=(symbol,)
        )
    finally:
        con.close()

    if df.empty or len(df) < 30:
        return

    df = df.sort_values("t_start_ms")
    close = df["close"]

    df["sma20"] = sma(close, 20)
    df["ema20"] = ema(close, 20)
    df["rsi14"] = rsi(close, 14)

    m, s, h = macd(close)
    df["macd"] = m
    df["macd_signal"] = s
    df["macd_hist"] = h

    lo, mid, up = bollinger(close, 20, 2.0)
    df["bb_lower"] = lo
    df["bb_mid"] = mid
    df["bb_upper"] = up

    last = df.iloc[-1].to_dict()

    latest_key = f"latest:{symbol}"
    write_latest(r, latest_key, {k: str(v) for k, v in last.items() if v == v})  # v==v skips NaN

def main() -> None:
    cfg = Config()
    setup_logging(cfg)
    r = redis_client(cfg)
    init_sqlite(cfg.sqlite_path)

    bucket_ms = cfg.candle_sec * 1000
    streams = [stream_key(cfg, s) for s in cfg.symbols]

    # Ensure consumer groups exist
    for st in streams:
        ensure_group(r, st, cfg.consumer_group)

    # Per-symbol in-progress candle
    current = {}

    while True:
        try:
            resp = r.xreadgroup(
                groupname=cfg.consumer_group,
                consumername=cfg.consumer_name,
                streams={st: ">" for st in streams},
                count=100,
                block=2000,
            )
            if not resp:
                continue

            for st, msgs in resp:
                # stream name is trades:<symbol>
                symbol = st.split(":")[-1]
                for msg_id, fields in msgs:
                    try:
                        ts = int(fields["ts_ms"])
                        price = float(fields["price"])
                        qty = float(fields["qty"])

                        t0 = floor_bucket(ts, bucket_ms)
                        t1 = t0 + bucket_ms

                        c = current.get(symbol)
                        if c is None or c.t_start_ms != t0:
                            # flush previous candle if exists
                            if c is not None:
                                insert_candle(cfg.sqlite_path, {
                                    "symbol": symbol,
                                    "t_start_ms": c.t_start_ms,
                                    "t_end_ms": c.t_end_ms,
                                    "open": c.open,
                                    "high": c.high,
                                    "low": c.low,
                                    "close": c.close,
                                    "volume": c.volume,
                                })
                                compute_and_cache(cfg, r, symbol)

                            # start new candle
                            c = Candle(t_start_ms=t0, t_end_ms=t1, open=price, high=price, low=price, close=price, volume=qty)
                            current[symbol] = c
                        else:
                            # update candle
                            c.high = max(c.high, price)
                            c.low = min(c.low, price)
                            c.close = price
                            c.volume += qty
                            current[symbol] = c

                        r.xack(st, cfg.consumer_group, msg_id)
                    except Exception as e:
                        log.exception("Bad message %s %s: %s", st, msg_id, e)
                        # send to dead letter queue
                        r.xadd(cfg.dlq_stream, {"stream": st, "id": msg_id, "err": str(e), "fields": str(fields)})
                        r.xack(st, cfg.consumer_group, msg_id)

        except Exception as e:
            log.exception("Consumer loop error: %s", e)
            time.sleep(1.0)

if __name__ == "__main__":
    main()
