import asyncio
import json
import logging
from typing import Any

import websockets
import redis

from .common import Config, setup_logging, now_ms

log = logging.getLogger("producer")

def redis_client(cfg: Config) -> redis.Redis:
    return redis.Redis(
        host=cfg.redis_host,
        port=cfg.redis_port,
        password=cfg.redis_password or None,
        decode_responses=True,
    )

def stream_key(cfg: Config, symbol: str) -> str:
    return f"{cfg.stream_prefix}{symbol}"

def normalize_binance_trade(msg: dict[str, Any], symbol: str) -> dict[str, str]:
    # Binance aggTrade fields: e (event), E (event time), p (price), q (qty), m (is buyer market maker)
    price = float(msg["p"])
    qty = float(msg["q"])
    # If buyer is market maker => sell initiated (commonly interpreted)
    side = "sell" if msg.get("m") else "buy"
    ts = int(msg.get("E") or now_ms())
    return {
        "ts_ms": str(ts),
        "symbol": symbol,
        "price": f"{price:.10f}",
        "qty": f"{qty:.10f}",
        "side": side,
        "src": "binance",
    }

async def run_symbol(cfg: Config, r: redis.Redis, symbol: str) -> None:
    url = f"wss://stream.binance.com:9443/ws/{symbol}@aggTrade"
    skey = stream_key(cfg, symbol)

    backoff = 1.0
    while True:
        try:
            log.info("WS connect %s", url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                backoff = 1.0
                async for raw in ws:
                    msg = json.loads(raw)
                    event = normalize_binance_trade(msg, symbol)
                    # XADD with approximate trimming
                    r.xadd(skey, event, maxlen=cfg.stream_maxlen, approximate=True)
        except Exception as e:
            log.warning("WS error (%s): %s | reconnect in %.1fs", symbol, e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)

async def main() -> None:
    cfg = Config()
    setup_logging(cfg)
    r = redis_client(cfg)

    tasks = [
        asyncio.create_task(
            run_symbol(cfg, r, s)) for s in cfg.symbols
            ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
