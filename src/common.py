import os
import time
import json
import logging
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _env(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None or v == "" else v

@dataclass(frozen=True)
class Config:
    redis_host: str = _env("REDIS_HOST", "localhost")
    redis_port: int = int(_env("REDIS_PORT", "6379"))
    redis_password: str = _env("REDIS_PASSWORD", "")

    symbols: tuple[str, ...] = tuple(s.strip().lower() for s in _env("SYMBOLS", "btcusdt").split(",") if s.strip())
    candle_sec: int = int(_env("CANDLE_SEC", "5"))

    sqlite_path: str = _env("SQLITE_PATH", "./data/crypto.db")

    stream_prefix: str = _env("STREAM_PREFIX", "trades:")
    consumer_group: str = _env("CONSUMER_GROUP", "cg_analytics")
    consumer_name: str = _env("CONSUMER_NAME", "worker-1")

    stream_maxlen: int = int(_env("STREAM_MAXLEN", "20000"))
    dlq_stream: str = _env("DLQ_STREAM", "trades:DLQ")

    log_level: str = _env("LOG_LEVEL", "INFO")

def setup_logging(cfg: Config) -> None:
    lvl = getattr(logging, cfg.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

def now_ms() -> int:
    return int(time.time() * 1000)

def jdump(obj) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
