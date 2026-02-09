import redis
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from .common import Config
from .storage import read_candles, read_latest

st.set_page_config(page_title="Crypto Stream Analytics", layout="wide")

cfg = Config()
r = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, password=cfg.redis_password or None, decode_responses=True)

st.title("📈 Crypto Real-Time Analytics (Redis Streams + SQLite)")

symbol = st.selectbox("Symbol", list(cfg.symbols), index=0)
refresh = st.slider("Auto-refresh (seconds)", 1, 10, 2)

latest = read_latest(r, f"latest:{symbol}")
col1, col2, col3, col4 = st.columns(4)
if latest:
    col1.metric("Close", f"{float(latest.get('close', 'nan')):.4f}")
    col2.metric("RSI14", f"{float(latest.get('rsi14', 'nan')):.2f}")
    col3.metric("MACD", f"{float(latest.get('macd', 'nan')):.4f}")
    col4.metric("BB Mid", f"{float(latest.get('bb_mid', 'nan')):.4f}")
else:
    st.info("Waiting for enough candles to compute indicators...")

df = read_candles(cfg.sqlite_path, symbol, limit=500)
if not df.empty:
    fig = go.Figure(
        data=[go.Candlestick(
            x=pd.to_datetime(df["t_start_ms"], unit="ms"),
            open=df["open"], high=df["high"], low=df["low"], close=df["close"]
        )]
    )
    fig.update_layout(height=520, xaxis_title="Time", yaxis_title="Price")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Recent candles")
    st.dataframe(df.tail(50), use_container_width=True)
else:
    st.warning("No candle data yet (producer/consumer still warming up).")

st.caption("Tip: leave this open; it updates automatically.")
st.experimental_rerun() if refresh else None
