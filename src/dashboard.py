import redis
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from src.common import Config
from src.storage import read_candles, read_latest
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Crypto Stream Analytics", layout="wide")

cfg = Config()
r = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, password=cfg.redis_password or None, decode_responses=True)

st.title("📈 Crypto Real-Time Analytics (Redis Streams + SQLite)")

symbol = st.selectbox("Symbol", list(cfg.symbols), index=0)
refresh = st.slider("Auto-refresh (seconds)", 1, 10, 2)

# ---- Tooltip metric cards (hover) ----
st.markdown("""
<style>
.metric-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: 6px; }
.metric-card { border: 1px solid rgba(250,250,250,0.12); border-radius: 14px; padding: 14px 14px 10px 14px; }
.metric-title { font-size: 0.9rem; opacity: 0.85; margin-bottom: 6px; }
.metric-value { font-size: 1.6rem; font-weight: 700; line-height: 1.1; }
.metric-sub { font-size: 0.8rem; opacity: 0.75; margin-top: 6px; }
</style>
""", unsafe_allow_html=True)

def metric_card(title: str, value: str, tooltip: str, sub: str = ""):
    # tooltip shows on hover via HTML title attr
    st.markdown(
        f"""
        <div class="metric-card" title="{tooltip.replace('"', '&quot;')}">
            <div class="metric-title">{title}</div>
            <div class="metric-value">{value}</div>
            {"<div class='metric-sub'>" + sub + "</div>" if sub else ""}
        </div>
        """,
        unsafe_allow_html=True
    )

latest = read_latest(r, f"latest:{symbol}")

# Optional: show live last-trade price if you added last_trade in consumer
last_trade = r.hgetall(f"last_trade:{symbol}")  # harmless even if missing

st.markdown('<div class="metric-grid">', unsafe_allow_html=True)
cols = st.columns(4)

with cols[0]:
    if last_trade and "price" in last_trade:
        metric_card(
            "Live Price",
            f"{float(last_trade['price']):.2f}",
            "Most recent trade price from the live Binance stream. Updates on every incoming trade."
        )
    elif latest:
        metric_card(
            "Close",
            f"{float(latest.get('close', 'nan')):.4f}",
            "Close = the last candle’s closing price (the final trade price within the candle window).",
            sub=f"Symbol: {symbol.upper()}"
        )
    else:
        metric_card("Close", "—", "Close = last candle’s closing price (needs candle data).")

with cols[1]:
    if latest and latest.get("rsi14") is not None:
        metric_card(
            "RSI14",
            f"{float(latest.get('rsi14', 'nan')):.2f}",
            "RSI (0–100) measures momentum over the last 14 periods. "
            "Rule of thumb: <30 oversold (possible bounce), >70 overbought (possible pullback), ~40–60 neutral."
        )
    else:
        metric_card("RSI14", "—", "RSI needs enough candles (usually 14+).")

with cols[2]:
    if latest and latest.get("macd") is not None:
        metric_card(
            "MACD",
            f"{float(latest.get('macd', 'nan')):.4f}",
            "MACD = EMA(12) − EMA(26). If MACD is >0, momentum is bullish; <0 bearish. "
            "More important than the value: MACD crossing above/below its signal line indicates potential trend shifts."
        )
    else:
        metric_card("MACD", "—", "MACD needs enough candles (usually 26+).")

with cols[3]:
    if latest and latest.get("bb_mid") is not None:
        metric_card(
            "BB Mid",
            f"{float(latest.get('bb_mid', 'nan')):.4f}",
            "Bollinger Bands middle line = 20-period moving average (a ‘fair value’ baseline). "
            "Price above BB Mid often signals strength; below suggests weakness (context matters)."
        )
    else:
        metric_card("BB Mid", "—", "Bollinger Bands need enough candles (usually 20+).")

# ---- Chart ----
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

    with st.expander("What do these indicators mean?"):
        st.markdown("""
- **Close**: last price at the end of the candle window.  
- **RSI14**: momentum (0–100). <30 oversold, >70 overbought (rules of thumb).  
- **MACD**: trend/momentum from EMA differences; watch **crossovers** more than raw value.  
- **BB Mid**: 20-period moving average baseline; price above = stronger, below = weaker (in context).  
""")

    st.subheader("Recent candles")
    st.dataframe(df.tail(50), use_container_width=True)
else:
    st.warning("No candle data yet (producer/consumer still warming up).")

st.caption("Hover the cards above to see what each metric means.")
st_autorefresh(interval=refresh * 1000, key="refresh")
