import numpy as np
import pandas as pd

from src.indicators import sma, ema, rsi, macd, bollinger


def test_sma_basic():
    s = pd.Series([1, 2, 3, 4, 5], dtype=float)
    out = sma(s, 3)
    assert np.isnan(out.iloc[0])
    assert np.isnan(out.iloc[1])
    assert out.iloc[2] == 2.0  # (1+2+3)/3
    assert out.iloc[4] == 4.0  # (3+4+5)/3


def test_ema_monotonic_increasing():
    s = pd.Series(np.arange(1, 51), dtype=float)
    out = ema(s, 10)
    # EMA should be non-decreasing for strictly increasing series
    assert (out.diff().dropna() >= -1e-12).all()


def test_rsi_range_and_trend():
    # Increasing series should push RSI high (eventually)
    s_up = pd.Series(np.arange(1, 200), dtype=float)
    r_up = rsi(s_up, 14)
    last = r_up.iloc[-1]
    assert 0 <= last <= 100
    assert last > 60  # should be relatively high for steady uptrend

    # Decreasing series should push RSI low (eventually)
    s_dn = pd.Series(np.arange(200, 1, -1), dtype=float)
    r_dn = rsi(s_dn, 14)
    last2 = r_dn.iloc[-1]
    assert 0 <= last2 <= 100
    assert last2 < 40  # should be relatively low for steady downtrend


def test_macd_shapes_and_consistency():
    s = pd.Series(np.linspace(100, 200, 300), dtype=float)
    m, sig, hist = macd(s)
    assert len(m) == len(s)
    assert len(sig) == len(s)
    assert len(hist) == len(s)
    # hist = macd - signal (allow tiny floating error)
    assert np.allclose(hist.values, (m - sig).values, atol=1e-10, equal_nan=True)


def test_bollinger_ordering():
    s = pd.Series(np.linspace(100, 120, 200), dtype=float)
    lo, mid, up = bollinger(s, n=20, k=2.0)
    # Where defined (not NaN), lower <= mid <= upper
    mask = mid.notna() & lo.notna() & up.notna()
    assert (lo[mask] <= mid[mask]).all()
    assert (mid[mask] <= up[mask]).all()
