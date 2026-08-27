# indicators.py
# Fibonacci Retracement Python Trading Application - Mathematical Engines
# This file contains the pure technical indicators including ATR, EMA, and our high-performance Adaptive CCI.

import numpy as np
import pandas as pd
import config

def calculate_atr(df, period=config.ATR_PERIOD):
    """
    Calculates the standard Average True Range (ATR) over a given period [2].
    Formula:
      True Range (TR) = Max(High - Low, Abs(High - Close_prev), Abs(Low - Close_prev))
      ATR = Wilders Moving Average (or EMA) of TR.
    """
    high = df['High']
    low = df['Low']
    close_prev = df['Close'].shift(1)
    
    tr1 = high - low
    tr2 = (high - close_prev).abs()
    tr3 = (low - close_prev).abs()
    
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # Wilder's smoothing (EMA with alpha = 1 / period)
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    return atr

def calculate_ema(series, period):
    """
    Calculates the standard Exponential Moving Average (EMA).
    """
    return series.ewm(span=period, adjust=False).mean()

def calculate_cci(df, period=config.ACCI_CCI_PERIOD):
    """
    Calculates the standard Commodity Channel Index (CCI) [5].
    Formula:
      TP (Typical Price) = (High + Low + Close) / 3
      SMA_TP = SMA(TP, period)
      Mean Deviation = Mean(Abs(TP - SMA_TP)) over period
      CCI = (TP - SMA_TP) / (0.015 * Mean Deviation)
    """
    tp = (df['High'] + df['Low'] + df['Close']) / 3
    sma_tp = tp.rolling(window=period).mean()
    
    # Custom mean absolute deviation to match MT5 exactly
    def mad(x):
        return np.mean(np.abs(x - np.mean(x)))
        
    mean_dev = tp.rolling(window=period).apply(mad, raw=True)
    
    cci = (tp - sma_tp) / (0.015 * mean_dev)
    # Fill NaNs with 0 to prevent downstream breaking
    return cci.fillna(0)

class AdaptiveCCIReading:
    """
    A simple container class representing a single candle's ACCI readings [35, 36].
    Matches the original 'AdaptiveCCIReading' structure.
    """
    def __init__(self, cci0, cci1, up0, up1, dn0, dn1, ok=True):
        self.cci0 = cci0  # Current CCI [35]
        self.cci1 = cci1  # Previous CCI [36]
        self.up0 = up0    # Current Upper Band [36]
        self.up1 = up1    # Previous Upper Band [36]
        self.dn0 = dn0    # Current Lower Band [36]
        self.dn1 = dn1    # Previous Lower Band [36]
        self.ok = ok      # Calculation validation status [35]

def get_adaptive_cci_series(df):
    """
    Generates time-series arrays for CCI and its adaptive bands.
    Adaptive Band Logic:
      - We start with a base threshold of ACCI_BASE_THRESHOLD (e.g., 100) [5].
      - We scale the band width based on the ratio of short-term ATR to long-term ATR.
      - During periods of high volatility, bands expand to filter out noise.
      - During low volatility, bands contract back to capture breakout momentum.
      - Bands are smoothed using an EMA to prevent erratic triggers [5].
    """
    cci = calculate_cci(df, config.ACCI_CCI_PERIOD)
    atr = calculate_atr(df, config.ACCI_ATR_PERIOD)
    
    # Rolling average ATR serves as the benchmark of historical volatility
    atr_baseline = atr.rolling(window=config.ACCI_ATR_PERIOD * 3).mean().fillna(atr)
    
    # Calculate volatility ratio
    volatility_ratio = (atr / atr_baseline).fillna(1.0)
    
    # Calculate the dynamic band width offset
    # Band = Base_Threshold * (1 + VolatilityFactor * (Ratio - 1))
    band_offset = config.ACCI_BASE_THRESHOLD * (1.0 + config.ACCI_VOLATILITY_FACTOR * (volatility_ratio - 1.0))
    band_offset = band_offset.clip(lower=config.ACCI_BASE_THRESHOLD * 0.5) # Prevent bands from collapsing too much
    
    # Smooth the offsets using EMA smoothing factor [5]
    smoothed_offset = band_offset.ewm(alpha=config.ACCI_EMA_SMOOTHING, adjust=False).mean()
    
    up_band = smoothed_offset
    dn_band = -smoothed_offset
    
    return cci, up_band, dn_band

def get_latest_acci_reading(df):
    """
    Extracts the latest two completed candles' Adaptive CCI readings.
    Returns an AdaptiveCCIReading object [35].
    """
    if len(df) < config.ACCI_CCI_PERIOD + 10:
        return AdaptiveCCIReading(0, 0, 100, 100, -100, -100, ok=False)
        
    cci, up, dn = get_adaptive_cci_series(df)
    
    # We evaluate index -1 (current, developing bar) and -2 (previous, fully closed bar)
    # Note: In backtesting or scan triggers, we focus on index -2 and -3 to avoid flash/repaint signals.
    # To remain robust, we return the last two available rows.
    return AdaptiveCCIReading(
        cci0=cci.iloc[-1],
        cci1=cci.iloc[-2],
        up0=up.iloc[-1],
        up1=up.iloc[-2],
        dn0=dn.iloc[-1],
        dn1=dn.iloc[-2],
        ok=True
    )

# =====================================================================
# ACCI LOGIC TRIGGERS (Ported from MQL5) [35, 36, 37]
# =====================================================================

def ACCI_H4_ExitSupportLong(ar):
    """
    Checks if parent (H4) CCI has crossed above or is supporting above the lower band [35].
    Signals that sellers have exhausted and buyers are stepping back in.
    """
    if not ar.ok: return False
    # Crosses above lower band or holds safely above it
    return (ar.cci1 < ar.dn1 and ar.cci0 > ar.dn0) or (ar.cci0 > ar.dn0 and ar.cci1 > ar.dn1)

def ACCI_H4_ExitSupportShort(ar):
    """
    Checks if parent (H4) CCI has crossed below or is resisting below the upper band [35].
    Signals that buyers have exhausted and sellers are taking control.
    """
    if not ar.ok: return False
    return (ar.cci1 > ar.up1 and ar.cci0 < ar.up0) or (ar.cci0 < ar.up0 and ar.cci1 < ar.up1)

def ACCI_H1_ThrustLong(ar):
    """
    Checks if entry (H1/M30/M15) CCI is in a strong upward thrust (above the upper band) [36].
    """
    if not ar.ok: return False
    return ar.cci0 > ar.up0

def ACCI_H1_ThrustShort(ar):
    """
    Checks if entry (H1/M30/M15) CCI is in a strong downward thrust (below the lower band) [36].
    """
    if not ar.ok: return False
    return ar.cci0 < ar.dn0

def ACCI_H1_OverextendedLong(ar, margin_frac=config.ACCI_OVEREXT_MARGIN_FRAC):
    """
    Checks if buying pressure is extremely overextended (exhaustion danger zone) [37].
    Formula: CCI is above the upper band by more than the safety margin fraction [7].
    """
    if not ar.ok: return False
    band_width = ar.up0 - ar.dn0
    threshold = ar.up0 + (margin_frac * band_width)
    return ar.cci0 > threshold

def ACCI_H1_OverextendedShort(ar, margin_frac=config.ACCI_OVEREXT_MARGIN_FRAC):
    """
    Checks if selling pressure is extremely overextended [37].
    Formula: CCI is below the lower band by more than the safety margin fraction [7].
    """
    if not ar.ok: return False
    band_width = ar.up0 - ar.dn0
    threshold = ar.dn0 - (margin_frac * band_width)
    return ar.cci0 < threshold

if __name__ == "__main__":
    # Test indicators calculation
    from data_feed import DataFeed
    feed = DataFeed()
    raw = feed.fetch_raw_data("EURUSD", timeframe="M15", days=10)
    
    atr = calculate_atr(raw)
    cci, up, dn = get_adaptive_cci_series(raw)
    reading = get_latest_acci_reading(raw)
    
    print("Indicators calculation check:")
    print("Latest ATR Value:", atr.iloc[-1])
    print("Latest CCI Value:", reading.cci0)
    print("Latest ACCI Bands: [", reading.dn0, ",", reading.up0, "]")
    print("Thrust Long Triggered:", ACCI_H1_ThrustLong(reading))
