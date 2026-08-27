# patterns.py
# Fibonacci Retracement Python Trading Application - Pattern Detection Engine
# This module implements the mathematical rules for detecting Pinbars and Engulfing patterns [3].
# Highly optimized to run on Pandas DataFrames or individual bar datasets.

import numpy as np
import pandas as pd
import config
from indicators import calculate_atr

class PatternSignal:
    """
    A unified structure that holds information about a detected candle pattern [12, 15].
    Matches the original 'PatternSignal' struct from pattern_lib.mqh.
    """
    def __init__(self, name="", direction=0, bar_time=None, tf="", open_p=0, high_p=0, low_p=0, close_p=0):
        self.name = name          # e.g., "Pinbar" or "Engulfing" [25]
        self.direction = direction # +1 for BUY/BULLISH, -1 for SELL/BEARISH [12, 15, 25]
        self.bar_time = bar_time   # Time of the pattern-triggering candle [16]
        self.tf = tf              # Timeframe (H4, H1, M30, M15) [16]
        self.open = open_p        # Candle Open price [16]
        self.high = high_p        # Candle High price [16]
        self.low = low_p          # Candle Low price [16]
        self.close = close_p      # Candle Close price [16]
        
        # Risk Management Levels (computed later) [17, 18]
        self.entry_price = 0.0     # Breakout entry (Entry 1)
        self.entry2_price = 0.0    # Swing retracement entry (Entry 2)
        self.sl = 0.0              # Dynamic Stop-Loss
        self.tp1 = 0.0             # Take-Profit 1 for Entry 1
        self.tp2 = 0.0             # Take-Profit 2 for Entry 1
        self.tp1_swing = 0.0       # Take-Profit 1 for Entry 2
        self.tp2_swing = 0.0       # Take-Profit 2 for Entry 2
        
        # Scoring & Confidence [18]
        self.confidence = "weak"
        self.reasons = ""

def detect_patterns_for_symbol(df, timeframe, atr_series=None):
    """
    Scans the latest closed bar of a symbol DataFrame for candle patterns [30, 33].
    This matches the core function 'DetectPatternsForSymbol' from MT5 [30, 33].
    
    Returns (True, PatternSignal) if a valid pattern is found, else (False, None).
    """
    if len(df) < 3:
        return False, None
        
    # We always analyze the latest completed bar. 
    # In live scanning, df.iloc[-1] is the active/forming bar. 
    # So we look at df.iloc[-2] as the finished candle we are scanning [30, 32].
    idx_completed = -2
    idx_prev = -3
    
    row_curr = df.iloc[idx_completed]
    row_prev = df.iloc[idx_prev]
    
    # Calculate ATR if not provided
    if atr_series is None:
        atr_series = calculate_atr(df, config.ATR_PERIOD)
        
    current_atr = atr_series.iloc[idx_completed]
    bar_time = df.index[idx_completed]
    
    # Prices of the completed bar
    o = row_curr['Open']
    h = row_curr['High']
    l = row_curr['Low']
    c = row_curr['Close']
    rng = h - l
    
    # Prices of the previous bar (used for engulfing and relative momentum checks)
    o_prev = row_prev['Open']
    h_prev = row_prev['High']
    l_prev = row_prev['Low']
    c_prev = row_prev['Close']
    
    # Avoid div-by-zero on completely flat/non-ticking bars
    if rng <= 0:
        return False, None
        
    # Minimum range filters [3]
    min_atr_factor = config.MIN_RANGE_ATR_H1
    min_point_spread = config.MIN_RANGE_SPR_H1
    if "H4" in timeframe:
        min_atr_factor = config.MIN_RANGE_ATR_H4
        min_point_spread = config.MIN_RANGE_SPR_H4
        
    # Range check vs ATR: ensures the candle has healthy trading volume [3]
    if rng < (min_atr_factor * current_atr):
        return False, None
        
    # Range check vs Points [3]
    # In Python, we can calculate this. 1 point = 0.0001 for EURUSD, 0.01 for USDJPY.
    # To keep it generic, we skip point checking if it's not a major currency, 
    # but let's implement a clean scaling check:
    point_size = 0.01 if "JPY" in df.columns or "jpy" in df.index.name or c > 50 else 0.0001
    if rng < (min_point_spread * point_size):
        return False, None
        
    # Calculate body, upper wick, and lower wick of the completed candle
    body_high = max(o, c)
    body_low = min(o, c)
    body_size = body_high - body_low
    
    upper_wick = h - body_high
    lower_wick = body_low - l
    
    # =====================================================================
    # 1. PINBAR PATTERN SCANNER [2, 3]
    # =====================================================================
    # Bullish Pinbar: huge lower wick (tail) and small body near the top of the candle [2]
    # Bearish Pinbar: huge upper wick (tail) and small body near the bottom of the candle [2]
    
    # Tail fraction is the ratio of our main tail to the entire candle's range
    tail_frac_bull = lower_wick / rng
    tail_frac_bear = upper_wick / rng
    
    # Opposite wick fraction is the ratio of our 'nose' to the entire range
    opp_wick_frac_bull = upper_wick / rng
    opp_wick_frac_bear = lower_wick / rng
    
    # A Bullish Pinbar must have:
    # - A long lower tail [2]
    # - A tiny upper nose [3]
    # - Close position inside the top portion of the candle
    is_bull_pin = (tail_frac_bull >= config.TAIL_FRAC_MIN) and \
                  (opp_wick_frac_bull <= config.OPP_WICK_MAX_FRAC) and \
                  (body_size / rng <= 0.35)
                  
    # A Bearish Pinbar must have:
    # - A long upper tail [2]
    # - A tiny lower nose [3]
    # - Close position inside the bottom portion of the candle
    is_bear_pin = (tail_frac_bear >= config.TAIL_FRAC_MIN) and \
                  (opp_wick_frac_bear <= config.OPP_WICK_MAX_FRAC) and \
                  (body_size / rng <= 0.35)
                  
    if is_bull_pin:
        return True, PatternSignal(
            name="Pinbar", direction=1, bar_time=bar_time, tf=timeframe,
            open_p=o, high_p=h, low_p=l, close_p=c
        )
        
    if is_bear_pin:
        return True, PatternSignal(
            name="Pinbar", direction=-1, bar_time=bar_time, tf=timeframe,
            open_p=o, high_p=h, low_p=l, close_p=c
        )
        
    # =====================================================================
    # 2. ENGULFING PATTERN SCANNER [3]
    # =====================================================================
    if config.USE_ENGULFING:
        body_prev = abs(c_prev - o_prev)
        
        # Bullish Engulfing:
        # - Current candle is bullish (Close > Open)
        # - Current candle body size is larger than the previous candle body size
        # - Current candle body wraps completely around (engulfs) the previous body
        is_bull_engulf = (c > o) and (c_prev < o_prev) and \
                         (o < c_prev) and (c > o_prev) and \
                         (body_size > body_prev)
                         
        # Bearish Engulfing:
        # - Current candle is bearish (Close < Open)
        # - Current candle body size is larger than previous candle body size
        # - Current candle body wraps completely around the previous body
        is_bear_engulf = (c < o) and (c_prev > o_prev) and \
                         (o > c_prev) and (c < o_prev) and \
                         (body_size > body_prev)
                         
        if is_bull_engulf:
            return True, PatternSignal(
                name="Engulfing", direction=1, bar_time=bar_time, tf=timeframe,
                open_p=o, high_p=h, low_p=l, close_p=c
            )
            
        if is_bear_engulf:
            return True, PatternSignal(
                name="Engulfing", direction=-1, bar_time=bar_time, tf=timeframe,
                open_p=o, high_p=h, low_p=l, close_p=c
            )
            
    return False, None

if __name__ == "__main__":
    # Test pattern detection with mock data
    from data_feed import DataFeed
    feed = DataFeed()
    raw = feed.fetch_raw_data("EURUSD", timeframe="M15", days=10)
    
    # Resample to H4 and run a pattern search on each candle to find if one triggers
    h4 = feed.resample_candles(raw, "H4")
    atr_h4 = calculate_atr(h4)
    
    patterns_found = 0
    for i in range(20, len(h4)):
        sub_df = h4.iloc[:i]
        found, sig = detect_patterns_for_symbol(sub_df, "H4", atr_h4.iloc[:i])
        if found:
            patterns_found += 1
            print(f"Found {sig.name} ({'BUY' if sig.direction > 0 else 'SELL'}) at {sig.bar_time}")
            
    print(f"Total H4 patterns scanned and triggered: {patterns_found}")
