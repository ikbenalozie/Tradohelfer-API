# scanner.py
# Fibonacci Retracement Python Trading Application - Scanner Coordination Core
# This module maintains the state of each symbol, tracks parent active windows, 
# scores children signals using the Multi-Timeframe Scoring Matrix, and outputs results.
# Upgraded: Added 24-hour historical Startup Backfill Mode for preloading the mobile application charts.

import os
import csv
import json
from datetime import datetime, timedelta
import pytz
import pandas as pd
import requests

import config
from data_feed import DataFeed
from indicators import (
    calculate_atr, calculate_ema, get_latest_acci_reading, get_adaptive_cci_series,
    ACCI_H4_ExitSupportLong, ACCI_H4_ExitSupportShort,
    ACCI_H1_ThrustLong, ACCI_H1_ThrustShort,
    ACCI_H1_OverextendedLong, ACCI_H1_OverextendedShort
)
from patterns import detect_patterns_for_symbol, PatternSignal

class ScannerState:
    """
    Keeps track of active parent windows and direction state for a single symbol [6, 31, 32].
    """
    def __init__(self, symbol):
        self.symbol = symbol
        
        # H4 Parent State (Controls H1 Children) [6, 9, 31, 32]
        self.h4_dir = 0           # +1 for Buy bias, -1 for Sell bias, 0 for neutral
        self.h4_time = None       # Start time of triggering H4 bar
        self.h4_expire = None     # Expiration of active H4 context (H4 time + 16 hrs)
        self.h4_pattern_id = ""   # Unique ID of triggering H4 pattern
        
        # H1 Parent State (Controls M30 Children)
        self.h1_dir = 0
        self.h1_time = None
        self.h1_expire = None     # Expiration of H1 context (H1 time + 4 hrs)
        self.h1_pattern_id = ""
        
        # M30 Parent State (Controls M15 Children)
        self.m30_dir = 0
        self.m30_time = None
        self.m30_expire = None    # Expiration of M30 context (M30 time + 2 hrs)
        self.m30_pattern_id = ""


class TradingScanner:
    """
    The orchestrator engine. Connects to our DataFeed, calculates indicators,
    evaluates parent states, runs child multi-timeframe checks, and records alerts.
    """
    def __init__(self):
        self.feed = DataFeed()
        self.states = {sym: ScannerState(sym) for sym in config.SYMBOLS}
        self.ny_tz = pytz.timezone(config.NEW_YORK_TIMEZONE)
        
        # Run startup backfill to populate past signals in the cloud database immediately
        self.perform_startup_backfill()
        
    def perform_startup_backfill(self):
        """
        Scans all symbols historically for signals triggered in the past N hours
        and writes them to the SQLite database via webhook.
        This ensures the mobile application has historical alerts immediately on launch!
        """
        backfill_hours = getattr(config, 'BACKFILL_HOURS', 24)
        if backfill_hours <= 0:
            return
            
        print(f"\n⚡ [STARTUP BACKFILL] Initiating historical search across all assets for the past {backfill_hours} hours... ⚡")
        if not config.ENABLE_WEBHOOK or not config.WEBHOOK_URL:
            print("[STARTUP BACKFILL] Webhook is disabled or URL not set. Skipping backfill.")
            return
            
        now_ny = datetime.now(self.ny_tz)
        cutoff_time = now_ny - timedelta(hours=backfill_hours)
        
        for symbol in config.SYMBOLS:
            try:
                print(f"[STARTUP BACKFILL] Scanning {symbol}...")
                df_raw = self.feed.fetch_raw_data(symbol, timeframe="M15", days=30)
                if df_raw.empty or len(df_raw) < 100:
                    continue
                    
                df_h4 = self.feed.resample_candles(df_raw, "H4")
                df_h1 = self.feed.resample_candles(df_raw, "H1")
                df_m30 = self.feed.resample_candles(df_raw, "M30")
                df_m15 = self.feed.resample_candles(df_raw, "M15")
                
                # Pre-calculate parent pattern timelines across 30 days
                h4_parents = self._precalculate_parent_patterns(symbol, df_h4, "H4")
                h1_parents = self._precalculate_parent_patterns(symbol, df_h1, "H1")
                m30_parents = self._precalculate_parent_patterns(symbol, df_m30, "M30")
                
                # Check child timeframes
                self._backfill_child_tf(symbol, df_h1, df_h4, h4_parents, "H1", "H4", cutoff_time)
                self._backfill_child_tf(symbol, df_m30, df_h1, h1_parents, "M30", "H1", cutoff_time)
                self._backfill_child_tf(symbol, df_m15, df_m30, m30_parents, "M15", "M30", cutoff_time)
                
            except Exception as e:
                print(f"[STARTUP BACKFILL ERROR] Failed to backfill {symbol}: {e}")
        print("⚡ [STARTUP BACKFILL COMPLETE] All historical signals have been synchronised to the server! ⚡\n")

    def _precalculate_parent_patterns(self, symbol, df_parent, tf):
        parent_signals = []
        atr_series = calculate_atr(df_parent)
        for j in range(20, len(df_parent)):
            slice_df = df_parent.iloc[:j+1]
            found, pattern = detect_patterns_for_symbol(slice_df, tf, atr_series.iloc[:j+1])
            if found:
                tf_hours = 4 if "H4" in tf else (1 if "H1" in tf else 0.5)
                expire_time = pattern.bar_time + timedelta(hours=4 * tf_hours)
                pattern_id = f"{symbol}-{tf}-{int(pattern.bar_time.timestamp())}-{pattern.name}"
                parent_signals.append({
                    "direction": pattern.direction,
                    "bar_time": pattern.bar_time,
                    "expire": expire_time,
                    "id": pattern_id,
                    "name": pattern.name
                })
        return parent_signals

    def _backfill_child_tf(self, symbol, df_child, df_parent, parent_patterns, child_tf, parent_tf, cutoff_time):
        atr_child = calculate_atr(df_child)
        for i in range(50, len(df_child) - 1):
            child_time = df_child.index[i]
            if child_time < cutoff_time:
                continue
                
            active_parent = None
            for p in parent_patterns:
                if p["bar_time"] <= child_time <= p["expire"]:
                    active_parent = p
                    break
                    
            if active_parent:
                child_slice = df_child.iloc[:i+2]
                child_found, child_pattern = detect_patterns_for_symbol(child_slice, child_tf, atr_child.iloc[:i+2])
                
                if child_found and child_pattern.direction == active_parent["direction"]:
                    # We have a matching historical signal! Run evaluations and trigger
                    df_parent_sliced = df_parent[df_parent.index <= child_time]
                    self._evaluate_and_trigger_child(
                        symbol=symbol,
                        child_sig=child_pattern,
                        parent_id=active_parent["id"],
                        parent_dir=active_parent["direction"],
                        df_child=child_slice,
                        df_parent=df_parent_sliced,
                        child_tf=child_tf,
                        parent_tf=parent_tf,
                        is_backfill=True
                    )
        
    def run_scan_cycle(self):
        """
        Executes a single sweep across all targeted trading pairs [30].
        This matches the 'OnTimer' logic from the original EA [30].
        """
        now_ny = datetime.now(self.ny_tz)
        
        if config.DEBUG_SUMMARY:
            print(f"\n--- SCAN CYCLE INITIATED at {now_ny.strftime('%Y-%m-%d %H:%M:%S EST')} ---")
            
        for symbol in config.SYMBOLS:
            try:
                self._scan_symbol(symbol, now_ny)
            except Exception as e:
                print(f"[ERROR] Failed to scan {symbol}: {e}")
                
    def _scan_symbol(self, symbol, now_ny):
        """
        Core scanning steps for a single asset:
        1. Fetch raw sub-candles (M15 is our base granularity)
        2. Resample raw bars into New York Close-aligned higher timeframes: 1D, H4, H1, M30, M15 [2]
        3. Check and update the Parent biases (H4, H1, M30)
        4. Check and evaluate lower timeframe entry children (H1, M30, M15)
        """
        state = self.states[symbol]
        
        # Fetch raw 15-minute bars to construct all timeframes
        df_raw = self.feed.fetch_raw_data(symbol, timeframe="M15", days=30)
        
        if df_raw.empty or len(df_raw) < 50:
            if config.DEBUG_SUMMARY:
                print(f"[SCANNER] Insufficient data to scan {symbol}. Rows: {len(df_raw)}")
            return
            
        # Resample our timeframes to New York Close alignments [2]
        df_h4 = self.feed.resample_candles(df_raw, "H4")
        df_h1 = self.feed.resample_candles(df_raw, "H1")
        df_m30 = self.feed.resample_candles(df_raw, "M30")
        df_m15 = self.feed.resample_candles(df_raw, "M15")
        
        # =====================================================================
        # STEP A: UPDATE PARENT STRUCTURES [30, 31, 32]
        # =====================================================================
        
        # A1: H4 Parent (controls H1) [31, 32]
        h4_found, h4_pattern = detect_patterns_for_symbol(df_h4, "H4")
        if h4_found and (state.h4_time is None or h4_pattern.bar_time != state.h4_time):
            state.h4_dir = h4_pattern.direction
            state.h4_time = h4_pattern.bar_time
            state.h4_expire = h4_pattern.bar_time + timedelta(hours=16)  # 4 * H4 time [31]
            state.h4_pattern_id = f"{symbol}-H4-{int(h4_pattern.bar_time.timestamp())}-{h4_pattern.name}"
            
            # Record/log the structural breakout [31]
            self._log_and_emit(symbol, h4_pattern, is_parent=True, df_child=df_h4)
            
        # A2: H1 Parent (controls M30)
        h1_found, h1_pattern = detect_patterns_for_symbol(df_h1, "H1")
        if h1_found and (state.h1_time is None or h1_pattern.bar_time != state.h1_time):
            state.h1_dir = h1_pattern.direction
            state.h1_time = h1_pattern.bar_time
            state.h1_expire = h1_pattern.bar_time + timedelta(hours=4)  # 4 * H1 time
            state.h1_pattern_id = f"{symbol}-H1-{int(h1_pattern.bar_time.timestamp())}-{h1_pattern.name}"
            
            self._log_and_emit(symbol, h1_pattern, is_parent=True, df_child=df_h1)
            
        # A3: M30 Parent (controls M15)
        m30_found, m30_pattern = detect_patterns_for_symbol(df_m30, "M30")
        if m30_found and (state.m30_time is None or m30_pattern.bar_time != state.m30_time):
            state.m30_dir = m30_pattern.direction
            state.m30_time = m30_pattern.bar_time
            state.m30_expire = m30_pattern.bar_time + timedelta(hours=2)  # 4 * M30 time (2 hours)
            state.m30_pattern_id = f"{symbol}-M30-{int(m30_pattern.bar_time.timestamp())}-{m30_pattern.name}"
            
            self._log_and_emit(symbol, m30_pattern, is_parent=True, df_child=df_m30)
            
        # =====================================================================
        # STEP B: SCAN ACTIVE CHILDREN WINDOWS [32, 33]
        # =====================================================================
        
        # B1: H1 Child (Gated by active H4 parent) [32, 33]
        is_h4_active = (state.h4_dir != 0) and (now_ny <= state.h4_expire)
        if is_h4_active:
            child_found, child_pattern = detect_patterns_for_symbol(df_h1, "H1")
            # Only trigger on newly closed bars
            if child_found and child_pattern.direction == state.h4_dir:
                self._evaluate_and_trigger_child(
                    symbol=symbol, child_sig=child_pattern, parent_id=state.h4_pattern_id,
                    parent_dir=state.h4_dir, df_child=df_h1, df_parent=df_h4,
                    child_tf="H1", parent_tf="H4"
                )
                
        # B2: M30 Child (Gated by active H1 parent)
        is_h1_active = (state.h1_dir != 0) and (now_ny <= state.h1_expire)
        if is_h1_active:
            child_found, child_pattern = detect_patterns_for_symbol(df_m30, "M30")
            if child_found and child_pattern.direction == state.h1_dir:
                self._evaluate_and_trigger_child(
                    symbol=symbol, child_sig=child_pattern, parent_id=state.h1_pattern_id,
                    parent_dir=state.h1_dir, df_child=df_m30, df_parent=df_h1,
                    child_tf="M30", parent_tf="H1"
                )
                
        # B3: M15 Child (Gated by active M30 parent)
        is_m30_active = (state.m30_dir != 0) and (now_ny <= state.m30_expire)
        if is_m30_active:
            child_found, child_pattern = detect_patterns_for_symbol(df_m15, "M15")
            if child_found and child_pattern.direction == state.m30_dir:
                self._evaluate_and_trigger_child(
                    symbol=symbol, child_sig=child_pattern, parent_id=state.m30_pattern_id,
                    parent_dir=state.m30_dir, df_child=df_m15, df_parent=df_m30,
                    child_tf="M15", parent_tf="M30"
                )

    def _evaluate_and_trigger_child(self, symbol, child_sig, parent_id, parent_dir, 
                                     df_child, df_parent, child_tf, parent_tf, is_backfill=False):
        """
        Runs the full multi-timeframe scoring matrix against a detected child signal [33, 34, 35].
        Computes dynamic risk metrics, stop-losses, targets, and logs the results [23, 24, 25, 27].
        """
        score = 0.0
        reasons = []
        
        # 1. Outer-Quartile Close Check vs Last Closed Parent [33, 34]
        parent_last_bar = df_parent.iloc[-2]
        parent_high = parent_last_bar['High']
        parent_low = parent_last_bar['Low']
        parent_range = parent_high - parent_low
        
        child_close = child_sig.close
        
        if child_sig.direction > 0: # BUY
            is_outer = child_close >= (parent_high - 0.25 * parent_range)
        else: # SELL
            is_outer = child_close <= (parent_low + 0.25 * parent_range)
            
        if is_outer:
            score += 1.0
            reasons.append("outer_quartile")
            
        # 2. ATR Ratio Check [34]
        atr_child_series = calculate_atr(df_child)
        atr_parent_series = calculate_atr(df_parent)
        
        atr_child = atr_child_series.iloc[-2]
        atr_parent = atr_parent_series.iloc[-2]
        
        ratio = atr_child / atr_parent if atr_parent > 0 else 0
        if config.RATIO_MIN <= ratio <= config.RATIO_MAX: # [6, 7]
            score += 1.0
            reasons.append("atr_ratio_ok")
            
        # 3. Trend Alignment (20 EMA > 50 EMA for BUY) [34]
        ema20 = calculate_ema(df_child['Close'], 20).iloc[-2]
        ema50 = calculate_ema(df_child['Close'], 50).iloc[-2]
        
        is_trend_aligned = False
        if child_sig.direction > 0: # BUY
            is_trend_aligned = (child_close > ema20) and (ema20 > ema50)
        else: # SELL
            is_trend_aligned = (child_close < ema20) and (ema20 < ema50)
            
        if is_trend_aligned:
            score += 1.0
            reasons.append("trend_aligned")
            
        # 4. Adaptive CCI Votes [34, 35]
        if config.USE_ADAPTIVE_CCI:
            ar_child = get_latest_acci_reading(df_child)
            ar_parent = get_latest_acci_reading(df_parent)
            
            # A. Parent CCI Exit Support Check [35]
            if ar_parent.ok:
                if child_sig.direction > 0 and ACCI_H4_ExitSupportLong(ar_parent):
                    score += 1.0
                    reasons.append("cci_parent_exit_support")
                elif child_sig.direction < 0 and ACCI_H4_ExitSupportShort(ar_parent):
                    score += 1.0
                    reasons.append("cci_parent_exit_support")
                    
            # B. Child CCI Thrust Check [36]
            if ar_child.ok:
                thrust = ACCI_H1_ThrustLong(ar_child) if child_sig.direction > 0 else ACCI_H1_ThrustShort(ar_child)
                if thrust:
                    score += 0.5
                    reasons.append("cci_child_thrust")
                    
                # C. Child CCI Exit Support Check [36, 37]
                exit_child = False
                if child_sig.direction > 0:
                    exit_child = (ar_child.cci1 < ar_child.dn1) and (ar_child.cci0 > ar_child.dn0)
                else:
                    exit_child = (ar_child.cci1 > ar_child.up1) and (ar_child.cci0 < ar_child.up0)
                    
                if exit_child:
                    score += 0.5
                    reasons.append("cci_child_exit_support")
                    
                # D. Overextension Penalty Check [37, 38]
                overextended = ACCI_H1_OverextendedLong(ar_child) if child_sig.direction > 0 else ACCI_H1_OverextendedShort(ar_child)
                child_range = child_sig.high - child_sig.low
                
                if overextended and atr_child > 0 and child_range >= config.H1_BIG_BAR_ATR_MULT * atr_child: # [7, 38]
                    score -= 0.5
                    reasons.append("cci_overext_penalty")
                    child_sig.reasons = "prefer_entry2" # Suggest swing entry because breakout is risky [38]
                    
        # Classify final signal confidence [38]
        child_sig.confidence = "strong" if score >= 3.0 else ("normal" if score >= 1.5 else "weak")
        child_sig.reasons = "|".join(reasons) if not child_sig.reasons else f"{child_sig.reasons}|" + "|".join(reasons)
        
        # =====================================================================
        # DYNAMIC RISK CALCULATOR ENGINE [23, 24, 25]
        # =====================================================================
        spread_points = 1.5 # Placeholder estimated spread (1.5 pips)
        point_scale = 0.01 if "JPY" in symbol or child_sig.close > 50 else 0.0001
        spr_price = spread_points * point_scale
        
        entry_buf = config.ENTRY_BUF_ATR * atr_child
        if entry_buf < spr_price:
            entry_buf = spr_price
            
        # Dynamically scale Stop-Loss size based on timeframe ATR ratios [6, 7, 23]
        clamped_ratio = max(config.RATIO_MIN, min(ratio, config.RATIO_MAX))
        w = (clamped_ratio - config.RATIO_MIN) / (config.RATIO_MAX - config.RATIO_MIN)
        sl_multiplier = config.H1_DYN_MIN + w * (config.H1_DYN_MAX - config.H1_DYN_MIN)
        
        sl_buf = sl_multiplier * atr_child
        if sl_buf < 2.0 * spr_price:
            sl_buf = 2.0 * spr_price
            
        # Assign coordinates [23, 24]
        H = child_sig.high
        L = child_sig.low
        
        if child_sig.direction > 0: # BUY
            child_sig.entry_price = H + entry_buf
            child_sig.sl = L - sl_buf
        else: # SELL
            child_sig.entry_price = L - entry_buf
            child_sig.sl = H + sl_buf
            
        # Entry 1 Take Profits [24]
        R1 = abs(child_sig.entry_price - child_sig.sl)
        child_sig.tp1 = child_sig.entry_price + R1 if child_sig.direction > 0 else child_sig.entry_price - R1
        child_sig.tp2 = child_sig.entry_price + 2.0 * R1 if child_sig.direction > 0 else child_sig.entry_price - 2.0 * R1
        
        # Entry 2 (Swing Midpoint Entry) [24]
        child_sig.entry2_price = 0.5 * (H + L)
        R2 = abs(child_sig.entry2_price - child_sig.sl)
        child_sig.tp1_swing = child_sig.entry2_price + R2 if child_sig.direction > 0 else child_sig.entry2_price - R2
        child_sig.tp2_swing = child_sig.entry2_price + 2.0 * R2 if child_sig.direction > 0 else child_sig.entry2_price - 2.0 * R2 # [24, 25]
        
        self._log_and_emit(symbol, child_sig, is_parent=False, parent_id=parent_id, df_child=df_child, is_backfill=is_backfill)

    def _log_and_emit(self, symbol, sig, is_parent=False, parent_id="", df_child=None, is_backfill=False):
        """
        Formats signal output, prints to console, and appends to the csv file [25, 26, 27].
        Matches 'PrintSignalMsg' and 'WriteCSV' in the original code [25, 26, 27].
        Sends rich JSON payload (including embedded candlestick data for mobile charts) to Webhook API.
        """
        direction_label = "BUY" if sig.direction > 0 else "SELL"
        dg = 2 if "JPY" in symbol or sig.close > 50 else 5
        
        if is_parent:
            # Major structure update [31]
            if not is_backfill:
                msg = f"--- [PARENT BLOCK] [{symbol} {sig.tf}] Pattern: {sig.name} Dir: {direction_label} at {sig.bar_time.strftime('%Y-%m-%d %H:%M EST')}"
                print(msg)
        else:
            # Children Signal update [25, 26, 39]
            prefix = "[BACKFILL]" if is_backfill else "🚨"
            msg = (
                f"{prefix} [{symbol} {sig.tf}] {sig.name} ({sig.confidence.upper()}) | Dir: {direction_label} | "
                f"Entry1: {sig.entry_price:.{dg}f} | SL: {sig.sl:.{dg}f} | TP1: {sig.tp1:.{dg}f} | TP2: {sig.tp2:.{dg}f} | "
                f"Entry2 (Swing): {sig.entry2_price:.{dg}f} | TP1_s: {sig.tp1_swing:.{dg}f} | TP2_s: {sig.tp2_swing:.{dg}f} | "
                f"Reasons: {sig.reasons}"
            )
            print(msg)
            
            # Build and send rich JSON payload if webhook is enabled [3, 4]
            if config.ENABLE_WEBHOOK and config.WEBHOOK_URL:
                try:
                    # Capture last 25 candles leading up to the signal for the MT5-grade mobile chart rendering
                    candles_list = []
                    if df_child is not None:
                        # Grab completed bars up to current
                        df_tail = df_child.tail(25)
                        for timestamp, row in df_tail.iterrows():
                            candles_list.append({
                                "time": timestamp.isoformat(),
                                "open": float(row['Open']),
                                "high": float(row['High']),
                                "low": float(row['Low']),
                                "close": float(row['Close'])
                            })
                            
                    # Construct high-fidelity MQL5-style zones for shading on phone [12, 13, 14, 15]
                    payload = {
                        "type": "signal",
                        "symbol": symbol,
                        "tf": sig.tf,
                        "pattern": sig.name,
                        "dir": direction_label,
                        "bar_time": sig.bar_time.isoformat(),
                        "open": float(sig.open),
                        "high": float(sig.high),
                        "low": float(sig.low),
                        "close": float(sig.close),
                        "entry1": float(sig.entry_price),
                        "entry1_kind": "breakout",
                        "entry2": float(sig.entry2_price),
                        "entry2_kind": "swing",
                        "sl": float(sig.sl),
                        "tp1": float(sig.tp1),
                        "tp2": float(sig.tp2),
                        "tp1_swing": float(sig.tp1_swing),
                        "tp2_swing": float(sig.tp2_swing),
                        "confidence": sig.confidence,
                        "reasons": sig.reasons,
                        "parent_id": parent_id,
                        "zones": {
                            "entry1": {
                                "risk": {"low": float(min(sig.entry_price, sig.sl)), "high": float(max(sig.entry_price, sig.sl))},
                                "r1": {"low": float(min(sig.entry_price, sig.tp1)), "high": float(max(sig.entry_price, sig.tp1))},
                                "r2": {"low": float(min(sig.tp1, sig.tp2)), "high": float(max(sig.tp1, sig.tp2))}
                            },
                            "entry2": {
                                "risk": {"low": float(min(sig.entry2_price, sig.sl)), "high": float(max(sig.entry2_price, sig.sl))},
                                "r1": {"low": float(min(sig.entry2_price, sig.tp1_swing)), "high": float(max(sig.entry2_price, sig.tp1_swing))},
                                "r2": {"low": float(min(sig.tp1_swing, sig.tp2_swing)), "high": float(max(sig.tp1_swing, sig.tp2_swing))}
                            }
                        },
                        "chart_candles": candles_list
                    }
                    
                    headers = {"Content-Type": "application/json"}
                    # Parse custom authorization header if supplied [4, 11]
                    if config.AUTH_HEADER:
                        if ":" in config.AUTH_HEADER:
                            k, v = config.AUTH_HEADER.split(":", 1)
                            headers[k.strip()] = v.strip()
                        else:
                            headers["X-Api-Key"] = config.AUTH_HEADER
                            
                    # Fire-and-forget webhook request [10, 11]
                    requests.post(
                        config.WEBHOOK_URL, 
                        json=payload, 
                        headers=headers, 
                        timeout=config.WEBHOOK_TIMEOUT_MS / 1000.0
                    )
                    if config.DEBUG_SUMMARY and not is_backfill:
                        print(f"[WEBHOOK] Transmitted signal {symbol} {sig.tf} payload successfully.")
                except Exception as ex:
                    if not is_backfill:
                        print(f"[WEBHOOK ERROR] Failed to send webhook packet: {ex}")
                
        # Save to local CSV spreadsheet [27]
        if config.WRITE_CSV:
            file_exists = os.path.exists(config.CSV_PATH)
            with open(config.CSV_PATH, mode='a', newline='') as f:
                writer = csv.writer(f, delimiter=';')
                if not file_exists:
                    writer.writerow([
                        "symbol", "tf", "pattern", "bar_time", "open", "high", "low", "close", "dir",
                        "entry1", "entry2", "sl", "tp1", "tp2", "tp1_swing", "tp2_swing", "reasons", "parent_id"
                    ])
                
                writer.writerow([
                    symbol, sig.tf, sig.name, sig.bar_time.isoformat(),
                    f"{sig.open:.{dg}f}", f"{sig.high:.{dg}f}", f"{sig.low:.{dg}f}", f"{sig.close:.{dg}f}",
                    direction_label, f"{sig.entry_price:.{dg}f}", f"{sig.entry2_price:.{dg}f}",
                    f"{sig.sl:.{dg}f}", f"{sig.tp1:.{dg}f}", f"{sig.tp2:.{dg}f}",
                    f"{sig.tp1_swing:.{dg}f}", f"{sig.tp2_swing:.{dg}f}", sig.reasons, parent_id
                ])

if __name__ == "__main__":
    # Test scanner coordination with mock feed data
    scanner = TradingScanner()
    scanner.run_scan_cycle()
