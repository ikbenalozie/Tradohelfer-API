# scanner.py
# Fibonacci Retracement Python Trading Application - Scanner Coordination Core
# Upgraded: Added Smart Consolidation Verification to solve broker-side publishing delays, preventing double queries or missed signals!

import os
import csv
import json
import time
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
    Keeps track of active parent windows and direction state for a single symbol.
    """
    def __init__(self, symbol):
        self.symbol = symbol
        
        # H4 Parent State (Controls H1 Children)
        self.h4_dir = 0           
        self.h4_time = None       
        self.h4_expire = None     
        self.h4_pattern_id = ""   
        
        # H1 Parent State (Controls M30 Children)
        self.h1_dir = 0
        self.h1_time = None
        self.h1_expire = None     
        self.h1_pattern_id = ""
        
        # M30 Parent State (Controls M15 Children)
        self.m30_dir = 0
        self.m30_time = None
        self.m30_expire = None    
        self.m30_pattern_id = ""


class TradingScanner:
    """
    The orchestrator engine. Connects to our DataFeed, calculates indicators,
    evaluates parent states, runs child multi-timeframe checks, and records alerts.
    Supports Smart Consolidation Verification to eliminate API publishing lag!
    """
    def __init__(self):
        self.feed = DataFeed()
        self.states = {sym: ScannerState(sym) for sym in config.SYMBOLS}
        self.ny_tz = pytz.timezone(config.NEW_YORK_TIMEZONE)
        self.last_m15_scan = None
        self.signals_queue = [] # Internal queue to collect signals for batch delivery
        
    def flush_signals_queue(self):
        """
        Gathers all queued signals from the current cycle/backfill and posts them
        as a single batch request to the API Webhook, optimizing network latency and token usage.
        """
        if not self.signals_queue:
            return
            
        if not config.ENABLE_WEBHOOK or not config.WEBHOOK_URL:
            self.signals_queue.clear()
            return
            
        count = len(self.signals_queue)
        if config.DEBUG_SUMMARY:
            print(f"\n📦 [BATCH ENGINE] Packaging {count} signals for transmission...")
            
        payload = {
            "type": "batch",
            "count": count,
            "signals": self.signals_queue
        }
        
        headers = {"Content-Type": "application/json"}
        if config.AUTH_HEADER:
            if ":" in config.AUTH_HEADER:
                k, v = config.AUTH_HEADER.split(":", 1)
                headers[k.strip()] = v.strip()
            else:
                headers["X-Api-Key"] = config.AUTH_HEADER
                
        try:
            response = requests.post(
                config.WEBHOOK_URL,
                json=payload,
                headers=headers,
                timeout=config.WEBHOOK_TIMEOUT_MS / 1000.0
            )
            if response.status_code in [200, 201]:
                if config.DEBUG_SUMMARY:
                    print(f"✅ [BATCH ENGINE] Successfully flushed batch of {count} signals to cloud database!")
            else:
                print(f"❌ [BATCH ENGINE ERROR] Batch post returned status code {response.status_code}: {response.text}")
        except Exception as ex:
            print(f"❌ [BATCH ENGINE ERROR] Failed to transmit batch payload: {ex}")
            
        # Clear queue for next run
        self.signals_queue.clear()

    def run_startup_backfill(self, backfill_hours=24):
        """
        Smart Resume Startup Backfill:
        Queries our own API database to find the last recorded signal time for each asset.
        If a signal is found within the last 7 days, we only backfill from that point forward!
        This reduces redundant API queries by 99% upon server restarts.
        """
        if not config.ENABLE_WEBHOOK or not config.WEBHOOK_URL:
            print("[STARTUP BACKFILL] Webhook is disabled or URL not set. Skipping backfill.")
            return

        print("\n⚡ [STARTUP BACKFILL] Initiating historical search across all assets... ⚡")
        now_ny = datetime.now(self.ny_tz)
        default_backfill_start = now_ny - timedelta(hours=backfill_hours)

        # 1. Query our own API to get recent signals to compute last active timestamps
        last_signal_times = {}
        try:
            port = os.getenv("PORT", "8000")
            api_url = f"http://127.0.0.1:{port}/api/signals?limit=150"
            response = requests.get(api_url, timeout=5)
            if response.status_code == 200:
                signals = response.json()
                for sig in signals:
                    sym = sig.get("symbol")
                    sig_time_str = sig.get("bar_time")
                    if sym and sig_time_str:
                        try:
                            parsed_time = pd.to_datetime(sig_time_str).to_pydatetime()
                            if parsed_time.tzinfo is not None:
                                parsed_time = parsed_time.astimezone(self.ny_tz)
                            else:
                                parsed_time = self.ny_tz.localize(parsed_time)
                                
                            if sym not in last_signal_times or parsed_time > last_signal_times[sym]:
                                last_signal_times[sym] = parsed_time
                        except Exception:
                            pass
                print(f"[STARTUP BACKFILL] Successfully mapped last recorded signals for {len(last_signal_times)} assets from cloud database.")
            else:
                print(f"[STARTUP BACKFILL WARNING] API returned status {response.status_code} during sync. Falling back to default {backfill_hours}h backfill.")
        except Exception as ex:
            print(f"[STARTUP BACKFILL WARNING] Could not query database for smart resume ({ex}). Falling back to default {backfill_hours}h backfill.")

        for i, symbol in enumerate(config.SYMBOLS):
            try:
                # Politeness delay to prevent rate-limiting during historical data fetch
                if i > 0:
                    time.sleep(1.5)
                
                # Check if we have a recent signal for this asset to resume from
                last_sig_time = last_signal_times.get(symbol)
                if last_sig_time:
                    backfill_start_time = last_sig_time + timedelta(minutes=15)
                    # Safety boundary: don't look back further than 7 days, and if we are already fully caught up, skip!
                    if backfill_start_time >= now_ny - timedelta(minutes=15):
                        print(f"✅ [STARTUP BACKFILL] {symbol} is already 100% caught up! (Last signal: {last_sig_time.strftime('%Y-%m-%d %H:%M EST')}). Skipping backfill.")
                        continue
                    elif backfill_start_time < now_ny - timedelta(days=7):
                        backfill_start_time = now_ny - timedelta(days=7)
                    
                    diff_hours = (now_ny - backfill_start_time).total_seconds() / 3600.0
                    print(f"🔄 [STARTUP BACKFILL] Smart Resume active for {symbol}. Backfilling only the last {diff_hours:.1f} hours of new data (since {last_sig_time.strftime('%Y-%m-%d %H:%M EST')})...")
                else:
                    backfill_start_time = default_backfill_start
                    print(f"⏳ [STARTUP BACKFILL] No recent signal found in DB for {symbol}. Performing default {backfill_hours}h backfill...")

                print(f"[STARTUP BACKFILL] Scanning {symbol} history...")
                
                # Fetch deeper raw M15 data to build robust historical indicators (15 days is plenty)
                df_raw = self.feed.fetch_raw_data(symbol, timeframe="M15", days=15)
                if df_raw.empty or len(df_raw) < 100:
                    continue

                # Resample standard timeframes
                df_h4 = self.feed.resample_candles(df_raw, "H4")
                df_h1 = self.feed.resample_candles(df_raw, "H1")
                df_m30 = self.feed.resample_candles(df_raw, "M30")
                df_m15 = self.feed.resample_candles(df_raw, "M15")
                
                # A. Simulate H1 Children (H4 Parent context)
                h1_backfill_bars = df_h1[df_h1.index >= backfill_start_time]
                for idx in range(len(h1_backfill_bars)):
                    h1_bar_time = h1_backfill_bars.index[idx]
                    
                    h4_before = df_h4[df_h4.index < h1_bar_time]
                    if len(h4_before) < 3:
                        continue
                    
                    h4_found, h4_pattern = detect_patterns_for_symbol(h4_before, "H4")
                    if h4_found:
                        h4_expire = h4_pattern.bar_time + timedelta(hours=16)
                        if h1_bar_time <= h4_expire:
                            h1_before = df_h1[df_h1.index <= h1_bar_time]
                            child_found, child_pattern = detect_patterns_for_symbol(h1_before, "H1")
                            
                            if child_found and child_pattern.direction == h4_pattern.direction:
                                parent_id = f"{symbol}-H4-{int(h4_pattern.bar_time.timestamp())}-{h4_pattern.name}"
                                self._evaluate_and_trigger_child_historical(
                                    symbol=symbol, child_sig=child_pattern, parent_id=parent_id,
                                    df_child=h1_before, df_parent=h4_before
                                )

                # B. Simulate M30 Children (H1 Parent context)
                m30_backfill_bars = df_m30[df_m30.index >= backfill_start_time]
                for idx in range(len(m30_backfill_bars)):
                    m30_bar_time = m30_backfill_bars.index[idx]
                    
                    h1_before = df_h1[df_h1.index < m30_bar_time]
                    if len(h1_before) < 3:
                        continue
                        
                    h1_found, h1_pattern = detect_patterns_for_symbol(h1_before, "H1")
                    if h1_found:
                        h1_expire = h1_pattern.bar_time + timedelta(hours=4)
                        if m30_bar_time <= h1_expire:
                            m30_before = df_m30[df_m30.index <= m30_bar_time]
                            child_found, child_pattern = detect_patterns_for_symbol(m30_before, "M30")
                            if child_found and child_pattern.direction == h1_pattern.direction:
                                parent_id = f"{symbol}-H1-{int(h1_pattern.bar_time.timestamp())}-{h1_pattern.name}"
                                self._evaluate_and_trigger_child_historical(
                                    symbol=symbol, child_sig=child_pattern, parent_id=parent_id,
                                    df_child=m30_before, df_parent=h1_before
                                )

                # C. Simulate M15 Children (M30 Parent context)
                m15_backfill_bars = df_m15[df_m15.index >= backfill_start_time]
                for idx in range(len(m15_backfill_bars)):
                    m15_bar_time = m15_backfill_bars.index[idx]
                    
                    m30_before = df_m30[df_m30.index < m15_bar_time]
                    if len(m30_before) < 3:
                        continue
                        
                    m30_found, m30_pattern = detect_patterns_for_symbol(m30_before, "M30")
                    if m30_found:
                        m30_expire = m30_pattern.bar_time + timedelta(hours=2)
                        if m15_bar_time <= m30_expire:
                            m15_before = df_m15[df_m15.index <= m15_bar_time]
                            child_found, child_pattern = detect_patterns_for_symbol(m15_before, "M15")
                            if child_found and child_pattern.direction == m30_pattern.direction:
                                parent_id = f"{symbol}-M30-{int(m30_pattern.bar_time.timestamp())}-{m30_pattern.name}"
                                self._evaluate_and_trigger_child_historical(
                                    symbol=symbol, child_sig=child_pattern, parent_id=parent_id,
                                    df_child=m15_before, df_parent=m30_before
                                )

            except Exception as e:
                print(f"[ERROR] Failed to backfill {symbol}: {e}")

        # Flush all accumulated historical backfill signals in a single, high-speed HTTP batch POST!
        if self.signals_queue:
            self.flush_signals_queue()

        print("⚡ [STARTUP BACKFILL COMPLETE] All historical signals have been synchronised to the server! ⚡\n")

    def run_scan_cycle(self):
        """
        Executes a single sweep across all targeted trading pairs.
        Uses Smart Clock Sync and Smart Consolidation Verification to completely eliminate close latency!
        """
        now_ny = datetime.now(self.ny_tz)
        
        # Determine the start of the current 15-minute bar (e.g. 08:34 -> 08:30)
        current_m15_time = now_ny.replace(minute=(now_ny.minute // 15) * 15, second=0, microsecond=0)
        
        # If we have already successfully scanned this 15-minute candle, skip the API queries
        if self.last_m15_scan == current_m15_time:
            return
            
        if config.DEBUG_SUMMARY:
            print(f"\n--- SCAN CYCLE INITIATED at {now_ny.strftime('%Y-%m-%d %H:%M:%S EST')} ---")
            
        # We scan one landmark symbol (like EURUSD) to verify if the broker API has consolidated the bar yet!
        landmark_symbol = "EURUSD"
        try:
            # Fetch minimal data to inspect timestamp of the latest published completed bar
            test_df = self.feed.fetch_raw_data(landmark_symbol, timeframe="M15", days=2)
            if not test_df.empty:
                latest_api_time = test_df.index[-1].to_pydatetime()
                # Expected open time of the completed bar (e.g. if now is 14:00, the last completed bar started at 13:45)
                expected_last_completed_start = current_m15_time - timedelta(minutes=15)
                
                # If the latest bar in the API is older than expected, the API is still lagging.
                # Abort this sweep, let it retry on the next 30-second clock boundary, protecting API credits!
                if latest_api_time < expected_last_completed_start:
                    if config.DEBUG_SUMMARY:
                        print(f"⏳ [CONSOLIDATION VERIFICATION] API hasn't published the closed candle yet (Latest: {latest_api_time.strftime('%H:%M EST')}, Expected: {expected_last_completed_start.strftime('%H:%M EST')}). Retrying in 30s...")
                    return
        except Exception as ex:
            print(f"[CONSOLIDATION ERROR] Verification failed: {ex}. Proceeding with standard sweep.")

        # If verification passed, execute the full market scan!
        if config.DEBUG_SUMMARY:
            print(f"⚡ [CONSOLIDATION VERIFIED] Candle closed successfully. Scanning 32 assets on {current_m15_time.strftime('%H:%M EST')} clock boundary...")
            
        for i, symbol in enumerate(config.SYMBOLS):
            try:
                # Add a 1.5-second politeness delay to respect Tiingo's rate limit
                if i > 0:
                    time.sleep(1.5)
                self._scan_symbol(symbol, now_ny)
            except Exception as e:
                print(f"[ERROR] Failed to scan {symbol}: {e}")
                
        # Flush any live signals generated during this 15-minute close boundary sweep!
        if self.signals_queue:
            self.flush_signals_queue()
                
        # Mark this 15-minute bar as fully scanned
        self.last_m15_scan = current_m15_time
                
    def _scan_symbol(self, symbol, now_ny):
        """
        Core scanning steps for a single asset. Uses the "Lazy Scan" depth (10 days)
        to dramatically reduce API transfer payload sizes during 30-second loop cycles.
        """
        state = self.states[symbol]
        
        # Lazy Scan: 10 days is mathematically optimal to calculate EMAs/ATRs
        df_raw = self.feed.fetch_raw_data(symbol, timeframe="M15", days=10)
        
        if df_raw.empty or len(df_raw) < 50:
            if config.DEBUG_SUMMARY:
                print(f"[SCANNER] Insufficient data to scan {symbol}. Rows: {len(df_raw)}")
            return
            
        df_h4 = self.feed.resample_candles(df_raw, "H4")
        df_h1 = self.feed.resample_candles(df_raw, "H1")
        df_m30 = self.feed.resample_candles(df_raw, "M30")
        df_m15 = self.feed.resample_candles(df_raw, "M15")
        
        # =====================================================================
        # STEP A: UPDATE PARENT STRUCTURES
        # =====================================================================
        
        # A1: H4 Parent (controls H1)
        h4_found, h4_pattern = detect_patterns_for_symbol(df_h4, "H4")
        if h4_found and (state.h4_time is None or h4_pattern.bar_time != state.h4_time):
            state.h4_dir = h4_pattern.direction
            state.h4_time = h4_pattern.bar_time
            state.h4_expire = h4_pattern.bar_time + timedelta(hours=16)  # 4 * H4 time
            state.h4_pattern_id = f"{symbol}-H4-{int(h4_pattern.bar_time.timestamp())}-{h4_pattern.name}"
            
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
        # STEP B: SCAN ACTIVE CHILDREN WINDOWS
        # =====================================================================
        
        # B1: H1 Child (Gated by active H4 parent)
        is_h4_active = (state.h4_dir != 0) and (now_ny <= state.h4_expire)
        if is_h4_active:
            child_found, child_pattern = detect_patterns_for_symbol(df_h1, "H1")
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

    def _evaluate_and_trigger_child_historical(self, symbol, child_sig, parent_id, df_child, df_parent):
        """
        Wrapper to run scoring and trigger posting during the startup historical backfill.
        """
        self._evaluate_and_trigger_child(
            symbol=symbol, child_sig=child_sig, parent_id=parent_id,
            parent_dir=child_sig.direction, df_child=df_child, df_parent=df_parent,
            child_tf=child_sig.tf, parent_tf="H4" if "H1" in child_sig.tf else ("H1" if "M30" in child_sig.tf else "M30")
        )

    def _evaluate_and_trigger_child(self, symbol, child_sig, parent_id, parent_dir, 
                                     df_child, df_parent, child_tf, parent_tf):
        """
        Runs the full multi-timeframe scoring matrix against a detected child signal.
        Computes dynamic risk metrics, stop-losses, targets, and logs the results.
        """
        score = 0.0
        reasons = []
        
        # 1. Outer-Quartile Close Check vs Last Closed Parent
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
            
        # 2. ATR Ratio Check
        atr_child_series = calculate_atr(df_child)
        atr_parent_series = calculate_atr(df_parent)
        
        atr_child = atr_child_series.iloc[-2]
        atr_parent = atr_parent_series.iloc[-2]
        
        ratio = atr_child / atr_parent if atr_parent > 0 else 0
        if config.RATIO_MIN <= ratio <= config.RATIO_MAX:
            score += 1.0
            reasons.append("atr_ratio_ok")
            
        # 3. Trend Alignment (20 EMA > 50 EMA for BUY)
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
            
        # 4. Adaptive CCI Votes
        if config.USE_ADAPTIVE_CCI:
            ar_child = get_latest_acci_reading(df_child)
            ar_parent = get_latest_acci_reading(df_parent)
            
            # A. Parent CCI Exit Support Check
            if ar_parent.ok:
                if child_sig.direction > 0 and ACCI_H4_ExitSupportLong(ar_parent):
                    score += 1.0
                    reasons.append("cci_parent_exit_support")
                elif child_sig.direction < 0 and ACCI_H4_ExitSupportShort(ar_parent):
                    score += 1.0
                    reasons.append("cci_parent_exit_support")
                    
            # B. Child CCI Thrust Check
            if ar_child.ok:
                thrust = ACCI_H1_ThrustLong(ar_child) if child_sig.direction > 0 else ACCI_H1_ThrustShort(ar_child)
                if thrust:
                    score += 0.5
                    reasons.append("cci_child_thrust")
                    
                # C. Child CCI Exit Support Check
                exit_child = False
                if child_sig.direction > 0:
                    exit_child = (ar_child.cci1 < ar_child.dn1) and (ar_child.cci0 > ar_child.dn0)
                else:
                    exit_child = (ar_child.cci1 > ar_child.up1) and (ar_child.cci0 < ar_child.up0)
                    
                if exit_child:
                    score += 0.5
                    reasons.append("cci_child_exit_support")
                    
                # D. Overextension Penalty Check
                overextended = ACCI_H1_OverextendedLong(ar_child) if child_sig.direction > 0 else ACCI_H1_OverextendedShort(ar_child)
                child_range = child_sig.high - child_sig.low
                
                if overextended and atr_child > 0 and child_range >= config.H1_BIG_BAR_ATR_MULT * atr_child:
                    score -= 0.5
                    reasons.append("cci_overext_penalty")
                    child_sig.reasons = "prefer_entry2"
                    
        # Classify final signal confidence
        child_sig.confidence = "strong" if score >= 3.0 else ("normal" if score >= 1.5 else "weak")
        child_sig.reasons = "|".join(reasons) if not child_sig.reasons else f"{child_sig.reasons}|" + "|".join(reasons)
        
        # =====================================================================
        # DYNAMIC RISK CALCULATOR ENGINE
        # =====================================================================
        spread_points = 1.5
        point_scale = 0.01 if "JPY" in symbol or child_sig.close > 50 else 0.0001
        spr_price = spread_points * point_scale
        
        entry_buf = config.ENTRY_BUF_ATR * atr_child
        if entry_buf < spr_price:
            entry_buf = spr_price
            
        # Dynamically scale Stop-Loss size based on timeframe ATR ratios
        clamped_ratio = max(config.RATIO_MIN, min(ratio, config.RATIO_MAX))
        w = (clamped_ratio - config.RATIO_MIN) / (config.RATIO_MAX - config.RATIO_MIN)
        sl_multiplier = config.H1_DYN_MIN + w * (config.H1_DYN_MAX - config.H1_DYN_MIN)
        
        sl_buf = sl_multiplier * atr_child
        if sl_buf < 2.0 * spr_price:
            sl_buf = 2.0 * spr_price
            
        # Assign coordinates
        H = child_sig.high
        L = child_sig.low
        
        if child_sig.direction > 0: # BUY
            child_sig.entry_price = H + entry_buf
            child_sig.sl = L - sl_buf
        else: # SELL
            child_sig.entry_price = L - entry_buf
            child_sig.sl = H + sl_buf
            
        # Entry 1 Take Profits
        R1 = abs(child_sig.entry_price - child_sig.sl)
        child_sig.tp1 = child_sig.entry_price + R1 if child_sig.direction > 0 else child_sig.entry_price - R1
        child_sig.tp2 = child_sig.entry_price + 2.0 * R1 if child_sig.direction > 0 else child_sig.entry_price - 2.0 * R1
        
        # Entry 2 (Swing Midpoint Entry)
        child_sig.entry2_price = 0.5 * (H + L)
        R2 = abs(child_sig.entry2_price - child_sig.sl)
        child_sig.tp1_swing = child_sig.entry2_price + R2 if child_sig.direction > 0 else child_sig.entry2_price - R2
        child_sig.tp2_swing = child_sig.entry2_price + 2.0 * R2 if child_sig.direction > 0 else child_sig.entry2_price - 2.0 * R2
        
        self._log_and_emit(symbol, child_sig, is_parent=False, parent_id=parent_id, df_child=df_child)

    def _log_and_emit(self, symbol, sig, is_parent=False, parent_id="", df_child=None):
        """
        Formats signal output, prints to console, and appends to the csv file.
        Pushes rich payloads to self.signals_queue for unified batched HTTP dispatch!
        """
        direction_label = "BUY" if sig.direction > 0 else "SELL"
        dg = 2 if "JPY" in symbol or sig.close > 50 else 5
        
        if is_parent:
            msg = f"--- [PARENT BLOCK] [{symbol} {sig.tf}] Pattern: {sig.name} Dir: {direction_label} at {sig.bar_time.strftime('%Y-%m-%d %H:%M EST')}"
            print(msg)
        else:
            msg = (
                f"🚨 [{symbol} {sig.tf}] {sig.name} ({sig.confidence.upper()}) | Dir: {direction_label} | "
                f"Entry1: {sig.entry_price:.{dg}f} | SL: {sig.sl:.{dg}f} | TP1: {sig.tp1:.{dg}f} | TP2: {sig.tp2:.{dg}f} | "
                f"Entry2 (Swing): {sig.entry2_price:.{dg}f} | TP1_s: {sig.tp1_swing:.{dg}f} | TP2_s: {sig.tp2_swing:.{dg}f} | "
                f"Reasons: {sig.reasons}"
            )
            print(msg)
            
            # Build and enqueue signal details if webhook is enabled
            if config.ENABLE_WEBHOOK and config.WEBHOOK_URL:
                try:
                    candles_list = []
                    if df_child is not None:
                        df_tail = df_child.tail(25)
                        for timestamp, row in df_tail.iterrows():
                            candles_list.append({
                                "time": timestamp.isoformat(),
                                "open": float(row['Open']),
                                "high": float(row['High']),
                                "low": float(row['Low']),
                                "close": float(row['Close'])
                            })
                            
                    signal_payload = {
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
                    self.signals_queue.append(signal_payload)
                    
                except Exception as ex:
                    print(f"[QUEUE ERROR] Failed to stage payload: {ex}")
                
        # Save to local CSV spreadsheet
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
    scanner = TradingScanner()
    scanner.run_scan_cycle()
