# data_feed.py
# Fibonacci Retracement Python Trading Application - Timezone Aware Data Feed with API Key Pooling
# This module connects to our data source (Tiingo) with multi-key rotation and caching
# or generates realistic, high-quality mock data if no key is present.

import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import config

class DataFeed:
    """
    DataFeed acts as our 'Ear to the Ground'. It fetches raw market data and organizes it 
    into structured pandas DataFrames. Enforces the True New York Close (17:00 Close) standard.
    Now supports an API Key Pool to distribute load and bypass rate limits!
    """
    
    def __init__(self):
        # Support both a single key or a comma-separated list of keys
        raw_keys = getattr(config, "TIINGO_API_KEY", "")
        if isinstance(raw_keys, str):
            if "," in raw_keys:
                self.api_keys = [k.strip() for k in raw_keys.split(",") if k.strip()]
            else:
                self.api_keys = [raw_keys.strip()] if raw_keys.strip() else []
        elif isinstance(raw_keys, list):
            self.api_keys = [str(k).strip() for k in raw_keys if str(k).strip()]
        else:
            self.api_keys = []
            
        # Filter out placeholder keys
        self.api_keys = [k for k in self.api_keys if "your_tiingo_api_key_here" not in k and len(k) >= 5]
        
        self.current_key_idx = 0
        self.key_cooldowns = {} # key -> datetime when cooldown ends
        self.ny_tz = pytz.timezone(config.NEW_YORK_TIMEZONE)
        self.utc_tz = pytz.utc
        
    def _get_active_api_key(self):
        """
        Rotates through keys, skipping any that are currently on cooldown due to 429 rate limits.
        """
        if not self.api_keys:
            return None
            
        now = datetime.now()
        num_keys = len(self.api_keys)
        
        for _ in range(num_keys):
            key = self.api_keys[self.current_key_idx]
            # Move index forward for next time (round-robin)
            self.current_key_idx = (self.current_key_idx + 1) % num_keys
            
            # Check if this key is on cooldown
            cooldown_until = self.key_cooldowns.get(key)
            if cooldown_until and now < cooldown_until:
                continue # Skip this key, try the next one
                
            return key
            
        # If all keys are on cooldown, return the first one (fallback)
        return self.api_keys[0]

    def _mark_key_as_cooldown(self, key, seconds=120):
        """
        Puts a rate-limited key on cooldown.
        """
        self.key_cooldowns[key] = datetime.now() + timedelta(seconds=seconds)
        if config.DEBUG_SUMMARY:
            print(f"[API KEY POOL] Key ending in '...{key[-5:] if len(key) > 5 else key}' "
                  f"marked as rate-limited (429). Placed on cooldown for {seconds}s.")

    def fetch_raw_data(self, symbol, timeframe="15min", days=30):
        """
        Fetches historical candle data. Rotates through keys if rate limits are hit.
        """
        sym_lower = symbol.lower()
        active_key = self._get_active_api_key()
        
        # Checking if API key pool is empty
        if not active_key:
            if config.DEBUG_SUMMARY:
                print(f"[DATA FEED] No active Tiingo API keys found in pool. Generating true-to-life offline data for {symbol}...")
            return self._generate_mock_data(symbol, timeframe, days)
        
        url = f"https://api.tiingo.com/tiingo/fx/{sym_lower}/prices"
        
        # Map timeframe to Tiingo interval format
        tiingo_res = "15min"
        if timeframe in ["M15", "15min"]:
            tiingo_res = "15min"
        elif timeframe in ["M30", "30min"]:
            tiingo_res = "30min"
        elif timeframe in ["H1", "1hour"]:
            tiingo_res = "1hour"
        elif timeframe in ["H4", "4hour"]:
            tiingo_res = "4hour"
        elif timeframe in ["1D", "1day"]:
            tiingo_res = "1day"
            
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        max_retries = max(3, len(self.api_keys))
        retry_delay = 1.5  # initial sleep duration in seconds
        
        for attempt in range(max_retries):
            # Refresh active key on retry
            if attempt > 0:
                active_key = self._get_active_api_key()
                
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Token {active_key}'
            }
            
            params = {
                'resampleFreq': tiingo_res,
                'startDate': start_date
            }
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                # Check for rate-limiting (HTTP 429)
                if response.status_code == 429:
                    self._mark_key_as_cooldown(active_key)
                    
                    # If we have multiple keys, try another key immediately without waiting!
                    if len(self.api_keys) > 1:
                        if config.DEBUG_SUMMARY:
                            print(f"[API KEY POOL] Swapping to another key in pool immediately...")
                        continue
                        
                    # If we only have 1 key, we must sleep
                    if config.DEBUG_SUMMARY:
                        print(f"[DATA FEED WARNING] Rate limit hit (429) for {symbol}. "
                              f"Retrying in {retry_delay:.1f}s (Attempt {attempt + 1}/{max_retries})...")
                    time.sleep(retry_delay)
                    retry_delay *= 2.0  # Exponential backoff
                    continue
                    
                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        raise ValueError("Empty data returned from Tiingo.")
                    
                    df = pd.DataFrame(data)
                    df['date'] = pd.to_datetime(df['date'])
                    df.rename(columns={
                        'date': 'datetime',
                        'open': 'Open',
                        'high': 'High',
                        'low': 'Low',
                        'close': 'Close'
                    }, inplace=True)
                    
                    # Make timezone-aware UTC if not already, then convert to America/New_York
                    if df['datetime'].dt.tz is None:
                        df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert(self.ny_tz)
                    else:
                        df['datetime'] = df['datetime'].dt.tz_convert(self.ny_tz)
                    df.set_index('datetime', inplace=True)
                    return df[['Open', 'High', 'Low', 'Close']]
                else:
                    if config.DEBUG_SUMMARY:
                        print(f"[DATA FEED WARNING] API request failed with status code {response.status_code} for {symbol}. Using fallback mock data...")
                    return self._generate_mock_data(symbol, timeframe, days)
                    
            except Exception as e:
                if config.DEBUG_SUMMARY:
                    print(f"[DATA FEED WARNING] Error reaching API ({e}) for {symbol}. Using fallback mock data...")
                return self._generate_mock_data(symbol, timeframe, days)
                
        # If all retries were exhausted
        if config.DEBUG_SUMMARY:
            print(f"[DATA FEED WARNING] Rate limit retry limit exhausted for {symbol}. Using fallback mock data...")
        return self._generate_mock_data(symbol, timeframe, days)
            
    def _generate_mock_data(self, symbol, timeframe, days):
        """
        Generates mathematically robust, high-quality synthetic candles.
        Weekend gaps removed to mimic true Forex markets with exact New York timestamps.
        """
        freq_minutes = 15
        if timeframe in ["M15", "15min"]: freq_minutes = 15
        elif timeframe in ["M30", "30min"]: freq_minutes = 30
        elif timeframe in ["H1", "1hour"]: freq_minutes = 60
        elif timeframe in ["H4", "4hour"]: freq_minutes = 240
        elif timeframe in ["1D", "1day"]: freq_minutes = 1440
        
        total_intervals = int((days * 1440) / freq_minutes)
        
        now_ny = datetime.now(self.ny_tz)
        start_ny = now_ny - timedelta(days=days)
        start_ny = start_ny.replace(hour=17, minute=0, second=0, microsecond=0)
        
        times = []
        curr = start_ny
        while len(times) < total_intervals:
            day_of_week = curr.weekday()
            hour = curr.hour
            
            is_weekend = False
            if day_of_week == 5: # Saturday
                is_weekend = True
            elif day_of_week == 4 and hour >= 17: # Friday post-close
                is_weekend = True
            elif day_of_week == 6 and hour < 17: # Sunday pre-open
                is_weekend = True
                
            if not is_weekend:
                times.append(curr)
            curr += timedelta(minutes=freq_minutes)
            
        base_price = 1.1000
        volatility = 0.0008
        if "JPY" in symbol:
            base_price = 150.00
            volatility = 0.12
        elif "XAUUSD" in symbol or "XAU" in symbol:
            base_price = 2500.00
            volatility = 1.80
        elif "XAGUSD" in symbol or "XAG" in symbol:
            base_price = 30.00
            volatility = 0.05
            
        np.random.seed(42)
        returns = np.random.normal(loc=0.0, scale=volatility, size=len(times))
        prices = base_price * np.exp(np.cumsum(returns))
        
        opens = []
        highs = []
        lows = []
        closes = []
        
        last_close = base_price
        for i in range(len(times)):
            op = last_close
            cl = prices[i]
            
            noise_high = abs(np.random.normal(0, volatility * 0.4))
            noise_low = abs(np.random.normal(0, volatility * 0.4))
            
            hi = max(op, cl) + noise_high
            lo = min(op, cl) - noise_low
            
            if i % 47 == 0:
                direction = 1 if np.random.rand() > 0.5 else -1
                if direction == 1:
                    lo = min(op, cl) - (volatility * 3.5)
                    hi = max(op, cl) + (volatility * 0.2)
                    cl = op + (volatility * 0.5)
                else:
                    hi = max(op, cl) + (volatility * 3.5)
                    lo = min(op, cl) - (volatility * 0.2)
                    cl = op - (volatility * 0.5)
            elif i % 59 == 0:
                direction = 1 if np.random.rand() > 0.5 else -1
                if direction == 1:
                    op = last_close - (volatility * 1.5)
                    cl = last_close + (volatility * 2.0)
                    hi = cl + (volatility * 0.1)
                    lo = op - (volatility * 0.1)
                else:
                    op = last_close + (volatility * 1.5)
                    cl = last_close - (volatility * 2.0)
                    hi = op + (volatility * 0.1)
                    lo = cl - (volatility * 0.1)
            
            opens.append(op)
            highs.append(hi)
            lows.append(lo)
            closes.append(cl)
            last_close = cl
            
        df = pd.DataFrame({
            'Open': opens,
            'High': highs,
            'Low': lows,
            'Close': closes
        }, index=times)
        
        df.index.name = "datetime"
        return df

    def resample_candles(self, df_raw, interval="1D"):
        """
        Resamples raw sub-candles (e.g., 15-minute bars) to larger timeframes.
        """
        if df_raw.empty:
            return df_raw
            
        if not isinstance(df_raw.index, pd.DatetimeIndex):
            df_raw.index = pd.to_datetime(df_raw.index)
            
        offset_rule = "17h"
        
        if interval == "1D":
            resampled = df_raw.resample('24h', offset=offset_rule).agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last'
            }).dropna()
        elif interval == "H4":
            resampled = df_raw.resample('4h', offset=offset_rule).agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last'
            }).dropna()
        elif interval == "H1":
            resampled = df_raw.resample('1h').agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last'
            }).dropna()
        elif interval == "M30":
            resampled = df_raw.resample('30min').agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last'
            }).dropna()
        elif interval == "M15":
            resampled = df_raw.resample('15min').agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last'
            }).dropna()
        else:
            raise ValueError(f"Unsupported resampling interval: {interval}")
            
        return resampled
