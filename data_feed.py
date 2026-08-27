# data_feed-v6.py
# Fibonacci Retracement Python Trading Application - Timezone Aware Data Feed with API Key Pooling & Dual Fallback
# This module connects to our data source (Tiingo) with multi-key rotation and caching
# or falls back to Twelve Data / Yahoo Finance / Coinbase Public APIs if rate limits are hit.

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
    Now supports an API Key Pool and dynamic dual-provider fallbacks!
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
        Fetches historical candle data. Rotates through keys or fallbacks if rate limits are hit.
        """
        sym_lower = symbol.lower()
        
        # Crypto Optimization: Coinbase has 100% free, unlimited, key-less data for BTCUSD
        if symbol == "BTCUSD":
            cb_df = self._fetch_from_coinbase_free(symbol, timeframe, days)
            if cb_df is not None and not cb_df.empty:
                return cb_df

        active_key = self._get_active_api_key()
        
        # Checking if API key pool is empty
        if not active_key:
            if config.DEBUG_SUMMARY:
                print(f"[DATA FEED] No active Tiingo API keys found in pool. Checking fallbacks for {symbol}...")
            fallback_df = self._fetch_from_fallback_chain(symbol, timeframe, days)
            if fallback_df is not None and not fallback_df.empty:
                return fallback_df
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
                    
                    if len(self.api_keys) > 1:
                        if config.DEBUG_SUMMARY:
                            print(f"[API KEY POOL] Swapping to another key in pool immediately...")
                        continue
                        
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
                        print(f"[DATA FEED WARNING] API request failed with status {response.status_code} for {symbol}. Trying fallbacks...")
                    fallback_df = self._fetch_from_fallback_chain(symbol, timeframe, days)
                    if fallback_df is not None and not fallback_df.empty:
                        return fallback_df
                    return self._generate_mock_data(symbol, timeframe, days)
                    
            except Exception as e:
                if config.DEBUG_SUMMARY:
                    print(f"[DATA FEED WARNING] Error reaching API ({e}) for {symbol}. Trying fallbacks...")
                fallback_df = self._fetch_from_fallback_chain(symbol, timeframe, days)
                if fallback_df is not None and not fallback_df.empty:
                    return fallback_df
                return self._generate_mock_data(symbol, timeframe, days)
                
        # If all retries were exhausted
        if config.DEBUG_SUMMARY:
            print(f"[DATA FEED WARNING] Rate limit retry limit exhausted for {symbol}. Trying fallbacks...")
        fallback_df = self._fetch_from_fallback_chain(symbol, timeframe, days)
        if fallback_df is not None and not fallback_df.empty:
            return fallback_df
        return self._generate_mock_data(symbol, timeframe, days)

    def _fetch_from_fallback_chain(self, symbol, timeframe, days):
        """
        Chains fallback providers together to ensure 100% uptime.
        """
        # Fallback 1: Twelve Data API (Extremely generous, simple free key)
        twelve_df = self._fetch_from_twelvedata(symbol, timeframe, days)
        if twelve_df is not None and not twelve_df.empty:
            return twelve_df
            
        # Fallback 2: Yahoo Finance API
        yf_df = self._fetch_from_yfinance(symbol, timeframe, days)
        if yf_df is not None and not yf_df.empty:
            return yf_df
            
        return None

    def _fetch_from_coinbase_free(self, symbol, timeframe, days):
        """
        Fetches BTCUSD historical candles from Coinbase Pro Public API.
        This is 100% free, require no keys, and has very generous rate limits!
        """
        try:
            if config.DEBUG_SUMMARY:
                print(f"[CRYPTO BOOSTER] Fetching BTCUSD directly from Coinbase Pro public REST API...")
            # Coinbase uses intervals in seconds: 900 (15m), 1800 (30m), 3600 (1h), 21600 (4h), 86400 (1D)
            granularity = 900
            if timeframe in ["M30", "30min"]: granularity = 1800
            elif timeframe in ["H1", "1hour"]: granularity = 3600
            elif timeframe in ["H4", "4hour"]: granularity = 21600
            elif timeframe in ["1D", "1day"]: granularity = 86400
            
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)
            
            url = "https://api.pro.coinbase.com/products/BTC-USD/candles"
            params = {
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "granularity": granularity
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Format: [ [time, low, high, open, close, volume], ... ]
                df = pd.DataFrame(data, columns=['time', 'Low', 'High', 'Open', 'Close', 'Volume'])
                df['datetime'] = pd.to_datetime(df['time'], unit='s')
                df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert(self.ny_tz)
                df.set_index('datetime', inplace=True)
                df.sort_index(inplace=True)
                return df[['Open', 'High', 'Low', 'Close']]
        except Exception as e:
            if config.DEBUG_SUMMARY:
                print(f"[CRYPTO BOOSTER WARNING] Coinbase query failed: {e}")
        return None

    def _fetch_from_twelvedata(self, symbol, timeframe, days):
        """
        Fetches from Twelve Data (very generous free tier, handles Forex and Cryptos perfectly).
        """
        # Format FX symbols: EURUSD -> EUR/USD
        formatted_sym = symbol
        if len(symbol) == 6 and symbol.isupper() and symbol not in ["XAUUSD", "XAGUSD", "USOUSD"]:
            formatted_sym = f"{symbol[:3]}/{symbol[3:]}"
        elif symbol in ["XAUUSD", "XAU"]:
            formatted_sym = "XAU/USD"
        elif symbol in ["XAGUSD", "XAG"]:
            formatted_sym = "XAG/USD"
            
        interval = "15min"
        if timeframe in ["M30", "30min"]: interval = "30min"
        elif timeframe in ["H1", "1hour"]: interval = "1h"
        elif timeframe in ["H4", "4hour"]: interval = "4h"
        elif timeframe in ["1D", "1day"]: interval = "1day"
        
        # We can use a public fallback key or let user override in config if they register for free
        td_key = getattr(config, "TWELVEDATA_API_KEY", "demo")
        url = "https://api.twelvedata.com/time_series"
        params = {
            "symbol": formatted_sym,
            "interval": interval,
            "outputsize": min(5000, days * 96), # Get enough candles to represent 'days'
            "apikey": td_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if "values" in data:
                    df = pd.DataFrame(data["values"])
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    df.rename(columns={
                        'open': 'Open',
                        'high': 'High',
                        'low': 'Low',
                        'close': 'Close'
                    }, inplace=True)
                    df = df.astype({'Open': float, 'High': float, 'Low': float, 'Close': float})
                    df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert(self.ny_tz)
                    df.set_index('datetime', inplace=True)
                    df.sort_index(inplace=True)
                    return df[['Open', 'High', 'Low', 'Close']]
        except Exception as e:
            if config.DEBUG_SUMMARY:
                print(f"[FALLBACK WARNING] Twelve Data query failed for {symbol}: {e}")
        return None

    def _fetch_from_yfinance(self, symbol, timeframe, days):
        """
        Fetches from Yahoo Finance REST API.
        """
        if symbol == "BTCUSD":
            yf_ticker = "BTC-USD"
        elif symbol in ["XAUUSD", "XAU"]:
            yf_ticker = "XAUUSD=X"
        elif symbol in ["XAGUSD", "XAG"]:
            yf_ticker = "XAGUSD=X"
        elif symbol in ["USOUSD", "USO"]:
            yf_ticker = "CL=F"
        else:
            yf_ticker = f"{symbol}=X"
            
        interval = "15m"
        if timeframe in ["M15", "15min"]: interval = "15m"
        elif timeframe in ["M30", "30min"]: interval = "30m"
        elif timeframe in ["H1", "1hour"]: interval = "60m"
        elif timeframe in ["H4", "4hour"]: interval = "1h"
        elif timeframe in ["1D", "1day"]: interval = "1d"
        
        range_str = f"{days}d"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_ticker}"
        params = {
            "interval": interval,
            "range": range_str
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
        }
        
        try:
            if config.DEBUG_SUMMARY:
                print(f"[FALLBACK COOPERATION] Fetching {symbol} via Yahoo Finance fallback...")
            response = requests.get(url, params=params, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                result = data['chart']['result'][0]
                timestamps = result['timestamp']
                quotes = result['indicators']['quote'][0]
                
                df = pd.DataFrame({
                    'Open': quotes['open'],
                    'High': quotes['high'],
                    'Low': quotes['low'],
                    'Close': quotes['close']
                }, index=pd.to_datetime(timestamps, unit='s'))
                
                df.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)
                df.index = df.index.tz_localize('UTC').dt.tz_convert(self.ny_tz) if hasattr(df.index, 'dt') else df.index.tz_localize('UTC').tz_convert(self.ny_tz)
                df.index.name = 'datetime'
                return df[['Open', 'High', 'Low', 'Close']]
        except Exception as e:
            if config.DEBUG_SUMMARY:
                print(f"[FALLBACK WARNING] Yahoo Finance query failed for {symbol}: {e}")
        return None

    def _generate_mock_data(self, symbol, timeframe, days):
        """
        Fallback generator.
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
            if day_of_week == 5:
                is_weekend = True
            elif day_of_week == 4 and hour >= 17:
                is_weekend = True
            elif day_of_week == 6 and hour < 17:
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
        
        opens, highs, lows, closes = [], [], [], []
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
        if df_raw.empty:
            return df_raw
        if not isinstance(df_raw.index, pd.DatetimeIndex):
            df_raw.index = pd.to_datetime(df_raw.index)
        offset_rule = "17h"
        
        if interval == "1D":
            resampled = df_raw.resample('24h', offset=offset_rule).agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
            }).dropna()
        elif interval == "H4":
            resampled = df_raw.resample('4h', offset=offset_rule).agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
            }).dropna()
        elif interval == "H1":
            resampled = df_raw.resample('1h').agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
            }).dropna()
        elif interval == "M30":
            resampled = df_raw.resample('30min').agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
            }).dropna()
        elif interval == "M15":
            resampled = df_raw.resample('15min').agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
            }).dropna()
        else:
            raise ValueError(f"Unsupported resampling interval: {interval}")
            
        return resampled
