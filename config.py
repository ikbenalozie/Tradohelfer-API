# config.py
# Fibonacci Retracement Python Trading Application - Core Configuration Settings
# This file serves as the "Central Control Panel" of our application. 
# It contains all the adjustable dials, parameters, and credentials so we do not have to hardcode them.

import os

# =====================================================================
# 1. API DATA FEED CREDENTIALS (Tiingo - Professional Free Tier API)
# =====================================================================
# To use this scanner, register for a free account at https://api.tiingo.com
# The free API key allows you to scan currency pairs and cryptocurrencies.
# Supports pooling: list multiple keys separated by commas to load-balance requests!
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "2fac73017858992590f586b0f1c4253272a2ee6f, 5df00b430c4fcbb3b30ddcc70564a62bd1c75f9d, 16934076793cf20c6f01106f6b765622362c9732, 7f64a92153c5f962e9717e37b06ec88bf30a2700, e3c76de88aa3850661f29426661db73b36d10b13")

# =====================================================================
# 2. TARGET SYMBOLS TO SCAN
# =====================================================================
# A complete list of major and minor Forex pairs, commodities, and BTC.
# Fully scalable under our new multi-key pooled and throttled engine.
SYMBOLS = [
    # --- Majors & Major Minors ---
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    
    # --- Euro Crosses ---
    "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURNZD", "EURCAD",
    
    # --- Pound Crosses ---
    "GBPJPY", "GBPCHF", "GBPAUD", "GBPNZD", "GBPCAD",
    
    # --- Aussie & Kiwi Crosses ---
    "AUDJPY", "AUDCHF", "AUDCAD", "AUDNZD",
    "NZDJPY", "NZDCHF", "NZDCAD",
    
    # --- Cadet & Franc Crosses ---
    "CADJPY", "CADCHF",
    "CHFJPY",
    
    # --- Metals & Commodities ---
    "XAUUSD", "XAGUSD", "USOUSD",
    
    # --- Cryptocurrencies ---
    "BTCUSD"
]

# =====================================================================
# 3. TIME DIRECTION CONSTRAINTS & TIMEZONES
# =====================================================================
# True New York Daily Close logic requires converting UTC data into New York Time
NEW_YORK_TIMEZONE = "America/New_York"

# =====================================================================
# 4. TECHNICAL INDICATOR INPUTS (Matched to the original MQL5 source)
# =====================================================================
ATR_PERIOD = 14              # Period used for volatility measurement (ATR) [2]
TAIL_FRAC_MIN = 0.35         # Minimum tail-to-body fraction for Pinbars [2]
OPP_WICK_MAX_FRAC = 0.50     # Maximum opposite wick size relative to body [3]
USE_ENGULFING = True         # Allow scanning for engulfing patterns [3]

# Timeframe minimum ranges (relative to ATR)
MIN_RANGE_ATR_H4 = 0.50      # H4 candle range must be at least 50% of H4 ATR [3]
MIN_RANGE_SPR_H4 = 4.0       # H4 range in points (scaled to decimal) [3]
MIN_RANGE_ATR_H1 = 0.30      # H1 candle range must be at least 30% of H1 ATR [3]
MIN_RANGE_SPR_H1 = 3.0       # H1 range in points (scaled to decimal) [3]

# Entry offsets
ENTRY_BUF_ATR = 0.05         # Breakout entry buffer as a fraction of ATR [3]

# =====================================================================
# 5. ADAPTIVE CCI TECHNICAL PARAMETERS
# =====================================================================
USE_ADAPTIVE_CCI = True      # Filter entry signals using our dynamic momentum band [5]
ACCI_CCI_PERIOD = 14         # Period used for base Commodity Channel Index calculation [5]
ACCI_BASE_THRESHOLD = 100    # Initial base threshold for momentum bands [5]
ACCI_EMA_SMOOTHING = 0.2     # Smoothing multiplier (Alpha) for the Adaptive Bands [5]
ACCI_ATR_PERIOD = 14         # ATR period used to scale volatility bands [5]
ACCI_VOLATILITY_FACTOR = 0.5 # Volatility multiplier to expand/contract the bands [5]

# ACCI Momentum & Overextension thresholds
ACCI_OVEREXT_MARGIN_FRAC = 0.10  # 10% safety margin added to bands for overextension checks [7]
H1_BIG_BAR_ATR_MULT = 1.50       # Large candle threshold: 1.5x ATR [7]

# =====================================================================
# 6. DYNAMIC RISK MANAGEMENT CONSTANTS
# =====================================================================
# Dynamic Stop-Loss factors depending on current timeframe relationships [6, 7]
H1_DYN_MIN = 0.18            # Stop-Loss multiplier for tight volatility ranges
H1_DYN_MAX = 0.30            # Stop-Loss multiplier for wide volatility ranges
RATIO_MIN = 0.15             # Lower clamp limit for ATR ratio [6]
RATIO_MAX = 0.45             # Upper clamp limit for ATR ratio [7]

# =====================================================================
# 7. EXECUTION & LOGGING SETTINGS
# =====================================================================
TIMER_SECONDS = 30           # Refresh interval for the scanner (checks for new data) [3]
WRITE_CSV = True             # Output detected signals to a local spreadsheet [3]
CSV_PATH = "signals_multi_tf.csv"  # Output filename for alerts and historical logs [27]
DEBUG_SUMMARY = True         # Show detailed diagnostic logs in the terminal [3]

# Webhook Alerting (Optional)
ENABLE_WEBHOOK = True       # Set to True to send automated POST signals [4]
WEBHOOK_URL = "https://tradohelfer-api.onrender.com/webhook"             # URL of your Discord, Telegram, or custom server [4]
AUTH_HEADER = "X-Api-Key: Ikealoben_2025bijna"  # Secure API key header for webhook endpoints [4]
WEBHOOK_TIMEOUT_MS = 15000   # Webhook request timeout in milliseconds [4]
