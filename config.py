# config.py
# Fibonacci Retracement Python Trading Application - Core Configuration Settings
# This file serves as the "Central Control Panel" of our application. 

import os

# =====================================================================
# 1. API DATA FEED CREDENTIALS (Tiingo - Professional Free Tier API)
# =====================================================================
# Supports pooling: list multiple keys separated by commas to load-balance requests!
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "2fac73017858992590f586b0f1c4253272a2ee6f, 5df00b430c4fcbb3b30ddcc70564a62bd1c75f9d, 16934076793cf20c6f01106f6b765622362c9732, 7f64a92153c5f962e9717e37b06ec88bf30a2700, e3c76de88aa3850661f29426661db73b36d10b13")

# =====================================================================
# 1B. PUSH NOTIFICATION CREDENTIALS (OneSignal - 100% Free Push Service)
# =====================================================================
# Register at https://onesignal.com to get your App ID and REST API Key.
# Set them here to enable instant high-priority mobile lockscreen push alerts!
ONESIGNAL_APP_ID = os.getenv("ONESIGNAL_APP_ID", "your_onesignal_app_id_here")
ONESIGNAL_API_KEY = os.getenv("ONESIGNAL_API_KEY", "your_onesignal_api_key_here")

# =====================================================================
# 2. TARGET SYMBOLS TO SCAN
# =====================================================================
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
NEW_YORK_TIMEZONE = "America/New_York"

# =====================================================================
# 4. TECHNICAL INDICATOR INPUTS (Matched to the original MQL5 source)
# =====================================================================
ATR_PERIOD = 14              
TAIL_FRAC_MIN = 0.35         
OPP_WICK_MAX_FRAC = 0.50     
USE_ENGULFING = True         

# Timeframe minimum ranges (relative to ATR)
MIN_RANGE_ATR_H4 = 0.50      
MIN_RANGE_SPR_H4 = 4.0       
MIN_RANGE_ATR_H1 = 0.30      
MIN_RANGE_SPR_H1 = 3.0       

# Entry offsets
ENTRY_BUF_ATR = 0.05         

# =====================================================================
# 5. ADAPTIVE CCI TECHNICAL PARAMETERS
# =====================================================================
USE_ADAPTIVE_CCI = True      
ACCI_CCI_PERIOD = 14         
ACCI_BASE_THRESHOLD = 100    
ACCI_EMA_SMOOTHING = 0.2     
ACCI_ATR_PERIOD = 14         
ACCI_VOLATILITY_FACTOR = 0.5 

# ACCI Momentum & Overextension thresholds
ACCI_OVEREXT_MARGIN_FRAC = 0.10  
H1_BIG_BAR_ATR_MULT = 1.50       

# =====================================================================
# 6. DYNAMIC RISK MANAGEMENT CONSTANTS
# =====================================================================
H1_DYN_MIN = 0.18            
H1_DYN_MAX = 0.30            
RATIO_MIN = 0.15             
RATIO_MAX = 0.45             

# =====================================================================
# 7. EXECUTION & LOGGING SETTINGS
# =====================================================================
TIMER_SECONDS = 30           
WRITE_CSV = True             
CSV_PATH = "signals_multi_tf.csv"  
DEBUG_SUMMARY = True         

# Webhook Alerting
ENABLE_WEBHOOK = True       
WEBHOOK_URL = ""             
AUTH_HEADER = "X-Api-Key: Ikealoben_2025bijna"  
WEBHOOK_TIMEOUT_MS = 15000   
