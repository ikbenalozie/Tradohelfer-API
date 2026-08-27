# app_api.py
# FastAPI Web Server Backend for Fibonacci Retracement Python Trading Application
# Receives scanner signal webhook payloads, stores them in an SQLite database,
# and serves them to our Flutter mobile application with high efficiency.

import os
import json
import sqlite3
from typing import List, Dict, Any, Optional
from datetime import datetime
import uvicorn
from fastapi import FastAPI, HTTPException, Header, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(
    title="Gemini Notebook Trading Scanner API",
    description="Backend API to receive trading alerts and serve candlestick data to mobile clients.",
    version="1.0.1"
)

# Enable CORS (Cross-Origin Resource Sharing) so our mobile/web app can connect freely
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "scanner_signals.db"
API_KEY_HEADER = "X-Api-Key"
EXPECTED_API_KEY = "Ikealoben_2025bijna"  # Matches config.py Auth Header

# =====================================================================
# DATABASE SETUP (SQLite - Free, lightweight and zero-dependency)
# =====================================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            tf TEXT NOT NULL,
            pattern TEXT NOT NULL,
            dir TEXT NOT NULL,
            bar_time TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            entry1 REAL,
            entry2 REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            tp1_swing REAL,
            tp2_swing REAL,
            confidence TEXT,
            reasons TEXT,
            parent_id TEXT,
            zones TEXT,            -- JSON encoded string
            chart_candles TEXT,    -- JSON encoded string
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

# Initialize Database
init_db()

# =====================================================================
# PYDANTIC SCHEMAS (Data Validation Models)
# =====================================================================
class ZoneRange(BaseModel):
    low: float
    high: float

class EntryZone(BaseModel):
    risk: ZoneRange
    r1: ZoneRange
    r2: ZoneRange

class Zones(BaseModel):
    entry1: EntryZone
    entry2: EntryZone

class Candle(BaseModel):
    time: str
    open: float
    high: float
    low: float
    close: float

class SignalPayload(BaseModel):
    type: str
    symbol: str
    tf: str
    pattern: str
    dir: str
    bar_time: str
    open: float
    high: float
    low: float
    close: float
    entry1: float
    entry1_kind: str
    entry2: float
    entry2_kind: str
    sl: float
    tp1: float
    tp2: float
    tp1_swing: float
    tp2_swing: float
    confidence: str
    reasons: str
    parent_id: Optional[str] = ""
    zones: Zones
    chart_candles: List[Candle]

# =====================================================================
# SECURITY DEPENDENCY (Validates API key from scanner webhook)
# =====================================================================
def verify_api_key(x_api_key: Optional[str] = Header(None, alias=API_KEY_HEADER)):
    if x_api_key != EXPECTED_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized: Invalid X-Api-Key header value."
        )
    return x_api_key

# =====================================================================
# API ROUTERS & ENDPOINTS
# =====================================================================

@app.get("/")
def read_root():
    return {
        "status": "online",
        "service": "Gemini Notebook Scanner API",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

@app.post("/webhook", status_code=status.HTTP_201_CREATED)
def receive_webhook(payload: SignalPayload, auth: str = Depends(verify_api_key)):
    """
    Webhook endpoint hit by our Python scanner.
    Saves the full detailed signal including zones and historical candles.
    Avoids duplicate entries by checking if the signal was already logged.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Deduplication check: verify if we already logged this pattern at this bar_time
        cursor.execute("""
            SELECT id FROM signals 
            WHERE symbol = ? AND tf = ? AND bar_time = ? AND pattern = ?
        """, (payload.symbol, payload.tf, payload.bar_time, payload.pattern))
        
        existing = cursor.fetchone()
        if existing:
            conn.close()
            return {"success": True, "message": f"Signal already logged for {payload.symbol} {payload.tf} at {payload.bar_time}. Skipping duplicate."}
            
        cursor.execute("""
            INSERT INTO signals (
                symbol, tf, pattern, dir, bar_time, open, high, low, close,
                entry1, entry2, sl, tp1, tp2, tp1_swing, tp2_swing,
                confidence, reasons, parent_id, zones, chart_candles, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            payload.symbol,
            payload.tf,
            payload.pattern,
            payload.dir,
            payload.bar_time,
            payload.open,
            payload.high,
            payload.low,
            payload.close,
            payload.entry1,
            payload.entry2,
            payload.sl,
            payload.tp1,
            payload.tp2,
            payload.tp1_swing,
            payload.tp2_swing,
            payload.confidence,
            payload.reasons,
            payload.parent_id,
            json.dumps(payload.zones.dict()),
            json.dumps([c.dict() for c in payload.chart_candles]),
            datetime.utcnow().isoformat() + "Z"
        ))
        
        conn.commit()
        conn.close()
        return {"success": True, "message": f"Signal received and logged for {payload.symbol} {payload.tf}."}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Database write error: {ex}")

@app.get("/api/signals")
def get_signals(limit: int = 50):
    """
    Returns a list of recent signals. 
    To save mobile internet bandwidth, we EXCLUDE the massive 'chart_candles' list in this index view!
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, symbol, tf, pattern, dir, bar_time, open, high, low, close,
                   entry1, entry2, sl, tp1, tp2, tp1_swing, tp2_swing, confidence, reasons, parent_id, created_at
            FROM signals
            ORDER BY id DESC
            LIMIT ?
        """, (limit,))
        
        rows = cursor.fetchall()
        conn.close()
        
        signals = []
        for r in rows:
            signals.append(dict(r))
            
        return signals
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Database read error: {ex}")

@app.get("/api/signals/{signal_id}")
def get_signal_details(signal_id: int):
    """
    Returns the complete detailed signal for a single trade.
    Includes the 'chart_candles' array and the coordinates of the risk-reward 'zones' 
    to draw our MT5-grade candlestick charts instantly.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            raise HTTPException(status_code=404, detail="Signal not found.")
            
        data = dict(row)
        # Parse the JSON columns back into native lists/dicts
        data["zones"] = json.loads(data["zones"])
        data["chart_candles"] = json.loads(data["chart_candles"])
        
        return data
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Database detailed read error: {ex}")

# =====================================================================
# WEB SERVER IGNITION (Uvicorn)
# =====================================================================
if __name__ == "__main__":
    # To run locally: python app_api.py
    # Runs on port 8000 by default. Works perfectly on Render free tier.
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app_api:app", host="0.0.0.0", port=port, reload=True)
