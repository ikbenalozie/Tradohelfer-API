# app_api.py (Unified Batch-Capable Version with Self-Healing Port Router and Memory Monitor)
# 100% Free-Tier Unified Backend for Fibonacci Retracement Trading Application
# This file combines the FastAPI server AND the Python scanner into a single service,
# allowing you to run your entire trading application on Render's FREE Web Service tier!

import os
import json
import sqlite3
import threading
import time
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import uvicorn
from fastapi import FastAPI, HTTPException, Header, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(
    title="Gemini Notebook Free-Tier Trading App",
    description="Unified API & Background Scanner running on a single free web service instance with memory tracking and batch-webhook processing.",
    version="2.3.0"
)

# Enable CORS (Cross-Origin Resource Sharing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "scanner_signals.db"
API_KEY_HEADER = "X-Api-Key"
EXPECTED_API_KEY = "Ikealoben_2025bijna"

# =====================================================================
# MEMORY MONITORING UTILITY
# =====================================================================
def get_memory_usage_mb() -> float:
    """
    Returns the current resident memory usage of this process in Megabytes (MB).
    Reads the Linux /proc filesystem directly for 0-dependency performance on Render,
    with a fallback check for local Windows or macOS testing environments.
    """
    try:
        # Standard Linux proc filesystem (100% reliable on Render, 0 dependencies)
        with open('/proc/self/status', 'r') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    # Line looks like: VmRSS:     45320 kB
                    parts = line.split()
                    return float(parts[1]) / 1024.0  # Convert kB to MB
    except Exception:
        pass
    
    try:
        # Fallback for local Windows or macOS test environments
        import psutil
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024.0 * 1024.0)  # Convert bytes to MB
    except Exception:
        return 0.0

# =====================================================================
# DATABASE SETUP
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

init_db()

# =====================================================================
# DATA VALIDATION SCHEMAS
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

class BatchPayload(BaseModel):
    type: str  # e.g., "batch"
    count: int
    signals: List[SignalPayload]

# =====================================================================
# SECURITY DEPENDENCY
# =====================================================================
def verify_api_key(x_api_key: Optional[str] = Header(None, alias=API_KEY_HEADER)):
    if x_api_key != EXPECTED_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized: Invalid X-Api-Key header value."
        )
    return x_api_key

# =====================================================================
# ENDPOINTS
# =====================================================================
@app.get("/")
def read_root():
    return {
        "status": "online",
        "service": "Gemini Unified Free Service",
        "time": datetime.utcnow().isoformat() + "Z",
        "info": "To keep this service awake for free, set up a ping on UptimeRobot to hit this URL every 10 minutes."
    }

@app.get("/api/health")
def get_health():
    """
    Lightweight diagnostic endpoint to monitor server health, thread count, 
    SQLite database records, and active process memory utilisation vs Render's 512MB limit.
    """
    mem_used = get_memory_usage_mb()
    mem_capacity = 512.0  # Render Free Tier Limit
    mem_percent = (mem_used / mem_capacity) * 100.0 if mem_used > 0 else 0.0
    
    # Classify overall server health based on memory utilisation
    health_status = "healthy"
    if mem_percent >= 90.0:
        health_status = "warning: critical memory usage (approaching 512MB container limit)"
    elif mem_percent >= 75.0:
        health_status = "warning: high memory usage"
        
    # Count stored signals in database
    total_signals = 0
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM signals")
        total_signals = cursor.fetchone()[0]
        conn.close()
    except Exception as e:
        total_signals = -1

    return {
        "status": health_status,
        "memory_used_mb": round(mem_used, 2),
        "memory_capacity_mb": mem_capacity,
        "memory_utilisation_percent": f"{round(mem_percent, 1)}%",
        "sqlite_total_signals": total_signals,
        "active_threads": threading.active_count(),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

@app.post("/webhook", status_code=status.HTTP_201_CREATED)
def receive_webhook(payload: Union[SignalPayload, BatchPayload], auth: str = Depends(verify_api_key)):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Determine if payload is single signal or batch
        if isinstance(payload, BatchPayload) or (hasattr(payload, "type") and payload.type == "batch"):
            signals_list = payload.signals
            is_batch = True
        else:
            signals_list = [payload]
            is_batch = False
            
        inserted_count = 0
        duplicate_count = 0
        
        # Single database transaction for lightning-speed commits
        for sig in signals_list:
            # Deduplication Guard
            cursor.execute("""
                SELECT id FROM signals 
                WHERE symbol = ? AND tf = ? AND pattern = ? AND bar_time = ?
            """, (sig.symbol, sig.tf, sig.pattern, sig.bar_time))
            
            if cursor.fetchone():
                duplicate_count += 1
                continue
                
            cursor.execute("""
                INSERT INTO signals (
                    symbol, tf, pattern, dir, bar_time, open, high, low, close,
                    entry1, entry2, sl, tp1, tp2, tp1_swing, tp2_swing,
                    confidence, reasons, parent_id, zones, chart_candles, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sig.symbol,
                sig.tf,
                sig.pattern,
                sig.dir,
                sig.bar_time,
                sig.open,
                sig.high,
                sig.low,
                sig.close,
                sig.entry1,
                sig.entry2,
                sig.sl,
                sig.tp1,
                sig.tp2,
                sig.tp1_swing,
                sig.tp2_swing,
                sig.confidence,
                sig.reasons,
                sig.parent_id,
                json.dumps(sig.zones.dict()),
                json.dumps([c.dict() for c in sig.chart_candles]),
                datetime.utcnow().isoformat() + "Z"
            ))
            inserted_count += 1
            
        conn.commit()
        conn.close()
        
        if is_batch:
            return {
                "success": True, 
                "message": f"Successfully processed batch payload. Signals Inserted: {inserted_count}, Duplicates Ignored: {duplicate_count}."
            }
        else:
            if inserted_count > 0:
                return {"success": True, "message": f"Signal received for {payload.symbol} {payload.tf}."}
            else:
                return {"success": True, "message": "Duplicate signal ignored."}
                
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Database error: {ex}")

@app.get("/api/signals")
def get_signals(limit: int = 50):
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
        return [dict(r) for r in rows]
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Database error: {ex}")

@app.get("/api/signals/{signal_id}")
def get_signal_details(signal_id: int):
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
        data["zones"] = json.loads(data["zones"])
        data["chart_candles"] = json.loads(data["chart_candles"])
        return data
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Database error: {ex}")

# =====================================================================
# BACKGROUND THREAD IGNITION (Runs scanner in parallel)
# =====================================================================
def start_background_scanner():
    """
    Spawns main.py's scanning loop inside a background daemon thread.
    This runs continuously in parallel with the FastAPI server.
    Automatically handles self-healing port mapping based on Render's configuration.
    """
    print("\n⚡ [FREE TIER ENGINE] Spawning background market scanner thread... ⚡")
    try:
        import config
        # Self-Healing Port Alignment: Read dynamic port assigned by Render ($PORT)
        # Overrides config.py values at runtime so the loop can communicate locally on the correct port.
        port = os.getenv("PORT", "8000")
        config.ENABLE_WEBHOOK = True
        config.WEBHOOK_URL = f"http://127.0.0.1:{port}/webhook"
        print(f"⚡ [FREE TIER ENGINE] Self-healing router successfully active! Webhook overridden to: {config.WEBHOOK_URL}")

        import main
        # Run main loop as a separate daemon thread so it doesn't block the API
        scanner_thread = threading.Thread(target=main.main, daemon=True)
        scanner_thread.start()
        print("⚡ [FREE TIER ENGINE] Scanner thread successfully initialized and running! ⚡\n")
    except Exception as ex:
        print(f"❌ [FREE TIER ENGINE ERROR] Failed to start scanner background thread: {ex}")

@app.on_event("startup")
def on_startup():
    start_background_scanner()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app_api:app", host="0.0.0.0", port=port, reload=False)
