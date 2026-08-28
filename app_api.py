# app_api.py (Upgraded Version with OneSignal Push notifications, Self-Healing Port Router, and Memory Monitor)
# 100% Free-Tier Unified Backend for Fibonacci Retracement Trading Application

import os
import json
import sqlite3
import threading
import time
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime
import uvicorn
from fastapi import FastAPI, HTTPException, Header, Depends, status, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import config

app = FastAPI(
    title="Tradohelfer Unified Cloud Workstation",
    description="FastAPI Web Server & Background Market Scanner on a single free Render Web Service.",
    version="3.0.0"
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
# ONESIGNAL PUSH NOTIFICATION DISPATCHER (0-dependency Server Integration)
# =====================================================================
def trigger_push_notification(title: str, body: str, signal_id: Optional[int] = None):
    """
    Fires a high-priority push notification to all Android subscribers via OneSignal REST API.
    Does not block request execution threads by running in an isolated safety-try context.
    """
    app_id = getattr(config, "ONESIGNAL_APP_ID", "")
    api_key = getattr(config, "ONESIGNAL_API_KEY", "")
    
    # Skip if placeholder or empty
    if not app_id or not api_key or "your_onesignal" in app_id:
        return
        
    url = "https://onesignal.com/api/v1/notifications"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": f"Basic {api_key}"
    }
    
    # Signal redirection payload data
    data_payload = {}
    if signal_id is not None:
        data_payload["signal_id"] = str(signal_id)
        
    payload = {
        "app_id": app_id,
        "headings": {"en": title},
        "contents": {"en": body},
        "included_segments": ["All"], # Direct broadcast to all installed devices
        "priority": 10,               # High priority delivery to bypass Android sleep/doze locks
        "data": data_payload
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=8)
        if config.DEBUG_SUMMARY:
            print(f"[ONESIGNAL PUSH] Dispatched alert. Status: {response.status_code}")
    except Exception as ex:
        print(f"❌ [ONESIGNAL ERROR] Failed to send push notification: {ex}")

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
        with open('/proc/self/status', 'r') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    parts = line.split()
                    return float(parts[1]) / 1024.0  # Convert kB to MB
    except Exception:
        pass
    
    try:
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
    type: str
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
    except Exception:
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
def receive_webhook(payload: dict = Body(...), auth: str = Depends(verify_api_key)):
    """
    Dual-mode Webhook Endpoint:
    Seamlessly parses single signals OR bundled high-speed batches inside a single atomic SQLite transaction!
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Determine if payload is raw dictionary representing a single signal or a batch
        # Using raw parsing to accommodate dynamic typing
        payload_data = payload
        if hasattr(payload, "dict"):
            payload_data = payload.dict()
            
        if not isinstance(payload_data, dict):
            # Parse from request raw if needed
            raise ValueError("Payload must be a structured JSON object.")

        is_batch = payload_data.get("type") == "batch"
        
        # 1. BATCH MODE COMPILATION (Transactional)
        if is_batch:
            signals_list = payload_data.get("signals", [])
            count = len(signals_list)
            added_count = 0
            latest_signal_id = None
            push_summary_elements = []
            
            for raw_sig in signals_list:
                # Deduplication Guard
                cursor.execute("""
                    SELECT id FROM signals 
                    WHERE symbol = ? AND tf = ? AND pattern = ? AND bar_time = ?
                """, (raw_sig['symbol'], raw_sig['tf'], raw_sig['pattern'], raw_sig['bar_time']))
                
                if cursor.fetchone():
                    continue
                
                cursor.execute("""
                    INSERT INTO signals (
                        symbol, tf, pattern, dir, bar_time, open, high, low, close,
                        entry1, entry2, sl, tp1, tp2, tp1_swing, tp2_swing,
                        confidence, reasons, parent_id, zones, chart_candles, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    raw_sig['symbol'],
                    raw_sig['tf'],
                    raw_sig['pattern'],
                    raw_sig['dir'],
                    raw_sig['bar_time'],
                    raw_sig['open'],
                    raw_sig['high'],
                    raw_sig['low'],
                    raw_sig['close'],
                    raw_sig['entry1'],
                    raw_sig['entry2'],
                    raw_sig['sl'],
                    raw_sig['tp1'],
                    raw_sig['tp2'],
                    raw_sig['tp1_swing'],
                    raw_sig['tp2_swing'],
                    raw_sig['confidence'],
                    raw_sig['reasons'],
                    raw_sig.get('parent_id', ""),
                    json.dumps(raw_sig['zones']),
                    json.dumps(raw_sig['chart_candles']),
                    datetime.utcnow().isoformat() + "Z"
                ))
                latest_signal_id = cursor.lastrowid
                added_count += 1
                
                # Collect details for push notifications summary
                if len(push_summary_elements) < 3:
                    push_summary_elements.append(f"{raw_sig['symbol']} {raw_sig['tf']} ({raw_sig['dir']})")
            
            conn.commit()
            conn.close()
            
            # Send high-priority batch notification if new setups were recorded
            if added_count > 0:
                summary_text = ", ".join(push_summary_elements)
                if added_count > 3:
                    summary_text += f", and {added_count - 3} more"
                
                trigger_push_notification(
                    title=f"🚨 {added_count} New Trading Setups Detected!",
                    body=f"Target setups closed on chart bounds: {summary_text}.",
                    signal_id=latest_signal_id
                )
                
            return {"success": True, "message": f"Processed batch of {count} signals. Committed {added_count} new entries."}
            
        # 2. SINGLE SIGNAL MODE
        else:
            # Re-map schema validation internally
            raw_sig = payload_data
            cursor.execute("""
                SELECT id FROM signals 
                WHERE symbol = ? AND tf = ? AND pattern = ? AND bar_time = ?
            """, (raw_sig['symbol'], raw_sig['tf'], raw_sig['pattern'], raw_sig['bar_time']))
            
            if cursor.fetchone():
                conn.close()
                return {"success": True, "message": "Duplicate signal ignored."}
                
            cursor.execute("""
                INSERT INTO signals (
                    symbol, tf, pattern, dir, bar_time, open, high, low, close,
                    entry1, entry2, sl, tp1, tp2, tp1_swing, tp2_swing,
                    confidence, reasons, parent_id, zones, chart_candles, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                raw_sig['symbol'],
                raw_sig['tf'],
                raw_sig['pattern'],
                raw_sig['dir'],
                raw_sig['bar_time'],
                raw_sig['open'],
                raw_sig['high'],
                raw_sig['low'],
                raw_sig['close'],
                raw_sig['entry1'],
                raw_sig['entry2'],
                raw_sig['sl'],
                raw_sig['tp1'],
                raw_sig['tp2'],
                raw_sig['tp1_swing'],
                raw_sig['tp2_swing'],
                raw_sig['confidence'],
                raw_sig['reasons'],
                raw_sig.get('parent_id', ""),
                json.dumps(raw_sig['zones']),
                json.dumps(raw_sig['chart_candles']),
                datetime.utcnow().isoformat() + "Z"
            ))
            new_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            # Fire instant push alert!
            trigger_push_notification(
                title=f"🚨 New setup: {raw_sig['symbol']} {raw_sig['tf']} ({raw_sig['dir']})!",
                body=f"Pattern: {raw_sig['pattern'].upper()} | Confidence: {raw_sig['confidence'].upper()} | Entry: {raw_sig['entry1']}",
                signal_id=new_id
            )
            
            return {"success": True, "message": f"Signal received and logged for {raw_sig['symbol']} {raw_sig['tf']}."}
            
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Database execution error: {ex}")

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
        # Self-Healing Port Alignment: Read dynamic port assigned by Render ($PORT)
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
