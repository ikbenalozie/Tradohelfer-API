# main.py
# Fibonacci Retracement Python Trading Application - Central Entry Point
# Upgraded: Runs a polite 24-hour startup backfill sweep before starting the infinite scan loop.

import time
import sys
import config
from scanner import TradingScanner

def main():
    """
    Main loop execution.
    Initializes components, runs a startup backfill sweep, and then processes targets on a set timer.
    """
    print("=" * 75)
    print("  FIBONACCI RETRACEMENT MULTI-TIMEFRAME SCANNER (INDEPENDENT PYTHON APP)  ")
    print("=" * 75)
    print("Initializing components...")
    print(f"Monitoring {len(config.SYMBOLS)} Assets: {', '.join(config.SYMBOLS)}")
    print(f"Candles Standard: True New York Daily Close (17:00 EST/EDT)")
    print(f"Multi-Timeframe Cascade: H4->H1, H1->M30, M30->M15")
    print(f"Scan rate interval: Checked every {config.TIMER_SECONDS} seconds.")
    print("To stop the scanner, press [Ctrl + C] anytime.")
    print("-" * 75)
    
    try:
        scanner = TradingScanner()
    except Exception as e:
        print(f"[FATAL ERROR] Initialization failed: {e}")
        sys.exit(1)
        
    # Execute Startup Backfill (Politely pushes last 24 hours of history to database)
    try:
        scanner.run_startup_backfill(backfill_hours=24)
    except Exception as e:
        print(f"[WARNING] Startup backfill hit an error, but continuing to live scan: {e}")
        
    print("Initialization complete! Starting live scan cycle loop...")
    
    cycle_count = 0
    while True:
        try:
            cycle_count += 1
            if config.DEBUG_SUMMARY:
                print(f"\n[CYCLE #{cycle_count}] Processing targets...")
            
            scanner.run_scan_cycle()
            
            # Sleep for configured interval
            time.sleep(config.TIMER_SECONDS)
            
        except KeyboardInterrupt:
            print("\nShutting down scanner gracefully...")
            print("Spreadsheet CSV logs saved. Goodbye!")
            sys.exit(0)
        except Exception as e:
            print(f"[LOOP EXCEPTION] Unexpected error occurred: {e}")
            print(f"Retrying in {config.TIMER_SECONDS} seconds...")
            time.sleep(config.TIMER_SECONDS)

if __name__ == "__main__":
    main()
