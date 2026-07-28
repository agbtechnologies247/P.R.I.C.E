import os
import sys
import asyncio
import sqlite3
import datetime
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fyers_apiv3 import fyersModel

# Inject python-broker path into sys.path to enable local imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import local modules
import config
from database import init_db
import state
import websocket_handler
from models import HistoryRequest
import auth
import orders

app = FastAPI(title="PRICE Python Broker Adapter (Fyers Bridge)")

# Include routers
app.include_router(auth.router)
app.include_router(orders.router)

@app.on_event("startup")
def startup_event():
    state.main_loop = asyncio.get_running_loop()
    init_db()

    if os.environ.get("MOCK_BROKER_KEYS") == "true":
        print("MOCK_BROKER_KEYS is true. Starting Python broker in SIMULATED mode.")
        state.fyers_client = "mock"
        return

    client_id = os.environ.get("FYERS_CLIENT_ID")
    
    # Read persisted token if available
    token = None
    token_file_path = os.path.join(os.getcwd(), "token.txt")
    if os.path.exists(token_file_path):
        try:
            with open(token_file_path, "r") as f:
                token = f.read().strip()
            print("Found persisted access token in token.txt")
        except Exception as e:
            print(f"Error reading token.txt: {e}")
            
    # Or fallback to reading access token from env
    if not token:
        token = os.environ.get("FYERS_ACCESS_TOKEN")
        
    if client_id and token:
        try:
            state.fyers_client = fyersModel.FyersModel(
                client_id=client_id,
                token=token,
                log_path=os.getcwd()
            )
            print("Successfully initialized Fyers client from token.")
            
            # Start Live Market Feed WebSockets
            websocket_handler.start_fyers_socket(client_id, token)
        except Exception as e:
            print(f"Error initializing Fyers API client: {e}")
            state.fyers_client = None
    else:
        print("Fyers API Client uninitialized. Please utilize /auth_url and /login_token to authenticate.")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    state.connected_ws.append(websocket)
    print(f"Rust Client subscribed to data stream. Active clients: {len(state.connected_ws)}")
    try:
        while True:
            # Keep client alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        state.connected_ws.remove(websocket)
        print(f"Rust Client disconnected. Active clients: {len(state.connected_ws)}")

@app.post("/history")
def parse_date_str(d_str: str) -> datetime.datetime:
    d_str = d_str.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(d_str, fmt)
        except ValueError:
            pass
    raise ValueError(f"Invalid date string format: {d_str}")

@app.post("/history")
def get_history(req: HistoryRequest):
    fyers = state.get_client()
    
    try:
        from_dt = parse_date_str(req.range_from)
        to_dt = parse_date_str(req.range_to)
        epoch_from = int(from_dt.timestamp())
        epoch_to = int(to_dt.timestamp())
    except Exception as parse_err:
        raise HTTPException(status_code=400, detail=f"Invalid date parameter format: {parse_err}")

    # 1. Try to check local SQLite database cache first
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT timestamp, open, high, low, close, volume FROM historical_candles WHERE symbol = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC",
            (req.symbol, epoch_from, epoch_to)
        )
        rows = cursor.fetchall()
        conn.close()
        
        if len(rows) > 0:
            print(f"Retrieved {len(rows)} cached candles from SQLite for {req.symbol}")
            return {"status": "success", "data": {"candles": [list(r) for r in rows]}}
    except Exception as e:
        print(f"SQLite reading cache failed: {e}")

    # 2. Cache miss: If in mock mode, generate mock candles
    if fyers is None or fyers == "mock":
        raise HTTPException(status_code=400, detail="Fyers API client is uninitialized or unauthenticated. Please authenticate via dashboard first.")

    # Format parameters for Fyers API
    # Try date_format="1" (YYYY-MM-DD) first, fallback to date_format="0" (Epoch timestamps)
    date_format_options = [
        ("1", from_dt.strftime("%Y-%m-%d"), to_dt.strftime("%Y-%m-%d")),
        ("0", str(int(from_dt.replace(hour=9, minute=15, second=0).timestamp())), str(int(to_dt.replace(hour=15, minute=30, second=0).timestamp())))
    ]

    last_error_msg = "Unknown error from Fyers history API"
    last_err_code = -1

    for df_mode, r_from, r_to in date_format_options:
        data = {
            "symbol": req.symbol,
            "resolution": req.resolution,
            "date_format": df_mode,
            "range_from": r_from,
            "range_to": r_to,
            "cont_flag": "1"
        }

        max_retries = 2
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Querying Fyers history API (mode={df_mode}): {data} (Attempt {attempt}/{max_retries})")
                res = fyers.history(data=data)
                if res.get("s") == "ok":
                    candles = res.get("candles", [])
                    if len(candles) > 0:
                        try:
                            conn = sqlite3.connect(config.DB_PATH)
                            cursor = conn.cursor()
                            for c in candles:
                                cursor.execute(
                                    "INSERT OR REPLACE INTO historical_candles (symbol, timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                    (req.symbol, int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), int(c[5]))
                                )
                            conn.commit()
                            conn.close()
                            print(f"Cached {len(candles)} new candles in SQLite database for {req.symbol}")
                        except Exception as e:
                            print(f"SQLite writing cache failed: {e}")
                    return {"status": "success", "data": {"candles": candles}}
                else:
                    last_err_code = res.get("code", -1)
                    last_error_msg = res.get("message", "Unknown error")
                    print(f"Fyers history response error (mode={df_mode}): code={last_err_code}, msg={last_error_msg}")

                    if last_err_code in [-429, 429] and attempt < max_retries:
                        import time
                        time.sleep(2.0 * attempt)
                        continue
            except Exception as e:
                print(f"Exception during Fyers history query (mode={df_mode}): {e}")
                last_error_msg = str(e)
                import time
                time.sleep(1.0)

    COMMON_ERRORS = {
        -8: "Token is Expired. Please regenerate the token.",
        -15: "Invalid token. Please regenerate the token.",
        -16: "Server unable to authenticate token. Please authenticate again.",
        -17: "Token is Invalid or Expired. Please authenticate again.",
        -50: "Invalid parameters passed to Fyers API.",
        -300: "Invalid symbol provided.",
        -352: "Invalid App ID provided.",
        -403: "Data API permission missing or invalid parameters.",
        -429: "Fyers API rate limit exceeded.",
    }
    friendly_desc = COMMON_ERRORS.get(last_err_code)
    error_msg = f"{last_error_msg} (Code {last_err_code}: {friendly_desc})" if friendly_desc else f"{last_error_msg} (Code {last_err_code})"
    raise HTTPException(status_code=400, detail=f"Fyers history query failed: {error_msg}")

