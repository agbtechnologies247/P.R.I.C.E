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
def get_history(req: HistoryRequest):
    fyers = state.get_client()
    
    # 1. Try to check local SQLite database cache first
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cursor = conn.cursor()
        epoch_from = int(datetime.datetime.strptime(req.range_from, "%Y-%m-%d").timestamp())
        epoch_to = int(datetime.datetime.strptime(req.range_to, "%Y-%m-%d").replace(hour=23, minute=59, second=59).timestamp())
        
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

    # 3. Cache miss: Query Fyers API
    try:
        data = {
            "symbol": req.symbol,
            "resolution": req.resolution,
            "date_format": "1",  # "1" for YYYY-MM-DD string date format
            "range_from": req.range_from,
            "range_to": req.range_to,
            "cont_flag": "1"
        }
        res = fyers.history(data=data)
        if res.get("s") == "ok":
            candles = res.get("candles", [])
            # Write to SQLite Cache asynchronously
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
            err_code = res.get("code")
            detail = res.get("message", "Unknown error from Fyers history API")
            
            # Fyers API Error Codes Mapping
            COMMON_ERRORS = {
                -8: "Token is Expired. Please regenerate the token.",
                -15: "Invalid token. Please regenerate the token.",
                -16: "Server unable to authenticate token. Please authenticate again.",
                -17: "Token is Invalid or Expired. Please authenticate again.",
                -50: "Invalid parameters passed to Fyers API.",
                -300: "Invalid symbol provided.",
                -352: "Invalid App ID provided.",
                -429: "Fyers API rate limit exceeded.",
            }
            friendly_desc = COMMON_ERRORS.get(err_code)
            if friendly_desc:
                error_msg = f"{detail} (Code {err_code}: {friendly_desc})"
            elif err_code is not None:
                error_msg = f"{detail} (Code {err_code})"
            else:
                error_msg = detail
                
            raise HTTPException(status_code=400, detail=f"Fyers history query failed: {error_msg}")
    except HTTPException:
        raise
    except Exception as e:
        print(f"Fyers client history exception: {e}")
        raise HTTPException(status_code=500, detail=f"Fyers history query failed with server error: {str(e)}")
