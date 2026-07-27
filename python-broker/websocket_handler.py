import time
import asyncio
import threading
from fyers_apiv3.FyersWebsocket import data_ws
import state

def on_open():
    print("Fyers Live Market Data Socket connection established.")
    # Standard initial symbols (Nifty index + 50 constituents)
    symbols = [
        "NSE:NIFTY50-INDEX",
        "NSE:INDIAVIX-INDEX",
        "NSE:RELIANCE-EQ", "NSE:BHARTIARTL-EQ", "NSE:HDFCBANK-EQ", "NSE:ICICIBANK-EQ",
        "NSE:SBIN-EQ", "NSE:TCS-EQ", "NSE:BAJFINANCE-EQ", "NSE:LT-EQ", "NSE:HINDUNILVR-EQ",
        "NSE:SUNPHARMA-EQ", "NSE:MARUTI-EQ", "NSE:INFY-EQ", "NSE:TITAN-EQ", "NSE:ADANIENT-EQ",
        "NSE:ADANIPORTS-EQ", "NSE:M&M-EQ", "NSE:KOTAKBANK-EQ", "NSE:AXISBANK-EQ", "NSE:ITC-EQ",
        "NSE:ULTRACEMCO-EQ", "NSE:HCLTECH-EQ", "NSE:NTPC-EQ", "NSE:ONGC-EQ", "NSE:BAJAJ-AUTO-EQ",
        "NSE:JSWSTEEL-EQ", "NSE:BAJAJFINSV-EQ", "NSE:BEL-EQ", "NSE:ETERNAL-EQ", "NSE:POWERGRID-EQ",
        "NSE:COALINDIA-EQ", "NSE:ASIANPAINT-EQ", "NSE:SHRIRAMFIN-EQ", "NSE:TATASTEEL-EQ", "NSE:HINDALCO-EQ",
        "NSE:GRASIM-EQ", "NSE:EICHERMOT-EQ", "NSE:INDIGO-EQ", "NSE:SBILIFE-EQ", "NSE:WIPRO-EQ",
        "NSE:JIOFIN-EQ", "NSE:TRENT-EQ", "NSE:TECHM-EQ", "NSE:APOLLOHOSP-EQ", "NSE:HDFCLIFE-EQ",
        "NSE:TMPV-EQ", "NSE:CIPLA-EQ", "NSE:TATACONSUM-EQ", "NSE:MAXHEALTH-EQ", "NSE:DRREDDY-EQ",
        "NSE:NESTLEIND-EQ"
    ]
    if state.fyers_socket:
        state.fyers_socket.subscribe(symbols=symbols, data_type="SymbolUpdate")
        state.fyers_socket.keep_running()

def on_close(message):
    print(f"Fyers Live Market Data Socket connection closed: {message}")

def on_error(message):
    print(f"Fyers Live Market Data Socket connection error: {message}")

def on_message(message):
    # Route tick to connected Rust WebSocket clients
    symbol = message.get("symbol")
    price = message.get("ltp")
    volume = message.get("vol_tradedtoday", 0)
    oi = message.get("oi", 0)

    if state.main_loop and symbol and price:
        tick_data = {
            "symbol": symbol,
            "price": float(price),
            "volume": int(volume),
            "oi": int(oi),
            "timestamp": int(time.time())
        }
        # Run async send thread-safely in the main asyncio loop
        for ws in list(state.connected_ws):
            asyncio.run_coroutine_threadsafe(ws.send_json(tick_data), state.main_loop)

def start_fyers_socket(client_id: str, token: str):
    access_token = f"{client_id}:{token}"
    
    state.fyers_socket = data_ws.FyersDataSocket(
        access_token=access_token,
        litemode=False,
        write_to_file=False,
        reconnect=True,
        on_connect=on_open,
        on_close=on_close,
        on_error=on_error,
        on_message=on_message
    )
    
    t = threading.Thread(target=state.fyers_socket.connect, daemon=True)
    t.start()
    print("Fyers Data Websocket service started in background thread.")
