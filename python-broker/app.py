import os
import time
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fyers_apiv3 import fyersModel

# Load .env configuration programmatically
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(env_path):
    try:
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key.strip()] = val.strip()
        print("Successfully loaded environment configuration from .env")
    except Exception as e:
        print(f"Error loading .env file: {e}")

app = FastAPI(title="PRICE Python Broker Adapter (Fyers Bridge)")

# Real Fyers Client reference
fyers_client = None

class OrderRequest(BaseModel):
    symbol: str
    qty: int
    type: int  # 1: Limit, 2: Market
    side: int  # 1: Buy, -1: Sell
    limitPrice: Optional[float] = 0.0
    stopPrice: Optional[float] = 0.0

class ModifyOrderRequest(BaseModel):
    id: str
    qty: int
    type: int
    limitPrice: Optional[float] = 0.0

class HistoryRequest(BaseModel):
    symbol: str
    resolution: str
    date_format: str
    range_from: str
    range_to: str

class TokenRequest(BaseModel):
    auth_code: str

@app.on_event("startup")
def startup_event():
    global fyers_client
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
            fyers_client = fyersModel.FyersModel(
                client_id=client_id,
                token=token,
                log_path=os.getcwd()
            )
            print("Successfully initialized Fyers client from token.")
        except Exception as e:
            print(f"Error initializing Fyers API client: {e}")
            fyers_client = None
    else:
        print("Fyers API Client uninitialized. Please utilize /auth_url and /login_token to authenticate.")

def get_client() -> fyersModel.FyersModel:
    if fyers_client is None:
        raise HTTPException(
            status_code=503, 
            detail="Fyers API Client is not initialized. Ensure FYERS_CLIENT_ID and FYERS_ACCESS_TOKEN are set."
        )
    return fyers_client

@app.get("/auth_url")
def get_auth_url():
    client_id = os.environ.get("FYERS_CLIENT_ID")
    secret_key = os.environ.get("FYERS_SECRET_KEY")
    redirect_uri = os.environ.get("FYERS_REDIRECT_URI", "https://price.agbtechnologies.in")
    
    if not client_id or not secret_key:
        raise HTTPException(status_code=400, detail="FYERS_CLIENT_ID and FYERS_SECRET_KEY are not configured.")
        
    try:
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            grant_type="authorization_code"
        )
        url = session.generate_authcode()
        return {"status": "success", "auth_url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating auth url: {e}")

@app.post("/login_token")
def login_token(req: TokenRequest):
    global fyers_client
    client_id = os.environ.get("FYERS_CLIENT_ID")
    secret_key = os.environ.get("FYERS_SECRET_KEY")
    redirect_uri = os.environ.get("FYERS_REDIRECT_URI", "https://price.agbtechnologies.in")
    
    if not client_id or not secret_key:
        raise HTTPException(status_code=400, detail="FYERS_CLIENT_ID and FYERS_SECRET_KEY are not configured.")
        
    try:
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            grant_type="authorization_code"
        )
        session.set_token(req.auth_code)
        res = session.generate_token()
        if res.get("s") == "ok":
            access_token = res.get("access_token")
            fyers_client = fyersModel.FyersModel(
                client_id=client_id,
                token=access_token,
                log_path=os.getcwd()
            )
            
            # Save token to token.txt so it persists across runs
            token_file_path = os.path.join(os.getcwd(), "token.txt")
            with open(token_file_path, "w") as f:
                f.write(access_token)
                
            return {"status": "success", "access_token": access_token}
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Token generation failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers exchange token error: {e}")

@app.get("/health")
def health():
    return {
        "status": "healthy" if fyers_client else "unconfigured", 
        "mode": "fyers",
        "initialized": fyers_client is not None
    }

@app.post("/login")
def login():
    get_client()
    return {"status": "success", "access_token": "fyers_active_session"}

@app.post("/logout")
def logout():
    # Remove persisted token to log out
    token_file_path = os.path.join(os.getcwd(), "token.txt")
    if os.path.exists(token_file_path):
        try:
            os.remove(token_file_path)
        except Exception:
            pass
    global fyers_client
    fyers_client = None
    return {"status": "success"}

@app.get("/profile")
def profile():
    fyers = get_client()
    try:
        res = fyers.get_profile()
        if res.get("s") == "ok":
            data = res.get("data", {})
            return {
                "status": "success",
                "data": {
                    "name": data.get("name", "Fyers Client"),
                    "fy_id": data.get("fy_id", "FYERS"),
                    "email": data.get("email_id", "fyers@client.com"),
                    "pin_set": True
                }
            }
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers profile check failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@app.get("/funds")
def funds():
    fyers = get_client()
    try:
        res = fyers.funds()
        if res.get("s") == "ok":
            fund_limits = res.get("fund_limit", [])
            available = 0.0
            utilised = 0.0
            limit = 0.0
            for item in fund_limits:
                title = item.get("title", "")
                amount = item.get("equityAmount", 0.0)
                if "Total Balance" in title or "Adhoc Margin" in title:
                    limit = amount
                elif "Utilized Margin" in title or "Margin Used" in title:
                    utilised = amount
                elif "Clear Balance" in title or "Available Balance" in title:
                    available = amount
                    
            if limit == 0.0:
                limit = available + utilised
            return {
                "status": "success",
                "data": {
                    "available_balance": available if available > 0.0 else limit - utilised,
                    "utilised_balance": utilised,
                    "limit_amount": limit
                }
            }
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers funds check failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@app.get("/positions")
def positions():
    fyers = get_client()
    try:
        res = fyers.positions()
        if res.get("s") == "ok":
            pos_list = res.get("netPositions", [])
            mapped = []
            for p in pos_list:
                mapped.append({
                    "symbol": p.get("symbol"),
                    "side": 1 if p.get("side", 1) > 0 else -1,
                    "buy_qty": p.get("buyQty", 0),
                    "sell_qty": p.get("sellQty", 0),
                    "avg_price": p.get("avgPrice", 0.0),
                    "current_price": p.get("ltp", 0.0),
                    "pnl": p.get("pl", 0.0)
                })
            return {"status": "success", "data": mapped}
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers positions check failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@app.get("/holdings")
def holdings():
    fyers = get_client()
    try:
        res = fyers.holdings()
        if res.get("s") == "ok":
            holdings_list = res.get("holdings", [])
            mapped = []
            for h in holdings_list:
                mapped.append({
                    "symbol": h.get("symbol"),
                    "qty": h.get("quantity", 0),
                    "avg_price": h.get("costPrice", 0.0),
                    "current_price": h.get("ltp", 0.0),
                    "pnl": h.get("pl", 0.0)
                })
            return {"status": "success", "data": mapped}
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers holdings check failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@app.post("/order")
def place_order(req: OrderRequest):
    fyers = get_client()
    try:
        data = {
            "symbol": req.symbol,
            "qty": req.qty,
            "type": req.type,
            "side": req.side,
            "productType": "INTRADAY",
            "limitPrice": req.limitPrice,
            "stopPrice": req.stopPrice,
            "validity": "DAY",
            "offlineOrder": False
        }
        res = fyers.place_order(data)
        if res.get("s") == "ok":
            return {
                "status": "success",
                "message": res.get("message", "Order placed successfully"),
                "order_id": res.get("id")
            }
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers place order failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@app.put("/order")
def modify_order(req: ModifyOrderRequest):
    fyers = get_client()
    try:
        data = {
            "id": req.id,
            "qty": req.qty,
            "type": req.type,
            "limitPrice": req.limitPrice
        }
        res = fyers.modify_order(data)
        if res.get("s") == "ok":
            return {
                "status": "success",
                "message": res.get("message", "Order modified successfully"),
                "order_id": req.id
            }
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers modify order failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@app.delete("/order")
def cancel_order(order_id: str):
    fyers = get_client()
    try:
        res = fyers.cancel_order({"id": order_id})
        if res.get("s") == "ok":
            return {"status": "success", "message": "Order cancelled successfully"}
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers cancel order failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@app.get("/orders")
def get_orders():
    fyers = get_client()
    try:
        res = fyers.orderbook()
        if res.get("s") == "ok":
            orders_list = res.get("orderBook", [])
            mapped = []
            for o in orders_list:
                status_str = "PENDING"
                fyers_status = o.get("status")
                if fyers_status == 2:
                    status_str = "FILLED"
                elif fyers_status == 1:
                    status_str = "CANCELLED"
                elif fyers_status == 5:
                    status_str = "REJECTED"
                    
                mapped.append({
                    "id": o.get("id"),
                    "symbol": o.get("symbol"),
                    "qty": o.get("qty", 0),
                    "side": 1 if o.get("side") == 1 else -1,
                    "price": o.get("avgPrice", 0.0),
                    "status": status_str,
                    "avg_price": o.get("avgPrice", 0.0),
                    "filled_qty": o.get("filledQty", 0),
                    "timestamp": int(time.time())
                })
            return {"status": "success", "data": mapped}
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers orderbook check failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@app.get("/trades")
def get_trades():
    fyers = get_client()
    try:
        res = fyers.tradebook()
        if res.get("s") == "ok":
            trade_list = res.get("tradeBook", [])
            mapped = []
            for t in trade_list:
                mapped.append({
                    "trade_id": t.get("id"),
                    "order_id": t.get("orderId"),
                    "symbol": t.get("symbol"),
                    "qty": t.get("qty", 0),
                    "price": t.get("tradeValue", 0.0) / max(1, t.get("qty", 1)),
                    "side": 1 if t.get("side") == 1 else -1,
                    "timestamp": int(time.time())
                })
            return {"status": "success", "data": mapped}
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers tradebook check failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@app.post("/quotes")
def get_quotes(symbols: List[str]):
    fyers = get_client()
    try:
        symbols_str = ",".join(symbols)
        res = fyers.quotes({"symbols": symbols_str})
        if res.get("s") == "ok":
            quotes_list = res.get("d", [])
            mapped = {}
            for item in quotes_list:
                val = item.get("v", {})
                sym = val.get("symbol")
                mapped[sym] = {
                    "symbol": sym,
                    "last_price": val.get("lp", 500.0),
                    "bid": val.get("bid", 0.0),
                    "ask": val.get("ask", 0.0),
                    "volume": val.get("volume", 0),
                    "oi": val.get("oi", 0),
                    "prev_close": val.get("prev_close", 0.0)
                }
            return {"status": "success", "data": mapped}
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers quotes query failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@app.post("/history")
def get_history(req: HistoryRequest):
    fyers = get_client()
    try:
        data = {
            "symbol": req.symbol,
            "resolution": req.resolution,
            "date_format": req.date_format,
            "range_from": req.range_from,
            "range_to": req.range_to,
            "cont_flag": "1"
        }
        res = fyers.history(data=data)
        if res.get("s") == "ok":
            return {"status": "success", "data": {"candles": res.get("candles", [])}}
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers history check failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")
