import time
import uuid
from typing import List
from fastapi import APIRouter, HTTPException
import state
from models import OrderRequest, ModifyOrderRequest

router = APIRouter()

@router.get("/funds")
def funds():
    fyers = state.get_client()
    if fyers == "mock":
        return {
            "status": "success",
            "data": {
                "available_balance": 10000.0,
                "utilised_balance": 0.0,
                "limit_amount": 10000.0
            }
        }
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

@router.get("/positions")
def positions():
    fyers = state.get_client()
    if fyers == "mock":
        return {"status": "success", "data": []}
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

@router.get("/holdings")
def holdings():
    fyers = state.get_client()
    if fyers == "mock":
        return {"status": "success", "data": []}
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

@router.post("/order")
def place_order(req: OrderRequest):
    fyers = state.get_client()
    if fyers == "mock":
        order_id = f"mock-ord-{uuid.uuid4().hex[:12]}"
        return {
            "status": "success",
            "message": "Simulated order placed successfully",
            "order_id": order_id
        }
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

@router.put("/order")
def modify_order(req: ModifyOrderRequest):
    fyers = state.get_client()
    if fyers == "mock":
        return {
            "status": "success",
            "message": "Simulated order modified successfully",
            "order_id": req.id
        }
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

@router.delete("/order")
def cancel_order(order_id: str):
    fyers = state.get_client()
    if fyers == "mock":
        return {"status": "success", "message": "Simulated order cancelled successfully"}
    try:
        res = fyers.cancel_order({"id": order_id})
        if res.get("s") == "ok":
            return {"status": "success", "message": "Order cancelled successfully"}
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers cancel order failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@router.get("/orders")
def get_orders():
    fyers = state.get_client()
    if fyers == "mock":
        return {"status": "success", "data": []}
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

@router.get("/trades")
def get_trades():
    fyers = state.get_client()
    if fyers == "mock":
        return {"status": "success", "data": []}
    try:
        res = fyers.tradebook()
        if res.get("s") == "ok":
            trades_list = res.get("tradeBook", [])
            mapped = []
            for t in trades_list:
                mapped.append({
                    "id": t.get("id"),
                    "symbol": t.get("symbol"),
                    "qty": t.get("qty", 0),
                    "side": 1 if t.get("side") == 1 else -1,
                    "price": t.get("tradePrice", 0.0),
                    "timestamp": int(time.time())
                })
            return {"status": "success", "data": mapped}
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers tradebook check failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")

@router.post("/quotes")
def get_quotes(symbols: List[str]):
    fyers = state.get_client()
    if fyers == "mock":
        mapped = {}
        for sym in symbols:
            price = 24100.0 if "NIFTY50" in sym else (15.0 if "VIX" in sym else 100.0)
            mapped[sym] = {
                "symbol": sym,
                "last_price": price,
                "bid": price * 0.999,
                "ask": price * 1.001,
                "volume": 1000,
                "oi": 10000,
                "prev_close": price
            }
        return {"status": "success", "data": mapped}
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
