from pydantic import BaseModel
from typing import List, Optional

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

class SubscribeRequest(BaseModel):
    symbols: List[str]
