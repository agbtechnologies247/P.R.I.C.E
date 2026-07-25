use std::sync::Arc;
use tokio::sync::Mutex;
use dashmap::DashMap;
use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;
use price_core::{PriceError, Result};
use crate::models::*;
use crate::traits::Broker;

pub struct PaperBroker {
    funds: Arc<Mutex<AccountFunds>>,
    orders: DashMap<String, Order>,
    positions: DashMap<String, Position>,
    trades: Arc<Mutex<Vec<Trade>>>,
    holdings: Arc<Mutex<Vec<Holding>>>,
}

impl PaperBroker {
    pub fn new(initial_balance: f64) -> Self {
        Self {
            funds: Arc::new(Mutex::new(AccountFunds {
                available_balance: initial_balance,
                utilised_balance: 0.0,
                limit_amount: initial_balance,
            })),
            orders: DashMap::new(),
            positions: DashMap::new(),
            trades: Arc::new(Mutex::new(Vec::new())),
            holdings: Arc::new(Mutex::new(vec![
                Holding {
                    symbol: "NSE:SBIN-EQ".to_string(),
                    qty: 50,
                    avg_price: 580.0,
                    current_price: 595.0,
                    pnl: 750.0,
                }
            ])),
        }
    }
}

#[async_trait]
impl Broker for PaperBroker {
    async fn login(&self) -> Result<String> {
        Ok("paper_session_token_12345".to_string())
    }

    async fn logout(&self) -> Result<()> {
        Ok(())
    }

    async fn profile(&self) -> Result<UserProfile> {
        Ok(UserProfile {
            name: "Paper Trader".to_string(),
            fy_id: "FY-PAPER".to_string(),
            email: "paper@price.com".to_string(),
            pin_set: true,
        })
    }

    async fn funds(&self) -> Result<AccountFunds> {
        let funds = self.funds.lock().await;
        Ok(funds.clone())
    }

    async fn positions(&self) -> Result<Vec<Position>> {
        Ok(self.positions.iter().map(|r| r.value().clone()).collect())
    }

    async fn holdings(&self) -> Result<Vec<Holding>> {
        let h = self.holdings.lock().await;
        Ok(h.clone())
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        let order_id = format!("paper-ord-{}", Uuid::new_v4().simple());
        let now = Utc::now().timestamp();
        
        let mut funds = self.funds.lock().await;
        let execution_price = if request.r#type == 1 { request.limit_price } else { 500.0 };
        let cost = request.qty as f64 * execution_price;
        
        if request.side == Side::Buy && funds.available_balance < cost {
            return Err(PriceError::InsufficientFunds {
                available: funds.available_balance,
                required: cost,
            });
        }
        
        // Update balance
        if request.side == Side::Buy {
            funds.available_balance -= cost;
            funds.utilised_balance += cost;
        } else {
            funds.available_balance += cost;
            funds.utilised_balance -= cost;
        }
        
        let order = Order {
            id: order_id.clone(),
            broker: BrokerType::Paper,
            symbol: request.symbol.clone(),
            side: request.side,
            quantity: request.qty,
            avg_price: execution_price,
            status: OrderStatus::FILLED,
            timestamp: now,
        };
        
        self.orders.insert(order_id.clone(), order);
        
        // Record Trade
        let mut trades = self.trades.lock().await;
        trades.push(Trade {
            trade_id: format!("paper-trd-{}", Uuid::new_v4().simple()),
            order_id: order_id.clone(),
            symbol: request.symbol.clone(),
            qty: request.qty,
            price: execution_price,
            side: request.side,
            timestamp: now,
        });
        
        // Update Position
        let symbol = request.symbol.clone();
        let qty_change = request.qty * (request.side as i32);
        
        if self.positions.contains_key(&symbol) {
            let mut pos = self.positions.get_mut(&symbol).unwrap();
            let prev_qty = if pos.side == Side::Buy { pos.buy_qty } else { -pos.sell_qty };
            let new_qty = prev_qty + qty_change;
            
            if new_qty == 0 {
                drop(pos);
                self.positions.remove(&symbol);
            } else {
                pos.side = if new_qty > 0 { Side::Buy } else { Side::Sell };
                pos.buy_qty = if new_qty > 0 { new_qty } else { 0 };
                pos.sell_qty = if new_qty < 0 { -new_qty } else { 0 };
                pos.avg_price = execution_price;
                pos.current_price = execution_price;
                pos.pnl = 0.0;
            }
        } else {
            self.positions.insert(symbol.clone(), Position {
                symbol: symbol.clone(),
                side: request.side,
                buy_qty: if request.side == Side::Buy { request.qty } else { 0 },
                sell_qty: if request.side == Side::Sell { request.qty } else { 0 },
                avg_price: execution_price,
                current_price: execution_price,
                pnl: 0.0,
            });
        }
        
        Ok(OrderResponse {
            status: "success".to_string(),
            message: "Order placed".to_string(),
            order_id,
        })
    }

    async fn modify_order(&self, request: ModifyOrder) -> Result<OrderResponse> {
        if !self.orders.contains_key(&request.id) {
            return Err(PriceError::InvalidOrder(format!("Order {} not found", request.id)));
        }
        let mut order = self.orders.get_mut(&request.id).unwrap();
        if order.status != OrderStatus::PENDING {
            return Err(PriceError::InvalidOrder("Can only modify pending orders".to_string()));
        }
        order.quantity = request.qty;
        order.avg_price = request.limit_price;
        
        Ok(OrderResponse {
            status: "success".to_string(),
            message: "Order modified".to_string(),
            order_id: request.id,
        })
    }

    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        if !self.orders.contains_key(order_id) {
            return Err(PriceError::InvalidOrder(format!("Order {} not found", order_id)));
        }
        let mut order = self.orders.get_mut(order_id).unwrap();
        if order.status != OrderStatus::PENDING {
            return Err(PriceError::InvalidOrder("Can only cancel pending orders".to_string()));
        }
        order.status = OrderStatus::CANCELLED;
        Ok(())
    }

    async fn orderbook(&self) -> Result<Vec<Order>> {
        Ok(self.orders.iter().map(|r| r.value().clone()).collect())
    }

    async fn trades(&self) -> Result<Vec<Trade>> {
        let t = self.trades.lock().await;
        Ok(t.clone())
    }

    async fn quotes(&self, symbols: Vec<String>) -> Result<Vec<Quote>> {
        let mut quotes = Vec::new();
        for sym in symbols {
            quotes.push(Quote {
                symbol: sym,
                last_price: 500.0,
                bid: 499.90,
                ask: 500.10,
                volume: 12000,
                oi: 1500000,
                prev_close: 495.0,
            });
        }
        Ok(quotes)
    }

    async fn history(&self, request: HistoryRequest) -> Result<CandleSeries> {
        // Return 100 dummy minutes
        let mut candles = Vec::new();
        let now_ts = Utc::now().timestamp() as f64;
        let mut base_price = 500.0;
        for i in 0..100 {
            let t = now_ts - (100.0 - i as f64) * 60.0;
            let o = base_price + (i % 5) as f64 - 2.0;
            let h = o + 2.0;
            let l = o - 2.0;
            let c = o + (i % 3) as f64 - 1.0;
            let v = 1000.0 + (i * 50) as f64;
            candles.push(vec![t, o, h, l, c, v]);
            base_price = c;
        }
        Ok(CandleSeries { candles })
    }
}
