use std::sync::Arc;
use tokio::sync::Mutex;
use dashmap::DashMap;
use async_trait::async_trait;
use chrono::{Utc, DateTime};
use uuid::Uuid;
use price_core::{PriceError, Result};
use price_broker::{Broker, BrokerType, OrderStatus, Side, UserProfile, AccountFunds, Order, Position, Holding, Quote, Trade, OrderRequest, OrderResponse, ModifyOrder, HistoryRequest, CandleSeries};
use price_timeseries::TimescaleClient;

pub struct ReplayBroker {
    funds: Arc<Mutex<AccountFunds>>,
    orders: DashMap<String, Order>,
    positions: DashMap<String, Position>,
    trades: Arc<Mutex<Vec<Trade>>>,
    holdings: Arc<Mutex<Vec<Holding>>>,
    current_prices: DashMap<String, Quote>,
    slippage_pct: f64,
    commission: f64,
    pub current_time: Arc<std::sync::Mutex<DateTime<Utc>>>,
    db: TimescaleClient,
    python_broker_url: String,
}

impl ReplayBroker {
    pub fn new(
        initial_balance: f64,
        slippage_pct: f64,
        commission: f64,
        db: TimescaleClient,
        python_broker_url: String,
    ) -> Self {
        Self {
            funds: Arc::new(Mutex::new(AccountFunds {
                available_balance: initial_balance,
                utilised_balance: 0.0,
                limit_amount: initial_balance,
            })),
            orders: DashMap::new(),
            positions: DashMap::new(),
            trades: Arc::new(Mutex::new(Vec::new())),
            holdings: Arc::new(Mutex::new(Vec::new())),
            current_prices: DashMap::new(),
            slippage_pct,
            commission,
            current_time: Arc::new(std::sync::Mutex::new(Utc::now())),
            db,
            python_broker_url,
        }
    }

    pub fn set_current_time(&self, time: DateTime<Utc>) {
        let mut t = self.current_time.lock().unwrap();
        *t = time;
    }

    pub fn update_price(&self, symbol: &str, price: f64, volume: u64, oi: u64) {
        let bid = price * (1.0 - self.slippage_pct);
        let ask = price * (1.0 + self.slippage_pct);
        
        self.current_prices.insert(symbol.to_string(), Quote {
            symbol: symbol.to_string(),
            last_price: price,
            bid,
            ask,
            volume,
            oi,
            prev_close: price,
        });

        if let Some(mut pos) = self.positions.get_mut(symbol) {
            pos.current_price = price;
            let qty = if pos.side == Side::Buy { pos.buy_qty } else { pos.sell_qty };
            let direction = if pos.side == Side::Buy { 1.0 } else { -1.0 };
            pos.pnl = (price - pos.avg_price) * (qty as f64) * direction;
        }
    }

    pub async fn get_equity(&self) -> f64 {
        let funds = self.funds.lock().await;
        let mut open_pnl = 0.0;
        for r in self.positions.iter() {
            open_pnl += r.value().pnl;
        }
        funds.available_balance + funds.utilised_balance + open_pnl
    }
}


#[async_trait]
impl Broker for ReplayBroker {
    async fn login(&self) -> Result<String> {
        Ok("replay_token".to_string())
    }

    async fn logout(&self) -> Result<()> {
        Ok(())
    }

    async fn profile(&self) -> Result<UserProfile> {
        Ok(UserProfile {
            name: "Replay Tester".to_string(),
            fy_id: "REPLAY".to_string(),
            email: "backtester@price.com".to_string(),
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
        let order_id = format!("replay-ord-{}", Uuid::new_v4().simple());
        let now = Utc::now().timestamp();
        
        let quotes_res = self.quotes(vec![request.symbol.clone()]).await?;
        let ltp = quotes_res.first().map(|q| q.last_price).unwrap_or(100.0);

        let execution_price = match request.side {
            Side::Buy => ltp * (1.0 + self.slippage_pct),
            Side::Sell => ltp * (1.0 - self.slippage_pct),
        };

        let cost = request.qty as f64 * execution_price;
        let total_charge = cost + self.commission;

        let mut funds = self.funds.lock().await;
        if request.side == Side::Buy && funds.available_balance < total_charge {
            return Err(PriceError::InsufficientFunds {
                available: funds.available_balance,
                required: total_charge,
            });
        }

        if request.side == Side::Buy {
            funds.available_balance -= total_charge;
            funds.utilised_balance += cost;
        } else {
            funds.available_balance += cost - self.commission;
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

        let mut trades = self.trades.lock().await;
        trades.push(Trade {
            trade_id: format!("replay-trd-{}", Uuid::new_v4().simple()),
            order_id: order_id.clone(),
            symbol: request.symbol.clone(),
            qty: request.qty,
            price: execution_price,
            side: request.side,
            timestamp: now,
        });

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
            message: "Order filled".to_string(),
            order_id,
        })
    }

    async fn modify_order(&self, request: ModifyOrder) -> Result<OrderResponse> {
        Ok(OrderResponse {
            status: "success".to_string(),
            message: "Modified".to_string(),
            order_id: request.id,
        })
    }

    async fn cancel_order(&self, _order_id: &str) -> Result<()> {
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
            if let Some(q) = self.current_prices.get(&sym) {
                quotes.push(q.value().clone());
            } else {
                let current_time = {
                    let t_guard = self.current_time.lock().unwrap();
                    *t_guard
                };

                let mut price = None;
                let start_range = current_time - chrono::Duration::seconds(30);
                let end_range = current_time + chrono::Duration::seconds(30);

                if let Ok(candles) = self.db.get_candles(&sym, "1m", start_range, end_range).await {
                    if let Some(c) = candles.first() {
                        price = Some(c.close);
                    }
                }

                let final_price = if let Some(p) = price {
                    p
                } else {
                    return Err(PriceError::SymbolNotFound(sym));
                };

                let bid = final_price * (1.0 - self.slippage_pct);
                let ask = final_price * (1.0 + self.slippage_pct);

                quotes.push(Quote {
                    symbol: sym.clone(),
                    last_price: final_price,
                    bid,
                    ask,
                    volume: 5000,
                    oi: 100000,
                    prev_close: final_price,
                });
            }
        }
        Ok(quotes)
    }

    async fn history(&self, _request: HistoryRequest) -> Result<CandleSeries> {
        Ok(CandleSeries { candles: Vec::new() })
    }
}
