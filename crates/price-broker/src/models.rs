use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerType {
    Fyers,
    Zerodha,
    Angel,
    Upstox,
    Paper,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderStatus {
    PENDING,
    FILLED,
    CANCELLED,
    REJECTED,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Side {
    Buy = 1,
    Sell = -1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProfile {
    pub name: String,
    pub fy_id: String,
    pub email: String,
    pub pin_set: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountFunds {
    pub available_balance: f64,
    pub utilised_balance: f64,
    pub limit_amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub broker: BrokerType,
    pub symbol: String,
    pub side: Side,
    pub quantity: i32,
    pub avg_price: f64,
    pub status: OrderStatus,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub side: Side,
    pub buy_qty: i32,
    pub sell_qty: i32,
    pub avg_price: f64,
    pub current_price: f64,
    pub pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Holding {
    pub symbol: String,
    pub qty: i32,
    pub avg_price: f64,
    pub current_price: f64,
    pub pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub symbol: String,
    pub last_price: f64,
    pub bid: f64,
    pub ask: f64,
    pub volume: u64,
    pub oi: u64,
    pub prev_close: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub trade_id: String,
    pub order_id: String,
    pub symbol: String,
    pub qty: i32,
    pub price: f64,
    pub side: Side,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub symbol: String,
    pub qty: i32,
    pub r#type: i32, // 1 for Limit, 2 for Market
    pub side: Side,
    pub limit_price: f64,
    pub stop_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub status: String,
    pub message: String,
    pub order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyOrder {
    pub id: String,
    pub qty: i32,
    pub r#type: i32,
    pub limit_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryRequest {
    pub symbol: String,
    pub resolution: String,
    pub date_format: String,
    pub range_from: String,
    pub range_to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleSeries {
    pub candles: Vec<Vec<f64>>, // [[timestamp, open, high, low, close, volume], ...]
}
