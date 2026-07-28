use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerType {
    Fyers,
    Zerodha,
    Angel,
    Upstox,
    Paper,
    DeltaExchange,
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
    pub r#type: i32, // 1 for Limit, 2 for Market, 3 for Stop Market, 4 for Stop Limit
    pub side: Side,
    pub limit_price: f64,
    pub stop_price: f64,
    pub leverage: Option<u32>,
    pub reduce_only: Option<bool>,
    pub post_only: Option<bool>,
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

/// Funding rate snapshot for a perpetual futures contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingRate {
    pub symbol: String,
    pub rate: f64,            // e.g. 0.0001 = 0.01%
    pub timestamp: i64,       // Unix seconds
    pub next_funding_time: i64,
}

/// Instrument / product metadata from Delta Exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentMeta {
    pub product_id: i64,
    pub symbol: String,
    pub contract_type: String,  // "perpetual_futures", "call_options", etc.
    pub contract_size: f64,
    pub min_size: f64,
    pub tick_size: f64,
    pub max_leverage: f64,
    pub underlying_asset: String,
}

/// Margin mode for a futures position.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MarginMode {
    Isolated,
    Cross,
}

/// Static leverage configuration for Delta Exchange perpetual contracts.
/// BTC and ETH trade at 200x, SOL trades at 100x per user specification.
pub struct DeltaLeverageConfig;

impl DeltaLeverageConfig {
    /// Returns the configured leverage for a symbol.
    /// Defaults to 10x for any unknown symbol.
    pub fn leverage_for(symbol: &str) -> u32 {
        let s = symbol.to_uppercase();
        if s.contains("BTC") {
            200
        } else if s.contains("ETH") {
            200
        } else if s.contains("SOL") {
            100
        } else {
            10
        }
    }

    /// Returns true if the symbol is a supported crypto perpetual.
    pub fn is_supported_perp(symbol: &str) -> bool {
        let s = symbol.to_uppercase();
        s.contains("BTC") || s.contains("ETH") || s.contains("SOL")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leverage_config_btc() {
        assert_eq!(DeltaLeverageConfig::leverage_for("BTCUSD_PERP"), 200);
        assert_eq!(DeltaLeverageConfig::leverage_for("BTC-PERPETUAL"), 200);
    }

    #[test]
    fn test_leverage_config_eth() {
        assert_eq!(DeltaLeverageConfig::leverage_for("ETHUSD_PERP"), 200);
    }

    #[test]
    fn test_leverage_config_sol() {
        assert_eq!(DeltaLeverageConfig::leverage_for("SOLUSD_PERP"), 100);
    }

    #[test]
    fn test_leverage_config_unknown() {
        assert_eq!(DeltaLeverageConfig::leverage_for("XRPUSD_PERP"), 10);
    }
}

