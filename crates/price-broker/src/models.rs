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

/// Delta Exchange order type enumeration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderType {
    LimitOrder,
    MarketOrder,
    StopMarketOrder,
    StopLimitOrder,
}

impl OrderType {
    /// Convert from legacy integer type code to OrderType.
    pub fn from_legacy_int(code: i32) -> Self {
        match code {
            1 => OrderType::LimitOrder,
            2 => OrderType::MarketOrder,
            3 => OrderType::StopMarketOrder,
            4 => OrderType::StopLimitOrder,
            _ => OrderType::MarketOrder,
        }
    }

    /// Returns the Delta Exchange API string representation.
    pub fn as_delta_str(&self) -> &'static str {
        match self {
            OrderType::LimitOrder => "limit_order",
            OrderType::MarketOrder => "market_order",
            OrderType::StopMarketOrder => "stop_market_order",
            OrderType::StopLimitOrder => "stop_limit_order",
        }
    }
}

/// Time-in-force for order lifecycle.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeInForce {
    /// Good-Till-Cancelled (default)
    GTC,
    /// Immediate-Or-Cancel
    IOC,
    /// Fill-Or-Kill
    FOK,
}

impl TimeInForce {
    pub fn as_delta_str(&self) -> &'static str {
        match self {
            TimeInForce::GTC => "gtc",
            TimeInForce::IOC => "ioc",
            TimeInForce::FOK => "fok",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProfile {
    pub name: String,
    /// Unique user identifier. Named `fy_id` for backward compatibility with Fyers.
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

/// Per-asset wallet balance from Delta Exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletBalance {
    pub asset_symbol: String,
    pub asset_id: i64,
    pub balance: f64,
    pub available_balance: f64,
    pub order_margin: f64,
    pub position_margin: f64,
    pub commission: f64,
    pub unrealized_pnl: f64,
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
    /// Delta Exchange product_id (populated by Delta broker only)
    #[serde(default)]
    pub product_id: Option<i64>,
    /// Liquidation price (populated by Delta broker only)
    #[serde(default)]
    pub liquidation_price: Option<f64>,
    /// Leverage applied to this position
    #[serde(default)]
    pub leverage: Option<f64>,
    /// Margin allocated to this position
    #[serde(default)]
    pub margin: Option<f64>,
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
    /// Client-specified order ID for reconciliation (Delta: `client_id`)
    #[serde(default)]
    pub client_id: Option<String>,
    /// Time-in-force for the order (default: GTC)
    #[serde(default)]
    pub time_in_force: Option<TimeInForce>,
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
    /// Product ID required by Delta Exchange for edit/cancel
    #[serde(default)]
    pub product_id: Option<i64>,
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
    /// Initial margin requirement (e.g. "0.5" = 0.5%)
    #[serde(default)]
    pub initial_margin: Option<f64>,
    /// Maintenance margin requirement
    #[serde(default)]
    pub maintenance_margin: Option<f64>,
    /// Taker commission rate (e.g. 0.0005 = 0.05%)
    #[serde(default)]
    pub taker_commission_rate: Option<f64>,
    /// Maker commission rate (e.g. 0.0002 = 0.02%)
    #[serde(default)]
    pub maker_commission_rate: Option<f64>,
    /// Max position size in contracts
    #[serde(default)]
    pub position_size_limit: Option<i64>,
    /// Product trading status: "operational", "disrupted_cancel_only", "disrupted_post_only"
    #[serde(default)]
    pub trading_status: Option<String>,
    /// Product state: "live", "expired", "upcoming"
    #[serde(default)]
    pub state: Option<String>,
    /// Contract notional type: "vanilla" or "inverse"
    #[serde(default)]
    pub notional_type: Option<String>,
    /// Settling asset symbol (e.g. "USD", "USDT")
    #[serde(default)]
    pub settling_asset: Option<String>,
    /// Quoting asset symbol
    #[serde(default)]
    pub quoting_asset: Option<String>,
}

/// Margin mode for a futures position.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MarginMode {
    Isolated,
    Cross,
}

/// L2 Orderbook snapshot from Delta Exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2Orderbook {
    pub symbol: String,
    /// Bids as [[price, size], ...] sorted best-first (highest price first)
    pub bids: Vec<[f64; 2]>,
    /// Asks as [[price, size], ...] sorted best-first (lowest price first)
    pub asks: Vec<[f64; 2]>,
    pub timestamp: i64,
}

/// Bracket order request with take-profit and stop-loss attached.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BracketOrderRequest {
    pub product_id: i64,
    pub size: i32,
    pub side: Side,
    pub order_type: OrderType,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    /// Take-profit trigger price
    pub take_profit_price: f64,
    /// Stop-loss trigger price
    pub stop_loss_price: f64,
    /// Optional trailing stop amount
    pub trail_amount: Option<f64>,
}

/// Heartbeat configuration for the Deadman Switch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatConfig {
    /// Interval in seconds between expected heartbeat acknowledgments.
    pub interval_secs: u64,
    /// Action on heartbeat expiry: "cancel_all_orders"
    pub action: String,
}

/// Heartbeat status returned by the API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatStatus {
    pub id: String,
    pub interval_secs: u64,
    pub state: String, // "active", "expired"
    pub created_at: String,
    pub last_ack_at: Option<String>,
}

/// Pagination cursors for Delta Exchange API responses.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PaginationMeta {
    pub after: Option<String>,
    pub before: Option<String>,
}

/// Market Maker Protection (MMP) configuration for Delta Exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MmpConfig {
    pub window_ms: u64,
    pub frozen_time_ms: u64,
    pub qty_limit: f64,
    pub delta_limit: f64,
}

/// Asset metadata from Delta Exchange (/v2/assets).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetMeta {
    pub id: i64,
    pub symbol: String,
    pub precision: i32,
    pub deposit_status: String,
    pub withdrawal_status: String,
    pub base_withdrawal_fee: f64,
    pub min_withdrawal_amount: f64,
}

/// Spot index metadata from Delta Exchange (/v2/indices).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotIndexMeta {
    pub id: i64,
    pub symbol: String,
    pub underlying_asset_id: i64,
    pub quoting_asset_id: i64,
    pub tick_size: f64,
    pub index_type: String,
}

/// Option chain item from Delta Exchange (/v2/products/{symbol}/option_chain).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionChainItem {
    pub symbol: String,
    pub strike_price: f64,
    pub contract_type: String, // "call_options" or "put_options"
    pub expiry_date: String,
    pub product_id: i64,
    pub mark_price: f64,
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub vega: Option<f64>,
    pub theta: Option<f64>,
}

/// Wallet transaction log entry from Delta Exchange (/v2/wallet/transactions).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletTransaction {
    pub id: i64,
    pub asset_symbol: String,
    pub amount: f64,
    pub balance: f64,
    pub transaction_type: String, // "deposit", "withdrawal", "realized_pnl", "commission", etc.
    pub timestamp: i64,
    pub meta_data: Option<serde_json::Value>,
}

/// Sub-account details from Delta Exchange (/v2/sub_accounts).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubAccount {
    pub id: i64,
    pub email: String,
    pub name: String,
    pub user_type: String,
}

/// 24h Volume statistics from Delta Exchange (/v2/stats).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeStats {
    pub volume_24h_usd: f64,
    pub volume_24h_btc: f64,
    pub open_interest_usd: f64,
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

    #[test]
    fn test_order_type_delta_str() {
        assert_eq!(OrderType::LimitOrder.as_delta_str(), "limit_order");
        assert_eq!(OrderType::MarketOrder.as_delta_str(), "market_order");
        assert_eq!(OrderType::StopMarketOrder.as_delta_str(), "stop_market_order");
        assert_eq!(OrderType::StopLimitOrder.as_delta_str(), "stop_limit_order");
    }

    #[test]
    fn test_order_type_from_legacy() {
        assert_eq!(OrderType::from_legacy_int(1), OrderType::LimitOrder);
        assert_eq!(OrderType::from_legacy_int(2), OrderType::MarketOrder);
        assert_eq!(OrderType::from_legacy_int(3), OrderType::StopMarketOrder);
        assert_eq!(OrderType::from_legacy_int(4), OrderType::StopLimitOrder);
        assert_eq!(OrderType::from_legacy_int(99), OrderType::MarketOrder);
    }
}
