use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// Journal Entry Types
// ─────────────────────────────────────────────────────────────────────────────

/// A completed trade (entry + exit lifecycle).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeJournalEntry {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub session_id: String,
    pub symbol: String,
    pub side: String,          // "buy" | "sell"
    pub entry_price: f64,
    pub exit_price: f64,
    pub qty: i32,
    pub pnl: f64,
    pub slippage: f64,         // entry_price - requested_price
    pub fill_latency_ms: i64,  // milliseconds from signal to fill
    pub exit_reason: String,   // "target", "stop", "timeout", "manual"
    pub broker: String,
    pub leverage: u32,
}

/// Every strategy signal evaluation (entry or rejection).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionJournalEntry {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub session_id: String,
    pub symbol: String,
    pub signal_score: f64,
    pub threshold: f64,
    pub decision: String,      // "entry", "rejected", "no_signal"
    pub rejection_reason: Option<String>,
    pub regime: String,
    pub ml_confidence: f64,
    pub vix: f64,
    pub atr: f64,
    pub slope: f64,
    pub compression: f64,
    pub fib_confluence: f64,
    pub sr_proximity: f64,
    pub oi_increasing: bool,
    pub volume_spike: bool,
}

/// Market state snapshot at each evaluation tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketJournalEntry {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub session_id: String,
    pub symbol: String,
    pub price: f64,
    pub vwap: f64,
    pub atr: f64,
    pub vix: f64,
    pub volume: u64,
    pub oi: u64,
    pub regime: String,
    pub cvd: f64,
    pub oi_delta: i64,
    pub divergence_detected: bool,
}

/// Order lifecycle events (submitted, routed, filled, cancelled, rejected).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionJournalEntry {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub session_id: String,
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub qty: i32,
    pub order_type: String,    // "market", "limit", "stop_market"
    pub requested_price: f64,
    pub filled_price: f64,
    pub status: String,        // "submitted", "filled", "cancelled", "rejected"
    pub broker: String,
    pub leverage: u32,
    pub latency_ms: i64,
}

/// Risk evaluation snapshot at trade entry/exit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskJournalEntry {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub session_id: String,
    pub symbol: String,
    pub event: String,         // "pre_entry_check", "post_exit_check"
    pub leverage_used: f64,
    pub leverage_limit: f64,
    pub concentration: f64,
    pub concentration_limit: f64,
    pub margin_utilization: f64,
    pub portfolio_exposure: f64,
    pub portfolio_delta: f64,
    pub available_balance: f64,
    pub check_passed: bool,
    pub rejection_reason: Option<String>,
}

/// ML model prediction log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MlJournalEntry {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub session_id: String,
    pub symbol: String,
    pub price: f64,
    pub vwap: f64,
    pub vix: f64,
    pub slope: f64,
    pub expansion: f64,
    pub compression: f64,
    pub curvature: f64,
    pub fib_confluence: f64,
    pub sr_proximity: f64,
    pub oi_increasing: bool,
    pub volume_spike: bool,
    pub prediction_score: f64,
    pub threshold: f64,
    pub passed: bool,
}

/// Portfolio-level state (exposure, Greeks, drawdown) logged periodically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioJournalEntry {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub session_id: String,
    pub total_exposure: f64,
    pub leverage_usage: f64,
    pub margin_utilization: f64,
    pub portfolio_delta: f64,
    pub portfolio_gamma: f64,
    pub open_positions: i32,
    pub unrealized_pnl: f64,
    pub available_balance: f64,
    pub max_drawdown_pct: f64,
}
