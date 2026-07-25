use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickData {
    pub symbol: String,
    pub price: f64,
    pub volume: u64,
    pub oi: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngineEvent {
    TickReceived(TickData),
    CandleClosed(Candle),
    IndicatorsUpdated {
        timestamp: DateTime<Utc>,
        vwap: f64,
        atr: f64,
        adx: f64,
        spread: f64,
    },
    PatternDetected {
        pattern_name: String,
        confidence: f64,
    },
    ConfidenceUpdated {
        score: f64,
    },
    TradeCandidate {
        symbol: String,
        side: i8, // 1 for Buy, -1 for Sell
        confidence: f64,
        price: f64,
    },
    RiskApproved {
        order_id: String,
        allocated_capital: f64,
    },
    OrderPlaced {
        order_id: String,
        symbol: String,
        qty: i32,
        price: f64,
    },
    OrderFilled {
        order_id: String,
        fill_price: f64,
        qty: i32,
    },
    PositionOpened {
        symbol: String,
        qty: i32,
        avg_price: f64,
    },
    PositionClosed {
        symbol: String,
        qty: i32,
        exit_price: f64,
        pnl: f64,
    },
    TradeRecorded {
        trade_id: String,
        pnl: f64,
        reason: String,
    },
    MLUpdated {
        samples_count: usize,
    },
}
