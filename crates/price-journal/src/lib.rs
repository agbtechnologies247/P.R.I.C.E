use sqlx::PgPool;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{info, warn};
use uuid::Uuid;
use chrono::Utc;

pub mod entries;
pub mod schema;

pub use entries::*;

/// Commands that can be sent to a journal writer task.
#[derive(Debug)]
enum JournalCmd {
    Trade(TradeJournalEntry),
    Decision(DecisionJournalEntry),
    Market(MarketJournalEntry),
    Execution(ExecutionJournalEntry),
    Risk(RiskJournalEntry),
    Ml(MlJournalEntry),
    Portfolio(PortfolioJournalEntry),
    Shutdown,
}

/// The central journal manager.
#[derive(Clone)]
pub struct JournalManager {
    tx: mpsc::Sender<JournalCmd>,
    pub session_id: String,
}

impl JournalManager {
    pub fn new(pool: PgPool, buffer_size: usize, flush_interval_secs: u64) -> Self {
        let (tx, mut rx) = mpsc::channel::<JournalCmd>(buffer_size);
        let session_id = Uuid::new_v4().to_string();

        tokio::spawn(async move {
            let mut trade_buf: Vec<TradeJournalEntry> = Vec::new();
            let mut decision_buf: Vec<DecisionJournalEntry> = Vec::new();
            let mut market_buf: Vec<MarketJournalEntry> = Vec::new();
            let mut execution_buf: Vec<ExecutionJournalEntry> = Vec::new();
            let mut risk_buf: Vec<RiskJournalEntry> = Vec::new();
            let mut ml_buf: Vec<MlJournalEntry> = Vec::new();
            let mut portfolio_buf: Vec<PortfolioJournalEntry> = Vec::new();

            let mut ticker = interval(Duration::from_secs(flush_interval_secs));

            loop {
                tokio::select! {
                    Some(cmd) = rx.recv() => {
                        match cmd {
                            JournalCmd::Trade(e) => trade_buf.push(e),
                            JournalCmd::Decision(e) => decision_buf.push(e),
                            JournalCmd::Market(e) => market_buf.push(e),
                            JournalCmd::Execution(e) => execution_buf.push(e),
                            JournalCmd::Risk(e) => risk_buf.push(e),
                            JournalCmd::Ml(e) => ml_buf.push(e),
                            JournalCmd::Portfolio(e) => portfolio_buf.push(e),
                            JournalCmd::Shutdown => {
                                // Final flush on shutdown
                                flush_all(&pool, &mut trade_buf, &mut decision_buf, &mut market_buf,
                                    &mut execution_buf, &mut risk_buf, &mut ml_buf, &mut portfolio_buf).await;
                                info!("[Journal] Shutdown flush complete.");
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        flush_all(&pool, &mut trade_buf, &mut decision_buf, &mut market_buf,
                            &mut execution_buf, &mut risk_buf, &mut ml_buf, &mut portfolio_buf).await;
                    }
                }
            }
        });

        Self { tx, session_id }
    }

    pub fn log_trade(&self, mut entry: TradeJournalEntry) {
        if entry.id.is_empty() { entry.id = Uuid::new_v4().to_string(); }
        if entry.timestamp == chrono::DateTime::<Utc>::default() { entry.timestamp = Utc::now(); }
        let _ = self.tx.try_send(JournalCmd::Trade(entry));
    }

    pub fn log_decision(&self, mut entry: DecisionJournalEntry) {
        if entry.id.is_empty() { entry.id = Uuid::new_v4().to_string(); }
        if entry.timestamp == chrono::DateTime::<Utc>::default() { entry.timestamp = Utc::now(); }
        let _ = self.tx.try_send(JournalCmd::Decision(entry));
    }

    pub fn log_market(&self, mut entry: MarketJournalEntry) {
        if entry.id.is_empty() { entry.id = Uuid::new_v4().to_string(); }
        if entry.timestamp == chrono::DateTime::<Utc>::default() { entry.timestamp = Utc::now(); }
        let _ = self.tx.try_send(JournalCmd::Market(entry));
    }

    pub fn log_execution(&self, mut entry: ExecutionJournalEntry) {
        if entry.id.is_empty() { entry.id = Uuid::new_v4().to_string(); }
        if entry.timestamp == chrono::DateTime::<Utc>::default() { entry.timestamp = Utc::now(); }
        let _ = self.tx.try_send(JournalCmd::Execution(entry));
    }

    pub fn log_risk(&self, mut entry: RiskJournalEntry) {
        if entry.id.is_empty() { entry.id = Uuid::new_v4().to_string(); }
        if entry.timestamp == chrono::DateTime::<Utc>::default() { entry.timestamp = Utc::now(); }
        let _ = self.tx.try_send(JournalCmd::Risk(entry));
    }

    pub fn log_ml(&self, mut entry: MlJournalEntry) {
        if entry.id.is_empty() { entry.id = Uuid::new_v4().to_string(); }
        if entry.timestamp == chrono::DateTime::<Utc>::default() { entry.timestamp = Utc::now(); }
        let _ = self.tx.try_send(JournalCmd::Ml(entry));
    }

    pub fn log_portfolio(&self, mut entry: PortfolioJournalEntry) {
        if entry.id.is_empty() { entry.id = Uuid::new_v4().to_string(); }
        if entry.timestamp == chrono::DateTime::<Utc>::default() { entry.timestamp = Utc::now(); }
        let _ = self.tx.try_send(JournalCmd::Portfolio(entry));
    }

    pub async fn shutdown(self) {
        let _ = self.tx.send(JournalCmd::Shutdown).await;
    }
}

async fn flush_all(
    pool: &PgPool,
    trades: &mut Vec<TradeJournalEntry>,
    decisions: &mut Vec<DecisionJournalEntry>,
    markets: &mut Vec<MarketJournalEntry>,
    executions: &mut Vec<ExecutionJournalEntry>,
    risks: &mut Vec<RiskJournalEntry>,
    mls: &mut Vec<MlJournalEntry>,
    portfolios: &mut Vec<PortfolioJournalEntry>,
) {
    flush_trades(pool, trades).await;
    flush_decisions(pool, decisions).await;
    flush_markets(pool, markets).await;
    flush_executions(pool, executions).await;
    flush_risks(pool, risks).await;
    flush_mls(pool, mls).await;
    flush_portfolios(pool, portfolios).await;
}

async fn flush_trades(pool: &PgPool, buf: &mut Vec<TradeJournalEntry>) {
    if buf.is_empty() { return; }
    for e in buf.iter() {
        let _ = sqlx::query(
            "INSERT INTO trade_journal (id, timestamp, session_id, symbol, side, entry_price, exit_price,
             qty, pnl, slippage, fill_latency_ms, exit_reason, broker, leverage)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT DO NOTHING"
        )
        .bind(&e.id).bind(e.timestamp).bind(&e.session_id).bind(&e.symbol).bind(&e.side)
        .bind(e.entry_price).bind(e.exit_price).bind(e.qty).bind(e.pnl).bind(e.slippage)
        .bind(e.fill_latency_ms).bind(&e.exit_reason).bind(&e.broker).bind(e.leverage as i32)
        .execute(pool).await
        .map_err(|err| warn!("[Journal] Trade flush error: {}", err));
    }
    buf.clear();
}

async fn flush_decisions(pool: &PgPool, buf: &mut Vec<DecisionJournalEntry>) {
    if buf.is_empty() { return; }
    for e in buf.iter() {
        let _ = sqlx::query(
            "INSERT INTO decision_journal (id, timestamp, session_id, symbol, signal_score, threshold,
             decision, rejection_reason, regime, ml_confidence, vix, atr, slope, compression,
             fib_confluence, sr_proximity, oi_increasing, volume_spike)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) ON CONFLICT DO NOTHING"
        )
        .bind(&e.id).bind(e.timestamp).bind(&e.session_id).bind(&e.symbol)
        .bind(e.signal_score).bind(e.threshold).bind(&e.decision).bind(&e.rejection_reason)
        .bind(&e.regime).bind(e.ml_confidence).bind(e.vix).bind(e.atr).bind(e.slope)
        .bind(e.compression).bind(e.fib_confluence).bind(e.sr_proximity)
        .bind(e.oi_increasing).bind(e.volume_spike)
        .execute(pool).await
        .map_err(|err| warn!("[Journal] Decision flush error: {}", err));
    }
    buf.clear();
}

async fn flush_markets(pool: &PgPool, buf: &mut Vec<MarketJournalEntry>) {
    if buf.is_empty() { return; }
    for e in buf.iter() {
        let _ = sqlx::query(
            "INSERT INTO market_journal (id, timestamp, session_id, symbol, price, vwap, atr, vix,
             volume, oi, regime, cvd, oi_delta, divergence_detected)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT DO NOTHING"
        )
        .bind(&e.id).bind(e.timestamp).bind(&e.session_id).bind(&e.symbol)
        .bind(e.price).bind(e.vwap).bind(e.atr).bind(e.vix)
        .bind(e.volume as i64).bind(e.oi as i64).bind(&e.regime)
        .bind(e.cvd).bind(e.oi_delta).bind(e.divergence_detected)
        .execute(pool).await
        .map_err(|err| warn!("[Journal] Market flush error: {}", err));
    }
    buf.clear();
}

async fn flush_executions(pool: &PgPool, buf: &mut Vec<ExecutionJournalEntry>) {
    if buf.is_empty() { return; }
    for e in buf.iter() {
        let _ = sqlx::query(
            "INSERT INTO execution_journal (id, timestamp, session_id, order_id, symbol, side, qty,
             order_type, requested_price, filled_price, status, broker, leverage, latency_ms)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT DO NOTHING"
        )
        .bind(&e.id).bind(e.timestamp).bind(&e.session_id).bind(&e.order_id).bind(&e.symbol)
        .bind(&e.side).bind(e.qty).bind(&e.order_type).bind(e.requested_price)
        .bind(e.filled_price).bind(&e.status).bind(&e.broker).bind(e.leverage as i32).bind(e.latency_ms)
        .execute(pool).await
        .map_err(|err| warn!("[Journal] Execution flush error: {}", err));
    }
    buf.clear();
}

async fn flush_risks(pool: &PgPool, buf: &mut Vec<RiskJournalEntry>) {
    if buf.is_empty() { return; }
    for e in buf.iter() {
        let _ = sqlx::query(
            "INSERT INTO risk_journal (id, timestamp, session_id, symbol, event, leverage_used,
             leverage_limit, concentration, concentration_limit, margin_utilization,
             portfolio_exposure, portfolio_delta, available_balance, check_passed, rejection_reason)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT DO NOTHING"
        )
        .bind(&e.id).bind(e.timestamp).bind(&e.session_id).bind(&e.symbol).bind(&e.event)
        .bind(e.leverage_used).bind(e.leverage_limit).bind(e.concentration)
        .bind(e.concentration_limit).bind(e.margin_utilization).bind(e.portfolio_exposure)
        .bind(e.portfolio_delta).bind(e.available_balance).bind(e.check_passed).bind(&e.rejection_reason)
        .execute(pool).await
        .map_err(|err| warn!("[Journal] Risk flush error: {}", err));
    }
    buf.clear();
}

async fn flush_mls(pool: &PgPool, buf: &mut Vec<MlJournalEntry>) {
    if buf.is_empty() { return; }
    for e in buf.iter() {
        let _ = sqlx::query(
            "INSERT INTO ml_journal (id, timestamp, session_id, symbol, price, vwap, vix, slope,
             expansion, compression, curvature, fib_confluence, sr_proximity, oi_increasing,
             volume_spike, prediction_score, threshold, passed)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) ON CONFLICT DO NOTHING"
        )
        .bind(&e.id).bind(e.timestamp).bind(&e.session_id).bind(&e.symbol)
        .bind(e.price).bind(e.vwap).bind(e.vix).bind(e.slope).bind(e.expansion)
        .bind(e.compression).bind(e.curvature).bind(e.fib_confluence).bind(e.sr_proximity)
        .bind(e.oi_increasing).bind(e.volume_spike).bind(e.prediction_score).bind(e.threshold).bind(e.passed)
        .execute(pool).await
        .map_err(|err| warn!("[Journal] ML flush error: {}", err));
    }
    buf.clear();
}

async fn flush_portfolios(pool: &PgPool, buf: &mut Vec<PortfolioJournalEntry>) {
    if buf.is_empty() { return; }
    for e in buf.iter() {
        let _ = sqlx::query(
            "INSERT INTO portfolio_journal (id, timestamp, session_id, total_exposure, leverage_usage,
             margin_utilization, portfolio_delta, portfolio_gamma, open_positions, unrealized_pnl,
             available_balance, max_drawdown_pct)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING"
        )
        .bind(&e.id).bind(e.timestamp).bind(&e.session_id).bind(e.total_exposure)
        .bind(e.leverage_usage).bind(e.margin_utilization).bind(e.portfolio_delta)
        .bind(e.portfolio_gamma).bind(e.open_positions).bind(e.unrealized_pnl)
        .bind(e.available_balance).bind(e.max_drawdown_pct)
        .execute(pool).await
        .map_err(|err| warn!("[Journal] Portfolio flush error: {}", err));
    }
    buf.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_journal_entry_serialization() {
        let entry = TradeJournalEntry {
            id: "test-id".to_string(),
            timestamp: Utc::now(),
            session_id: "sess-001".to_string(),
            symbol: "BTCUSD_PERP".to_string(),
            side: "buy".to_string(),
            entry_price: 65000.0,
            exit_price: 66500.0,
            qty: 1,
            pnl: 1500.0,
            slippage: 5.0,
            fill_latency_ms: 45,
            exit_reason: "target".to_string(),
            broker: "DeltaExchange".to_string(),
            leverage: 200,
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("BTCUSD_PERP"));
        assert!(json.contains("200"));
    }

    #[test]
    fn test_decision_journal_entry_serialization() {
        let entry = DecisionJournalEntry {
            id: "dec-001".to_string(),
            timestamp: Utc::now(),
            session_id: "sess-001".to_string(),
            symbol: "ETHUSD_PERP".to_string(),
            signal_score: 78.5,
            threshold: 60.0,
            decision: "entry".to_string(),
            rejection_reason: None,
            regime: "BullishTrending".to_string(),
            ml_confidence: 82.0,
            vix: 14.5,
            atr: 350.0,
            slope: 0.8,
            compression: 0.1,
            fib_confluence: 0.75,
            sr_proximity: 0.9,
            oi_increasing: true,
            volume_spike: true,
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("BullishTrending"));
        assert!(json.contains("entry"));
    }
}
