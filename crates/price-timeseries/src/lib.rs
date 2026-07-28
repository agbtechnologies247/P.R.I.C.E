use sqlx::{PgPool, Row};
use chrono::{DateTime, Utc, NaiveDate};
use price_core::Candle;

pub mod schema;

#[derive(Clone)]
pub struct TimescaleClient {
    pub pool: PgPool,
}

impl TimescaleClient {
    pub async fn new(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self { pool })
    }

    pub async fn init_db(&self) -> anyhow::Result<()> {
        schema::init_schema(&self.pool).await
    }

    pub async fn insert_candles(
        &self,
        symbol: &str,
        exchange: &str,
        interval: &str,
        candles: &[Candle],
    ) -> anyhow::Result<()> {
        if candles.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        for c in candles {
            let _ = sqlx::query(
                "INSERT INTO candles (timestamp, symbol, exchange, interval, open, high, low, close, volume)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (symbol, interval, timestamp) DO UPDATE
                 SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume"
            )
            .bind(c.timestamp)
            .bind(symbol)
            .bind(exchange)
            .bind(interval)
            .bind(c.open)
            .bind(c.high)
            .bind(c.low)
            .bind(c.close)
            .bind(c.volume as i64)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_candles(
        &self,
        symbol: &str,
        interval: &str,
        from_date: DateTime<Utc>,
        to_date: DateTime<Utc>,
    ) -> anyhow::Result<Vec<Candle>> {
        let rows = sqlx::query(
            "SELECT timestamp, open, high, low, close, volume
             FROM candles
             WHERE symbol = $1 AND interval = $2 AND timestamp >= $3 AND timestamp <= $4
             ORDER BY timestamp ASC"
        )
        .bind(symbol)
        .bind(interval)
        .bind(from_date)
        .bind(to_date)
        .fetch_all(&self.pool)
        .await?;

        let candles = rows
            .into_iter()
            .map(|r| Candle {
                timestamp: r.get::<DateTime<Utc>, _>("timestamp"),
                open: r.get::<f64, _>("open"),
                high: r.get::<f64, _>("high"),
                low: r.get::<f64, _>("low"),
                close: r.get::<f64, _>("close"),
                volume: r.get::<i64, _>("volume") as u64,
            })
            .collect();

        Ok(candles)
    }

    pub async fn mark_job_status(
        &self,
        symbol: &str,
        from_date: NaiveDate,
        to_date: NaiveDate,
        status: &str,
    ) -> anyhow::Result<()> {
        let _ = sqlx::query(
            "INSERT INTO download_jobs (symbol, from_date, to_date, status, last_updated, retry_count)
             VALUES ($1, $2, $3, $4, NOW(), 0)
             ON CONFLICT (symbol, from_date, to_date) DO UPDATE
             SET status = EXCLUDED.status, last_updated = NOW(), retry_count = download_jobs.retry_count + 1"
        )
        .bind(symbol)
        .bind(from_date)
        .bind(to_date)
        .bind(status)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_job_status(
        &self,
        symbol: &str,
        from_date: NaiveDate,
        to_date: NaiveDate,
    ) -> anyhow::Result<Option<String>> {
        let row = sqlx::query(
            "SELECT status FROM download_jobs WHERE symbol = $1 AND from_date = $2 AND to_date = $3"
        )
        .bind(symbol)
        .bind(from_date)
        .bind(to_date)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.get::<String, _>("status")))
    }

    pub async fn get_holidays(&self) -> anyhow::Result<Vec<NaiveDate>> {
        let rows = sqlx::query(
            "SELECT date FROM market_sessions WHERE holiday = TRUE"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.get::<NaiveDate, _>("date")).collect())
    }

    pub async fn get_last_candle_timestamp(
        &self,
        symbol: &str,
    ) -> anyhow::Result<Option<DateTime<Utc>>> {
        let row = sqlx::query(
            "SELECT max(timestamp) as max_ts FROM candles WHERE symbol = $1"
        )
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => {
                let max_ts: Option<DateTime<Utc>> = r.get("max_ts");
                Ok(max_ts)
            }
            None => Ok(None)
        }
    }

    pub async fn get_db_stats(&self) -> anyhow::Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            "SELECT symbol, count(*) as count FROM candles GROUP BY symbol ORDER BY count DESC"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| (r.get::<String, _>("symbol"), r.get::<i64, _>("count")))
            .collect())
    }

    pub async fn delete_candles(&self, symbols: &[&str]) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM candles WHERE symbol = ANY($1)")
            .bind(symbols)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn insert_context_log(
        &self,
        log: &ExecutionContextLog,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO execution_context_logs (
                timestamp, trade_id, symbol, side, price, qty, regime, ml_confidence,
                portfolio_delta, portfolio_gamma, portfolio_exposure, leverage_usage,
                margin_utilization, vix, atr, vwap, open_interest, volume, outcome_pnl
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)"
        )
        .bind(log.timestamp)
        .bind(&log.trade_id)
        .bind(&log.symbol)
        .bind(&log.side)
        .bind(log.price)
        .bind(log.qty)
        .bind(&log.regime)
        .bind(log.ml_confidence)
        .bind(log.portfolio_delta)
        .bind(log.portfolio_gamma)
        .bind(log.portfolio_exposure)
        .bind(log.leverage_usage)
        .bind(log.margin_utilization)
        .bind(log.vix)
        .bind(log.atr)
        .bind(log.vwap)
        .bind(log.open_interest)
        .bind(log.volume)
        .bind(log.outcome_pnl)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_symbol_mappings(&self) -> anyhow::Result<Vec<SymbolMapping>> {
        let rows = sqlx::query(
            "SELECT canonical_symbol, broker_name, broker_symbol, exchange, asset_class, tick_size, lot_size, max_leverage
             FROM symbol_mappings"
        )
        .fetch_all(&self.pool)
        .await?;

        let mappings = rows.into_iter().map(|r| SymbolMapping {
            canonical_symbol: r.get("canonical_symbol"),
            broker_name: r.get("broker_name"),
            broker_symbol: r.get("broker_symbol"),
            exchange: r.get("exchange"),
            asset_class: r.get("asset_class"),
            tick_size: r.get("tick_size"),
            lot_size: r.get("lot_size"),
            max_leverage: r.get("max_leverage"),
        }).collect();

        Ok(mappings)
    }

    pub async fn get_symbol_mapping(&self, symbol: &str) -> anyhow::Result<Option<SymbolMapping>> {
        let row = sqlx::query(
            "SELECT canonical_symbol, broker_name, broker_symbol, exchange, asset_class, tick_size, lot_size, max_leverage
             FROM symbol_mappings
             WHERE canonical_symbol = $1 OR broker_symbol = $1"
        )
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| SymbolMapping {
            canonical_symbol: r.get("canonical_symbol"),
            broker_name: r.get("broker_name"),
            broker_symbol: r.get("broker_symbol"),
            exchange: r.get("exchange"),
            asset_class: r.get("asset_class"),
            tick_size: r.get("tick_size"),
            lot_size: r.get("lot_size"),
            max_leverage: r.get("max_leverage"),
        }))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SymbolMapping {
    pub canonical_symbol: String,
    pub broker_name: String,
    pub broker_symbol: String,
    pub exchange: String,
    pub asset_class: String,
    pub tick_size: f64,
    pub lot_size: f64,
    pub max_leverage: i32,
}

#[derive(Debug, Clone)]
pub struct ExecutionContextLog {
    pub timestamp: DateTime<Utc>,
    pub trade_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub qty: i32,
    pub regime: String,
    pub ml_confidence: f64,
    pub portfolio_delta: f64,
    pub portfolio_gamma: f64,
    pub portfolio_exposure: f64,
    pub leverage_usage: f64,
    pub margin_utilization: f64,
    pub vix: f64,
    pub atr: f64,
    pub vwap: f64,
    pub open_interest: Option<i64>,
    pub volume: Option<i64>,
    pub outcome_pnl: Option<f64>,
}

