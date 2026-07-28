use sqlx::PgPool;
use anyhow::Result;

/// Creates all journal hypertables in TimescaleDB at startup.
pub async fn init_journal_schema(pool: &PgPool) -> Result<()> {
    // Trade Journal
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS trade_journal (
            id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            session_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price DOUBLE PRECISION,
            exit_price DOUBLE PRECISION,
            qty INTEGER,
            pnl DOUBLE PRECISION,
            slippage DOUBLE PRECISION,
            fill_latency_ms BIGINT,
            exit_reason TEXT,
            broker TEXT,
            leverage INTEGER,
            PRIMARY KEY (id, timestamp)
        )"
    ).execute(pool).await?;
    let _ = sqlx::query(
        "SELECT create_hypertable('trade_journal', 'timestamp', if_not_exists => TRUE)"
    ).execute(pool).await;

    // Decision Journal
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS decision_journal (
            id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            session_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            signal_score DOUBLE PRECISION,
            threshold DOUBLE PRECISION,
            decision TEXT,
            rejection_reason TEXT,
            regime TEXT,
            ml_confidence DOUBLE PRECISION,
            vix DOUBLE PRECISION,
            atr DOUBLE PRECISION,
            slope DOUBLE PRECISION,
            compression DOUBLE PRECISION,
            fib_confluence DOUBLE PRECISION,
            sr_proximity DOUBLE PRECISION,
            oi_increasing BOOLEAN,
            volume_spike BOOLEAN,
            PRIMARY KEY (id, timestamp)
        )"
    ).execute(pool).await?;
    let _ = sqlx::query(
        "SELECT create_hypertable('decision_journal', 'timestamp', if_not_exists => TRUE)"
    ).execute(pool).await;

    // Market Journal
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS market_journal (
            id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            session_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            price DOUBLE PRECISION,
            vwap DOUBLE PRECISION,
            atr DOUBLE PRECISION,
            vix DOUBLE PRECISION,
            volume BIGINT,
            oi BIGINT,
            regime TEXT,
            cvd DOUBLE PRECISION,
            oi_delta BIGINT,
            divergence_detected BOOLEAN,
            PRIMARY KEY (id, timestamp)
        )"
    ).execute(pool).await?;
    let _ = sqlx::query(
        "SELECT create_hypertable('market_journal', 'timestamp', if_not_exists => TRUE)"
    ).execute(pool).await;

    // Execution Journal
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS execution_journal (
            id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            session_id TEXT NOT NULL,
            order_id TEXT,
            symbol TEXT NOT NULL,
            side TEXT,
            qty INTEGER,
            order_type TEXT,
            requested_price DOUBLE PRECISION,
            filled_price DOUBLE PRECISION,
            status TEXT,
            broker TEXT,
            leverage INTEGER,
            latency_ms BIGINT,
            PRIMARY KEY (id, timestamp)
        )"
    ).execute(pool).await?;
    let _ = sqlx::query(
        "SELECT create_hypertable('execution_journal', 'timestamp', if_not_exists => TRUE)"
    ).execute(pool).await;

    // Risk Journal
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS risk_journal (
            id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            session_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            event TEXT,
            leverage_used DOUBLE PRECISION,
            leverage_limit DOUBLE PRECISION,
            concentration DOUBLE PRECISION,
            concentration_limit DOUBLE PRECISION,
            margin_utilization DOUBLE PRECISION,
            portfolio_exposure DOUBLE PRECISION,
            portfolio_delta DOUBLE PRECISION,
            available_balance DOUBLE PRECISION,
            check_passed BOOLEAN,
            rejection_reason TEXT,
            PRIMARY KEY (id, timestamp)
        )"
    ).execute(pool).await?;
    let _ = sqlx::query(
        "SELECT create_hypertable('risk_journal', 'timestamp', if_not_exists => TRUE)"
    ).execute(pool).await;

    // ML Journal
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ml_journal (
            id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            session_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            price DOUBLE PRECISION,
            vwap DOUBLE PRECISION,
            vix DOUBLE PRECISION,
            slope DOUBLE PRECISION,
            expansion DOUBLE PRECISION,
            compression DOUBLE PRECISION,
            curvature DOUBLE PRECISION,
            fib_confluence DOUBLE PRECISION,
            sr_proximity DOUBLE PRECISION,
            oi_increasing BOOLEAN,
            volume_spike BOOLEAN,
            prediction_score DOUBLE PRECISION,
            threshold DOUBLE PRECISION,
            passed BOOLEAN,
            PRIMARY KEY (id, timestamp)
        )"
    ).execute(pool).await?;
    let _ = sqlx::query(
        "SELECT create_hypertable('ml_journal', 'timestamp', if_not_exists => TRUE)"
    ).execute(pool).await;

    // Portfolio Journal
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS portfolio_journal (
            id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            session_id TEXT NOT NULL,
            total_exposure DOUBLE PRECISION,
            leverage_usage DOUBLE PRECISION,
            margin_utilization DOUBLE PRECISION,
            portfolio_delta DOUBLE PRECISION,
            portfolio_gamma DOUBLE PRECISION,
            open_positions INTEGER,
            unrealized_pnl DOUBLE PRECISION,
            available_balance DOUBLE PRECISION,
            max_drawdown_pct DOUBLE PRECISION,
            PRIMARY KEY (id, timestamp)
        )"
    ).execute(pool).await?;
    let _ = sqlx::query(
        "SELECT create_hypertable('portfolio_journal', 'timestamp', if_not_exists => TRUE)"
    ).execute(pool).await;

    tracing::info!("All 7 journal hypertables initialized successfully.");
    Ok(())
}
