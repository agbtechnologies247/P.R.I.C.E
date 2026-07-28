pub async fn init_schema(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    // 1. Create candles table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS candles (
            timestamp TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            interval TEXT NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            volume BIGINT NOT NULL
        );"
    )
    .execute(pool)
    .await?;

    // Create unique index
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_candles_sym_int_time ON candles(symbol, interval, timestamp);"
    )
    .execute(pool)
    .await?;

    // Check if it is a hypertable
    let is_hyper: (bool,) = sqlx::query_as(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables WHERE table_name = 'hypertable' AND table_schema = '_timescaledb_internal'
        ) AND EXISTS (
            SELECT 1 FROM pg_tables WHERE tablename = 'candles'
        );"
    )
    .fetch_one(pool)
    .await
    .unwrap_or((false,));

    if is_hyper.0 {
        // Safe execution of create_hypertable
        let _ = sqlx::query("SELECT create_hypertable('candles', 'timestamp', if_not_exists => TRUE);")
            .execute(pool)
            .await;

        // 1. Configure hypertable compression
        let _ = sqlx::query(
            "ALTER TABLE candles SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'symbol, interval'
            );"
        )
        .execute(pool)
        .await;

        let _ = sqlx::query("SELECT add_compression_policy('candles', INTERVAL '7 days', if_not_exists => TRUE);")
            .execute(pool)
            .await;

        // 2. Create 5-minute Continuous Aggregate view
        let _ = sqlx::query(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS candles_5m
             WITH (timescaledb.continuous) AS
             SELECT
                 time_bucket('5 minutes', timestamp) AS timestamp,
                 symbol,
                 exchange,
                 '5m'::TEXT AS interval,
                 first(open, timestamp) AS open,
                 max(high) AS high,
                 min(low) AS low,
                 last(close, timestamp) AS close,
                 sum(volume) AS volume
             FROM candles
             GROUP BY timestamp, symbol, exchange;"
        )
        .execute(pool)
        .await;

        // Add refresh policy for 5m continuous aggregate
        let _ = sqlx::query(
            "SELECT add_continuous_aggregate_policy('candles_5m',
                start_offset => INTERVAL '1 hour',
                end_offset => INTERVAL '1 minute',
                schedule_interval => INTERVAL '5 minutes',
                if_not_exists => TRUE);"
        )
        .execute(pool)
        .await;
    }

    // 2. Create download_jobs table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS download_jobs (
            symbol TEXT NOT NULL,
            from_date DATE NOT NULL,
            to_date DATE NOT NULL,
            status TEXT NOT NULL,
            last_updated TIMESTAMPTZ DEFAULT NOW(),
            retry_count INT DEFAULT 0,
            PRIMARY KEY (symbol, from_date, to_date)
        );"
    )
    .execute(pool)
    .await?;

    // 3. Create market_sessions table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS market_sessions (
            date DATE PRIMARY KEY,
            market TEXT NOT NULL,
            open_time TIME NOT NULL,
            close_time TIME NOT NULL,
            holiday BOOLEAN DEFAULT FALSE
        );"
    )
    .execute(pool)
    .await?;

    // 4. Create execution_context_logs table for Market Memory (Gap 10)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS execution_context_logs (
            timestamp TIMESTAMPTZ NOT NULL,
            trade_id TEXT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            price DOUBLE PRECISION NOT NULL,
            qty INT NOT NULL,
            regime TEXT NOT NULL,
            ml_confidence DOUBLE PRECISION NOT NULL,
            portfolio_delta DOUBLE PRECISION NOT NULL,
            portfolio_gamma DOUBLE PRECISION NOT NULL,
            portfolio_exposure DOUBLE PRECISION NOT NULL,
            leverage_usage DOUBLE PRECISION NOT NULL,
            margin_utilization DOUBLE PRECISION NOT NULL,
            vix DOUBLE PRECISION NOT NULL,
            atr DOUBLE PRECISION NOT NULL,
            vwap DOUBLE PRECISION NOT NULL,
            open_interest BIGINT,
            volume BIGINT,
            outcome_pnl DOUBLE PRECISION
        );"
    )
    .execute(pool)
    .await?;

    // 5. Create symbol_mappings table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS symbol_mappings (
            canonical_symbol TEXT PRIMARY KEY,
            broker_name TEXT NOT NULL,
            broker_symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            tick_size DOUBLE PRECISION NOT NULL DEFAULT 0.05,
            lot_size DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            max_leverage INT NOT NULL DEFAULT 1
        );"
    )
    .execute(pool)
    .await?;

    // Seed default symbol mappings if empty
    let seed_data = vec![
        ("BTCUSD_PERP", "DELTA", "BTCUSD", "DELTA", "CRYPTO_PERP", 0.5, 1.0, 200),
        ("ETHUSD_PERP", "DELTA", "ETHUSD", "DELTA", "CRYPTO_PERP", 0.05, 1.0, 200),
        ("SOLUSD_PERP", "DELTA", "SOLUSD", "DELTA", "CRYPTO_PERP", 0.01, 1.0, 100),
        ("NSE:NIFTY50-INDEX", "FYERS", "NSE:NIFTY50-INDEX", "NSE", "INDEX", 0.05, 50.0, 1),
        ("NSE:NIFTYBANK-INDEX", "FYERS", "NSE:NIFTYBANK-INDEX", "NSE", "INDEX", 0.05, 15.0, 1),
        ("NSE:RELIANCE-EQ", "FYERS", "NSE:RELIANCE-EQ", "NSE", "EQUITY", 0.05, 1.0, 1),
    ];

    for (canon, b_name, b_sym, exch, asset, tick, lot, lev) in seed_data {
        let _ = sqlx::query(
            "INSERT INTO symbol_mappings (canonical_symbol, broker_name, broker_symbol, exchange, asset_class, tick_size, lot_size, max_leverage)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (canonical_symbol) DO NOTHING"
        )
        .bind(canon)
        .bind(b_name)
        .bind(b_sym)
        .bind(exch)
        .bind(asset)
        .bind(tick)
        .bind(lot)
        .bind(lev)
        .execute(pool)
        .await;
    }

    Ok(())
}

