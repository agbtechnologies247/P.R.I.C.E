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

    Ok(())
}
