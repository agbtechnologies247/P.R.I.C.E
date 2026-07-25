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
