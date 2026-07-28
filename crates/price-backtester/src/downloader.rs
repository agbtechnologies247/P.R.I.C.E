use chrono::{NaiveDate, Utc};
use price_core::Candle;
use price_timeseries::TimescaleClient;

pub struct HistoricalDownloader {
    client: reqwest::Client,
    python_broker_url: String,
    db: TimescaleClient,
}

impl HistoricalDownloader {
    pub fn new(python_broker_url: &str, db: TimescaleClient) -> Self {
        Self {
            client: reqwest::Client::new(),
            python_broker_url: python_broker_url.trim_end_matches('/').to_string(),
            db,
        }
    }

    /// Slices any large historical request into rate-limited, resumable 90-day chunks.
    pub async fn download_history(
        &self,
        symbol: &str,
        exchange: &str,
        from_date: NaiveDate,
        to_date: NaiveDate,
    ) -> anyhow::Result<()> {
        let segments = partition_date_range(from_date, to_date, 7);
        let total_segments = segments.len();
        tracing::info!(
            "Partitioned historical download request for {} into {} weekly segments.",
            symbol, total_segments
        );

        let mut any_failed = false;
        let mut last_error = None;

        for (idx, (seg_start, seg_end)) in segments.into_iter().enumerate() {
            let seg_start_str = seg_start.format("%Y-%m-%d").to_string();
            let seg_end_str = seg_end.format("%Y-%m-%d").to_string();

            // Check if segment is already complete
            if let Ok(Some(status)) = self.db.get_job_status(symbol, seg_start, seg_end).await {
                if status == "COMPLETED" {
                    tracing::info!(
                        "[{}/{}] Segment {} to {} for {} is already COMPLETED. Skipping.",
                        idx + 1, total_segments, seg_start_str, seg_end_str, symbol
                    );
                    continue;
                }
            }

            let mut success = false;
            let mut attempts = 0;
            let max_attempts = 3;

            while attempts < max_attempts && !success {
                attempts += 1;
                self.db.mark_job_status(symbol, seg_start, seg_end, "IN_PROGRESS").await?;

                tracing::info!(
                    "[{}/{}] Downloading segment {} to {} for {} (Attempt {}/{})...",
                    idx + 1, total_segments, seg_start_str, seg_end_str, symbol, attempts, max_attempts
                );

                match self.fetch_and_store_segment(symbol, exchange, seg_start, seg_end).await {
                    Ok(_) => {
                        self.db.mark_job_status(symbol, seg_start, seg_end, "COMPLETED").await?;
                        tracing::info!(
                            "[{}/{}] Successfully downloaded and saved segment {} to {}.",
                            idx + 1, total_segments, seg_start_str, seg_end_str
                        );
                        success = true;
                    }
                    Err(e) => {
                        tracing::error!(
                            "[{}/{}] Attempt {}/{} failed for segment {} to {}: {:?}",
                            idx + 1, total_segments, attempts, max_attempts, seg_start_str, seg_end_str, e
                        );
                        if attempts < max_attempts {
                            let backoff_secs = 2u64.pow(attempts as u32);
                            tracing::warn!("Retrying in {} seconds...", backoff_secs);
                            tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
                        } else {
                            self.db.mark_job_status(symbol, seg_start, seg_end, &format!("FAILED: {}", e)).await?;
                            any_failed = true;
                            last_error = Some(e);
                        }
                    }
                }
            }

            // Expose a rate limit delay (5 seconds) between successful requests to prevent broker bans
            if idx + 1 < total_segments && success {
                tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
            }
        }

        if any_failed {
            anyhow::bail!("Historical downloader execution had segment failures. Last error: {:?}", last_error);
        }

        Ok(())
    }

    async fn fetch_and_store_segment(
        &self,
        symbol: &str,
        exchange: &str,
        from_date: NaiveDate,
        to_date: NaiveDate,
    ) -> anyhow::Result<()> {
        // Resolve symbol mapping if available
        let (canonical_symbol, broker_symbol, exch) = if let Ok(Some(mapping)) = self.db.get_symbol_mapping(symbol).await {
            (mapping.canonical_symbol, mapping.broker_symbol, mapping.exchange)
        } else {
            let ex = if exchange.is_empty() { "NSE" } else { exchange };
            (symbol.to_string(), symbol.to_string(), ex.to_string())
        };

        if exch.to_uppercase() == "DELTA" {
            self.fetch_delta_segment(&canonical_symbol, &broker_symbol, from_date, to_date).await
        } else {
            self.fetch_fyers_segment(&canonical_symbol, &broker_symbol, &exch, from_date, to_date).await
        }
    }

    async fn fetch_delta_segment(
        &self,
        canonical_symbol: &str,
        broker_symbol: &str,
        from_date: NaiveDate,
        to_date: NaiveDate,
    ) -> anyhow::Result<()> {
        let from_time = from_date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
        let to_time = to_date.and_hms_opt(23, 59, 59).unwrap().and_utc().timestamp();

        let delta_url = std::env::var("DELTA_BASE_URL")
            .unwrap_or_else(|_| "https://api.delta.exchange".to_string());
        let url = format!("{}/v2/history/candles?symbol={}&resolution=1m&start={}&end={}",
            delta_url, broker_symbol, from_time, to_time);

        let res = self.client.get(&url).send().await?;
        if !res.status().is_success() {
            let status = res.status();
            let err_text = res.text().await.unwrap_or_default();
            anyhow::bail!("Delta API status {}: {}", status, err_text);
        }

        let body: serde_json::Value = res.json().await?;
        let mut parsed_candles = Vec::new();

        if let Some(result_arr) = body.get("result").and_then(|r| r.as_array()) {
            for item in result_arr {
                let ts_epoch = if let Some(t) = item.get("time").and_then(|t| t.as_i64()) {
                    t
                } else if let Some(t_str) = item.get("time").and_then(|t| t.as_str()) {
                    t_str.parse().unwrap_or(0)
                } else {
                    0
                };

                if ts_epoch == 0 { continue; }
                let timestamp = chrono::DateTime::<Utc>::from_timestamp(ts_epoch, 0)
                    .unwrap_or_else(|| Utc::now());

                let parse_f64 = |val: &serde_json::Value| -> f64 {
                    if let Some(f) = val.as_f64() {
                        f
                    } else if let Some(s) = val.as_str() {
                        s.parse().unwrap_or(0.0)
                    } else {
                        0.0
                    }
                };

                let open = parse_f64(&item["open"]);
                let high = parse_f64(&item["high"]);
                let low = parse_f64(&item["low"]);
                let close = parse_f64(&item["close"]);
                let volume = parse_f64(&item["volume"]).round() as u64;

                parsed_candles.push(Candle {
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume,
                });
            }
        }

        if parsed_candles.is_empty() {
            tracing::warn!("No Delta candles returned for {} ({} to {}).", canonical_symbol, from_date, to_date);
            return Ok(());
        }

        // Sort ascending by timestamp
        parsed_candles.sort_by_key(|c| c.timestamp);

        self.db.insert_candles(canonical_symbol, "DELTA", "1m", &parsed_candles).await?;
        tracing::info!("Saved {} Delta candles for {} in TimescaleDB.", parsed_candles.len(), canonical_symbol);
        Ok(())
    }

    async fn fetch_fyers_segment(
        &self,
        canonical_symbol: &str,
        broker_symbol: &str,
        exchange: &str,
        from_date: NaiveDate,
        to_date: NaiveDate,
    ) -> anyhow::Result<()> {
        let from_str = from_date.format("%Y-%m-%d").to_string();
        let to_str = to_date.format("%Y-%m-%d").to_string();

        let url = format!("{}/history", self.python_broker_url);
        let payload = serde_json::json!({
            "symbol": broker_symbol,
            "resolution": "1", // 1 minute resolution
            "date_format": "0", // Epoch timestamps
            "range_from": from_str,
            "range_to": to_str,
        });

        let res = self.client.post(&url).json(&payload).send().await?;
        if !res.status().is_success() {
            let status = res.status();
            let err_text = res.text().await.unwrap_or_default();
            anyhow::bail!("HTTP status {}: {}", status, err_text);
        }

        let body: serde_json::Value = res.json().await?;
        if body["status"] != "success" {
            let detail = body["detail"].as_str().unwrap_or("Unknown error from python-broker history API");
            anyhow::bail!("History API error: {}", detail);
        }

        let candles_val = body["data"].get("candles").ok_or_else(|| {
            anyhow::anyhow!("Missing candles key in response data")
        })?;

        let mut parsed_candles = Vec::new();
        if let Some(arr) = candles_val.as_array() {
            for c_val in arr {
                if let Some(c_arr) = c_val.as_array() {
                    if c_arr.len() >= 6 {
                        let ts_epoch = c_arr[0].as_f64().unwrap_or(0.0) as i64;
                        let timestamp = chrono::DateTime::<Utc>::from_timestamp(ts_epoch, 0)
                            .unwrap_or_else(|| Utc::now());

                        // Validate market session hours for NSE
                        if !price_core::is_indian_market_hours(timestamp) {
                            continue;
                        }

                        let open = c_arr[1].as_f64().unwrap_or(0.0);
                        let high = c_arr[2].as_f64().unwrap_or(0.0);
                        let low = c_arr[3].as_f64().unwrap_or(0.0);
                        let close = c_arr[4].as_f64().unwrap_or(0.0);
                        let volume = c_arr[5].as_u64().unwrap_or(0);
                        
                        parsed_candles.push(Candle {
                            timestamp,
                            open,
                            high,
                            low,
                            close,
                            volume,
                        });
                    }
                }
            }
        }

        if parsed_candles.is_empty() {
            tracing::warn!("No Fyers candles returned for segment {} to {}.", from_str, to_str);
            return Ok(());
        }

        self.db.insert_candles(canonical_symbol, exchange, "1m", &parsed_candles).await?;
        Ok(())
    }
}


/// Partitions a date range into chunks of specified maximum length in days.
fn partition_date_range(start: NaiveDate, end: NaiveDate, chunk_days: i64) -> Vec<(NaiveDate, NaiveDate)> {
    let mut segments = Vec::new();
    let mut current_start = start;
    while current_start <= end {
        let current_end = (current_start + chrono::Duration::days(chunk_days - 1)).min(end);
        segments.push((current_start, current_end));
        current_start = current_end + chrono::Duration::days(1);
    }
    segments
}
