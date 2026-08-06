use chrono::{NaiveDate, Utc};
use price_core::Candle;
use price_timeseries::TimescaleClient;
use hmac::{Hmac, Mac, KeyInit};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub struct HistoricalDownloader {
    client: reqwest::Client,
    python_broker_url: String,
    db: TimescaleClient,
}

impl HistoricalDownloader {
    pub fn new(python_broker_url: &str, db: TimescaleClient) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "User-Agent",
            reqwest::header::HeaderValue::from_static("price-engine-rust/1.1"),
        );
        headers.insert(
            "Content-Type",
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            client,
            python_broker_url: python_broker_url.trim_end_matches('/').to_string(),
            db,
        }
    }

    /// Generates a Delta Exchange HMAC-SHA256 signature.
    /// signature_data = method + timestamp + path + query_string
    fn delta_sign(secret: &str, method: &str, timestamp: u64, path: &str, query_string: &str) -> String {
        let data = format!("{}{}{}{}", method, timestamp, path, query_string);
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .expect("HMAC accepts any key length");
        mac.update(data.as_bytes());
        mac.finalize().into_bytes().iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Slices any large historical request into rate-limited, resumable chunks.
    pub async fn download_history(
        &self,
        symbol: &str,
        exchange: &str,
        from_date: NaiveDate,
        to_date: NaiveDate,
    ) -> anyhow::Result<()> {
        let chunk_size = if exchange.to_uppercase() == "DELTA" { 14 } else { 30 };
        let segments = partition_date_range(from_date, to_date, chunk_size);
        let total_segments = segments.len();
        tracing::info!(
            "Partitioned historical download request for {} into {} segments (chunk size {} days).",
            symbol, total_segments, chunk_size
        );

        let mut any_failed = false;
        let mut last_error = None;

        for (idx, (seg_start, seg_end)) in segments.into_iter().enumerate() {
            let seg_start_str = seg_start.format("%Y-%m-%d").to_string();
            let seg_end_str   = seg_end.format("%Y-%m-%d").to_string();

            // Skip already-completed segments
            if let Ok(Some(status)) = self.db.get_job_status(symbol, seg_start, seg_end).await {
                if status == "COMPLETED" {
                    tracing::info!(
                        "[{}/{}] Segment {} to {} for {} already COMPLETED. Skipping.",
                        idx + 1, total_segments, seg_start_str, seg_end_str, symbol
                    );
                    continue;
                }
            }

            let mut success          = false;
            let mut attempts         = 0;
            let max_attempts         = 3;
            let mut is_rate_blocked  = false;

            while attempts < max_attempts && !success && !is_rate_blocked {
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
                            "[{}/{}] Successfully downloaded segment {} to {}.",
                            idx + 1, total_segments, seg_start_str, seg_end_str
                        );
                        success = true;
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        // 403 from CloudFront: mark as SKIPPED and continue to next segment
                        if err_str.contains("403") || err_str.contains("Forbidden") || err_str.contains("CloudFront") {
                            tracing::warn!(
                                "[{}/{}] Segment {} to {} blocked (403). Marking SKIPPED.",
                                idx + 1, total_segments, seg_start_str, seg_end_str
                            );
                            self.db.mark_job_status(symbol, seg_start, seg_end, "SKIPPED: 403 rate-limited").await?;
                            is_rate_blocked = true;
                            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        } else {
                            tracing::error!(
                                "[{}/{}] Attempt {}/{} failed for segment {} to {}: {:?}",
                                idx + 1, total_segments, attempts, max_attempts, seg_start_str, seg_end_str, e
                            );
                            if attempts < max_attempts {
                                let backoff_secs = if exchange.to_uppercase() == "DELTA" {
                                    match attempts { 1 => 5, 2 => 15, _ => 30 }
                                } else {
                                    2u64.pow(attempts as u32)
                                };
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
            }

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
        let to_time   = to_date.and_hms_opt(23, 59, 59).unwrap().and_utc().timestamp();

        let delta_url  = std::env::var("DELTA_BASE_URL")
            .unwrap_or_else(|_| "https://api.india.delta.exchange".to_string());
        let api_key    = std::env::var("DELTA_API_KEY").unwrap_or_default();
        let api_secret = std::env::var("DELTA_API_SECRET").unwrap_or_default();

        let path         = "/v2/history/candles";
        let query_string = format!("?symbol={}&resolution=1m&start={}&end={}", broker_symbol, from_time, to_time);
        let url          = format!("{}{}{}", delta_url, path, query_string);

        let now_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut req = self.client.get(&url);

        if !api_key.is_empty() && !api_secret.is_empty() {
            let sig = Self::delta_sign(&api_secret, "GET", now_ts, path, &query_string);
            req = req
                .header("api-key", &api_key)
                .header("signature", sig)
                .header("timestamp", now_ts.to_string());
        }

        let res    = req.send().await?;
        let status = res.status();

        if !status.is_success() {
            let err_text = res.text().await.unwrap_or_default();
            anyhow::bail!("Delta API status {} {}: {}", status.as_u16(),
                status.canonical_reason().unwrap_or(""), err_text);
        }

        let body: serde_json::Value = res.json().await?;
        let mut parsed_candles = Vec::new();

        if let Some(result_arr) = body.get("result").and_then(|r| r.as_array()) {
            for item in result_arr {
                let ts_epoch = item.get("time")
                    .and_then(|t| t.as_i64())
                    .or_else(|| item.get("time").and_then(|t| t.as_str()).and_then(|s| s.parse().ok()))
                    .unwrap_or(0);

                if ts_epoch == 0 { continue; }

                let timestamp = chrono::DateTime::<Utc>::from_timestamp(ts_epoch, 0)
                    .unwrap_or_else(|| Utc::now());

                let parse_f64 = |val: &serde_json::Value| -> f64 {
                    val.as_f64()
                        .or_else(|| val.as_str().and_then(|s| s.parse().ok()))
                        .unwrap_or(0.0)
                };

                let open   = parse_f64(&item["open"]);
                let high   = parse_f64(&item["high"]);
                let low    = parse_f64(&item["low"]);
                let close  = parse_f64(&item["close"]);
                let volume = parse_f64(&item["volume"]).round() as u64;

                parsed_candles.push(Candle { timestamp, open, high, low, close, volume });
            }
        }

        if parsed_candles.is_empty() {
            tracing::warn!("No Delta candles returned for {} ({} to {}).", canonical_symbol, from_date, to_date);
            return Ok(());
        }

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
        let from_str = format!("{} 09:15:00", from_date.format("%Y-%m-%d"));
        let to_str   = format!("{} 15:30:00", to_date.format("%Y-%m-%d"));

        let url     = format!("{}/history", self.python_broker_url);
        let payload = serde_json::json!({
            "symbol":      broker_symbol,
            "resolution":  "1",
            "date_format": "1",
            "range_from":  from_str,
            "range_to":    to_str,
        });

        let res = self.client.post(&url).json(&payload).send().await?;
        if !res.status().is_success() {
            let status   = res.status();
            let err_text = res.text().await.unwrap_or_default();
            anyhow::bail!("HTTP status {}: {}", status, err_text);
        }

        let body: serde_json::Value = res.json().await?;
        if body["status"] != "success" {
            let detail = body["detail"].as_str()
                .unwrap_or("Unknown error from python-broker history API");
            anyhow::bail!("History API error: {}", detail);
        }

        let candles_val = body["data"].get("candles")
            .ok_or_else(|| anyhow::anyhow!("Missing candles key in response data"))?;

        let mut parsed_candles = Vec::new();
        if let Some(arr) = candles_val.as_array() {
            for c_val in arr {
                if let Some(c_arr) = c_val.as_array() {
                    if c_arr.len() >= 6 {
                        let ts_epoch  = c_arr[0].as_f64().unwrap_or(0.0) as i64;
                        let timestamp = chrono::DateTime::<Utc>::from_timestamp(ts_epoch, 0)
                            .unwrap_or_else(|| Utc::now());

                        if !price_core::is_indian_market_hours(timestamp) { continue; }

                        let open   = c_arr[1].as_f64().unwrap_or(0.0);
                        let high   = c_arr[2].as_f64().unwrap_or(0.0);
                        let low    = c_arr[3].as_f64().unwrap_or(0.0);
                        let close  = c_arr[4].as_f64().unwrap_or(0.0);
                        let volume = c_arr[5].as_u64().unwrap_or(0);

                        parsed_candles.push(Candle { timestamp, open, high, low, close, volume });
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


/// Partitions a date range into chunks of the specified maximum size in days.
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
