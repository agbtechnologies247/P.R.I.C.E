use chrono::{NaiveDate, DateTime, Utc};
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

    pub async fn download_history(
        &self,
        symbol: &str,
        exchange: &str,
        from_date: NaiveDate,
        to_date: NaiveDate,
    ) -> anyhow::Result<()> {
        let from_str = from_date.format("%Y-%m-%d").to_string();
        let to_str = to_date.format("%Y-%m-%d").to_string();

        tracing::info!("Downloading historical data for {} from {} to {}...", symbol, from_str, to_str);
        
        self.db.mark_job_status(symbol, from_date, to_date, "IN_PROGRESS").await?;

        let url = format!("{}/history", self.python_broker_url);
        let payload = serde_json::json!({
            "symbol": symbol,
            "resolution": "1", // 1 minute resolution
            "date_format": "0", // Epoch timestamps
            "range_from": from_str,
            "range_to": to_str,
        });

        match self.client.post(&url).json(&payload).send().await {
            Ok(res) => {
                if res.status().is_success() {
                    let body: serde_json::Value = res.json().await?;
                    if body["status"] == "success" {
                        if let Some(candles_val) = body["data"].get("candles") {
                            let mut parsed_candles = Vec::new();
                            if let Some(arr) = candles_val.as_array() {
                                for c_val in arr {
                                    if let Some(c_arr) = c_val.as_array() {
                                        if c_arr.len() >= 6 {
                                            let ts_epoch = c_arr[0].as_f64().unwrap_or(0.0) as i64;
                                            let timestamp = DateTime::<Utc>::from_timestamp(ts_epoch, 0)
                                                .unwrap_or_else(|| Utc::now());
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
                            
                            let candle_count = parsed_candles.len();
                            self.db.insert_candles(symbol, exchange, "1m", &parsed_candles).await?;
                            self.db.mark_job_status(symbol, from_date, to_date, "COMPLETED").await?;
                            tracing::info!("Successfully stored {} candles for {} in TimescaleDB.", candle_count, symbol);
                            return Ok(());
                        }
                    }
                    let detail = body["detail"].as_str().unwrap_or("Unknown error from python-broker history API");
                    self.db.mark_job_status(symbol, from_date, to_date, &format!("FAILED: {}", detail)).await?;
                    anyhow::bail!("History download failed: {}", detail);
                } else {
                    let err_text = res.text().await.unwrap_or_default();
                    self.db.mark_job_status(symbol, from_date, to_date, &format!("FAILED HTTP: {}", err_text)).await?;
                    anyhow::bail!("History download failed HTTP {}: {}", url, err_text);
                }
            }
            Err(e) => {
                self.db.mark_job_status(symbol, from_date, to_date, &format!("FAILED: {}", e)).await?;
                anyhow::bail!("Network error communicating with Python Bridge: {:?}", e);
            }
        }
    }
}
