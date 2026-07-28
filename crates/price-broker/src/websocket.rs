use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use serde_json::json;
use hmac::{Hmac, Mac, KeyInit};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn, error, debug};
use price_core::TickData;
use chrono::Utc;

type HmacSha256 = Hmac<Sha256>;

const DELTA_WS_URL: &str = "wss://socket.delta.exchange";

/// Event types emitted from the Delta WebSocket feed.
#[derive(Debug, Clone)]
pub enum DeltaWsEvent {
    /// A live market tick received from a ticker subscription.
    Tick(TickData),
    /// A funding rate update for a perpetual contract.
    FundingRate { symbol: String, rate: f64, next_time: i64 },
    /// An order state change (filled, cancelled, rejected).
    OrderUpdate { order_id: String, symbol: String, status: String, filled_qty: i32, avg_price: f64 },
    /// A position change update.
    PositionUpdate { symbol: String, size: i32, entry_price: f64, unrealized_pnl: f64 },
    /// WebSocket connection status.
    Connected,
    Disconnected,
}

/// Delta Exchange WebSocket client.
/// Provides authenticated market data and private account update streams.
pub struct DeltaWebSocketClient {
    api_key: Option<String>,
    api_secret: Option<String>,
    /// Symbols to subscribe to market data for.
    symbols: Vec<String>,
}

impl DeltaWebSocketClient {
    pub fn new(
        api_key: Option<String>,
        api_secret: Option<String>,
        symbols: Vec<String>,
    ) -> Self {
        Self { api_key, api_secret, symbols }
    }

    fn generate_auth_signature(&self, timestamp: u64) -> Option<(String, String)> {
        let secret = self.api_secret.as_ref()?;
        let key = self.api_key.as_ref()?;
        let msg = format!("GET{}/live", timestamp);
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
        mac.update(msg.as_bytes());
        let result = mac.finalize();
        let sig = result.into_bytes().iter().map(|b| format!("{:02x}", b)).collect::<String>();
        Some((key.clone(), sig))
    }

    /// Start the WebSocket event loop. Returns an mpsc receiver of DeltaWsEvent.
    /// Automatically reconnects on disconnection.
    /// 
    /// # Arguments
    /// * `buffer_size` — size of the bounded mpsc channel for events
    pub async fn start(self, buffer_size: usize) -> mpsc::Receiver<DeltaWsEvent> {
        let (tx, rx) = mpsc::channel::<DeltaWsEvent>(buffer_size);
        let symbols = self.symbols.clone();
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();

        tokio::spawn(async move {
            loop {
                let tx_clone = tx.clone();
                match connect_async(DELTA_WS_URL).await {
                    Ok((ws_stream, _)) => {
                        info!("[DeltaWS] Connected to {}", DELTA_WS_URL);
                        let _ = tx_clone.send(DeltaWsEvent::Connected).await;

                        let (mut write, mut read) = ws_stream.split();

                        // Authenticate if keys are available
                        let timestamp = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs();

                        if let (Some(key), Some(secret)) = (&api_key, &api_secret) {
                            let msg = format!("GET{}/live", timestamp);
                            let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
                            mac.update(msg.as_bytes());
                            let sig = mac.finalize().into_bytes()
                                .iter().map(|b| format!("{:02x}", b)).collect::<String>();

                            let auth_msg = json!({
                                "type": "auth",
                                "payload": {
                                    "api-key": key,
                                    "signature": sig,
                                    "timestamp": timestamp.to_string()
                                }
                            });
                            if let Ok(text) = serde_json::to_string(&auth_msg) {
                                let _ = write.send(Message::Text(text)).await;
                                info!("[DeltaWS] Sent authentication frame");
                            }

                            // Subscribe to private order and position updates
                            let private_subs = json!({
                                "type": "subscribe",
                                "payload": {
                                    "channels": [
                                        {"name": "orders"},
                                        {"name": "positions"}
                                    ]
                                }
                            });
                            if let Ok(text) = serde_json::to_string(&private_subs) {
                                let _ = write.send(Message::Text(text)).await;
                            }
                        }

                        // Subscribe to public ticker and funding rate channels
                        for symbol in &symbols {
                            let pub_sub = json!({
                                "type": "subscribe",
                                "payload": {
                                    "channels": [
                                        {
                                            "name": "v2/ticker",
                                            "symbols": [symbol]
                                        },
                                        {
                                            "name": "funding_rate",
                                            "symbols": [symbol]
                                        }
                                    ]
                                }
                            });
                            if let Ok(text) = serde_json::to_string(&pub_sub) {
                                let _ = write.send(Message::Text(text)).await;
                                debug!("[DeltaWS] Subscribed to ticker+funding_rate for {}", symbol);
                            }
                        }

                        // Process incoming messages
                        while let Some(msg) = read.next().await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                        let ch = data["type"].as_str().unwrap_or("");
                                        match ch {
                                            "v2/ticker" => {
                                                if let Some(tick_data) = parse_ticker_event(&data) {
                                                    let _ = tx_clone.send(DeltaWsEvent::Tick(tick_data)).await;
                                                }
                                            }
                                            "funding_rate" => {
                                                if let Some(fr) = parse_funding_rate_event(&data) {
                                                    let _ = tx_clone.send(fr).await;
                                                }
                                            }
                                            "orders" => {
                                                if let Some(ou) = parse_order_update_event(&data) {
                                                    let _ = tx_clone.send(ou).await;
                                                }
                                            }
                                            "positions" => {
                                                if let Some(pu) = parse_position_update_event(&data) {
                                                    let _ = tx_clone.send(pu).await;
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                Ok(Message::Ping(p)) => {
                                    let _ = write.send(Message::Pong(p)).await;
                                }
                                Ok(Message::Close(_)) => {
                                    warn!("[DeltaWS] Server sent close frame. Reconnecting...");
                                    break;
                                }
                                Err(e) => {
                                    error!("[DeltaWS] Connection error: {}. Reconnecting...", e);
                                    break;
                                }
                                _ => {}
                            }
                        }

                        let _ = tx_clone.send(DeltaWsEvent::Disconnected).await;
                    }
                    Err(e) => {
                        error!("[DeltaWS] Failed to connect: {}. Retrying in 5s...", e);
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });

        rx
    }
}

fn parse_ticker_event(data: &serde_json::Value) -> Option<TickData> {
    let payload = &data["payload"];
    let symbol = payload["symbol"].as_str()?.to_string();
    let price: f64 = payload["close"].as_str().unwrap_or("0").parse().ok()?;
    let volume: u64 = payload["volume"].as_str().unwrap_or("0").parse().unwrap_or(0);
    let oi: u64 = payload["open_interest"].as_str().unwrap_or("0").parse().unwrap_or(0);
    if price <= 0.0 { return None; }
    Some(TickData {
        symbol,
        price,
        volume,
        oi,
        timestamp: Utc::now(),
    })
}

fn parse_funding_rate_event(data: &serde_json::Value) -> Option<DeltaWsEvent> {
    let payload = &data["payload"];
    let symbol = payload["symbol"].as_str()?.to_string();
    let rate: f64 = payload["funding_rate"].as_str().unwrap_or("0").parse().ok()?;
    let next_time: i64 = payload["next_funding_realization"].as_i64().unwrap_or(0);
    Some(DeltaWsEvent::FundingRate { symbol, rate, next_time })
}

fn parse_order_update_event(data: &serde_json::Value) -> Option<DeltaWsEvent> {
    let payload = &data["payload"];
    let order_id = payload["id"].as_i64()?.to_string();
    let symbol = payload["symbol"].as_str()?.to_string();
    let status = payload["state"].as_str().unwrap_or("open").to_string();
    let filled_qty = payload["size"].as_str().unwrap_or("0").parse().unwrap_or(0i32);
    let avg_price: f64 = payload["average_fill_price"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
    Some(DeltaWsEvent::OrderUpdate { order_id, symbol, status, filled_qty, avg_price })
}

fn parse_position_update_event(data: &serde_json::Value) -> Option<DeltaWsEvent> {
    let payload = &data["payload"];
    let symbol = payload["symbol"].as_str()?.to_string();
    let size: i32 = payload["size"].as_str().unwrap_or("0").parse().unwrap_or(0);
    let entry_price: f64 = payload["entry_price"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
    let unrealized_pnl: f64 = payload["unrealized_pnl"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
    Some(DeltaWsEvent::PositionUpdate { symbol, size, entry_price, unrealized_pnl })
}
