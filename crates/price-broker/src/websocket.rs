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

/// Delta Exchange India Production WebSocket URL
pub const DELTA_INDIA_WS_URL: &str = "wss://socket.india.delta.exchange";
/// Delta Exchange Global Production WebSocket URL
pub const DELTA_GLOBAL_WS_URL: &str = "wss://socket.delta.exchange";
/// Delta Exchange Testnet WebSocket URL
pub const DELTA_TESTNET_WS_URL: &str = "wss://cdn-ind.testnet.deltaex.org/ws";

const DELTA_WS_URL: &str = DELTA_INDIA_WS_URL;

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
    /// Mark price update for a contract.
    MarkPrice { symbol: String, mark_price: f64, spot_price: f64 },
    /// L2 orderbook snapshot.
    OrderbookSnapshot { symbol: String, bids: Vec<[f64; 2]>, asks: Vec<[f64; 2]> },
    /// Public trade event.
    PublicTrade { symbol: String, price: f64, size: f64, side: String, timestamp: i64 },
    /// Margin update for a wallet asset.
    MarginUpdate { asset_symbol: String, available_balance: f64, position_margin: f64, order_margin: f64 },
    /// User trade fill event.
    UserTrade { trade_id: String, order_id: String, symbol: String, size: i32, price: f64, side: String, role: String },
    /// System status event (maintenance, degraded mode, etc.).
    SystemStatus { status: String, message: String },
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
    /// Custom WebSocket URL override (for testing or different regions).
    ws_url: Option<String>,
}

impl DeltaWebSocketClient {
    pub fn new(
        api_key: Option<String>,
        api_secret: Option<String>,
        symbols: Vec<String>,
    ) -> Self {
        Self { api_key, api_secret, symbols, ws_url: None }
    }

    /// Create a client with a custom WebSocket URL.
    pub fn with_url(
        api_key: Option<String>,
        api_secret: Option<String>,
        symbols: Vec<String>,
        ws_url: String,
    ) -> Self {
        Self { api_key, api_secret, symbols, ws_url: Some(ws_url) }
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
        let ws_url = self.ws_url.clone().unwrap_or_else(|| DELTA_WS_URL.to_string());

        tokio::spawn(async move {
            loop {
                let tx_clone = tx.clone();
                match connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        info!("[DeltaWS] Connected to {}", ws_url);
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

                            // Subscribe to private channels
                            let private_subs = json!({
                                "type": "subscribe",
                                "payload": {
                                    "channels": [
                                        {"name": "orders"},
                                        {"name": "positions"},
                                        {"name": "margins"},
                                        {"name": "v2/user_trades"},
                                        {"name": "portfolio_margins"}
                                    ]
                                }
                            });
                            if let Ok(text) = serde_json::to_string(&private_subs) {
                                let _ = write.send(Message::Text(text)).await;
                                debug!("[DeltaWS] Subscribed to private channels: orders, positions, margins, v2/user_trades, portfolio_margins");
                            }
                        }

                        // Enable heartbeat for connection health monitoring
                        let heartbeat_msg = json!({ "type": "enable_heartbeat" });
                        if let Ok(text) = serde_json::to_string(&heartbeat_msg) {
                            let _ = write.send(Message::Text(text)).await;
                            debug!("[DeltaWS] Enabled heartbeat");
                        }

                        // Subscribe to system_status (no symbol required)
                        let system_sub = json!({
                            "type": "subscribe",
                            "payload": {
                                "channels": [{"name": "system_status"}]
                            }
                        });
                        if let Ok(text) = serde_json::to_string(&system_sub) {
                            let _ = write.send(Message::Text(text)).await;
                        }

                        // Subscribe to public channels per symbol
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
                                        },
                                        {
                                            "name": "mark_price",
                                            "symbols": [symbol]
                                        },
                                        {
                                            "name": "ob_l2",
                                            "symbols": [symbol]
                                        },
                                        {
                                            "name": "trades",
                                            "symbols": [symbol]
                                        }
                                    ]
                                }
                            });
                            if let Ok(text) = serde_json::to_string(&pub_sub) {
                                let _ = write.send(Message::Text(text)).await;
                                debug!("[DeltaWS] Subscribed to ticker+funding_rate+mark_price+ob_l2+trades for {}", symbol);
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
                                            "mark_price" => {
                                                if let Some(mp) = parse_mark_price_event(&data) {
                                                    let _ = tx_clone.send(mp).await;
                                                }
                                            }
                                            "ob_l2" => {
                                                if let Some(ob) = parse_orderbook_event(&data) {
                                                    let _ = tx_clone.send(ob).await;
                                                }
                                            }
                                            "trades" => {
                                                if let Some(trades) = parse_public_trade_event(&data) {
                                                    for t in trades {
                                                        let _ = tx_clone.send(t).await;
                                                    }
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
                                            "margins" => {
                                                if let Some(mu) = parse_margin_update_event(&data) {
                                                    let _ = tx_clone.send(mu).await;
                                                }
                                            }
                                            "v2/user_trades" => {
                                                if let Some(ut) = parse_user_trade_event(&data) {
                                                    let _ = tx_clone.send(ut).await;
                                                }
                                            }
                                            "system_status" => {
                                                if let Some(ss) = parse_system_status_event(&data) {
                                                    let _ = tx_clone.send(ss).await;
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
    let bid: Option<f64> = payload["best_bid"].as_str().and_then(|v| v.parse().ok());
    let ask: Option<f64> = payload["best_ask"].as_str().and_then(|v| v.parse().ok());
    let mark: Option<f64> = payload["mark_price"].as_str().and_then(|v| v.parse().ok());
    if price <= 0.0 { return None; }
    Some(TickData {
        symbol,
        price,
        volume,
        oi,
        timestamp: Utc::now(),
        bid,
        ask,
        mark_price: mark,
    })
}

fn parse_funding_rate_event(data: &serde_json::Value) -> Option<DeltaWsEvent> {
    let payload = &data["payload"];
    let symbol = payload["symbol"].as_str()?.to_string();
    let rate: f64 = payload["funding_rate"].as_str().unwrap_or("0").parse().ok()?;
    let next_time: i64 = payload["next_funding_realization"].as_i64().unwrap_or(0);
    Some(DeltaWsEvent::FundingRate { symbol, rate, next_time })
}

fn parse_mark_price_event(data: &serde_json::Value) -> Option<DeltaWsEvent> {
    let payload = &data["payload"];
    let symbol = payload["symbol"].as_str()?.to_string();
    let mark_price: f64 = payload["mark_price"].as_str().unwrap_or("0").parse().ok()?;
    let spot_price: f64 = payload["spot_price"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
    Some(DeltaWsEvent::MarkPrice { symbol, mark_price, spot_price })
}

fn parse_orderbook_event(data: &serde_json::Value) -> Option<DeltaWsEvent> {
    let payload = &data["payload"];
    let symbol = payload["symbol"].as_str()?.to_string();
    let mut bids = Vec::new();
    let mut asks = Vec::new();

    if let Some(bid_arr) = payload["buy"].as_array() {
        for b in bid_arr {
            let price: f64 = b["price"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let size: f64 = b["size"].as_f64().unwrap_or(0.0);
            bids.push([price, size]);
        }
    }
    if let Some(ask_arr) = payload["sell"].as_array() {
        for a in ask_arr {
            let price: f64 = a["price"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let size: f64 = a["size"].as_f64().unwrap_or(0.0);
            asks.push([price, size]);
        }
    }

    Some(DeltaWsEvent::OrderbookSnapshot { symbol, bids, asks })
}

fn parse_public_trade_event(data: &serde_json::Value) -> Option<Vec<DeltaWsEvent>> {
    let payload = &data["payload"];
    let symbol = payload["symbol"].as_str()?.to_string();
    let mut events = Vec::new();

    if let Some(trades) = payload["trades"].as_array() {
        for t in trades {
            let price: f64 = t["price"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let size: f64 = t["size"].as_f64().unwrap_or(0.0);
            let side = t["buyer_role"].as_str().unwrap_or("taker").to_string();
            let timestamp = t["timestamp"].as_i64().unwrap_or(0);
            events.push(DeltaWsEvent::PublicTrade {
                symbol: symbol.clone(),
                price, size, side, timestamp,
            });
        }
    }

    if events.is_empty() { None } else { Some(events) }
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

fn parse_margin_update_event(data: &serde_json::Value) -> Option<DeltaWsEvent> {
    let payload = &data["payload"];
    let asset_symbol = payload["asset"]["symbol"].as_str().unwrap_or("").to_string();
    let available_balance: f64 = payload["available_balance"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
    let position_margin: f64 = payload["position_margin"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
    let order_margin: f64 = payload["order_margin"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
    Some(DeltaWsEvent::MarginUpdate { asset_symbol, available_balance, position_margin, order_margin })
}

fn parse_user_trade_event(data: &serde_json::Value) -> Option<DeltaWsEvent> {
    let payload = &data["payload"];
    let trade_id = payload["id"].as_i64()?.to_string();
    let order_id = payload["order_id"].as_i64().unwrap_or(0).to_string();
    let symbol = payload["symbol"].as_str().unwrap_or("").to_string();
    let size: i32 = payload["size"].as_str().unwrap_or("0").parse().unwrap_or(0);
    let price: f64 = payload["price"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
    let side = payload["side"].as_str().unwrap_or("buy").to_string();
    let role = payload["role"].as_str().unwrap_or("taker").to_string();
    Some(DeltaWsEvent::UserTrade { trade_id, order_id, symbol, size, price, side, role })
}

fn parse_system_status_event(data: &serde_json::Value) -> Option<DeltaWsEvent> {
    let payload = &data["payload"];
    let status = payload["status"].as_str().unwrap_or("operational").to_string();
    let message = payload["message"].as_str().unwrap_or("").to_string();
    Some(DeltaWsEvent::SystemStatus { status, message })
}
