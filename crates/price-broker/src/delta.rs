use async_trait::async_trait;
use price_core::{PriceError, Result};
use reqwest::Client;
use crate::models::*;
use crate::traits::Broker;
use hmac::{Hmac, Mac, KeyInit};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type HmacSha256 = Hmac<Sha256>;

pub struct DeltaExchangeClient {
    client: Client,
    base_url: String,
    api_key: Option<String>,
    api_secret: Option<String>,
    /// Cached instrument metadata: symbol → InstrumentMeta
    instrument_cache: Arc<RwLock<HashMap<String, InstrumentMeta>>>,
}

impl DeltaExchangeClient {
    pub fn new(base_url: &str, api_key: Option<String>, api_secret: Option<String>) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("User-Agent", reqwest::header::HeaderValue::from_static("price-engine-rust"));
        headers.insert("Content-Type", reqwest::header::HeaderValue::from_static("application/json"));
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            api_secret,
            instrument_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn generate_signature(&self, method: &str, timestamp: u64, path: &str, query_or_body: &str) -> Option<String> {
        let secret = self.api_secret.as_ref()?;
        let data = format!("{}{}{}{}", method, timestamp, path, query_or_body);
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
        mac.update(data.as_bytes());
        let result = mac.finalize();
        let bytes = result.into_bytes();
        Some(bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>())
    }

    fn add_auth_headers(&self, builder: reqwest::RequestBuilder, method: &str, path: &str, body: &str) -> reqwest::RequestBuilder {
        if let (Some(key), Some(_)) = (&self.api_key, &self.api_secret) {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            if let Some(sig) = self.generate_signature(method, timestamp, path, body) {
                return builder
                    .header("api-key", key)
                    .header("signature", sig)
                    .header("timestamp", timestamp.to_string());
            }
        }
        builder
    }

    /// Resolves the Delta Exchange product_id from a symbol name using the cached instrument list.
    /// Falls back to 27 (BTCUSD Perpetual) if not found.
    pub async fn resolve_product_id(&self, symbol: &str) -> i64 {
        // Try the cache first
        if let Ok(cache) = self.instrument_cache.read() {
            if let Some(meta) = cache.get(symbol) {
                return meta.product_id;
            }
        }
        // Attempt a live fetch to populate cache
        if let Ok(instruments) = self.get_instruments().await {
            if let Ok(mut cache) = self.instrument_cache.write() {
                for inst in &instruments {
                    cache.insert(inst.symbol.clone(), inst.clone());
                }
            }
            // Search again after cache update
            if let Ok(cache) = self.instrument_cache.read() {
                if let Some(meta) = cache.get(symbol) {
                    return meta.product_id;
                }
            }
        }
        tracing::warn!("Could not resolve product_id for symbol '{}'. Using fallback 27 (BTCUSD_PERP).", symbol);
        27 // Default fallback: BTCUSD Perpetual product_id on Delta Exchange
    }

    /// Fetches all instruments / products from Delta Exchange.
    pub async fn get_instruments(&self) -> Result<Vec<InstrumentMeta>> {
        let path = "/v2/products";
        let url = format!("{}{}", self.base_url, path);
        let res = self.client.get(&url)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
        let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if body["success"].as_bool().unwrap_or(false) {
            let mut instruments = Vec::new();
            if let Some(arr) = body["result"].as_array() {
                for item in arr {
                    instruments.push(InstrumentMeta {
                        product_id: item["id"].as_i64().unwrap_or(0),
                        symbol: item["symbol"].as_str().unwrap_or("").to_string(),
                        contract_type: item["contract_type"].as_str().unwrap_or("").to_string(),
                        contract_size: item["contract_size"].as_str().unwrap_or("1").parse().unwrap_or(1.0),
                        min_size: item["min_size"].as_str().unwrap_or("1").parse().unwrap_or(1.0),
                        tick_size: item["tick_size"].as_str().unwrap_or("0.5").parse().unwrap_or(0.5),
                        max_leverage: item["max_leverage_notional"].as_str().unwrap_or("200").parse().unwrap_or(200.0),
                        underlying_asset: item["underlying_asset"]["symbol"].as_str().unwrap_or("").to_string(),
                    });
                }
            }
            Ok(instruments)
        } else {
            Err(PriceError::BrokerError("Failed to fetch Delta instruments".to_string()))
        }
    }

    /// Fetches the current funding rate for a perpetual futures symbol.
    pub async fn get_funding_rate(&self, symbol: &str) -> Result<FundingRate> {
        let path = format!("/v2/tickers/{}", symbol);
        let url = format!("{}{}", self.base_url, path);
        let res = self.client.get(&url)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
        let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if body["success"].as_bool().unwrap_or(false) {
            let data = &body["result"];
            let rate: f64 = data["funding_rate"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
            let next_time: i64 = data["next_funding_realization"].as_i64().unwrap_or(0);
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
            Ok(FundingRate {
                symbol: symbol.to_string(),
                rate,
                timestamp: ts,
                next_funding_time: next_time,
            })
        } else {
            Err(PriceError::BrokerError(format!("Failed to fetch funding rate for {}", symbol)))
        }
    }

    /// Sets leverage for a given product on Delta Exchange.
    /// Uses the leverage defined by DeltaLeverageConfig if not overridden.
    pub async fn set_leverage(&self, product_id: i64, leverage: u32) -> Result<()> {
        if self.api_key.is_none() {
            tracing::info!("Simulated leverage set to {}x for product_id={}", leverage, product_id);
            return Ok(());
        }

        let path = "/v2/products/leverage/set";
        let url = format!("{}{}", self.base_url, path);
        let payload = serde_json::json!({
            "product_id": product_id,
            "leverage": leverage.to_string()
        });
        let body_str = payload.to_string();
        let req = self.client.post(&url).body(body_str.clone());
        let req = self.add_auth_headers(req, "POST", path, &body_str);

        let res = req.send().await.map_err(|e| PriceError::Network(e.to_string()))?;
        let resp: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if resp["success"].as_bool().unwrap_or(false) {
            tracing::info!("Leverage set to {}x for product_id={}", leverage, product_id);
            Ok(())
        } else {
            Err(PriceError::BrokerError(format!(
                "Failed to set leverage for product_id={}: {}",
                product_id,
                resp["error"]["message"].as_str().unwrap_or("unknown error")
            )))
        }
    }

    /// Sets the margin mode (isolated or cross) for a given product.
    pub async fn change_margin_mode(&self, product_id: i64, mode: MarginMode) -> Result<()> {
        if self.api_key.is_none() {
            return Ok(());
        }
        let mode_str = match mode {
            MarginMode::Isolated => "isolated",
            MarginMode::Cross => "cross",
        };
        let path = "/v2/positions/change_margin_mode";
        let url = format!("{}{}", self.base_url, path);
        let payload = serde_json::json!({
            "product_id": product_id,
            "margin_mode": mode_str
        });
        let body_str = payload.to_string();
        let req = self.client.post(&url).body(body_str.clone());
        let req = self.add_auth_headers(req, "POST", path, &body_str);

        let res = req.send().await.map_err(|e| PriceError::Network(e.to_string()))?;
        let resp: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if resp["success"].as_bool().unwrap_or(false) {
            Ok(())
        } else {
            Err(PriceError::BrokerError(format!(
                "Failed to change margin mode for product_id={}", product_id
            )))
        }
    }
}


#[async_trait]
impl Broker for DeltaExchangeClient {
    async fn login(&self) -> Result<String> {
        // Delta Exchange uses API Key authentication per request.
        // Login acts as a connectivity & verification check.
        if self.api_key.is_none() || self.api_secret.is_none() {
            tracing::warn!("Delta Exchange: API keys missing. Initializing in SIMULATION fallback mode.");
            return Ok("mock-delta-token".to_string());
        }
        
        let profile = self.profile().await?;
        Ok(format!("delta-auth-{}", profile.fy_id))
    }

    async fn logout(&self) -> Result<()> {
        Ok(())
    }

    async fn profile(&self) -> Result<UserProfile> {
        if self.api_key.is_none() {
            return Ok(UserProfile {
                name: "Simulated Delta Trader".to_string(),
                fy_id: "delta-mock-id".to_string(),
                email: "trader@delta-simulation.com".to_string(),
                pin_set: true,
            });
        }

        let path = "/v2/profile";
        let url = format!("{}{}", self.base_url, path);
        let req = self.client.get(&url);
        let req = self.add_auth_headers(req, "GET", path, "");

        let res = req.send().await.map_err(|e| PriceError::Network(e.to_string()))?;
        let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if body["success"].as_bool().unwrap_or(false) {
            let data = &body["result"];
            Ok(UserProfile {
                name: data["username"].as_str().unwrap_or("Delta Trader").to_string(),
                fy_id: data["id"].as_i64().unwrap_or(0).to_string(),
                email: data["email"].as_str().unwrap_or("").to_string(),
                pin_set: true,
            })
        } else {
            Err(PriceError::Authentication(body["error"]["message"].as_str().unwrap_or("Delta auth failed").to_string()))
        }
    }

    async fn funds(&self) -> Result<AccountFunds> {
        if self.api_key.is_none() {
            return Ok(AccountFunds {
                available_balance: 50000.0,
                utilised_balance: 0.0,
                limit_amount: 50000.0,
            });
        }

        let path = "/v2/wallet/balances";
        let url = format!("{}{}", self.base_url, path);
        let req = self.client.get(&url);
        let req = self.add_auth_headers(req, "GET", path, "");

        let res = req.send().await.map_err(|e| PriceError::Network(e.to_string()))?;
        let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if body["success"].as_bool().unwrap_or(false) {
            let mut available = 0.0;
            if let Some(arr) = body["result"].as_array() {
                for item in arr {
                    // Total balances across USDC, USDT, BTC etc.
                    let balance: f64 = item["balance"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                    available += balance;
                }
            }
            Ok(AccountFunds {
                available_balance: available,
                utilised_balance: 0.0,
                limit_amount: available,
            })
        } else {
            Err(PriceError::BrokerError("Failed to fetch Delta balances".to_string()))
        }
    }

    async fn positions(&self) -> Result<Vec<Position>> {
        if self.api_key.is_none() {
            return Ok(Vec::new());
        }

        let path = "/v2/positions";
        let url = format!("{}{}", self.base_url, path);
        let req = self.client.get(&url);
        let req = self.add_auth_headers(req, "GET", path, "");

        let res = req.send().await.map_err(|e| PriceError::Network(e.to_string()))?;
        let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if body["success"].as_bool().unwrap_or(false) {
            let mut positions = Vec::new();
            if let Some(arr) = body["result"].as_array() {
                for p in arr {
                    let size: i32 = p["size"].as_str().unwrap_or("0").parse().unwrap_or(0);
                    if size == 0 {
                        continue;
                    }
                    let side = if size > 0 { Side::Buy } else { Side::Sell };
                    let entry_price: f64 = p["entry_price"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                    let mark_price: f64 = p["mark_price"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                    let pnl: f64 = p["realized_pnl"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0) 
                        + p["unrealized_pnl"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                    
                    positions.push(Position {
                        symbol: p["symbol"].as_str().unwrap_or("").to_string(),
                        side,
                        buy_qty: if size > 0 { size } else { 0 },
                        sell_qty: if size < 0 { -size } else { 0 },
                        avg_price: entry_price,
                        current_price: mark_price,
                        pnl,
                    });
                }
            }
            Ok(positions)
        } else {
            Err(PriceError::BrokerError("Failed to fetch Delta positions".to_string()))
        }
    }

    async fn holdings(&self) -> Result<Vec<Holding>> {
        // Futures accounts do not have equities holdings, only positions.
        Ok(Vec::new())
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        if self.api_key.is_none() {
            let order_id = format!("delta-sim-{}", uuid::Uuid::new_v4().simple());
            return Ok(OrderResponse {
                status: "success".to_string(),
                message: "Simulated Delta Order placed".to_string(),
                order_id,
            });
        }

        // Step 1: Resolve product_id from symbol via instrument cache
        let product_id = self.resolve_product_id(&request.symbol).await;

        // Step 2: Auto-configure leverage from DeltaLeverageConfig before entry
        let leverage = request.leverage.unwrap_or_else(|| DeltaLeverageConfig::leverage_for(&request.symbol));
        if let Err(e) = self.set_leverage(product_id, leverage).await {
            tracing::warn!("Could not set leverage before order: {:?}. Proceeding with existing leverage.", e);
        }

        let path = "/v2/orders";
        let url = format!("{}{}", self.base_url, path);
        
        let delta_side = match request.side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        let order_type = match request.r#type {
            1 => "limit",
            2 => "market",
            3 => "stop_market",
            4 => "stop_limit",
            _ => "market",
        };

        let mut payload = serde_json::json!({
            "product_id": product_id,
            "size": request.qty,
            "side": delta_side,
            "order_type": order_type,
        });

        if request.r#type == 1 || request.r#type == 4 {
            payload["limit_price"] = serde_json::json!(request.limit_price.to_string());
        }
        if request.r#type == 3 || request.r#type == 4 {
            payload["stop_price"] = serde_json::json!(request.stop_price.to_string());
        }
        if let Some(ro) = request.reduce_only {
            payload["reduce_only"] = serde_json::json!(ro);
        }
        if let Some(po) = request.post_only {
            payload["post_only"] = serde_json::json!(po);
        }

        let body_str = payload.to_string();
        let req = self.client.post(&url).body(body_str.clone());
        let req = self.add_auth_headers(req, "POST", path, &body_str);

        let res = req.send().await.map_err(|e| PriceError::Network(e.to_string()))?;
        let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if body["success"].as_bool().unwrap_or(false) {
            tracing::info!("Order placed on Delta Exchange: symbol={} side={} qty={} leverage={}x", 
                &request.symbol, delta_side, request.qty, leverage);
            Ok(OrderResponse {
                status: "success".to_string(),
                message: format!("Order placed on Delta Exchange at {}x leverage", leverage),
                order_id: body["result"]["id"].as_i64().unwrap_or(0).to_string(),
            })
        } else {
            Err(PriceError::BrokerError(body["error"]["message"].as_str().unwrap_or("Delta order placement rejected").to_string()))
        }
    }

    async fn modify_order(&self, request: ModifyOrder) -> Result<OrderResponse> {
        if self.api_key.is_none() {
            return Ok(OrderResponse {
                status: "success".to_string(),
                message: "Simulated Delta Order modified".to_string(),
                order_id: request.id,
            });
        }

        let path = "/v2/orders";
        let url = format!("{}{}", self.base_url, path);

        let mut payload = serde_json::json!({
            "id": request.id,
            "size": request.qty,
        });

        if request.r#type == 1 {
            payload["limit_price"] = serde_json::json!(request.limit_price.to_string());
        }

        let body_str = payload.to_string();
        let req = self.client.put(&url).body(body_str.clone());
        let req = self.add_auth_headers(req, "PUT", path, &body_str);

        let res = req.send().await.map_err(|e| PriceError::Network(e.to_string()))?;
        let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if body["success"].as_bool().unwrap_or(false) {
            Ok(OrderResponse {
                status: "success".to_string(),
                message: "Order modified successfully".to_string(),
                order_id: body["result"]["id"].as_i64().unwrap_or(0).to_string(),
            })
        } else {
            Err(PriceError::BrokerError(body["error"]["message"].as_str().unwrap_or("Delta order modification failed").to_string()))
        }
    }

    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        if self.api_key.is_none() {
            return Ok(());
        }

        let path = "/v2/orders";
        let url = format!("{}{}", self.base_url, path);
        let payload = serde_json::json!({ "id": order_id });
        let body_str = payload.to_string();
        
        let req = self.client.delete(&url).body(body_str.clone());
        let req = self.add_auth_headers(req, "DELETE", path, &body_str);

        let res = req.send().await.map_err(|e| PriceError::Network(e.to_string()))?;
        let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if body["success"].as_bool().unwrap_or(false) {
            Ok(())
        } else {
            Err(PriceError::BrokerError(body["error"]["message"].as_str().unwrap_or("Delta order cancellation failed").to_string()))
        }
    }

    async fn orderbook(&self) -> Result<Vec<Order>> {
        if self.api_key.is_none() {
            return Ok(Vec::new());
        }

        let path = "/v2/orders";
        let url = format!("{}{}", self.base_url, path);
        let req = self.client.get(&url);
        let req = self.add_auth_headers(req, "GET", path, "");

        let res = req.send().await.map_err(|e| PriceError::Network(e.to_string()))?;
        let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if body["success"].as_bool().unwrap_or(false) {
            let mut orders = Vec::new();
            if let Some(arr) = body["result"].as_array() {
                for o in arr {
                    let side = if o["side"].as_str().unwrap_or("buy") == "buy" { Side::Buy } else { Side::Sell };
                    let status = match o["state"].as_str().unwrap_or("open") {
                        "open" | "pending" => OrderStatus::PENDING,
                        "filled" => OrderStatus::FILLED,
                        "cancelled" => OrderStatus::CANCELLED,
                        _ => OrderStatus::REJECTED,
                    };
                    orders.push(Order {
                        id: o["id"].as_i64().unwrap_or(0).to_string(),
                        broker: BrokerType::DeltaExchange,
                        symbol: o["symbol"].as_str().unwrap_or("").to_string(),
                        side,
                        quantity: o["size"].as_i64().unwrap_or(0) as i32,
                        avg_price: o["limit_price"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0),
                        status,
                        timestamp: o["created_at"].as_i64().unwrap_or(0),
                    });
                }
            }
            Ok(orders)
        } else {
            Err(PriceError::BrokerError("Failed to fetch Delta orderbook".to_string()))
        }
    }

    async fn trades(&self) -> Result<Vec<Trade>> {
        if self.api_key.is_none() {
            return Ok(Vec::new());
        }

        let path = "/v2/fills";
        let url = format!("{}{}", self.base_url, path);
        let req = self.client.get(&url);
        let req = self.add_auth_headers(req, "GET", path, "");

        let res = req.send().await.map_err(|e| PriceError::Network(e.to_string()))?;
        let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if body["success"].as_bool().unwrap_or(false) {
            let mut trades = Vec::new();
            if let Some(arr) = body["result"].as_array() {
                for t in arr {
                    let side = if t["side"].as_str().unwrap_or("buy") == "buy" { Side::Buy } else { Side::Sell };
                    trades.push(Trade {
                        trade_id: t["id"].as_i64().unwrap_or(0).to_string(),
                        order_id: t["order_id"].as_i64().unwrap_or(0).to_string(),
                        symbol: t["symbol"].as_str().unwrap_or("").to_string(),
                        qty: t["size"].as_str().unwrap_or("0").parse().unwrap_or(0),
                        price: t["price"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0),
                        side,
                        timestamp: t["created_at"].as_i64().unwrap_or(0),
                    });
                }
            }
            Ok(trades)
        } else {
            Err(PriceError::BrokerError("Failed to fetch Delta trades".to_string()))
        }
    }

    async fn quotes(&self, symbols: Vec<String>) -> Result<Vec<Quote>> {
        let mut quotes = Vec::new();
        for sym in symbols {
            let url = format!("{}/v2/tickers/{}", self.base_url, sym);
            let res = self.client.get(&url).send().await.map_err(|e| PriceError::Network(e.to_string()))?;
            let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

            if body["success"].as_bool().unwrap_or(false) {
                let data = &body["result"];
                let last: f64 = data["close"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                let bid: f64 = data["best_bid"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                let ask: f64 = data["best_ask"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                let volume: u64 = data["volume_24h"].as_str().unwrap_or("0").parse().unwrap_or(0);
                let oi: u64 = data["open_interest"].as_str().unwrap_or("0").parse().unwrap_or(0);
                
                quotes.push(Quote {
                    symbol: sym,
                    last_price: last,
                    bid,
                    ask,
                    volume,
                    oi,
                    prev_close: last - data["price_change_24h"].as_str().unwrap_or("0.0").parse::<f64>().unwrap_or(0.0),
                });
            } else {
                // Return dummy quote if fetching fails or symbol doesn't exist
                quotes.push(Quote {
                    symbol: sym,
                    last_price: 500.0,
                    bid: 499.9,
                    ask: 500.1,
                    volume: 50000,
                    oi: 2000000,
                    prev_close: 495.0,
                });
            }
        }
        Ok(quotes)
    }

    async fn history(&self, request: HistoryRequest) -> Result<CandleSeries> {
        let url = format!("{}/v2/history/candles", self.base_url);
        let res = self.client.get(&url)
            .query(&[
                ("symbol", &request.symbol),
                ("resolution", &request.resolution),
                ("start_time", &request.range_from),
                ("end_time", &request.range_to),
            ])
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json().await.map_err(|e| PriceError::Network(e.to_string()))?;

        if body["success"].as_bool().unwrap_or(false) {
            let mut candles = Vec::new();
            if let Some(arr) = body["result"].as_array() {
                for item in arr {
                    let t = item["time"].as_i64().unwrap_or(0) as f64;
                    let o = item["open"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                    let h = item["high"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                    let l = item["low"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                    let c = item["close"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                    let v = item["volume"].as_str().unwrap_or("0.0").parse().unwrap_or(0.0);
                    candles.push(vec![t, o, h, l, c, v]);
                }
            }
            Ok(CandleSeries { candles })
        } else {
            Err(PriceError::BrokerError("Failed to fetch Delta history".to_string()))
        }
    }

    fn supports_leverage(&self) -> bool {
        true
    }

    fn broker_type(&self) -> BrokerType {
        BrokerType::DeltaExchange
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
