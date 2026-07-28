use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;
use tokio::time;
use chrono::Utc;
use futures::StreamExt;
use tracing::{info, warn, error, debug, Level};
use tracing_subscriber::FmtSubscriber;
use dotenvy::dotenv;

use price_core::TickData;
use price_broker::Broker;
use price_risk::RiskEngine;
use price_strategy::{OpportunityEngine, ExitEvaluator};
use price_execution::ExecutionOrchestrator;

#[derive(serde::Deserialize, Debug)]
struct WsTick {
    symbol: String,
    price: f64,
    volume: u64,
    oi: u64,
    timestamp: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize logging
    dotenv().ok();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting PRICE Quantitative Trading Worker...");

    // 2. Load configurations
    let python_broker_url = std::env::var("PYTHON_BROKER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8001".to_string());
    let daily_loss_limit = std::env::var("DAILY_LOSS_LIMIT")
        .unwrap_or_else(|_| "2000.0".to_string())
        .parse::<f64>()?;
    let max_trades = std::env::var("DAILY_MAX_TRADES")
        .unwrap_or_else(|_| "3".to_string())
        .parse::<i32>()?;


    // 3. Setup Broker
    let broker: Arc<dyn Broker> = if let Ok(delta_key) = std::env::var("DELTA_API_KEY") {
        let delta_secret = std::env::var("DELTA_API_SECRET").unwrap_or_default();
        let delta_url = std::env::var("DELTA_BASE_URL").unwrap_or_else(|_| "https://api.delta.exchange".to_string());
        info!("Initializing DeltaExchangeClient with API Key: {}...", delta_key);
        Arc::new(price_broker::DeltaExchangeClient::new(&delta_url, Some(delta_key), Some(delta_secret)))
    } else {
        info!("Initializing HybridBroker (simultaneous Live + Paper trading)...");
        Arc::new(price_broker::HybridBroker::new(&python_broker_url, 10000.0))
    };
    // Test login
    match broker.login().await {
        Ok(token) => info!("Successfully authenticated hybrid live client. Token length: {}", token.len()),
        Err(e) => warn!("Hybrid live client login failed: {:?}. Proceeding with mock/simulated fallbacks.", e),
    }

    // 4. Initialize Engines & TimescaleClient
    let database_url = std::env::var("DATABASE_URL").ok();
    let timescale_client = if let Some(ref db_url) = database_url {
        match price_timeseries::TimescaleClient::new(db_url).await {
            Ok(client) => {
                let _ = client.init_db().await;
                info!("TimescaleDB client successfully connected for trade memory logging.");
                Some(client)
            }
            Err(e) => {
                warn!("Failed to initialize TimescaleDB client: {:?}. Proceeding without persistent market memory.", e);
                None
            }
        }
    } else {
        None
    };

    let risk_engine = RiskEngine::new(max_trades, daily_loss_limit);
    let opportunity_engine = OpportunityEngine::new(85.0, 75.0); // Thresholds
    let exit_evaluator = ExitEvaluator::new(1.5, 0.8, 15); // Target ATR mult, SL ATR mult, max hold 15 mins

    let mut orchestrator = ExecutionOrchestrator::new(
        broker.clone(),
        risk_engine,
        opportunity_engine,
        exit_evaluator,
        timescale_client.clone(),
    );

    // Test broker details on startup
    if let Ok(profile) = broker.profile().await {
        info!("Profile: Welcome {} ({})", profile.name, profile.fy_id);
    }
    if let Ok(funds) = broker.funds().await {
        info!("Initial Funds -> Limit: {}, Available: {}", funds.limit_amount, funds.available_balance);
    }

    // Define 50 constituent stock symbols and weightages
    let weights: HashMap<&str, f64> = [
        ("NSE:RELIANCE-EQ", 0.0908), ("NSE:BHARTIARTL-EQ", 0.0622), ("NSE:HDFCBANK-EQ", 0.0600), ("NSE:ICICIBANK-EQ", 0.0540),
        ("NSE:SBIN-EQ", 0.0492), ("NSE:TCS-EQ", 0.0428), ("NSE:BAJFINANCE-EQ", 0.0331), ("NSE:LT-EQ", 0.0273), ("NSE:HINDUNILVR-EQ", 0.0264),
        ("NSE:SUNPHARMA-EQ", 0.0244), ("NSE:MARUTI-EQ", 0.0222), ("NSE:INFY-EQ", 0.0222), ("NSE:TITAN-EQ", 0.0218), ("NSE:ADANIENT-EQ", 0.0215),
        ("NSE:ADANIPORTS-EQ", 0.0214), ("NSE:M&M-EQ", 0.0206), ("NSE:KOTAKBANK-EQ", 0.0201), ("NSE:AXISBANK-EQ", 0.0200), ("NSE:ITC-EQ", 0.0186),
        ("NSE:ULTRACEMCO-EQ", 0.0183), ("NSE:HCLTECH-EQ", 0.0181), ("NSE:NTPC-EQ", 0.0177), ("NSE:ONGC-EQ", 0.0164), ("NSE:BAJAJ-AUTO-EQ", 0.0160),
        ("NSE:JSWSTEEL-EQ", 0.0159), ("NSE:BAJAJFINSV-EQ", 0.0158), ("NSE:BEL-EQ", 0.0155), ("NSE:ETERNAL-EQ", 0.0142), ("NSE:POWERGRID-EQ", 0.0141),
        ("NSE:COALINDIA-EQ", 0.0138), ("NSE:ASIANPAINT-EQ", 0.0133), ("NSE:SHRIRAMFIN-EQ", 0.0124), ("NSE:TATASTEEL-EQ", 0.0120), ("NSE:HINDALCO-EQ", 0.0111),
        ("NSE:GRASIM-EQ", 0.0110), ("NSE:EICHERMOT-EQ", 0.0110), ("NSE:INDIGO-EQ", 0.0101), ("NSE:SBILIFE-EQ", 0.0098), ("NSE:WIPRO-EQ", 0.0092),
        ("NSE:JIOFIN-EQ", 0.0081), ("NSE:TRENT-EQ", 0.0081), ("NSE:TECHM-EQ", 0.0080), ("NSE:APOLLOHOSP-EQ", 0.0066), ("NSE:HDFCLIFE-EQ", 0.0063),
        ("NSE:TMPV-EQ", 0.0063), ("NSE:CIPLA-EQ", 0.0060), ("NSE:TATACONSUM-EQ", 0.0057), ("NSE:MAXHEALTH-EQ", 0.0055), ("NSE:DRREDDY-EQ", 0.0050),
        ("NSE:NESTLEIND-EQ", 0.0049)
    ].iter().cloned().collect();

    // Map to track stock details: symbol -> (ltp, prev_close)
    let mut stock_prices: HashMap<String, (f64, f64)> = HashMap::new();

    info!("Starting live WebSocket ingestion stream...");

    // Fetch quotes for initial prev_close cache
    let http_client = reqwest::Client::new();
    let symbols: Vec<String> = weights.keys().map(|&s| s.to_string()).collect();
    info!("Fetching initial quotes for {} constituent stocks...", symbols.len());
    
    match http_client.post(&format!("{}/quotes", python_broker_url))
        .json(&symbols)
        .send()
        .await {
        Ok(res) => {
            if let Ok(json_res) = res.json::<serde_json::Value>().await {
                if let Some(data) = json_res.get("data").and_then(|d| d.as_object()) {
                    for (sym, quote) in data {
                        let lp = quote.get("last_price").and_then(|p| p.as_f64()).unwrap_or(0.0);
                        let pc = quote.get("prev_close").and_then(|p| p.as_f64()).unwrap_or(lp);
                        stock_prices.insert(sym.clone(), (lp, pc));
                    }
                    info!("Quotes initialized successfully. Total cached: {}", stock_prices.len());
                }
            }
        }
        Err(e) => {
            warn!("Could not fetch stock quotes: {:?}. Base prices will be set dynamically.", e);
        }
    }

    // Active Option symbols tracking (strike-based)
    let mut last_subscribed_strike: Option<f64> = None;

    // Connect to local python-broker websocket
    let ws_url = python_broker_url.replace("http://", "ws://") + "/ws";
    let mut retry_delay = Duration::from_secs(2);

    loop {
        info!("Establishing connection to WebSocket bridge at {}...", ws_url);
        match tokio_tungstenite::connect_async(&ws_url).await {
            Ok((ws_stream, _)) => {
                info!("WebSocket connection established. Streaming live ticks.");
                retry_delay = Duration::from_secs(2); // Reset backoff delay

                let (_, mut read) = ws_stream.split();
                let mut step = 0;
                let mut last_status_send = std::time::Instant::now();
                let server_url = std::env::var("SERVER_URL").unwrap_or_else(|_| "http://127.0.0.1:8000".to_string());

                let mut candle_aggregators: HashMap<String, price_indicators::CandleAggregator> = HashMap::new();
                let mut crypto_prices: HashMap<String, f64> = HashMap::new();

                while let Some(message) = read.next().await {
                    match message {
                        Ok(msg) => {
                            if let Ok(text) = msg.into_text() {
                                if let Ok(ws_tick) = serde_json::from_str::<WsTick>(&text) {
                                    step += 1;
                                    
                                    // 1. Process Crypto Ticks & Price Memory
                                    if ws_tick.symbol.contains("BTC") {
                                        crypto_prices.insert("BTC".to_string(), ws_tick.price);
                                    } else if ws_tick.symbol.contains("ETH") {
                                        crypto_prices.insert("ETH".to_string(), ws_tick.price);
                                    } else if ws_tick.symbol.contains("SOL") {
                                        crypto_prices.insert("SOL".to_string(), ws_tick.price);
                                    }

                                    // 2. Real-Time Tick-by-Tick OHLC Candle Aggregation
                                    let tick_time = chrono::DateTime::from_timestamp(ws_tick.timestamp, 0)
                                        .unwrap_or_else(Utc::now);
                                    let tick = TickData {
                                        symbol: ws_tick.symbol.clone(),
                                        price: ws_tick.price,
                                        volume: ws_tick.volume,
                                        oi: ws_tick.oi,
                                        timestamp: tick_time,
                                    };

                                    let agg = candle_aggregators.entry(ws_tick.symbol.clone())
                                        .or_insert_with(price_indicators::CandleAggregator::new);
                                    if let Some(closed_candle) = agg.ingest_tick(&tick) {
                                        info!("1m OHLC Candle Closed for {}: O={:.2} H={:.2} L={:.2} C={:.2} V={}",
                                            ws_tick.symbol, closed_candle.open, closed_candle.high, closed_candle.low, closed_candle.close, closed_candle.volume);
                                        if let Some(ref db) = timescale_client {
                                            let db_clone = db.clone();
                                            let sym_clone = ws_tick.symbol.clone();
                                            let exchange = if sym_clone.contains("USD") { "DELTA" } else { "NSE" };
                                            tokio::spawn(async move {
                                                let _ = db_clone.insert_candles(&sym_clone, exchange, "1m", &[closed_candle]).await;
                                            });
                                        }
                                    }

                                    // 3. Process Stock Constituent updates
                                    if weights.contains_key(ws_tick.symbol.as_str()) {
                                        let prev_record = stock_prices.get(&ws_tick.symbol);
                                        let prev_close = match prev_record {
                                            Some(&(_, pc)) => pc,
                                            None => ws_tick.price // Use first price as prev_close base
                                        };
                                        stock_prices.insert(ws_tick.symbol.clone(), (ws_tick.price, prev_close));

                                        // Re-calculate the weighted Nifty constituent delta
                                        let mut weighted_delta = 0.0;
                                        for (&sym, &weight) in &weights {
                                            if let Some(&(ltp, pc)) = stock_prices.get(sym) {
                                                if pc > 0.0 {
                                                    weighted_delta += weight * (ltp - pc) / pc;
                                                }
                                            }
                                        }
                                        orchestrator.update_weighted_delta(weighted_delta);
                                        debug!("Stock constituent tick: {}. Price: {}. New Weighted Delta: {:.6}", 
                                            ws_tick.symbol, ws_tick.price, weighted_delta);
                                    }

                                    // 4. Perform Dynamic Strike subscription adjustments
                                    if ws_tick.symbol == "NSE:NIFTY50-INDEX" {
                                        let strike = (ws_tick.price / 50.0).round() * 50.0;
                                        if last_subscribed_strike != Some(strike) {
                                            let tick_time = chrono::DateTime::from_timestamp(ws_tick.timestamp, 0)
                                                .unwrap_or_else(Utc::now);
                                            let tick_date = tick_time.naive_utc().date();
                                            let holidays = price_core::get_nse_holidays_2026();
                                            let expiry_date = price_core::calculate_nifty_expiry(tick_date, &holidays);
                                            let suffix = price_core::format_fyers_expiry_suffix(expiry_date);
                                            let current_expiry_prefix = format!("NSE:NIFTY{}", suffix);

                                            let new_ce = format!("{}{:.0}CE", current_expiry_prefix, strike);
                                            let new_pe = format!("{}{:.0}PE", current_expiry_prefix, strike);
                                            
                                            let client_clone = http_client.clone();
                                            let url_clone = python_broker_url.clone();
                                            
                                            tokio::spawn(async move {
                                                info!("ATM Strike shift detected! Dynamically subscribing to: CE={}, PE={}", new_ce, new_pe);
                                                let _ = client_clone.post(&format!("{}/subscribe", url_clone))
                                                    .json(&serde_json::json!({
                                                        "symbols": vec![new_ce, new_pe]
                                                    }))
                                                    .send()
                                                    .await;
                                            });
                                            
                                            last_subscribed_strike = Some(strike);
                                        }
                                    }

                                    // 5. Filter and pipe active option contracts + index/VIX ticks to Orchestrator
                                    let tick_time = chrono::DateTime::from_timestamp(ws_tick.timestamp, 0)
                                        .unwrap_or_else(Utc::now);
                                    let tick_date = tick_time.naive_utc().date();
                                    let holidays = price_core::get_nse_holidays_2026();
                                    let expiry_date = price_core::calculate_nifty_expiry(tick_date, &holidays);
                                    let suffix = price_core::format_fyers_expiry_suffix(expiry_date);
                                    let current_expiry_prefix = format!("NSE:NIFTY{}", suffix);

                                    let is_active_option = if let Some(strike) = last_subscribed_strike {
                                        let active_ce = format!("{}{:.0}CE", current_expiry_prefix, strike);
                                        let active_pe = format!("{}{:.0}PE", current_expiry_prefix, strike);
                                        ws_tick.symbol == active_ce || ws_tick.symbol == active_pe
                                    } else {
                                        false
                                    };

                                    if ws_tick.symbol == "NSE:NIFTY50-INDEX" || ws_tick.symbol == "NSE:INDIAVIX-INDEX" || is_active_option {
                                        match orchestrator.ingest_tick(tick).await {
                                            Ok(events) => {
                                                for event in events {
                                                    debug!("Engine Event: {:?}", event);
                                                }
                                            }
                                            Err(e) => {
                                                error!("Live ingestion step {} error: {:?}", step, e);
                                            }
                                        }

                                    }

                                    // Send status update to server once per second on any incoming tick
                                    if last_status_send.elapsed() >= Duration::from_millis(1000) {
                                        last_status_send = std::time::Instant::now();
                                        
                                        let (prob, conf) = if let Some(ref opt) = orchestrator.last_opportunity {
                                            (opt.probability, opt.confidence)
                                        } else {
                                            (0.0, 0.0)
                                        };
                                        
                                        let decision = format!("{:?}", orchestrator.last_decision.unwrap_or(price_strategy::Decision::Wait));
                                        let quality = orchestrator.last_quality.as_ref().map(|q| q.total).unwrap_or(0.0);
                                        let target = orchestrator.last_target_option.clone().unwrap_or_else(|| "--".to_string());

                                        let payload = serde_json::json!({
                                            "nifty_price": orchestrator.current_nifty_spot,
                                            "vix": orchestrator.current_vix,
                                            "ml_confidence": orchestrator.current_ml_confidence,
                                            "opportunity_confidence": conf,
                                            "opportunity_probability": prob,
                                            "decision": decision,
                                            "quality_score": quality,
                                            "target_option": target,
                                            "btc_price": crypto_prices.get("BTC").copied().unwrap_or(0.0),
                                            "eth_price": crypto_prices.get("ETH").copied().unwrap_or(0.0),
                                            "sol_price": crypto_prices.get("SOL").copied().unwrap_or(0.0),
                                            "timestamp": chrono::Utc::now().to_rfc3339(),
                                        });

                                        let client_clone = http_client.clone();
                                        let url_clone = format!("{}/live-status", server_url);
                                        tokio::spawn(async move {
                                            let _ = client_clone.post(&url_clone)
                                                .json(&payload)
                                                .send()
                                                .await;
                                        });
                                    }

                                    // Log status updates periodically
                                    if step % 100 == 0 {
                                        let (trades, pnl) = orchestrator.get_risk_status();
                                        info!(
                                            "[LIVE STATUS] Ticks Ingested: {} | Active Trades: {} | Daily PnL: {:.2} | Position: {}",
                                            step,
                                            trades,
                                            pnl,
                                            if orchestrator.active_position().is_some() { "YES" } else { "NO" }
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Websocket frame read error: {:?}. Reconnecting...", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                error!("WebSocket connection failed: {:?}. Retrying in {:?}...", e, retry_delay);
                time::sleep(retry_delay).await;
                // Exponential backoff
                retry_delay = (retry_delay * 2).min(Duration::from_secs(60));
            }
        }
    }

    info!("Shutting down PRICE Quantitative Trading Worker...");
    Ok(())
}
