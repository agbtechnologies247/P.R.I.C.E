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
use price_broker::{Broker, PaperBroker, FyersClient};
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
    let use_simulated = std::env::var("USE_SIMULATED_FEED")
        .unwrap_or_else(|_| "true".to_string()) == "true";
    let python_broker_url = std::env::var("PYTHON_BROKER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8001".to_string());
    let daily_loss_limit = std::env::var("DAILY_LOSS_LIMIT")
        .unwrap_or_else(|_| "2000.0".to_string())
        .parse::<f64>()?;
    let max_trades = std::env::var("DAILY_MAX_TRADES")
        .unwrap_or_else(|_| "3".to_string())
        .parse::<i32>()?;

    let ce_symbol = std::env::var("ACTIVE_CE_SYMBOL").unwrap_or_else(|_| "NSE:NIFTY2673024100CE".to_string());
    let pe_symbol = std::env::var("ACTIVE_PE_SYMBOL").unwrap_or_else(|_| "NSE:NIFTY2673024100PE".to_string());

    // 3. Setup Broker abstraction
    let broker: Arc<dyn Broker> = if use_simulated {
        info!("Using high-fidelity in-memory PaperBroker...");
        Arc::new(PaperBroker::new(10000.0))
    } else {
        info!("Connecting to Python Broker Bridge at {}...", python_broker_url);
        let client = FyersClient::new(&python_broker_url);
        // Test connection
        match client.login().await {
            Ok(token) => info!("Successfully authenticated via Python Bridge. Token length: {}", token.len()),
            Err(e) => warn!("Python bridge login failed: {:?}. Proceeding with offline fallback.", e),
        }
        Arc::new(client)
    };

    // 4. Initialize Engines
    let risk_engine = RiskEngine::new(max_trades, daily_loss_limit);
    let opportunity_engine = OpportunityEngine::new(85.0, 75.0); // Thresholds
    let exit_evaluator = ExitEvaluator::new(1.5, 0.8, 15); // Target ATR mult, SL ATR mult, max hold 15 mins

    let mut orchestrator = ExecutionOrchestrator::new(
        broker.clone(),
        risk_engine,
        opportunity_engine,
        exit_evaluator,
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

    // 5. Ingestion Loop (Simulated OR WebSocket live data feed)
    if use_simulated {
        info!("Starting tick ingestion simulation loop...");
        let mut interval = time::interval(Duration::from_millis(500));
        let mut step = 0;
        let mut simulated_price = 500.0;
        
        loop {
            interval.tick().await;
            step += 1;
            let change = (step as f64 * 0.015).sin() * 0.5 + 0.15;
            simulated_price += change;

            let tick = TickData {
                symbol: "NSE:NIFTYBANK-ATM-CE".to_string(),
                price: simulated_price,
                volume: 12000 + (step * 10) % 5000,
                oi: 1500000 + (step * 100) % 20000,
                timestamp: Utc::now(),
            };

            match orchestrator.ingest_tick(tick).await {
                Ok(events) => {
                    for event in events {
                        debug!("Engine Event: {:?}", event);
                    }
                }
                Err(e) => {
                    error!("Error processing tick step {}: {:?}", step, e);
                }
            }

            if step % 20 == 0 {
                let (trades, pnl) = orchestrator.get_risk_status();
                info!(
                    "[STATUS REPORT] Step: {} | Price: {:.2} | Trades Today: {} | Daily PnL: {:.2} | Active Position: {}",
                    step,
                    simulated_price,
                    trades,
                    pnl,
                    if orchestrator.active_position().is_some() { "YES" } else { "NO" }
                );
            }

            if step >= 200 {
                info!("Ingested 200 simulation steps. Worker execution completed safely.");
                break;
            }
        }
    } else {
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

        // Subscribe to our active options contracts
        info!("Subscribing to active Call ({}) and Put ({}) option contracts...", ce_symbol, pe_symbol);
        let sub_res = http_client.post(&format!("{}/subscribe", python_broker_url))
            .json(&serde_json::json!({
                "symbols": vec![ce_symbol.clone(), pe_symbol.clone()]
            }))
            .send()
            .await;
        if let Err(e) = sub_res {
            warn!("Failed to subscribe options: {:?}", e);
        }

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

                    while let Some(message) = read.next().await {
                        match message {
                            Ok(msg) => {
                                if let Ok(text) = msg.into_text() {
                                    if let Ok(ws_tick) = serde_json::from_str::<WsTick>(&text) {
                                        step += 1;
                                        
                                        // 1. Process Stock Constituent updates
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
                                            debug!("Stock constituent tick: {}. Price: {}. New Weighted Delta: {:.6}", 
                                                ws_tick.symbol, ws_tick.price, weighted_delta);
                                        }

                                        // 2. Process Nifty Index and Option ticks to pipe to Execution Orchestrator
                                        if ws_tick.symbol == "NSE:NIFTY50-INDEX" || ws_tick.symbol == ce_symbol || ws_tick.symbol == pe_symbol {
                                            let tick = TickData {
                                                symbol: ws_tick.symbol.clone(),
                                                price: ws_tick.price,
                                                volume: ws_tick.volume,
                                                oi: ws_tick.oi,
                                                timestamp: Utc::now(),
                                            };
                                            
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
    }

    info!("Shutting down PRICE Quantitative Trading Worker...");
    Ok(())
}
