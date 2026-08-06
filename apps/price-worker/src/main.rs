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

    let server_url = std::env::var("SERVER_URL").unwrap_or_else(|_| "http://127.0.0.1:8000".to_string());
    let orchestrator_arc = Arc::new(tokio::sync::Mutex::new(orchestrator));
    let crypto_prices_arc = Arc::new(tokio::sync::Mutex::new(HashMap::<String, f64>::new()));

    // 5. Spawn 1-second continuous live status heartbeat task to server
    {
        let orch_clone = orchestrator_arc.clone();
        let crypto_clone = crypto_prices_arc.clone();
        let client_clone = http_client.clone();
        let url_clone = format!("{}/live-status", server_url);
        
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let (nifty, vix, ml, prob, conf, decision, quality, target) = {
                    let orch = orch_clone.lock().await;
                    let (prob, conf) = if let Some(ref opt) = orch.last_opportunity {
                        (opt.probability, opt.confidence)
                    } else {
                        (0.0, 0.0)
                    };
                    let decision = format!("{:?}", orch.last_decision.unwrap_or(price_strategy::Decision::Wait));
                    let quality = orch.last_quality.as_ref().map(|q| q.total).unwrap_or(0.0);
                    let target = orch.last_target_option.clone().unwrap_or_else(|| "--".to_string());
                    (orch.current_nifty_spot, orch.current_vix, orch.current_ml_confidence, prob, conf, decision, quality, target)
                };

                let (btc_p, eth_p, sol_p) = {
                    let cp = crypto_clone.lock().await;
                    (
                        cp.get("BTC").copied().unwrap_or(0.0),
                        cp.get("ETH").copied().unwrap_or(0.0),
                        cp.get("SOL").copied().unwrap_or(0.0),
                    )
                };

                let payload = serde_json::json!({
                    "nifty_price": nifty,
                    "vix": vix,
                    "ml_confidence": ml,
                    "opportunity_confidence": conf,
                    "opportunity_probability": prob,
                    "decision": decision,
                    "quality_score": quality,
                    "target_option": target,
                    "btc_price": btc_p,
                    "eth_price": eth_p,
                    "sol_price": sol_p,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                });

                let _ = client_clone.post(&url_clone)
                    .json(&payload)
                    .send()
                    .await;
            }
        });
    }

    // 6. Spawn Delta Exchange live ticker polling task (2-seconds interval)
    {
        let crypto_clone = crypto_prices_arc.clone();
        let delta_base_url = std::env::var("DELTA_BASE_URL")
            .unwrap_or_else(|_| price_broker::DELTA_INDIA_PROD_URL.to_string());
        
        tokio::spawn(async move {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("User-Agent", reqwest::header::HeaderValue::from_static("price-engine-rust"));
            headers.insert("Accept", reqwest::header::HeaderValue::from_static("application/json"));
            let delta_client = reqwest::Client::builder()
                .default_headers(headers)
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new());

            let mut interval = time::interval(Duration::from_secs(2));
            let urls = vec![
                format!("{}/v2/tickers", delta_base_url.trim_end_matches('/')),
                "https://api.india.delta.exchange/v2/tickers".to_string(),
                "https://api.delta.exchange/v2/tickers".to_string(),
            ];

            let extract_price = |v: &serde_json::Value| -> Option<f64> {
                if let Some(s) = v.as_str() {
                    s.parse::<f64>().ok()
                } else if let Some(n) = v.as_f64() {
                    Some(n)
                } else if let Some(i) = v.as_i64() {
                    Some(i as f64)
                } else {
                    None
                }
            };

            loop {
                interval.tick().await;
                for url in &urls {
                    if let Ok(res) = delta_client.get(url).send().await {
                        if let Ok(json_val) = res.json::<serde_json::Value>().await {
                            if let Some(result_arr) = json_val.get("result").and_then(|r| r.as_array()) {
                                let mut cp = crypto_clone.lock().await;
                                for t in result_arr {
                                    let sym = t.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
                                    let c_type = t.get("contract_type").and_then(|c| c.as_str()).unwrap_or("");
                                    let u_asset = t.get("underlying_asset_symbol").and_then(|a| a.as_str()).unwrap_or("");
                                    let price = t.get("mark_price")
                                        .and_then(&extract_price)
                                        .or_else(|| t.get("close").and_then(&extract_price))
                                        .or_else(|| t.get("spot_price").and_then(&extract_price))
                                        .or_else(|| t.get("last_price").and_then(&extract_price));

                                    if let Some(p) = price {
                                        if p > 0.0 {
                                            let s_upper = sym.to_uppercase();
                                            let is_perp = c_type == "perpetual_futures" || c_type.is_empty();
                                            let is_btc = (is_perp && u_asset == "BTC") || s_upper == "BTCUSDT" || s_upper == "BTCUSD" || s_upper == "BTCUSD_PERP";
                                            let is_eth = (is_perp && u_asset == "ETH") || s_upper == "ETHUSDT" || s_upper == "ETHUSD" || s_upper == "ETHUSD_PERP";
                                            let is_sol = (is_perp && u_asset == "SOL") || s_upper == "SOLUSDT" || s_upper == "SOLUSD" || s_upper == "SOLUSD_PERP";

                                            if is_btc {
                                                cp.insert("BTC".to_string(), p);
                                            } else if is_eth {
                                                cp.insert("ETH".to_string(), p);
                                            } else if is_sol {
                                                cp.insert("SOL".to_string(), p);
                                            }
                                        }
                                    }
                                }
                                if !cp.is_empty() {
                                    break; // Successfully updated from current URL
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // ──────────────────────────────────────────────────────────────────────────
    // 7. Delta Exchange 5-Minute Swing Trading Loop
    //    - Runs continuously 24/7 (crypto never closes)
    //    - Symbols: BTCUSD_PERP (200x), ETHUSD_PERP (200x), SOLUSD_PERP (100x)
    //    - Position margin: 100 USDT per trade
    //    - Signal: EMA-9 / EMA-21 crossover on 5m candles + momentum confirm
    //    - Both LIVE (DeltaExchangeClient) and PAPER run simultaneously
    // ──────────────────────────────────────────────────────────────────────────
    {
        let delta_key   = std::env::var("DELTA_API_KEY").ok();
        let delta_sec   = std::env::var("DELTA_API_SECRET").ok();
        let delta_url   = std::env::var("DELTA_BASE_URL")
            .unwrap_or_else(|_| price_broker::DELTA_INDIA_PROD_URL.to_string());
        let margin_usdt: f64 = std::env::var("DELTA_POSITION_MARGIN_USDT")
            .unwrap_or_else(|_| "100.0".to_string())
            .parse().unwrap_or(100.0);

        // Symbols config: (symbol, max_leverage)
        let delta_symbols: Vec<(&str, u32)> = vec![
            ("BTCUSD_PERP", 200),
            ("ETHUSD_PERP", 200),
            ("SOLUSD_PERP", 100),
        ];

        if delta_key.is_some() {
            let live_client = std::sync::Arc::new(price_broker::DeltaExchangeClient::new(
                &delta_url,
                delta_key.clone(),
                delta_sec.clone(),
            ));
            // Paper client (no keys)
            let paper_client = std::sync::Arc::new(price_broker::DeltaExchangeClient::new(
                &delta_url, None, None,
            ));

            info!("[DeltaLoop] Starting 5m swing trading loop — symbols: BTCUSD_PERP, ETHUSD_PERP, SOLUSD_PERP");
            info!("[DeltaLoop] Margin per trade: ${:.0} USDT | Both LIVE + PAPER running", margin_usdt);

            // Heartbeat setup — create deadman switch on startup
            {
                let hb_client = live_client.clone();
                tokio::spawn(async move {
                    // 300s interval = 5 min. Our cron also acks every 5 min as backup.
                    match hb_client.create_heartbeat(300).await {
                        Ok(hb) => info!("[DeltaLoop] Deadman heartbeat created: id={} interval={}s", hb.id, hb.interval_secs),
                        Err(e) => warn!("[DeltaLoop] Heartbeat creation failed (will retry): {:?}", e),
                    }
                    // Ack loop: every 240s (4 min) to stay within the 5-min window
                    let mut ack_interval = time::interval(Duration::from_secs(240));
                    loop {
                        ack_interval.tick().await;
                        if let Err(e) = hb_client.ack_heartbeat().await {
                            warn!("[DeltaLoop] Heartbeat ACK failed: {:?}", e);
                        }
                    }
                });
            }

            // Spawn one trading loop per symbol
            for (symbol, max_lev) in delta_symbols {
                let sym = symbol.to_string();
                let lc  = live_client.clone();
                let pc  = paper_client.clone();

                tokio::spawn(async move {
                    // Stagger symbol starts by a few seconds to avoid burst
                    let stagger_secs = match sym.as_str() {
                        "BTCUSD_PERP" => 0,
                        "ETHUSD_PERP" => 10,
                        _             => 20,
                    };
                    time::sleep(Duration::from_secs(stagger_secs)).await;

                    info!("[DeltaLoop][{}] Starting 5m trading loop | leverage={}x | margin=${:.0}", sym, max_lev, margin_usdt);

                    // Set leverage on startup
                    let prod_id = lc.resolve_product_id(&sym).await;
                    if let Err(e) = lc.set_leverage(prod_id, max_lev).await {
                        warn!("[DeltaLoop][{}] Could not set leverage to {}x: {:?}", sym, max_lev, e);
                    } else {
                        info!("[DeltaLoop][{}] Leverage set to {}x (product_id={})", sym, max_lev, prod_id);
                    }

                    // Wait for the next 5-minute boundary so all symbols are in sync
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
                    let secs_until_next_5m = 300 - (now_secs % 300);
                    info!("[DeltaLoop][{}] Syncing to 5m boundary — waiting {}s", sym, secs_until_next_5m);
                    time::sleep(Duration::from_secs(secs_until_next_5m)).await;

                    let mut loop_interval = time::interval(Duration::from_secs(300)); // 5 minutes
                    let mut candle_buffer: Vec<price_broker::Candle5m> = Vec::new();

                    loop {
                        loop_interval.tick().await;
                        let now = chrono::Utc::now();
                        info!("[DeltaLoop][{}] === 5m tick at {} ===", sym, now.format("%H:%M:%S"));

                        // ── 1. Fetch latest 20 candles ────────────────────────
                        let candles = match lc.get_historical_candles_5m(&sym, 20).await {
                            Ok(c) if c.len() >= 10 => c,
                            Ok(c) => {
                                warn!("[DeltaLoop][{}] Not enough candles ({}/10), skipping", sym, c.len());
                                continue;
                            }
                            Err(e) => {
                                warn!("[DeltaLoop][{}] Failed to fetch candles: {:?}", sym, e);
                                continue;
                            }
                        };

                        // Append new candles to rolling buffer (keep last 50)
                        candle_buffer.extend_from_slice(&candles);
                        if candle_buffer.len() > 50 {
                            candle_buffer.drain(..candle_buffer.len() - 50);
                        }

                        let n = candle_buffer.len();
                        let closes: Vec<f64> = candle_buffer.iter().map(|c| c.close).collect();
                        let current_price = *closes.last().unwrap();

                        // ── 2. Compute EMA-9 and EMA-21 ──────────────────────
                        fn ema(prices: &[f64], period: usize) -> f64 {
                            if prices.len() < period { return *prices.last().unwrap_or(&0.0); }
                            let k = 2.0 / (period as f64 + 1.0);
                            let mut ema_val = prices[..period].iter().sum::<f64>() / period as f64;
                            for &p in &prices[period..] {
                                ema_val = p * k + ema_val * (1.0 - k);
                            }
                            ema_val
                        }

                        let ema9_now  = ema(&closes, 9);
                        let ema21_now = ema(&closes, 21);
                        // Previous bar EMAs for crossover detection
                        let ema9_prev  = if n > 2 { ema(&closes[..n-1], 9)  } else { ema9_now };
                        let ema21_prev = if n > 2 { ema(&closes[..n-1], 21) } else { ema21_now };

                        let bullish_cross = ema9_prev <= ema21_prev && ema9_now > ema21_now;
                        let bearish_cross = ema9_prev >= ema21_prev && ema9_now < ema21_now;
                        let trend_up   = ema9_now > ema21_now;
                        let trend_down = ema9_now < ema21_now;

                        // Momentum: last candle bullish/bearish
                        let last_candle = candle_buffer.last().unwrap();
                        let momentum_bull = last_candle.close > last_candle.open;
                        let momentum_bear = last_candle.close < last_candle.open;

                        // ATR-based stop-loss (14-period, simplified)
                        let atr: f64 = if n >= 14 {
                            candle_buffer[n-14..].windows(2).map(|w| {
                                let tr = (w[1].high - w[1].low)
                                    .max((w[1].high - w[0].close).abs())
                                    .max((w[1].low  - w[0].close).abs());
                                tr
                            }).sum::<f64>() / 13.0
                        } else {
                            current_price * 0.002 // Fallback: 0.2% ATR
                        };
                        // SL = 1.5x ATR away; TP = 2x ATR away
                        let sl_dist = atr * 1.5;
                        let tp_dist = atr * 2.0;

                        info!("[DeltaLoop][{}] Price={:.2} EMA9={:.2} EMA21={:.2} ATR={:.2} | Bull={} Bear={}",
                            sym, current_price, ema9_now, ema21_now, atr, bullish_cross, bearish_cross);

                        // ── 3. Check existing open position (LIVE) ──────────
                        let live_position = match lc.get_current_position_for_symbol(&sym).await {
                            Ok(pos) => pos,
                            Err(e) => { warn!("[DeltaLoop][{}] Position query failed: {:?}", sym, e); None }
                        };

                        // ── 4. EXIT LOGIC — close if signal reverses ─────────
                        if let Some((size, ref open_side, entry_price)) = live_position {
                            let should_exit = match open_side {
                                price_broker::Side::Buy  => trend_down || bearish_cross,
                                price_broker::Side::Sell => trend_up   || bullish_cross,
                            };
                            if should_exit {
                                info!("[DeltaLoop][{}] EXIT signal — closing {:?} {}contracts @ {:.2} (entry={:.2})",
                                    sym, open_side, size, current_price, entry_price);
                                let close_side = match open_side {
                                    price_broker::Side::Buy  => price_broker::Side::Sell,
                                    price_broker::Side::Sell => price_broker::Side::Buy,
                                };
                                // Calculate number of contracts based on margin
                                let contracts = ((margin_usdt * max_lev as f64) / current_price).max(1.0) as i32;
                                let close_req = price_broker::OrderRequest {
                                    symbol: sym.clone(),
                                    qty: contracts,
                                    side: close_side,
                                    r#type: 2, // Market order
                                    limit_price: 0.0,
                                    stop_price: 0.0,
                                    leverage: None,
                                    reduce_only: Some(true),
                                    post_only: None,
                                    client_id: None,
                                    time_in_force: None,
                                };
                                match lc.place_order(close_req).await {
                                    Ok(resp) => info!("[DeltaLoop][{}] LIVE EXIT order placed: {}", sym, resp.order_id),
                                    Err(e)   => warn!("[DeltaLoop][{}] LIVE EXIT order failed: {:?}", sym, e),
                                }
                            } else {
                                info!("[DeltaLoop][{}] Holding {:?} position | size={} entry={:.2}", sym, open_side, size, entry_price);
                            }
                            continue; // Don't enter a new position while one is open
                        }

                        // ── 5. ENTRY LOGIC — place bracket order ─────────────
                        let signal = if (bullish_cross || trend_up) && momentum_bull {
                            Some(price_broker::Side::Buy)
                        } else if (bearish_cross || trend_down) && momentum_bear {
                            Some(price_broker::Side::Sell)
                        } else {
                            None
                        };

                        if let Some(entry_side) = signal {
                            let (sl_price, tp_price) = match entry_side {
                                price_broker::Side::Buy => (
                                    (current_price - sl_dist).max(0.01),
                                    current_price + tp_dist,
                                ),
                                price_broker::Side::Sell => (
                                    current_price + sl_dist,
                                    (current_price - tp_dist).max(0.01),
                                ),
                            };
                            // Contracts = (margin_usdt * leverage) / price
                            let contracts = ((margin_usdt * max_lev as f64) / current_price).max(1.0) as i32;

                            let bracket_req = price_broker::BracketOrderRequest {
                                product_id: prod_id,
                                side: entry_side.clone(),
                                size: contracts,
                                order_type: price_broker::OrderType::MarketOrder,
                                limit_price: None,
                                stop_price: None,
                                stop_loss_price: sl_price,
                                take_profit_price: tp_price,
                                trail_amount: None,
                            };

                            info!("[DeltaLoop][{}] ENTRY {:?} {} contracts @ ~{:.2} | SL={:.2} TP={:.2}",
                                sym, entry_side, contracts, current_price, sl_price, tp_price);

                            // Place LIVE bracket order
                            match lc.place_bracket_order(&bracket_req).await {
                                Ok(resp) => info!("[DeltaLoop][{}] LIVE ENTRY order placed: {}", sym, resp.order_id),
                                Err(e)   => warn!("[DeltaLoop][{}] LIVE ENTRY order failed: {:?}", sym, e),
                            }

                            // Mirror as PAPER trade (no real keys)
                            let paper_req = price_broker::OrderRequest {
                                symbol: sym.clone(),
                                qty: contracts,
                                side: entry_side,
                                r#type: 2, // Market order
                                limit_price: 0.0,
                                stop_price: 0.0,
                                leverage: None,
                                reduce_only: None,
                                post_only: None,
                                client_id: None,
                                time_in_force: None,
                            };
                            match pc.place_order(paper_req).await {
                                Ok(resp) => info!("[DeltaLoop][{}] PAPER ENTRY order placed: {}", sym, resp.order_id),
                                Err(e)   => warn!("[DeltaLoop][{}] PAPER ENTRY order failed: {:?}", sym, e),
                            }
                        } else {
                            info!("[DeltaLoop][{}] No signal this 5m bar — waiting", sym);
                        }

                        // ── 6. Post signal state to server dashboard ──────────────
                        let base_sym = if sym.contains("BTC") { "BTC" } else if sym.contains("ETH") { "ETH" } else { "SOL" };
                        let direction_str = if trend_up || bullish_cross { "LONG" } else if trend_down || bearish_cross { "SHORT" } else { "FLAT" };
                        let action_str = if signal.is_some() { "ENTRY" } else if live_position.is_some() { "HOLD" } else { "FLAT" };
                        
                        let server_url = std::env::var("PRICE_SERVER_URL").unwrap_or_else(|_| "http://127.0.0.1:8000".to_string());
                        let signal_payload = serde_json::json!({
                            "symbol": base_sym,
                            "price": current_price,
                            "ema9": ema9_now,
                            "ema21": ema21_now,
                            "atr": atr,
                            "direction": direction_str,
                            "action": action_str,
                            "bull_cross": bullish_cross,
                            "bear_cross": bearish_cross,
                            "leverage": max_lev,
                            "margin_usdt": margin_usdt,
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        });
                        
                        let _ = reqwest::Client::new()
                            .post(format!("{}/live-status/crypto", server_url))
                            .json(&signal_payload)
                            .timeout(std::time::Duration::from_secs(3))
                            .send()
                            .await;
                    } // end loop
                }); // end tokio::spawn per symbol
            }
        } else {
            warn!("[DeltaLoop] DELTA_API_KEY not set — Delta trading loop disabled");
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
                let mut candle_aggregators: HashMap<String, price_indicators::CandleAggregator> = HashMap::new();

                while let Some(message) = read.next().await {
                    match message {
                        Ok(msg) => {
                            if let Ok(text) = msg.into_text() {
                                if let Ok(ws_tick) = serde_json::from_str::<WsTick>(&text) {
                                    step += 1;
                                    
                                    // 1. Process Crypto Ticks & Price Memory
                                    if ws_tick.symbol.contains("BTC") {
                                        crypto_prices_arc.lock().await.insert("BTC".to_string(), ws_tick.price);
                                    } else if ws_tick.symbol.contains("ETH") {
                                        crypto_prices_arc.lock().await.insert("ETH".to_string(), ws_tick.price);
                                    } else if ws_tick.symbol.contains("SOL") {
                                        crypto_prices_arc.lock().await.insert("SOL".to_string(), ws_tick.price);
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
                                        bid: None,
                                        ask: None,
                                        mark_price: None,
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
                                        orchestrator_arc.lock().await.update_weighted_delta(weighted_delta);
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
                                         match orchestrator_arc.lock().await.ingest_tick(tick).await {
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
                                         let orch = orchestrator_arc.lock().await;
                                         let (trades, pnl) = orch.get_risk_status();
                                         let has_pos = orch.active_position().is_some();
                                         info!(
                                             "[LIVE STATUS] Ticks Ingested: {} | Active Trades: {} | Daily PnL: {:.2} | Position: {}",
                                             step,
                                             trades,
                                             pnl,
                                             if has_pos { "YES" } else { "NO" }
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
