use axum::{
    routing::{get, post},
    Json, Router, Extension,
    response::{Html, IntoResponse},
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, Level, error};
use tracing_subscriber::FmtSubscriber;
use dotenvy::dotenv;
use sqlx::Row;

use price_broker::{Broker, OrderRequest, Side};
use price_timeseries::TimescaleClient;

use std::sync::RwLock;
use std::collections::HashMap;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct LiveStatusInfo {
    nifty_price: f64,
    vix: f64,
    ml_confidence: f64,
    opportunity_confidence: f64,
    opportunity_probability: f64,
    decision: String,
    quality_score: f64,
    target_option: String,
    timestamp: String,
    #[serde(default)]
    btc_price: f64,
    #[serde(default)]
    eth_price: f64,
    #[serde(default)]
    sol_price: f64,
}

/// Per-symbol live signal state posted by the price-worker 5m trading loop.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
struct CryptoSignal {
    symbol: String,
    price: f64,
    ema9: f64,
    ema21: f64,
    atr: f64,
    /// "LONG" | "SHORT" | "FLAT"
    direction: String,
    /// "ENTRY" | "EXIT" | "HOLD"
    action: String,
    /// EMA crossover bull flag
    bull_cross: bool,
    /// EMA crossover bear flag
    bear_cross: bool,
    leverage: u32,
    margin_usdt: f64,
    timestamp: String,
}

struct AppState {
    /// Primary broker used for Fyers live trading (via HybridBroker or python-broker)
    broker: Arc<dyn price_broker::Broker>,
    /// Dedicated Delta Exchange client for live crypto positions/orders/balance
    delta_client: Arc<price_broker::DeltaExchangeClient>,
    /// Paper trading broker (10 000 INR virtual capital)
    paper_broker: Arc<price_broker::PaperBroker>,
    python_broker_url: String,
    db: TimescaleClient,
    live_status: RwLock<Option<LiveStatusInfo>>,
    crypto_prices: Arc<tokio::sync::Mutex<std::collections::HashMap<String, f64>>>,
    /// Per-symbol crypto signals from the 5m Delta loop (BTC/ETH/SOL)
    crypto_signals: RwLock<std::collections::HashMap<String, CryptoSignal>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize logging
    dotenv().ok();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let python_broker_url = std::env::var("PYTHON_BROKER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8001".to_string());

    // 2. Setup Brokers (Fyers via HybridBroker, Delta standalone, Paper standalone)
    let broker: Arc<dyn price_broker::Broker> = {
        info!("Initializing HybridBroker (Fyers live + Paper trading)...");
        Arc::new(price_broker::HybridBroker::new(&python_broker_url, 10000.0))
    };

    let delta_client = {
        let delta_key    = std::env::var("DELTA_API_KEY").ok();
        let delta_secret = std::env::var("DELTA_API_SECRET").ok();
        let delta_url    = std::env::var("DELTA_BASE_URL")
            .unwrap_or_else(|_| "https://api.india.delta.exchange".to_string());
        info!("Initializing Delta Exchange client (key={})...",
            delta_key.as_deref().unwrap_or("<none>"));
        Arc::new(price_broker::DeltaExchangeClient::new(&delta_url, delta_key, delta_secret))
    };

    let paper_broker = Arc::new(price_broker::PaperBroker::new(10_000.0));

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5433/price".to_string());
    let db = TimescaleClient::new(&db_url).await?;
    db.init_db().await?;

    let crypto_prices = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));

    let state = Arc::new(AppState {
        broker,
        delta_client,
        paper_broker,
        python_broker_url: python_broker_url.clone(),
        db: db.clone(),
        live_status: RwLock::new(None),
        crypto_prices: crypto_prices.clone(),
        crypto_signals: RwLock::new(std::collections::HashMap::new()),
    });

    // Spawn server-side ticker polling loop for BTC, ETH, SOL
    {
        let cp_clone = crypto_prices.clone();
        tokio::spawn(async move {
            info!("[CryptoTicker] Starting server-side Delta Exchange ticker polling loop...");
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("User-Agent", reqwest::header::HeaderValue::from_static("price-engine-rust"));
            headers.insert("Accept", reqwest::header::HeaderValue::from_static("application/json"));
            let client = reqwest::Client::builder()
                .default_headers(headers)
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new());

            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
            let urls = vec![
                "https://api.india.delta.exchange/v2/tickers",
                "https://api.delta.exchange/v2/tickers",
            ];
            let extract_p = |v: &serde_json::Value| -> Option<f64> {
                if let Some(s) = v.as_str() { s.parse::<f64>().ok() }
                else if let Some(n) = v.as_f64() { Some(n) }
                else if let Some(i) = v.as_i64() { Some(i as f64) }
                else { None }
            };
            let mut tick_count: u64 = 0;
            loop {
                interval.tick().await;
                tick_count += 1;
                let mut fetched = false;
                for url in &urls {
                    match client.get(*url).send().await {
                        Ok(res) => {
                            let status = res.status();
                            match res.json::<serde_json::Value>().await {
                                Ok(json_val) => {
                                    if let Some(arr) = json_val.get("result").and_then(|r| r.as_array()) {
                                        let mut cp = cp_clone.lock().await;
                                        for t in arr {
                                            let sym = t.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
                                            let _c_type = t.get("contract_type").and_then(|c| c.as_str()).unwrap_or("");
                                            let _u_asset = t.get("underlying_asset_symbol").and_then(|a| a.as_str()).unwrap_or("");
                                            let price = t.get("mark_price")
                                                .and_then(&extract_p)
                                                .or_else(|| t.get("close").and_then(&extract_p))
                                                .or_else(|| t.get("spot_price").and_then(&extract_p))
                                                .or_else(|| t.get("last_price").and_then(&extract_p));

                                            if let Some(p) = price {
                                                if p > 0.0 {
                                                    // Match the actual symbols Delta Exchange returns for perpetuals
                                                    let is_btc = sym == "BTCUSD_PERP" || sym == "BTCUSDT" || sym == "BTCUSD";
                                                    let is_eth = sym == "ETHUSD_PERP" || sym == "ETHUSDT" || sym == "ETHUSD";
                                                    let is_sol = sym == "SOLUSD_PERP" || sym == "SOLUSDT" || sym == "SOLUSD";

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
                                            if tick_count <= 3 || tick_count % 100 == 0 {
                                                info!("[CryptoTicker] tick={} url={} tickers={} BTC={:.2} ETH={:.2} SOL={:.2}", 
                                                    tick_count, url, arr.len(),
                                                    cp.get("BTC").copied().unwrap_or(0.0),
                                                    cp.get("ETH").copied().unwrap_or(0.0),
                                                    cp.get("SOL").copied().unwrap_or(0.0));
                                            }
                                            fetched = true;
                                            break;
                                        }
                                    } else if tick_count <= 5 {
                                        info!("[CryptoTicker] tick={} url={} status={} no 'result' array in response", tick_count, url, status);
                                    }
                                }
                                Err(e) => {
                                    if tick_count <= 5 {
                                        info!("[CryptoTicker] tick={} url={} JSON parse error: {}", tick_count, url, e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            if tick_count <= 5 {
                                info!("[CryptoTicker] tick={} url={} HTTP error: {}", tick_count, url, e);
                            }
                        }
                    }
                }
                if !fetched && tick_count <= 5 {
                    info!("[CryptoTicker] tick={} WARN: no prices fetched from any URL", tick_count);
                }
            }
        });
    }

    // Start background downloader task
    start_background_downloader(db.clone(), python_broker_url.clone()).await;

    // 3. Build routes
    let app = Router::new()
        .route("/", get(index_handler))
        .route("/health", get(health_handler))
        .route("/portfolio", get(portfolio_handler))
        .route("/orders", get(orders_handler))
        .route("/trades", get(trades_handler))
        .route("/order", post(place_order_handler))
        .route("/broker/auth_url", get(auth_url_handler))
        .route("/broker/login_token", post(login_token_handler))
        .route("/live-status", get(get_live_status_handler).post(post_live_status_handler))
        .route("/live-status/crypto", get(get_crypto_signal_handler).post(post_crypto_signal_handler))
        .route("/database/jobs", get(database_jobs_handler))
        .route("/database/candles-preview", get(candles_preview_handler))
        .route("/database/download-status", get(download_status_handler))
        .route("/database/start-download", post(start_download_handler))
        .route("/database/download", get(download_handler))
        .route("/database/symbol-mappings", get(symbol_mappings_handler))
        .route("/journals/trade", get(journals_trade_handler))
        .route("/journals/decision", get(journals_decision_handler))
        .route("/journals/risk", get(journals_risk_handler))
        .route("/journals/ml", get(journals_ml_handler))
        .route("/research/performance", get(research_performance_handler))
        .route("/metrics", get(metrics_handler))
        .route("/crypto-prices", get(crypto_prices_handler))
        .route("/favicon.ico", get(favicon_handler))
        .layer(Extension(state));

    // 4. Run server
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8000".to_string())
        .parse::<u16>()?;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("PRICE REST server running at http://{}", addr);

    axum::serve(listener, app).await?;
    Ok(())
}

async fn index_handler() -> Html<&'static str> {
    Html(r##"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PRICE Monitoring Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=Space+Grotesk:wght@400;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #0b0f19;
            --card-bg: rgba(20, 26, 46, 0.6);
            --border-color: rgba(255, 255, 255, 0.08);
            --primary: #6366f1;
            --primary-gradient: linear-gradient(135deg, #6366f1 0%, #a855f7 100%);
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --text-main: #f3f4f6;
            --text-muted: #9ca3af;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            background-color: var(--bg-color);
            color: var(--text-main);
            font-family: 'Outfit', sans-serif;
            min-height: 100vh;
            padding: 2rem;
            background-image: radial-gradient(circle at 10% 20%, rgba(99, 102, 241, 0.05) 0%, transparent 40%),
                              radial-gradient(circle at 90% 80%, rgba(168, 85, 247, 0.05) 0%, transparent 40%);
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 1.5rem;
            flex-wrap: wrap;
            gap: 1.5rem;
        }

        h1 {
            font-family: 'Space Grotesk', sans-serif;
            font-size: 2rem;
            font-weight: 700;
            background: var(--primary-gradient);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .status-badge {
            background: rgba(16, 185, 129, 0.1);
            border: 1px solid var(--success);
            color: var(--success);
            padding: 0.4rem 1rem;
            border-radius: 50px;
            font-size: 0.85rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .status-dot {
            width: 8px;
            height: 8px;
            background-color: var(--success);
            border-radius: 50%;
            display: inline-block;
            box-shadow: 0 0 10px var(--success);
        }

        .live-pulse {
            width: 8px;
            height: 8px;
            background-color: var(--success);
            border-radius: 50%;
            display: inline-block;
            box-shadow: 0 0 10px var(--success);
            animation: pulse-glow 1.5s infinite;
        }

        @keyframes pulse-glow {
            0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
            70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }
            100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
        }

        .badge-trade { background: rgba(16, 185, 129, 0.15); border: 1px solid var(--success); color: var(--success); }
        .badge-wait { background: rgba(99, 102, 241, 0.15); border: 1px solid var(--primary); color: var(--primary); }
        .badge-reduce { background: rgba(245, 158, 11, 0.15); border: 1px solid var(--warning); color: var(--warning); }
        .badge-cancel { background: rgba(239, 68, 68, 0.15); border: 1px solid var(--danger); color: var(--danger); }

        .grid-container {
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 2rem;
        }

        @media (max-width: 1024px) {
            .grid-container {
                grid-template-columns: 1fr;
            }
        }

        .card {
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 1.5rem;
            backdrop-filter: blur(16px);
            margin-bottom: 2rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        }

        .card-title {
            font-family: 'Space Grotesk', sans-serif;
            font-size: 1.25rem;
            font-weight: 700;
            margin-bottom: 1.25rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            padding-bottom: 0.75rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 1.5rem;
            margin-bottom: 1.5rem;
        }

        .metric-card {
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid rgba(255, 255, 255, 0.04);
            border-radius: 12px;
            padding: 1.25rem;
            text-align: center;
        }

        .metric-label {
            font-size: 0.85rem;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
        }

        .metric-value {
            font-size: 1.6rem;
            font-weight: 700;
            color: var(--text-main);
        }

        .metric-value.highlight {
            color: var(--primary);
        }

        .btn {
            background: var(--primary-gradient);
            border: none;
            color: white;
            padding: 0.75rem 1.5rem;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            text-decoration: none;
            display: inline-block;
            text-align: center;
        }

        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(99, 102, 241, 0.4);
        }

        .btn-secondary {
            background: rgba(255, 255, 255, 0.08);
            border: 1px solid rgba(255, 255, 255, 0.1);
        }

        .btn-secondary:hover {
            background: rgba(255, 255, 255, 0.15);
            box-shadow: none;
        }

        .form-group {
            margin-bottom: 1.25rem;
        }

        .form-group label {
            display: block;
            font-size: 0.85rem;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
        }

        .input-text {
            width: 100%;
            background: rgba(0, 0, 0, 0.2);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 0.75rem;
            color: white;
            font-family: inherit;
            margin-bottom: 1rem;
        }

        .input-text:focus {
            outline: none;
            border-color: var(--primary);
        }

        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
            margin-top: 0.5rem;
        }

        th {
            color: var(--text-muted);
            font-weight: 600;
            font-size: 0.85rem;
            padding: 0.75rem 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }

        td {
            padding: 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.03);
            font-size: 0.9rem;
        }

        .pnl-green {
            color: var(--success);
            font-weight: 600;
        }

        .pnl-red {
            color: var(--danger);
            font-weight: 600;
        }

        .status-unconfigured {
            color: var(--warning);
            border-color: var(--warning);
            background: rgba(245, 158, 11, 0.1);
        }

        .status-unconfigured .status-dot {
            background-color: var(--warning);
            box-shadow: 0 0 10px var(--warning);
        }
        .nav-tabs {
            display: flex;
            gap: 1rem;
            margin-bottom: 1.5rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 0.5rem;
        }

        .nav-tab {
            padding: 0.6rem 1.25rem;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
            color: var(--text-muted);
            background: transparent;
            border: 1px solid transparent;
            transition: all 0.2s ease;
        }

        .nav-tab.active {
            color: white;
            background: rgba(99, 102, 241, 0.15);
            border-color: var(--primary);
        }

        .tab-content {
            display: none;
        }

        .tab-content.active {
            display: block;
        }
    </style>
</head>
<body>

    <header>
        <div>
            <h1>P.R.I.C.E</h1>
            <p style="color: var(--text-muted); font-size: 0.85rem; margin-top: 0.25rem;">Predictive Risk Intelligence & Capital Engine (Multi-Broker Enterprise)</p>
        </div>

        <!-- Live Ticker Header -->
        <div style="display: flex; gap: 1.25rem; align-items: center; background: rgba(20, 26, 46, 0.8); border: 1px solid var(--border-color); padding: 0.6rem 1.25rem; border-radius: 12px; backdrop-filter: blur(8px); box-shadow: 0 4px 20px rgba(0,0,0,0.2); flex-wrap: wrap;">
            <div style="display: flex; align-items: center; gap: 0.4rem;">
                <span class="live-pulse" id="nifty-pulse-dot" style="background-color: var(--danger); box-shadow: 0 0 10px var(--danger);"></span>
                <span style="font-size: 0.75rem; color: var(--text-muted); font-weight: 600; letter-spacing: 0.05em;">NIFTY:</span>
                <span style="font-size: 1rem; font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: var(--text-main);" id="live-nifty-val">₹0.00</span>
            </div>
            <div style="width: 1px; height: 16px; background: var(--border-color);"></div>
            <div style="display: flex; align-items: center; gap: 0.4rem;">
                <span style="font-size: 0.75rem; color: var(--warning); font-weight: 600; letter-spacing: 0.05em;">INDIA VIX:</span>
                <span style="font-size: 1rem; font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: var(--warning);" id="live-vix-val">0.00</span>
            </div>
            <div style="width: 1px; height: 16px; background: var(--border-color);"></div>
            <div style="display: flex; align-items: center; gap: 0.4rem;">
                <span style="font-size: 0.75rem; color: #f59e0b; font-weight: 700; letter-spacing: 0.05em;">BTC (200x):</span>
                <span style="font-size: 1rem; font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: #fbbf24;" id="live-btc-val">$0.00</span>
            </div>
            <div style="width: 1px; height: 16px; background: var(--border-color);"></div>
            <div style="display: flex; align-items: center; gap: 0.4rem;">
                <span style="font-size: 0.75rem; color: #6366f1; font-weight: 700; letter-spacing: 0.05em;">ETH (200x):</span>
                <span style="font-size: 1rem; font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: #818cf8;" id="live-eth-val">$0.00</span>
            </div>
            <div style="width: 1px; height: 16px; background: var(--border-color);"></div>
            <div style="display: flex; align-items: center; gap: 0.4rem;">
                <span style="font-size: 0.75rem; color: #10b981; font-weight: 700; letter-spacing: 0.05em;">SOL (100x):</span>
                <span style="font-size: 1rem; font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: #34d399;" id="live-sol-val">$0.00</span>
            </div>
        </div>

        <div class="status-badge" id="platform-status-badge">
            <span class="status-dot"></span>
            <span id="platform-status-text">Live Monitoring</span>
        </div>
    </header>

    <!-- Navigation Tabs -->
    <div class="nav-tabs">
        <button class="nav-tab active" onclick="switchTab('tab-live', this)">📈 Live Dashboard</button>
        <button class="nav-tab" onclick="switchTab('tab-journals', this)">📔 Enterprise Journals</button>
        <button class="nav-tab" onclick="switchTab('tab-research', this)">🔬 Research & Analytics</button>
        <button class="nav-tab" onclick="switchTab('tab-downloader', this)">📥 Data Downloader</button>
    </div>

    <!-- TAB 1: Live Dashboard -->
    <div id="tab-live" class="tab-content active">
        <div class="grid-container">
            <div>
                <!-- Live Opportunity Section -->
                <div class="card" style="border-left: 4px solid var(--primary);">
                    <div class="card-title">
                        <span>Live Opportunity & Quantitative Decision Pipeline</span>
                        <span id="last-update-time" style="font-size: 0.75rem; color: var(--danger); font-weight: 600; letter-spacing: 0.05em; background: rgba(239, 68, 68, 0.1); padding: 0.2rem 0.6rem; border-radius: 4px;">Worker offline</span>
                    </div>
                    <h3 style="font-size: 0.75rem; color: var(--success); margin-bottom: 0.75rem; letter-spacing: 0.08em; opacity: 0.8;">📊 NIFTY OPTIONS — NSE/FYERS</h3>
                    <div class="metrics-grid" style="margin-bottom: 1.5rem;">
                        <div class="metric-card" style="padding: 1rem;">
                            <div class="metric-label">Target Symbol / Contract</div>
                            <div class="metric-value highlight" style="font-size: 1.4rem; font-family: 'Space Grotesk', sans-serif;" id="opp-target-option">--</div>
                        </div>
                        <div class="metric-card" style="padding: 1rem;">
                            <div class="metric-label">Pipeline Decision</div>
                            <div style="margin-top: 0.4rem; display: flex; justify-content: center;">
                                <span id="opp-decision-badge" class="status-badge badge-wait" style="display: inline-flex; justify-content: center; width: 120px;">WAIT</span>
                            </div>
                        </div>
                        <div class="metric-card" style="padding: 1rem;">
                            <div class="metric-label">Trade Quality Score</div>
                            <div class="metric-value" id="opp-quality-score" style="color: var(--warning);">0.0</div>
                        </div>
                    </div>
                    <div class="metrics-grid" style="margin-bottom: 1.5rem;">
                        <div class="metric-card" style="padding: 1rem; background: rgba(255,255,255,0.01);">
                            <div class="metric-label">Opportunity Confidence</div>
                            <div class="metric-value" id="opp-confidence">0.0%</div>
                        </div>
                        <div class="metric-card" style="padding: 1rem; background: rgba(255,255,255,0.01);">
                            <div class="metric-label">ML Win Probability</div>
                            <div class="metric-value" id="opp-probability">0.0%</div>
                        </div>
                        <div class="metric-card" style="padding: 1rem; background: rgba(255,255,255,0.01);">
                            <div class="metric-label">ML Confidence Proxy</div>
                            <div class="metric-value" id="opp-ml-confidence">0.0%</div>
                        </div>
                    </div>

                    <!-- ===== CRYPTO FUTURES SIGNALS — DELTA EXCHANGE ===== -->
                    <div style="border-top: 1px solid rgba(255,255,255,0.06); padding-top: 1rem; margin-top: 0.25rem;">
                        <h3 style="font-size: 0.75rem; color: #f59e0b; margin-bottom: 0.75rem; letter-spacing: 0.08em; opacity: 0.9;">⚡ CRYPTO PERPETUALS — DELTA EXCHANGE (5m Loop)</h3>
                        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem;">
                            <!-- BTC -->
                            <div style="background: rgba(251,191,36,0.05); border: 1px solid rgba(251,191,36,0.2); border-radius: 12px; padding: 1rem;">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.6rem;">
                                    <span style="font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: #fbbf24; font-size: 0.9rem;">₿ BTC (200x)</span>
                                    <span id="btc-sig-badge" class="status-badge badge-wait" style="font-size: 0.65rem; padding: 0.2rem 0.5rem;">HOLD</span>
                                </div>
                                <div style="font-size: 1.1rem; font-weight: 700; color: #fbbf24; font-family: 'Space Grotesk', sans-serif; margin-bottom: 0.5rem;" id="btc-sig-price">$0.00</div>
                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.3rem; font-size: 0.75rem; color: var(--text-muted);">
                                    <span>EMA9: <strong id="btc-ema9" style="color: var(--text-main);">—</strong></span>
                                    <span>EMA21: <strong id="btc-ema21" style="color: var(--text-main);">—</strong></span>
                                    <span>ATR: <strong id="btc-atr" style="color: var(--text-main);">—</strong></span>
                                    <span id="btc-direction-lbl" style="color: var(--text-muted);">FLAT</span>
                                </div>
                            </div>
                            <!-- ETH -->
                            <div style="background: rgba(99,102,241,0.05); border: 1px solid rgba(99,102,241,0.2); border-radius: 12px; padding: 1rem;">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.6rem;">
                                    <span style="font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: #818cf8; font-size: 0.9rem;">Ξ ETH (200x)</span>
                                    <span id="eth-sig-badge" class="status-badge badge-wait" style="font-size: 0.65rem; padding: 0.2rem 0.5rem;">HOLD</span>
                                </div>
                                <div style="font-size: 1.1rem; font-weight: 700; color: #818cf8; font-family: 'Space Grotesk', sans-serif; margin-bottom: 0.5rem;" id="eth-sig-price">$0.00</div>
                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.3rem; font-size: 0.75rem; color: var(--text-muted);">
                                    <span>EMA9: <strong id="eth-ema9" style="color: var(--text-main);">—</strong></span>
                                    <span>EMA21: <strong id="eth-ema21" style="color: var(--text-main);">—</strong></span>
                                    <span>ATR: <strong id="eth-atr" style="color: var(--text-main);">—</strong></span>
                                    <span id="eth-direction-lbl" style="color: var(--text-muted);">FLAT</span>
                                </div>
                            </div>
                            <!-- SOL -->
                            <div style="background: rgba(16,185,129,0.05); border: 1px solid rgba(16,185,129,0.2); border-radius: 12px; padding: 1rem;">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.6rem;">
                                    <span style="font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: #34d399; font-size: 0.9rem;">◎ SOL (100x)</span>
                                    <span id="sol-sig-badge" class="status-badge badge-wait" style="font-size: 0.65rem; padding: 0.2rem 0.5rem;">HOLD</span>
                                </div>
                                <div style="font-size: 1.1rem; font-weight: 700; color: #34d399; font-family: 'Space Grotesk', sans-serif; margin-bottom: 0.5rem;" id="sol-sig-price">$0.00</div>
                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.3rem; font-size: 0.75rem; color: var(--text-muted);">
                                    <span>EMA9: <strong id="sol-ema9" style="color: var(--text-main);">—</strong></span>
                                    <span>EMA21: <strong id="sol-ema21" style="color: var(--text-main);">—</strong></span>
                                    <span>ATR: <strong id="sol-atr" style="color: var(--text-main);">—</strong></span>
                                    <span id="sol-direction-lbl" style="color: var(--text-muted);">FLAT</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="card">
                    <div class="card-title">Portfolio & Account Balance (Triple Broker)</div>

                    <h3 style="font-size: 0.85rem; color: var(--success); margin-bottom: 0.75rem; letter-spacing: 0.05em;">📈 LIVE FYERS ACCOUNT</h3>
                    <div class="metrics-grid" style="margin-bottom: 1.5rem;">
                        <div class="metric-card" style="padding: 1rem;">
                            <div class="metric-label">Total Capital Limit</div>
                            <div class="metric-value highlight" id="live-val-limit">₹0.00</div>
                        </div>
                        <div class="metric-card" style="padding: 1rem;">
                            <div class="metric-label">Utilized Margin</div>
                            <div class="metric-value" id="live-val-utilized">₹0.00</div>
                        </div>
                        <div class="metric-card" style="padding: 1rem;">
                            <div class="metric-label">Available Balance</div>
                            <div class="metric-value" id="live-val-available">₹0.00</div>
                        </div>
                    </div>

                    <h3 style="font-size: 0.85rem; color: #fbbf24; margin-bottom: 0.75rem; letter-spacing: 0.05em;">⚡ LIVE DELTA ACCOUNT (USDT Perpetuals)</h3>
                    <div class="metrics-grid" style="margin-bottom: 1.5rem;">
                        <div class="metric-card" style="padding: 1rem; border-color: rgba(251,191,36,0.15);">
                            <div class="metric-label">Total Limit</div>
                            <div class="metric-value" style="color: #fbbf24;" id="delta-val-limit">$0.00</div>
                        </div>
                        <div class="metric-card" style="padding: 1rem; border-color: rgba(251,191,36,0.15);">
                            <div class="metric-label">Utilized Margin</div>
                            <div class="metric-value" id="delta-val-utilized">$0.00</div>
                        </div>
                        <div class="metric-card" style="padding: 1rem; border-color: rgba(251,191,36,0.15);">
                            <div class="metric-label">Available USDT</div>
                            <div class="metric-value" style="color: #fbbf24;" id="delta-val-available">$0.00</div>
                        </div>
                    </div>

                    <h3 style="font-size: 0.85rem; color: var(--primary); margin-bottom: 0.75rem; letter-spacing: 0.05em;">🤖 PAPER TRADING ACCOUNT</h3>
                    <div class="metrics-grid">
                        <div class="metric-card" style="padding: 1rem;">
                            <div class="metric-label">Total Capital Limit</div>
                            <div class="metric-value highlight" id="paper-val-limit">₹0.00</div>
                        </div>
                        <div class="metric-card" style="padding: 1rem;">
                            <div class="metric-label">Utilized Margin</div>
                            <div class="metric-value" id="paper-val-utilized">₹0.00</div>
                        </div>
                        <div class="metric-card" style="padding: 1rem;">
                            <div class="metric-label">Available Balance</div>
                            <div class="metric-value" id="paper-val-available">₹0.00</div>
                        </div>
                    </div>
                </div>

                <div class="card">
                    <div class="card-title">Active Net Positions</div>
                    <table id="positions-table">
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Side</th>
                                <th>Qty</th>
                                <th>Avg Price</th>
                                <th>LTP</th>
                                <th>PnL</th>
                                <th>Broker</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td colspan="7" style="text-align: center; color: var(--text-muted);">No active positions found</td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <div class="card">
                    <div class="card-title">Recent Order Log</div>
                    <table id="orders-table">
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Symbol</th>
                                <th>Side</th>
                                <th>Qty</th>
                                <th>Price</th>
                                <th>Status</th>
                                <th>Broker</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td colspan="7" style="text-align: center; color: var(--text-muted);">No recent orders found</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div>
                <div class="card">
                    <div class="card-title">Fyers API Activation</div>
                    <p style="font-size: 0.85rem; color: var(--text-muted); margin-bottom: 1.5rem; line-height: 1.4;">
                        Use this panel to exchange authorization code for a persistent Fyers session token.
                    </p>
                    <div class="form-group" style="text-align: center; margin-bottom: 1.5rem;">
                        <a href="#" target="_blank" class="btn" id="fyers-login-btn">1. Authorize App on Fyers</a>
                    </div>
                    <div class="form-group">
                        <label for="auth-code-input">2. Paste Auth Code</label>
                        <input type="text" id="auth-code-input" class="input-text" placeholder="e.g. eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...">
                    </div>
                    <button class="btn btn-secondary" style="width: 100%;" id="activate-token-btn">3. Generate & Save Token</button>
                    <div id="activation-message" style="margin-top: 1rem; font-size: 0.85rem; font-weight: 600; text-align: center;"></div>
                </div>
            </div>
        </div>
    </div>

    <!-- TAB 2: Enterprise Journals -->
    <div id="tab-journals" class="tab-content">
        <div class="card">
            <div class="card-title">
                <span>Enterprise Audit Journals (TimescaleDB Hypertables)</span>
                <div style="display: flex; gap: 0.5rem;">
                    <button class="btn btn-secondary" onclick="loadJournalTab('trade')" style="padding: 0.4rem 0.8rem; font-size: 0.8rem;">Trade Journal</button>
                    <button class="btn btn-secondary" onclick="loadJournalTab('decision')" style="padding: 0.4rem 0.8rem; font-size: 0.8rem;">Decision Journal</button>
                    <button class="btn btn-secondary" onclick="loadJournalTab('risk')" style="padding: 0.4rem 0.8rem; font-size: 0.8rem;">Risk Audit</button>
                    <button class="btn btn-secondary" onclick="loadJournalTab('ml')" style="padding: 0.4rem 0.8rem; font-size: 0.8rem;">ML Predictions</button>
                </div>
            </div>
            <div style="overflow-x: auto;">
                <table id="journal-data-table">
                    <thead>
                        <tr id="journal-table-head">
                            <th>Timestamp</th>
                            <th>Symbol</th>
                            <th>Event / Details</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td colspan="3" style="text-align: center; color: var(--text-muted);">Select a journal to view logs</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- TAB 3: Research & Performance Analytics -->
    <div id="tab-research" class="tab-content">
        <div class="card">
            <div class="card-title">
                <span>Research & Performance Analytics Lab</span>
                <select id="research-symbol-select" onchange="loadResearchData()" class="input-text" style="width: auto; padding: 0.4rem 1rem; font-size: 0.85rem;">
                    <option value="BTCUSD_PERP">BTCUSD_PERP</option>
                    <option value="ETHUSD_PERP">ETHUSD_PERP</option>
                    <option value="SOLUSD_PERP">SOLUSD_PERP</option>
                    <option value="NSE:NIFTY50-INDEX">NSE:NIFTY50-INDEX</option>
                </select>
            </div>
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-label">Sharpe Ratio</div>
                    <div class="metric-value highlight" id="perf-sharpe">0.00</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Win Rate %</div>
                    <div class="metric-value" id="perf-winrate" style="color: var(--success);">0.0%</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Profit Factor</div>
                    <div class="metric-value" id="perf-profitfactor">0.00</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Max Drawdown</div>
                    <div class="metric-value" id="perf-maxdrawdown" style="color: var(--danger);">0.0%</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Trade Expectancy</div>
                    <div class="metric-value" id="perf-expectancy">₹0.00</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Total Trades</div>
                    <div class="metric-value" id="perf-totaltrades">0</div>
                </div>
            </div>
        </div>
    </div>

    <!-- TAB 4: Symbol Mappings removed - handled by automated system -->

    <!-- TAB 5: Historical Data Downloader -->
    <div id="tab-downloader" class="tab-content">
        <div class="grid-container">
            <div>
                <div class="card">
                    <div class="card-title">Historical Data Downloader</div>
                    <div id="downloader-alert-box" style="display: none; padding: 0.75rem; border-radius: 6px; background: rgba(239, 68, 68, 0.15); border: 1px solid var(--danger); margin-bottom: 0.75rem; font-size: 0.8rem; color: #fca5a5; line-height: 1.4;"></div>
                    <div class="form-group" style="margin-bottom: 0.75rem;">
                        <label for="download-year-select">Select Year</label>
                        <select id="download-year-select" class="input-text" style="background-color: #121824; border: 1px solid var(--border-color); color: var(--text-main); width: 100%; padding: 0.5rem; border-radius: 4px;">
                            <option value="2026">2026</option>
                        </select>
                    </div>
                    <div class="form-group" style="margin-bottom: 0.75rem;">
                        <label for="download-symbol-select">Select Symbol</label>
                        <select id="download-symbol-select" class="input-text" style="background-color: #121824; border: 1px solid var(--border-color); color: var(--text-main); width: 100%; padding: 0.5rem; border-radius: 4px;">
                            <option value="BTCUSD_PERP">BTCUSD_PERP (Delta Exchange 24/7)</option>
                            <option value="ETHUSD_PERP">ETHUSD_PERP (Delta Exchange 24/7)</option>
                            <option value="SOLUSD_PERP">SOLUSD_PERP (Delta Exchange 24/7)</option>
                            <option value="NSE:NIFTY50-INDEX">NSE:NIFTY50-INDEX (Fyers)</option>
                            <option value="NSE:INDIAVIX-INDEX">NSE:INDIAVIX-INDEX (Fyers)</option>
                        </select>
                    </div>
                    <div id="downloader-info-box" style="margin: 1rem 0; padding: 0.75rem; border-radius: 6px; background: rgba(255,255,255,0.03); border: 1px solid var(--border-color);">
                        <div style="font-size: 0.85rem; color: var(--text-muted); display: flex; justify-content: space-between; margin-bottom: 0.25rem;">
                            <span>Status:</span>
                            <strong id="downloader-status-val" style="color: var(--warning);">Checking...</strong>
                        </div>
                        <div style="font-size: 0.85rem; color: var(--text-muted); display: flex; justify-content: space-between;">
                            <span>Progress:</span>
                            <strong id="downloader-progress-val">0 / 0 days</strong>
                        </div>
                        <div id="downloader-time-row" style="font-size: 0.85rem; color: var(--text-muted); display: flex; justify-content: space-between; margin-top: 0.25rem;">
                            <span>Est. Time Left:</span>
                            <strong id="downloader-time-val" style="color: #6366f1;">--:--</strong>
                        </div>
                    </div>
                    <button id="downloader-action-btn" class="btn" style="width: 100%;">Start Data Collection</button>
                </div>
            </div>

            <div>
                <div class="card">
                    <div class="card-title">Ingestion Pipeline Monitor</div>
                    <h3 style="font-size: 0.8rem; color: var(--primary); margin-bottom: 0.5rem; letter-spacing: 0.05em;">🔄 ACTIVE & PAST JOBS</h3>
                    <div style="max-height: 160px; overflow-y: auto; margin-bottom: 1.5rem; border: 1px solid var(--border-color); border-radius: 8px; background: rgba(0,0,0,0.15);">
                        <table style="margin-top: 0;" id="pipeline-jobs-table">
                            <thead>
                                <tr style="position: sticky; top: 0; background: #101524; z-index: 1;">
                                    <th style="padding: 0.5rem; font-size: 0.75rem;">Symbol</th>
                                    <th style="padding: 0.5rem; font-size: 0.75rem;">Year</th>
                                    <th style="padding: 0.5rem; font-size: 0.75rem;">Status</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr>
                                    <td colspan="3" style="text-align: center; color: var(--text-muted); padding: 0.75rem; font-size: 0.8rem;">No active pipeline jobs</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- TAB: Crypto Perpetuals removed - handled by automated opportunity system -->
    <div id="tab-crypto" class="tab-content" style="display:none!important">
        <!-- Live Crypto Market Cards -->
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(310px, 1fr)); gap: 1.25rem; margin-bottom: 1.5rem;">
            <!-- BTC Card -->
            <div class="card" style="border-top: 4px solid #f59e0b; position: relative;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
                    <div>
                        <strong style="font-size: 1.1rem; color: #fbbf24;">BTCUSD_PERP</strong>
                        <span style="font-size: 0.75rem; color: var(--text-muted); display: block;">Bitcoin Perpetual Futures</span>
                    </div>
                    <span class="status-badge" style="background: rgba(245, 158, 11, 0.2); color: #fbbf24; border-color: rgba(245, 158, 11, 0.4);">⚡ 200x Max Leverage</span>
                </div>
                <div style="font-size: 1.8rem; font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: var(--text-main); margin-bottom: 0.5rem;" id="crypto-btc-card-price">$0.00</div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; font-size: 0.8rem; color: var(--text-muted); background: rgba(0,0,0,0.2); padding: 0.5rem 0.75rem; border-radius: 8px;">
                    <div>24h High: <span id="crypto-btc-high" style="color: var(--success); font-weight: 600;">$0.00</span></div>
                    <div>24h Low: <span id="crypto-btc-low" style="color: var(--danger); font-weight: 600;">$0.00</span></div>
                    <div>Funding Rate: <span id="crypto-btc-funding" style="color: var(--warning); font-weight: 600;">+0.0100%</span></div>
                    <div>24/7 Market: <span style="color: var(--success); font-weight: 600;">ACTIVE</span></div>
                </div>
                <div style="display: flex; gap: 0.5rem; margin-top: 1rem;">
                    <button class="btn" style="flex: 1; background: var(--success); font-weight: bold;" onclick="openCryptoOrderModal('BTCUSD_PERP', '1')">LONG BTC</button>
                    <button class="btn" style="flex: 1; background: var(--danger); font-weight: bold;" onclick="openCryptoOrderModal('BTCUSD_PERP', '-1')">SHORT BTC</button>
                </div>
            </div>

            <!-- ETH Card -->
            <div class="card" style="border-top: 4px solid #6366f1; position: relative;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
                    <div>
                        <strong style="font-size: 1.1rem; color: #818cf8;">ETHUSD_PERP</strong>
                        <span style="font-size: 0.75rem; color: var(--text-muted); display: block;">Ethereum Perpetual Futures</span>
                    </div>
                    <span class="status-badge" style="background: rgba(99, 102, 241, 0.2); color: #818cf8; border-color: rgba(99, 102, 241, 0.4);">⚡ 200x Max Leverage</span>
                </div>
                <div style="font-size: 1.8rem; font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: var(--text-main); margin-bottom: 0.5rem;" id="crypto-eth-card-price">$0.00</div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; font-size: 0.8rem; color: var(--text-muted); background: rgba(0,0,0,0.2); padding: 0.5rem 0.75rem; border-radius: 8px;">
                    <div>24h High: <span id="crypto-eth-high" style="color: var(--success); font-weight: 600;">$0.00</span></div>
                    <div>24h Low: <span id="crypto-eth-low" style="color: var(--danger); font-weight: 600;">$0.00</span></div>
                    <div>Funding Rate: <span id="crypto-eth-funding" style="color: var(--warning); font-weight: 600;">+0.0100%</span></div>
                    <div>24/7 Market: <span style="color: var(--success); font-weight: 600;">ACTIVE</span></div>
                </div>
                <div style="display: flex; gap: 0.5rem; margin-top: 1rem;">
                    <button class="btn" style="flex: 1; background: var(--success); font-weight: bold;" onclick="openCryptoOrderModal('ETHUSD_PERP', '1')">LONG ETH</button>
                    <button class="btn" style="flex: 1; background: var(--danger); font-weight: bold;" onclick="openCryptoOrderModal('ETHUSD_PERP', '-1')">SHORT ETH</button>
                </div>
            </div>

            <!-- SOL Card -->
            <div class="card" style="border-top: 4px solid #10b981; position: relative;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
                    <div>
                        <strong style="font-size: 1.1rem; color: #34d399;">SOLUSD_PERP</strong>
                        <span style="font-size: 0.75rem; color: var(--text-muted); display: block;">Solana Perpetual Futures</span>
                    </div>
                    <span class="status-badge" style="background: rgba(16, 185, 129, 0.2); color: #34d399; border-color: rgba(16, 185, 129, 0.4);">⚡ 100x Max Leverage</span>
                </div>
                <div style="font-size: 1.8rem; font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: var(--text-main); margin-bottom: 0.5rem;" id="crypto-sol-card-price">$0.00</div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; font-size: 0.8rem; color: var(--text-muted); background: rgba(0,0,0,0.2); padding: 0.5rem 0.75rem; border-radius: 8px;">
                    <div>24h High: <span id="crypto-sol-high" style="color: var(--success); font-weight: 600;">$0.00</span></div>
                    <div>24h Low: <span id="crypto-sol-low" style="color: var(--danger); font-weight: 600;">$0.00</span></div>
                    <div>Funding Rate: <span id="crypto-sol-funding" style="color: var(--warning); font-weight: 600;">+0.0100%</span></div>
                    <div>24/7 Market: <span style="color: var(--success); font-weight: 600;">ACTIVE</span></div>
                </div>
                <div style="display: flex; gap: 0.5rem; margin-top: 1rem;">
                    <button class="btn" style="flex: 1; background: var(--success); font-weight: bold;" onclick="openCryptoOrderModal('SOLUSD_PERP', '1')">LONG SOL</button>
                    <button class="btn" style="flex: 1; background: var(--danger); font-weight: bold;" onclick="openCryptoOrderModal('SOLUSD_PERP', '-1')">SHORT SOL</button>
                </div>
            </div>
        </div>

        <!-- Delta Exchange Order Execution & Dynamic Risk Calculator -->
        <div class="grid-container" style="margin-bottom: 1.5rem;">
            <div class="card">
                <div class="card-title">⚡ Delta Exchange Order Execution & Risk Engine</div>
                <form id="crypto-order-form" onsubmit="submitCryptoOrder(event)">
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1rem;">
                        <div>
                            <label style="display: block; font-size: 0.8rem; color: var(--text-muted); margin-bottom: 0.25rem;">Contract Symbol</label>
                            <select id="crypto-order-symbol" class="select-input" onchange="updateCryptoCalc()">
                                <option value="BTCUSD_PERP">BTCUSD_PERP (Max 200x)</option>
                                <option value="ETHUSD_PERP">ETHUSD_PERP (Max 200x)</option>
                                <option value="SOLUSD_PERP">SOLUSD_PERP (Max 100x)</option>
                            </select>
                        </div>
                        <div>
                            <label style="display: block; font-size: 0.8rem; color: var(--text-muted); margin-bottom: 0.25rem;">Order Side</label>
                            <select id="crypto-order-side" class="select-input" onchange="updateCryptoCalc()">
                                <option value="1">BUY / LONG</option>
                                <option value="-1">SELL / SHORT</option>
                            </select>
                        </div>
                    </div>

                    <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 1rem; margin-bottom: 1rem;">
                        <div>
                            <label style="display: block; font-size: 0.8rem; color: var(--text-muted); margin-bottom: 0.25rem;">Order Type</label>
                            <select id="crypto-order-type" class="select-input">
                                <option value="2">Market Order</option>
                                <option value="1">Limit Order</option>
                            </select>
                        </div>
                        <div>
                            <label style="display: block; font-size: 0.8rem; color: var(--text-muted); margin-bottom: 0.25rem;">Contracts (Qty)</label>
                            <input type="number" id="crypto-order-qty" class="select-input" value="1" min="1" step="1" oninput="updateCryptoCalc()" />
                        </div>
                        <div>
                            <label style="display: block; font-size: 0.8rem; color: var(--text-muted); margin-bottom: 0.25rem;">Leverage: <span id="crypto-lev-label" style="color: var(--primary); font-weight: bold;">50x</span></label>
                            <input type="range" id="crypto-order-leverage" min="1" max="200" value="50" style="width: 100%; cursor: pointer;" oninput="updateCryptoCalc()" />
                        </div>
                    </div>

                    <!-- Risk Engine Result Box -->
                    <div style="background: rgba(15, 23, 42, 0.8); border: 1px solid var(--border-color); border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 1rem; display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; font-size: 0.8rem;">
                        <div>Initial Margin: <strong id="calc-init-margin" style="color: var(--text-main); font-size: 0.95rem; display: block;">$0.00</strong></div>
                        <div>Maintenance Margin: <strong id="calc-maint-margin" style="color: var(--warning); font-size: 0.95rem; display: block;">$0.00</strong></div>
                        <div>Est. Liquidation Price: <strong id="calc-liq-price" style="color: var(--danger); font-size: 0.95rem; display: block;">$0.00</strong></div>
                        <div>Risk Engine Status: <strong style="color: var(--success); font-size: 0.95rem; display: block;">PASSED</strong></div>
                    </div>

                    <button type="submit" class="btn" style="width: 100%; background: var(--primary); font-weight: 700; font-size: 1rem;">🚀 Submit Order to Delta Exchange Engine</button>
                </form>
            </div>
        </div>

        <!-- Crypto Active Positions Table -->
        <div class="card">
            <div class="card-title">💼 Active Delta Exchange Crypto Positions & Orders</div>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Side</th>
                        <th>Quantity</th>
                        <th>Entry Price</th>
                        <th>Current Price</th>
                        <th>Leverage</th>
                        <th>Est. Liquidation Price</th>
                        <th>Unrealized PnL ($)</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody id="crypto-positions-tbody">
                    <tr><td colspan="9" style="text-align: center; color: var(--text-muted); padding: 1rem;">No active crypto perpetual positions</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        function switchTab(tabId, btnElem) {
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.nav-tab').forEach(b => b.classList.remove('active'));
            
            const target = document.getElementById(tabId);
            if (target) target.classList.add('active');
            
            if (btnElem) {
                btnElem.classList.add('active');
            } else if (window.event && window.event.target && window.event.target.classList.contains('nav-tab')) {
                window.event.target.classList.add('active');
            } else {
                const navBtns = document.querySelectorAll('.nav-tab');
                navBtns.forEach(btn => {
                    if (btn.getAttribute('onclick') && btn.getAttribute('onclick').includes(tabId)) {
                        btn.classList.add('active');
                    }
                });
            }

            if (tabId === 'tab-journals') loadJournalTab('trade');
            if (tabId === 'tab-research') loadResearchData();
            if (tabId === 'tab-symbols') loadSymbolMappings();
            if (tabId === 'tab-crypto') fetchCryptoLiveQuotes();
        }

        // Fetch crypto prices from our server (avoids CORS issues with direct Delta Exchange calls)
        async function fetchCryptoLiveQuotes() {
            try {
                const res = await fetch('/crypto-prices');
                const data = await res.json();
                if (data.status === 'success' && data.prices) {
                    const fmt = (v) => `$${(v || 0).toLocaleString('en-US', {minimumFractionDigits: 2})}`;
                    const btc = data.prices.BTC || 0;
                    const eth = data.prices.ETH || 0;
                    const sol = data.prices.SOL || 0;
                    if (btc > 0) document.getElementById('live-btc-val').textContent = fmt(btc);
                    if (eth > 0) document.getElementById('live-eth-val').textContent = fmt(eth);
                    if (sol > 0) document.getElementById('live-sol-val').textContent = fmt(sol);
                }
            } catch (e) {
                console.error("Error fetching crypto prices: ", e);
            }
        }

        async function loadJournalTab(type) {
            const head = document.getElementById('journal-table-head');
            const tbody = document.querySelector('#journal-data-table tbody');
            tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; color: var(--text-muted);">Loading journal logs...</td></tr>';
            
            try {
                const res = await fetch(`/journals/${type}`);
                const data = await res.json();
                
                if (type === 'trade') {
                    head.innerHTML = `<th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Entry P</th><th>Exit P</th><th>PnL</th><th>Broker</th><th>Leverage</th>`;
                    if (data.trades && data.trades.length > 0) {
                        tbody.innerHTML = data.trades.map(t => `
                            <tr>
                                <td>${new Date(t.timestamp).toLocaleTimeString()}</td>
                                <td><strong>${t.symbol}</strong></td>
                                <td>${t.side.toUpperCase()}</td>
                                <td>${t.qty}</td>
                                <td>₹${t.entry_price.toFixed(2)}</td>
                                <td>₹${t.exit_price.toFixed(2)}</td>
                                <td class="${t.pnl >= 0 ? 'pnl-green' : 'pnl-red'}">₹${t.pnl.toFixed(2)}</td>
                                <td>${t.broker}</td>
                                <td>${t.leverage}x</td>
                            </tr>
                        `).join('');
                    } else {
                        tbody.innerHTML = '<tr><td colspan="9" style="text-align:center; color: var(--text-muted);">No trade journal entries recorded yet</td></tr>';
                    }
                } else if (type === 'decision') {
                    head.innerHTML = `<th>Time</th><th>Symbol</th><th>Decision</th><th>Score</th><th>Threshold</th><th>Regime</th><th>ML Confidence</th>`;
                    if (data.decisions && data.decisions.length > 0) {
                        tbody.innerHTML = data.decisions.map(d => `
                            <tr>
                                <td>${new Date(d.timestamp).toLocaleTimeString()}</td>
                                <td><strong>${d.symbol}</strong></td>
                                <td><span class="status-badge ${d.decision === 'Trade' ? 'badge-trade' : 'badge-wait'}">${d.decision}</span></td>
                                <td>${d.score.toFixed(1)}</td>
                                <td>${d.threshold.toFixed(1)}</td>
                                <td>${d.regime}</td>
                                <td>${d.ml_confidence.toFixed(1)}%</td>
                            </tr>
                        `).join('');
                    } else {
                        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; color: var(--text-muted);">No decision journal entries recorded yet</td></tr>';
                    }
                } else if (type === 'risk') {
                    head.innerHTML = `<th>Time</th><th>Symbol</th><th>Event</th><th>Lev Used</th><th>Lev Limit</th><th>Passed</th>`;
                    if (data.risk_logs && data.risk_logs.length > 0) {
                        tbody.innerHTML = data.risk_logs.map(r => `
                            <tr>
                                <td>${new Date(r.timestamp).toLocaleTimeString()}</td>
                                <td><strong>${r.symbol}</strong></td>
                                <td>${r.event}</td>
                                <td>${r.leverage_used.toFixed(1)}x</td>
                                <td>${r.leverage_limit.toFixed(1)}x</td>
                                <td style="color: ${r.check_passed ? 'var(--success)' : 'var(--danger)'}">${r.check_passed ? 'PASSED' : 'REJECTED'}</td>
                            </tr>
                        `).join('');
                    } else {
                        tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; color: var(--text-muted);">No risk journal entries recorded yet</td></tr>';
                    }
                } else if (type === 'ml') {
                    head.innerHTML = `<th>Time</th><th>Symbol</th><th>Price</th><th>Prediction Score</th><th>Passed</th>`;
                    if (data.ml_predictions && data.ml_predictions.length > 0) {
                        tbody.innerHTML = data.ml_predictions.map(m => `
                            <tr>
                                <td>${new Date(m.timestamp).toLocaleTimeString()}</td>
                                <td><strong>${m.symbol}</strong></td>
                                <td>₹${m.price.toFixed(2)}</td>
                                <td style="font-weight: 700; color: var(--primary);">${m.prediction_score.toFixed(1)}%</td>
                                <td style="color: ${m.passed ? 'var(--success)' : 'var(--danger)'}">${m.passed ? 'PASS' : 'FAIL'}</td>
                            </tr>
                        `).join('');
                    } else {
                        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color: var(--text-muted);">No ML journal entries recorded yet</td></tr>';
                    }
                }
            } catch (e) {
                console.error("Failed to load journal", e);
            }
        }

        async function loadResearchData() {
            const sym = document.getElementById('research-symbol-select').value;
            try {
                const res = await fetch(`/research/performance?symbol=${encodeURIComponent(sym)}`);
                const data = await res.json();
                if (data.status === 'success' && data.performance) {
                    const p = data.performance;
                    document.getElementById('perf-sharpe').textContent = p.sharpe_ratio.toFixed(2);
                    document.getElementById('perf-winrate').textContent = `${p.win_rate.toFixed(1)}%`;
                    document.getElementById('perf-profitfactor').textContent = p.profit_factor === Infinity ? '∞' : p.profit_factor.toFixed(2);
                    document.getElementById('perf-maxdrawdown').textContent = `${p.max_drawdown_pct.toFixed(1)}%`;
                    document.getElementById('perf-expectancy').textContent = `₹${p.expectancy.toFixed(2)}`;
                    document.getElementById('perf-totaltrades').textContent = p.total_trades;
                }
            } catch (e) {
                console.error("Failed to load research data", e);
            }
        }

        async function loadSymbolMappings() {
            const tbody = document.querySelector('#symbol-mappings-table tbody');
            try {
                const res = await fetch('/database/symbol-mappings');
                const data = await res.json();
                if (data.status === 'success' && data.mappings) {
                    tbody.innerHTML = data.mappings.map(m => `
                        <tr>
                            <td><strong>${m.canonical_symbol}</strong></td>
                            <td><span style="color: ${m.broker_name === 'DELTA' ? 'var(--primary)' : 'var(--success)'}; font-weight:600;">${m.broker_name}</span></td>
                            <td><code>${m.broker_symbol}</code></td>
                            <td>${m.exchange}</td>
                            <td>${m.asset_class}</td>
                            <td>${m.tick_size}</td>
                            <td><strong style="color: var(--warning);">${m.max_leverage}x</strong></td>
                        </tr>
                    `).join('');
                }
            } catch (e) {
                console.error("Failed to load symbol mappings", e);
            }
        }

        // Fetch Auth URL on load
        async function fetchAuthUrl() {
            try {
                const response = await fetch('/broker/auth_url');
                const data = await response.json();
                if (data.status === 'success' && data.auth_url) {
                    const loginBtn = document.getElementById('fyers-login-btn');
                    loginBtn.href = data.auth_url;
                }
            } catch (e) {
                console.error("Failed to load auth URL: ", e);
            }
        }

        // Fetch Health & Broker Status
        async function checkHealth() {
            try {
                const response = await fetch('/health');
                const data = await response.json();
                const badge = document.getElementById('platform-status-badge');
                const text = document.getElementById('platform-status-text');
                
                if (data.broker_connection === 'connected') {
                    badge.classList.remove('status-unconfigured');
                    text.textContent = 'Live System Active';
                } else {
                    badge.classList.add('status-unconfigured');
                    text.textContent = 'Broker Activation Pending';
                }
            } catch (e) {
                console.error("Health query failed: ", e);
            }
        }

        // Fetch Portfolio and balance (Triple Broker)
        async function fetchPortfolio() {
            try {
                const response = await fetch('/portfolio');
                const data = await response.json();
                
                if (data.live_funds) {
                    document.getElementById('live-val-limit').textContent = `₹${(data.live_funds.limit_amount || 0).toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                    document.getElementById('live-val-utilized').textContent = `₹${(data.live_funds.utilised_balance || 0).toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                    document.getElementById('live-val-available').textContent = `₹${(data.live_funds.available_balance || 0).toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                }
                if (data.delta_funds) {
                    document.getElementById('delta-val-limit').textContent = `$${(data.delta_funds.limit_amount || 0).toLocaleString('en-US', {minimumFractionDigits: 2})}`;
                    document.getElementById('delta-val-utilized').textContent = `$${(data.delta_funds.utilised_balance || 0).toLocaleString('en-US', {minimumFractionDigits: 2})}`;
                    document.getElementById('delta-val-available').textContent = `$${(data.delta_funds.available_balance || 0).toLocaleString('en-US', {minimumFractionDigits: 2})}`;
                }
                if (data.paper_funds) {
                    document.getElementById('paper-val-limit').textContent = `₹${(data.paper_funds.limit_amount || 0).toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                    document.getElementById('paper-val-utilized').textContent = `₹${(data.paper_funds.utilised_balance || 0).toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                    document.getElementById('paper-val-available').textContent = `₹${(data.paper_funds.available_balance || 0).toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                }

                const posTableBody = document.querySelector('#positions-table tbody');
                if (data.positions && data.positions.length > 0) {
                    posTableBody.innerHTML = '';
                    data.positions.forEach(p => {
                        const row = document.createElement('tr');
                        const pnlClass = p.pnl >= 0 ? 'pnl-green' : 'pnl-red';
                        const currency = p.broker === 'DELTA' ? '$' : '₹';
                        row.innerHTML = `
                            <td><strong>${p.symbol}</strong></td>
                            <td>${p.side === 1 ? 'BUY' : 'SELL'}</td>
                            <td>${p.buy_qty || p.sell_qty}</td>
                            <td>${currency}${(p.avg_price || 0).toFixed(2)}</td>
                            <td>${currency}${(p.current_price || 0).toFixed(2)}</td>
                            <td class="${pnlClass}">${currency}${(p.pnl || 0).toFixed(2)}</td>
                            <td><span style="font-weight:600; font-size:0.8rem; color: ${p.broker === 'DELTA' ? '#fbbf24' : p.broker === 'FYERS' ? 'var(--success)' : 'var(--primary)'}">${p.broker || 'PAPER'}</span></td>
                        `;
                        posTableBody.appendChild(row);
                    });
                } else {
                    posTableBody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--text-muted);">No active positions found</td></tr>`;
                }
            } catch (e) {
                console.error("Failed to fetch portfolio: ", e);
            }
        }

        // Fetch Order Log
        async function fetchOrders() {
            try {
                const response = await fetch('/orders');
                const data = await response.json();
                const ordTableBody = document.querySelector('#orders-table tbody');
                
                if (data.orders && data.orders.length > 0) {
                    ordTableBody.innerHTML = '';
                    data.orders.forEach(o => {
                        const row = document.createElement('tr');
                        const brokerTag = o.broker_tag || (o.broker && o.broker.toString()) || 'PAPER';
                        const currency = brokerTag === 'DELTA' ? '$' : '₹';
                        row.innerHTML = `
                            <td><code>${(o.id || '').substring(0, 8)}...</code></td>
                            <td><strong>${o.symbol}</strong></td>
                            <td>${o.side === 1 || o.side === 'Buy' ? 'BUY' : 'SELL'}</td>
                            <td>${o.quantity || o.qty || 0}</td>
                            <td>${currency}${(o.avg_price || o.limit_price || 0).toFixed(2)}</td>
                            <td><span style="font-weight:600; color: ${o.status === 'FILLED' ? 'var(--success)' : 'var(--text-muted)'}">${o.status}</span></td>
                            <td><span style="font-weight:600; font-size:0.8rem; color: ${brokerTag === 'DELTA' ? '#fbbf24' : brokerTag === 'FYERS' ? 'var(--success)' : 'var(--primary)'}">${brokerTag}</span></td>
                        `;
                        ordTableBody.appendChild(row);
                    });
                } else {
                    ordTableBody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--text-muted);">No recent orders found</td></tr>`;
                }
            } catch (e) {
                console.error("Failed to fetch orders: ", e);
            }
        }

        // Handle Activation Click
        document.getElementById('activate-token-btn').addEventListener('click', async () => {
            const input = document.getElementById('auth-code-input').value.trim();
            const msg = document.getElementById('activation-message');
            msg.textContent = '';
            
            if (!input) {
                msg.textContent = 'Please paste the authorization code first.';
                msg.style.color = 'var(--danger)';
                return;
            }

            let authCode = input;
            if (input.includes('auth_code=')) {
                const urlParams = new URLSearchParams(input.split('?')[1]);
                authCode = urlParams.get('auth_code') || input;
            }

            msg.textContent = 'Authenticating with Fyers API...';
            msg.style.color = 'var(--warning)';

            try {
                const response = await fetch('/broker/login_token', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ auth_code: authCode })
                });
                
                const data = await response.json();
                if (response.ok && data.status === 'success') {
                    msg.textContent = 'Fyers Broker activated successfully!';
                    msg.style.color = 'var(--success)';
                    setTimeout(() => {
                        checkHealth();
                        fetchPortfolio();
                        fetchOrders();
                    }, 1000);
                } else {
                    msg.textContent = `Activation Failed: ${data.detail || 'check parameters'}`;
                    msg.style.color = 'var(--danger)';
                }
            } catch (e) {
                msg.textContent = `Request error: ${e.message}`;
                msg.style.color = 'var(--danger)';
            }
        });

        // Downloader Logic
        const yearSelect = document.getElementById('download-year-select');
        const symbolSelect = document.getElementById('download-symbol-select');
        const statusVal = document.getElementById('downloader-status-val');
        const progressVal = document.getElementById('downloader-progress-val');
        const timeVal = document.getElementById('downloader-time-val');
        const timeRow = document.getElementById('downloader-time-row');
        const actionBtn = document.getElementById('downloader-action-btn');
        let statusPollInterval = null;

        // Dynamic 25-years options populating
        const currentYear = new Date().getFullYear();
        yearSelect.innerHTML = '';
        for (let y = currentYear; y > currentYear - 25; y--) {
            const opt = document.createElement('option');
            opt.value = y;
            opt.textContent = y;
            yearSelect.appendChild(opt);
        }

        async function checkDownloadStatus() {
            const symbol = symbolSelect.value;
            const year = yearSelect.value;
            if (!symbol || !year) return;
            try {
                const res = await fetch(`/database/download-status?year=${year}&symbol=${encodeURIComponent(symbol)}`);
                const data = await res.json();
                if (data.status === 'success') {
                    const ds = data.download_status;
                    progressVal.textContent = `${data.completed_days} / ${data.total_days} days`;
                    
                    if (ds === 'COMPLETED') {
                        statusVal.textContent = 'COMPLETED';
                        statusVal.style.color = 'var(--success)';
                        timeRow.style.display = 'none';
                        actionBtn.textContent = 'Download Data (Excel)';
                        actionBtn.disabled = false;
                        actionBtn.onclick = () => {
                            window.location.href = `/database/download?year=${year}&symbol=${encodeURIComponent(symbol)}`;
                        };
                        if (statusPollInterval) {
                            clearInterval(statusPollInterval);
                            statusPollInterval = null;
                        }
                    } else if (ds === 'IN_PROGRESS') {
                        statusVal.textContent = 'IN_PROGRESS';
                        statusVal.style.color = 'var(--warning)';
                        timeRow.style.display = 'flex';
                        timeVal.textContent = data.time_remaining;
                        actionBtn.textContent = `Collecting (${data.time_remaining})...`;
                        actionBtn.disabled = true;
                        actionBtn.onclick = null;
                        
                        if (!statusPollInterval) {
                            statusPollInterval = setInterval(checkDownloadStatus, 2000);
                        }
                    } else {
                        statusVal.textContent = 'NOT_STARTED';
                        statusVal.style.color = 'var(--danger)';
                        timeRow.style.display = 'flex';
                        timeVal.textContent = data.time_remaining;
                        actionBtn.textContent = 'Start Data Collection';
                        actionBtn.disabled = false;
                        actionBtn.onclick = startDataCollection;
                        if (statusPollInterval) {
                            clearInterval(statusPollInterval);
                            statusPollInterval = null;
                        }
                    }
                }
            } catch (e) {
                console.error("Error fetching download status", e);
            }
        }

        async function startDataCollection() {
            const symbol = symbolSelect.value;
            const year = yearSelect.value;
            if (!symbol || !year) return;
            actionBtn.disabled = true;
            actionBtn.textContent = 'Starting...';
            try {
                const res = await fetch('/database/start-download', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ year: parseInt(year), symbol })
                });
                const data = await res.json();
                if (data.status === 'success') {
                    checkDownloadStatus();
                    fetchPipelineJobs();
                } else {
                    alert('Failed to start download: ' + (data.message || 'unknown error'));
                    checkDownloadStatus();
                }
            } catch (e) {
                console.error("Error starting collection", e);
                checkDownloadStatus();
            }
        }

        async function fetchLiveStatus() {
            try {
                const res = await fetch('/live-status');
                const data = await res.json();
                const niftyPulse = document.getElementById('nifty-pulse-dot');
                    if (data.status === 'success' && data.live_status) {
                    const ls = data.live_status;
                    document.getElementById('live-nifty-val').textContent = `₹${ls.nifty_price.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                    document.getElementById('live-vix-val').textContent = ls.vix.toFixed(2);

                    // Helper to set crypto price in UI
                    const setCryptoPrice = (coin, price) => {
                        if (!price || price <= 0) return;
                        const fmt = `$${price.toLocaleString('en-US', {minimumFractionDigits: 2})}`;
                        document.getElementById(`live-${coin}-val`).textContent = fmt;
                        const card = document.getElementById(`crypto-${coin}-card-price`);
                        if (card) card.textContent = fmt;
                    };

                    // Try from live-status first
                    setCryptoPrice('btc', ls.btc_price);
                    setCryptoPrice('eth', ls.eth_price);
                    setCryptoPrice('sol', ls.sol_price);

                    // Fallback: if any crypto price is still 0, fetch from /crypto-prices endpoint
                    if (!ls.btc_price || !ls.eth_price || !ls.sol_price) {
                        try {
                            const cpRes = await fetch('/crypto-prices');
                            const cpData = await cpRes.json();
                            if (cpData.status === 'success' && cpData.prices) {
                                if (!ls.btc_price) setCryptoPrice('btc', cpData.prices.BTC);
                                if (!ls.eth_price) setCryptoPrice('eth', cpData.prices.ETH);
                                if (!ls.sol_price) setCryptoPrice('sol', cpData.prices.SOL);
                            }
                        } catch(e) {}
                    }

                    // Last resort: direct Delta Exchange API fetch (client-side)
                    const btcEl = document.getElementById('live-btc-val');
                    if (btcEl && btcEl.textContent === '$0.00') {
                        try {
                            const dRes = await fetch('https://api.delta.exchange/v2/tickers/BTCUSDT');
                            const dData = await dRes.json();
                            if (dData.success && dData.result) {
                                const mp = parseFloat(dData.result.mark_price) || parseFloat(dData.result.close) || 0;
                                if (mp > 0) setCryptoPrice('btc', mp);
                            }
                            const eRes = await fetch('https://api.delta.exchange/v2/tickers/ETHUSDT');
                            const eData = await eRes.json();
                            if (eData.success && eData.result) {
                                const ep = parseFloat(eData.result.mark_price) || parseFloat(eData.result.close) || 0;
                                if (ep > 0) setCryptoPrice('eth', ep);
                            }
                            const sRes = await fetch('https://api.delta.exchange/v2/tickers/SOLUSDT');
                            const sData = await sRes.json();
                            if (sData.success && sData.result) {
                                const sp = parseFloat(sData.result.mark_price) || parseFloat(sData.result.close) || 0;
                                if (sp > 0) setCryptoPrice('sol', sp);
                            }
                        } catch(e) {}
                    }

                    updateCryptoCalc();
                    
                    document.getElementById('opp-target-option').textContent = ls.target_option;
                    document.getElementById('opp-quality-score').textContent = ls.quality_score.toFixed(1);
                    
                    if (ls.quality_score >= 75.0) {
                        document.getElementById('opp-quality-score').style.color = 'var(--success)';
                    } else if (ls.quality_score >= 50.0) {
                        document.getElementById('opp-quality-score').style.color = 'var(--warning)';
                    } else {
                        document.getElementById('opp-quality-score').style.color = 'var(--danger)';
                    }

                    document.getElementById('opp-confidence').textContent = `${ls.opportunity_confidence.toFixed(1)}%`;
                    document.getElementById('opp-probability').textContent = `${ls.opportunity_probability.toFixed(1)}%`;
                    document.getElementById('opp-ml-confidence').textContent = `${ls.ml_confidence.toFixed(1)}%`;
                    
                    const badge = document.getElementById('opp-decision-badge');
                    badge.textContent = ls.decision.toUpperCase();
                    badge.className = 'status-badge'; 
                    if (ls.decision === 'Trade') {
                        badge.classList.add('badge-trade');
                    } else if (ls.decision === 'ReduceSize') {
                        badge.classList.add('badge-reduce');
                    } else if (ls.decision === 'Cancel') {
                        badge.classList.add('badge-cancel');
                    } else {
                        badge.classList.add('badge-wait');
                    }

                    const lastTime = new Date(ls.timestamp);
                    const diff = (new Date() - lastTime) / 1000;
                    if (isNaN(diff) || diff < 60) {
                        niftyPulse.style.backgroundColor = 'var(--success)';
                        niftyPulse.style.boxShadow = '0 0 10px var(--success)';
                        document.getElementById('last-update-time').textContent = 'LIVE CONNECTED';
                        document.getElementById('last-update-time').style.color = 'var(--success)';
                        document.getElementById('last-update-time').style.background = 'rgba(16, 185, 129, 0.1)';
                    } else {
                        niftyPulse.style.backgroundColor = 'var(--warning)';
                        niftyPulse.style.boxShadow = '0 0 10px var(--warning)';
                        document.getElementById('last-update-time').textContent = `STANDBY: ${Math.round(diff)}s AGO`;
                        document.getElementById('last-update-time').style.color = 'var(--warning)';
                        document.getElementById('last-update-time').style.background = 'rgba(245, 158, 11, 0.1)';
                    }
                } else {
                    niftyPulse.style.backgroundColor = 'var(--danger)';
                    niftyPulse.style.boxShadow = '0 0 10px var(--danger)';
                    document.getElementById('last-update-time').textContent = 'WORKER OFFLINE';
                    document.getElementById('last-update-time').style.color = 'var(--danger)';
                    document.getElementById('last-update-time').style.background = 'rgba(239, 68, 68, 0.1)';
                }
            } catch (e) {
                console.error("Failed to fetch live status: ", e);
            }
        }

        function updateCryptoCalc() {
            const symElem = document.getElementById('crypto-order-symbol');
            if (!symElem) return;
            const symbol = symElem.value;
            const side = parseInt(document.getElementById('crypto-order-side').value);
            const qty = parseFloat(document.getElementById('crypto-order-qty').value) || 1;
            const levRange = document.getElementById('crypto-order-leverage');
            const maxLev = symbol.includes('SOL') ? 100 : 200;
            levRange.max = maxLev;
            if (parseInt(levRange.value) > maxLev) levRange.value = maxLev;
            const leverage = parseInt(levRange.value);
            document.getElementById('crypto-lev-label').textContent = `${leverage}x`;

            let estPrice = 0;
            if (symbol.includes('BTC')) {
                const text = document.getElementById('live-btc-val').textContent.replace('$', '').replace(/,/g, '');
                estPrice = parseFloat(text) || 95000.0;
            } else if (symbol.includes('ETH')) {
                const text = document.getElementById('live-eth-val').textContent.replace('$', '').replace(/,/g, '');
                estPrice = parseFloat(text) || 3300.0;
            } else {
                const text = document.getElementById('live-sol-val').textContent.replace('$', '').replace(/,/g, '');
                estPrice = parseFloat(text) || 220.0;
            }

            const notional = estPrice * qty;
            const initMargin = notional / leverage;
            const maintMarginRate = symbol.includes('SOL') ? 0.01 : 0.005;
            const maintMargin = notional * maintMarginRate;

            let liqPrice = 0;
            if (side === 1) {
                liqPrice = estPrice * (1.0 - (1.0 / leverage) + maintMarginRate);
            } else {
                liqPrice = estPrice * (1.0 + (1.0 / leverage) - maintMarginRate);
            }

            document.getElementById('calc-init-margin').textContent = `$${initMargin.toFixed(2)}`;
            document.getElementById('calc-maint-margin').textContent = `$${maintMargin.toFixed(2)}`;
            document.getElementById('calc-liq-price').textContent = `$${liqPrice.toFixed(2)}`;

            if (symbol.includes('BTC')) {
                const c = document.getElementById('crypto-btc-card-price');
                if (c) c.textContent = `$${estPrice.toLocaleString('en-US', {minimumFractionDigits: 2})}`;
            } else if (symbol.includes('ETH')) {
                const c = document.getElementById('crypto-eth-card-price');
                if (c) c.textContent = `$${estPrice.toLocaleString('en-US', {minimumFractionDigits: 2})}`;
            } else if (symbol.includes('SOL')) {
                const c = document.getElementById('crypto-sol-card-price');
                if (c) c.textContent = `$${estPrice.toLocaleString('en-US', {minimumFractionDigits: 2})}`;
            }
        }

        function openCryptoOrderModal(symbol, sideStr) {
            document.getElementById('crypto-order-symbol').value = symbol;
            document.getElementById('crypto-order-side').value = sideStr;
            switchTab('tab-crypto');
            updateCryptoCalc();
        }

        async function submitCryptoOrder(e) {
            e.preventDefault();
            const symbol = document.getElementById('crypto-order-symbol').value;
            const side = parseInt(document.getElementById('crypto-order-side').value);
            const qty = parseInt(document.getElementById('crypto-order-qty').value);
            const orderType = parseInt(document.getElementById('crypto-order-type').value);

            try {
                const res = await fetch('/order', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        symbol: symbol,
                        qty: qty,
                        type: orderType,
                        side: side,
                        limitPrice: 0,
                        stopPrice: 0
                    })
                });
                const data = await res.json();
                if (data.status === 'success' || data.order_id) {
                    alert(`✅ Order submitted successfully to Delta Exchange! Order ID: ${data.order_id || 'EXEC_OK'}`);
                    fetchPortfolio();
                    fetchOrders();
                } else {
                    alert(`⚠️ Order execution message: ${data.message || data.detail || 'check parameters'}`);
                }
            } catch (err) {
                alert(`❌ Failed to send order: ${err.message}`);
            }
        }

        async function fetchPipelineJobs() {
            try {
                const res = await fetch('/database/jobs');
                const data = await res.json();
                const tbody = document.querySelector('#pipeline-jobs-table tbody');
                const alertBox = document.getElementById('downloader-alert-box');
                if (!tbody) return;
                
                let showPermissionAlert = false;
                let permissionAlertMessage = "";
                
                if (data.status === 'success' && data.jobs && data.jobs.length > 0) {
                    tbody.innerHTML = '';
                    data.jobs.forEach(j => {
                        const row = document.createElement('tr');
                        let statusColor = 'var(--text-muted)';
                        if (j.status === 'COMPLETED') statusColor = 'var(--success)';
                        else if (j.status === 'IN_PROGRESS') statusColor = 'var(--warning)';
                        else if (j.status.startsWith('FAILED')) {
                            statusColor = 'var(--danger)';
                            if (j.status.includes("Additional permission required") || j.status.includes("permission") || j.status.includes("-15") || j.status.includes("-17")) {
                                showPermissionAlert = true;
                                permissionAlertMessage = j.status;
                            }
                        }
                        
                        row.innerHTML = `
                            <td style="padding: 0.5rem; font-size: 0.8rem;"><strong>${j.symbol}</strong></td>
                            <td style="padding: 0.5rem; font-size: 0.8rem;">${j.from_date.substring(0, 4)}</td>
                            <td style="padding: 0.5rem; color: ${statusColor}; font-weight: 600; font-size: 0.8rem;">${j.status}</td>
                        `;
                        tbody.appendChild(row);
                    });
                } else {
                    tbody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: var(--text-muted); padding: 0.75rem; font-size: 0.8rem;">No active pipeline jobs</td></tr>';
                }
                
                if (alertBox) alertBox.style.display = 'none';
            } catch (e) {
                console.error("Failed to fetch jobs: ", e);
            }
        }

        async function fetchCandlesPreview() {
            const symbolSelect = document.getElementById('download-symbol-select');
            const yearSelect = document.getElementById('download-year-select');
            if (!symbolSelect || !yearSelect) return;
            const symbol = symbolSelect.value;
            const year = yearSelect.value;
            if (!symbol || !year) return;
            try {
                const res = await fetch(`/database/candles-preview?symbol=${encodeURIComponent(symbol)}&year=${year}`);
                const data = await res.json();
                const tbody = document.querySelector('#candles-preview-table tbody');
                if (!tbody) return;
                if (data.status === 'success' && data.candles && data.candles.length > 0) {
                    tbody.innerHTML = '';
                    data.candles.forEach(c => {
                        const row = document.createElement('tr');
                        const timeStr = new Date(c.timestamp).toLocaleTimeString('en-IN', {hour: '2-digit', minute:'2-digit', second: '2-digit'});
                        row.innerHTML = `
                            <td style="padding: 0.4rem; font-size: 0.75rem;">${timeStr}</td>
                            <td style="padding: 0.4rem; font-size: 0.75rem;">₹${c.open.toFixed(2)}</td>
                            <td style="padding: 0.4rem; font-size: 0.75rem;">₹${c.high.toFixed(2)}</td>
                            <td style="padding: 0.4rem; font-size: 0.75rem;">₹${c.low.toFixed(2)}</td>
                            <td style="padding: 0.4rem; font-size: 0.75rem;">₹${c.close.toFixed(2)}</td>
                            <td style="padding: 0.4rem; font-size: 0.75rem;">${c.volume}</td>
                        `;
                        tbody.appendChild(row);
                    });
                } else {
                    tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--text-muted); padding: 0.75rem; font-size: 0.8rem;">No candles found in database</td></tr>';
                }
            } catch (e) {
                console.error("Failed to fetch candles preview: ", e);
            }
        }

        yearSelect.addEventListener('change', () => {
            if (statusPollInterval) {
                clearInterval(statusPollInterval);
                statusPollInterval = null;
            }
            checkDownloadStatus();
            fetchCandlesPreview();
        });
        symbolSelect.addEventListener('change', () => {
            if (statusPollInterval) {
                clearInterval(statusPollInterval);
                statusPollInterval = null;
            }
            checkDownloadStatus();
            fetchCandlesPreview();
        });

        async function fetchCryptoSignals() {
            try {
                const res = await fetch('/live-status/crypto');
                const data = await res.json();
                if (data.status === 'success' && data.signals) {
                    const symbols = ['BTC', 'ETH', 'SOL'];
                    symbols.forEach(sym => {
                        const s = data.signals[sym];
                        if (!s) return;
                        const key = sym.toLowerCase();
                        
                        const priceElem = document.getElementById(`${key}-sig-price`);
                        if (priceElem && s.price > 0) {
                            priceElem.textContent = `$${s.price.toLocaleString('en-US', {minimumFractionDigits: 2})}`;
                        }
                        
                        const badge = document.getElementById(`${key}-sig-badge`);
                        if (badge) {
                            badge.textContent = s.action || 'HOLD';
                            badge.className = 'status-badge ' + (s.action === 'ENTRY' ? 'badge-trade' : s.action === 'EXIT' ? 'badge-cancel' : 'badge-wait');
                        }
                        
                        const ema9 = document.getElementById(`${key}-ema9`);
                        if (ema9) ema9.textContent = s.ema9 > 0 ? `$${s.ema9.toFixed(2)}` : '—';
                        
                        const ema21 = document.getElementById(`${key}-ema21`);
                        if (ema21) ema21.textContent = s.ema21 > 0 ? `$${s.ema21.toFixed(2)}` : '—';
                        
                        const atr = document.getElementById(`${key}-atr`);
                        if (atr) atr.textContent = s.atr > 0 ? `$${s.atr.toFixed(2)}` : '—';
                        
                        const dir = document.getElementById(`${key}-direction-lbl`);
                        if (dir) {
                            dir.textContent = s.direction || 'FLAT';
                            dir.style.color = s.direction === 'LONG' ? 'var(--success)' : s.direction === 'SHORT' ? 'var(--danger)' : 'var(--text-muted)';
                        }
                    });
                }
            } catch (e) {
                console.error("Failed to fetch crypto signals: ", e);
            }
        }

        // Initialize and poll status
        fetchAuthUrl();
        checkHealth();
        fetchPortfolio();
        fetchOrders();
        checkDownloadStatus();
        fetchLiveStatus();
        fetchCryptoLiveQuotes();
        fetchCryptoSignals();
        fetchPipelineJobs();
        fetchCandlesPreview();
        
        setInterval(fetchLiveStatus, 1000);
        setInterval(fetchCryptoLiveQuotes, 3000);
        setInterval(fetchCryptoSignals, 3000);
        setInterval(() => {
            fetchPortfolio();
            fetchOrders();
            fetchPipelineJobs();
            fetchCandlesPreview();
        }, 5000);
    </script>
</body>
</html>
"##)
}

async fn favicon_handler() -> impl IntoResponse {
    axum::http::StatusCode::NO_CONTENT
}

async fn health_handler(Extension(state): Extension<Arc<AppState>>) -> Json<serde_json::Value> {
    let broker_ok = state.broker.profile().await.is_ok();
    let cp = state.crypto_prices.lock().await;
    Json(serde_json::json!({
        "status": "healthy",
        "broker_connection": if broker_ok { "connected" } else { "offline_simulated" },
        "system": "PRICE Predictive Risk Intelligence & Capital Engine",
        "version": "1.1.0",
        "crypto_cache": {
            "btc": cp.get("BTC").copied().unwrap_or(0.0),
            "eth": cp.get("ETH").copied().unwrap_or(0.0),
            "sol": cp.get("SOL").copied().unwrap_or(0.0)
        }
    }))
}

async fn crypto_prices_handler(Extension(state): Extension<Arc<AppState>>) -> Json<serde_json::Value> {
    let cp = state.crypto_prices.lock().await;
    Json(serde_json::json!({
        "status": "success",
        "prices": {
            "BTC": cp.get("BTC").copied().unwrap_or(0.0),
            "ETH": cp.get("ETH").copied().unwrap_or(0.0),
            "SOL": cp.get("SOL").copied().unwrap_or(0.0)
        },
        "cache_size": cp.len(),
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

async fn portfolio_handler(Extension(state): Extension<Arc<AppState>>) -> Json<serde_json::Value> {
    // ── Fyers / HybridBroker (live Fyers + embedded paper) ─────────────────────
    let (fyers_funds, hybrid_paper_funds) =
        if let Some(hybrid) = state.broker.as_any().downcast_ref::<price_broker::HybridBroker>() {
            (hybrid.live.funds().await.ok(), hybrid.paper.funds().await.ok())
        } else {
            (state.broker.funds().await.ok(), None)
        };

    // ── Delta Exchange (live perpetuals account) ────────────────────────────────
    let delta_funds     = state.delta_client.funds().await.ok();
    let delta_positions = state.delta_client.positions().await.unwrap_or_default();

    // ── Paper broker (standalone 10 000 INR virtual capital) ───────────────────
    let paper_funds = if let Some(pf) = hybrid_paper_funds {
        Some(pf)
    } else {
        state.paper_broker.funds().await.ok()
    };
    let paper_positions = state.paper_broker.positions().await.unwrap_or_default();

    // ── Fyers positions (from live broker) ─────────────────────────────────────
    let fyers_positions = state.broker.positions().await.unwrap_or_default();
    let fyers_holdings  = state.broker.holdings().await.unwrap_or_default();

    // ── Merge all positions with broker tag injected into JSON ─────────────────
    let tag_positions = |positions: Vec<price_broker::Position>, broker: &str| -> Vec<serde_json::Value> {
        positions.into_iter().map(|p| {
            let mut v = serde_json::to_value(&p).unwrap_or_default();
            if let serde_json::Value::Object(ref mut m) = v {
                m.insert("broker".to_string(), serde_json::Value::String(broker.to_string()));
            }
            v
        }).collect()
    };

    let mut all_positions: Vec<serde_json::Value> = Vec::new();
    all_positions.extend(tag_positions(fyers_positions, "FYERS"));
    all_positions.extend(tag_positions(delta_positions, "DELTA"));
    all_positions.extend(tag_positions(paper_positions, "PAPER"));

    Json(serde_json::json!({
        "live_funds":  fyers_funds,
        "delta_funds": delta_funds,
        "paper_funds": paper_funds,
        "positions":   all_positions,
        "holdings":    fyers_holdings
    }))
}

async fn orders_handler(Extension(state): Extension<Arc<AppState>>) -> Json<serde_json::Value> {
    let tag_orders = |orders: Vec<price_broker::Order>, broker_tag: &str| -> Vec<serde_json::Value> {
        orders.into_iter().map(|o| {
            let mut v = serde_json::to_value(&o).unwrap_or_default();
            if let serde_json::Value::Object(ref mut m) = v {
                m.insert("broker_tag".to_string(), serde_json::Value::String(broker_tag.to_string()));
            }
            v
        }).collect()
    };
    let fyers_orders = state.broker.orderbook().await.unwrap_or_default();
    let delta_orders = state.delta_client.orderbook().await.unwrap_or_default();
    let paper_orders = state.paper_broker.orderbook().await.unwrap_or_default();

    let mut all_orders: Vec<serde_json::Value> = Vec::new();
    all_orders.extend(tag_orders(fyers_orders, "FYERS"));
    all_orders.extend(tag_orders(delta_orders, "DELTA"));
    all_orders.extend(tag_orders(paper_orders, "PAPER"));
    Json(serde_json::json!({ "orders": all_orders }))
}

async fn trades_handler(Extension(state): Extension<Arc<AppState>>) -> Json<serde_json::Value> {
    let trades = state.broker.trades().await.unwrap_or_default();
    Json(serde_json::json!({
        "trades": trades
    }))
}

#[derive(serde::Deserialize)]
struct ManualOrder {
    symbol: String,
    qty: i32,
    side: String, // "BUY" or "SELL"
    limit_price: f64,
}

async fn place_order_handler(
    Extension(state): Extension<Arc<AppState>>,
    Json(payload): Json<ManualOrder>,
) -> Json<serde_json::Value> {
    let side = if payload.side.to_uppercase() == "BUY" { Side::Buy } else { Side::Sell };
    let req = OrderRequest {
        symbol: payload.symbol.clone(),
        qty: payload.qty,
        r#type: 1, // Always limit order for futures
        side,
        limit_price: payload.limit_price,
        stop_price: 0.0,
        leverage: None,
        reduce_only: None,
        post_only: None,
        client_id: None,
        time_in_force: None,
    };

    // Route crypto perpetuals to Delta Exchange; everything else to Fyers
    let is_crypto = payload.symbol.contains("PERP") || payload.symbol.starts_with("BTC") ||
        payload.symbol.starts_with("ETH") || payload.symbol.starts_with("SOL");
    let broker: &dyn price_broker::Broker = if is_crypto { state.delta_client.as_ref() } else { state.broker.as_ref() };

    match broker.place_order(req).await {
        Ok(resp) => Json(serde_json::json!({
            "status": "success",
            "order_id": resp.order_id,
            "message": resp.message
        })),
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "message": e.to_string()
        })),
    }
}

async fn auth_url_handler(
    Extension(state): Extension<Arc<AppState>>,
) -> impl axum::response::IntoResponse {
    let client = reqwest::Client::new();
    match client.get(format!("{}/auth_url", state.python_broker_url)).send().await {
        Ok(res) => {
            let status = axum::http::StatusCode::from_u16(res.status().as_u16())
                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
            let body = res.json::<serde_json::Value>().await.unwrap_or_else(|_| {
                serde_json::json!({"status": "error", "detail": "Failed to parse Python response"})
            });
            (status, Json(body)).into_response()
        }
        Err(e) => {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"status": "error", "detail": e.to_string()})),
            ).into_response()
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
struct LoginTokenRequest {
    auth_code: String,
}

async fn login_token_handler(
    Extension(state): Extension<Arc<AppState>>,
    Json(payload): Json<LoginTokenRequest>,
) -> impl axum::response::IntoResponse {
    let client = reqwest::Client::new();
    match client.post(format!("{}/login_token", state.python_broker_url))
        .json(&payload)
        .send()
        .await
    {
        Ok(res) => {
            let status = axum::http::StatusCode::from_u16(res.status().as_u16())
                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
            let body = res.json::<serde_json::Value>().await.unwrap_or_else(|_| {
                serde_json::json!({"status": "error", "detail": "Failed to parse Python response"})
            });
            (status, Json(body)).into_response()
        }
        Err(e) => {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"status": "error", "detail": e.to_string()})),
            ).into_response()
        }
    }
}

async fn metrics_handler(Extension(state): Extension<Arc<AppState>>) -> impl IntoResponse {
    let mut body = String::new();

    // 1. Fetch funds metrics
    if let Ok(funds) = state.broker.funds().await {
        body.push_str("# HELP price_portfolio_balance_rupees Total available portfolio balance in rupees\n");
        body.push_str("# TYPE price_portfolio_balance_rupees gauge\n");
        body.push_str(&format!("price_portfolio_balance_rupees {:.2}\n", funds.available_balance));

        body.push_str("# HELP price_portfolio_utilized_margin_rupees Total utilized margin in rupees\n");
        body.push_str("# TYPE price_portfolio_utilized_margin_rupees gauge\n");
        body.push_str(&format!("price_portfolio_utilized_margin_rupees {:.2}\n", funds.utilised_balance));

        body.push_str("# HELP price_portfolio_limit_amount_rupees Total limit amount in rupees\n");
        body.push_str("# TYPE price_portfolio_limit_amount_rupees gauge\n");
        body.push_str(&format!("price_portfolio_limit_amount_rupees {:.2}\n", funds.limit_amount));
    }

    // 2. Fetch position metrics
    if let Ok(positions) = state.broker.positions().await {
        body.push_str("# HELP price_open_positions_count Total count of open positions\n");
        body.push_str("# TYPE price_open_positions_count gauge\n");
        body.push_str(&format!("price_open_positions_count {}\n", positions.len()));
        
        let mut total_pnl = 0.0;
        for p in &positions {
            total_pnl += p.pnl;
        }
        body.push_str("# HELP price_open_positions_pnl_rupees Total unrealized PnL of open positions\n");
        body.push_str("# TYPE price_open_positions_pnl_rupees gauge\n");
        body.push_str(&format!("price_open_positions_pnl_rupees {:.2}\n", total_pnl));
    }

    // 3. Fetch orders & trades metrics
    if let Ok(orders) = state.broker.orderbook().await {
        body.push_str("# HELP price_orders_count Total count of orders in the orderbook\n");
        body.push_str("# TYPE price_orders_count counter\n");
        body.push_str(&format!("price_orders_count {}\n", orders.len()));
    }

    if let Ok(trades) = state.broker.trades().await {
        body.push_str("# HELP price_trades_count Total count of executed trades in the tradebook\n");
        body.push_str("# TYPE price_trades_count counter\n");
        body.push_str(&format!("price_trades_count {}\n", trades.len()));
    }

    axum::response::Response::builder()
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(axum::body::Body::from(body))
        .unwrap()
}

#[derive(serde::Deserialize)]
struct StatusParams {
    year: i32,
    symbol: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct StartParams {
    year: i32,
    symbol: String,
}

#[derive(serde::Deserialize)]
struct DownloadParams {
    year: i32,
    symbol: String,
}

async fn download_status_handler(
    Extension(state): Extension<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<StatusParams>,
) -> impl IntoResponse {
    let year = params.year;
    let symbol = params.symbol;

    let expected_days = get_trading_days(year);
    let expected_count = expected_days.len() as i64;
    let completed_count = get_completed_jobs_count(&state.db.pool, &symbol, year).await.unwrap_or(0);
    
    let active_count = sqlx::query(
        "SELECT count(*) as count 
         FROM download_jobs 
         WHERE symbol = $1 
           AND EXTRACT(YEAR FROM from_date)::integer = $2 
           AND (status = 'PENDING' OR status = 'IN_PROGRESS')"
    )
    .bind(&symbol)
    .bind(year)
    .fetch_one(&state.db.pool)
    .await
    .map(|r| r.get::<i64, _>("count"))
    .unwrap_or(0);

    let (status, time_remaining) = if completed_count >= expected_count {
        ("COMPLETED".to_string(), "00:00".to_string())
    } else if active_count > 0 {
        let remaining_days = expected_count - completed_count;
        let seconds = remaining_days * 5;
        ("IN_PROGRESS".to_string(), format_time_remaining(seconds))
    } else {
        ("NOT_STARTED".to_string(), "00:00".to_string())
    };

    Json(serde_json::json!({
        "status": "success",
        "download_status": status,
        "completed_days": completed_count,
        "total_days": expected_count,
        "time_remaining": time_remaining
    }))
}

async fn start_download_handler(
    Extension(state): Extension<Arc<AppState>>,
    Json(payload): Json<StartParams>,
) -> impl IntoResponse {
    use chrono::Datelike;
    let year = payload.year;
    let symbol = payload.symbol;

    // Delete any existing download jobs for the symbol and year to start clean
    let _ = sqlx::query(
        "DELETE FROM download_jobs 
         WHERE symbol = $1 
           AND EXTRACT(YEAR FROM from_date)::integer = $2"
    )
    .bind(&symbol)
    .bind(year)
    .execute(&state.db.pool)
    .await;

    let start_date = chrono::NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
    let now = chrono::Utc::now().naive_utc().date();
    let end_date = if year == now.year() {
        now
    } else {
        chrono::NaiveDate::from_ymd_opt(year, 12, 31).unwrap()
    };

    let mut current_start = start_date;
    while current_start <= end_date {
        let current_end = (current_start + chrono::Duration::days(6)).min(end_date);
        
        let _ = sqlx::query(
            "INSERT INTO download_jobs (symbol, from_date, to_date, status, last_updated) 
             VALUES ($1, $2, $3, 'PENDING', NOW()) 
             ON CONFLICT (symbol, from_date, to_date) DO UPDATE
             SET status = CASE WHEN download_jobs.status = 'COMPLETED' THEN 'COMPLETED' ELSE 'PENDING' END,
                 last_updated = CASE WHEN download_jobs.status = 'COMPLETED' THEN download_jobs.last_updated ELSE NOW() END"
        )
        .bind(&symbol)
        .bind(current_start)
        .bind(current_end)
        .execute(&state.db.pool)
        .await;

        current_start = current_end + chrono::Duration::days(1);
    }

    Json(serde_json::json!({
        "status": "success",
        "message": "Download pipeline started for requested data"
    }))
}

async fn download_handler(
    Extension(state): Extension<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<DownloadParams>,
) -> impl IntoResponse {
    let year = params.year;
    let symbol = params.symbol;

    let expected_days = get_trading_days(year);
    let expected_count = expected_days.len() as i64;
    let completed_count = get_completed_jobs_count(&state.db.pool, &symbol, year).await.unwrap_or(0);

    if completed_count < expected_count {
        let remaining_days = expected_count - completed_count;
        let seconds = remaining_days * 5;
        let time_remaining = format_time_remaining(seconds);
        return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::response::Html(format!(
                "<h3>Data is still being collected at VPS. As per current rate it will take {} time.</h3>",
                time_remaining
            ))
        ).into_response();
    }

    // Query candles
    let candles_query = sqlx::query(
        "SELECT timestamp, open, high, low, close, volume 
         FROM candles 
         WHERE symbol = $1 
           AND EXTRACT(YEAR FROM timestamp)::integer = $2 
         ORDER BY timestamp ASC"
    )
    .bind(&symbol)
    .bind(year)
    .fetch_all(&state.db.pool)
    .await;

    match candles_query {
        Ok(rows) => {
            let mut csv_data = String::new();
            csv_data.push_str("Timestamp (UTC),Open,High,Low,Close,Volume\n");
            for r in rows {
                let ts: chrono::DateTime<chrono::Utc> = r.get("timestamp");
                let open: f64 = r.get("open");
                let high: f64 = r.get("high");
                let low: f64 = r.get("low");
                let close: f64 = r.get("close");
                let vol: i64 = r.get("volume");
                csv_data.push_str(&format!(
                    "{},{},{},{},{},{}\n",
                    ts.to_rfc3339(), open, high, low, close, vol
                ));
            }

            let filename = format!("{}_{}.csv", symbol.replace(":", "_"), year);
            axum::response::Response::builder()
                .status(axum::http::StatusCode::OK)
                .header("content-type", "text/csv")
                .header("content-disposition", format!("attachment; filename=\"{}\"", filename))
                .body(axum::body::Body::from(csv_data))
                .unwrap()
                .into_response()
        }
        Err(e) => {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to retrieve data: {}", e)
            ).into_response()
        }
    }
}

fn get_trading_days(year: i32) -> Vec<chrono::NaiveDate> {
    use chrono::{NaiveDate, Datelike, Weekday};
    let start_date = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
    let now = chrono::Utc::now().naive_utc().date();
    let end_date = if year == now.year() {
        now
    } else {
        NaiveDate::from_ymd_opt(year, 12, 31).unwrap()
    };

    // Standard fixed-date NSE holidays
    let fixed_holidays = vec![
        (1, 26),  // Republic Day
        (5, 1),   // Maharashtra Day
        (8, 15),  // Independence Day
        (10, 2),  // Gandhi Jayanti
        (12, 25), // Christmas
    ];

    let mut days = Vec::new();
    let mut curr = start_date;
    while curr <= end_date {
        let wd = curr.weekday();
        let is_weekend = wd == Weekday::Sat || wd == Weekday::Sun;
        let is_fixed_holiday = fixed_holidays.iter().any(|&(m, d)| curr.month() == m && curr.day() == d);
        if !is_weekend && !is_fixed_holiday {
            days.push(curr);
        }
        if let Some(next) = curr.succ_opt() {
            curr = next;
        } else {
            break;
        }
    }
    days
}

async fn get_completed_jobs_count(pool: &sqlx::PgPool, symbol: &str, year: i32) -> anyhow::Result<i64> {
    let row = sqlx::query(
        "SELECT count(distinct (timestamp::date)) as count 
         FROM candles 
         WHERE symbol = $1 
           AND EXTRACT(YEAR FROM timestamp)::integer = $2"
    )
    .bind(symbol)
    .bind(year)
    .fetch_one(pool)
    .await?;
    
    let count: i64 = row.get("count");
    Ok(count)
}

async fn get_downloaded_dates(pool: &sqlx::PgPool, symbol: &str, year: i32) -> anyhow::Result<std::collections::HashSet<chrono::NaiveDate>> {
    let rows = sqlx::query(
        "SELECT from_date 
         FROM download_jobs 
         WHERE symbol = $1 
           AND EXTRACT(YEAR FROM from_date)::integer = $2"
    )
    .bind(symbol)
    .bind(year)
    .fetch_all(pool)
    .await?;
    
    let mut dates = std::collections::HashSet::new();
    for r in rows {
        let d: chrono::NaiveDate = r.get("from_date");
        dates.insert(d);
    }
    Ok(dates)
}

fn format_time_remaining(seconds: i64) -> String {
    if seconds <= 0 {
        return "00:00".to_string();
    }
    let h = seconds / 3600;
    let m = (seconds % 3600) / 60;
    let s = seconds % 60;
    if h > 0 {
        format!("{}:{:02}:{:02}", h, m, s)
    } else {
        format!("{:02}:{:02}", m, s)
    }
}

async fn start_background_downloader(db: TimescaleClient, python_broker_url: String) {
    tokio::spawn(async move {
        info!("Starting background historical data downloader thread...");
        let downloader = price_backtester::HistoricalDownloader::new(&python_broker_url, db.clone());
        loop {
            // Query for the next PENDING or FAILED job
            let next_job = sqlx::query(
                "SELECT symbol, from_date, to_date 
                 FROM download_jobs 
                 WHERE status = 'PENDING'
                 ORDER BY last_updated ASC
                 LIMIT 1"
            )
            .fetch_optional(&db.pool)
            .await;

            match next_job {
                Ok(Some(row)) => {
                    let symbol: String = row.get("symbol");
                    let from_date: chrono::NaiveDate = row.get("from_date");
                    let to_date: chrono::NaiveDate = row.get("to_date");
                    // Resolve exchange from symbol mapping if available
                    let exchange = if let Ok(Some(m)) = db.get_symbol_mapping(&symbol).await {
                        m.exchange
                    } else {
                        "NSE".to_string()
                    };

                    info!("Background downloader executing job for {} (exchange {}) from {} to {}", symbol, exchange, from_date, to_date);
                    
                    // Mark as IN_PROGRESS
                    let _ = db.mark_job_status(&symbol, from_date, to_date, "IN_PROGRESS").await;

                    // Download history
                    match downloader.download_history(&symbol, &exchange, from_date, to_date).await {
                        Ok(_) => {
                            info!("Successfully finished background download job for {} from {} to {}", symbol, from_date, to_date);
                            let _ = db.mark_job_status(&symbol, from_date, to_date, "COMPLETED").await;
                        }
                        Err(e) => {
                            error!("Background downloader job failed for {} from {} to {}: {:?}", symbol, from_date, to_date, e);
                            let _ = db.mark_job_status(&symbol, from_date, to_date, &format!("FAILED: {}", e)).await;
                        }
                    }
                    
                    // Sleep for 5 seconds to comply with rate limits (1 request every 5 seconds)
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
                Ok(None) => {
                    // No pending jobs, wait before querying again
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
                Err(e) => {
                    error!("Database error in background downloader loop: {:?}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    });
}

async fn symbol_mappings_handler(
    Extension(state): Extension<Arc<AppState>>,
) -> Json<serde_json::Value> {
    match state.db.get_symbol_mappings().await {
        Ok(mappings) => Json(serde_json::json!({
            "status": "success",
            "count": mappings.len(),
            "mappings": mappings
        })),
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "message": e.to_string()
        })),
    }
}


async fn get_live_status_handler(
    Extension(state): Extension<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let mut status = state.live_status.read().unwrap().clone();
    let cp = state.crypto_prices.lock().await;
    let btc = cp.get("BTC").copied().unwrap_or(0.0);
    let eth = cp.get("ETH").copied().unwrap_or(0.0);
    let sol = cp.get("SOL").copied().unwrap_or(0.0);

    if let Some(ref mut info) = status {
        if info.btc_price == 0.0 && btc > 0.0 { info.btc_price = btc; }
        if info.eth_price == 0.0 && eth > 0.0 { info.eth_price = eth; }
        if info.sol_price == 0.0 && sol > 0.0 { info.sol_price = sol; }
    } else {
        status = Some(LiveStatusInfo {
            nifty_price: 0.0,
            vix: 0.0,
            ml_confidence: 0.0,
            opportunity_confidence: 0.0,
            opportunity_probability: 0.0,
            decision: "Wait".to_string(),
            quality_score: 0.0,
            target_option: "--".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            btc_price: btc,
            eth_price: eth,
            sol_price: sol,
        });
    }

    Json(serde_json::json!({
        "status": "success",
        "live_status": status
    }))
}

async fn post_live_status_handler(
    Extension(state): Extension<Arc<AppState>>,
    Json(payload): Json<LiveStatusInfo>,
) -> Json<serde_json::Value> {
    *state.live_status.write().unwrap() = Some(payload);
    Json(serde_json::json!({
        "status": "success"
    }))
}

/// GET /live-status/crypto — returns all per-symbol crypto signals
async fn get_crypto_signal_handler(
    Extension(state): Extension<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let signals = state.crypto_signals.read().unwrap().clone();
    let cp      = state.crypto_prices.lock().await;
    // Merge in latest live prices even if no signal has been posted yet
    let mut merged: std::collections::HashMap<String, serde_json::Value> = HashMap::new();
    for sym in &["BTC", "ETH", "SOL"] {
        let price = cp.get(*sym).copied().unwrap_or(0.0);
        if let Some(sig) = signals.get(*sym) {
            let mut v = serde_json::to_value(sig).unwrap_or_default();
            if let serde_json::Value::Object(ref mut m) = v {
                if m.get("price").and_then(|p| p.as_f64()).unwrap_or(0.0) == 0.0 && price > 0.0 {
                    m.insert("price".to_string(), serde_json::json!(price));
                }
            }
            merged.insert(sym.to_string(), v);
        } else {
            merged.insert(sym.to_string(), serde_json::json!({
                "symbol":      sym,
                "price":       price,
                "ema9":        0.0,
                "ema21":       0.0,
                "atr":         0.0,
                "direction":   "FLAT",
                "action":      "HOLD",
                "bull_cross":  false,
                "bear_cross":  false,
                "leverage":    if *sym == "SOL" { 100u32 } else { 200u32 },
                "margin_usdt": 0.0,
                "timestamp":   ""
            }));
        }
    }
    Json(serde_json::json!({
        "status":  "success",
        "signals": merged
    }))
}

/// POST /live-status/crypto — price-worker posts one signal per 5m tick per symbol
async fn post_crypto_signal_handler(
    Extension(state): Extension<Arc<AppState>>,
    Json(payload): Json<CryptoSignal>,
) -> Json<serde_json::Value> {
    let key = payload.symbol.clone();
    state.crypto_signals.write().unwrap().insert(key, payload);
    Json(serde_json::json!({ "status": "success" }))
}

async fn database_jobs_handler(
    Extension(state): Extension<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let jobs = sqlx::query(
        "SELECT symbol, from_date, to_date, status, last_updated, retry_count 
         FROM download_jobs 
         ORDER BY last_updated DESC 
         LIMIT 20"
    )
    .fetch_all(&state.db.pool)
    .await;

    match jobs {
        Ok(rows) => {
            let mut list = Vec::new();
            for r in rows {
                let symbol: String = r.get("symbol");
                let from_date: chrono::NaiveDate = r.get("from_date");
                let to_date: chrono::NaiveDate = r.get("to_date");
                let status: String = r.get("status");
                let last_updated: chrono::DateTime<chrono::Utc> = r.get("last_updated");
                let retry_count: i32 = r.get("retry_count");

                list.push(serde_json::json!({
                    "symbol": symbol,
                    "from_date": from_date.to_string(),
                    "to_date": to_date.to_string(),
                    "status": status,
                    "last_updated": last_updated.to_rfc3339(),
                    "retry_count": retry_count
                }));
            }
            Json(serde_json::json!({
                "status": "success",
                "jobs": list
            }))
        }
        Err(e) => {
            Json(serde_json::json!({
                "status": "error",
                "message": e.to_string()
            }))
        }
    }
}

#[derive(serde::Deserialize)]
struct PreviewParams {
    symbol: String,
    year: i32,
}

async fn candles_preview_handler(
    Extension(state): Extension<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<PreviewParams>,
) -> Json<serde_json::Value> {
    let candles = sqlx::query(
        "SELECT timestamp, open, high, low, close, volume 
         FROM candles 
         WHERE symbol = $1 
           AND EXTRACT(YEAR FROM timestamp)::integer = $2 
         ORDER BY timestamp DESC 
         LIMIT 10"
    )
    .bind(&params.symbol)
    .bind(params.year)
    .fetch_all(&state.db.pool)
    .await;

    match candles {
        Ok(rows) => {
            let mut list = Vec::new();
            for r in rows {
                let ts: chrono::DateTime<chrono::Utc> = r.get("timestamp");
                let open: f64 = r.get("open");
                let high: f64 = r.get("high");
                let low: f64 = r.get("low");
                let close: f64 = r.get("close");
                let vol: i64 = r.get("volume");

                list.push(serde_json::json!({
                    "timestamp": ts.to_rfc3339(),
                    "open": open,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": vol
                }));
            }
            Json(serde_json::json!({
                "status": "success",
                "candles": list
            }))
        }
        Err(e) => {
            Json(serde_json::json!({
                "status": "error",
                "message": e.to_string()
            }))
        }
    }
}

async fn journals_trade_handler(
    Extension(state): Extension<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let rows = sqlx::query(
        "SELECT id, timestamp, session_id, symbol, side, entry_price, exit_price, qty, pnl, slippage, fill_latency_ms, exit_reason, broker, leverage
         FROM trade_journal ORDER BY timestamp DESC LIMIT 50"
    )
    .fetch_all(&state.db.pool)
    .await;

    match rows {
        Ok(items) => {
            let mut list = Vec::new();
            for r in items {
                let id: String = r.get("id");
                let ts: chrono::DateTime<chrono::Utc> = r.get("timestamp");
                let sym: String = r.get("symbol");
                let side: String = r.get("side");
                let entry_p: f64 = r.get::<Option<f64>, _>("entry_price").unwrap_or(0.0);
                let exit_p: f64 = r.get::<Option<f64>, _>("exit_price").unwrap_or(0.0);
                let qty: i32 = r.get::<Option<i32>, _>("qty").unwrap_or(0);
                let pnl: f64 = r.get::<Option<f64>, _>("pnl").unwrap_or(0.0);
                let slip: f64 = r.get::<Option<f64>, _>("slippage").unwrap_or(0.0);
                let lat: i64 = r.get::<Option<i64>, _>("fill_latency_ms").unwrap_or(0);
                let reason: String = r.get::<Option<String>, _>("exit_reason").unwrap_or_default();
                let broker: String = r.get::<Option<String>, _>("broker").unwrap_or_default();
                let lev: i32 = r.get::<Option<i32>, _>("leverage").unwrap_or(1);

                list.push(serde_json::json!({
                    "id": id,
                    "timestamp": ts.to_rfc3339(),
                    "symbol": sym,
                    "side": side,
                    "entry_price": entry_p,
                    "exit_price": exit_p,
                    "qty": qty,
                    "pnl": pnl,
                    "slippage": slip,
                    "fill_latency_ms": lat,
                    "exit_reason": reason,
                    "broker": broker,
                    "leverage": lev
                }));
            }
            Json(serde_json::json!({ "status": "success", "count": list.len(), "trades": list }))
        }
        Err(e) => Json(serde_json::json!({ "status": "error", "message": e.to_string() }))
    }
}

async fn journals_decision_handler(
    Extension(state): Extension<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let rows = sqlx::query(
        "SELECT id, timestamp, symbol, signal_score, threshold, decision, rejection_reason, regime, ml_confidence, vix, atr
         FROM decision_journal ORDER BY timestamp DESC LIMIT 50"
    )
    .fetch_all(&state.db.pool)
    .await;

    match rows {
        Ok(items) => {
            let mut list = Vec::new();
            for r in items {
                let id: String = r.get("id");
                let ts: chrono::DateTime<chrono::Utc> = r.get("timestamp");
                let sym: String = r.get("symbol");
                let score: f64 = r.get::<Option<f64>, _>("signal_score").unwrap_or(0.0);
                let thresh: f64 = r.get::<Option<f64>, _>("threshold").unwrap_or(0.0);
                let dec: String = r.get::<Option<String>, _>("decision").unwrap_or_default();
                let rej: Option<String> = r.get("rejection_reason");
                let regime: String = r.get::<Option<String>, _>("regime").unwrap_or_default();
                let ml: f64 = r.get::<Option<f64>, _>("ml_confidence").unwrap_or(0.0);
                let vix: f64 = r.get::<Option<f64>, _>("vix").unwrap_or(0.0);

                list.push(serde_json::json!({
                    "id": id,
                    "timestamp": ts.to_rfc3339(),
                    "symbol": sym,
                    "score": score,
                    "threshold": thresh,
                    "decision": dec,
                    "rejection_reason": rej,
                    "regime": regime,
                    "ml_confidence": ml,
                    "vix": vix
                }));
            }
            Json(serde_json::json!({ "status": "success", "count": list.len(), "decisions": list }))
        }
        Err(e) => Json(serde_json::json!({ "status": "error", "message": e.to_string() }))
    }
}

async fn journals_risk_handler(
    Extension(state): Extension<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let rows = sqlx::query(
        "SELECT id, timestamp, symbol, event, leverage_used, leverage_limit, concentration, margin_utilization, check_passed, rejection_reason
         FROM risk_journal ORDER BY timestamp DESC LIMIT 50"
    )
    .fetch_all(&state.db.pool)
    .await;

    match rows {
        Ok(items) => {
            let mut list = Vec::new();
            for r in items {
                let id: String = r.get("id");
                let ts: chrono::DateTime<chrono::Utc> = r.get("timestamp");
                let sym: String = r.get("symbol");
                let event: String = r.get::<Option<String>, _>("event").unwrap_or_default();
                let lev_used: f64 = r.get::<Option<f64>, _>("leverage_used").unwrap_or(0.0);
                let lev_lim: f64 = r.get::<Option<f64>, _>("leverage_limit").unwrap_or(0.0);
                let conc: f64 = r.get::<Option<f64>, _>("concentration").unwrap_or(0.0);
                let passed: bool = r.get::<Option<bool>, _>("check_passed").unwrap_or(true);
                let rej: Option<String> = r.get("rejection_reason");

                list.push(serde_json::json!({
                    "id": id,
                    "timestamp": ts.to_rfc3339(),
                    "symbol": sym,
                    "event": event,
                    "leverage_used": lev_used,
                    "leverage_limit": lev_lim,
                    "concentration": conc,
                    "check_passed": passed,
                    "rejection_reason": rej
                }));
            }
            Json(serde_json::json!({ "status": "success", "count": list.len(), "risk_logs": list }))
        }
        Err(e) => Json(serde_json::json!({ "status": "error", "message": e.to_string() }))
    }
}

async fn journals_ml_handler(
    Extension(state): Extension<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let rows = sqlx::query(
        "SELECT id, timestamp, symbol, price, vwap, vix, slope, prediction_score, threshold, passed
         FROM ml_journal ORDER BY timestamp DESC LIMIT 50"
    )
    .fetch_all(&state.db.pool)
    .await;

    match rows {
        Ok(items) => {
            let mut list = Vec::new();
            for r in items {
                let id: String = r.get("id");
                let ts: chrono::DateTime<chrono::Utc> = r.get("timestamp");
                let sym: String = r.get("symbol");
                let p: f64 = r.get::<Option<f64>, _>("price").unwrap_or(0.0);
                let score: f64 = r.get::<Option<f64>, _>("prediction_score").unwrap_or(0.0);
                let passed: bool = r.get::<Option<bool>, _>("passed").unwrap_or(false);

                list.push(serde_json::json!({
                    "id": id,
                    "timestamp": ts.to_rfc3339(),
                    "symbol": sym,
                    "price": p,
                    "prediction_score": score,
                    "passed": passed
                }));
            }
            Json(serde_json::json!({ "status": "success", "count": list.len(), "ml_predictions": list }))
        }
        Err(e) => Json(serde_json::json!({ "status": "error", "message": e.to_string() }))
    }
}

#[derive(serde::Deserialize)]
struct ResearchParams {
    symbol: Option<String>,
}

async fn research_performance_handler(
    Extension(state): Extension<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<ResearchParams>,
) -> Json<serde_json::Value> {
    let sym = params.symbol.unwrap_or_else(|| "BTCUSD_PERP".to_string());
    
    // Fetch closed trades for this symbol
    let rows = sqlx::query(
        "SELECT symbol, pnl, entry_price, exit_price, qty FROM trade_journal WHERE symbol = $1 ORDER BY timestamp ASC"
    )
    .bind(&sym)
    .fetch_all(&state.db.pool)
    .await;

    let trades = match rows {
        Ok(items) => {
            items.into_iter().map(|r| price_research::ClosedTrade {
                symbol: r.get("symbol"),
                pnl: r.get::<Option<f64>, _>("pnl").unwrap_or(0.0),
                entry_price: r.get::<Option<f64>, _>("entry_price").unwrap_or(0.0),
                exit_price: r.get::<Option<f64>, _>("exit_price").unwrap_or(0.0),
                qty: r.get::<Option<i32>, _>("qty").unwrap_or(0),
            }).collect()
        }
        Err(_) => Vec::new()
    };

    let report = price_research::PerformanceAnalyzer::analyze(&sym, &trades);

    Json(serde_json::json!({
        "status": "success",
        "symbol": sym,
        "performance": report
    }))
}



