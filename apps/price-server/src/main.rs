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
}

struct AppState {
    broker: Arc<price_broker::HybridBroker>,
    python_broker_url: String,
    db: TimescaleClient,
    live_status: RwLock<Option<LiveStatusInfo>>,
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

    // 2. Setup Hybrid broker client (executing both Live & Paper)
    let broker = Arc::new(price_broker::HybridBroker::new(&python_broker_url, 10000.0));

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5433/price".to_string());
    let db = TimescaleClient::new(&db_url).await?;
    db.init_db().await?;

    let state = Arc::new(AppState {
        broker,
        python_broker_url: python_broker_url.clone(),
        db: db.clone(),
        live_status: RwLock::new(None),
    });

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
        .route("/database/jobs", get(database_jobs_handler))
        .route("/database/candles-preview", get(candles_preview_handler))
        .route("/database/download-status", get(download_status_handler))
        .route("/database/start-download", post(start_download_handler))
        .route("/database/download", get(download_handler))
        .route("/metrics", get(metrics_handler))
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
    </style>
</head>
<body>

    <header>
        <div>
            <h1>P.R.I.C.E</h1>
            <p style="color: var(--text-muted); font-size: 0.85rem; margin-top: 0.25rem;">Predictive Risk Intelligence & Capital Engine</p>
        </div>

        <!-- Live Ticker Header -->
        <div style="display: flex; gap: 2rem; align-items: center; background: rgba(20, 26, 46, 0.8); border: 1px solid var(--border-color); padding: 0.6rem 1.5rem; border-radius: 12px; backdrop-filter: blur(8px); box-shadow: 0 4px 20px rgba(0,0,0,0.2);">
            <div style="display: flex; align-items: center; gap: 0.5rem;">
                <span class="live-pulse" id="nifty-pulse-dot" style="background-color: var(--danger); box-shadow: 0 0 10px var(--danger);"></span>
                <span style="font-size: 0.8rem; color: var(--text-muted); font-weight: 600; letter-spacing: 0.05em;">NIFTY SPOT:</span>
                <span style="font-size: 1.15rem; font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: var(--text-main);" id="live-nifty-val">₹0.00</span>
            </div>
            <div style="width: 1px; height: 20px; background: var(--border-color);"></div>
            <div style="display: flex; align-items: center; gap: 0.5rem;">
                <span style="font-size: 0.8rem; color: var(--text-muted); font-weight: 600; letter-spacing: 0.05em;">INDIA VIX:</span>
                <span style="font-size: 1.15rem; font-weight: 700; font-family: 'Space Grotesk', sans-serif; color: var(--warning);" id="live-vix-val">0.00</span>
            </div>
        </div>

        <div class="status-badge" id="platform-status-badge">
            <span class="status-dot"></span>
            <span id="platform-status-text">Live Monitoring</span>
        </div>
    </header>

    <div class="grid-container">
        <!-- Main Column -->
        <div>
            <!-- Live Opportunity Section -->
            <div class="card" style="border-left: 4px solid var(--primary);">
                <div class="card-title">
                    <span>Live Opportunity & Quantitative Decision Engine</span>
                    <span id="last-update-time" style="font-size: 0.75rem; color: var(--danger); font-weight: 600; letter-spacing: 0.05em; background: rgba(239, 68, 68, 0.1); padding: 0.2rem 0.6rem; border-radius: 4px;">Worker offline</span>
                </div>
                <div class="metrics-grid" style="margin-bottom: 1.5rem;">
                    <div class="metric-card" style="padding: 1rem;">
                        <div class="metric-label">Target Option Contract</div>
                        <div class="metric-value highlight" style="font-size: 1.4rem; font-family: 'Space Grotesk', sans-serif;" id="opp-target-option">--</div>
                    </div>
                    <div class="metric-card" style="padding: 1rem;">
                        <div class="metric-label">Engine Decision</div>
                        <div style="margin-top: 0.4rem; display: flex; justify-content: center;">
                            <span id="opp-decision-badge" class="status-badge badge-wait" style="display: inline-flex; justify-content: center; width: 120px;">WAIT</span>
                        </div>
                    </div>
                    <div class="metric-card" style="padding: 1rem;">
                        <div class="metric-label">Trade Quality Score</div>
                        <div class="metric-value" id="opp-quality-score" style="color: var(--warning);">0.0</div>
                    </div>
                </div>
                <div class="metrics-grid">
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
            </div>

            <div class="card">
                <div class="card-title">Portfolio & Account Balance (Dual Engines)</div>
                
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
                            <th>Buy Qty</th>
                            <th>Avg Price</th>
                            <th>LTP</th>
                            <th>PnL</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td colspan="6" style="text-align: center; color: var(--text-muted);">No active positions found</td>
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
                            <th>Avg Price</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td colspan="6" style="text-align: center; color: var(--text-muted);">No recent orders found</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Sidebar / Auth Console -->
        <div>
            <div class="card">
                <div class="card-title">Historical Data Downloader</div>
                <div class="form-group" style="margin-bottom: 0.75rem;">
                    <label for="download-year-select">Select Year</label>
                    <select id="download-year-select" class="input-text" style="background-color: #121824; border: 1px solid var(--border-color); color: var(--text-main); width: 100%; padding: 0.5rem; border-radius: 4px;">
                        <option value="2026">2026</option>
                    </select>
                </div>
                <div class="form-group" style="margin-bottom: 0.75rem;">
                    <label for="download-symbol-select">Select Symbol</label>
                    <select id="download-symbol-select" class="input-text" style="background-color: #121824; border: 1px solid var(--border-color); color: var(--text-main); width: 100%; padding: 0.5rem; border-radius: 4px;">
                        <option value="NSE:NIFTY50-INDEX">NSE:NIFTY50-INDEX</option>
                        <option value="NSE:INDIAVIX-INDEX">NSE:INDIAVIX-INDEX</option>
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

            <!-- Ingestion Pipeline Monitor Card -->
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

                <h3 style="font-size: 0.8rem; color: var(--success); margin-bottom: 0.5rem; letter-spacing: 0.05em;">📊 LATEST INGESTED CANDLES (10)</h3>
                <div style="max-height: 200px; overflow-y: auto; border: 1px solid var(--border-color); border-radius: 8px; background: rgba(0,0,0,0.15);">
                    <table style="margin-top: 0;" id="candles-preview-table">
                        <thead>
                            <tr style="position: sticky; top: 0; background: #101524; z-index: 1;">
                                <th style="padding: 0.4rem; font-size: 0.75rem;">Time</th>
                                <th style="padding: 0.4rem; font-size: 0.75rem;">O</th>
                                <th style="padding: 0.4rem; font-size: 0.75rem;">H</th>
                                <th style="padding: 0.4rem; font-size: 0.75rem;">L</th>
                                <th style="padding: 0.4rem; font-size: 0.75rem;">C</th>
                                <th style="padding: 0.4rem; font-size: 0.75rem;">V</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td colspan="6" style="text-align: center; color: var(--text-muted); padding: 0.75rem; font-size: 0.8rem;">Select symbol to preview</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="card">
                <div class="card-title">Fyers API Activation</div>
                <p style="font-size: 0.85rem; color: var(--text-muted); margin-bottom: 1.5rem; line-height: 1.4;">
                    Your bridge is currently running in live-only mode. Use this console to complete user authorization and exchange your authentication code for a persistent session token.
                </p>
                
                <div class="form-group" style="text-align: center; margin-bottom: 1.5rem;">
                    <a href="#" target="_blank" class="btn" id="fyers-login-btn">1. Authorize App on Fyers</a>
                </div>

                <div class="form-group">
                    <label for="auth-code-input">2. Paste Auth Code from Redirect Link</label>
                    <input type="text" id="auth-code-input" class="input-text" placeholder="e.g. eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...">
                </div>

                <button class="btn btn-secondary" style="width: 100%;" id="activate-token-btn">3. Generate & Save Token</button>
                <div id="activation-message" style="margin-top: 1rem; font-size: 0.85rem; font-weight: 600; text-align: center;"></div>
            </div>
        </div>
    </div>

    <script>
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

        // Fetch Portfolio and balance
        async function fetchPortfolio() {
            try {
                const response = await fetch('/portfolio');
                const data = await response.json();
                
                if (data.live_funds) {
                    document.getElementById('live-val-limit').textContent = `₹${data.live_funds.limit_amount.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                    document.getElementById('live-val-utilized').textContent = `₹${data.live_funds.utilised_balance.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                    document.getElementById('live-val-available').textContent = `₹${data.live_funds.available_balance.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                }
                if (data.paper_funds) {
                    document.getElementById('paper-val-limit').textContent = `₹${data.paper_funds.limit_amount.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                    document.getElementById('paper-val-utilized').textContent = `₹${data.paper_funds.utilised_balance.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                    document.getElementById('paper-val-available').textContent = `₹${data.paper_funds.available_balance.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                }

                const posTableBody = document.querySelector('#positions-table tbody');
                if (data.positions && data.positions.length > 0) {
                    posTableBody.innerHTML = '';
                    data.positions.forEach(p => {
                        const row = document.createElement('tr');
                        const pnlClass = p.pnl >= 0 ? 'pnl-green' : 'pnl-red';
                        row.innerHTML = `
                            <td><strong>${p.symbol}</strong></td>
                            <td>${p.side === 1 ? 'BUY' : 'SELL'}</td>
                            <td>${p.buy_qty || p.sell_qty}</td>
                            <td>₹${p.avg_price.toFixed(2)}</td>
                            <td>₹${p.current_price.toFixed(2)}</td>
                            <td class="${pnlClass}">₹${p.pnl.toFixed(2)}</td>
                        `;
                        posTableBody.appendChild(row);
                    });
                } else {
                    posTableBody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: var(--text-muted);">No active positions found</td></tr>`;
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
                        row.innerHTML = `
                            <td><code>${o.id.substring(0, 8)}...</code></td>
                            <td><strong>${o.symbol}</strong></td>
                            <td>${o.side === 1 ? 'BUY' : 'SELL'}</td>
                            <td>${o.qty}</td>
                            <td>₹${o.avg_price.toFixed(2)}</td>
                            <td><span style="font-weight:600; color: ${o.status === 'FILLED' ? 'var(--success)' : 'var(--text-muted)'}">${o.status}</span></td>
                        `;
                        ordTableBody.appendChild(row);
                    });
                } else {
                    ordTableBody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: var(--text-muted);">No recent orders found</td></tr>`;
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
                    if (diff < 5) {
                        niftyPulse.style.backgroundColor = 'var(--success)';
                        niftyPulse.style.boxShadow = '0 0 10px var(--success)';
                        document.getElementById('last-update-time').textContent = 'LIVE CONNECTED';
                        document.getElementById('last-update-time').style.color = 'var(--success)';
                        document.getElementById('last-update-time').style.background = 'rgba(16, 185, 129, 0.1)';
                    } else {
                        niftyPulse.style.backgroundColor = 'var(--warning)';
                        niftyPulse.style.boxShadow = '0 0 10px var(--warning)';
                        document.getElementById('last-update-time').textContent = `INACTIVE: ${Math.round(diff)}s AGO`;
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

        async function fetchPipelineJobs() {
            try {
                const res = await fetch('/database/jobs');
                const data = await res.json();
                const tbody = document.querySelector('#pipeline-jobs-table tbody');
                if (data.status === 'success' && data.jobs && data.jobs.length > 0) {
                    tbody.innerHTML = '';
                    data.jobs.forEach(j => {
                        const row = document.createElement('tr');
                        let statusColor = 'var(--text-muted)';
                        if (j.status === 'COMPLETED') statusColor = 'var(--success)';
                        else if (j.status === 'IN_PROGRESS') statusColor = 'var(--warning)';
                        else if (j.status.startsWith('FAILED')) statusColor = 'var(--danger)';
                        
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
            } catch (e) {
                console.error("Failed to fetch jobs: ", e);
            }
        }

        async function fetchCandlesPreview() {
            const symbol = symbolSelect.value;
            const year = yearSelect.value;
            if (!symbol || !year) return;
            try {
                const res = await fetch(`/database/candles-preview?symbol=${encodeURIComponent(symbol)}&year=${year}`);
                const data = await res.json();
                const tbody = document.querySelector('#candles-preview-table tbody');
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

        // Initialize and poll status
        fetchAuthUrl();
        checkHealth();
        fetchPortfolio();
        fetchOrders();
        checkDownloadStatus();
        fetchLiveStatus();
        fetchPipelineJobs();
        fetchCandlesPreview();
        
        setInterval(fetchLiveStatus, 1000);
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

async fn health_handler(Extension(state): Extension<Arc<AppState>>) -> Json<serde_json::Value> {
    let broker_ok = state.broker.profile().await.is_ok();
    Json(serde_json::json!({
        "status": "healthy",
        "broker_connection": if broker_ok { "connected" } else { "offline_simulated" },
        "system": "PRICE Predictive Risk Intelligence & Capital Engine",
        "version": "1.0.0"
    }))
}

async fn portfolio_handler(Extension(state): Extension<Arc<AppState>>) -> Json<serde_json::Value> {
    let live_funds = state.broker.live.funds().await.ok();
    let paper_funds = state.broker.paper.funds().await.ok();
    let positions = state.broker.positions().await.unwrap_or_default();
    let holdings = state.broker.holdings().await.unwrap_or_default();
    
    Json(serde_json::json!({
        "live_funds": live_funds,
        "paper_funds": paper_funds,
        "positions": positions,
        "holdings": holdings
    }))
}

async fn orders_handler(Extension(state): Extension<Arc<AppState>>) -> Json<serde_json::Value> {
    let orders = state.broker.orderbook().await.unwrap_or_default();
    Json(serde_json::json!({
        "orders": orders
    }))
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
        symbol: payload.symbol,
        qty: payload.qty,
        r#type: 1, // Limit order
        side,
        limit_price: payload.limit_price,
        stop_price: 0.0,
    };
    
    match state.broker.place_order(req).await {
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
    let year = payload.year;
    let symbol = payload.symbol;

    let expected_days = get_trading_days(year);

    for date in expected_days {
        let _ = sqlx::query(
            "INSERT INTO download_jobs (symbol, from_date, to_date, status, last_updated) 
             VALUES ($1, $2, $2, 'PENDING', NOW()) 
             ON CONFLICT (symbol, from_date, to_date) DO UPDATE
             SET status = CASE WHEN download_jobs.status = 'COMPLETED' THEN 'COMPLETED' ELSE 'PENDING' END,
                 last_updated = CASE WHEN download_jobs.status = 'COMPLETED' THEN download_jobs.last_updated ELSE NOW() END"
        )
        .bind(&symbol)
        .bind(date)
        .execute(&state.db.pool)
        .await;
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
        "SELECT count(*) as count 
         FROM download_jobs 
         WHERE symbol = $1 
           AND EXTRACT(YEAR FROM from_date)::integer = $2 
           AND status = 'COMPLETED'"
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
                 WHERE status = 'PENDING' OR status LIKE 'FAILED%' 
                 ORDER BY 
                   CASE WHEN status = 'PENDING' THEN 0 ELSE 1 END,
                   CASE WHEN status = 'PENDING' THEN last_updated END DESC,
                   last_updated ASC
                 LIMIT 1"
            )
            .fetch_optional(&db.pool)
            .await;

            match next_job {
                Ok(Some(row)) => {
                    let symbol: String = row.get("symbol");
                    let from_date: chrono::NaiveDate = row.get("from_date");
                    let to_date: chrono::NaiveDate = row.get("to_date");
                    info!("Background downloader executing job for {} from {} to {}", symbol, from_date, to_date);
                    
                    // Mark as IN_PROGRESS
                    let _ = db.mark_job_status(&symbol, from_date, to_date, "IN_PROGRESS").await;

                    // Download history
                    match downloader.download_history(&symbol, "NSE", from_date, to_date).await {
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

async fn get_live_status_handler(
    Extension(state): Extension<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let status = state.live_status.read().unwrap().clone();
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


