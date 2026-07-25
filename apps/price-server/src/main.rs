use axum::{
    routing::{get, post},
    Json, Router, Extension,
    response::Html,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use dotenvy::dotenv;

use price_broker::{Broker, PaperBroker, FyersClient, OrderRequest, Side};

struct AppState {
    broker: Arc<dyn Broker>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize logging
    dotenv().ok();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting PRICE Quantitative Monitoring Server...");

    // 2. Setup broker client
    let use_simulated = std::env::var("USE_SIMULATED_FEED")
        .unwrap_or_else(|_| "true".to_string()) == "true";
    let python_broker_url = std::env::var("PYTHON_BROKER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8001".to_string());
        
    let broker: Arc<dyn Broker> = if use_simulated {
        Arc::new(PaperBroker::new(100000.0))
    } else {
        Arc::new(FyersClient::new(&python_broker_url))
    };

    let state = Arc::new(AppState { broker });

    // 3. Build routes
    let app = Router::new()
        .route("/", get(index_handler))
        .route("/health", get(health_handler))
        .route("/portfolio", get(portfolio_handler))
        .route("/orders", get(orders_handler))
        .route("/trades", get(trades_handler))
        .route("/order", post(place_order_handler))
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
        <div class="status-badge" id="platform-status-badge">
            <span class="status-dot"></span>
            <span id="platform-status-text">Live Monitoring</span>
        </div>
    </header>

    <div class="grid-container">
        <!-- Main Column -->
        <div>
            <div class="card">
                <div class="card-title">Portfolio & Account Balance</div>
                <div class="metrics-grid">
                    <div class="metric-card">
                        <div class="metric-label">Total Capital Limit</div>
                        <div class="metric-value highlight" id="val-limit">₹0.00</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Utilized Margin</div>
                        <div class="metric-value" id="val-utilized">₹0.00</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Available Balance</div>
                        <div class="metric-value" id="val-available">₹0.00</div>
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
                
                if (data.funds) {
                    document.getElementById('val-limit').textContent = `₹${data.funds.limit_amount.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                    document.getElementById('val-utilized').textContent = `₹${data.funds.utilised_balance.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
                    document.getElementById('val-available').textContent = `₹${data.funds.available_balance.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
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

            // Extract auth code from redirect URL if full URL is pasted
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
                    // Reload health and balance after successful activation
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

        // Initialize and poll status
        fetchAuthUrl();
        checkHealth();
        fetchPortfolio();
        fetchOrders();
        
        setInterval(() => {
            fetchPortfolio();
            fetchOrders();
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
    let funds = state.broker.funds().await.ok();
    let positions = state.broker.positions().await.unwrap_or_default();
    let holdings = state.broker.holdings().await.unwrap_or_default();
    
    Json(serde_json::json!({
        "funds": funds,
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
