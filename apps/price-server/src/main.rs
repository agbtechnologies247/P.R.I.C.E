use axum::{
    routing::{get, post},
    Json, Router, Extension,
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
