use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use chrono::Utc;
use tracing::{info, warn, error, debug, Level};
use tracing_subscriber::FmtSubscriber;
use dotenvy::dotenv;

use price_core::TickData;
use price_broker::{Broker, PaperBroker, FyersClient};
use price_risk::RiskEngine;
use price_strategy::{OpportunityEngine, ExitEvaluator};
use price_execution::ExecutionOrchestrator;

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

    // 5. Ingestion Loop (Simulated market data feed)
    info!("Starting tick ingestion feed loop...");
    let mut interval = time::interval(Duration::from_millis(500));
    let mut step = 0;
    
    // Simulate a trending index price starting at 500.0
    let mut simulated_price = 500.0;
    
    loop {
        interval.tick().await;
        step += 1;

        // Simulate random walk with slight upward drift to trigger buy rules
        let change = (step as f64 * 0.015).sin() * 0.5 + 0.15; // cyclical trend + drift
        simulated_price += change;

        let tick = TickData {
            symbol: "NSE:NIFTYBANK-ATM-CE".to_string(),
            price: simulated_price,
            volume: 12000 + (step * 10) % 5000,
            oi: 1500000 + (step * 100) % 20000,
            timestamp: Utc::now(),
        };

        // Pipe tick into execution orchestrator
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

        // Print status report every 20 ticks
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

    info!("Shutting down PRICE Quantitative Trading Worker...");
    Ok(())
}
