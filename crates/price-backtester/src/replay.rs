use std::path::Path;
use std::fs::{self, File};
use std::io::Write;
use chrono::{NaiveDate, Utc, TimeZone};
use price_core::TickData;
use price_timeseries::TimescaleClient;
use price_risk::RiskEngine;
use price_strategy::{OpportunityEngine, ExitEvaluator};
use price_execution::ExecutionOrchestrator;
use price_broker::Broker;
use crate::broker::ReplayBroker;

#[derive(serde::Serialize)]
pub struct BacktestReport {
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate: f64,
    pub initial_capital: f64,
    pub final_equity: f64,
    pub net_profit: f64,
    pub net_profit_pct: f64,
    pub max_drawdown_pct: f64,
    pub sharpe_ratio: f64,
}

pub struct ReplayRunner {
    db: TimescaleClient,
}

impl ReplayRunner {
    pub fn new(db: TimescaleClient) -> Self {
        Self { db }
    }

    pub async fn run_backtest(
        &self,
        symbol: &str,
        from_date: NaiveDate,
        to_date: NaiveDate,
        initial_capital: f64,
        output_dir: &str,
    ) -> anyhow::Result<BacktestReport> {
        let from_time = Utc.from_local_datetime(&from_date.and_hms_opt(9, 0, 0).unwrap()).unwrap();
        let to_time = Utc.from_local_datetime(&to_date.and_hms_opt(15, 30, 0).unwrap()).unwrap();

        // 1. Fetch candles from TimescaleDB
        let spot_candles = self.db.get_candles(symbol, "1m", from_time, to_time).await?;
        let vix_candles = self.db.get_candles("NSE:INDIAVIX-INDEX", "1m", from_time, to_time).await?;

        if spot_candles.is_empty() {
            anyhow::bail!("No historical candles found for {} in TimescaleDB between {} and {}", symbol, from_date, to_date);
        }
        if vix_candles.is_empty() {
            anyhow::bail!("No historical VIX candles found in TimescaleDB between {} and {}", from_date, to_date);
        }

        tracing::info!("Loaded {} spot candles and {} VIX candles.", spot_candles.len(), vix_candles.len());

        // Map VIX candles by timestamp for quick lookup
        let mut vix_map = std::collections::HashMap::new();
        for vc in vix_candles {
            vix_map.insert(vc.timestamp, vc.close);
        }

        // 2. Initialize backtesting engines
        let python_broker_url = std::env::var("PYTHON_BROKER_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8001".to_string());
        let broker = std::sync::Arc::new(ReplayBroker::new(
            initial_capital,
            0.0005,
            20.0,
            self.db.clone(),
            python_broker_url,
        ));
        let risk_engine = RiskEngine::new(3, 5000.0);
        let opportunity_engine = OpportunityEngine::new(85.0, 75.0);
        let exit_evaluator = ExitEvaluator::new(1.5, 0.8, 15);

        let mut orchestrator = ExecutionOrchestrator::new(
            broker.clone(),
            risk_engine,
            opportunity_engine,
            exit_evaluator,
        );

        let mut equity_curve = Vec::new();
        let mut peak_equity = initial_capital;
        let mut max_drawdown = 0.0;
        
        let mut last_vix = vix_candles.first().map(|c| c.close).unwrap_or(0.0);
        let mut daily_equities = Vec::new();
        let mut last_date: Option<NaiveDate> = None;

        // 3. Replay loop
        for sc in spot_candles {
            let current_dt = sc.timestamp;
            broker.set_current_time(current_dt);

            // Update VIX first
            if let Some(&vix_val) = vix_map.get(&current_dt) {
                last_vix = vix_val;
            }
            broker.update_price("NSE:INDIAVIX-INDEX", last_vix, 0, 0);
            let vix_tick = TickData {
                symbol: "NSE:INDIAVIX-INDEX".to_string(),
                price: last_vix,
                volume: 0,
                oi: 0,
                timestamp: current_dt,
            };
            let _ = orchestrator.ingest_tick(vix_tick).await?;

            // Update Spot price
            broker.update_price(symbol, sc.close, sc.volume, 0);
            let spot_tick = TickData {
                symbol: symbol.to_string(),
                price: sc.close,
                volume: sc.volume,
                oi: 0,
                timestamp: current_dt,
            };
            let _ = orchestrator.ingest_tick(spot_tick).await?;

            // Track equity & drawdowns
            let current_equity = broker.get_equity().await;
            equity_curve.push((current_dt, current_equity));

            if current_equity > peak_equity {
                peak_equity = current_equity;
            }
            let dd = (peak_equity - current_equity) / peak_equity;
            if dd > max_drawdown {
                max_drawdown = dd;
            }

            // Track daily equity for Sharpe ratio
            let tick_date = current_dt.naive_utc().date();
            if let Some(ld) = last_date {
                if ld != tick_date {
                    daily_equities.push(current_equity);
                    last_date = Some(tick_date);
                }
            } else {
                last_date = Some(tick_date);
            }
        }

        // Add final equity
        let final_equity = broker.get_equity().await;
        daily_equities.push(final_equity);

        // 4. Calculate metrics
        let trades = broker.trades().await?;
        
        // Match trades to calculate win/loss
        // Since options are bought and sold, buy and sell trades of the same symbol represent a round trip.
        // We can look at individual realized PnL of closed positions or compute simple trade returns.
        // For standard metrics, let's analyze trades by matching buys and sells.
        let mut buys = Vec::new();
        let mut sells = Vec::new();
        for t in &trades {
            if t.side == price_broker::Side::Buy {
                buys.push(t);
            } else {
                sells.push(t);
            }
        }

        let mut round_trips = Vec::new();
        let mut winning_trades = 0;
        let mut losing_trades = 0;
        let limit = buys.len().min(sells.len());
        for i in 0..limit {
            let buy = buys[i];
            let sell = sells[i];
            let pnl = (sell.price - buy.price) * (buy.qty as f64);
            round_trips.push(pnl);
            if pnl > 0.0 {
                winning_trades += 1;
            } else {
                losing_trades += 1;
            }
        }

        let win_rate = if limit > 0 {
            winning_trades as f64 / limit as f64
        } else {
            0.0
        };

        let net_profit = final_equity - initial_capital;
        let net_profit_pct = (net_profit / initial_capital) * 100.0;

        // Calculate Sharpe ratio based on daily equities
        let mut daily_returns = Vec::new();
        for i in 1..daily_equities.len() {
            let r = (daily_equities[i] - daily_equities[i - 1]) / daily_equities[i - 1];
            daily_returns.push(r);
        }

        let sharpe_ratio = if daily_returns.len() > 1 {
            let mean: f64 = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
            let variance: f64 = daily_returns.iter().map(|&r| {
                let diff = r - mean;
                diff * diff
            }).sum::<f64>() / (daily_returns.len() - 1) as f64;
            let std_dev = variance.sqrt();
            if std_dev > 0.0 {
                (mean / std_dev) * 252.0f64.sqrt()
            } else {
                0.0
            }
        } else {
            0.0
        };

        let report = BacktestReport {
            total_trades: limit,
            winning_trades,
            losing_trades,
            win_rate,
            initial_capital,
            final_equity,
            net_profit,
            net_profit_pct,
            max_drawdown_pct: max_drawdown * 100.0,
            sharpe_ratio,
        };

        // 5. Output reports
        let dir = Path::new(output_dir);
        if !dir.exists() {
            fs::create_dir_all(dir)?;
        }

        // Write metrics.json
        let metrics_json = serde_json::to_string_pretty(&report)?;
        let mut f_metrics = File::create(dir.join("metrics.json"))?;
        f_metrics.write_all(metrics_json.as_bytes())?;

        // Write trades.csv
        let mut f_trades = File::create(dir.join("trades.csv"))?;
        writeln!(f_trades, "trade_id,order_id,symbol,qty,price,side,timestamp")?;
        for t in &trades {
            writeln!(
                f_trades,
                "{},{},{},{},{:.2},{:?},{}",
                t.trade_id, t.order_id, t.symbol, t.qty, t.price, t.side, t.timestamp
            )?;
        }

        // Write equity_curve.csv
        let mut f_equity = File::create(dir.join("equity_curve.csv"))?;
        writeln!(f_equity, "timestamp,equity")?;
        for ec in &equity_curve {
            writeln!(f_equity, "{},{}", ec.0.to_rfc3339(), ec.1)?;
        }

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use price_core::Candle;
    use chrono::{Utc, TimeZone, NaiveDate};

    #[tokio::test]
    async fn test_backtest_runner_integration() {
        let db_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5433/price".to_string());
        
        let client = match TimescaleClient::new(&db_url).await {
            Ok(c) => c,
            Err(_) => return,
        };

        if client.init_db().await.is_err() {
            return;
        }

        let date = NaiveDate::from_ymd_opt(2026, 7, 24).unwrap();
        let base_time = Utc.with_ymd_and_hms(2026, 7, 24, 9, 15, 0).unwrap();
        
        let mut spot_candles = Vec::new();
        let mut vix_candles = Vec::new();
        
        for m in 0..30 {
            let ts = base_time + chrono::Duration::minutes(m);
            let price = 24000.0 + (m as f64) * 5.0;
            spot_candles.push(Candle {
                timestamp: ts,
                open: price - 2.0,
                high: price + 3.0,
                low: price - 3.0,
                close: price,
                volume: 6000,
            });
            vix_candles.push(Candle {
                timestamp: ts,
                open: 15.0,
                high: 15.2,
                low: 14.8,
                close: 15.0,
                volume: 0,
            });
        }

        let _ = client.insert_candles("NSE:NIFTY50-INDEX", "NSE", "1m", &spot_candles).await;
        let _ = client.insert_candles("NSE:INDIAVIX-INDEX", "NSE", "1m", &vix_candles).await;

        let runner = ReplayRunner::new(client.clone());
        let output_dir = "./target/debug/test_results";
        let res = runner.run_backtest("NSE:NIFTY50-INDEX", date, date, 100000.0, output_dir).await;
        
        // Cleanup synthetic test candles from database
        let _ = sqlx::query("DELETE FROM candles WHERE symbol IN ('NSE:NIFTY50-INDEX', 'NSE:INDIAVIX-INDEX')")
            .execute(&client.pool)
            .await;
        
        assert!(res.is_ok(), "Backtest run failed: {:?}", res.err());
        let report = res.unwrap();
        assert!(report.final_equity > 0.0);
    }
}
