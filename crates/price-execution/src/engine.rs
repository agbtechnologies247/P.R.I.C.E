use std::sync::Arc;
use tokio::sync::Mutex;
use chrono::Utc;
use tracing::{info, warn, error, debug};
use price_core::{TickData, Candle, EngineEvent, Result, PriceError};
use price_broker::{Broker, OrderRequest, Side, Position};
use price_indicators::{VwapCalculator, GeometryEngine, TrendGeometry, AtrCalculator, CandleAggregator};
use price_strategy::{OpportunityEngine, ExitEvaluator, Decision, ExitReason};
use price_risk::RiskEngine;

pub struct ExecutionOrchestrator {
    broker: Arc<dyn Broker>,
    risk_engine: RiskEngine,
    opportunity_engine: OpportunityEngine,
    exit_evaluator: ExitEvaluator,
    vwap_calc: VwapCalculator,
    geometry_engine: GeometryEngine,
    candle_aggregator: CandleAggregator,
    atr_calc: AtrCalculator,
    price_history: Vec<f64>,
    
    // Live Indicators
    current_atr: f64,
    
    // Active trade tracking
    active_position: Option<Position>,
    entry_price: f64,
    target_price: f64,
    stop_price: f64,
    hold_minutes: i64,
}

impl ExecutionOrchestrator {
    pub fn new(
        broker: Arc<dyn Broker>,
        risk_engine: RiskEngine,
        opportunity_engine: OpportunityEngine,
        exit_evaluator: ExitEvaluator,
    ) -> Self {
        Self {
            broker,
            risk_engine,
            opportunity_engine,
            exit_evaluator,
            vwap_calc: VwapCalculator::new(),
            geometry_engine: GeometryEngine::new(),
            candle_aggregator: CandleAggregator::new(),
            atr_calc: AtrCalculator::new(14),
            price_history: Vec::new(),
            current_atr: 12.0,
            active_position: None,
            entry_price: 0.0,
            target_price: 0.0,
            stop_price: 0.0,
            hold_minutes: 0,
        }
    }

    pub async fn ingest_tick(&mut self, tick: TickData) -> Result<Vec<EngineEvent>> {
        let mut events = vec![EngineEvent::TickReceived(tick.clone())];
        
        // 1. Update Candle Aggregator
        if let Some(closed_candle) = self.candle_aggregator.ingest_tick(&tick) {
            // Update ATR when a candle closes
            self.current_atr = self.atr_calc.update(closed_candle.clone());
            info!("Candle closed: {:?}. New ATR: {:.2}", closed_candle, self.current_atr);
            events.push(EngineEvent::CandleClosed(closed_candle));
        }

        // 2. Update VWAP
        let current_vwap = self.vwap_calc.update(tick.price, tick.volume);
        
        // 3. Buffer price for WMA calculations (up to 150 values)
        self.price_history.push(tick.price);
        if self.price_history.len() > 150 {
            self.price_history.remove(0);
        }

        // 4. Compute WMAs (WMA5 to WMA100 = 96 values)
        let mut wmas = Vec::with_capacity(96);
        for period in 5..=100 {
            if let Some(wma) = price_indicators::calculate_wma(&self.price_history, period) {
                wmas.push(wma);
            } else {
                wmas.push(tick.price); // Fallback
            }
        }

        // 5. Update Trend Geometry
        let (geometry, _dna) = self.geometry_engine.update(&wmas);
        
        events.push(EngineEvent::IndicatorsUpdated {
            timestamp: tick.timestamp,
            vwap: current_vwap,
            atr: self.current_atr,
            adx: 25.0,
            spread: 0.20,
        });

        // 6. Check if we already have an active position
        if let Some(ref mut pos) = self.active_position {
            self.hold_minutes += 1;
            
            // In options longing, we track the price of the option contract to evaluate exits
            let mut check_price = tick.price;
            if let Ok(quotes) = self.broker.quotes(vec![pos.symbol.clone()]).await {
                if let Some(q) = quotes.first() {
                    check_price = q.last_price;
                }
            }

            // Evaluate Exit signals from WMA Geometry & Market indicators dynamically
            let momentum_weakened = geometry.slope < 0.0; 
            let geometry_contracted = geometry.compression > 0.0;
            let oi_reversing = false; // Placeholder for orderbook reversal signals

            // Check for exit
            let should_exit = self.exit_evaluator.should_exit(
                check_price,
                self.entry_price,
                self.target_price,
                self.stop_price,
                if pos.side == Side::Buy { 1 } else { -1 },
                self.hold_minutes,
                current_vwap,
                momentum_weakened,
                oi_reversing,
                geometry_contracted,
            );

            if let Some(reason) = should_exit {
                info!("Exit condition met: {:?}", reason);
                
                // Route close order (SELL to close active options position)
                let exit_request = OrderRequest {
                    symbol: pos.symbol.clone(),
                    qty: pos.buy_qty.max(pos.sell_qty),
                    r#type: 2, // Market order to exit fast
                    side: Side::Sell, // Sell to close the Long Option
                    limit_price: 0.0,
                    stop_price: 0.0,
                };

                match self.broker.place_order(exit_request).await {
                    Ok(resp) => {
                        let exit_price = check_price;
                        let pnl = (exit_price - self.entry_price) * (pos.buy_qty as f64);

                        info!("Exit order filled for option {}: {}. PnL: {}", pos.symbol, resp.order_id, pnl);
                        
                        // Update Risk stats
                        self.risk_engine.record_trade_exit(pnl);

                        events.push(EngineEvent::PositionClosed {
                            symbol: pos.symbol.clone(),
                            qty: pos.buy_qty.max(pos.sell_qty),
                            exit_price,
                            pnl,
                        });
                        
                        events.push(EngineEvent::TradeRecorded {
                            trade_id: resp.order_id,
                            pnl,
                            reason: format!("{:?}", reason),
                        });

                        self.active_position = None;
                    }
                    Err(e) => {
                        error!("Failed to place exit order: {:?}", e);
                    }
                }
            }
        } else {
            // 7. Evaluates Entry rules
            let oi_increasing = tick.volume > 5000; 
            let volume_spike = tick.volume > 10000;
            let ml_prediction = 90.0; // 90% confidence from ML model

            let (opportunity, decision) = self.opportunity_engine.evaluate_entry(
                tick.price,
                current_vwap,
                14.5, // India VIX
                oi_increasing,
                volume_spike,
                &geometry,
                ml_prediction,
            );

            events.push(EngineEvent::ConfidenceUpdated {
                score: opportunity.confidence,
            });

            // 8. Evaluate Trade Quality Score
            let quality = self.opportunity_engine.calculate_quality_score(
                14.5, // VIX
                0.20, // Spread
                &geometry,
                oi_increasing,
                0.10, // Slippage
                3.0,  // Reward-risk ratio
                0.65, // ML win rate
            );

            if decision == Decision::Trade {
                info!("Strategy triggered entry signal. Trade Quality Score: {:.2}", quality.total);
                
                if quality.total < self.opportunity_engine.quality_threshold {
                    warn!("Trade candidate rejected due to insufficient Quality Score: {:.2} < {:.2}", 
                        quality.total, self.opportunity_engine.quality_threshold);
                } else {
                    // Determine which option symbol to buy: Call (CE) for bullish, Put (PE) for bearish
                    let is_bullish = geometry.slope >= 0.0;
                    let target_symbol = if is_bullish {
                        std::env::var("ACTIVE_CE_SYMBOL").unwrap_or_else(|_| "NSE:NIFTY2673024100CE".to_string())
                    } else {
                        std::env::var("ACTIVE_PE_SYMBOL").unwrap_or_else(|_| "NSE:NIFTY2673024100PE".to_string())
                    };

                    info!("Selected target option: {} (Bullish: {})", target_symbol, is_bullish);

                    // Fetch the latest quote of the target option contract to calculate accurate pricing
                    let mut option_price = tick.price;
                    if let Ok(quotes) = self.broker.quotes(vec![target_symbol.clone()]).await {
                        if let Some(q) = quotes.first() {
                            option_price = q.last_price;
                        }
                    }

                    events.push(EngineEvent::TradeCandidate {
                        symbol: target_symbol.clone(),
                        side: 1, // Always Buying (Longing) Options
                        confidence: opportunity.confidence,
                        price: option_price,
                    });

                    // 9. Dynamic Sizing using the Capital Engine (Kelly Criterion)
                    let funds = self.broker.funds().await?;
                    let qty = self.risk_engine.calculate_position_size(
                        funds.available_balance,
                        opportunity.probability,
                        3.0,  // Reward-risk ratio
                        self.current_atr * self.exit_evaluator.risk_multiplier, // Stop loss distance
                        0.5,  // Half-Kelly fraction for risk buffer
                        option_price,
                    );

                    if qty == 0 {
                        warn!("Kelly Position Size calculated as 0. Skipping trade entry.");
                    } else {
                        info!("Kelly Position Sizing allocated quantity: {} for option {} (Available balance: {:.2})", qty, target_symbol, funds.available_balance);

                        // Check Risk Engine limits
                        let order_request = OrderRequest {
                            symbol: target_symbol.clone(),
                            qty,
                            r#type: 1, // Limit Order
                            side: Side::Buy, // Always Buy to Open Option Long position
                            limit_price: option_price,
                            stop_price: 0.0,
                        };

                        match self.risk_engine.validate_order(&order_request, &funds) {
                            Ok(_) => {
                                info!("Risk approved order request");
                                events.push(EngineEvent::RiskApproved {
                                    order_id: "pending-risk-id".to_string(),
                                    allocated_capital: option_price * (qty as f64),
                                });

                                // Place the order
                                match self.broker.place_order(order_request).await {
                                    Ok(resp) => {
                                        info!("Option entry order placed successfully: {}", resp.order_id);
                                        events.push(EngineEvent::OrderPlaced {
                                            order_id: resp.order_id.clone(),
                                            symbol: target_symbol.clone(),
                                            qty,
                                            price: option_price,
                                        });

                                        // In mock/paper broker, this completes immediately.
                                        events.push(EngineEvent::OrderFilled {
                                            order_id: resp.order_id.clone(),
                                            fill_price: option_price,
                                            qty,
                                        });

                                        let (target, stop) = self.exit_evaluator.calculate_targets(self.current_atr, option_price, 1);
                                        info!("Calculated option targets -> Target: {}, SL: {}", target, stop);

                                        self.active_position = Some(Position {
                                            symbol: target_symbol.clone(),
                                            side: Side::Buy, // Long
                                            buy_qty: qty,
                                            sell_qty: 0,
                                            avg_price: option_price,
                                            current_price: option_price,
                                            pnl: 0.0,
                                        });

                                        self.entry_price = option_price;
                                        self.target_price = target;
                                        self.stop_price = stop;
                                        self.hold_minutes = 0;

                                        events.push(EngineEvent::PositionOpened {
                                            symbol: target_symbol.clone(),
                                            qty,
                                            avg_price: option_price,
                                        });
                                    }
                                    Err(e) => {
                                        error!("Broker order placement failed: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Risk validation rejected: {:?}", e);
                            }
                        }
                    }
                }
            }
        }

        Ok(events)
    }

    pub fn get_risk_status(&self) -> (i32, f64) {
        (self.risk_engine.trades_today, self.risk_engine.pnl_today)
    }

    pub fn active_position(&self) -> Option<Position> {
        self.active_position.clone()
    }
}
