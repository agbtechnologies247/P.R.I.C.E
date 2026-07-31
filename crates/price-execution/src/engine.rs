use std::sync::Arc;
use tracing::{info, warn, error, debug};
use price_core::{TickData, EngineEvent, Result};
use price_broker::{Broker, OrderRequest, Side, Position, DeltaLeverageConfig};
use price_indicators::{VwapCalculator, GeometryEngine, AtrCalculator, CandleAggregator, OrderFlowTracker};
use price_strategy::{OpportunityEngine, ExitEvaluator, Decision};
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
    closed_candles: Vec<price_core::Candle>,
    ml_predictor: price_ml::MlPredictor,
    
    // Portfolio risk & memory
    pub portfolio_risk: price_risk::PortfolioRiskManager,
    pub timeseries: Option<price_timeseries::TimescaleClient>,
    pub last_regime: String,
    pub order_flow: OrderFlowTracker,
    
    // Live Indicators
    pub current_atr: f64,
    pub current_vix: f64,
    pub current_spread: f64,
    pub current_ml_confidence: f64,
    pub current_nifty_spot: f64,
    
    // Active trade tracking
    active_position: Option<Position>,
    entry_price: f64,
    target_price: f64,
    stop_price: f64,
    hold_minutes: i64,
    
    // Live opportunity tracking
    pub last_opportunity: Option<price_strategy::TradeOpportunity>,
    pub last_decision: Option<price_strategy::Decision>,
    pub last_quality: Option<price_strategy::TradeQualityScore>,
    pub last_target_option: Option<String>,
}

impl ExecutionOrchestrator {
    pub fn new(
        broker: Arc<dyn Broker>,
        risk_engine: RiskEngine,
        opportunity_engine: OpportunityEngine,
        exit_evaluator: ExitEvaluator,
        timeseries: Option<price_timeseries::TimescaleClient>,
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
            closed_candles: Vec::new(),
            ml_predictor: price_ml::MlPredictor::new(None),
            portfolio_risk: price_risk::PortfolioRiskManager::new(0.80, 0.35, 10.0, 0.15),
            timeseries,
            order_flow: OrderFlowTracker::new(100),
            last_regime: "RangeBound".to_string(),
            current_atr: 0.0,
            current_vix: 0.0,
            current_spread: 0.05,
            current_ml_confidence: 0.0,
            current_nifty_spot: 0.0,
            active_position: None,
            entry_price: 0.0,
            target_price: 0.0,
            stop_price: 0.0,
            hold_minutes: 0,
            last_opportunity: None,
            last_decision: None,
            last_quality: None,
            last_target_option: None,
        }
    }

    pub fn update_weighted_delta(&mut self, delta: f64) {
        // Map delta to an ML confidence score proxy from 0.0 to 100.0
        // e.g. delta of 0.0 -> 50.0. delta of +0.5% -> 100.0, delta of -0.5% -> 0.0
        let mapped = (50.0 + delta * 10000.0).max(0.0).min(100.0);
        self.current_ml_confidence = mapped;
        debug!("Weighted delta updated: {:.6} -> ML confidence proxy: {:.2}", delta, mapped);
    }

    pub fn classify_regime(&self, geometry: &price_indicators::TrendGeometry) -> String {
        if geometry.slope > 0.05 && geometry.expansion > 0.05 {
            "BullishTrending".to_string()
        } else if geometry.slope < -0.05 && geometry.expansion > 0.05 {
            "BearishTrending".to_string()
        } else if geometry.compression > 0.05 {
            "MeanReverting".to_string()
        } else {
            "RangeBound".to_string()
        }
    }

    pub async fn log_trade_context(
        &self,
        symbol: &str,
        side: &str,
        price: f64,
        qty: i32,
        trade_id: Option<String>,
        outcome_pnl: Option<f64>,
    ) {
        if let Some(ref db) = self.timeseries {
            let positions = self.broker.positions().await.unwrap_or_default();
            let funds = self.broker.funds().await.unwrap_or_default();
            let balance = funds.available_balance + funds.utilised_balance;
            
            let exposure = self.portfolio_risk.calculate_exposure(&positions);
            let leverage_usage = self.portfolio_risk.calculate_leverage_usage(&positions, balance);
            let margin_util = self.portfolio_risk.calculate_margin_utilization(&positions, balance);
            let (delta, gamma) = self.portfolio_risk.calculate_greeks(&positions, 0.15);

            let log = price_timeseries::ExecutionContextLog {
                timestamp: chrono::Utc::now(),
                trade_id,
                symbol: symbol.to_string(),
                side: side.to_string(),
                price,
                qty,
                regime: self.last_regime.clone(),
                ml_confidence: self.current_ml_confidence,
                portfolio_delta: delta,
                portfolio_gamma: gamma,
                portfolio_exposure: exposure,
                leverage_usage,
                margin_utilization: margin_util,
                vix: self.current_vix,
                atr: self.current_atr,
                vwap: 0.0,
                open_interest: None,
                volume: None,
                outcome_pnl,
            };

            if let Err(e) = db.insert_context_log(&log).await {
                error!("Failed to write execution context log: {:?}", e);
            } else {
                info!("Successfully wrote trade memory context to TimescaleDB.");
            }
        }
    }

    pub async fn ingest_tick(&mut self, tick: TickData) -> Result<Vec<EngineEvent>> {
        let mut events = vec![EngineEvent::TickReceived(tick.clone())];

        // ── Step 1: Tick Normalization ──────────────────────────────────────────
        // Reject clearly invalid ticks (zero price, impossible price, zero timestamp)
        if tick.price <= 0.0 || tick.price > 10_000_000.0 {
            warn!("[Pipeline] Tick rejected during normalization: price={:.2} symbol={}", tick.price, tick.symbol);
            return Ok(events);
        }
        debug!("[Pipeline] Step 1: Tick normalized — symbol={} price={:.2} vol={}", &tick.symbol, tick.price, tick.volume);

        // ── Step 2: Order Flow Feature Generation ──────────────────────────────
        self.order_flow.update(tick.price, tick.volume, tick.oi);
        debug!("[Pipeline] Step 2: Order flow updated — CVD={:.2} OI_delta={}", self.order_flow.cvd, self.order_flow.last_oi_delta);
        
        // Update India VIX state if the tick is VIX
        if tick.symbol == "NSE:INDIAVIX-INDEX" {
            self.current_vix = tick.price;
            debug!("Live India VIX updated to: {:.2}", self.current_vix);
            return Ok(events);
        }

        // Update Nifty spot price if index tick is received
        if tick.symbol == "NSE:NIFTY50-INDEX" {
            self.current_nifty_spot = tick.price;
        }
        
        // 1. Update Candle Aggregator
        if let Some(closed_candle) = self.candle_aggregator.ingest_tick(&tick) {
            // Update ATR when a candle closes
            self.current_atr = self.atr_calc.update(closed_candle.clone());
            self.closed_candles.push(closed_candle.clone());
            if self.closed_candles.len() > 100 {
                self.closed_candles.remove(0);
            }
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
        self.last_regime = self.classify_regime(&geometry);
        
        events.push(EngineEvent::IndicatorsUpdated {
            timestamp: tick.timestamp,
            vwap: current_vwap,
            atr: self.current_atr,
            adx: 0.0,
            spread: self.current_spread,
        });

        // 6. Check if we already have an active position
        let is_delta = self.broker.broker_type() == price_broker::BrokerType::DeltaExchange;
        if let Some(pos) = self.active_position.clone() {
            self.hold_minutes += 1;
            
            // In options longing / crypto futures, we track the price to evaluate exits
            let mut check_price = tick.price;
            if !is_delta {
                if let Ok(quotes) = self.broker.quotes(vec![pos.symbol.clone()]).await {
                    if let Some(q) = quotes.first() {
                        check_price = q.last_price;
                        // Update spread dynamically from active option quotes
                        self.current_spread = (q.ask - q.bid).abs().max(0.05);
                    }
                }
            }

            // Evaluate Exit signals from WMA Geometry & Market indicators dynamically
            let momentum_weakened = if pos.side == Side::Buy {
                geometry.slope < 0.0
            } else {
                geometry.slope > 0.0
            };
            let geometry_contracted = geometry.compression > 0.0;
            let oi_reversing = self.order_flow.detect_divergence(10) || self.order_flow.detect_oi_reversal(10);

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
                
                // Route close order
                let exit_request = OrderRequest {
                    symbol: pos.symbol.clone(),
                    qty: pos.buy_qty.max(pos.sell_qty),
                    r#type: 2, // Market order to exit fast
                    side: if pos.side == Side::Buy { Side::Sell } else { Side::Buy },
                    limit_price: 0.0,
                    stop_price: 0.0,
                    leverage: if is_delta { Some(10) } else { None },
                    reduce_only: Some(true),
                    post_only: None,
                    client_id: None,
                    time_in_force: None,
                };

                match self.broker.place_order(exit_request).await {
                    Ok(resp) => {
                        let exit_price = check_price;
                        let pnl = if pos.side == Side::Buy {
                            (exit_price - self.entry_price) * (pos.buy_qty.max(pos.sell_qty) as f64)
                        } else {
                            (self.entry_price - exit_price) * (pos.buy_qty.max(pos.sell_qty) as f64)
                        };

                        info!("Exit order filled for contract {}: {}. PnL: {}", pos.symbol, resp.order_id, pnl);
                        
                        // Update Risk stats
                        self.risk_engine.record_trade_exit(pnl);

                        // Log trade memory context to TimescaleDB
                        self.log_trade_context(
                            &pos.symbol,
                            if pos.side == Side::Buy { "SELL" } else { "BUY" },
                            exit_price,
                            pos.buy_qty.max(pos.sell_qty),
                            Some(resp.order_id.clone()),
                            Some(pnl),
                        ).await;

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
            // Determine target symbol/contract
            let target_symbol = if is_delta {
                tick.symbol.clone()
            } else {
                let strike = (self.current_nifty_spot / 50.0).round() * 50.0;
                let tick_date = tick.timestamp.naive_utc().date();
                let holidays = price_core::get_nse_holidays_2026();
                let expiry_date = price_core::calculate_nifty_expiry(tick_date, &holidays);
                let suffix = price_core::format_fyers_expiry_suffix(expiry_date);
                let prefix = format!("NSE:NIFTY{}", suffix);
                if geometry.slope >= 0.0 {
                    format!("{}{:.0}CE", prefix, strike)
                } else {
                    format!("{}{:.0}PE", prefix, strike)
                }
            };
            self.last_target_option = Some(target_symbol.clone());

            // Update option spread dynamically (throttled to once every 5 seconds)
            if !is_delta && tick.timestamp.timestamp() % 5 == 0 {
                if let Ok(quotes) = self.broker.quotes(vec![target_symbol.clone()]).await {
                    if let Some(q) = quotes.first() {
                        self.current_spread = (q.ask - q.bid).abs().max(0.05);
                    }
                }
            }

            // 7. Evaluates Entry rules
            let oi_increasing = tick.volume > 5000; 
            let volume_spike = tick.volume > 10000;

            let pattern = if !self.closed_candles.is_empty() {
                price_indicators::detect_patterns(&self.closed_candles)
                    .last()
                    .cloned()
                    .unwrap_or(price_indicators::Pattern::None)
            } else {
                price_indicators::Pattern::None
            };

            let fib_confluence_score = if let Some(fib) = price_indicators::calculate_fib_levels(&self.closed_candles) {
                price_indicators::calculate_confluence_score(tick.price, &fib, 15.0)
            } else {
                0.0
            };

            let sr_zones = price_indicators::calculate_sr_zones(&self.closed_candles, 15.0);
            let sr_proximity_score = sr_zones.iter()
                .filter(|z| !z.is_resistance)
                .map(|z| {
                    let diff = (tick.price - z.price).abs();
                    (1.0 - diff / 25.0).max(0.0)
                })
                .fold(0.0f64, |acc: f64, val: f64| acc.max(val));

            // Calculate live ML win rate probability dynamically using price-ml
            let ml_features = price_ml::MlFeatures {
                price: tick.price,
                vwap: current_vwap,
                vix: self.current_vix,
                oi_increasing,
                volume_spike,
                slope: geometry.slope,
                expansion: geometry.expansion,
                compression: geometry.compression,
                curvature: geometry.curvature,
                fib_confluence: fib_confluence_score,
                sr_proximity: sr_proximity_score,
            };
            self.current_ml_confidence = self.ml_predictor.predict_win_rate(&ml_features);

            let (opportunity, decision) = self.opportunity_engine.evaluate_entry(
                tick.price,
                current_vwap,
                self.current_vix,
                oi_increasing,
                volume_spike,
                &geometry,
                self.current_ml_confidence,
                pattern,
                fib_confluence_score,
                sr_proximity_score,
            );

            self.last_opportunity = Some(opportunity.clone());
            self.last_decision = Some(decision);

            events.push(EngineEvent::ConfidenceUpdated {
                score: opportunity.confidence,
            });

            // 8. Evaluate Trade Quality Score
            let quality = self.opportunity_engine.calculate_quality_score(
                self.current_vix,
                self.current_spread,
                &geometry,
                oi_increasing,
                self.current_spread, // Slippage
                3.0,  // Reward-risk ratio
                self.current_ml_confidence / 100.0, // ML win rate
            );
            self.last_quality = Some(quality.clone());

            if decision == Decision::Trade {
                info!("Strategy triggered entry signal. Trade Quality Score: {:.2}", quality.total);
                
                if quality.total < self.opportunity_engine.quality_threshold {
                    warn!("Trade candidate rejected due to insufficient Quality Score: {:.2} < {:.2}", 
                        quality.total, self.opportunity_engine.quality_threshold);
                } else {
                    // Fetch the latest quote of the target option contract to calculate accurate pricing
                    let mut option_price = tick.price;
                    if !is_delta {
                        if let Ok(quotes) = self.broker.quotes(vec![target_symbol.clone()]).await {
                            if let Some(q) = quotes.first() {
                                option_price = q.last_price;
                                // Update spread dynamically
                                self.current_spread = (q.ask - q.bid).abs().max(0.05);
                            }
                        }
                    }

                    events.push(EngineEvent::TradeCandidate {
                        symbol: target_symbol.clone(),
                        side: if is_delta && geometry.slope < 0.0 { -1 } else { 1 },
                        confidence: opportunity.confidence,
                        price: option_price,
                    });

                    // ── Step 9: Capital Allocation ────────────────────────────────────────
                    // Determine leverage and compute max usable margin for this entry
                    let funds = self.broker.funds().await?;
                    let leverage_to_use = if is_delta {
                        DeltaLeverageConfig::leverage_for(&target_symbol)
                    } else {
                        1 // No leverage for options (premium-based)
                    };

                    let qty = if is_delta {
                        // Capital allocation: risk 5% of available balance, amplified by configured leverage
                        let effective_budget = funds.available_balance * 0.05 * (leverage_to_use as f64);
                        let raw_qty = (effective_budget / option_price).floor() as i32;
                        raw_qty.max(1)
                    } else {
                        self.risk_engine.calculate_position_size(
                            funds.available_balance,
                            opportunity.probability,
                            3.0,  // Reward-risk ratio
                            self.current_atr * self.exit_evaluator.risk_multiplier,
                            0.5,  // Half-Kelly fraction
                            option_price,
                        )
                    };

                    info!("[Pipeline] Step 9: Capital Allocation — symbol={} leverage={}x qty={} balance={:.2}",
                        target_symbol, leverage_to_use, qty, funds.available_balance);

                    if qty == 0 {
                        warn!("[Pipeline] Kelly Position Size calculated as 0. Skipping trade entry.");
                    } else {
                        // ── Step 10: Execution Optimizer ──────────────────────────────────────
                        // Select order type: use limit orders when spread is tight enough, market otherwise
                        let use_limit_order = if is_delta {
                            // Use limit for Delta when spread is less than 0.05% of price
                            let spread_pct = self.current_spread / option_price;
                            spread_pct < 0.0005 && self.current_atr < option_price * 0.002
                        } else {
                            true // Always limit for options to avoid slippage on wide spreads
                        };

                        let order_type_code = if use_limit_order { 1 } else { 2 };
                        info!("[Pipeline] Step 10: Execution Optimizer — order_type={} spread={:.4} atr={:.2}",
                            if use_limit_order { "limit" } else { "market" }, self.current_spread, self.current_atr);

                        let order_request = OrderRequest {
                            symbol: target_symbol.clone(),
                            qty,
                            r#type: order_type_code,
                            side: if is_delta && geometry.slope < 0.0 { Side::Sell } else { Side::Buy },
                            limit_price: if order_type_code == 1 { option_price } else { 0.0 },
                            stop_price: 0.0,
                            leverage: if is_delta { Some(leverage_to_use) } else { None },
                            reduce_only: None,
                            post_only: if use_limit_order && is_delta { Some(false) } else { None },
                            client_id: None,
                            time_in_force: None,
                        };

                        match self.risk_engine.validate_order(&order_request, &funds) {
                            Ok(_) => {
                                // Check Portfolio concentration & leverage limits
                                let current_positions = self.broker.positions().await.unwrap_or_default();
                                let total_balance = funds.available_balance + funds.utilised_balance;
                                if let Err(e) = self.portfolio_risk.validate_portfolio_limits(&current_positions, &order_request, total_balance) {
                                    warn!("Portfolio Risk validation failed: {:?}", e);
                                    events.push(EngineEvent::RiskRejected(format!("Portfolio risk limit violation: {:?}", e)));
                                    return Ok(events);
                                }

                                info!("Risk approved order request");
                                events.push(EngineEvent::RiskApproved {
                                    order_id: "pending-risk-id".to_string(),
                                    allocated_capital: if is_delta { (option_price * qty as f64) / 10.0 } else { option_price * (qty as f64) },
                                });

                                // Place the order
                                match self.broker.place_order(order_request).await {
                                    Ok(resp) => {
                                        info!("Option entry order placed successfully: {}", resp.order_id);

                                        // Log trade memory context to TimescaleDB
                                        self.log_trade_context(
                                            &target_symbol,
                                            if is_delta && geometry.slope < 0.0 { "SELL" } else { "BUY" },
                                            option_price,
                                            qty,
                                            Some(resp.order_id.clone()),
                                            None,
                                        ).await;
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

                                        let side_sign = if is_delta && geometry.slope < 0.0 { -1 } else { 1 };
                                        let (target, stop) = self.exit_evaluator.calculate_targets(self.current_atr, option_price, side_sign);
                                        info!("Calculated contract targets -> Target: {}, SL: {}", target, stop);

                                        self.active_position = Some(Position {
                                            symbol: target_symbol.clone(),
                                            side: if side_sign > 0 { Side::Buy } else { Side::Sell },
                                            buy_qty: if side_sign > 0 { qty } else { 0 },
                                            sell_qty: if side_sign < 0 { qty } else { 0 },
                                            avg_price: option_price,
                                            current_price: option_price,
                                            pnl: 0.0,
                                            product_id: None,
                                            liquidation_price: None,
                                            leverage: None,
                                            margin: None,
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
