use serde::{Deserialize, Serialize};
use price_indicators::{TrendGeometry, Pattern};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeOpportunity {
    pub probability: f64,
    pub reward: f64,
    pub risk: f64,
    pub confidence: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Decision {
    Trade,
    Wait,
    ReduceSize,
    Cancel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeQualityScore {
    pub market_quality: f64,
    pub trend_quality: f64,
    pub option_quality: f64,
    pub execution_quality: f64,
    pub risk_quality: f64,
    pub ml_quality: f64,
    pub total: f64,
}

pub struct OpportunityEngine {
    pub confidence_threshold: f64,
    pub quality_threshold: f64,
}

impl OpportunityEngine {
    pub fn new(confidence_threshold: f64, quality_threshold: f64) -> Self {
        Self {
            confidence_threshold,
            quality_threshold,
        }
    }

    pub fn evaluate_entry(
        &self,
        price: f64,
        vwap: f64,
        vix: f64,
        oi_increasing: bool,
        volume_spike: bool,
        geometry: &TrendGeometry,
        ml_prediction: f64, // 0.0 to 100.0 confidence
        pattern: Pattern,
        fib_confluence_score: f64,
        sr_proximity_score: f64,
    ) -> (TradeOpportunity, Decision) {
        // Calculate the Entry Score based on weights (100 total):
        // VWAP: 15
        // WMA Geometry: 15
        // OI: 10
        // Volume: 10
        // Liquidity: 10
        // India VIX: 10
        // ML Prediction: 10
        // Price Action Pattern: 10
        // Fibonacci Confluence: 10
        // Support/Resistance Proximity: 10
        let mut score = 0.0;

        // 1. VWAP (15 points)
        if price > vwap {
            score += 15.0;
        } else {
            let dist = (price - vwap).abs() / vwap;
            if dist < 0.001 {
                score += 7.5;
            }
        }

        // 2. WMA Geometry (15 points)
        if geometry.expansion > 0.0 && geometry.slope > 0.0 {
            score += 15.0;
        } else if geometry.slope > 0.0 {
            score += 7.5;
        }

        // 3. OI (10 points)
        if oi_increasing {
            score += 10.0;
        }

        // 4. Volume (10 points)
        if volume_spike {
            score += 10.0;
        }

        // 5. Liquidity (10 points)
        score += 10.0;

        // 6. India VIX (10 points)
        if vix >= 10.0 && vix <= 22.0 {
            score += 10.0;
        } else if vix < 10.0 {
            score += 5.0;
        }

        // 7. ML Prediction (10 points)
        score += (ml_prediction / 100.0) * 10.0;

        // 8. Price Action Pattern (10 points)
        match pattern {
            Pattern::BullishEngulfing | Pattern::PinBar => score += 10.0,
            Pattern::InsideBar => score += 5.0,
            Pattern::Doji => score += 3.0,
            _ => {}
        }

        // 9. Fibonacci Confluence (10 points)
        score += fib_confluence_score.min(1.0).max(0.0) * 10.0;

        // 10. Support/Resistance Proximity (10 points)
        score += sr_proximity_score.min(1.0).max(0.0) * 10.0;

        let confidence = score;
        let probability = confidence / 100.0;
        let reward = 30.0;
        let risk = 10.0;

        let opportunity = TradeOpportunity {
            probability,
            reward,
            risk,
            confidence,
        };

        let decision = if confidence >= self.confidence_threshold {
            Decision::Trade
        } else if confidence >= self.confidence_threshold - 10.0 {
            Decision::ReduceSize
        } else {
            Decision::Wait
        };

        (opportunity, decision)
    }

    pub fn calculate_quality_score(
        &self,
        vix: f64,
        spread: f64,
        geometry: &TrendGeometry,
        oi_increasing: bool,
        avg_slippage: f64,
        reward_risk_ratio: f64,
        ml_win_rate: f64,
    ) -> TradeQualityScore {
        // Market Quality (VIX, spread)
        let vix_score = if vix >= 10.0 && vix <= 22.0 { 100.0 } else { 50.0 };
        let spread_score = if spread < 0.5 { 100.0 } else if spread < 1.5 { 70.0 } else { 30.0 };
        let market_quality = (vix_score + spread_score) / 2.0;

        // Trend Quality (WMA geometry, slope consistency)
        let trend_quality = if geometry.slope.abs() > 0.05 && geometry.expansion > 0.0 {
            100.0
        } else if geometry.slope.abs() > 0.02 {
            70.0
        } else {
            30.0
        };

        // Option Quality (OI change, premium stability)
        let option_quality = if oi_increasing { 100.0 } else { 50.0 };

        // Execution Quality (expected slippage)
        let execution_quality = if avg_slippage < 0.5 { 100.0 } else if avg_slippage < 1.5 { 70.0 } else { 40.0 };

        // Risk Quality (reward-to-risk ratio)
        let risk_quality = if reward_risk_ratio >= 3.0 {
            100.0
        } else if reward_risk_ratio >= 2.0 {
            80.0
        } else {
            50.0
        };

        // ML Quality (historical win rate)
        let ml_quality = ml_win_rate * 100.0;

        let total = (market_quality + trend_quality + option_quality + execution_quality + risk_quality + ml_quality) / 6.0;

        TradeQualityScore {
            market_quality,
            trend_quality,
            option_quality,
            execution_quality,
            risk_quality,
            ml_quality,
            total,
        }
    }
}
