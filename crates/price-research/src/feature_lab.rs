use price_core::Candle;
use price_ml::MlFeatures;
use price_indicators::{
    VwapCalculator, AtrCalculator, GeometryEngine,
    calculate_fib_levels, calculate_confluence_score, calculate_sr_zones,
    detect_smc_patterns, calculate_volume_profile, OrderFlowTracker,
};
use serde::{Deserialize, Serialize};

/// A complete feature vector extracted from a historical candle window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVector {
    pub timestamp: i64,
    pub symbol: String,
    pub price: f64,
    pub vwap: f64,
    pub atr: f64,
    pub vix: f64,
    pub slope: f64,
    pub expansion: f64,
    pub compression: f64,
    pub curvature: f64,
    pub fib_confluence: f64,
    pub sr_proximity: f64,
    pub volume_spike: bool,
    pub oi_increasing: bool,
    pub cvd: f64,
    pub oi_delta: i64,
    pub divergence_detected: bool,
    pub has_bullish_fvg: bool,
    pub has_bearish_fvg: bool,
    pub volume_poc: f64,
}

pub struct FeatureLab;

impl FeatureLab {
    /// Extract a FeatureVector from a window of candles.
    pub fn extract(
        symbol: &str,
        candles: &[Candle],
        vix: f64,
        oi_values: &[u64],
    ) -> Option<FeatureVector> {
        if candles.len() < 10 {
            return None;
        }

        let last = candles.last()?;

        // ATR: update returns the ATR value directly
        let mut atr_calc = AtrCalculator::new(14);
        let mut atr_val = 10.0_f64;
        for c in candles {
            atr_val = atr_calc.update(c.clone());
        }
        let atr = if atr_val > 0.0 { atr_val } else { 10.0 };

        // VWAP: update(price, volume: u64) -> f64
        let mut vwap_calc = VwapCalculator::new();
        let mut vwap = last.close;
        for c in candles {
            vwap = vwap_calc.update(c.close, c.volume);
        }

        // WMA Geometry
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        let mut geometry_engine = GeometryEngine::new();
        // Map close prices into a 96-element array for GeometryEngine
        let n = closes.len();
        let wma_input: Vec<f64> = (0..96).map(|i| {
            let idx = (i * n / 96).min(n.saturating_sub(1));
            closes[idx]
        }).collect();
        let (geometry, _dna) = geometry_engine.update(&wma_input);

        // Fibonacci
        let fib = calculate_fib_levels(candles);
        let fib_confluence = if let Some(ref f) = fib {
            calculate_confluence_score(last.close, f, atr * 0.5)
        } else {
            0.0
        };

        // S/R Proximity
        let sr_zones = calculate_sr_zones(candles, atr * 0.3);
        let sr_proximity = if !sr_zones.is_empty() {
            let nearest: f64 = sr_zones.iter()
                .map(|z| (z.price - last.close).abs())
                .fold(f64::MAX, f64::min);
            let range = atr * 3.0;
            if range > 0.0 { (1.0_f64 - (nearest / range)).max(0.0_f64).min(1.0_f64) } else { 0.0 }
        } else { 0.0 };

        // Volume spike
        let avg_vol: f64 = if candles.len() > 1 {
            candles[..candles.len()-1].iter().map(|c| c.volume as f64).sum::<f64>()
                / (candles.len() - 1) as f64
        } else { 1.0 };
        let volume_spike = last.volume as f64 > avg_vol * 1.8;

        // OI trend
        let oi_increasing = if oi_values.len() >= 2 {
            oi_values[oi_values.len()-1] > oi_values[oi_values.len()-2]
        } else { false };

        // Order flow
        let mut of_tracker = OrderFlowTracker::new(50);
        for (i, c) in candles.iter().enumerate() {
            let oi = if i < oi_values.len() { oi_values[i] } else { 0 };
            of_tracker.update(c.close, c.volume, oi);
        }
        let divergence_detected = of_tracker.detect_divergence(10);

        // SMC signals
        let smc = detect_smc_patterns(candles);

        // Volume Profile POC
        let volume_poc = calculate_volume_profile(candles, 20.0)
            .map(|vp| vp.poc)
            .unwrap_or(last.close);

        Some(FeatureVector {
            timestamp: last.timestamp.timestamp(),
            symbol: symbol.to_string(),
            price: last.close,
            vwap,
            atr,
            vix,
            slope: geometry.slope,
            expansion: geometry.expansion,
            compression: geometry.compression,
            curvature: geometry.curvature,
            fib_confluence,
            sr_proximity,
            volume_spike,
            oi_increasing,
            cvd: of_tracker.cvd,
            oi_delta: of_tracker.last_oi_delta,
            divergence_detected,
            has_bullish_fvg: smc.bullish_fvg_active,
            has_bearish_fvg: smc.bearish_fvg_active,
            volume_poc,
        })
    }

    /// Extract feature vectors for a rolling window of candle history.
    pub fn extract_series(
        symbol: &str,
        candles: &[Candle],
        vix: f64,
        window: usize,
    ) -> Vec<FeatureVector> {
        if candles.len() < window {
            return Vec::new();
        }
        let empty_oi = vec![0u64; window];
        (window..=candles.len())
            .filter_map(|end| {
                let slice = &candles[end - window..end];
                Self::extract(symbol, slice, vix, &empty_oi)
            })
            .collect()
    }

    /// Convert a FeatureVector to MlFeatures for ML model consumption.
    pub fn to_ml_features(fv: &FeatureVector) -> MlFeatures {
        MlFeatures {
            price: fv.price,
            vwap: fv.vwap,
            vix: fv.vix,
            oi_increasing: fv.oi_increasing,
            volume_spike: fv.volume_spike,
            slope: fv.slope,
            expansion: fv.expansion,
            compression: fv.compression,
            curvature: fv.curvature,
            fib_confluence: fv.fib_confluence,
            sr_proximity: fv.sr_proximity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_candles(n: usize) -> Vec<Candle> {
        (0..n).map(|i| Candle {
            timestamp: Utc::now(),
            open: 100.0 + i as f64,
            high: 102.0 + i as f64,
            low: 99.0 + i as f64,
            close: 101.0 + i as f64,
            volume: 1000 + (i * 10) as u64,
        }).collect()
    }

    #[test]
    fn test_feature_lab_extraction() {
        let candles = make_candles(30);
        let result = FeatureLab::extract("BTCUSD_PERP", &candles, 14.0, &[]);
        assert!(result.is_some());
        let fv = result.unwrap();
        assert_eq!(fv.symbol, "BTCUSD_PERP");
        assert!(fv.price > 0.0);
        assert!(fv.atr > 0.0);
    }

    #[test]
    fn test_feature_lab_series() {
        let candles = make_candles(50);
        let series = FeatureLab::extract_series("ETHUSD_PERP", &candles, 15.0, 20);
        assert!(!series.is_empty());
        assert_eq!(series.len(), 31); // 50 - 20 + 1
    }
}
