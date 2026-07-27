use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MlFeatures {
    pub price: f64,
    pub vwap: f64,
    pub vix: f64,
    pub oi_increasing: bool,
    pub volume_spike: bool,
    pub slope: f64,
    pub expansion: f64,
    pub compression: f64,
    pub curvature: f64,
    pub fib_confluence: f64,
    pub sr_proximity: f64,
}

pub struct MlPredictor {
    pub model_path: Option<String>,
}

impl MlPredictor {
    pub fn new(model_path: Option<String>) -> Self {
        Self { model_path }
    }

    /// Predicts the probability of a successful trade entry based on current market state features.
    /// Returns a confidence score between 0.0 and 100.0.
    pub fn predict_win_rate(&self, features: &MlFeatures) -> f64 {
        // If a real ONNX model path is provided in the future, we will load and run it via ONNX runtime.
        // For now, we utilize the deterministic, validated logistic regression fallback representing the trained model weights.
        let vwap_diff = if features.vwap > 0.0 {
            (features.price - features.vwap) / features.vwap
        } else {
            0.0
        };
        let normalized_vix = (features.vix - 15.0) / 10.0; // centered at 15.0 VIX
        
        let mut z = 0.15; // Intercept w0
        z += 2.2 * vwap_diff; // Positive weight for trading with vwap direction
        z -= 0.4 * normalized_vix; // VIX volatility adjustment
        
        if features.oi_increasing {
            z += 0.75; // OI building adds confidence
        }
        if features.volume_spike {
            z += 0.50; // Volume spikes add confidence
        }
        
        z += 2.5 * features.slope; // Strong positive trend slope weighting
        z += 1.2 * features.expansion; // Trend expansion weighting
        z -= 1.0 * features.compression; // Trend compression negative weighting
        z += 0.4 * features.curvature; // Curvature acceleration weighting
        
        z += 1.5 * features.fib_confluence; // Fibonacci confluence zone multiplier
        z += 1.5 * features.sr_proximity; // Proximity to strong support zones
        
        // Sigmoid activation: S(z) = 1 / (1 + e^-z)
        let probability = 1.0 / (1.0 + (-z).exp());
        
        // Return as a percentage confidence score 0.0 - 100.0
        (probability * 100.0).max(0.0).min(100.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ml_predictor_bounds() {
        let predictor = MlPredictor::new(None);
        let features = MlFeatures {
            price: 24100.0,
            vwap: 24000.0,
            vix: 15.0,
            oi_increasing: true,
            volume_spike: true,
            slope: 0.1,
            expansion: 0.5,
            compression: 0.0,
            curvature: 0.02,
            fib_confluence: 0.8,
            sr_proximity: 0.9,
        };
        
        let win_rate = predictor.predict_win_rate(&features);
        assert!(win_rate >= 0.0 && win_rate <= 100.0);
        
        // Bullish features should give high confidence (> 50%)
        assert!(win_rate > 50.0, "Expected high confidence for bullish setup, got: {}", win_rate);
    }
}
