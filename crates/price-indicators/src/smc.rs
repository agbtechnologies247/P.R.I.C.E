use price_core::Candle;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmcSignals {
    pub bos_detected: bool,
    pub choch_detected: bool,
    pub bullish_fvg_active: bool,
    pub bearish_fvg_active: bool,
}

/// Detects SMC patterns in a slice of closed candles.
pub fn detect_smc_patterns(candles: &[Candle]) -> SmcSignals {
    let mut bos_detected = false;
    let mut choch_detected = false;
    let mut bullish_fvg_active = false;
    let mut bearish_fvg_active = false;

    if candles.len() < 3 {
        return SmcSignals {
            bos_detected,
            choch_detected,
            bullish_fvg_active,
            bearish_fvg_active,
        };
    }

    // 1. Detect Fair Value Gaps (FVG) on the last 3 candles (indices: len-3, len-2, len-1)
    let n = candles.len();
    let c1 = &candles[n - 3];
    let c3 = &candles[n - 1];

    // Bullish FVG: Low of candle 3 is greater than High of candle 1
    if c3.low > c1.high {
        bullish_fvg_active = true;
    }
    // Bearish FVG: High of candle 3 is lower than Low of candle 1
    if c3.high < c1.low {
        bearish_fvg_active = true;
    }

    // 2. Swing Highs & Lows to check structures (BOS & CHoCH)
    if n >= 5 {
    // Let's identify swing highs/lows on the dataset.
    // A swing high is a high greater than the highs of 2 candles before and after it.
    let mut swing_highs = Vec::new();
    let mut swing_lows = Vec::new();

    for i in 2..(n - 2) {
        let h = candles[i].high;
        if h > candles[i-1].high && h > candles[i-2].high && h > candles[i+1].high && h > candles[i+2].high {
            swing_highs.push(h);
        }

        let l = candles[i].low;
        if l < candles[i-1].low && l < candles[i-2].low && l < candles[i+1].low && l < candles[i+2].low {
            swing_lows.push(l);
        }
    }

    // Check structure breaks based on the last candle close
    let last_close = candles[n - 1].close;

    // BOS: Break of continuation structure
    if let Some(&last_swing_high) = swing_highs.last() {
        if last_close > last_swing_high {
            bos_detected = true;
        }
    }

    // CHoCH: Change of Character (break of opposite structure)
    if let Some(&last_swing_low) = swing_lows.last() {
        if last_close < last_swing_low {
            choch_detected = true;
        }
    }
    }

    SmcSignals {
        bos_detected,
        choch_detected,
        bullish_fvg_active,
        bearish_fvg_active,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_fvg_detection() {
        let mut candles = Vec::new();
        // Create 3 candles to simulate a bullish gap
        candles.push(Candle {
            timestamp: Utc::now(),
            open: 100.0,
            high: 105.0,
            low: 99.0,
            close: 104.0,
            volume: 100,
        });
        candles.push(Candle {
            timestamp: Utc::now(),
            open: 104.0,
            high: 112.0,
            low: 103.0,
            close: 111.0,
            volume: 150,
        });
        candles.push(Candle {
            timestamp: Utc::now(),
            open: 111.0,
            high: 115.0,
            low: 107.0, // Low is 107.0, which is > 105.0 (High of candle 1) -> Bullish FVG!
            close: 114.0,
            volume: 120,
        });

        let sigs = detect_smc_patterns(&candles);
        assert!(sigs.bullish_fvg_active);
        assert!(!sigs.bearish_fvg_active);
    }
}
