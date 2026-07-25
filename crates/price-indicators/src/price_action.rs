use price_core::Candle;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Pattern {
    Doji,
    PinBar,
    BullishEngulfing,
    BearishEngulfing,
    InsideBar,
    OutsideBar,
    None,
}

pub fn detect_patterns(candles: &[Candle]) -> Vec<Pattern> {
    if candles.is_empty() {
        return Vec::new();
    }
    
    let mut patterns = vec![Pattern::None; candles.len()];
    
    for i in 0..candles.len() {
        let current = &candles[i];
        let range = (current.high - current.low).max(0.01);
        let body = (current.close - current.open).abs();
        let body_pct = body / range;
        
        // 1. Doji: body is <= 10% of total range
        if body_pct <= 0.10 && range > 0.05 {
            patterns[i] = Pattern::Doji;
            continue;
        }
        
        // 2. Pin Bar (Hammer/Shooting Star): body <= 30% of range, one wick is >= 60% of range
        let upper_wick = current.high - current.open.max(current.close);
        let lower_wick = current.open.min(current.close) - current.low;
        if body_pct <= 0.30 {
            if upper_wick / range >= 0.60 || lower_wick / range >= 0.60 {
                patterns[i] = Pattern::PinBar;
                continue;
            }
        }
        
        // Patterns needing previous candle
        if i >= 1 {
            let prev = &candles[i - 1];
            let prev_bullish = prev.close >= prev.open;
            let current_bullish = current.close >= current.open;
            
            // 3. Inside Bar
            if current.high <= prev.high && current.low >= prev.low {
                patterns[i] = Pattern::InsideBar;
                continue;
            }
            
            // 4. Outside Bar
            if current.high > prev.high && current.low < prev.low {
                patterns[i] = Pattern::OutsideBar;
                continue;
            }

            // 5. Engulfing Patterns
            if !prev_bullish && current_bullish && current.close >= prev.open && current.open <= prev.close {
                patterns[i] = Pattern::BullishEngulfing;
                continue;
            }
            if prev_bullish && !current_bullish && current.close <= prev.open && current.open >= prev.close {
                patterns[i] = Pattern::BearishEngulfing;
                continue;
            }
        }
    }
    
    patterns
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_doji_detection() {
        let candles = vec![
            Candle {
                timestamp: Utc::now(),
                open: 100.0,
                high: 105.0,
                low: 95.0,
                close: 100.2, // body = 0.2, range = 10.0 (body_pct = 2%)
                volume: 100,
            }
        ];
        let patterns = detect_patterns(&candles);
        assert_eq!(patterns[0], Pattern::Doji);
    }

    #[test]
    fn test_pin_bar_detection() {
        let candles = vec![
            Candle {
                timestamp: Utc::now(),
                open: 100.0,
                high: 110.0,
                low: 99.0,
                close: 101.5, // range = 11.0, body = 1.5, upper wick = 8.5 (77% upper wick)
                volume: 100,
            }
        ];
        let patterns = detect_patterns(&candles);
        assert_eq!(patterns[0], Pattern::PinBar);
    }
}
