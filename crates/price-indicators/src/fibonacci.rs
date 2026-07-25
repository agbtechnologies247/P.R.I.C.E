use price_core::Candle;

#[derive(Debug, Clone, serde::Serialize)]
pub struct FibLevels {
    pub low: f64,
    pub high: f64,
    pub fib_236: f64,
    pub fib_382: f64,
    pub fib_500: f64,
    pub fib_618: f64,
    pub fib_786: f64,
    pub is_bullish: bool,
}

pub fn calculate_fib_levels(candles: &[Candle]) -> Option<FibLevels> {
    if candles.len() < 10 {
        return None;
    }
    
    let window = if candles.len() > 50 {
        &candles[candles.len() - 50..]
    } else {
        candles
    };
    
    let mut low_price = f64::MAX;
    let mut high_price = f64::MIN;
    let mut low_idx = 0;
    let mut high_idx = 0;
    
    for (i, c) in window.iter().enumerate() {
        if c.low < low_price {
            low_price = c.low;
            low_idx = i;
        }
        if c.high > high_price {
            high_price = c.high;
            high_idx = i;
        }
    }
    
    let is_bullish = low_idx < high_idx;
    let range = high_price - low_price;
    if range <= 0.05 {
        return None;
    }
    
    let (fib_236, fib_382, fib_500, fib_618, fib_786) = if is_bullish {
        (
            high_price - range * 0.236,
            high_price - range * 0.382,
            high_price - range * 0.500,
            high_price - range * 0.618,
            high_price - range * 0.786,
        )
    } else {
        (
            low_price + range * 0.236,
            low_price + range * 0.382,
            low_price + range * 0.500,
            low_price + range * 0.618,
            low_price + range * 0.786,
        )
    };
    
    Some(FibLevels {
        low: low_price,
        high: high_price,
        fib_236,
        fib_382,
        fib_500,
        fib_618,
        fib_786,
        is_bullish,
    })
}

pub fn calculate_confluence_score(price: f64, fib: &FibLevels, tolerance: f64) -> f64 {
    let levels = [
        fib.fib_236,
        fib.fib_382,
        fib.fib_500,
        fib.fib_618,
        fib.fib_786,
    ];
    
    let mut min_diff = f64::MAX;
    for &lvl in &levels {
        let diff = (price - lvl).abs();
        if diff < min_diff {
            min_diff = diff;
        }
    }
    
    if min_diff >= tolerance {
        0.0
    } else {
        1.0 - (min_diff / tolerance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_fib_retracement() {
        let candles = vec![
            Candle { timestamp: Utc::now(), open: 100.0, high: 100.0, low: 100.0, close: 100.0, volume: 1 },
            Candle { timestamp: Utc::now(), open: 100.0, high: 100.0, low: 90.0, close: 90.0, volume: 1 },
            Candle { timestamp: Utc::now(), open: 90.0, high: 120.0, low: 90.0, close: 120.0, volume: 1 },
            Candle { timestamp: Utc::now(), open: 120.0, high: 120.0, low: 120.0, close: 120.0, volume: 1 },
            Candle { timestamp: Utc::now(), open: 120.0, high: 120.0, low: 120.0, close: 120.0, volume: 1 },
            Candle { timestamp: Utc::now(), open: 120.0, high: 120.0, low: 120.0, close: 120.0, volume: 1 },
            Candle { timestamp: Utc::now(), open: 120.0, high: 120.0, low: 120.0, close: 120.0, volume: 1 },
            Candle { timestamp: Utc::now(), open: 120.0, high: 120.0, low: 120.0, close: 120.0, volume: 1 },
            Candle { timestamp: Utc::now(), open: 120.0, high: 120.0, low: 120.0, close: 120.0, volume: 1 },
            Candle { timestamp: Utc::now(), open: 120.0, high: 120.0, low: 120.0, close: 120.0, volume: 1 },
        ];
        
        let fib = calculate_fib_levels(&candles).unwrap();
        assert!(fib.is_bullish);
        assert_eq!(fib.fib_500, 105.0);
        assert!((fib.fib_618 - 101.46).abs() < 0.1);
    }
}
