use price_core::Candle;

pub struct AtrCalculator {
    period: usize,
    candles: Vec<Candle>,
    prev_atr: Option<f64>,
}

impl AtrCalculator {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            candles: Vec::new(),
            prev_atr: None,
        }
    }

    pub fn update(&mut self, new_candle: Candle) -> f64 {
        self.candles.push(new_candle);
        if self.candles.len() > self.period + 1 {
            self.candles.remove(0);
        }

        if self.candles.len() < 2 {
            return 10.0; // Default fallback ATR
        }

        let idx = self.candles.len() - 1;
        let high = self.candles[idx].high;
        let low = self.candles[idx].low;
        let prev_close = self.candles[idx - 1].close;

        let tr = (high - low)
            .max((high - prev_close).abs())
            .max((low - prev_close).abs());

        let atr = if let Some(prev) = self.prev_atr {
            let val = (prev * (self.period - 1) as f64 + tr) / self.period as f64;
            self.prev_atr = Some(val);
            val
        } else if self.candles.len() >= self.period {
            // First ATR is simple average of TRs
            let mut tr_sum = 0.0;
            for i in 1..self.candles.len() {
                let h = self.candles[i].high;
                let l = self.candles[i].low;
                let pc = self.candles[i - 1].close;
                let tr_val = (h - l).max((h - pc).abs()).max((l - pc).abs());
                tr_sum += tr_val;
            }
            let val = tr_sum / (self.candles.len() - 1) as f64;
            self.prev_atr = Some(val);
            val
        } else {
            tr // fallback for early periods
        };

        atr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_atr_calculator() {
        let mut calc = AtrCalculator::new(3);
        
        let c1 = Candle {
            timestamp: Utc::now(),
            open: 100.0,
            high: 110.0,
            low: 95.0,
            close: 105.0,
            volume: 1000,
        };
        
        // Only one candle, returns fallback/TR
        let atr1 = calc.update(c1);
        assert_eq!(atr1, 10.0);

        let c2 = Candle {
            timestamp: Utc::now(),
            open: 105.0,
            high: 115.0,
            low: 100.0,
            close: 112.0,
            volume: 1000,
        };

        // Two candles, calculates TR = max(115-100, |115-105|, |100-105|) = 15.0
        let atr2 = calc.update(c2);
        assert_eq!(atr2, 15.0);
    }
}
