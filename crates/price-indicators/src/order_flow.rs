#[derive(Debug, Clone)]
pub struct OrderFlowTracker {
    pub last_price: f64,
    pub last_oi: u64,
    pub last_direction: i8,
    pub cvd: f64,
    pub last_volume_delta: f64,
    pub last_oi_delta: i64,
    pub history: Vec<(f64, f64, f64)>, // (Price, CVD, OI_Delta) rolling cache
    pub max_history_len: usize,
}

impl OrderFlowTracker {
    pub fn new(max_history_len: usize) -> Self {
        Self {
            last_price: 0.0,
            last_oi: 0,
            last_direction: 1,
            cvd: 0.0,
            last_volume_delta: 0.0,
            last_oi_delta: 0,
            history: Vec::with_capacity(max_history_len),
            max_history_len,
        }
    }

    /// Update trackers with a new tick.
    pub fn update(&mut self, price: f64, volume: u64, oi: u64) {
        let direction = if self.last_price == 0.0 {
            1
        } else if price > self.last_price {
            1
        } else if price < self.last_price {
            -1
        } else {
            self.last_direction // Zero-tick test rule
        };

        let volume_delta = (volume as f64) * (direction as f64);
        self.cvd += volume_delta;

        let oi_delta = if self.last_oi == 0 {
            0
        } else {
            (oi as i64) - (self.last_oi as i64)
        };

        self.last_price = price;
        self.last_oi = oi;
        self.last_direction = direction;
        self.last_volume_delta = volume_delta;
        self.last_oi_delta = oi_delta;

        // Maintain rolling history
        if self.history.len() >= self.max_history_len {
            self.history.remove(0);
        }
        self.history.push((price, self.cvd, oi_delta as f64));
    }

    /// Checks if a divergence exists between the Price and Cumulative Volume Delta (CVD).
    /// * Bearish Divergence: Price is rising but CVD is falling -> returns true (indicating potential reversal down).
    /// * Bullish Divergence: Price is falling but CVD is rising -> returns true (indicating potential reversal up).
    pub fn detect_divergence(&self, window: usize) -> bool {
        let n = self.history.len();
        if n < window || window < 2 {
            return false;
        }

        let start_idx = n - window;
        let (start_price, start_cvd, _) = self.history[start_idx];
        let (end_price, end_cvd, _) = self.history[n - 1];

        let price_change = end_price - start_price;
        let cvd_change = end_cvd - start_cvd;

        // Price is rising but buying pressure (CVD) is falling
        if price_change > 0.0 && cvd_change < 0.0 {
            return true;
        }

        // Price is falling but buying pressure (CVD) is rising
        if price_change < 0.0 && cvd_change > 0.0 {
            return true;
        }

        false
    }

    /// Checks if Open Interest is dropping significantly, indicating a squeeze or position unwinding.
    pub fn detect_oi_reversal(&self, window: usize) -> bool {
        let n = self.history.len();
        if n < window || window < 2 {
            return false;
        }

        let start_idx = n - window;
        let mut negative_oi_deltas = 0;
        let mut total_checks = 0;

        for idx in start_idx..n {
            let (_, _, oi_d) = self.history[idx];
            total_checks += 1;
            if oi_d < 0.0 {
                negative_oi_deltas += 1;
            }
        }

        // Reversal signal if open interest drops consistently in 70%+ of ticks within the window
        (negative_oi_deltas as f64 / total_checks as f64) > 0.70
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tick_test_classification() {
        let mut tracker = OrderFlowTracker::new(10);
        
        // Initial tick
        tracker.update(100.0, 10, 1000);
        assert_eq!(tracker.last_direction, 1);
        assert_eq!(tracker.cvd, 10.0);

        // Price uptick
        tracker.update(101.0, 20, 1050);
        assert_eq!(tracker.last_direction, 1);
        assert_eq!(tracker.cvd, 30.0);
        assert_eq!(tracker.last_oi_delta, 50);

        // Price downtick
        tracker.update(99.5, 15, 1040);
        assert_eq!(tracker.last_direction, -1);
        assert_eq!(tracker.cvd, 15.0);
        assert_eq!(tracker.last_oi_delta, -10);

        // Flat price (zero-tick test)
        tracker.update(99.5, 5, 1040);
        assert_eq!(tracker.last_direction, -1); // should inherit previous downtick
        assert_eq!(tracker.cvd, 10.0);
    }

    #[test]
    fn test_detect_divergence() {
        let mut tracker = OrderFlowTracker::new(20);
        
        // Price rising, CVD falling (bearish divergence)
        let prices = vec![100.0, 101.0, 102.0, 103.0, 104.0];
        let cvd_vals = vec![50.0, 48.0, 46.0, 44.0, 42.0];
        
        for i in 0..5 {
            tracker.history.push((prices[i], cvd_vals[i], 0.0));
        }

        assert!(tracker.detect_divergence(5));
    }
}
