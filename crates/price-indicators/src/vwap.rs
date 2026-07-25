pub struct VwapCalculator {
    cumulative_pv: f64,
    cumulative_volume: f64,
}

impl VwapCalculator {
    pub fn new() -> Self {
        Self {
            cumulative_pv: 0.0,
            cumulative_volume: 0.0,
        }
    }

    pub fn update(&mut self, price: f64, volume: u64) -> f64 {
        self.cumulative_pv += price * (volume as f64);
        self.cumulative_volume += volume as f64;
        
        if self.cumulative_volume == 0.0 {
            price
        } else {
            self.cumulative_pv / self.cumulative_volume
        }
    }

    pub fn reset(&mut self) {
        self.cumulative_pv = 0.0;
        self.cumulative_volume = 0.0;
    }
}
