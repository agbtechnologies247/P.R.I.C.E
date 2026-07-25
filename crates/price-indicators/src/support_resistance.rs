use price_core::Candle;

#[derive(Debug, Clone, serde::Serialize)]
pub struct SrZone {
    pub price: f64,
    pub touches: usize,
    pub is_resistance: bool,
}

pub fn calculate_sr_zones(candles: &[Candle], threshold: f64) -> Vec<SrZone> {
    if candles.len() < 5 {
        return Vec::new();
    }
    
    let mut swing_highs = Vec::new();
    let mut swing_lows = Vec::new();
    
    for i in 2..(candles.len() - 2) {
        let current = &candles[i];
        let c_high = current.high;
        let c_low = current.low;
        
        let is_high = c_high > candles[i - 1].high 
            && c_high > candles[i - 2].high
            && c_high > candles[i + 1].high
            && c_high > candles[i + 2].high;
            
        let is_low = c_low < candles[i - 1].low
            && c_low < candles[i - 2].low
            && c_low < candles[i + 1].low
            && c_low < candles[i + 2].low;
            
        if is_high {
            swing_highs.push(c_high);
        }
        if is_low {
            swing_lows.push(c_low);
        }
    }
    
    let mut zones = Vec::new();
    
    let clustered_highs = cluster_levels(&swing_highs, threshold);
    for (price, count) in clustered_highs {
        zones.push(SrZone {
            price,
            touches: count,
            is_resistance: true,
        });
    }
    
    let clustered_lows = cluster_levels(&swing_lows, threshold);
    for (price, count) in clustered_lows {
        zones.push(SrZone {
            price,
            touches: count,
            is_resistance: false,
        });
    }
    
    zones.sort_by(|a, b| b.touches.cmp(&a.touches));
    zones
}

fn cluster_levels(levels: &[f64], threshold: f64) -> Vec<(f64, usize)> {
    let mut sorted = levels.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    
    let mut clusters = Vec::new();
    if sorted.is_empty() {
        return clusters;
    }
    
    let mut current_cluster = vec![sorted[0]];
    
    for &price in &sorted[1..] {
        if price - current_cluster[current_cluster.len() - 1] <= threshold {
            current_cluster.push(price);
        } else {
            let avg: f64 = current_cluster.iter().sum::<f64>() / current_cluster.len() as f64;
            clusters.push((avg, current_cluster.len()));
            current_cluster = vec![price];
        }
    }
    
    let avg: f64 = current_cluster.iter().sum::<f64>() / current_cluster.len() as f64;
    clusters.push((avg, current_cluster.len()));
    
    clusters
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_sr_swing_detection() {
        let candles = vec![
            Candle { timestamp: Utc::now(), open: 100.0, high: 100.0, low: 90.0, close: 95.0, volume: 10 },
            Candle { timestamp: Utc::now(), open: 95.0, high: 98.0, low: 88.0, close: 90.0, volume: 10 },
            Candle { timestamp: Utc::now(), open: 90.0, high: 105.0, low: 85.0, close: 100.0, volume: 10 },
            Candle { timestamp: Utc::now(), open: 100.0, high: 99.0, low: 87.0, close: 95.0, volume: 10 },
            Candle { timestamp: Utc::now(), open: 95.0, high: 97.0, low: 89.0, close: 92.0, volume: 10 },
        ];

        let zones = calculate_sr_zones(&candles, 5.0);
        assert!(!zones.is_empty());
        assert!(zones.iter().any(|z| (z.price - 105.0).abs() < 2.0 && z.is_resistance));
        assert!(zones.iter().any(|z| (z.price - 85.0).abs() < 2.0 && !z.is_resistance));
    }
}
