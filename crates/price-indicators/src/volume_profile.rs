use price_core::Candle;
use std::collections::HashMap;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VolumeProfile {
    pub poc: f64,
    pub vah: f64,
    pub val: f64,
}

/// Calculates the Volume Profile metrics (POC, VAH, VAL) from a slice of closed candles.
pub fn calculate_volume_profile(candles: &[Candle], bin_size: f64) -> Option<VolumeProfile> {
    if candles.is_empty() {
        return None;
    }

    // 1. Group volumes into discrete price bins (buckets) based on candle close prices
    let mut profile: HashMap<i64, f64> = HashMap::new();
    let mut total_volume = 0.0;

    for c in candles {
        // Bin price is calculated by rounding to the nearest bin_size step
        let bin_index = (c.close / bin_size).round() as i64;
        let vol = c.volume as f64;
        *profile.entry(bin_index).or_insert(0.0) += vol;
        total_volume += vol;
    }

    if total_volume == 0.0 {
        return None;
    }

    // 2. Find Point of Control (POC)
    let mut poc_bin = 0;
    let mut max_vol = 0.0;
    for (&bin_idx, &vol) in &profile {
        if vol > max_vol {
            max_vol = vol;
            poc_bin = bin_idx;
        }
    }
    let poc = poc_bin as f64 * bin_size;

    // 3. Find VAH and VAL (Value Area containing 70% of volume)
    let target_volume = total_volume * 0.70;
    let mut current_volume = max_vol;
    
    let mut lower_bin = poc_bin;
    let mut upper_bin = poc_bin;

    while current_volume < target_volume {
        let below_vol = profile.get(&(lower_bin - 1)).copied().unwrap_or(0.0);
        let above_vol = profile.get(&(upper_bin + 1)).copied().unwrap_or(0.0);

        if below_vol == 0.0 && above_vol == 0.0 {
            break; // No more volume to grab
        }

        if above_vol >= below_vol {
            upper_bin += 1;
            current_volume += above_vol;
        } else {
            lower_bin -= 1;
            current_volume += below_vol;
        }
    }

    let val = lower_bin as f64 * bin_size;
    let vah = upper_bin as f64 * bin_size;

    Some(VolumeProfile { poc, vah, val })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_volume_profile() {
        let mut candles = Vec::new();
        // Create simple dataset where most volume is at 100.0
        candles.push(Candle {
            timestamp: Utc::now(),
            open: 98.0,
            high: 101.0,
            low: 97.0,
            close: 100.0,
            volume: 1000,
        });
        candles.push(Candle {
            timestamp: Utc::now(),
            open: 100.0,
            high: 102.0,
            low: 99.0,
            close: 100.0,
            volume: 2000,
        });
        candles.push(Candle {
            timestamp: Utc::now(),
            open: 100.0,
            high: 105.0,
            low: 99.0,
            close: 105.0,
            volume: 500,
        });

        let vp = calculate_volume_profile(&candles, 1.0).unwrap();
        assert_eq!(vp.poc, 100.0);
        assert!(vp.vah >= vp.poc);
        assert!(vp.val <= vp.poc);
    }
}
