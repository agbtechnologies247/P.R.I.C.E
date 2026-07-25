use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendGeometry {
    pub compression: f64,
    pub expansion: f64,
    pub curvature: f64,
    pub parallelism: f64,
    pub slope: f64,
    pub divergence: f64,
    pub convergence: f64,
    pub alignment: f64,
    pub oscillation: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendDNA {
    pub compression: f64,
    pub expansion: f64,
    pub slope: f64,
    pub acceleration: f64,
    pub volatility: f64,
    pub momentum: f64,
    pub divergence: f64,
    pub convergence: f64,
    pub alignment: f64,
    pub oscillation: f64,
}

pub struct GeometryEngine {
    prev_distances: Option<Vec<f64>>,
    prev_slopes: Option<Vec<f64>>,
}

impl GeometryEngine {
    pub fn new() -> Self {
        Self {
            prev_distances: None,
            prev_slopes: None,
        }
    }

    pub fn update(&mut self, wma_values: &[f64]) -> (TrendGeometry, TrendDNA) {
        // We expect wma_values to have WMA5 to WMA100 (exactly 96 values)
        // If not, use dummy or fallback values
        let wmas = if wma_values.len() == 96 {
            wma_values.to_vec()
        } else {
            // Generate fallback values if we don't have enough data
            let mut fallback = Vec::new();
            let base = if wma_values.is_empty() { 500.0 } else { wma_values[0] };
            for i in 5..=100 {
                fallback.push(base - (i as f64) * 0.05); // slightly declining lines
            }
            fallback
        };

        // 1. Calculate 95 distances D_i = WMA_{i} - WMA_{i+1}
        let mut distances = Vec::with_capacity(95);
        for i in 0..95 {
            distances.push(wmas[i] - wmas[i + 1]);
        }

        // 2. Calculate 95 slopes
        let mut slopes = vec![0.0; 95];
        if let Some(ref prev_d) = self.prev_distances {
            for i in 0..95 {
                slopes[i] = distances[i] - prev_d[i];
            }
        }

        // 3. Calculate 95 accelerations
        let mut accelerations = vec![0.0; 95];
        if let Some(ref prev_s) = self.prev_slopes {
            for i in 0..95 {
                accelerations[i] = slopes[i] - prev_s[i];
            }
        }

        // Compute statistical indicators on the distances
        let n = distances.len() as f64;
        let sum: f64 = distances.iter().sum();
        let mean = sum / n;
        
        let variance: f64 = distances.iter().map(|d| {
            let diff = d - mean;
            diff * diff
        }).sum::<f64>() / n;
        
        let std_dev = variance.sqrt();

        // Calculate previous standard deviation for divergence/convergence
        let mut prev_std_dev = 0.0;
        if let Some(ref prev_d) = self.prev_distances {
            let prev_n = prev_d.len() as f64;
            let prev_sum: f64 = prev_d.iter().sum();
            let prev_mean = prev_sum / prev_n;
            let prev_variance: f64 = prev_d.iter().map(|d| {
                let diff = d - prev_mean;
                diff * diff
            }).sum::<f64>() / prev_n;
            prev_std_dev = prev_variance.sqrt();
        }

        // Divergence and Convergence rates
        let divergence_val = if self.prev_distances.is_some() && std_dev > prev_std_dev {
            std_dev - prev_std_dev
        } else {
            0.0
        };

        let convergence_val = if self.prev_distances.is_some() && std_dev < prev_std_dev {
            prev_std_dev - std_dev
        } else {
            0.0
        };

        // Alignment: Percentage of slopes with the same sign (consistency of trend direction)
        let pos_slopes = slopes.iter().filter(|&&s| s > 0.0).count();
        let neg_slopes = slopes.iter().filter(|&&s| s < 0.0).count();
        let alignment_val = if slopes.is_empty() {
            1.0
        } else {
            (pos_slopes.max(neg_slopes) as f64) / n
        };

        // Oscillation: Sign changes between adjacent slopes (noise indicator)
        let mut osc_changes = 0;
        for i in 0..94 {
            if slopes[i] * slopes[i + 1] < 0.0 {
                osc_changes += 1;
            }
        }
        let oscillation_val = osc_changes as f64 / 94.0;

        // Calculate Expansion vs Compression
        // Total spread is WMA5 - WMA100
        let current_range = wmas[0] - wmas[95];
        let mut range_change = 0.0;
        if let Some(ref prev_d) = self.prev_distances {
            let prev_range = wmas[0] - prev_d[0];
            range_change = current_range - prev_range;
        }

        let compression_val = if range_change < 0.0 { range_change.abs() } else { 0.0 };
        let expansion_val = if range_change > 0.0 { range_change } else { 0.0 };

        // Parallelism: Standard deviation of distances.
        let parallelism_val = 1.0 / (1.0 + std_dev);

        // Curvature: Second derivative indicator of the curve.
        let curvature_val = accelerations.iter().map(|a| a.abs()).sum::<f64>() / n;

        // Slopes / Momentum calculations
        let avg_slope: f64 = slopes.iter().sum::<f64>() / n;
        let avg_acceleration: f64 = accelerations.iter().sum::<f64>() / n;

        let geometry = TrendGeometry {
            compression: compression_val,
            expansion: expansion_val,
            curvature: curvature_val,
            parallelism: parallelism_val,
            slope: avg_slope,
            divergence: divergence_val,
            convergence: convergence_val,
            alignment: alignment_val,
            oscillation: oscillation_val,
        };

        let dna = TrendDNA {
            compression: compression_val,
            expansion: expansion_val,
            slope: avg_slope,
            acceleration: avg_acceleration,
            volatility: std_dev,
            momentum: range_change,
            divergence: divergence_val,
            convergence: convergence_val,
            alignment: alignment_val,
            oscillation: oscillation_val,
        };

        // Cache previous values
        self.prev_distances = Some(distances);
        self.prev_slopes = Some(slopes);

        (geometry, dna)
    }
}
