pub fn calculate_wma(values: &[f64], period: usize) -> Option<f64> {
    if values.len() < period || period == 0 {
        return None;
    }
    
    let start_idx = values.len() - period;
    let mut sum = 0.0;
    let mut weight_sum = 0.0;
    
    for i in 0..period {
        let weight = (i + 1) as f64;
        sum += values[start_idx + i] * weight;
        weight_sum += weight;
    }
    
    Some(sum / weight_sum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wma_calculation() {
        let values = vec![10.0, 20.0, 30.0];
        let wma = calculate_wma(&values, 3);
        assert!(wma.is_some());
        let expected = (10.0 * 1.0 + 20.0 * 2.0 + 30.0 * 3.0) / 6.0;
        assert!((wma.unwrap() - expected).abs() < 1e-9);
    }

    #[test]
    fn test_wma_insufficient_data() {
        let values = vec![10.0, 20.0];
        let wma = calculate_wma(&values, 3);
        assert!(wma.is_none());
    }
}
