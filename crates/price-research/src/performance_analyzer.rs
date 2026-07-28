use serde::{Deserialize, Serialize};

/// Performance metrics computed from a list of closed trades.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub symbol: String,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate: f64,       // percentage
    pub total_pnl: f64,
    pub avg_pnl_per_trade: f64,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub expectancy: f64,     // avg_win * win_rate - avg_loss * loss_rate
    pub max_drawdown: f64,
    pub max_drawdown_pct: f64,
    pub sharpe_ratio: f64,
    pub profit_factor: f64,  // gross_profit / gross_loss
    pub max_consecutive_losses: usize,
}

/// A simplified closed trade record for performance analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedTrade {
    pub symbol: String,
    pub pnl: f64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub qty: i32,
}

pub struct PerformanceAnalyzer;

impl PerformanceAnalyzer {
    /// Analyze a list of closed trades and return a full performance report.
    pub fn analyze(symbol: &str, trades: &[ClosedTrade]) -> PerformanceReport {
        if trades.is_empty() {
            return PerformanceReport {
                symbol: symbol.to_string(),
                total_trades: 0,
                winning_trades: 0,
                losing_trades: 0,
                win_rate: 0.0,
                total_pnl: 0.0,
                avg_pnl_per_trade: 0.0,
                avg_win: 0.0,
                avg_loss: 0.0,
                expectancy: 0.0,
                max_drawdown: 0.0,
                max_drawdown_pct: 0.0,
                sharpe_ratio: 0.0,
                profit_factor: 0.0,
                max_consecutive_losses: 0,
            };
        }

        let wins: Vec<f64> = trades.iter().filter(|t| t.pnl > 0.0).map(|t| t.pnl).collect();
        let losses: Vec<f64> = trades.iter().filter(|t| t.pnl <= 0.0).map(|t| t.pnl.abs()).collect();

        let total_trades = trades.len();
        let winning_trades = wins.len();
        let losing_trades = losses.len();
        let win_rate = winning_trades as f64 / total_trades as f64 * 100.0;
        let loss_rate = 1.0 - (win_rate / 100.0);

        let total_pnl: f64 = trades.iter().map(|t| t.pnl).sum();
        let avg_pnl_per_trade = total_pnl / total_trades as f64;
        let avg_win = if !wins.is_empty() { wins.iter().sum::<f64>() / wins.len() as f64 } else { 0.0 };
        let avg_loss = if !losses.is_empty() { losses.iter().sum::<f64>() / losses.len() as f64 } else { 0.0 };
        let expectancy = avg_win * (win_rate / 100.0) - avg_loss * loss_rate;

        // Drawdown calculation
        let mut peak = 0.0f64;
        let mut running_pnl = 0.0f64;
        let mut max_drawdown = 0.0f64;
        let mut max_drawdown_pct = 0.0f64;
        for trade in trades {
            running_pnl += trade.pnl;
            if running_pnl > peak { peak = running_pnl; }
            let dd = peak - running_pnl;
            if dd > max_drawdown { max_drawdown = dd; }
            if peak > 0.0 {
                let dd_pct = dd / peak * 100.0;
                if dd_pct > max_drawdown_pct { max_drawdown_pct = dd_pct; }
            }
        }

        // Sharpe Ratio (annualized, assuming daily PnLs)
        let pnls: Vec<f64> = trades.iter().map(|t| t.pnl).collect();
        let mean_pnl = total_pnl / total_trades as f64;
        let variance: f64 = pnls.iter().map(|p| (p - mean_pnl).powi(2)).sum::<f64>() / total_trades as f64;
        let std_dev = variance.sqrt();
        let sharpe_ratio = if std_dev > 0.0 { (mean_pnl / std_dev) * (252f64.sqrt()) } else { 0.0 };

        // Profit factor
        let gross_profit: f64 = wins.iter().sum();
        let gross_loss: f64 = losses.iter().sum();
        let profit_factor = if gross_loss > 0.0 { gross_profit / gross_loss } else { f64::INFINITY };

        // Max consecutive losses
        let mut max_consec_losses = 0usize;
        let mut current_consec = 0usize;
        for trade in trades {
            if trade.pnl <= 0.0 {
                current_consec += 1;
                if current_consec > max_consec_losses { max_consec_losses = current_consec; }
            } else {
                current_consec = 0;
            }
        }

        PerformanceReport {
            symbol: symbol.to_string(),
            total_trades,
            winning_trades,
            losing_trades,
            win_rate,
            total_pnl,
            avg_pnl_per_trade,
            avg_win,
            avg_loss,
            expectancy,
            max_drawdown,
            max_drawdown_pct,
            sharpe_ratio,
            profit_factor,
            max_consecutive_losses: max_consec_losses,
        }
    }
}

/// Grid search optimizer for strategy parameters.
pub struct ParameterOptimizer;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationResult {
    pub params: std::collections::HashMap<String, f64>,
    pub performance: PerformanceReport,
}

impl ParameterOptimizer {
    /// Run a grid search over parameter combinations.
    /// 
    /// `param_grid` — map of parameter_name → list of values to try
    /// `evaluate_fn` — closure that takes a param map and returns a list of ClosedTrade
    pub fn grid_search<F>(
        symbol: &str,
        param_grid: &std::collections::HashMap<String, Vec<f64>>,
        mut evaluate_fn: F,
    ) -> Vec<OptimizationResult>
    where
        F: FnMut(&std::collections::HashMap<String, f64>) -> Vec<ClosedTrade>,
    {
        let keys: Vec<&String> = param_grid.keys().collect();
        let value_sets: Vec<&Vec<f64>> = keys.iter().map(|k| &param_grid[*k]).collect();

        // Generate cartesian product of all parameter combinations
        let combinations = cartesian_product(&value_sets);
        let mut results = Vec::new();

        for combo in combinations {
            let params: std::collections::HashMap<String, f64> = keys.iter()
                .zip(combo.iter())
                .map(|(k, v)| ((*k).clone(), *v))
                .collect();
            let trades = evaluate_fn(&params);
            let performance = PerformanceAnalyzer::analyze(symbol, &trades);
            results.push(OptimizationResult { params, performance });
        }

        // Sort by expectancy descending
        results.sort_by(|a, b| b.performance.expectancy.partial_cmp(&a.performance.expectancy).unwrap());
        results
    }
}

fn cartesian_product(sets: &[&Vec<f64>]) -> Vec<Vec<f64>> {
    if sets.is_empty() { return vec![vec![]]; }
    let rest = cartesian_product(&sets[1..]);
    sets[0].iter().flat_map(|&v| {
        rest.iter().map(move |r| {
            let mut combo = vec![v];
            combo.extend_from_slice(r);
            combo
        })
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_trades() -> Vec<ClosedTrade> {
        vec![
            ClosedTrade { symbol: "BTC".to_string(), pnl: 500.0, entry_price: 60000.0, exit_price: 60500.0, qty: 1 },
            ClosedTrade { symbol: "BTC".to_string(), pnl: -200.0, entry_price: 60500.0, exit_price: 60300.0, qty: 1 },
            ClosedTrade { symbol: "BTC".to_string(), pnl: 800.0, entry_price: 60300.0, exit_price: 61100.0, qty: 1 },
            ClosedTrade { symbol: "BTC".to_string(), pnl: -300.0, entry_price: 61100.0, exit_price: 60800.0, qty: 1 },
            ClosedTrade { symbol: "BTC".to_string(), pnl: 1200.0, entry_price: 60800.0, exit_price: 62000.0, qty: 1 },
        ]
    }

    #[test]
    fn test_performance_analyzer() {
        let trades = sample_trades();
        let report = PerformanceAnalyzer::analyze("BTC", &trades);
        assert_eq!(report.total_trades, 5);
        assert_eq!(report.winning_trades, 3);
        assert_eq!(report.losing_trades, 2);
        assert!((report.win_rate - 60.0).abs() < 0.01);
        assert!((report.total_pnl - 2000.0).abs() < 0.01);
        assert!(report.sharpe_ratio > 0.0);
        assert!(report.profit_factor > 1.0);
    }

    #[test]
    fn test_performance_analyzer_empty() {
        let report = PerformanceAnalyzer::analyze("BTC", &[]);
        assert_eq!(report.total_trades, 0);
        assert_eq!(report.win_rate, 0.0);
    }
}
