use price_core::{PriceError, Result};
use price_broker::{Position, OrderRequest, Side};

pub struct PortfolioRiskManager {
    pub max_margin_utilization_pct: f64, // e.g., 0.80 for 80%
    pub max_concentration_pct: f64,      // e.g., 0.30 for 30%
    pub max_portfolio_leverage: f64,     // e.g., 5.0 for 5x leverage
    pub max_drawdown_limit: f64,         // e.g., 0.15 for 15%
}

impl PortfolioRiskManager {
    pub fn new(
        max_margin_utilization_pct: f64,
        max_concentration_pct: f64,
        max_portfolio_leverage: f64,
        max_drawdown_limit: f64,
    ) -> Self {
        Self {
            max_margin_utilization_pct,
            max_concentration_pct,
            max_portfolio_leverage,
            max_drawdown_limit,
        }
    }

    /// Calculate the net portfolio exposure (sum of nominal values of positions).
    pub fn calculate_exposure(&self, positions: &[Position]) -> f64 {
        positions
            .iter()
            .map(|p| (p.buy_qty.max(p.sell_qty) as f64) * p.current_price)
            .sum()
    }

    /// Calculate aggregate leverage usage = Total Exposure / Account Balance.
    pub fn calculate_leverage_usage(&self, positions: &[Position], balance: f64) -> f64 {
        if balance <= 0.0 {
            return 0.0;
        }
        let exposure = self.calculate_exposure(positions);
        exposure / balance
    }

    /// Calculate margin utilization = Sum of Initial Margins / Account Balance.
    /// Assumes default leverage of 10x for futures positions if not explicitly passed.
    pub fn calculate_margin_utilization(&self, positions: &[Position], balance: f64) -> f64 {
        if balance <= 0.0 {
            return 1.0;
        }
        let mut margin_used = 0.0;
        for p in positions {
            let nominal = (p.buy_qty.max(p.sell_qty) as f64) * p.current_price;
            // Downcast or check symbol to determine if leverage is present.
            // If it's a [LIVE] or [PAPER] crypto/future, default to 10x leverage margin requirement.
            let leverage = if p.symbol.contains("USDT") || p.symbol.contains("USD") {
                10.0
            } else {
                1.0 // options longing requires 100% premium margin
            };
            margin_used += nominal / leverage;
        }
        margin_used / balance
    }

    /// Estimate Portfolio Greeks (Delta & Gamma proxies).
    pub fn calculate_greeks(&self, positions: &[Position], volatility: f64) -> (f64, f64) {
        let mut delta = 0.0;
        let mut gamma = 0.0;

        for p in positions {
            let side_sign = if p.side == Side::Buy { 1.0 } else { -1.0 };
            let qty = p.buy_qty.max(p.sell_qty) as f64;
            
            // Delta proxy = qty * price * direction
            delta += qty * p.current_price * side_sign;
            
            // Gamma proxy = qty / (volatility * price) (higher volatility, lower gamma concentration)
            let vol = if volatility <= 0.0 { 0.15 } else { volatility };
            if p.current_price > 0.0 {
                gamma += qty / (vol * p.current_price);
            }
        }

        (delta, gamma)
    }

    /// Validates if a new trade request violates portfolio constraints.
    pub fn validate_portfolio_limits(
        &self,
        positions: &[Position],
        request: &OrderRequest,
        balance: f64,
    ) -> Result<()> {
        let is_delta = request.symbol.contains("USDT") || request.symbol.contains("USD");
        let leverage = request.leverage.unwrap_or(if is_delta { 10 } else { 1 }) as f64;
        
        let new_trade_nominal = (request.qty as f64) * if request.r#type == 1 { request.limit_price } else { 500.0 };
        let current_exposure = self.calculate_exposure(positions);
        let future_exposure = current_exposure + new_trade_nominal;
        
        // 1. Enforce portfolio leverage limits
        let future_leverage = future_exposure / balance;
        if future_leverage > self.max_portfolio_leverage {
            return Err(PriceError::RiskViolation(format!(
                "Portfolio leverage would exceed cap: {:.2}x > {:.2}x",
                future_leverage, self.max_portfolio_leverage
            )));
        }

        // 2. Enforce margin utilization limit
        let current_margin_util = self.calculate_margin_utilization(positions, balance);
        let new_margin = new_trade_nominal / leverage;
        let future_margin_util = current_margin_util + (new_margin / balance);
        if future_margin_util > self.max_margin_utilization_pct {
            return Err(PriceError::RiskViolation(format!(
                "Portfolio margin utilization would exceed cap: {:.1}% > {:.1}%",
                future_margin_util * 100.0, self.max_margin_utilization_pct * 100.0
            )));
        }

        // 3. Enforce symbol concentration limit
        let mut symbol_exposures = std::collections::HashMap::new();
        for p in positions {
            let nominal = (p.buy_qty.max(p.sell_qty) as f64) * p.current_price;
            *symbol_exposures.entry(p.symbol.clone()).or_insert(0.0) += nominal;
        }
        *symbol_exposures.entry(request.symbol.clone()).or_insert(0.0) += new_trade_nominal;

        for (sym, exp) in symbol_exposures {
            let pct = exp / future_exposure;
            if pct > self.max_concentration_pct {
                return Err(PriceError::RiskViolation(format!(
                    "Concentration for {} would exceed cap: {:.1}% > {:.1}%",
                    sym, pct * 100.0, self.max_concentration_pct * 100.0
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_exposure() {
        let manager = PortfolioRiskManager::new(0.80, 0.30, 5.0, 0.15);
        let positions = vec![
            Position {
                symbol: "BTCUSDT".to_string(),
                side: Side::Buy,
                buy_qty: 2,
                sell_qty: 0,
                avg_price: 50000.0,
                current_price: 51000.0,
                pnl: 2000.0,
            },
            Position {
                symbol: "ETHUSDT".to_string(),
                side: Side::Sell,
                buy_qty: 0,
                sell_qty: 10,
                avg_price: 3000.0,
                current_price: 2900.0,
                pnl: 1000.0,
            },
        ];

        let exp = manager.calculate_exposure(&positions);
        // 2 * 51000 + 10 * 2900 = 102000 + 29000 = 131000
        assert_eq!(exp, 131000.0);
    }

    #[test]
    fn test_calculate_leverage_usage() {
        let manager = PortfolioRiskManager::new(0.80, 0.30, 5.0, 0.15);
        let positions = vec![
            Position {
                symbol: "BTCUSDT".to_string(),
                side: Side::Buy,
                buy_qty: 2,
                sell_qty: 0,
                avg_price: 50000.0,
                current_price: 50000.0,
                pnl: 0.0,
            },
        ];
        let lev = manager.calculate_leverage_usage(&positions, 25000.0);
        // 100000 / 25000 = 4.0
        assert_eq!(lev, 4.0);
    }

    #[test]
    fn test_validate_portfolio_limits_ok() {
        let manager = PortfolioRiskManager::new(0.80, 1.0, 5.0, 0.15);
        let positions = vec![];
        let order = OrderRequest {
            symbol: "BTCUSDT".to_string(),
            qty: 1,
            r#type: 1,
            side: Side::Buy,
            limit_price: 50000.0,
            stop_price: 0.0,
            leverage: Some(10),
            reduce_only: None,
            post_only: None,
        };

        let result = manager.validate_portfolio_limits(&positions, &order, 25000.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_portfolio_limits_leverage_exceeded() {
        let manager = PortfolioRiskManager::new(0.80, 1.0, 2.0, 0.15); // max leverage 2x
        let positions = vec![];
        let order = OrderRequest {
            symbol: "BTCUSDT".to_string(),
            qty: 2,
            r#type: 1,
            side: Side::Buy,
            limit_price: 50000.0,
            stop_price: 0.0,
            leverage: Some(10),
            reduce_only: None,
            post_only: None,
        };

        // nominal = 100000. balance = 25000 -> future leverage = 4x > 2x limit -> error
        let result = manager.validate_portfolio_limits(&positions, &order, 25000.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_portfolio_limits_concentration_exceeded() {
        let manager = PortfolioRiskManager::new(0.80, 0.40, 5.0, 0.15); // max concentration 40%
        let positions = vec![
            Position {
                symbol: "ETHUSDT".to_string(),
                side: Side::Buy,
                buy_qty: 10,
                sell_qty: 0,
                avg_price: 3000.0,
                current_price: 3000.0,
                pnl: 0.0,
            },
        ];
        let order = OrderRequest {
            symbol: "BTCUSDT".to_string(),
            qty: 1,
            r#type: 1,
            side: Side::Buy,
            limit_price: 50000.0, // nominal = 50000. ETH = 30000. Total = 80000. BTC concentration = 50000/80000 = 62.5% > 40%
            stop_price: 0.0,
            leverage: Some(10),
            reduce_only: None,
            post_only: None,
        };

        let result = manager.validate_portfolio_limits(&positions, &order, 25000.0);
        assert!(result.is_err());
    }
}
