#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarginMode {
    Isolated,
    Cross,
}

pub struct LiquidationCalculator;

impl LiquidationCalculator {
    /// Computes the liquidation price of a futures position.
    /// Formula for long position:
    ///   Liquidation Price = Entry Price * (1 - (1 / Leverage) + Maintenance Margin Rate)
    /// Formula for short position:
    ///   Liquidation Price = Entry Price * (1 + (1 / Leverage) - Maintenance Margin Rate)
    pub fn calculate_liquidation_price(
        entry_price: f64,
        leverage: f64,
        side_sign: i32, // 1 for Buy/Long, -1 for Sell/Short
        maintenance_margin_rate: f64,
    ) -> f64 {
        if leverage <= 0.0 {
            return 0.0;
        }
        if side_sign > 0 {
            (entry_price * (1.0 - (1.0 / leverage) + maintenance_margin_rate)).max(0.0)
        } else {
            (entry_price * (1.0 + (1.0 / leverage) - maintenance_margin_rate)).max(0.0)
        }
    }
}

pub struct MarginManager {
    pub maintenance_margin_rate: f64,
}

impl MarginManager {
    pub fn new(maintenance_margin_rate: f64) -> Self {
        Self {
            maintenance_margin_rate,
        }
    }

    /// Checks if the available balance is sufficient to cover the initial margin.
    pub fn has_sufficient_margin(
        &self,
        qty: i32,
        price: f64,
        leverage: f64,
        available_balance: f64,
    ) -> bool {
        if leverage <= 0.0 {
            return false;
        }
        let initial_margin = (qty as f64 * price) / leverage;
        initial_margin <= available_balance
    }

    /// Evaluates if the current price is dangerously close to the liquidation price.
    /// Returns true if within the specified alert threshold percentage (e.g. 0.05 for 5%).
    pub fn is_liquidation_risk(
        &self,
        current_price: f64,
        entry_price: f64,
        leverage: f64,
        side_sign: i32,
        alert_threshold: f64,
    ) -> bool {
        let liq_price = LiquidationCalculator::calculate_liquidation_price(
            entry_price,
            leverage,
            side_sign,
            self.maintenance_margin_rate,
        );
        if liq_price == 0.0 {
            return false;
        }
        if side_sign > 0 {
            current_price <= liq_price * (1.0 + alert_threshold)
        } else {
            current_price >= liq_price * (1.0 - alert_threshold)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_liquidation_price_long() {
        let liq = LiquidationCalculator::calculate_liquidation_price(
            50000.0, // entry price
            10.0,    // leverage
            1,       // long
            0.05,    // maintenance margin rate (5%)
        );
        // 50000 * (1 - 0.10 + 0.05) = 50000 * 0.95 = 47500
        assert_eq!(liq, 47500.0);
    }

    #[test]
    fn test_calculate_liquidation_price_short() {
        let liq = LiquidationCalculator::calculate_liquidation_price(
            50000.0, // entry price
            10.0,    // leverage
            -1,      // short
            0.05,    // maintenance margin rate (5%)
        );
        // 50000 * (1 + 0.10 - 0.05) = 50000 * 1.05 = 52500
        assert_eq!(liq, 52500.0);
    }

    #[test]
    fn test_has_sufficient_margin() {
        let manager = MarginManager::new(0.05);
        let ok = manager.has_sufficient_margin(
            1,       // qty
            1000.0,  // price
            10.0,    // leverage
            150.0,   // balance
        );
        // initial margin = 1000 / 10 = 100 <= 150 -> true
        assert!(ok);

        let not_ok = manager.has_sufficient_margin(
            1,
            1000.0,
            10.0,
            50.0, // initial margin = 100 > 50 -> false
        );
        assert!(!not_ok);
    }
}
