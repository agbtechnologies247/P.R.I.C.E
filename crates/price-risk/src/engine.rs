use price_core::{PriceError, Result};
use price_broker::{OrderRequest, AccountFunds};

pub struct RiskEngine {
    pub max_trades_per_day: i32,
    pub daily_loss_limit: f64,
    pub trades_today: i32,
    pub pnl_today: f64,
}

impl RiskEngine {
    pub fn new(max_trades_per_day: i32, daily_loss_limit: f64) -> Self {
        Self {
            max_trades_per_day,
            daily_loss_limit,
            trades_today: 0,
            pnl_today: 0.0,
        }
    }

    pub fn validate_order(&self, request: &OrderRequest, funds: &AccountFunds) -> Result<()> {
        // 1. Max trades rule
        if self.trades_today >= self.max_trades_per_day {
            return Err(PriceError::RiskViolation(format!(
                "Max trades per day limit reached ({} / {})",
                self.trades_today, self.max_trades_per_day
            )));
        }

        // 2. Daily drawdown check
        if self.pnl_today <= -self.daily_loss_limit {
            return Err(PriceError::RiskViolation(format!(
                "Daily loss limit exceeded (PnL: {}, Limit: -{})",
                self.pnl_today, self.daily_loss_limit
            )));
        }

        // 3. Balance verification
        let cost = request.qty as f64 * if request.r#type == 1 { request.limit_price } else { 500.0 };
        if cost > funds.available_balance {
            return Err(PriceError::InsufficientFunds {
                available: funds.available_balance,
                required: cost,
            });
        }

        // 4. Quantity freeze safety limit (no absurd sizes)
        if request.qty <= 0 || request.qty > 5000 {
            return Err(PriceError::RiskViolation(format!(
                "Order quantity {} violates freeze limits (1 - 5000)",
                request.qty
            )));
        }

        Ok(())
    }

    /// Calculate dynamic position size based on the Kelly Criterion.
    /// Kf = p - (1 - p) / R
    pub fn calculate_position_size(
        &self,
        available_balance: f64,
        win_probability: f64,
        reward_risk_ratio: f64,
        stop_loss_points: f64,
        fractional_kelly: f64,
        entry_price: f64,
    ) -> i32 {
        if win_probability <= 0.0 || reward_risk_ratio <= 0.0 || stop_loss_points <= 0.0 {
            return 15; // default lot size fallback
        }

        let kelly_fraction = win_probability - (1.0 - win_probability) / reward_risk_ratio;
        if kelly_fraction <= 0.0 {
            return 0; // Kelly suggests avoiding the trade
        }

        let budget = available_balance * kelly_fraction * fractional_kelly;
        
        // Contract multiplier is 65 (lot size of Nifty options)
        let lot_size = 65.0;
        let cost_per_point_loss = lot_size * stop_loss_points;
        
        let lots = (budget / cost_per_point_loss).floor() as i32;
        let mut qty = lots * 65;
        
        // Cap by available balance (margin limit)
        let cost = qty as f64 * entry_price;
        if cost > available_balance && entry_price > 0.0 {
            let max_possible_qty = (available_balance / entry_price).floor() as i32;
            let max_possible_lots = max_possible_qty / 65;
            qty = max_possible_lots * 65;
        }

        // Return quantity within safe bounds (minimum 65 options, maximum 1300)
        qty.max(65).min(1300)
    }

    pub fn record_trade_exit(&mut self, pnl: f64) {
        self.trades_today += 1;
        self.pnl_today += pnl;
    }

    pub fn reset_daily(&mut self) {
        self.trades_today = 0;
        self.pnl_today = 0.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kelly_position_sizing() {
        let engine = RiskEngine::new(3, 2000.0);
        let qty = engine.calculate_position_size(
            10000.0, // available_balance
            0.60,    // win_probability
            3.0,     // reward_risk_ratio
            10.0,    // stop_loss_points
            0.5,     // fractional_kelly (Half-Kelly)
            5.0,     // entry_price
        );
        
        // Expected lots = floor((10000 * 0.46666 * 0.5) / 650.0) = 3 lots -> 195 qty
        assert_eq!(qty, 195);
    }

    #[test]
    fn test_kelly_negative_fraction() {
        let engine = RiskEngine::new(3, 2000.0);
        let qty = engine.calculate_position_size(
            10000.0,
            0.20, // 20% win rate is too low for 3:1 RR
            3.0,
            10.0,
            0.5,
            5.0,
        );
        // Kf = 0.20 - 0.80/3 = -0.066 -> should return 0 (no trade)
        assert_eq!(qty, 0);
    }

    #[test]
    fn test_kelly_capping() {
        let engine = RiskEngine::new(3, 2000.0);
        let qty = engine.calculate_position_size(
            10000.0,
            0.60,
            3.0,
            10.0,
            0.5,
            100.0, // entry_price (uncapped cost = 195 * 100 = 19500 > 10000) -> should cap to max lots possible = floor(10000 / 100) = 100 -> max lots = 1 -> 65 qty
        );
        assert_eq!(qty, 65);
    }
}
