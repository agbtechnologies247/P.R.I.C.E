use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExitReason {
    ProfitTarget,
    StopLoss,
    TimeStop,
    MomentumWeakened,
    VwapLost,
    OiReversed,
    VolumeCollapsed,
    GeometryContracted,
}

pub struct ExitEvaluator {
    pub target_multiplier: f64,
    pub risk_multiplier: f64,
    pub max_holding_minutes: i64,
}

impl ExitEvaluator {
    pub fn new(target_multiplier: f64, risk_multiplier: f64, max_holding_minutes: i64) -> Self {
        Self {
            target_multiplier,
            risk_multiplier,
            max_holding_minutes,
        }
    }

    pub fn calculate_targets(&self, atr: f64, entry_price: f64, side: i8) -> (f64, f64) {
        let stop_distance = atr * self.risk_multiplier;
        let target_distance = atr * self.target_multiplier;

        if side == 1 {
            // Long
            let target_price = entry_price + target_distance;
            let stop_price = entry_price - stop_distance;
            (target_price, stop_price)
        } else {
            // Short
            let target_price = entry_price - target_distance;
            let stop_price = entry_price + stop_distance;
            (target_price, stop_price)
        }
    }

    pub fn should_exit(
        &self,
        current_price: f64,
        entry_price: f64,
        target_price: f64,
        stop_price: f64,
        side: i8,
        minutes_held: i64,
        vwap: f64,
        momentum_weakened: bool,
        oi_reversing: bool,
        geometry_contracted: bool,
    ) -> Option<ExitReason> {
        // 1. Profit Target reached
        if side == 1 && current_price >= target_price {
            return Some(ExitReason::ProfitTarget);
        }
        if side == -1 && current_price <= target_price {
            return Some(ExitReason::ProfitTarget);
        }

        // 2. Stop Loss reached
        if side == 1 && current_price <= stop_price {
            return Some(ExitReason::StopLoss);
        }
        if side == -1 && current_price >= stop_price {
            return Some(ExitReason::StopLoss);
        }

        // 3. Time Stop
        if minutes_held >= self.max_holding_minutes {
            return Some(ExitReason::TimeStop);
        }

        // 4. VWAP Lost
        if side == 1 && current_price < vwap {
            return Some(ExitReason::VwapLost);
        }
        if side == -1 && current_price > vwap {
            return Some(ExitReason::VwapLost);
        }

        // 5. Momentum Weakening
        if momentum_weakened {
            return Some(ExitReason::MomentumWeakened);
        }

        // 6. OI Reversal
        if oi_reversing {
            return Some(ExitReason::OiReversed);
        }

        // 7. Geometry Contracted
        if geometry_contracted {
            return Some(ExitReason::GeometryContracted);
        }

        None
    }
}
