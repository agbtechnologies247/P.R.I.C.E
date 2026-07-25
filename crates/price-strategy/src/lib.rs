pub mod opportunity;
pub mod exit;

pub use opportunity::{TradeOpportunity, Decision, TradeQualityScore, OpportunityEngine};
pub use exit::{ExitReason, ExitEvaluator};
