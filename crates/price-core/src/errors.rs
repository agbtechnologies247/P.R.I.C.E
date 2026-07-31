use thiserror::Error;

#[derive(Error, Debug)]
pub enum PriceError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Network connection error: {0}")]
    Network(String),

    #[error("Authentication failed: {0}")]
    Authentication(String),

    #[error("Invalid order request: {0}")]
    InvalidOrder(String),

    #[error("Risk limit violation: {0}")]
    RiskViolation(String),

    #[error("Broker integration error: {0}")]
    BrokerError(String),

    #[error("Slippage limit exceeded: expected {expected}, actual {actual}")]
    SlippageExceeded { expected: f64, actual: f64 },

    #[error("Indicator error: {0}")]
    Indicator(String),

    #[error("Insufficient funds: available {available}, required {required}")]
    InsufficientFunds { available: f64, required: f64 },

    #[error("JSON serialization/deserialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("System failure: {0}")]
    System(String),

    #[error("Indian market is closed. Trades can only be placed Mon-Fri 09:15 - 15:30 IST (excluding holidays).")]
    MarketClosed,

    #[error("Rate limit exceeded. Retry after {retry_after_ms}ms. Quota used: {quota_used}/{quota_limit}")]
    RateLimitExceeded {
        retry_after_ms: u64,
        quota_used: u64,
        quota_limit: u64,
    },

    #[error("API signature expired. Server time drift detected: {0}")]
    SignatureExpired(String),
}

pub type Result<T> = std::result::Result<T, PriceError>;
