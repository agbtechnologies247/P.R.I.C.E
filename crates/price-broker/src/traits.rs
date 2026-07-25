use async_trait::async_trait;
use price_core::Result;
use crate::models::*;

#[async_trait]
pub trait Broker: Send + Sync {
    async fn login(&self) -> Result<String>;
    async fn logout(&self) -> Result<()>;
    async fn profile(&self) -> Result<UserProfile>;
    async fn funds(&self) -> Result<AccountFunds>;
    async fn positions(&self) -> Result<Vec<Position>>;
    async fn holdings(&self) -> Result<Vec<Holding>>;
    async fn place_order(&self, request: OrderRequest) -> Result<OrderResponse>;
    async fn modify_order(&self, request: ModifyOrder) -> Result<OrderResponse>;
    async fn cancel_order(&self, order_id: &str) -> Result<()>;
    async fn orderbook(&self) -> Result<Vec<Order>>;
    async fn trades(&self) -> Result<Vec<Trade>>;
    async fn quotes(&self, symbols: Vec<String>) -> Result<Vec<Quote>>;
    async fn history(&self, request: HistoryRequest) -> Result<CandleSeries>;
}
