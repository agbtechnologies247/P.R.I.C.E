use dashmap::DashMap;
use async_trait::async_trait;
use price_core::{Result, PriceError, is_indian_market_hours};
use crate::models::*;
use crate::traits::Broker;
use crate::client::FyersClient;
use crate::paper::PaperBroker;

pub struct HybridBroker {
    pub live: FyersClient,
    pub paper: PaperBroker,
    // Maps live order_id -> paper order_id
    pub order_map: DashMap<String, String>,
}

impl HybridBroker {
    pub fn new(python_broker_url: &str, initial_paper_balance: f64) -> Self {
        Self {
            live: FyersClient::new(python_broker_url),
            paper: PaperBroker::new(initial_paper_balance),
            order_map: DashMap::new(),
        }
    }
}

#[async_trait]
impl Broker for HybridBroker {
    async fn login(&self) -> Result<String> {
        // Login to live client (critical) and login to paper
        let token = self.live.login().await?;
        let _ = self.paper.login().await;
        Ok(token)
    }

    async fn logout(&self) -> Result<()> {
        let _ = self.live.logout().await;
        let _ = self.paper.logout().await;
        Ok(())
    }

    async fn profile(&self) -> Result<UserProfile> {
        self.live.profile().await
    }

    async fn funds(&self) -> Result<AccountFunds> {
        // Return live funds by default for standard queries
        self.live.funds().await
    }

    async fn positions(&self) -> Result<Vec<Position>> {
        let mut live_pos = self.live.positions().await.unwrap_or_default();
        for p in &mut live_pos {
            p.symbol = format!("{} [LIVE]", p.symbol);
        }

        let mut paper_pos = self.paper.positions().await.unwrap_or_default();
        for p in &mut paper_pos {
            p.symbol = format!("{} [PAPER]", p.symbol);
        }

        live_pos.extend(paper_pos);
        Ok(live_pos)
    }

    async fn holdings(&self) -> Result<Vec<Holding>> {
        let mut live_holdings = self.live.holdings().await.unwrap_or_default();
        for h in &mut live_holdings {
            h.symbol = format!("{} [LIVE]", h.symbol);
        }

        let mut paper_holdings = self.paper.holdings().await.unwrap_or_default();
        for h in &mut paper_holdings {
            h.symbol = format!("{} [PAPER]", h.symbol);
        }

        live_holdings.extend(paper_holdings);
        Ok(live_holdings)
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        // Market hours guard: block trading outside NSE hours
        if !is_indian_market_hours(chrono::Utc::now()) {
            tracing::warn!("Order placement blocked: Outside Indian market hours.");
            return Err(PriceError::MarketClosed);
        }

        // Place on live
        let live_res = self.live.place_order(request.clone()).await;
        // Place on paper
        let paper_res = self.paper.place_order(request).await;

        match (live_res, paper_res) {
            (Ok(l), Ok(p)) => {
                self.order_map.insert(l.order_id.clone(), p.order_id.clone());
                tracing::info!("Hybrid order placed successfully on both Live ({}) and Paper ({})", l.order_id, p.order_id);
                Ok(l)
            }
            (Ok(l), Err(e)) => {
                tracing::warn!("Hybrid order placed on Live ({}) but Paper placement failed: {:?}", l.order_id, e);
                Ok(l)
            }
            (Err(e), Ok(p)) => {
                tracing::warn!("Hybrid order placed on Paper ({}) but Live placement failed: {:?}", p.order_id, e);
                Err(e)
            }
            (Err(e1), Err(e2)) => {
                tracing::error!("Hybrid order placement failed on both Live ({:?}) and Paper ({:?})", e1, e2);
                Err(e1)
            }
        }
    }

    async fn modify_order(&self, request: ModifyOrder) -> Result<OrderResponse> {
        if !is_indian_market_hours(chrono::Utc::now()) {
            return Err(PriceError::MarketClosed);
        }

        // Modify on live
        let live_res = self.live.modify_order(request.clone()).await;

        // If mapped, modify on paper
        if let Some(paper_id) = self.order_map.get(&request.id) {
            let mut paper_req = request.clone();
            paper_req.id = paper_id.clone();
            let _ = self.paper.modify_order(paper_req).await;
        }

        live_res
    }

    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        if !is_indian_market_hours(chrono::Utc::now()) {
            return Err(PriceError::MarketClosed);
        }

        // Cancel on live
        let live_res = self.live.cancel_order(order_id).await;

        // If mapped, cancel on paper
        if let Some(paper_id) = self.order_map.get(order_id) {
            let _ = self.paper.cancel_order(&paper_id).await;
        }

        live_res
    }

    async fn orderbook(&self) -> Result<Vec<Order>> {
        let mut live_orders = self.live.orderbook().await.unwrap_or_default();
        for o in &mut live_orders {
            o.symbol = format!("{} [LIVE]", o.symbol);
        }

        let mut paper_orders = self.paper.orderbook().await.unwrap_or_default();
        for o in &mut paper_orders {
            o.symbol = format!("{} [PAPER]", o.symbol);
        }

        live_orders.extend(paper_orders);
        Ok(live_orders)
    }

    async fn trades(&self) -> Result<Vec<Trade>> {
        let mut live_trades = self.live.trades().await.unwrap_or_default();
        for t in &mut live_trades {
            t.symbol = format!("{} [LIVE]", t.symbol);
        }

        let mut paper_trades = self.paper.trades().await.unwrap_or_default();
        for t in &mut paper_trades {
            t.symbol = format!("{} [PAPER]", t.symbol);
        }

        live_trades.extend(paper_trades);
        Ok(live_trades)
    }

    async fn quotes(&self, symbols: Vec<String>) -> Result<Vec<Quote>> {
        self.live.quotes(symbols).await
    }

    async fn history(&self, request: HistoryRequest) -> Result<CandleSeries> {
        self.live.history(request).await
    }

    fn broker_type(&self) -> BrokerType {
        self.live.broker_type()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
