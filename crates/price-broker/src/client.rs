use async_trait::async_trait;
use price_core::{PriceError, Result};
use reqwest::Client;
use crate::models::*;
use crate::traits::Broker;

pub struct FyersClient {
    client: Client,
    base_url: String,
}

impl FyersClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }
}

#[async_trait]
impl Broker for FyersClient {
    async fn login(&self) -> Result<String> {
        let url = format!("{}/login", self.base_url);
        let res = self.client.post(&url)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            Ok(body["access_token"].as_str().unwrap_or("").to_string())
        } else {
            Err(PriceError::Authentication(body["message"].as_str().unwrap_or("Unknown authentication error").to_string()))
        }
    }

    async fn logout(&self) -> Result<()> {
        let url = format!("{}/logout", self.base_url);
        let _res = self.client.post(&url)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
        Ok(())
    }

    async fn profile(&self) -> Result<UserProfile> {
        let url = format!("{}/profile", self.base_url);
        let res = self.client.get(&url)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            let profile: UserProfile = serde_json::from_value(body["data"].clone())?;
            Ok(profile)
        } else {
            Err(PriceError::BrokerError("Failed to fetch profile".to_string()))
        }
    }

    async fn funds(&self) -> Result<AccountFunds> {
        let url = format!("{}/funds", self.base_url);
        let res = self.client.get(&url)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            let funds: AccountFunds = serde_json::from_value(body["data"].clone())?;
            Ok(funds)
        } else {
            Err(PriceError::BrokerError("Failed to fetch funds".to_string()))
        }
    }

    async fn positions(&self) -> Result<Vec<Position>> {
        let url = format!("{}/positions", self.base_url);
        let res = self.client.get(&url)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            let pos: Vec<Position> = serde_json::from_value(body["data"].clone())?;
            Ok(pos)
        } else {
            Err(PriceError::BrokerError("Failed to fetch positions".to_string()))
        }
    }

    async fn holdings(&self) -> Result<Vec<Holding>> {
        let url = format!("{}/holdings", self.base_url);
        let res = self.client.get(&url)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            let holdings: Vec<Holding> = serde_json::from_value(body["data"].clone())?;
            Ok(holdings)
        } else {
            Err(PriceError::BrokerError("Failed to fetch holdings".to_string()))
        }
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        let url = format!("{}/order", self.base_url);
        
        // Map Rust OrderRequest side to Python's integer format
        let side_int = match request.side {
            Side::Buy => 1,
            Side::Sell => -1,
        };
        
        let payload = serde_json::json!({
            "symbol": request.symbol,
            "qty": request.qty,
            "type": request.r#type,
            "side": side_int,
            "limitPrice": request.limit_price,
            "stopPrice": request.stop_price
        });
        
        let res = self.client.post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            let response: OrderResponse = serde_json::from_value(body)?;
            Ok(response)
        } else {
            Err(PriceError::InvalidOrder(body["detail"].as_str().unwrap_or("Failed to place order").to_string()))
        }
    }

    async fn modify_order(&self, request: ModifyOrder) -> Result<OrderResponse> {
        let url = format!("{}/order", self.base_url);
        let payload = serde_json::json!({
            "id": request.id,
            "qty": request.qty,
            "type": request.r#type,
            "limitPrice": request.limit_price
        });
        
        let res = self.client.put(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            let response: OrderResponse = serde_json::from_value(body)?;
            Ok(response)
        } else {
            Err(PriceError::InvalidOrder(body["detail"].as_str().unwrap_or("Failed to modify order").to_string()))
        }
    }

    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let url = format!("{}/order", self.base_url);
        let res = self.client.delete(&url)
            .query(&[("order_id", order_id)])
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            Ok(())
        } else {
            Err(PriceError::InvalidOrder(body["detail"].as_str().unwrap_or("Failed to cancel order").to_string()))
        }
    }

    async fn orderbook(&self) -> Result<Vec<Order>> {
        let url = format!("{}/orders", self.base_url);
        let res = self.client.get(&url)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            let orders: Vec<Order> = serde_json::from_value(body["data"].clone())?;
            Ok(orders)
        } else {
            Err(PriceError::BrokerError("Failed to fetch orderbook".to_string()))
        }
    }

    async fn trades(&self) -> Result<Vec<Trade>> {
        let url = format!("{}/trades", self.base_url);
        let res = self.client.get(&url)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            let trades: Vec<Trade> = serde_json::from_value(body["data"].clone())?;
            Ok(trades)
        } else {
            Err(PriceError::BrokerError("Failed to fetch trades".to_string()))
        }
    }

    async fn quotes(&self, symbols: Vec<String>) -> Result<Vec<Quote>> {
        let url = format!("{}/quotes", self.base_url);
        let res = self.client.post(&url)
            .json(&symbols)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            let mut quotes = Vec::new();
            for sym in &symbols {
                if let Some(val) = body["data"].get(sym) {
                    let quote: Quote = serde_json::from_value(val.clone())?;
                    quotes.push(quote);
                }
            }
            Ok(quotes)
        } else {
            Err(PriceError::BrokerError("Failed to fetch quotes".to_string()))
        }
    }

    async fn history(&self, request: HistoryRequest) -> Result<CandleSeries> {
        let url = format!("{}/history", self.base_url);
        let res = self.client.post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        let body: serde_json::Value = res.json()
            .await
            .map_err(|e| PriceError::Network(e.to_string()))?;
            
        if body["status"] == "success" {
            let series: CandleSeries = serde_json::from_value(body["data"].clone())?;
            Ok(series)
        } else {
            Err(PriceError::BrokerError("Failed to fetch history".to_string()))
        }
    }
}
