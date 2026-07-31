use price_core::{TickData, Candle};

pub struct CandleAggregator {
    current_candle: Option<Candle>,
}

impl CandleAggregator {
    pub fn new() -> Self {
        Self { current_candle: None }
    }

    pub fn ingest_tick(&mut self, tick: &TickData) -> Option<Candle> {
        let tick_minute = tick.timestamp.format("%Y-%m-%d %H:%M").to_string();
        
        if let Some(ref mut c) = self.current_candle {
            let candle_minute = c.timestamp.format("%Y-%m-%d %H:%M").to_string();
            if tick_minute == candle_minute {
                c.high = c.high.max(tick.price);
                c.low = c.low.min(tick.price);
                c.close = tick.price;
                c.volume += tick.volume;
                None
            } else {
                let closed_candle = c.clone();
                *c = Candle {
                    timestamp: tick.timestamp,
                    open: tick.price,
                    high: tick.price,
                    low: tick.price,
                    close: tick.price,
                    volume: tick.volume,
                };
                Some(closed_candle)
            }
        } else {
            self.current_candle = Some(Candle {
                timestamp: tick.timestamp,
                open: tick.price,
                high: tick.price,
                low: tick.price,
                close: tick.price,
                volume: tick.volume,
            });
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_candle_aggregator() {
        let mut agg = CandleAggregator::new();
        
        let t1 = TickData {
            symbol: "NSE:NIFTYBANK-ATM-CE".to_string(),
            price: 100.0,
            volume: 10,
            oi: 1000,
            timestamp: Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap(),
            bid: None,
            ask: None,
            mark_price: None,
        };

        let t2 = TickData {
            symbol: "NSE:NIFTYBANK-ATM-CE".to_string(),
            price: 105.0,
            volume: 15,
            oi: 1000,
            timestamp: Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 30).unwrap(),
            bid: None,
            ask: None,
            mark_price: None,
        };

        let t3 = TickData {
            symbol: "NSE:NIFTYBANK-ATM-CE".to_string(),
            price: 98.0,
            volume: 5,
            oi: 1000,
            timestamp: Utc.with_ymd_and_hms(2026, 7, 25, 12, 1, 0).unwrap(),
            bid: None,
            ask: None,
            mark_price: None,
        };

        use chrono::Utc;

        // First tick starts the candle
        assert!(agg.ingest_tick(&t1).is_none());
        // Second tick updates the candle (same minute)
        assert!(agg.ingest_tick(&t2).is_none());
        
        // Third tick is in a new minute, so it should close the previous one
        let closed = agg.ingest_tick(&t3);
        assert!(closed.is_some());
        let c = closed.unwrap();
        assert_eq!(c.open, 100.0);
        assert_eq!(c.high, 105.0);
        assert_eq!(c.low, 100.0);
        assert_eq!(c.close, 105.0);
        assert_eq!(c.volume, 25);
    }
}
