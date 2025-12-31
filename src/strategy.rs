use crate::event::MarketEvent;
use std::collections::HashMap;

pub trait Strategy {
    fn on_event(&mut self, event: &MarketEvent, prev: Option<&MarketEvent>) -> Option<Order>;
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
pub enum OrderType {
    MarketBuy,
    MarketSell,
    LimitBuy,
    LimitSell,
}

#[derive(Debug, Clone, Copy)]
pub struct Order {
    pub order_type: OrderType,
    pub price: f64,
}

/// Holds parameters used to configure a trading strategy
#[derive(Clone, Debug)]
pub struct StrategyParams {
    params: HashMap<String, f64>,
}

impl StrategyParams {
    /// Create a new, empty parameter map
    pub fn new() -> Self {
        Self {
            params: HashMap::new(),
        }
    }

    /// Insert a key-value pair into the strategy parameters
    pub fn insert(&mut self, key: &str, value: f64) -> &mut Self {
        self.params.insert(key.to_string(), value);
        self
    }

    /// Retrieve a value from the parameters by key
    pub fn get(&self, key: &str) -> Option<f64> {
        self.params.get(key).copied()
    }
}
