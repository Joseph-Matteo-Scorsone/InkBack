use std::collections::HashMap;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Candle {
    /// The primary column
    pub date: String,
    /// All numeric columns, keyed by the CSV header name.
    pub fields: HashMap<String, f64>,
    /// All string columns, keyed by the CSV header name.
    pub string_fields: HashMap<String, String>,
}

#[allow(dead_code)]
impl Candle {
    /// Look up a numeric column by name (e.g. `"close"`).
    pub fn get(&self, key: &str) -> Option<f64> {
        self.fields.get(key).copied()
    }

    /// Look up a string column by name (e.g. `"footprint_data"`).
    pub fn get_string(&self, key: &str) -> Option<&String> {
        self.string_fields.get(key)
    }
}

pub trait Strategy {
    fn on_candle(&mut self, candle: &Candle, prev: Option<&Candle>) -> Option<Order>;
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