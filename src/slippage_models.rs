use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionCosts {
    pub commission: CommissionModel,
    pub slippage: SlippageModel,
    pub spread: SpreadModel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommissionModel {
    Fixed(f64),                    // Fixed fee per trade
    PerShare(f64),                 // Fee per share
    Percentage(f64),               // Percentage of trade value
    Tiered(Vec<(f64, f64)>),      // Volume-based tiers (volume, rate)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SlippageModel {
    Fixed(f64),                    // Fixed percentage slippage
    Linear(f64),                   // Linear with trade size
    SquareRoot(f64),               // Square root of trade size
    MarketImpact {                 // More sophisticated model
        permanent: f64,
        temporary: f64,
        liquidity_factor: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpreadModel {
    Fixed(f64),                    // Fixed spread in price units
    Percentage(f64),               // Percentage of mid price
    TimeDependent(Vec<(String, f64)>), // Different spreads by time
}

impl TransactionCosts {
    pub fn calculate_entry_cost(&self, price: f64, size: f64, volume: f64) -> f64 {
        let commission = self.calculate_commission(price, size, volume);
        let slippage = self.calculate_slippage(price, size, volume, true);
        let spread = self.calculate_spread(price) / 2.0; // Half spread for market orders
        
        commission + slippage + spread
    }
    
    pub fn calculate_exit_cost(&self, price: f64, size: f64, volume: f64) -> f64 {
        let commission = self.calculate_commission(price, size, volume);
        let slippage = self.calculate_slippage(price, size, volume, false);
        let spread = self.calculate_spread(price) / 2.0;
        
        commission + slippage + spread
    }
    
    pub fn adjust_fill_price(&self, order_price: f64, size: f64, volume: f64, is_buy: bool) -> f64 {
        let slippage_bps = match &self.slippage {
            SlippageModel::Fixed(bps) => *bps,
            SlippageModel::Linear(factor) => factor * (size / volume).min(1.0),
            SlippageModel::SquareRoot(factor) => factor * (size / volume).sqrt(),
            SlippageModel::MarketImpact { temporary, .. } => {
                temporary * (size / volume).powf(0.5)
            }
        };
        
        let spread_cost = self.calculate_spread(order_price) / 2.0;
        let total_impact = (slippage_bps / 10000.0) * order_price + spread_cost;
        
        if is_buy {
            order_price + total_impact
        } else {
            order_price - total_impact
        }
    }
    
    fn calculate_commission(&self, price: f64, size: f64, _volume: f64) -> f64 {
        match &self.commission {
            CommissionModel::Fixed(fee) => *fee,
            CommissionModel::PerShare(rate) => rate * size,
            CommissionModel::Percentage(pct) => (pct / 100.0) * price * size,
            CommissionModel::Tiered(tiers) => {
                let trade_value = price * size;
                for (threshold, rate) in tiers {
                    if trade_value <= *threshold {
                        return rate * trade_value;
                    }
                }
                // If above all tiers, use the last tier rate
                tiers.last().map_or(0.0, |(_, rate)| rate * trade_value)
            }
        }
    }
    
    fn calculate_slippage(&self, price: f64, size: f64, volume: f64, _is_entry: bool) -> f64 {
        match &self.slippage {
            SlippageModel::Fixed(bps) => (bps / 10000.0) * price * size,
            SlippageModel::Linear(factor) => {
                let impact = factor * (size / volume).min(1.0);
                (impact / 10000.0) * price * size
            }
            SlippageModel::SquareRoot(factor) => {
                let impact = factor * (size / volume).sqrt();
                (impact / 10000.0) * price * size
            }
            SlippageModel::MarketImpact { permanent, temporary, liquidity_factor } => {
                let participation_rate = size / volume;
                let perm_impact = permanent * participation_rate.powf(0.5);
                let temp_impact = temporary * participation_rate.powf(0.5);
                let liquidity_adj = 1.0 + liquidity_factor * (1.0 - (volume / 1000000.0).min(1.0));
                
                ((perm_impact + temp_impact) * liquidity_adj / 10000.0) * price * size
            }
        }
    }
    
    fn calculate_spread(&self, price: f64) -> f64 {
        match &self.spread {
            SpreadModel::Fixed(spread) => *spread,
            SpreadModel::Percentage(pct) => (pct / 100.0) * price,
            SpreadModel::TimeDependent(_) => {
                // Simplified - not every schema has bid and ask. Assuming constant spread for now.
                0.01 * price // 1% default
            }
        }
    }
}

// configurations for different markets
impl TransactionCosts {
    pub fn retail_stock_trading() -> Self {
        Self {
            commission: CommissionModel::Fixed(0.0), // Many brokers are zero commission now
            slippage: SlippageModel::Fixed(2.0),     // 2 basis points
            spread: SpreadModel::Percentage(0.01),   // 1 basis point
        }
    }
    
    pub fn futures_trading(tick_size: f64) -> Self {
        Self {
            commission: CommissionModel::Fixed(2.50),
            slippage: SlippageModel::Linear(5.0),
            spread: SpreadModel::Fixed(tick_size), // tick size for the future you are testing
        }
    }
}