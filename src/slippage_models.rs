use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionCosts {
    pub commission: CommissionModel,
    pub slippage: SlippageModel,
    pub spread: SpreadModel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommissionModel {
    Fixed(f64),              // Fixed fee per trade
    PerShare(f64),           // Fee per share
    Percentage(f64),         // Percentage of trade value
    Tiered(Vec<(f64, f64)>), // Volume-based tiers (volume, rate)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SlippageModel {
    Fixed(f64),      // Fixed percentage slippage
    Linear(f64),     // Linear with trade size
    SquareRoot(f64), // Square root of trade size
    TickBased(f64),  // Fixed number of ticks slippage
    MarketImpact {
        // More sophisticated model
        permanent: f64,
        temporary: f64,
        liquidity_factor: f64,
    },
    OptionsSlippage {
        // Options-specific slippage model
        base_slippage_bps: f64,  // Base slippage in basis points
        liquidity_factor: f64,   // Multiplier for low liquidity
        bid_ask_multiplier: f64, // Fraction of bid-ask spread as slippage
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpreadModel {
    Fixed(f64),                        // Fixed spread in price units
    Percentage(f64),                   // Percentage of mid price
    TimeDependent(Vec<(String, f64)>), // Different spreads by time
    OptionsBidAsk {
        // Options-specific bid-ask spread model
        min_spread: f64,     // Minimum spread in dollars
        spread_pct: f64,     // Percentage of option price
        max_spread_pct: f64, // Maximum spread as % of price (for cheap options)
    },
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

    pub fn adjust_fill_price(&self, order_price: f64, size: f64, is_buy: bool) -> f64 {
        let slippage_amount = match &self.slippage {
            SlippageModel::Fixed(bps) => (bps / 10000.0) * order_price,
            SlippageModel::Linear(factor) => {
                let impact = factor * (size).min(1.0);
                (impact / 10000.0) * order_price
            }
            SlippageModel::SquareRoot(factor) => {
                let impact = factor * (size).sqrt();
                (impact / 10000.0) * order_price
            }
            SlippageModel::TickBased(ticks) => *ticks,
            SlippageModel::MarketImpact { temporary, .. } => {
                let impact = temporary * (size).sqrt();
                (impact / 10000.0) * order_price
            }
            SlippageModel::OptionsSlippage {
                base_slippage_bps,
                liquidity_factor,
                bid_ask_multiplier,
            } => {
                let participation_rate = (size).min(1.0);
                let liquidity_penalty = if participation_rate > 0.1 {
                    liquidity_factor * participation_rate
                } else {
                    1.0
                };

                let bid_ask_spread = self.calculate_spread(order_price);
                let spread_slippage = bid_ask_multiplier * bid_ask_spread;

                let base_slippage = (base_slippage_bps * liquidity_penalty / 10000.0) * order_price;
                base_slippage + spread_slippage
            }
        };

        let spread_cost = self.calculate_spread(order_price) / 2.0;
        let total_impact = slippage_amount + spread_cost;

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
            SlippageModel::TickBased(ticks) => ticks * size,
            SlippageModel::MarketImpact {
                permanent,
                temporary,
                liquidity_factor,
            } => {
                let participation_rate = size / volume;
                let perm_impact = permanent * participation_rate.powf(0.5);
                let temp_impact = temporary * participation_rate.powf(0.5);
                let liquidity_adj = 1.0 + liquidity_factor * (1.0 - (volume / 1000000.0).min(1.0));

                ((perm_impact + temp_impact) * liquidity_adj / 10000.0) * price * size
            }
            SlippageModel::OptionsSlippage {
                base_slippage_bps,
                liquidity_factor,
                bid_ask_multiplier,
            } => {
                let participation_rate = (size / volume).min(1.0);
                let liquidity_penalty = if participation_rate > 0.1 {
                    liquidity_factor * participation_rate
                } else {
                    1.0
                };

                // Base slippage cost
                let base_cost = (base_slippage_bps * liquidity_penalty / 10000.0) * price * size;

                // Additional bid-ask spread cost
                let spread = self.calculate_spread(price);
                let spread_cost = bid_ask_multiplier * spread * size;

                base_cost + spread_cost
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
            SpreadModel::OptionsBidAsk {
                min_spread,
                spread_pct,
                max_spread_pct,
            } => {
                let percentage_spread = (spread_pct / 100.0) * price;
                let max_spread = (max_spread_pct / 100.0) * price;

                // Use the larger of minimum spread or percentage spread, but cap at max
                percentage_spread.max(*min_spread).min(max_spread)
            }
        }
    }
}

// configurations for different markets
impl TransactionCosts {
    pub fn equity_trading() -> Self {
        Self {
            commission: CommissionModel::Fixed(0.0), // Many brokers are zero commission now
            slippage: SlippageModel::Fixed(2.0),     // 2 basis points
            spread: SpreadModel::Percentage(0.01),   // 1 basis point
        }
    }

    pub fn futures_trading(tick_size: f64) -> Self {
        Self {
            commission: CommissionModel::Fixed(2.50),
            slippage: SlippageModel::TickBased(tick_size), // 1 tick of slippage
            spread: SpreadModel::Fixed(tick_size), // tick size for the future you are testing
        }
    }

    pub fn options_trading() -> Self {
        Self {
            commission: CommissionModel::PerShare(0.65), // $0.65 per contract (typical options commission)
            slippage: SlippageModel::OptionsSlippage {
                base_slippage_bps: 10.0, // 10 basis points base slippage
                liquidity_factor: 2.0,   // Options are less liquid than stocks
                bid_ask_multiplier: 0.5, // Half the bid-ask spread as slippage
            },
            spread: SpreadModel::OptionsBidAsk {
                min_spread: 0.05,     // Minimum $0.05 spread
                spread_pct: 2.0,      // 2% of option price
                max_spread_pct: 50.0, // Cap at 50% for very cheap options
            },
        }
    }
}
