use anyhow::Result;
use databento::dbn::{SType, Schema};
use std::{collections::VecDeque, usize};
use time::{macros::date, macros::time};

mod backtester;
mod event;
mod plot;
pub mod slippage_models;
mod strategy;
mod utils;

use crate::{
    backtester::{display_results, run_parallel_backtest},
    event::MarketEvent,
    slippage_models::TransactionCosts,
    strategy::{Order, OrderType, StrategyParams},
};
use strategy::Strategy;
use utils::fetch::fetch_and_save_data;

// InkBack schemas
#[derive(Clone)]
pub enum InkBackSchema {
    FootPrint,
    CombinedOptionsUnderlying,
}

/// Option Momentum Strategy
pub struct OptionsMomentumStrategy {
    // Strategy parameters
    pub lookback_periods: usize, // Periods to calculate momentum
    pub momentum_threshold: f64, // % momentum required for signal
    pub profit_target: f64,      // % profit target
    pub stop_loss: f64,          // % stop loss
    pub min_days_to_expiry: f64, // Minimum days to expiration

    // State tracking
    pub underlying_history: VecDeque<f64>,
    pub volume_history: VecDeque<u64>,
    pub position_state: PositionState,

    // Current contract tracking
    pub current_contract: Option<ContractInfo>,
}

#[derive(Debug, Clone)]
pub struct ContractInfo {
    pub instrument_id: u32,
    pub symbol: String,
    pub strike_price: f64,
    pub expiration: u64,
    pub option_type: OptionType,
    pub entry_price: f64,
    pub entry_time: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OptionType {
    Call,
    Put,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PositionState {
    Flat,
    Long,
    Short,
}

impl OptionsMomentumStrategy {
    pub fn new(params: &StrategyParams) -> Result<Self> {
        let lookback_periods = params
            .get("lookback_periods")
            .ok_or_else(|| anyhow::anyhow!("Missing lookback_periods parameter"))?
            as usize;

        let momentum_threshold = params
            .get("momentum_threshold")
            .ok_or_else(|| anyhow::anyhow!("Missing momentum_threshold parameter"))?
            / 100.0;

        let profit_target = params
            .get("profit_target")
            .ok_or_else(|| anyhow::anyhow!("Missing profit_target parameter"))?
            / 100.0;

        let stop_loss = params
            .get("stop_loss")
            .ok_or_else(|| anyhow::anyhow!("Missing stop_loss parameter"))?
            / 100.0;

        let min_days_to_expiry = params
            .get("min_days_to_expiry")
            .ok_or_else(|| anyhow::anyhow!("Missing min_days_to_expiry parameter"))?;

        Ok(Self {
            lookback_periods,
            momentum_threshold,
            profit_target,
            stop_loss,
            min_days_to_expiry,
            underlying_history: VecDeque::with_capacity(lookback_periods + 1),
            volume_history: VecDeque::with_capacity(lookback_periods + 1),
            position_state: PositionState::Flat,
            current_contract: None,
        })
    }

    // Add a reset method to ensure clean state
    pub fn reset(&mut self) {
        self.underlying_history.clear();
        self.volume_history.clear();
        self.position_state = PositionState::Flat;
        self.current_contract = None;
    }

    /// Calculate momentum as percentage price change over lookback period
    fn get_momentum(&self) -> Option<f64> {
        if self.underlying_history.len() < self.lookback_periods {
            return None;
        }

        let current_price = *self.underlying_history.back()?;
        let past_price = *self
            .underlying_history
            .get(self.underlying_history.len() - self.lookback_periods)?;
        Some((current_price - past_price) / past_price)
    }

    /// Parse option information from event data
    fn parse_option_info(
        &self,
        event: &MarketEvent,
    ) -> Option<(OptionType, f64, u64, u32, String, f64)> {
        // Get option type from instrument_class
        let instrument_class_str = event.get_string("instrument_class")?;
        let option_type = match instrument_class_str.chars().next()? {
            'C' => OptionType::Call,
            'P' => OptionType::Put,
            _ => {
                return None;
            }
        };

        // Get strike price, must be positive
        let strike_price = event.get("strike_price")?;
        if let Some(underlying_price) = self.underlying_history.back() {
            if strike_price > (underlying_price * 5.0) || strike_price <= 0.0 {
                return None;
            }
        }

        // Expiration, must be positive
        let expiration = event.get_u64("expiration")?;
        if expiration <= 0 {
            return None;
        }

        let instrument_id = event.get_u64("instrument_id")? as u32;
        let symbol = event.get_string("symbol")?.clone();
        let price = event.price();

        Some((
            option_type,
            strike_price,
            expiration,
            instrument_id,
            symbol,
            price,
        ))
    }

    /// Check if this option contract meets our trading criteria
    fn should_trade_option(&self, event: &MarketEvent) -> Option<OrderType> {
        // Attempt to parse info
        let parse_result = self.parse_option_info(event);
        if parse_result.is_none() {
            // println!("No parse result");
            return None;
        }
        let (option_type, strike_price, expiration, _instrument_id, _symbol, _price) =
            parse_result?;

        let lower = strike_price as f64 * 0.5;
        let upper = strike_price as f64 * 1.5;
        let within_50pct = (strike_price as f64) >= lower && (strike_price as f64) <= upper;
        if !within_50pct {
            return None;
        }

        // Check days to expiration
        let current_time_ns = event.timestamp();

        // Validate that we have valid timestamps
        if current_time_ns == 0 || expiration == 0 {
            return None;
        }

        // Convert nanoseconds to seconds
        let current_time = current_time_ns as f64 / 1_000_000_000.0;
        let expiration_seconds = expiration as f64 / 1_000_000_000.0;

        // Validate that expiration is in the future
        if expiration_seconds <= current_time {
            return None;
        }

        let days_to_expiry = (expiration_seconds - current_time) / 86400.0;

        if days_to_expiry <= self.min_days_to_expiry {
            return None;
        }

        // Get momentum
        let momentum = match self.get_momentum() {
            Some(m) => m,
            None => {
                return None;
            }
        };

        // println!(
        //     "Sym: {} | Mom: {:.6} | Thresh: {:.6}",
        //     symbol, momentum, self.momentum_threshold
        // );
        match option_type {
            OptionType::Call => {
                if momentum > self.momentum_threshold {
                    Some(OrderType::MarketBuy)
                } else {
                    None
                }
            }
            OptionType::Put => {
                if momentum < -self.momentum_threshold {
                    Some(OrderType::MarketBuy)
                } else {
                    None
                }
            }
        }
    }

    /// Check if we should exit current position
    fn should_exit_position(&self, current_price: f64, current_time_ns: u64) -> bool {
        if let Some(ref contract) = self.current_contract {
            let pnl_pct = (current_price - contract.entry_price) / contract.entry_price;

            // Exit on profit target or stop loss
            if pnl_pct >= self.profit_target || pnl_pct <= -self.stop_loss {
                return true;
            }

            // Force exit if too close to expiration
            let current_time = current_time_ns as f64 / 1_000_000_000.0;
            let expiration_seconds = contract.expiration as f64 / 1_000_000_000.0;

            if expiration_seconds > current_time {
                let days_to_expiry = (expiration_seconds - current_time) / 86400.0;
                if days_to_expiry <= self.min_days_to_expiry {
                    println!("Force exit: {:.2} days to expiry", days_to_expiry);
                    return true;
                }
            } else {
                return true; // Expired
            }

            false
        } else {
            false
        }
    }
}

impl Strategy for OptionsMomentumStrategy {
    fn on_event(&mut self, event: &MarketEvent, _prev: Option<&MarketEvent>) -> Option<Order> {
        // First, always try to update underlying state from any event that has underlying data
        if let Some(underlying_bid) = event.get("underlying_bid") {
            if let Some(underlying_ask) = event.get("underlying_ask") {
                let underlying_price = (underlying_bid + underlying_ask) / 2.0;

                // Update history
                self.underlying_history.push_back(underlying_price);
                if self.underlying_history.len() > self.lookback_periods + 1 {
                    self.underlying_history.pop_front();
                }
            }
        }

        // Update volume history from any event
        let size = event.volume() as u64;
        self.volume_history.push_back(size);
        if self.volume_history.len() > self.lookback_periods + 1 {
            self.volume_history.pop_front();
        }

        // Only process option events for trading signals
        if !matches!(event, MarketEvent::OptionTrade(_)) {
            return None;
        }

        // Get current prices for option trading
        let underlying_bid = event.get("underlying_bid")?;
        let underlying_ask = event.get("underlying_ask")?;
        let underlying_price = (underlying_bid + underlying_ask) / 2.0;
        let option_price = event.price();

        // If we're in a position, check for exit conditions first
        if self.position_state != PositionState::Flat {
            if let Some(ref current_contract) = self.current_contract {
                // Only exit if this event is for the same contract we're holding
                if let Some((_, _, _, instrument_id, _, _)) = self.parse_option_info(event) {
                    if instrument_id == current_contract.instrument_id {
                        let current_time_ns = event.timestamp();
                        if self.should_exit_position(option_price, current_time_ns) {
                            println!(
                                "Exiting position: {} at ${:.2} (entry: ${:.2})",
                                current_contract.symbol, option_price, current_contract.entry_price
                            );

                            // Reset position state
                            self.position_state = PositionState::Flat;
                            self.current_contract = None;

                            return Some(Order {
                                order_type: OrderType::MarketSell,
                                price: option_price,
                            });
                        }
                    }
                }
            }
            return None; // Stay in position if no exit signal
        }

        // Need enough history for momentum calculation
        if self.underlying_history.len() <= self.lookback_periods {
            return None;
        }

        // Check for entry signal
        if let Some(order_type) = self.should_trade_option(event) {
            if let Some((option_type, strike_price, expiration, instrument_id, symbol, _)) =
                self.parse_option_info(event)
            {
                println!(
                    "Entry signal EXEC: {} {:?} strike ${:.2} at ${:.2}, underlying: ${:.2}",
                    symbol, option_type, strike_price, option_price, underlying_price
                );

                // Create new contract info
                let contract_info = ContractInfo {
                    instrument_id,
                    symbol: symbol.clone(),
                    strike_price,
                    expiration,
                    option_type,
                    entry_price: option_price,
                    entry_time: event.date_string(),
                };

                // Update position state
                self.position_state = match order_type {
                    OrderType::MarketBuy => PositionState::Long,
                    OrderType::MarketSell => PositionState::Short,
                    OrderType::LimitBuy => todo!(),
                    OrderType::LimitSell => todo!(),
                };
                self.current_contract = Some(contract_info);

                return Some(Order {
                    order_type,
                    price: option_price,
                });
            }
        }

        None
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables
    dotenvy::dotenv().ok();

    // Define historical data range
    let start = date!(2025 - 11 - 01).with_time(time!(00:00)).assume_utc();
    let end = date!(2025 - 12 - 20).with_time(time!(00:00)).assume_utc();

    let starting_equity = 100_000.00;
    let exposure = 0.50; // % of capital allocated to each trade

    // Fetch and save combined options data to CSV
    let schema = Schema::Trades;
    let transaction_costs = TransactionCosts::options_trading();
    let symbol = "CL.v.0";
    let symbol_manager = fetch_and_save_data(
        "GLBX.MDP3",
        SType::Continuous,
        symbol,
        Some("LO.OPT"),
        schema,
        Some(InkBackSchema::CombinedOptionsUnderlying),
        start,
        end,
    )
    .await?;

    // Define parameter ranges for the momentum strategy
    let lookback_periods = vec![10]; // Momentum calculation periods
    let momentum_thresholds = vec![0.00001, 0.001]; // Momentum Threshold
    let profit_targets = vec![40.0]; // % profit targets
    let stop_losses = vec![30.0]; // % stop losses
    let min_days_to_expiry = vec![1.0]; // dte

    // Generate all parameter combinations
    let mut parameter_combinations = Vec::new();
    for lookback in &lookback_periods {
        for threshold in &momentum_thresholds {
            for profit in &profit_targets {
                for stop in &stop_losses {
                    for min_days in &min_days_to_expiry {
                        let mut params = StrategyParams::new();
                        params.insert("lookback_periods", *lookback as f64);
                        params.insert("momentum_threshold", *threshold);
                        params.insert("profit_target", *profit);
                        params.insert("stop_loss", *stop);
                        params.insert("min_days_to_expiry", *min_days);
                        parameter_combinations.push(params);
                    }
                }
            }
        }
    }

    let sorted_results = run_parallel_backtest(
        parameter_combinations,
        symbol_manager.clone(),
        &symbol,
        schema,
        Some(InkBackSchema::CombinedOptionsUnderlying),
        |params| Ok(Box::new(OptionsMomentumStrategy::new(params)?)),
        starting_equity,
        exposure,
        transaction_costs.clone(),
    );

    display_results(
        sorted_results,
        &symbol_manager.data_path,
        &symbol,
        schema,
        Some(InkBackSchema::CombinedOptionsUnderlying),
        starting_equity,
        exposure,
    )
    .await;

    Ok(())
}
