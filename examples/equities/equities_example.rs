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

/// Moving Average Cross Strategy
pub struct MovingAverageCrossStrategy {
    // Strategy parameters
    pub short_ma_period: usize, // Short moving average period
    pub long_ma_period: usize,  // Long moving average period
    pub volume_threshold: f64,  // Minimum volume threshold for trades
    pub profit_target: f64,     // % profit target
    pub stop_loss: f64,         // % stop loss

    // Moving average tracking
    pub short_ma_history: VecDeque<f64>,
    pub long_ma_history: VecDeque<f64>,
    pub volume_history: VecDeque<u64>,

    // Current state
    pub position_state: PositionState,
    pub entry_price: f64,
    pub entry_time: String,

    // Moving averages
    pub short_ma: f64,
    pub long_ma: f64,
    pub prev_short_ma: f64,
    pub prev_long_ma: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PositionState {
    Flat,
    Long,
    Short,
}

impl MovingAverageCrossStrategy {
    pub fn new(params: &StrategyParams) -> Result<Self> {
        let short_ma_period = params
            .get("short_ma_period")
            .ok_or_else(|| anyhow::anyhow!("Missing short_ma_period parameter"))?
            as usize;

        let long_ma_period = params
            .get("long_ma_period")
            .ok_or_else(|| anyhow::anyhow!("Missing long_ma_period parameter"))?
            as usize;

        if short_ma_period >= long_ma_period {
            return Err(anyhow::anyhow!(
                "Short MA period must be less than long MA period"
            ));
        }

        let volume_threshold = params.get("volume_threshold").unwrap_or(0.0);
        let profit_target = params.get("profit_target").unwrap_or(0.0) / 100.0;
        let stop_loss = params.get("stop_loss").unwrap_or(0.0) / 100.0;

        Ok(Self {
            short_ma_period,
            long_ma_period,
            volume_threshold,
            profit_target,
            stop_loss,
            short_ma_history: VecDeque::with_capacity(short_ma_period),
            long_ma_history: VecDeque::with_capacity(long_ma_period),
            volume_history: VecDeque::with_capacity(long_ma_period),
            position_state: PositionState::Flat,
            entry_price: 0.0,
            entry_time: String::new(),
            short_ma: 0.0,
            long_ma: 0.0,
            prev_short_ma: 0.0,
            prev_long_ma: 0.0,
        })
    }

    // Reset method to ensure clean state
    pub fn reset(&mut self) {
        self.short_ma_history.clear();
        self.long_ma_history.clear();
        self.volume_history.clear();
        self.position_state = PositionState::Flat;
        self.entry_price = 0.0;
        self.entry_time.clear();
        self.short_ma = 0.0;
        self.long_ma = 0.0;
        self.prev_short_ma = 0.0;
        self.prev_long_ma = 0.0;
    }

    /// Calculate simple moving average from a VecDeque
    fn calculate_sma(history: &VecDeque<f64>) -> f64 {
        if history.is_empty() {
            return 0.0;
        }
        history.iter().sum::<f64>() / history.len() as f64
    }

    /// Update moving averages
    fn update_moving_averages(&mut self, price: f64) {
        // Store previous values
        self.prev_short_ma = self.short_ma;
        self.prev_long_ma = self.long_ma;

        // Add new price to histories
        self.short_ma_history.push_back(price);
        self.long_ma_history.push_back(price);

        // Maintain window sizes
        if self.short_ma_history.len() > self.short_ma_period {
            self.short_ma_history.pop_front();
        }
        if self.long_ma_history.len() > self.long_ma_period {
            self.long_ma_history.pop_front();
        }

        // Calculate new moving averages
        if self.short_ma_history.len() == self.short_ma_period {
            self.short_ma = Self::calculate_sma(&self.short_ma_history);
        }
        if self.long_ma_history.len() == self.long_ma_period {
            self.long_ma = Self::calculate_sma(&self.long_ma_history);
        }
    }

    /// Check for moving average crossover signals
    fn check_crossover_signal(&self) -> Option<OrderType> {
        // Need full history for both MAs and previous values
        if self.short_ma_history.len() < self.short_ma_period
            || self.long_ma_history.len() < self.long_ma_period
            || self.prev_short_ma == 0.0
            || self.prev_long_ma == 0.0
        {
            return None;
        }

        // Golden Cross: Short MA crosses above Long MA (bullish signal)
        if self.prev_short_ma <= self.prev_long_ma && self.short_ma > self.long_ma {
            return Some(OrderType::MarketBuy);
        }

        // Death Cross: Short MA crosses below Long MA (bearish signal)
        if self.prev_short_ma >= self.prev_long_ma && self.short_ma < self.long_ma {
            return Some(OrderType::MarketSell);
        }

        None
    }

    /// Check volume conditions
    fn check_volume_condition(&self, current_volume: u64) -> bool {
        if self.volume_threshold <= 0.0 {
            return true; // No volume filter
        }

        if self.volume_history.len() < 20 {
            return true; // Not enough history, allow trade
        }

        // Calculate average volume over last 20 periods
        let avg_volume = self.volume_history.iter().rev().take(20).sum::<u64>() as f64 / 20.0;

        current_volume as f64 >= avg_volume * self.volume_threshold
    }

    /// Check if we should exit current position
    fn should_exit_position(&self, current_price: f64) -> bool {
        if self.position_state == PositionState::Flat || self.entry_price == 0.0 {
            return false;
        }

        let pnl_pct = match self.position_state {
            PositionState::Long => (current_price - self.entry_price) / self.entry_price,
            PositionState::Short => (self.entry_price - current_price) / self.entry_price,
            PositionState::Flat => return false,
        };

        // Exit on profit target or stop loss (if configured)
        if self.profit_target > 0.0 && pnl_pct >= self.profit_target {
            return true;
        }
        if self.stop_loss > 0.0 && pnl_pct <= -self.stop_loss {
            return true;
        }

        false
    }

    /// Get the trading price (handles bid/ask or uses close price)
    fn get_trading_price(&self, event: &MarketEvent) -> f64 {
        event.price()
    }
}

impl Strategy for MovingAverageCrossStrategy {
    fn on_event(&mut self, event: &MarketEvent, _prev: Option<&MarketEvent>) -> Option<Order> {
        let current_price = self.get_trading_price(event);

        // Skip invalid prices
        if current_price <= 0.0 {
            return None;
        }

        // Get volume (handle different possible field names)
        let volume = event.volume() as u64;

        // Update volume history
        self.volume_history.push_back(volume);
        if self.volume_history.len() > 100 {
            // Keep last 100 periods
            self.volume_history.pop_front();
        }

        // Update moving averages
        self.update_moving_averages(current_price);

        // If we're in a position, check for exit conditions first
        if self.position_state != PositionState::Flat {
            if self.should_exit_position(current_price) {
                let exit_order = match self.position_state {
                    PositionState::Long => OrderType::MarketSell,
                    PositionState::Short => OrderType::MarketBuy,
                    PositionState::Flat => return None,
                };

                // Reset position state
                self.position_state = PositionState::Flat;
                self.entry_price = 0.0;
                self.entry_time.clear();

                return Some(Order {
                    order_type: exit_order,
                    price: current_price,
                });
            }

            // Check for opposite crossover signal to close position
            if let Some(signal) = self.check_crossover_signal() {
                let should_close = match (self.position_state, signal) {
                    (PositionState::Long, OrderType::MarketSell) => true,
                    (PositionState::Short, OrderType::MarketBuy) => true,
                    _ => false,
                };

                if should_close {
                    let exit_order = match self.position_state {
                        PositionState::Long => OrderType::MarketSell,
                        PositionState::Short => OrderType::MarketBuy,
                        PositionState::Flat => return None,
                    };

                    // Reset position state
                    self.position_state = PositionState::Flat;
                    self.entry_price = 0.0;
                    self.entry_time.clear();

                    return Some(Order {
                        order_type: exit_order,
                        price: current_price,
                    });
                }
            }

            return None; // Stay in position if no exit signal
        }

        // Check for entry signal
        if let Some(signal) = self.check_crossover_signal() {
            // Check volume condition
            if !self.check_volume_condition(volume) {
                return None;
            }

            // Update position state
            self.position_state = match signal {
                OrderType::MarketBuy => PositionState::Long,
                OrderType::MarketSell => PositionState::Short,
                OrderType::LimitBuy => todo!(),
                OrderType::LimitSell => todo!(),
            };
            self.entry_price = current_price;
            self.entry_time = event.date_string();

            return Some(Order {
                order_type: signal,
                price: current_price,
            });
        }

        None
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables
    dotenvy::dotenv().ok();

    // Define historical data range
    let start = date!(2025 - 01 - 01).with_time(time!(00:00)).assume_utc();
    let end = date!(2025 - 12 - 01).with_time(time!(00:00)).assume_utc();

    let starting_equity = 100_000.00;
    let exposure = 0.50; // % of capital allocated to each trade

    // Fetch and save footprint data to CSV
    let schema = Schema::Ohlcv1H;
    // Set the tick size for equities
    let tick_size: f64 = 0.01;
    let transaction_costs = TransactionCosts::equities_trading(tick_size);
    let symbol = "SPY";
    let symbol_manager = fetch_and_save_data(
        "XNAS.ITCH",
        SType::RawSymbol,
        symbol,
        None,
        schema,
        None,
        start,
        end,
    )
    .await?;

    // Define parameter ranges for the moving average strategy
    let short_ma_periods = vec![10, 20]; // Short MA periods
    let long_ma_periods = vec![20, 50]; // Long MA periods
    let volume_thresholds = vec![0.0, 1.2]; // Volume multipliers (0 = no filter)
    let profit_targets = vec![5.0, 10.0]; // % profit targets
    let stop_losses = vec![3.0, 5.0]; // % stop losses

    // Generate all parameter combinations
    let mut parameter_combinations = Vec::new();
    for short_ma in &short_ma_periods {
        for long_ma in &long_ma_periods {
            if short_ma < long_ma {
                // Ensure short MA is less than long MA
                for volume_thresh in &volume_thresholds {
                    for profit_target in &profit_targets {
                        for stop_loss in &stop_losses {
                            let mut params = StrategyParams::new();
                            params.insert("short_ma_period", *short_ma as f64);
                            params.insert("long_ma_period", *long_ma as f64);
                            params.insert("volume_threshold", *volume_thresh);
                            params.insert("profit_target", *profit_target);
                            params.insert("stop_loss", *stop_loss);
                            parameter_combinations.push(params);
                        }
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
        Some(InkBackSchema::FootPrint),
        |params| Ok(Box::new(MovingAverageCrossStrategy::new(params)?)),
        starting_equity,
        exposure,
        transaction_costs.clone(),
    );

    display_results(
        sorted_results,
        &symbol_manager.data_path,
        &symbol,
        schema,
        Some(InkBackSchema::FootPrint),
        starting_equity,
        exposure,
    )
    .await;

    Ok(())
}
