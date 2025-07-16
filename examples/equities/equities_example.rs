use std::{collections::VecDeque, usize};
use rayon::prelude::*;
use time::{macros::date, macros::time};
use databento::{
    dbn::{Schema},
};
use anyhow::Result;

mod schema_handler;
mod utils;
mod strategy;
mod backtester;
mod plot;
pub mod slippage_models;

use plot::plot_equity_curves;
use strategy::Strategy;
use utils::fetch::fetch_and_save_csv;
use crate::{slippage_models::TransactionCosts, strategy::{Candle, Order, OrderType, StrategyParams}};

// InkBack schemas
pub enum InkBackSchema {
    FootPrint,
    CombinedOptionsUnderlying,
}

/// Moving Average Cross Strategy
pub struct MovingAverageCrossStrategy {
    // Strategy parameters
    pub short_ma_period: usize,      // Short moving average period
    pub long_ma_period: usize,       // Long moving average period
    pub volume_threshold: f64,       // Minimum volume threshold for trades
    pub profit_target: f64,          // % profit target
    pub stop_loss: f64,              // % stop loss
    
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
            .ok_or_else(|| anyhow::anyhow!("Missing short_ma_period parameter"))? as usize;
        
        let long_ma_period = params
            .get("long_ma_period")
            .ok_or_else(|| anyhow::anyhow!("Missing long_ma_period parameter"))? as usize;
        
        if short_ma_period >= long_ma_period {
            return Err(anyhow::anyhow!("Short MA period must be less than long MA period"));
        }
        
        let volume_threshold = params
            .get("volume_threshold")
            .unwrap_or(0.0);
        
        let profit_target = params
            .get("profit_target")
            .unwrap_or(0.0) / 100.0;
        
        let stop_loss = params
            .get("stop_loss")
            .unwrap_or(0.0) / 100.0;
        
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
        if self.short_ma_history.len() < self.short_ma_period ||
           self.long_ma_history.len() < self.long_ma_period ||
           self.prev_short_ma == 0.0 || self.prev_long_ma == 0.0 {
            return None;
        }

        // Golden Cross: Short MA crosses above Long MA (bullish signal)
        if self.prev_short_ma <= self.prev_long_ma && self.short_ma > self.long_ma {
            return Some(OrderType::Buy);
        }
        
        // Death Cross: Short MA crosses below Long MA (bearish signal)
        if self.prev_short_ma >= self.prev_long_ma && self.short_ma < self.long_ma {
            return Some(OrderType::Sell);
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
        let avg_volume = self.volume_history.iter()
            .rev()
            .take(20)
            .sum::<u64>() as f64 / 20.0;
        
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
    fn get_trading_price(&self, candle: &Candle) -> f64 {
        // Try to get bid/ask for more realistic pricing
        if let (Some(bid), Some(ask)) = (candle.get("bid"), candle.get("ask")) {
            if bid > 0.0 && ask > 0.0 && ask > bid {
                return (bid + ask) / 2.0; // Use mid price
            }
        }
        
        // Fall back to close price
        candle.get("close")
            .or_else(|| candle.get("price"))
            .unwrap_or(0.0)
    }
}

impl Strategy for MovingAverageCrossStrategy {
    fn on_candle(&mut self, candle: &Candle, _prev: Option<&Candle>) -> Option<Order> {
        let current_price = self.get_trading_price(candle);
        
        // Skip invalid prices
        if current_price <= 0.0 {
            return None;
        }

        // Get volume (handle different possible field names)
        let volume = candle.get("volume")
            .or_else(|| candle.get("size"))
            .unwrap_or(0.0) as u64;

        // Update volume history
        self.volume_history.push_back(volume);
        if self.volume_history.len() > 100 { // Keep last 100 periods
            self.volume_history.pop_front();
        }

        // Update moving averages
        self.update_moving_averages(current_price);

        // If we're in a position, check for exit conditions first
        if self.position_state != PositionState::Flat {
            if self.should_exit_position(current_price) {
                let exit_order = match self.position_state {
                    PositionState::Long => OrderType::Sell,
                    PositionState::Short => OrderType::Buy,
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
                    (PositionState::Long, OrderType::Sell) => true,
                    (PositionState::Short, OrderType::Buy) => true,
                    _ => false,
                };
                
                if should_close {
                    let exit_order = match self.position_state {
                        PositionState::Long => OrderType::Sell,
                        PositionState::Short => OrderType::Buy,
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
                OrderType::Buy => PositionState::Long,
                OrderType::Sell => PositionState::Short,
            };
            self.entry_price = current_price;
            self.entry_time = candle.date.clone();
            
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
    let start = date!(2024 - 06 - 01).with_time(time!(00:00)).assume_utc();
    let end = date!(2024 - 06 - 30).with_time(time!(00:00)).assume_utc();

    let starting_equity = 100_000.00;
    let exposure = 0.95; // Use 95% of account for stock trading

    // Create a client for historical market data
    let client = databento::HistoricalClient::builder().key_from_env()?.build()?;

    // Fetch SPY stock data
    println!("Fetching SPY stock data...");
    let schema = Schema::Ohlcv1H; // or Schema::Ohlcv1M for minute bars
    let csv_path = fetch_and_save_csv(
        client, 
        "XNAS.ITCH",        // NASDAQ dataset
        "SPY",              // SPY ETF
        None,               // No options data needed
        schema, 
        None,               // No custom schema needed
        start, 
        end
    ).await?;

    println!("Data saved to: {}", csv_path);

    // Run benchmark (buy and hold SPY)
    let benchmark = backtester::calculate_benchmark(
        &csv_path, 
        schema, 
        None,
        starting_equity, 
        exposure
    )?;
    
    println!("Benchmark (Buy & Hold) Return: {:.2}%, Max Drawdown: {:.2}%", 
        benchmark.total_return_pct, benchmark.max_drawdown_pct);

    // Store equity curves for plotting
    let mut equity_curves: Vec<(String, Vec<f64>)> = Vec::new();

    // Set transaction costs for stock trading
    let transaction_costs = TransactionCosts::equity_trading();

    // Define parameter ranges for the moving average strategy
    let short_ma_periods = vec![10, 20];       // Short MA periods
    let long_ma_periods = vec![20, 50];        // Long MA periods
    let volume_thresholds = vec![0.0, 1.2];    // Volume multipliers (0 = no filter)
    let profit_targets = vec![5.0, 10.0];      // % profit targets 
    let stop_losses = vec![3.0, 5.0];          // % stop losses

    // Generate all parameter combinations
    let mut parameter_combinations = Vec::new();
    for short_ma in &short_ma_periods {
        for long_ma in &long_ma_periods {
            if short_ma < long_ma { // Ensure short MA is less than long MA
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

    println!("Testing {} parameter combinations...", parameter_combinations.len());

    // Run backtests in parallel
    let results: Vec<_> = parameter_combinations
        .par_iter()
        .enumerate()
        .filter_map(|(index, params)| {
            // Create a fresh strategy instance for each parameter set
            let mut strategy = MovingAverageCrossStrategy::new(params).ok()?;

            println!("Testing strategy {} with params: {:?}", index + 1, params);

            let result = backtester::run_backtest(
                &csv_path,
                schema,
                None,
                &mut strategy,
                starting_equity,
                exposure,
                transaction_costs.clone(),
            ).ok()?;

            // Validate equity curve has reasonable values
            if result.equity_curve.iter().any(|&val| !val.is_finite()) {
                println!("Warning: Strategy {} has non-finite equity values", index + 1);
                return None;
            }

            // Create parameter label
            let param_str = format!(
                "MA_Cross_{}_{}_Vol({:.1})_TP({:.0}%)_SL({:.0}%)",
                params.get("short_ma_period").unwrap_or(0.0) as usize,
                params.get("long_ma_period").unwrap_or(0.0) as usize,
                params.get("volume_threshold").unwrap_or(0.0),
                params.get("profit_target").unwrap_or(0.0),
                params.get("stop_loss").unwrap_or(0.0),
            );

            // Store equity curve with validation
            let finite_curve: Vec<f64> = result.equity_curve.iter()
                .map(|&val| if val.is_finite() { val } else { starting_equity })
                .collect();
            
            Some((param_str, result, finite_curve))
        })
        .collect();

    // Sort results by total return
    let mut sorted_results = results;
    sorted_results.sort_by(|a, b| b.1.total_return_pct.partial_cmp(&a.1.total_return_pct).unwrap_or(std::cmp::Ordering::Equal));

    // Print results for all strategies
    println!("\n=== ALL STRATEGY RESULTS ===");
    println!("Benchmark (Buy & Hold): Return {:.2}%, Max DD: {:.2}%\n", 
        benchmark.total_return_pct, benchmark.max_drawdown_pct);

    for (i, (param_str, result, _)) in sorted_results.iter().enumerate() {
        println!(
            "{}. {}: Return: {:.2}%, Max DD: {:.2}%, Win Rate: {:.1}%, PF: {:.2}, Trades: {}, Fees: ${:.0}",
            i + 1,
            param_str,
            if result.total_return_pct.is_finite() { result.total_return_pct } else { 0.0 },
            if result.max_drawdown_pct.is_finite() { result.max_drawdown_pct } else { 0.0 },
            if result.win_rate.is_finite() { result.win_rate } else { 0.0 },
            if result.profit_factor.is_finite() { result.profit_factor } else { 0.0 },
            result.total_trades,
            if result.total_transaction_costs.is_finite() { result.total_transaction_costs } else { 0.0 }
        );

        // Store equity curve for plotting
        equity_curves.push((param_str.clone(), sorted_results[i].2.clone()));
    }

    // Print summary statistics
    if !sorted_results.is_empty() {
        let profitable_strategies = sorted_results.iter()
            .filter(|(_, result, _)| result.total_return_pct > 0.0)
            .count();
        
        let avg_return: f64 = sorted_results.iter()
            .map(|(_, result, _)| result.total_return_pct)
            .sum::<f64>() / sorted_results.len() as f64;
        
        let best_return = sorted_results.first().map(|(_, result, _)| result.total_return_pct).unwrap_or(0.0);
        let worst_return = sorted_results.last().map(|(_, result, _)| result.total_return_pct).unwrap_or(0.0);

        println!("\n=== SUMMARY STATISTICS ===");
        println!("Total strategies tested: {}", sorted_results.len());
        println!("Profitable strategies: {} ({:.1}%)", 
            profitable_strategies, 
            (profitable_strategies as f64 / sorted_results.len() as f64) * 100.0);
        println!("Average return: {:.2}%", avg_return);
        println!("Best return: {:.2}%", best_return);
        println!("Worst return: {:.2}%", worst_return);
        println!("Benchmark return: {:.2}%", benchmark.total_return_pct);
        
        let outperforming = sorted_results.iter()
            .filter(|(_, result, _)| result.total_return_pct > benchmark.total_return_pct)
            .count();
        println!("Strategies beating benchmark: {} ({:.1}%)", 
            outperforming,
            (outperforming as f64 / sorted_results.len() as f64) * 100.0);
    }

    // Plot equity curves
    if !equity_curves.is_empty() {
        println!("\nLaunching performance chart for all strategies...");
        let finite_benchmark: Vec<f64> = benchmark.equity_curve.iter()
            .map(|&val| if val.is_finite() { val } else { starting_equity })
            .collect();
        
        // Limit the number of curves plotted to avoid clutter
        let max_curves = 20;
        let curves_to_plot = if equity_curves.len() > max_curves {
            println!("Too many equity curves ({}), plotting only the top {} strategies.", 
                equity_curves.len(), max_curves);
            equity_curves.into_iter().take(max_curves).collect()
        } else {
            equity_curves
        };

        plot_equity_curves(curves_to_plot, Some(finite_benchmark));
    }

    Ok(())
}
