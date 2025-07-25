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

/// Option Momentum Strategy
pub struct OptionsMomentumStrategy {
    // Strategy parameters
    pub lookback_periods: usize,     // Periods to calculate momentum
    pub momentum_threshold: f64,     // % momentum required for signal
    pub profit_target: f64,          // % profit target
    pub stop_loss: f64,              // % stop loss
    pub min_days_to_expiry: f64,     // Minimum days to expiration
    
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
            .ok_or_else(|| anyhow::anyhow!("Missing lookback_periods parameter"))? as usize;
        
        let momentum_threshold = params
            .get("momentum_threshold")
            .ok_or_else(|| anyhow::anyhow!("Missing momentum_threshold parameter"))? / 100.0;
        
        let profit_target = params
            .get("profit_target")
            .ok_or_else(|| anyhow::anyhow!("Missing profit_target parameter"))? / 100.0;
        
        let stop_loss = params
            .get("stop_loss")
            .ok_or_else(|| anyhow::anyhow!("Missing stop_loss parameter"))? / 100.0;
        
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
        let past_price = *self.underlying_history.get(self.underlying_history.len() - self.lookback_periods)?;
        Some((current_price - past_price) / past_price)
    }

    /// Parse option information from candle data
    fn parse_option_info(&self, candle: &Candle) -> Option<(OptionType, f64, u64, u32, String)> {
        // Get option type from instrument_class
        let instrument_class_str = candle.get_string("instrument_class")?;
        let option_type = match instrument_class_str.chars().next()? {
            'C' => OptionType::Call,
            'P' => OptionType::Put,
            _ => {
                println!("Warning: Unknown instrument class: {}", instrument_class_str);
                return None;
            }
        };
        
        // Get strike price - must be positive
        let strike_price = candle.get("strike_price")?;
        if strike_price <= 0.0 {
            println!("Warning: Invalid strike price: {}", strike_price);
            return None;
        }

        // Expiration - must be positive
        let expiration_f64 = candle.get("expiration")?;
        if expiration_f64 <= 0.0 || !expiration_f64.is_finite() {
            println!("Warning: Invalid expiration: {}", expiration_f64);
            return None;
        }
        let expiration = expiration_f64 as u64;
        
        // Get instrument ID for contract tracking
        let instrument_id_f64 = candle.get("instrument_id")?;
        if instrument_id_f64 <= 0.0 || !instrument_id_f64.is_finite() {
            println!("Warning: Invalid instrument ID: {}", instrument_id_f64);
            return None;
        }
        let instrument_id = instrument_id_f64 as u32;
        
        // Get symbol for logging - use raw_symbol or symbol_def
        let symbol = candle.get_string("raw_symbol")
            .or_else(|| candle.get_string("symbol_def"))
            .or_else(|| candle.get_string("symbol"))
            .unwrap_or(&"UNKNOWN".to_string())
            .clone();
        
        Some((option_type, strike_price, expiration, instrument_id, symbol))
    }

    /// Check if this option contract meets our trading criteria
    fn should_trade_option(&self, candle: &Candle, underlying_price: f64) -> Option<OrderType> {
        let option_price = candle.get("price")?;
        
        // Filter out options with extremely small premiums (< $0.05)
        if option_price < 0.05 {
            return None;
        }
        
        let (option_type, strike_price, expiration, _instrument_id, _symbol) = self.parse_option_info(candle)?;
        //println!("expiration: {}\n", expiration);
        
        // Check days to expiration (assuming expiration is in UNIX timestamp format)
        let current_time_ns = candle.date.parse::<u64>().unwrap_or_else(|_| {
            println!("Warning: Failed to parse candle date: {}", candle.date);
            0
        });
        
        // Validate that we have valid timestamps
        if current_time_ns == 0 || expiration == 0 {
            return None;
        }
        
        // Convert nanoseconds to seconds
        let current_time = current_time_ns / 1_000_000_000;
        let expiration_seconds = expiration / 1_000_000_000;
        
        // Validate that expiration is in the future
        if expiration_seconds <= current_time {
            return None;
        }
        
        let days_to_expiry = (expiration_seconds - current_time) / 86400; // Convert seconds to days
        if days_to_expiry <= self.min_days_to_expiry as u64 {
            return None;
        }
        
        // Get momentum
        let momentum = self.get_momentum()?;
        
        match option_type {
            OptionType::Call => {
                // Calculate moneyness for calls (underlying/strike)
                let moneyness = underlying_price / strike_price;
                
                // Filter out options more than 20% out of the money for better liquidity
                if moneyness < 0.8 {
                    return None;
                }
                
                // Trade calls on positive momentum if the option is reasonable moneyness
                if momentum > self.momentum_threshold {
                    // Focus on near-the-money options for better delta exposure
                    if moneyness >= 0.90 && moneyness <= 1.10 { // 10% ITM to 10% OTM
                        Some(OrderType::MarketBuy)
                    } else {
                        None
                    }
                } else {
                    None
                }
            },
            OptionType::Put => {
                // Calculate moneyness for puts (strike/underlying)
                let moneyness = strike_price / underlying_price;
                
                // Filter out options more than 20% out of the money
                if moneyness < 0.8 {
                    return None;
                }
                
                // Trade puts on negative momentum if the option is reasonable moneyness
                if momentum < -self.momentum_threshold {
                    // Focus on near-the-money options for better delta exposure
                    if moneyness >= 0.90 && moneyness <= 1.10 { // 10% ITM to 10% OTM
                        Some(OrderType::MarketBuy)
                    } else {
                        None
                    }
                } else {
                    None
                }
            },
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
            
            // Force exit if too close to expiration (3 days or less)
            let current_time = current_time_ns / 1_000_000_000;
            let expiration_seconds = contract.expiration / 1_000_000_000;
            
            if expiration_seconds > current_time {
                let days_to_expiry = (expiration_seconds - current_time) / 86400;
                if days_to_expiry <= self.min_days_to_expiry as u64 {
                    println!("Force exit: {} days to expiry", days_to_expiry);
                    return true;
                }
            }
            
            false
        } else {
            false
        }
    }
}

impl Strategy for OptionsMomentumStrategy {
    fn on_candle(&mut self, candle: &Candle, _prev: Option<&Candle>) -> Option<Order> {
        // Get underlying price and option price
        let underlying_bid = candle.get("underlying_bid")?;
        let underlying_ask = candle.get("underlying_ask")?;
        let underlying_price = (underlying_bid + underlying_ask) / 2.0;

        let option_price = candle.get("price")?;
        let size = candle.get("size")? as u64;
        
        // Update price and volume history
        self.underlying_history.push_back(underlying_price);
        self.volume_history.push_back(size);
        
        if self.underlying_history.len() > self.lookback_periods + 1 {
            self.underlying_history.pop_front();
        }
        if self.volume_history.len() > self.lookback_periods + 1 {
            self.volume_history.pop_front();
        }

        // If we're in a position, check for exit conditions first
        if self.position_state != PositionState::Flat {
            if let Some(ref current_contract) = self.current_contract {
                // Only exit if this candle is for the same contract we're holding
                if let Some((_, _, _, instrument_id, _)) = self.parse_option_info(candle) {
                    if instrument_id == current_contract.instrument_id {
                        let current_time_ns = candle.date.parse::<u64>().unwrap_or(0);
                        if self.should_exit_position(option_price, current_time_ns) {
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
        if let Some(order_type) = self.should_trade_option(candle, underlying_price) {
            if let Some((option_type, strike_price, expiration, instrument_id, symbol)) = self.parse_option_info(candle) {
                
                // Create new contract info
                let contract_info = ContractInfo {
                    instrument_id,
                    symbol: symbol.clone(),
                    strike_price,
                    expiration,
                    option_type,
                    entry_price: option_price,
                    entry_time: candle.date.clone(),
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
    let start = date!(2025 - 06 - 01).with_time(time!(00:00)).assume_utc();
    let end = date!(2025 - 06 - 30).with_time(time!(00:00)).assume_utc();

    let starting_equity = 100_000.00;
    let exposure = 0.10; // % of the account to put on each trade.

    // Create a client for historical market data
    let client = databento::HistoricalClient::builder().key_from_env()?.build()?;

    // Fetch combined options and underlying data for crude oil
    println!("Fetching crude oil options and underlying data...");
    let schema = Schema::Trades;
    let csv_path = fetch_and_save_csv(
        client, 
        "GLBX.MDP3",        // Crude oil futures dataset
        "CL.c.0",           // Crude oil continuous contract
        Some("LO.OPT"),     // Light crude oil options
        schema, 
        Some(InkBackSchema::CombinedOptionsUnderlying), 
        start, 
        end
    ).await?;

    println!("Data saved to: {}", csv_path);

    // Run benchmark on underlying asset
    let benchmark = backtester::calculate_benchmark(
        &csv_path, 
        schema, 
        Some(InkBackSchema::CombinedOptionsUnderlying), 
        starting_equity, 
        exposure
    )?;
    
    println!("Benchmark Return: {:.2}%, Max Drawdown: {:.2}%", 
        benchmark.total_return_pct, benchmark.max_drawdown_pct);

    // Store equity curves for plotting
    let mut equity_curves: Vec<(String, Vec<f64>)> = Vec::new();

    // Set transaction costs for options trading
    let transaction_costs = TransactionCosts::options_trading();

    // Define parameter ranges for the momentum strategy
    let lookback_periods = vec![3, 5];                // Momentum calculation periods
    let momentum_thresholds = vec![0.4];              // % momentum threshold
    let profit_targets = vec![0.20, 0.40];            // % profit targets
    let stop_losses = vec![0.20, 30.0];               // % stop losses
    let min_days_to_expiry = vec![2.0];               // Minimum days to expiration

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

    println!("Testing {} parameter combinations...", parameter_combinations.len());

    // Run backtests in parallel
    let results: Vec<_> = parameter_combinations
        .par_iter()
        .enumerate()  // Add enumeration to track which strategy
        .filter_map(|(index, params)| {
            // Create a fresh strategy instance for each parameter set
            let mut strategy = OptionsMomentumStrategy::new(params).ok()?;

            println!("Testing strategy {} with params: {:?}", index + 1, params);

            let result = backtester::run_backtest(
                &csv_path,
                schema,
                Some(InkBackSchema::CombinedOptionsUnderlying),
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
                "Strategy_{}_Lookback({})_Momentum({:.1}%)_TP({:.0}%)_SL({:.0}%)_MinDays({:.0})",
                index + 1,  // Add strategy index to make each unique
                params.get("lookback_periods").unwrap_or(0.0) as usize,
                params.get("momentum_threshold").unwrap_or(0.0),
                params.get("profit_target").unwrap_or(0.0),
                params.get("stop_loss").unwrap_or(0.0),
                params.get("min_days_to_expiry").unwrap_or(0.0),
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
    println!("Benchmark: Return {:.2}%, Max DD: {:.2}%\n", 
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
        let max_curves = 20; // Adjust this value based on visualization needs
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
