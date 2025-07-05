use std::{collections::VecDeque, usize};
use rayon::prelude::*;
use time::{macros::date, macros::time};
use databento::{
    dbn::{Schema},
};
use serde_json::Value;
use std::collections::HashMap;
use anyhow::Result;

mod schema_handler;
mod utils;
mod strategy;
mod backtester;
mod plot;

use plot::plot_equity_curves;
use strategy::Strategy;
use utils::fetch::fetch_and_save_csv;
use crate::strategy::{Candle, Order, OrderType, StrategyParams};

// Custom schemas
pub enum InkBackSchema {
    FootPrint,
}

/// A footprint-based volume imbalance strategy
pub struct FootprintVolumeImbalance {
    imbalance_threshold: f64,
    volume_threshold: u64,
    tp: f64,
    sl: f64,
    lookback_periods: usize,
    
    candle_history: VecDeque<Candle>,
    last_signal: Option<OrderType>,
    current_position: Option<OrderType>,
    entry_price: Option<f64>,
}

impl FootprintVolumeImbalance {
    /// Construct a new footprint strategy from parameters
    pub fn new(params: &StrategyParams) -> Result<Self, anyhow::Error> {
        let imbalance_threshold = params
            .get("imbalance_threshold")
            .ok_or_else(|| anyhow::anyhow!("Missing imbalance_threshold parameter"))? as f64;
        let volume_threshold = params
            .get("volume_threshold")
            .ok_or_else(|| anyhow::anyhow!("Missing volume_threshold parameter"))? as u64;
        let lookback_periods = params
            .get("lookback_periods")
            .ok_or_else(|| anyhow::anyhow!("Missing lookback_periods parameter"))? as usize;

        let tp = params
            .get("tp")
            .ok_or_else(|| anyhow::anyhow!("Missing tp parameter"))? as f64;
        let sl = params
            .get("sl")
            .ok_or_else(|| anyhow::anyhow!("Missing sl parameter"))? as f64;

        Ok(Self {
            imbalance_threshold,
            volume_threshold,
            tp,
            sl,
            lookback_periods,
            candle_history: VecDeque::with_capacity(lookback_periods),
            last_signal: None,
            current_position: None,
            entry_price: None,
        })
    }

    /// Parse footprint data from JSON string
    fn parse_footprint_data(&self, footprint_json: &str) -> Result<HashMap<String, (u64, u64)>, anyhow::Error> {
        let parsed: Value = serde_json::from_str(footprint_json)?;
        let mut footprint_map = HashMap::new();

        if let Value::Object(obj) = parsed {
            for (price_str, volumes) in obj {
                if let Value::Array(vol_array) = volumes {
                    if vol_array.len() >= 2 {
                        let buy_vol = vol_array[0].as_u64().unwrap_or(0);
                        let sell_vol = vol_array[1].as_u64().unwrap_or(0);
                        footprint_map.insert(price_str, (buy_vol, sell_vol));
                    }
                }
            }
        }

        Ok(footprint_map)
    }

    /// Calculate volume imbalance for a candle
    fn calculate_imbalance(&self, candle: &Candle) -> Result<f64, anyhow::Error> {
        let footprint_data = candle.get_string("footprint_data")
            .ok_or_else(|| anyhow::anyhow!("Missing footprint_data in candle"))?;

        let footprint_map = self.parse_footprint_data(footprint_data)?;

        let mut total_buy_volume = 0u64;
        let mut total_sell_volume = 0u64;

        for (_, (buy_vol, sell_vol)) in footprint_map {
            total_buy_volume += buy_vol;
            total_sell_volume += sell_vol;
        }

        let total_volume = total_buy_volume + total_sell_volume;
        if total_volume == 0 {
            return Ok(0.0);
        }

        // Calculate imbalance as percentage: positive = more buying, negative = more selling
        let imbalance = (total_buy_volume as f64 - total_sell_volume as f64) / total_volume as f64;
        Ok(imbalance)
    }

    /// Calculate volume-weighted average imbalance over lookback periods
    fn calculate_average_imbalance(&self) -> Result<f64, anyhow::Error> {
        if self.candle_history.is_empty() {
            return Ok(0.0);
        }

        let mut weighted_imbalance = 0.0;
        let mut total_weight = 0.0;

        for candle in &self.candle_history {
            let volume = candle.get("volume")
                .ok_or_else(|| anyhow::anyhow!("Missing volume in candle"))?;
            let imbalance = self.calculate_imbalance(candle)?;
            
            weighted_imbalance += imbalance * volume;
            total_weight += volume;
        }

        if total_weight == 0.0 {
            Ok(0.0)
        } else {
            Ok(weighted_imbalance / total_weight)
        }
    }
}

impl Strategy for FootprintVolumeImbalance {
    fn on_candle(&mut self, candle: &Candle, _prev: Option<&Candle>) -> Option<Order> {
        let close = candle.get("close")
            .ok_or_else(|| anyhow::anyhow!("Missing 'close' in candle")).expect("Candle Error");

        let volume = candle.get("volume")
            .ok_or_else(|| anyhow::anyhow!("Missing 'volume' in candle")).expect("Candle Error") as u64;

        // Add candle to history
        self.candle_history.push_back(candle.clone());
        if self.candle_history.len() > self.lookback_periods {
            self.candle_history.pop_front();
        }

        // Need enough history to make decisions
        if self.candle_history.len() < self.lookback_periods {
        //    println!("Not enough history: {} < {}", self.candle_history.len(), self.lookback_periods);
            return None;
        }

        // If in a position, check TP/SL
        if let (Some(position), Some(entry)) = (self.current_position, self.entry_price) {
            match position {
                OrderType::Buy => {
                    if close >= entry * (1.0 + self.tp) || close <= entry * (1.0 - self.sl) {
                        //println!("Exiting BUY position: close={:.2}, entry={:.2}, tp_level={:.2}, sl_level={:.2}", 
                        //        close, entry, entry * (1.0 + self.tp), entry * (1.0 - self.sl));
                        self.current_position = None;
                        self.entry_price = None;
                        return Some(Order {
                            order_type: OrderType::Sell,
                            price: close,
                        });
                    }
                }
                OrderType::Sell => {
                    if close <= entry * (1.0 - self.tp) || close >= entry * (1.0 + self.sl) {
                        //println!("Exiting SELL position: close={:.2}, entry={:.2}, tp_level={:.2}, sl_level={:.2}", 
                        //        close, entry, entry * (1.0 - self.tp), entry * (1.0 + self.sl));
                        self.current_position = None;
                        self.entry_price = None;
                        return Some(Order {
                            order_type: OrderType::Buy,
                            price: close,
                        });
                    }
                }
            }
        }

        // Skip if volume is too low
        if volume < self.volume_threshold {
            //println!("Volume too low: {} < {}", volume, self.volume_threshold);
            return None;
        }

        // Calculate current imbalance
        let current_imbalance = match self.calculate_imbalance(candle) {
            Ok(imbalance) => {
            //    println!("Current imbalance: {:.4}", imbalance);
                imbalance
            },
            Err(e) => {
                println!("Error calculating imbalance: {}", e);
                return None;
            },
        };

        // Calculate average imbalance over lookback period
        let avg_imbalance = match self.calculate_average_imbalance() {
            Ok(imbalance) => {
            //    println!("Average imbalance: {:.4}", imbalance);
                imbalance
            },
            Err(e) => {
                println!("Error calculating average imbalance: {}", e);
                return None;
            },
        };

        // Print footprint data for debugging
        //if let Some(footprint_data) = candle.get_string("footprint_data") {
        //    println!("Footprint data sample: {}", footprint_data.chars().take(100).collect::<String>());
        //}

        // Generate signals based on imbalance
        let new_signal = if current_imbalance > self.imbalance_threshold && avg_imbalance > 0.0 {
            //println!("BUY signal: current_imbalance={:.4} > threshold={:.4} && avg_imbalance={:.4} > 0", 
            //        current_imbalance, self.imbalance_threshold, avg_imbalance);
            Some(OrderType::Buy)
        } else if current_imbalance < -self.imbalance_threshold && avg_imbalance < 0.0 {
            //println!("SELL signal: current_imbalance={:.4} < -{:.4} && avg_imbalance={:.4} < 0", 
            //        current_imbalance, self.imbalance_threshold, avg_imbalance);
            Some(OrderType::Sell)
        } else {
            //println!("No signal: current_imbalance={:.4}, threshold={:.4}, avg_imbalance={:.4}", 
            //        current_imbalance, self.imbalance_threshold, avg_imbalance);
            None
        };

        if let Some(signal) = new_signal {
            if Some(signal) != self.last_signal {
                //println!("Generating {:?} order at price {:.2}", signal, close);
                self.last_signal = Some(signal);
                self.current_position = Some(signal);
                self.entry_price = Some(close);
                return Some(Order {
                    order_type: signal,
                    price: close,
                });
            } else {
                //println!("Signal {:?} matches last signal, skipping", signal);
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
    let start = date!(2025 - 01 - 01).with_time(time!(00:00)).assume_utc();
    let end = date!(2025 - 06 - 01).with_time(time!(00:00)).assume_utc();

    let starting_equity = 100_000.00;
    let exposure = 0.50; // % of capital allocated to each trade

    // Create a client for historical market data
    let client = databento::HistoricalClient::builder().key_from_env()?.build()?;

    // Fetch and save footprint data to CSV
    let schema = Schema::Trades;
    let csv_path = fetch_and_save_csv(client, "GLBX.MDP3", "ES.v.0", schema, Some(InkBackSchema::FootPrint), start, end).await?;

    // Run a benchmark buy-and-hold simulation
    let benchmark = backtester::calculate_benchmark(&csv_path, schema, Some(InkBackSchema::FootPrint), starting_equity, exposure)?;
    
    // Store equity curves for plotting
    let mut equity_curves: Vec<(String, Vec<f64>)> = Vec::new();

    // Footprint strategy parameter ranges
    let imbalance_thresholds = vec![0.2, 0.3]; // 10%, 20%, 30% imbalance
    let volume_thresholds = vec![100, 200, 500]; // Minimum volume
    let lookback_periods = vec![3, 5]; // Lookback periods for average imbalance
    let tp_windows = vec![0.005, 0.01]; // 1%, 2% take profit
    let sl_windows = vec![0.005, 0.01]; // 0.5%, 1% stop loss

    // Generate all combinations of parameters using nested loops
    let mut parameter_combinations = Vec::new();
    for imbalance_threshold in &imbalance_thresholds {
        for volume_threshold in &volume_thresholds {
            for lookback in &lookback_periods {
                for tp in &tp_windows {
                    for sl in &sl_windows {
                        let mut params = StrategyParams::new();
                        params.insert("imbalance_threshold", *imbalance_threshold);
                        params.insert("volume_threshold", *volume_threshold as f64);
                        params.insert("lookback_periods", *lookback as f64);
                        params.insert("tp", *tp);
                        params.insert("sl", *sl);
                        parameter_combinations.push(params);
                    }
                }
            }
        }
    }

    // Run each strategy in parallel and collect backtest results
    let results: Vec<_> = parameter_combinations
        .par_iter()
        .map(|params| {
            let mut strategy = FootprintVolumeImbalance::new(params).expect("Invalid strategy parameters");

            let result = backtester::run_backtest(
                &csv_path,
                schema,
                Some(InkBackSchema::FootPrint),
                &mut strategy,
                starting_equity,
                exposure,
            ).expect("Backtest failed");

            // Format parameter combination as a label
            let param_str = format!(
                "Imbalance({:.1}) Vol({}) Lookback({}) TP({:.1}%) SL({:.1}%)",
                params.get("imbalance_threshold").unwrap_or(0.0) * 100.0,
                params.get("volume_threshold").unwrap_or(0.0) as u64,
                params.get("lookback_periods").unwrap_or(0.0) as usize,
                params.get("tp").unwrap_or(0.0) * 100.0,
                params.get("sl").unwrap_or(0.0) * 100.0,
            );

            (param_str, result)
        })
        .collect();

    // Print metrics and store curves for GUI plotting
    for (param_str, result) in results {
        println!(
            "{}: final_equity: {:.2}, total_return: {:.2}%, max_drawdown: {:.2}%, win_rate: {:.2}%, profit_factor: {:.2}",
            param_str,
            result.ending_equity,
            result.total_return_pct,
            result.max_drawdown_pct,
            result.win_rate,
            result.profit_factor
        );

        equity_curves.push((param_str, result.equity_curve));
    }

    // Show performance plots for all strategies vs. benchmark
    println!("Launching chart...");
    plot_equity_curves(equity_curves, Some(benchmark.equity_curve));

    Ok(())
}
