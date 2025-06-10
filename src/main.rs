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

use plot::plot_equity_curves;
use strategy::Strategy;
use utils::fetch::fetch_and_save_csv;
use crate::strategy::{Candle, Order, OrderType, StrategyParams};

/// A basic moving average crossover strategy
pub struct MovingAverageCross {
    short_window: usize,
    long_window: usize,
    
    tp: f64,
    sl: f64,
    closes: VecDeque<f64>,
    last_signal: Option<OrderType>,
    current_position: Option<OrderType>,
    entry_price: Option<f64>,
}

impl MovingAverageCross {
    /// Construct a new strategy from parameters; ensures short < long window
    pub fn new(params: &StrategyParams) -> Result<Self, anyhow::Error> {
        let short_window = params
            .get("short_window")
            .ok_or_else(|| anyhow::anyhow!("Missing short_window parameter"))? as usize;
        let long_window = params
            .get("long_window")
            .ok_or_else(|| anyhow::anyhow!("Missing long_window parameter"))? as usize;

        if short_window >= long_window {
            return Err(anyhow::anyhow!("short_window must be less than long_window"));
        }

        let tp = params
            .get("tp")
            .ok_or_else(|| anyhow::anyhow!("Missing tp parameter"))? as f64;
        let sl = params
            .get("sl")
            .ok_or_else(|| anyhow::anyhow!("Missing sl parameter"))? as f64;

        Ok(Self {
            short_window,
            long_window,
            tp,
            sl,
            closes: VecDeque::with_capacity(long_window),
            last_signal: None,
            current_position: None,
            entry_price: None,
        })
    }

    /// Helper function to compute the mean of a slice of `f64`
    fn mean(data: &[f64]) -> f64 {
        data.iter().sum::<f64>() / data.len() as f64
    }
}

impl Strategy for MovingAverageCross {
    fn on_candle(&mut self, candle: &Candle, _prev: Option<&Candle>) -> Option<Order> {
        let close = candle.get("close")
            .ok_or_else(|| anyhow::anyhow!("Missing ‘close’ in candle")).expect("Candle Error");

        self.closes.push_back(close);

        if self.closes.len() < self.long_window {
            return None;
        }

        if self.closes.len() > self.long_window {
            self.closes.pop_front();
        }

        // If in a position, check TP/SL
        if let (Some(position), Some(entry)) = (self.current_position, self.entry_price) {
            match position {
                OrderType::Buy => {
                    if close >= entry * (1.0 + self.tp) || close <= entry * (1.0 - self.sl) {
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

        // Normal MA crossover logic
        let closes_vec: Vec<f64> = self.closes.iter().copied().collect();
        let short_slice = &closes_vec[(self.long_window - self.short_window)..];
        let short_ma = Self::mean(short_slice);
        let long_ma = Self::mean(&closes_vec);

        let new_signal = if short_ma > long_ma {
            Some(OrderType::Buy)
        } else if short_ma < long_ma {
            Some(OrderType::Sell)
        } else {
            None
        };

        if let Some(signal) = new_signal {
            if Some(signal) != self.last_signal {
                self.last_signal = Some(signal);
                self.current_position = Some(signal);
                self.entry_price = Some(close);
                return Some(Order {
                    order_type: signal,
                    price: close,
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
    let start = date!(2023 - 01 - 01).with_time(time!(00:00)).assume_utc();
    let end = date!(2025 - 01 - 31).with_time(time!(00:00)).assume_utc();

    let starting_equity = 100_000.00;
    let exposure = 0.50; // % of capital allocated to each trade

    // Create a client for historical market data
    let client = databento::HistoricalClient::builder().key_from_env()?.build()?;

    // Fetch and save daily OHLCV candles to CSV
    let csv_path = fetch_and_save_csv(client, "GLBX.MDP3", "ES.v.0", Schema::Ohlcv1D, start, end).await?;
    //let csv_path = fetch_and_save_csv(client, "XNAS.ITCH", "AAPL", Schema::Ohlcv1D, start, end).await?;

    // Run a benchmark buy-and-hold simulation
    let benchmark = backtester::calculate_benchmark(&csv_path, starting_equity, exposure)?;
    
    // Store equity curves for plotting
    let mut equity_curves: Vec<(String, Vec<f64>)> = Vec::new();

    // Strategy parameter ranges
    let short_windows = [50, 60];
    let long_windows = [70, 150];
    let tp_windows = [0.05, 0.10];
    let sl_windows = [0.075, 0.15];

    // Generate all combinations of paramters windows
    let parameter_combinations: Vec<StrategyParams> = short_windows
            .to_vec()
            .into_iter()
            .flat_map(|short| {
                long_windows.to_vec().into_iter().flat_map(move |long| {
                    tp_windows.to_vec().into_iter().flat_map(move |tp| {
                        sl_windows.to_vec().into_iter().map(move |sl| {
                            let mut params = StrategyParams::new();
                            params.insert("short_window", short as f64);
                            params.insert("long_window", long as f64);
                            params.insert("tp", tp as f64);
                            params.insert("sl", sl as f64);
                            params
                        })
                    })
                })
            })
            .collect();

    // Run each strategy in parallel and collect backtest results
    let results: Vec<_> = parameter_combinations
        .par_iter()
        .map(|params| {
            let mut strategy = MovingAverageCross::new(params).expect("Invalid strategy parameters");

            let result = backtester::run_backtest(
                &csv_path,
                &mut strategy,
                starting_equity,
                exposure,
            ).expect("Backtest failed");

            // Format parameter combination as a label
            let param_str = format!(
                "MA({}, {}) Risk({}, {})",
                params.get("short_window").unwrap_or(0.0) as usize,
                params.get("long_window").unwrap_or(0.0) as usize,
                params.get("tp").unwrap_or(0.0) as f64,
                params.get("sl").unwrap_or(0.0) as f64,
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
