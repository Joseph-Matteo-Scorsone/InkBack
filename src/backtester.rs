use crate::event::MarketEvent;
use crate::slippage_models::TransactionCosts;
use crate::utils::fetch::{self, BacktestManager};
use crate::{
    plot::plot_equity_curves,
    strategy::{Order, OrderType, Strategy, StrategyParams},
    InkBackSchema,
};
use anyhow::Result;
use databento::dbn::Schema;
use futures::StreamExt;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq)]
enum Position {
    Long {
        entry: f64,
        size: f64,
        entry_date: String,
    },
    Short {
        entry: f64,
        size: f64,
        entry_date: String,
    },
    Neutral,
}

enum FutureTraded {
    NQ,
    ES,
    YM,
    CL,
    GC,
    SI,
}

impl Position {
    fn calculate_pnl_with_costs(
        &self,
        exit_price: f64,
        costs: &TransactionCosts,
        vol: f64,
        is_options: bool,
        futures_multiplier: Option<f64>,
    ) -> f64 {
        match self {
            Position::Long { entry, size, .. } => {
                let entry_cost = costs.calculate_entry_cost(*entry, *size, vol);
                let exit_cost = costs.calculate_exit_cost(exit_price, *size, vol);

                // Apply appropriate multiplier based on instrument type
                let multiplier = if is_options {
                    100.0
                } else if let Some(futures_mult) = futures_multiplier {
                    futures_mult
                } else {
                    1.0
                };
                let gross_pnl = (exit_price - entry) * size * multiplier;

                // Validate costs are finite
                if !entry_cost.is_finite() || !exit_cost.is_finite() || !gross_pnl.is_finite() {
                    println!("Warning: Non-finite values in PnL calculation");
                    return 0.0; // Return 0 PnL if costs are infinite
                }

                gross_pnl - entry_cost - exit_cost
            }
            Position::Short { entry, size, .. } => {
                let entry_cost = costs.calculate_entry_cost(*entry, *size, vol);
                let exit_cost = costs.calculate_exit_cost(exit_price, *size, vol);

                let multiplier = if is_options {
                    100.0
                } else if let Some(futures_mult) = futures_multiplier {
                    futures_mult
                } else {
                    1.0
                };
                let gross_pnl = (entry - exit_price) * size * multiplier;

                if !entry_cost.is_finite() || !exit_cost.is_finite() || !gross_pnl.is_finite() {
                    println!("Warning: Non-finite values in PnL calculation");
                    return 0.0;
                }

                gross_pnl - entry_cost - exit_cost
            }
            Position::Neutral => 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub entry_date: String,
    pub exit_date: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub size: f64,
    pub pnl: f64,
    pub pnl_pct: f64,
    pub trade_type: String,
    pub exit_reason: String,
    pub transaction_costs: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BacktestResult {
    pub starting_equity: f64,
    pub ending_equity: f64,
    pub total_return: f64,
    pub total_return_pct: f64,
    pub max_drawdown: f64,
    pub max_drawdown_pct: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub largest_win: f64,
    pub largest_loss: f64,
    pub equity_curve: Vec<f64>,
    pub trades: Vec<Trade>,
    pub total_transaction_costs: f64,
}

impl BacktestResult {
    fn calculate_metrics(
        starting_equity: f64,
        ending_equity: f64,
        equity_curve: Vec<f64>,
        trades: Vec<Trade>,
    ) -> Self {
        let total_return = ending_equity - starting_equity;
        let total_return_pct = if starting_equity == 0.0 {
            0.0
        } else {
            (ending_equity / starting_equity - 1.0) * 100.0
        };

        // Calculate max drawdown
        let mut peak = starting_equity;
        let mut max_dd = 0.0;
        let mut max_dd_pct = 0.0;

        for point in &equity_curve {
            if point > &peak {
                peak = *point;
            }
            let dd = peak - point;
            let dd_pct = (dd / peak) * 100.0;

            if dd > max_dd {
                max_dd = dd;
            }
            if dd_pct > max_dd_pct {
                max_dd_pct = dd_pct;
            }
        }

        // Trade statistics
        let total_trades = trades.len();
        let winning_trades = trades.iter().filter(|t| t.pnl > 0.0).count();
        let losing_trades = trades.iter().filter(|t| t.pnl < 0.0).count();
        let win_rate = if total_trades == 0 {
            0.0
        } else {
            (winning_trades as f64 / total_trades as f64) * 100.0
        };

        let gross_profit: f64 = trades.iter().filter(|t| t.pnl > 0.0).map(|t| t.pnl).sum();
        let gross_loss: f64 = trades
            .iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| t.pnl.abs())
            .sum();
        let profit_factor = if gross_loss == 0.0 {
            if gross_profit > 0.0 {
                1000.0
            } else {
                0.0
            }
        } else {
            gross_profit / gross_loss
        };

        let avg_win = if winning_trades == 0 {
            0.0
        } else {
            gross_profit / winning_trades as f64
        };
        let avg_loss = if losing_trades == 0 {
            0.0
        } else {
            gross_loss / losing_trades as f64
        };

        let largest_win = trades.iter().map(|t| t.pnl).fold(0.0, f64::max);
        let largest_loss = trades.iter().map(|t| t.pnl).fold(0.0, f64::min);

        let total_transaction_costs: f64 = trades.iter().map(|t| t.transaction_costs).sum();

        Self {
            starting_equity,
            ending_equity,
            total_return,
            total_return_pct,
            max_drawdown: max_dd,
            max_drawdown_pct: max_dd_pct,
            win_rate,
            profit_factor,
            total_trades,
            winning_trades,
            losing_trades,
            avg_win,
            avg_loss,
            largest_win,
            largest_loss,
            equity_curve,
            trades,
            total_transaction_costs,
        }
    }
}

// Core backtesting logic that works with events
pub async fn run_backtest(
    symbol: &str,
    backtest_manager: BacktestManager,
    strategy: &mut dyn Strategy,
    transaction_costs: TransactionCosts,
    starting_equity: f64,
    exposure: f64,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
) -> Result<BacktestResult> {
    let is_options_trading = matches!(
        custom_schema,
        Some(InkBackSchema::CombinedOptionsUnderlying)
    );
    let is_futures_trading =
        symbol.ends_with(".v.0") || symbol.ends_with(".c.0") || symbol.ends_with(".FUT");
    let futures_multiplier = if is_futures_trading {
        get_future_from_symbol(symbol).map(|future| get_future_multiplier(future))
    } else {
        None
    };

    let mut equity = starting_equity;
    let mut position = Position::Neutral;
    let mut trades = Vec::new();
    let mut equity_curve = vec![starting_equity];

    let mut pending_order: Option<Order> = None;
    let mut pending_limit_orders: Vec<Order> = Vec::new();

    let data_path = &backtest_manager.data_path;
    if data_path.is_empty() {
        return Err(anyhow::anyhow!("No data path provided"));
    }

    // GET THE STREAM
    let mut data_iter = fetch::get_data_stream(data_path, schema).await?;

    let mut prev_event: Option<MarketEvent> = None;

    // ASYNC LOOP
    while let Some(event_res) = data_iter.next().await {
        let event = event_res?; // Handle Result

        // Update Avg Volume for slippage
        let vol = event.volume() as f64;

        // Check Limit Orders
        let mut filled_limit_orders = Vec::new();
        pending_limit_orders.retain(|order| {
            if should_fill_limit_order(order, &event) {
                filled_limit_orders.push(*order);
                false
            } else {
                true
            }
        });

        if let Some(order) = filled_limit_orders.first() {
            if matches!(position, Position::Neutral) {
                let capital = equity * exposure;
                let size = if is_options_trading {
                    (capital / (order.price * 100.0)).floor()
                } else {
                    (capital / order.price).floor()
                };

                let adjusted_entry = transaction_costs.adjust_fill_price(
                    order.price,
                    size,
                    matches!(order.order_type, OrderType::LimitBuy),
                );

                match order.order_type {
                    OrderType::LimitBuy => {
                        position = Position::Long {
                            entry: adjusted_entry,
                            size,
                            entry_date: event.date_string(),
                        }
                    }
                    OrderType::LimitSell => {
                        position = Position::Short {
                            entry: adjusted_entry,
                            size,
                            entry_date: event.date_string(),
                        }
                    }
                    _ => {}
                }
            }
        }

        // Check Market Orders
        if let Some(order) = pending_order.take() {
            if matches!(position, Position::Neutral) {
                // Approximate fill at price
                let fill_price = event.price();
                let capital = equity * exposure;
                let size = if is_options_trading {
                    (capital / (fill_price * 100.0)).floor()
                } else {
                    (capital / fill_price).floor()
                };

                let adjusted_entry = transaction_costs.adjust_fill_price(
                    fill_price,
                    size,
                    order.order_type == OrderType::MarketBuy,
                );

                match order.order_type {
                    OrderType::MarketBuy => {
                        position = Position::Long {
                            entry: adjusted_entry,
                            size,
                            entry_date: event.date_string(),
                        }
                    }
                    OrderType::MarketSell => {
                        position = Position::Short {
                            entry: adjusted_entry,
                            size,
                            entry_date: event.date_string(),
                        }
                    }
                    _ => {}
                }
            }
        }

        // Strategy Logic
        if let Some(order) = strategy.on_event(&event, prev_event.as_ref()) {
            match position {
                Position::Long {
                    entry,
                    size,
                    ref entry_date,
                } => {
                    if order.order_type == OrderType::MarketSell {
                        let exit_price =
                            transaction_costs.adjust_fill_price(order.price, size, false);
                        let pnl = position.calculate_pnl_with_costs(
                            exit_price,
                            &transaction_costs,
                            vol,
                            is_options_trading,
                            futures_multiplier,
                        );

                        if pnl.is_finite() {
                            equity += pnl;
                            trades.push(Trade {
                                entry_date: entry_date.clone(),
                                exit_date: event.date_string(),
                                entry_price: entry,
                                exit_price,
                                size,
                                pnl,
                                pnl_pct: ((exit_price / entry) - 1.0) * 100.0,
                                trade_type: "Long".to_string(),
                                exit_reason: "Strategy".to_string(),
                                transaction_costs: 0.0, // Simplified
                            });
                            position = Position::Neutral;
                        }
                    }
                }
                Position::Short {
                    entry,
                    size,
                    ref entry_date,
                } => {
                    if order.order_type == OrderType::MarketBuy {
                        let exit_price =
                            transaction_costs.adjust_fill_price(order.price, size, true);
                        let pnl = position.calculate_pnl_with_costs(
                            exit_price,
                            &transaction_costs,
                            vol,
                            is_options_trading,
                            futures_multiplier,
                        );

                        if pnl.is_finite() {
                            equity += pnl;
                            trades.push(Trade {
                                entry_date: entry_date.clone(),
                                exit_date: event.date_string(),
                                entry_price: entry,
                                exit_price,
                                size,
                                pnl,
                                pnl_pct: ((entry / exit_price) - 1.0) * 100.0,
                                trade_type: "Short".to_string(),
                                exit_reason: "Strategy".to_string(),
                                transaction_costs: 0.0,
                            });
                            position = Position::Neutral;
                        }
                    }
                }
                // Entry Logic
                Position::Neutral => match order.order_type {
                    OrderType::MarketBuy | OrderType::MarketSell => pending_order = Some(order),
                    OrderType::LimitBuy | OrderType::LimitSell => pending_limit_orders.push(order),
                },
            }
        }

        // Update Equity Curve
        if equity.is_finite() {
            equity_curve.push(equity);
        } else {
            equity_curve.push(*equity_curve.last().unwrap_or(&starting_equity));
        }

        prev_event = Some(event);
    }

    Ok(BacktestResult::calculate_metrics(
        starting_equity,
        equity,
        equity_curve,
        trades,
    ))
}

pub fn run_parallel_backtest<F>(
    parameter_combinations: Vec<StrategyParams>,
    backtest_manager: BacktestManager,
    symbol: &str,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    strategy_constructor: F,
    starting_equity: f64,
    exposure: f64,
    transactions_model: TransactionCosts,
) -> Option<Vec<(String, BacktestResult, Vec<f64>)>>
where
    F: Fn(&StrategyParams) -> anyhow::Result<Box<dyn Strategy>> + Sync + Send,
{
    println!(
        "Testing {} parameter combinations...",
        parameter_combinations.len()
    );

    // Create a single runtime handle that all threads can use
    let handle = tokio::runtime::Handle::current();

    let results: Vec<_> = parameter_combinations
        .par_iter()
        .enumerate()
        .filter_map(|(index, params)| {
            let mut strategy = strategy_constructor(params).ok()?;

            // Use the existing runtime's handle
            let result = handle
                .block_on(run_backtest(
                    symbol,
                    backtest_manager.clone(),
                    strategy.as_mut(),
                    transactions_model.clone(),
                    starting_equity,
                    exposure,
                    schema.clone(),
                    custom_schema.clone(),
                ))
                .ok()?;

            if result.equity_curve.iter().any(|&val| !val.is_finite()) {
                return None;
            }

            let param_str = format!("Strategy_{}", index + 1);
            let finite_curve = result.equity_curve.clone();
            Some((param_str, result, finite_curve))
        })
        .collect();

    let mut sorted_results = results;
    sorted_results.sort_by(|a, b| {
        b.1.total_return_pct
            .partial_cmp(&a.1.total_return_pct)
            .unwrap()
    });
    Some(sorted_results)
}

pub async fn calculate_benchmark(
    csv_path: &str,
    symbol: &str,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    starting_equity: f64,
    exposure: f64,
) -> Result<BacktestResult> {
    let mut data_iter = fetch::get_data_stream(csv_path, schema).await?;

    let is_options_combined = matches!(
        custom_schema,
        Some(InkBackSchema::CombinedOptionsUnderlying)
    );

    // For combined options/underlying, filter to only underlying trades
    let mut first_underlying_price: Option<f64> = None;
    let mut last_underlying_price: Option<f64> = None;
    let mut first_event_date: Option<String> = None;
    let mut last_event_date: Option<String> = None;

    let multiplier = get_future_from_symbol(symbol)
        .map(get_future_multiplier)
        .unwrap_or(1.0);

    let mut equity_curve = vec![starting_equity];

    // Iterate through all events
    while let Some(res) = data_iter.next().await {
        if let Ok(event) = res {
            if is_options_combined {
                // underlying trades have event_type = "UND"
                if let Some(event_type) = event.get_string("event_type") {
                    if event_type != "UND" {
                        continue; // Skip options trades
                    }
                }
                // Also check if it's an OptionTrade variant
                if matches!(event, MarketEvent::OptionTrade(_)) {
                    continue; // Skip option trades
                }
            }

            let price = event.price();

            // Set first price if not set
            if first_underlying_price.is_none() {
                first_underlying_price = Some(price);
                first_event_date = Some(event.date_string());
            }

            // Update last price
            last_underlying_price = Some(price);
            last_event_date = Some(event.date_string());

            // Calculate equity based on buy and hold from first price
            if let Some(entry_price) = first_underlying_price {
                let capital = starting_equity * exposure;
                let size = capital / entry_price;
                let eq = (price - entry_price) * size * multiplier + starting_equity;
                equity_curve.push(eq);
            }
        }
    }

    // Ensure we found underlying data
    let entry_price = first_underlying_price
        .ok_or_else(|| anyhow::anyhow!("No underlying data found for benchmark"))?;
    let exit_price = last_underlying_price
        .ok_or_else(|| anyhow::anyhow!("No underlying data found for benchmark"))?;
    let entry_date = first_event_date
        .ok_or_else(|| anyhow::anyhow!("No underlying data found for benchmark"))?;
    let exit_date =
        last_event_date.ok_or_else(|| anyhow::anyhow!("No underlying data found for benchmark"))?;

    let capital = starting_equity * exposure;
    let size = capital / entry_price;
    let pnl = (exit_price - entry_price) * size * multiplier;

    // Construct single trade result
    let trade = Trade {
        entry_date,
        exit_date,
        entry_price,
        exit_price,
        size,
        pnl,
        pnl_pct: (exit_price / entry_price - 1.0) * 100.0,
        trade_type: "Benchmark".to_string(),
        exit_reason: "End".to_string(),
        transaction_costs: 0.0,
    };

    Ok(BacktestResult::calculate_metrics(
        starting_equity,
        *equity_curve.last().unwrap(),
        equity_curve,
        vec![trade],
    ))
}

pub async fn display_results(
    sorted_results: Option<Vec<(String, BacktestResult, Vec<f64>)>>,
    csv_path: &str,
    symbol: &str,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    starting_equity: f64,
    exposure: f64,
) {
    let mut equity_curves: Vec<(String, Vec<f64>)> = Vec::new();

    // Run benchmark on underlying asset
    let benchmark = calculate_benchmark(
        &csv_path,
        symbol,
        schema,
        custom_schema,
        starting_equity,
        exposure,
    )
    .await
    .unwrap();

    println!(
        "Benchmark Return: {:.2}%, Max Drawdown: {:.2}%",
        benchmark.total_return_pct, benchmark.max_drawdown_pct
    );

    if let Some(sorted_results) = sorted_results {
        // Print results for all strategies
        println!("\n=== ALL STRATEGY RESULTS ===");
        println!(
            "Benchmark: Return {:.2}%, Max DD: {:.2}%\n",
            benchmark.total_return_pct, benchmark.max_drawdown_pct
        );

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
            let profitable_strategies = sorted_results
                .iter()
                .filter(|(_, result, _)| result.total_return_pct > 0.0)
                .count();

            let avg_return: f64 = sorted_results
                .iter()
                .map(|(_, result, _)| result.total_return_pct)
                .sum::<f64>()
                / sorted_results.len() as f64;

            let best_return = sorted_results
                .first()
                .map(|(_, result, _)| result.total_return_pct)
                .unwrap_or(0.0);
            let worst_return = sorted_results
                .last()
                .map(|(_, result, _)| result.total_return_pct)
                .unwrap_or(0.0);

            println!("\n=== SUMMARY STATISTICS ===");
            println!("Total strategies tested: {}", sorted_results.len());
            println!(
                "Profitable strategies: {} ({:.1}%)",
                profitable_strategies,
                (profitable_strategies as f64 / sorted_results.len() as f64) * 100.0
            );
            println!("Average return: {:.2}%", avg_return);
            println!("Best return: {:.2}%", best_return);
            println!("Worst return: {:.2}%", worst_return);
            println!("Benchmark return: {:.2}%", benchmark.total_return_pct);

            let outperforming = sorted_results
                .iter()
                .filter(|(_, result, _)| result.total_return_pct > benchmark.total_return_pct)
                .count();
            println!(
                "Strategies beating benchmark: {} ({:.1}%)",
                outperforming,
                (outperforming as f64 / sorted_results.len() as f64) * 100.0
            );
        }

        // Plot equity curves
        if !equity_curves.is_empty() {
            println!("\nLaunching performance chart for all strategies...");
            let finite_benchmark: Vec<f64> = benchmark
                .equity_curve
                .iter()
                .map(|&val| {
                    if val.is_finite() {
                        val
                    } else {
                        starting_equity
                    }
                })
                .collect();

            // Limit the number of curves plotted to avoid clutter
            let max_curves = 20;
            let curves_to_plot = if equity_curves.len() > max_curves {
                println!(
                    "Too many equity curves ({}), plotting only the top {} strategies.",
                    equity_curves.len(),
                    max_curves
                );
                equity_curves.into_iter().take(max_curves).collect()
            } else {
                equity_curves
            };

            plot_equity_curves(curves_to_plot, Some(finite_benchmark));
        }
    } else {
        println!("Failed to run backtest - no results returned");
    }
}

fn get_future_multiplier(future_traded: FutureTraded) -> f64 {
    match future_traded {
        FutureTraded::NQ => 5.00,  // $5 per tick (0.25 tick size)
        FutureTraded::ES => 12.50, // $12.50 per tick (0.25 tick size)
        FutureTraded::YM => 5.00,  // $5 per tick (1.00 tick size)
        FutureTraded::CL => 10.00, // $10 per tick (0.01 tick size)
        FutureTraded::GC => 10.00, // $10 per tick (0.10 tick size)
        FutureTraded::SI => 25.00, // $25 per tick (0.005 tick size)
    }
}

fn get_future_from_symbol(symbol: &str) -> Option<FutureTraded> {
    if symbol.starts_with("NQ") {
        Some(FutureTraded::NQ)
    } else if symbol.starts_with("ES") {
        Some(FutureTraded::ES)
    } else if symbol.starts_with("YM") {
        Some(FutureTraded::YM)
    } else if symbol.starts_with("CL") {
        Some(FutureTraded::CL)
    } else if symbol.starts_with("GC") {
        Some(FutureTraded::GC)
    } else if symbol.starts_with("SI") {
        Some(FutureTraded::SI)
    } else {
        None
    }
}

// Helper function to check if a limit order should be filled based on current candle
pub fn should_fill_limit_order(order: &Order, event: &MarketEvent) -> bool {
    let high = event.high();
    let low = event.low();

    match order.order_type {
        OrderType::LimitBuy => low <= order.price, // Fill if price drops to or below limit price
        OrderType::LimitSell => high >= order.price, // Fill if price rises to or above limit price
        _ => false,                                // Not a limit order
    }
}
