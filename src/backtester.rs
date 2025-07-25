use crate::{strategy::{Candle, Order, OrderType, Strategy}, InkBackSchema};
use crate::slippage_models::TransactionCosts;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use databento::dbn::Schema;

use crate::schema_handler::{get_schema_handler};

#[derive(Debug, PartialEq)]
enum Position {
    Long { entry: f64, size: f64, entry_date: String },
    Short { entry: f64, size: f64, entry_date: String },
    Neutral,
}

impl Position {
    fn calculate_pnl_with_costs(
        &self,
        exit_price: f64,
        costs: &TransactionCosts,
        avg_volume: f64,
        is_options: bool,
    ) -> f64 {
        match self {
            Position::Long { entry, size, .. } => {
                let entry_cost = costs.calculate_entry_cost(*entry, *size, avg_volume);
                let exit_cost = costs.calculate_exit_cost(exit_price, *size, avg_volume);
                
                // Apply options multiplier only for options trading
                let multiplier = if is_options { 100.0 } else { 1.0 };
                let gross_pnl = (exit_price - entry) * size * multiplier;
                
                // Validate costs are finite - this is crucial
                if !entry_cost.is_finite() || !exit_cost.is_finite() || !gross_pnl.is_finite() {
                    println!("Warning: Non-finite values in PnL calculation");
                    return 0.0; // Return 0 PnL if costs are infinite
                }
                
                gross_pnl - entry_cost - exit_cost
            }
            Position::Short { entry, size, .. } => {
                let entry_cost = costs.calculate_entry_cost(*entry, *size, avg_volume);
                let exit_cost = costs.calculate_exit_cost(exit_price, *size, avg_volume);
                
                let multiplier = if is_options { 100.0 } else { 1.0 };
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
        let win_rate = if total_trades == 0 { 0.0 } else { (winning_trades as f64 / total_trades as f64) * 100.0 };
        
        let gross_profit: f64 = trades.iter().filter(|t| t.pnl > 0.0).map(|t| t.pnl).sum();
        let gross_loss: f64 = trades.iter().filter(|t| t.pnl < 0.0).map(|t| t.pnl.abs()).sum();
        let profit_factor = if gross_loss == 0.0 { 
            if gross_profit > 0.0 { 1000.0 } else { 0.0 } // Cap at 1000 instead of infinity
        } else { 
            gross_profit / gross_loss 
        };
        
        let avg_win = if winning_trades == 0 { 0.0 } else { gross_profit / winning_trades as f64 };
        let avg_loss = if losing_trades == 0 { 0.0 } else { gross_loss / losing_trades as f64 };
        
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

// Helper function to calculate average volume from candles
fn calculate_average_volume(candles: &[Candle]) -> f64 {
    let total_volume: f64 = candles.iter()
        .filter_map(|candle| candle.get("volume"))
        .sum();
    
    if candles.is_empty() {
        1000000.0 // Default fallback volume
    } else {
        total_volume / candles.len() as f64
    }
}

// Helper function to check if a limit order should be filled based on current candle
fn should_fill_limit_order(order: &Order, candle: &Candle) -> bool {
    let high = candle.get("high").unwrap_or_else(|| candle.get("price").unwrap_or(order.price));
    let low = candle.get("low").unwrap_or_else(|| candle.get("price").unwrap_or(order.price));
    
    match order.order_type {
        OrderType::LimitBuy => low <= order.price,  // Fill if price drops to or below limit price
        OrderType::LimitSell => high >= order.price, // Fill if price rises to or above limit price
        _ => false, // Not a limit order
    }
}

// Generic function that works with any schema
pub fn run_backtest_with_schema(
    csv_path: &str,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    strategy: &mut dyn Strategy,
    transaction_costs: TransactionCosts,
    starting_equity: f64,
    exposure: f64,
) -> Result<BacktestResult> {
    // Determine which schema to use for the handler
    let handler_schema = if let Some(ref custom_schema) = custom_schema {
        match custom_schema {
            InkBackSchema::FootPrint => Schema::Ohlcv1D, // FootPrint bars are stored as OHLCV format
            InkBackSchema::CombinedOptionsUnderlying => Schema::Definition,
        }
    } else {
        schema
    };

    // Get the appropriate schema handler
    let handler = get_schema_handler(handler_schema);
    
    // Convert CSV data to candles
    let candles = handler.csv_to_candles(csv_path)?;
    
    // Run backtest with candles
    run_backtest_with_candles(candles, strategy, transaction_costs, starting_equity, exposure)
}

// Core backtesting logic that works with candles
pub fn run_backtest_with_candles(
    candles: Vec<Candle>,
    strategy: &mut dyn Strategy,
    transaction_costs: TransactionCosts,
    starting_equity: f64,
    exposure: f64,
) -> Result<BacktestResult> {
    // Detect if we're trading options by checking for option-specific fields
    let is_options_trading = candles.iter().any(|candle| 
        candle.get_string("instrument_class").is_some() && 
        candle.get("strike_price").is_some()
    );
    let mut equity = starting_equity;
    let mut position = Position::Neutral;
    let mut prev_candle: Option<Candle> = None;
    let mut trades = Vec::new();
    let mut equity_curve = Vec::new();
    let mut pending_order: Option<Order> = None; // Store orders for next candle
    let mut pending_limit_orders: Vec<Order> = Vec::new(); // Store limit orders until filled or cancelled

    // Calculate average volume for transaction cost calculations
    let avg_volume = calculate_average_volume(&candles);

    // Add initial equity value
    equity_curve.push(starting_equity);

    for candle in candles.iter() {
        //  check if any pending limit orders should be filled
        let mut filled_limit_orders = Vec::new();
        pending_limit_orders.retain(|order| {
            if should_fill_limit_order(order, candle) {
                filled_limit_orders.push(*order);
                false // Remove from pending orders
            } else {
                true // Keep in pending orders
            }
        });
        
        // Process filled limit orders (only process the first one if multiple are filled)
        if let Some(order) = filled_limit_orders.first() {
            if matches!(position, Position::Neutral) {
                let capital = equity * exposure;
                let size = if is_options_trading {
                    let option_notional_value = order.price * 100.0;
                    (capital / option_notional_value).floor()
                } else {
                    (capital / order.price).floor()
                };
                
                let adjusted_entry_price = transaction_costs.adjust_fill_price(
                    order.price,
                    size,
                    avg_volume,
                    matches!(order.order_type, OrderType::LimitBuy)
                );
                
                match order.order_type {
                    OrderType::LimitBuy => {
                        position = Position::Long {
                            entry: adjusted_entry_price,
                            size,
                            entry_date: candle.date.clone(),
                        };
                    }
                    OrderType::LimitSell => {
                        position = Position::Short {
                            entry: adjusted_entry_price,
                            size,
                            entry_date: candle.date.clone(),
                        };
                    }
                    _ => {} // Should not happen for limit orders
                }
            }
        }
        
        // Then, execute any pending market order from the previous candle
        if let Some(order) = pending_order.take() {
            match position {
                Position::Neutral => {
                    let _entry_price = candle.get("open")
                        .unwrap_or_else(|| candle.get("close")
                            .unwrap_or_else(|| candle.get("price")
                                .unwrap_or(order.price)));
                    
                    let capital = equity * exposure;
                    let size = if is_options_trading {
                        // Use the order price for position sizing
                        let option_notional_value = order.price * 100.0;
                        (capital / option_notional_value).floor()
                    } else {
                        (capital / order.price).floor()
                    };
                    
                    let adjusted_entry_price = transaction_costs.adjust_fill_price(
                        order.price,
                        size,
                        avg_volume,
                        order.order_type == OrderType::MarketBuy
                    );
                    
                    // Extract contract info for better logging
                    let _contract_info = if is_options_trading {
                        // Get contract details from the candle
                        let instrument_class = candle.get_string("instrument_class").map(|s| s.as_str()).unwrap_or("UNK");
                        let strike_price = candle.get("strike_price").unwrap_or(0.0);
                        let symbol = candle.get_string("raw_symbol")
                            .or_else(|| candle.get_string("symbol_def"))
                            .or_else(|| candle.get_string("symbol"))
                            .map(|s| s.as_str())
                            .unwrap_or("UNKNOWN");
                        
                        let option_type = match instrument_class.chars().next().unwrap_or('U') {
                            'C' => format!("C{:.0}", strike_price),
                            'P' => format!("P{:.0}", strike_price),
                            _ => "UNK".to_string(),
                        };
                        
                        format!("{} {}", symbol, option_type)
                    } else {
                        "Stock".to_string()
                    };
                    
                    //println!("BUYING {} at order price {:.2}, adjusted price {:.2}, size {}, equity ${:.0}", 
                    //        contract_info, order.price, adjusted_entry_price, size, equity);
                    
                    match order.order_type {
                        OrderType::MarketBuy => {
                            position = Position::Long {
                                entry: adjusted_entry_price,
                                size,
                                entry_date: candle.date.clone(),
                            };
                        }
                        OrderType::MarketSell => {
                            position = Position::Short {
                                entry: adjusted_entry_price,
                                size,
                                entry_date: candle.date.clone(),
                            };
                        }
                        _ => {} // Limit orders handled above
                    }
                }
                _ => {
                    println!("Warning: Pending order while already in position");
                }
            }
        }

        if let Some(order) = strategy.on_candle(&candle, prev_candle.as_ref()) {
            match position {
                // If we're in a position and get an order, it must be an exit
                Position::Long { entry, size, ref entry_date } => {
                    if order.order_type == OrderType::MarketSell {
                        let exit_price = transaction_costs.adjust_fill_price(
                            order.price,
                            size,
                            avg_volume,
                            false // selling, so we get worse price
                        );
                        
                        // Extract contract info for better logging
                        let _contract_info = if is_options_trading {
                            let instrument_class = candle.get_string("instrument_class").map(|s| s.as_str()).unwrap_or("UNK");
                            let strike_price = candle.get("strike_price").unwrap_or(0.0);
                            let symbol = candle.get_string("raw_symbol")
                                .or_else(|| candle.get_string("symbol_def"))
                                .or_else(|| candle.get_string("symbol"))
                                .map(|s| s.as_str())
                                .unwrap_or("UNKNOWN");
                            
                            let option_type = match instrument_class.chars().next().unwrap_or('U') {
                                'C' => format!("C{:.0}", strike_price),
                                'P' => format!("P{:.0}", strike_price),
                                _ => "UNK".to_string(),
                            };
                            
                            format!("{} {}", symbol, option_type)
                        } else {
                            "Stock".to_string()
                        };
                        
                        //println!("SELLING {} at order price {:.2}, adjusted price {:.2} (Entry: {:.2})", 
                        //        contract_info, order.price, exit_price, entry);
                        
                        // Calculate PnL with transaction costs
                        let pnl = position.calculate_pnl_with_costs(exit_price, &transaction_costs, avg_volume, is_options_trading);
                        
                        // Calculate gross PnL with appropriate multiplier
                        let multiplier = if is_options_trading { 100.0 } else { 1.0 };
                        let gross_pnl = (exit_price - entry) * size * multiplier;
                        let transaction_cost = gross_pnl - pnl;

                        // Check what's causing infinite PnL
                        if !pnl.is_finite() {
                            println!("Debug Long: Infinite PnL - entry: {}, exit: {}, size: {}, gross_pnl: {}, pnl: {}", 
                                entry, exit_price, size, gross_pnl, pnl);
                            continue; // Skip adding this trade
                        }
                        
                        equity += pnl;

                        trades.push(Trade {
                            entry_date: entry_date.clone(),
                            exit_date: candle.date.clone(),
                            entry_price: entry,
                            exit_price,
                            size,
                            pnl,
                            pnl_pct: ((exit_price / entry) - 1.0) * 100.0,
                            trade_type: "Long".to_string(),
                            exit_reason: "Strategy".to_string(), // Strategy decided to exit
                            transaction_costs: transaction_cost,
                        });

                        position = Position::Neutral;
                    }
                }
                Position::Short { entry, size, ref entry_date } => {
                    if order.order_type == OrderType::MarketBuy {
                        let exit_price = transaction_costs.adjust_fill_price(
                            order.price,
                            size,
                            avg_volume,
                            true // buying to cover, so we get worse price
                        );
                        
                        // Extract contract info for better logging
                        let _contract_info = if is_options_trading {
                            let instrument_class = candle.get_string("instrument_class").map(|s| s.as_str()).unwrap_or("UNK");
                            let strike_price = candle.get("strike_price").unwrap_or(0.0);
                            let symbol = candle.get_string("raw_symbol")
                                .or_else(|| candle.get_string("symbol_def"))
                                .or_else(|| candle.get_string("symbol"))
                                .map(|s| s.as_str())
                                .unwrap_or("UNKNOWN");
                            
                            let option_type = match instrument_class.chars().next().unwrap_or('U') {
                                'C' => format!("C{:.0}", strike_price),
                                'P' => format!("P{:.0}", strike_price),
                                _ => "UNK".to_string(),
                            };
                            
                            format!("{} {}", symbol, option_type)
                        } else {
                            "Stock".to_string()
                        };
                        
                        //println!("COVERING {} at order price {:.2}, adjusted price {:.2} (Entry: {:.2})", 
                        //        contract_info, order.price, exit_price, entry);
                        
                        // Calculate PnL with transaction costs
                        let pnl = position.calculate_pnl_with_costs(exit_price, &transaction_costs, avg_volume, is_options_trading);
                        
                        // Calculate gross PnL with appropriate multiplier
                        let multiplier = if is_options_trading { 100.0 } else { 1.0 };
                        let gross_pnl = (entry - exit_price) * size * multiplier;
                        let transaction_cost = gross_pnl - pnl;

                        // Check what could cause infinite PnL
                        if !pnl.is_finite() {
                            println!("Debug Short: Infinite PnL - entry: {}, exit: {}, size: {}, gross_pnl: {}, pnl: {}", 
                                entry, exit_price, size, gross_pnl, pnl);
                            continue; // Skip adding this trade
                        }
                        
                        equity += pnl;

                        trades.push(Trade {
                            entry_date: entry_date.clone(),
                            exit_date: candle.date.clone(),
                            entry_price: entry,
                            exit_price,
                            size,
                            pnl,
                            pnl_pct: ((entry / exit_price) - 1.0) * 100.0,
                            trade_type: "Short".to_string(),
                            exit_reason: "Strategy".to_string(), // Strategy decided to exit
                            transaction_costs: transaction_cost,
                        });

                        position = Position::Neutral;
                    }
                }
                // If we're neutral and get an order, it's a new entry
                Position::Neutral => {
                    match order.order_type {
                        OrderType::MarketBuy | OrderType::MarketSell => {
                            // Market orders are executed next candle
                            pending_order = Some(order);
                        }
                        OrderType::LimitBuy | OrderType::LimitSell => {
                            // Limit orders are added to pending limit orders queue
                            pending_limit_orders.push(order);
                        }
                    }
                }
            }
        }

        // Ensure equity is finite before adding to curve
        if equity.is_finite() {
            equity_curve.push(equity);
        } else {
            // Use the last finite equity value
            let last_equity = equity_curve.last().copied().unwrap_or(starting_equity);
            equity_curve.push(last_equity);
            equity = last_equity; // Reset equity to last valid value
        }
        prev_candle = Some(candle.clone());
    }

    Ok(BacktestResult::calculate_metrics(
        starting_equity,
        equity,
        equity_curve,
        trades,
    ))
}

// Benchmark calculation that works with any schema
pub fn calculate_benchmark_with_schema(
    csv_path: &str,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    starting_equity: f64,
    exposure: f64,
) -> Result<BacktestResult> {

    if matches!(schema, Schema::Status | Schema::Definition | Schema::Statistics) {
        return Err(anyhow::anyhow!("Schema does not support price data"));
    }

    let handler = get_schema_handler(schema);
    let candles = handler.csv_to_candles(csv_path)?;
    
    if candles.is_empty() {
        return Err(anyhow::anyhow!("No candles found"));
    }

    let first_candle = &candles[0];
    let last_candle = &candles[candles.len() - 1];
    
    let mut equity = starting_equity;
    let mut equity_curve = vec![starting_equity];
    let mut trades = Vec::new();

    // Determine the key based on custom schema or regular schema
    let key = if let Some(ref custom_schema) = custom_schema {
        match custom_schema {
            InkBackSchema::FootPrint => "close", // FootPrint bars have OHLCV format
            InkBackSchema::CombinedOptionsUnderlying => "underlying_ask", // for simplicity
        }
    } else {
        match schema {
            Schema::Ohlcv1S | Schema::Ohlcv1M | Schema::Ohlcv1H | Schema::Ohlcv1D | Schema::OhlcvEod => "close",
            Schema::Mbo | Schema::Trades => "price",
            Schema::Mbp1 | Schema::Tbbo | Schema::Cbbo | Schema::Cbbo1S | Schema::Cbbo1M | Schema::Tcbbo | Schema::Bbo1S | Schema::Bbo1M => "ask_price", // for simplicity
            Schema::Mbp10 => "level_0_ask_price", // for simplicity
            Schema::Imbalance =>  "ref_price",
            _ => unreachable!(),
        }
    };

    let first_close = first_candle.get(key)
    .ok_or_else(|| anyhow::anyhow!("Missing {} in candle", key))?;

    let last_close = last_candle.get(key)
    .ok_or_else(|| anyhow::anyhow!("Missing {} in candle", key))?;

    let capital = equity * exposure;
    let size = capital / first_close;
    let entry_price = first_close;

    // Calculate equity progression
    for candle in &candles[1..] {
        let close = candle.get(key)
            .ok_or_else(|| anyhow::anyhow!("Missing {} in candle", key))?;

        equity = (close - entry_price) * size + starting_equity;
        equity_curve.push(equity);
    }

    let exit_price = last_close;
    let pnl = (exit_price - entry_price) * size;
    let pnl_pct = ((exit_price / entry_price) - 1.0) * 100.0;

    trades.push(Trade {
        entry_date: first_candle.date.clone(),
        exit_date: last_candle.date.clone(),
        entry_price,
        exit_price,
        size,
        pnl,
        pnl_pct,
        trade_type: "Benchmark".to_string(),
        exit_reason: "EndOfPeriod".to_string(),
        transaction_costs: 0.0,
    });

    Ok(BacktestResult::calculate_metrics(
        starting_equity,
        equity,
        equity_curve,
        trades,
    ))
}

pub fn run_backtest(
    path: &str,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    strategy: &mut dyn Strategy,
    starting_equity: f64,
    exposure: f64,
    transactions_model: TransactionCosts,
) -> Result<BacktestResult> {
    run_backtest_with_schema(path, schema, custom_schema, strategy, transactions_model, starting_equity, exposure)
}

pub fn calculate_benchmark(
    path: &str,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    starting_equity: f64,
    exposure: f64,
) -> Result<BacktestResult> {
    calculate_benchmark_with_schema(path, schema, custom_schema, starting_equity, exposure)
}
