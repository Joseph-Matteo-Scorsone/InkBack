use crate::{plot::plot_equity_curves, strategy::{Candle, Order, OrderType, Strategy, StrategyParams}, InkBackSchema};
use crate::slippage_models::TransactionCosts;
use anyhow::Result;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use databento::dbn::Schema;

use crate::schema_handler::{get_schema_handler};

#[derive(Debug, PartialEq)]
enum Position {
    Long { entry: f64, size: f64, entry_date: String },
    Short { entry: f64, size: f64, entry_date: String },
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

fn get_future_multiplier(future_traded: FutureTraded) -> f64 {
    match future_traded {
        FutureTraded::NQ => 5.00,   // $5 per tick (0.25 tick size)
        FutureTraded::ES => 12.50,  // $12.50 per tick (0.25 tick size)  
        FutureTraded::YM => 5.00,   // $5 per tick (1.00 tick size)
        FutureTraded::CL => 10.00,  // $10 per tick (0.01 tick size)
        FutureTraded::GC => 10.00,  // $10 per tick (0.10 tick size)
        FutureTraded::SI => 25.00,  // $25 per tick (0.005 tick size)
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

impl Position {
    fn calculate_pnl_with_costs(
        &self,
        exit_price: f64,
        costs: &TransactionCosts,
        avg_volume: f64,
        is_options: bool,
        futures_multiplier: Option<f64>,
    ) -> f64 {
        match self {
            Position::Long { entry, size, .. } => {
                let entry_cost = costs.calculate_entry_cost(*entry, *size, avg_volume);
                let exit_cost = costs.calculate_exit_cost(exit_price, *size, avg_volume);
                
                // Apply appropriate multiplier based on instrument type
                let multiplier = if is_options { 
                    100.0 
                } else if let Some(futures_mult) = futures_multiplier {
                    futures_mult
                } else { 
                    1.0 
                };
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
    if candles.is_empty() {
        return 1_000_000.0; // Default fallback if no candles at all
    }

    let total_volume: f64 = candles.iter()
        .map(|candle| {
            candle.get("volume")
                .or_else(|| candle.get("size"))  // fallback to "size"
                .unwrap_or(0.0)                  // default if neither
        })
        .sum();

    total_volume / candles.len() as f64
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
    symbol: &str,
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
    run_backtest_with_candles(symbol, candles, strategy, transaction_costs, starting_equity, exposure)
}

// Core backtesting logic that works with candles
pub fn run_backtest_with_candles(
    symbol: &str,
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

    let is_futures_trading = symbol.ends_with(".v.0") || symbol.ends_with(".c.0");
    let futures_multiplier = if is_futures_trading {
        get_future_from_symbol(symbol).map(|future| get_future_multiplier(future))
    } else {
        None
    };
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
                        let pnl = position.calculate_pnl_with_costs(exit_price, &transaction_costs, avg_volume, is_options_trading, futures_multiplier);
                        
                        // Calculate gross PnL with appropriate multiplier
                        let multiplier = if is_options_trading { 
                            100.0 
                        } else if let Some(futures_mult) = futures_multiplier {
                            futures_mult
                        } else { 
                            1.0 
                        };
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
                        let pnl = position.calculate_pnl_with_costs(exit_price, &transaction_costs, avg_volume, is_options_trading, futures_multiplier);
                        
                        // Calculate gross PnL with appropriate multiplier
                        let multiplier = if is_options_trading { 
                            100.0 
                        } else if let Some(futures_mult) = futures_multiplier {
                            futures_mult
                        } else { 
                            1.0 
                        };
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
    symbol: &str,
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

    // Check if we're trading futures and get the multiplier
    let is_futures_trading = symbol.ends_with(".v.0") || symbol.ends_with(".c.0");
    let futures_multiplier = if is_futures_trading {
        get_future_from_symbol(symbol).map(|future| get_future_multiplier(future))
    } else {
        None
    };

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

    // Apply appropriate multiplier for different instrument types
    let multiplier = if let Some(futures_mult) = futures_multiplier {
        futures_mult
    } else {
        1.0 // Default multiplier for stocks/other instruments
    };

    // Calculate equity progression
    for candle in &candles[1..] {
        let close = candle.get(key)
            .ok_or_else(|| anyhow::anyhow!("Missing {} in candle", key))?;

        equity = (close - entry_price) * size * multiplier + starting_equity;
        equity_curve.push(equity);
    }

    let exit_price = last_close;
    let pnl = (exit_price - entry_price) * size * multiplier;
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

pub fn run_individual_backtest(
    path: &str,
    symbol: &str,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    strategy: &mut dyn Strategy,
    starting_equity: f64,
    exposure: f64,
    transactions_model: TransactionCosts,
) -> Result<BacktestResult> {
    run_backtest_with_schema(path, symbol, schema, custom_schema, strategy, transactions_model, starting_equity, exposure)
}

pub fn run_parallel_backtest<F>(
    parameter_combinations: Vec<StrategyParams>,
    path: &str,
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

    println!("Testing {} parameter combinations...", parameter_combinations.len());

    // Run backtests in parallel
    let results: Vec<_> = parameter_combinations
        .par_iter()
        .enumerate()  // Add enumeration to track which strategy
        .filter_map(|(index, params)| {
            // Create a fresh strategy instance for each parameter set
            let mut strategy = strategy_constructor(params).ok()?;

            println!("Testing strategy {} with params: {:?}", index + 1, params);

            let result = run_individual_backtest(
                &path,
                &symbol,
                schema,
                custom_schema.clone(),
                strategy.as_mut(),
                starting_equity,
                exposure,
                transactions_model.clone(),
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

    Some(sorted_results)
}

pub fn display_results(
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
        exposure
    ).unwrap();
    
    println!("Benchmark Return: {:.2}%, Max Drawdown: {:.2}%", 
        benchmark.total_return_pct, benchmark.max_drawdown_pct);
    
    // Print results for all strategies
    println!("\n=== ALL STRATEGY RESULTS ===");
    println!("Benchmark: Return {:.2}%, Max DD: {:.2}%\n", 
        benchmark.total_return_pct, benchmark.max_drawdown_pct);

    if let Some(sorted_results) = sorted_results {
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
    } else {
        println!("Failed to run backtest - no results returned");
    }
}

pub fn calculate_benchmark(
    path: &str,
    symbol: &str,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    starting_equity: f64,
    exposure: f64,
) -> Result<BacktestResult> {
    calculate_benchmark_with_schema(path, symbol, schema, custom_schema, starting_equity, exposure)
}
