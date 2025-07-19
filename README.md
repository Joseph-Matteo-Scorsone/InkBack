# InkBack by Scorsone Enterprises

A high-performance historical backtesting framework written in Rust, designed for quantitative trading strategy development and analysis. Built with DataBento for market data and Iced for visualization.

## Features

- **Multi-asset Support**: Equities, futures, and options backtesting
- **Custom Strategy Development**: Implement your own trading strategies using the `Strategy` trait
- **Parallel Processing**: Run multiple backtests concurrently with different parameter combinations
- **Real-time Visualization**: Interactive equity curve plotting with Iced GUI
- **Order Flow Analysis**: Built-in footprint imbalance detection and volume analysis
- **Realistic Trading**: Includes slippage models, transaction costs, and order pending logic
- **Data Management**: Automatic DataBento data fetching and CSV caching
- **Risk Management**: Flexible position sizing and risk controls

## Architecture

InkBack follows a modular design:

- **Strategy**: Define custom trading logic by implementing the `Strategy` trait
- **Backtester**: Core engine that processes historical data and executes strategies
- **Data Handler**: Manages DataBento integration and local data storage
- **Visualization**: Real-time equity curve plotting and performance metrics

## Prerequisites

- **Rust**: Version 1.83.0 or higher
- **DataBento API Key**: Professional market data access
- **Dependencies**: Managed automatically via Cargo

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/Joseph-Matteo-Scorsone/InkBack.git
cd InkBack
```

### 2. Configure Environment

Create a `.env` file in the project root:

```env
DATABENTO_API_KEY=your_databento_api_key_here
```

### 3. Run Examples

```bash
# Footprint volume imbalance strategy
examples/footprint/footprint_example.rs

# Options momentum strategy  
examples/options/options_example.rs

# Futures strategy
examples/futures/futures_example.rs

# Equities strategy
examples/equities/equities_example.rs
```

## Creating Custom Strategies

### Basic Strategy Implementation

```rust
use crate::strategy::{Strategy, Candle, Order, OrderType};

pub struct MyStrategy {
    // Your strategy parameters and state
}

impl Strategy for MyStrategy {
    fn on_candle(&mut self, candle: &Candle, prev: Option<&Candle>) -> Option<Order> {
        // Implement your trading logic here
        // Return Some(Order) to place an order, None to do nothing
        
        let close_price = candle.get("close")?;
        
        // Example: Simple momentum strategy
        if let Some(prev_candle) = prev {
            let prev_close = prev_candle.get("close")?;
            if close_price > prev_close * 1.01 {
                return Some(Order {
                    order_type: OrderType::Buy,
                    price: close_price,
                });
            }
        }
        
        None
    }
}
```

### Accessing Market Data

The `Candle` struct provides access to all market data fields:

```rust
// Numeric fields (OHLCV, etc.)
let close = candle.get("close")?;
let volume = candle.get("volume")?;
let high = candle.get("high")?;

// String fields (symbols, footprint data, etc.)
let symbol = candle.get_string("symbol")?;
let footprint = candle.get_string("footprint_data")?;
```

## Data Sources

InkBack supports multiple DataBento schemas:

- **FootPrint**: Order flow and volume profile data
- **CombinedOptionsUnderlying**: Options chains with underlying data
- **OHLCV**: Traditional candlestick data
- **Trades**: Tick-by-tick trade data

## Configuration

### Slippage Models

Configure realistic transaction costs:

```rust
use crate::slippage_models::{TransactionCosts, LinearSlippage};

let costs = TransactionCosts {
    fixed_fee: 0.50,
    percentage_fee: 0.001,
    slippage: Box::new(LinearSlippage::new(0.0001)),
};
```

### Parameter Optimization

Run parameter sweeps for strategy optimization:

```rust
let param_ranges = vec![
    (10..=50).step_by(10).collect(), // lookback periods
    (0.01..=0.05).step_by(0.01).collect(), // thresholds
];

// Generate all combinations and run in parallel
let results = param_ranges
    .into_par_iter()
    .map(|params| run_backtest(params))
    .collect();
```

## Performance Metrics

InkBack automatically calculates:

- Total return and annualized return
- Sharpe ratio and maximum drawdown
- Win rate and profit factor
- Average trade duration
- Risk-adjusted metrics

## Data Management

- **Automatic Caching**: Downloaded data is saved locally as CSV
- **Compression Handling**: Automatic decompression of DataBento's 9th exponent format
- **Incremental Updates**: Only fetch new data when needed
- **Multiple Timeframes**: Support for various data frequencies

## Examples

The `examples/` directory contains complete implementations:

- **Footprint Strategy**: Volume imbalance detection using order flow
- **Options Momentum**: Momentum-based options trading
- **Futures Strategy**: Trend-following futures system
- **Equities Strategy**: Mean reversion equity strategy

## Output

![alt text](https://pbs.twimg.com/media/GwKiukOW8AAuPx9?format=jpg&name=900x900)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes with tests
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

# DISCLAIMER

PLEASE READ THIS DISCLAIMER CAREFULLY BEFORE USING THE SOFTWARE. BY ACCESSING OR USING THE SOFTWARE, YOU ACKNOWLEDGE AND AGREE TO BE BOUND BY THE TERMS HEREIN.

This software and related documentation ("Software") are provided solely for educational and research purposes. The Software is not intended, designed, tested, verified or certified for commercial deployment, live trading, or production use of any kind. The output of this software should not be used as financial, investment, legal, or tax advice.

ACKNOWLEDGMENT BY USING THE SOFTWARE, USERS ACKNOWLEDGE THAT THEY HAVE READ THIS DISCLAIMER, UNDERSTOOD IT, AND AGREE TO BE BOUND BY ITS TERMS AND CONDITIONS.
