# InkBack by Scorsone Enterprises

A high performance historical backtesting framework written in Rust, designed for quantitative trading strategy development and analysis. Built with DataBento for market data and egui for visualization.

## Features

- **Multi-asset Support**: Equities, futures, and options backtesting
- **Custom Strategy Development**: Implement your own trading strategies using the `Strategy` trait
- **Parallel Optimization**: Run parameter sweeps concurrently across all CPU cores with Rayon
- **Walk-Forward Optimization**: Rolling IS/OOS windows to validate strategies on unseen data
- **Risk-Adjusted Metrics**: Sharpe, Sortino, Calmar ratios computed per backtest
- **Interactive Visualization**: Equity curve plotting with egui: zoom, pan, toggle curves
- **Order Flow Analysis**: Built in footprint bar construction from tick data
- **Realistic Trading Costs**: Slippage models, commissions, bid ask spread, and fill price adjustment
- **Data Management**: Automatic DataBento data fetching, caching, and ZSTD compression
- **Options + Underlying**: Synchronized options trades with underlying bid/ask quotes via k-way merge

## Architecture

```
src/
├── main.rs              # Entry point, example strategy (MovingAverageCross)
├── event.rs             # MarketEvent enum (Trade, Mbp1, Ohlcv, Mbo, Footprint, OptionTrade, Definition)
├── strategy.rs          # Strategy trait, Order, OrderType, StrategyParams
├── backtester.rs        # Core backtest engine, parallel optimization, metrics
├── walkforward.rs       # Rolling walk-forward optimization
├── slippage_models.rs   # Commission, slippage, and spread models
├── plot.rs              # egui equity curve plotter
└── utils/
    └── fetch.rs         # DataBento fetching, caching, footprint processing, options merge
```

### Module Responsibilities

| Module | Responsibility |
|---|---|
| `strategy` | Define `Strategy` trait; implement `on_event` to return orders |
| `backtester` | Stream events, fill orders, track equity, compute all metrics |
| `walkforward` | Slice date range into IS/OOS windows, optimize IS, validate OOS |
| `slippage_models` | Configurable cost models per asset class |
| `utils/fetch` | Download from DataBento, cache locally, build footprint CSVs, merge options streams |
| `plot` | Immediate-mode GUI via `eframe`/`egui_plot` |

## Prerequisites

- **Rust**: 1.83.0 or higher
- **DataBento API Key**: Required for data downloads (cached locally after first fetch)
- **Dependencies**: Managed via Cargo

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/Joseph-Matteo-Scorsone/InkBack.git
cd InkBack
```

### 2. Configure Environment

```env
# .env
DATABENTO_API_KEY=your_databento_api_key_here
```

### 3. Run

```bash
cargo run --release
```

Data is cached in `src/data/` after the first download. Subsequent runs skip the API call.

## Creating Custom Strategies

Implement the `Strategy` trait — one method, called on every market event:

```rust
use inkback::strategy::{Strategy, Order, OrderType, StrategyParams};
use inkback::event::MarketEvent;

pub struct MyStrategy {
    threshold: f64,
}

impl MyStrategy {
    pub fn new(params: &StrategyParams) -> anyhow::Result<Self> {
        Ok(Self {
            threshold: params.get("threshold").unwrap_or(0.01),
        })
    }
}

impl Strategy for MyStrategy {
    fn on_event(&mut self, event: &MarketEvent, prev: Option<&MarketEvent>) -> Option<Order> {
        let price = event.price();
        let prev_price = prev?.price();

        if price > prev_price * (1.0 + self.threshold) {
            return Some(Order { order_type: OrderType::MarketBuy, price });
        }
        if price < prev_price * (1.0 - self.threshold) {
            return Some(Order { order_type: OrderType::MarketSell, price });
        }

        None
    }
}
```

### MarketEvent API

```rust
event.price()        // Close / last trade price (scaled from fixed-point)
event.volume()       // Volume as u64
event.high()         // High price (OHLCV); falls back to price() for tick data
event.low()          // Low price (OHLCV); falls back to price() for tick data
event.timestamp()    // ts_event as u64 nanoseconds
event.date_string()  // "YYYY-MM-DD" string for logging

// String fields
event.get_string("footprint_data")  // JSON bid/ask volume at each price level
event.get_string("option_type")     // "C" or "P"
event.get_string("symbol")          // Option symbol

// Numeric fields
event.get("underlying_bid")         // Best bid of the underlying
event.get("underlying_ask")         // Best ask of the underlying
event.get("strike_price")           // Option strike
```

## Running a Backtest

### Single Backtest

```rust
let result = run_backtest(
    symbol,
    backtest_manager.clone(),
    &mut strategy,
    transaction_costs.clone(),
    starting_equity,
    exposure,       // Fraction of equity allocated per trade (e.g. 0.50)
    schema,
    None,           // custom_schema
    None,           // custom interval
).await?;
```

### Parallel Parameter Optimization

```rust
// Build all parameter combinations
let mut combinations = Vec::new();
for period in [10, 20, 50] {
    let mut p = StrategyParams::new();
    p.insert("period", period as f64);
    combinations.push(p);
}

// Runs all combinations in parallel, sorted by Sharpe ratio
let results = run_parallel_backtest(
    combinations,
    backtest_manager,
    symbol,
    schema,
    None,
    |params| Ok(Box::new(MyStrategy::new(params)?)),
    starting_equity,
    exposure,
    transaction_costs,
);
```

### Walk-Forward Optimization

Walk forward splits the date range into `n_windows` rolling windows. Each window uses `is_fraction` of its span for in sample optimization (ranked by Sharpe) and runs the best parameters on the out of sample period, carrying equity forward.

```rust
let wf_summary = run_walk_forward(
    WalkForwardConfig {
        n_windows: 4,
        is_fraction: 0.70,   // 70% IS, 30% OOS per window
        start_ts,
        end_ts,
    },
    parameter_combinations,
    backtest_manager,
    symbol,
    schema,
    None,
    |params| Ok(Box::new(MyStrategy::new(params)?)),
    starting_equity,
    exposure,
    transaction_costs,
).await;

display_walk_forward_results(&wf_summary);
plot_walk_forward(&wf_summary);  // Opens egui window
```

## Transaction Cost Models

### Prebuilt Configurations

```rust
// Equities   zero commission, 2 bps slippage
let costs = TransactionCosts::equity_trading();

// Futures   $2.50 commission, 1-tick slippage
let costs = TransactionCosts::futures_trading(0.25); // tick size for ES

// Options   $0.65/contract commission, options specific slippage
let costs = TransactionCosts::options_trading();
```

### Custom Configuration

```rust
use inkback::slippage_models::{TransactionCosts, CommissionModel, SlippageModel, SpreadModel};

let costs = TransactionCosts {
    commission: CommissionModel::PerShare(0.005),
    slippage: SlippageModel::SquareRoot(5.0),   // sqrt market impact
    spread: SpreadModel::Percentage(0.01),       // 1 bp half-spread
};
```

### Available Models

**Commission**: `Fixed`, `PerShare`, `Percentage`, `Tiered`

**Slippage**: `Fixed` (bps), `Linear` (size-linear), `SquareRoot` (sqrt impact), `TickBased`, `MarketImpact` (permanent + temporary), `OptionsSlippage`

**Spread**: `Fixed`, `Percentage`, `TimeDependent`, `OptionsBidAsk`

## Data Sources

InkBack fetches from DataBento and caches as `.zst` or `.csv` in `src/data/`.

| Schema / Custom Schema | Description | File Format |
|---|---|---|
| `Schema::Ohlcv1H` / `1M` / `1S` / `1D` | OHLCV bars | `.zst` |
| `Schema::Trades` | Tick-by-tick trades | `.zst` |
| `Schema::Mbp1` | Top-of-book quotes | `.zst` |
| `Schema::Mbo` | Full order book | `.zst` |
| `InkBackSchema::FootPrint` | Footprint bars (bid/ask volume per price) | `.csv` built from trades |
| `InkBackSchema::CombinedOptionsUnderlying` | Options trades + synchronized underlying quotes | `.csv` built from k-way merge |

### Fetching Data

```rust
let manager = fetch_and_save_data(
    "GLBX.MDP3",        // DataBento dataset
    SType::Continuous,  // Symbol type
    "NQ.v.0",           // Symbol
    None,               // option_symbol (required for CombinedOptionsUnderlying)
    Schema::Ohlcv1H,
    None,               // custom_schema
    start,
    end,
    None,               // bar_interval_ns (only used for FootPrint bars, default 15s)
).await?;
```

## Performance Metrics

Every `BacktestResult` includes:

| Metric | Description |
|---|---|
| `total_return_pct` | Net return over the test period |
| `max_drawdown_pct` | Peak-to-trough equity decline |
| `sharpe_ratio` | Mean trade return / std of trade returns |
| `sortino_ratio` | Mean trade return / downside semi-std |
| `calmar_ratio` | Total return % / max drawdown % |
| `win_rate` | % of trades closed profitably |
| `profit_factor` | Gross profit / gross loss |
| `avg_win` / `avg_loss` | Average P&L per winning/losing trade |
| `largest_win` / `largest_loss` | Extremes |
| `total_transaction_costs` | Cumulative fees and slippage |
| `equity_curve` | Full equity series |
| `trades` | Complete trade log |

## Futures Multipliers

The backtester automatically applies contract multipliers based on symbol prefix:

| Symbol Prefix | Multiplier | Notes |
|---|---|---|
| `NQ` | 5.0 | $5/point |
| `ES` | 12.5 | $12.50/point |
| `YM` | 5.0 | $5/point |
| `CL` | 10.0 | $10/point |
| `GC` | 10.0 | $10/point |
| `SI` | 25.0 | $25/point |

Futures are detected by symbol suffix `.v.0`, `.c.0`, or `.FUT`.

## Output

The `plot_walk_forward` and `plot_equity_curves` functions open an interactive egui window with:
- Per-strategy / per-window equity curves
- Side panel with toggleable curve visibility
- Built-in zoom, pan, and legend

![alt text](https://pbs.twimg.com/media/HDZ_t8cWoAMsxEz?format=jpg&name=small)

## License

MIT License — see the LICENSE file for details.

---

# DISCLAIMER

PLEASE READ THIS DISCLAIMER CAREFULLY BEFORE USING THE SOFTWARE. BY ACCESSING OR USING THE SOFTWARE, YOU ACKNOWLEDGE AND AGREE TO BE BOUND BY THE TERMS HEREIN.

This software and related documentation ("Software") are provided solely for educational and research purposes. The Software is not intended, designed, tested, verified or certified for commercial deployment, live trading, or production use of any kind. The output of this software should not be used as financial, investment, legal, or tax advice.

ACKNOWLEDGMENT: BY USING THE SOFTWARE, USERS ACKNOWLEDGE THAT THEY HAVE READ THIS DISCLAIMER, UNDERSTOOD IT, AND AGREE TO BE BOUND BY ITS TERMS AND CONDITIONS.
