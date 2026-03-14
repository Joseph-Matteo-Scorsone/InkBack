use crate::backtester::{run_backtest, run_parallel_backtest_internal, BacktestResult, Trade};
use crate::plot::plot_equity_curves;
use crate::slippage_models::TransactionCosts;
use crate::strategy::{Strategy, StrategyParams};
use crate::utils::fetch::BacktestManager;
use crate::InkBackSchema;
use databento::dbn::Schema;
use serde::{Deserialize, Serialize};

/// Configuration for rolling walk forward optimisation.
pub struct WalkForwardConfig {
    /// Number of windows to slice the date range into.
    pub n_windows: usize,
    /// Fraction of each window used for in-sample optimisation (e.g. 0.7).
    pub is_fraction: f64,
    /// Overall start timestamp in nanoseconds.
    pub start_ts: u64,
    /// Overall end timestamp in nanoseconds (exclusive).
    pub end_ts: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WalkForwardWindowResult {
    pub window: usize,
    pub is_start_ts: u64,
    pub is_end_ts: u64,
    pub oos_start_ts: u64,
    pub oos_end_ts: u64,
    /// Label of the best IS parameter set.
    pub best_params: String,
    pub is_sharpe: f64,
    pub is_return_pct: f64,
    pub oos_result: BacktestResult,
}

pub struct WalkForwardSummary {
    pub windows: Vec<WalkForwardWindowResult>,
    /// Combined OOS equity curve (windows chained).
    pub combined_oos_equity: Vec<f64>,
    /// Aggregate metrics over all OOS trades.
    pub combined_result: BacktestResult,
}

/// Rolling walk-forward optimisation.
///
/// For each window:
///   1. Optimise all parameter combinations on the IS period (ranked by Sharpe).
///   2. Run the best IS params on the OOS period with equity carried forward.
///
/// Returns a [`WalkForwardSummary`] with per-window detail and a chained OOS equity curve.
pub async fn run_walk_forward<F>(
    config: WalkForwardConfig,
    parameter_combinations: Vec<StrategyParams>,
    backtest_manager: BacktestManager,
    symbol: &str,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    strategy_constructor: F,
    starting_equity: f64,
    exposure: f64,
    transaction_costs: TransactionCosts,
) -> WalkForwardSummary
where
    F: Fn(&StrategyParams) -> anyhow::Result<Box<dyn Strategy>> + Sync + Send,
{
    let total_ns = config.end_ts.saturating_sub(config.start_ts);
    let window_ns = total_ns / config.n_windows as u64;
    let is_ns = (window_ns as f64 * config.is_fraction) as u64;

    let mut window_results: Vec<WalkForwardWindowResult> = Vec::new();
    let mut all_oos_trades: Vec<Trade> = Vec::new();
    let mut combined_equity: Vec<f64> = vec![starting_equity];
    let mut current_equity = starting_equity;

    println!(
        "\n=== WALK-FORWARD OPTIMISATION ({} windows, {:.0}% IS / {:.0}% OOS) ===",
        config.n_windows,
        config.is_fraction * 100.0,
        (1.0 - config.is_fraction) * 100.0,
    );

    for w in 0..config.n_windows {
        let window_start = config.start_ts + w as u64 * window_ns;
        let window_end = window_start + window_ns;
        let is_end = window_start + is_ns;
        let oos_start = is_end;
        let oos_end = window_end;

        println!(
            "\nWindow {}/{}  IS [{} → {}]  OOS [{} → {}]",
            w + 1,
            config.n_windows,
            window_start,
            is_end,
            oos_start,
            oos_end
        );

        // IS: parallel optimisation (sorted by Sharpe internally)
        let is_results = run_parallel_backtest_internal(
            &parameter_combinations,
            &backtest_manager,
            symbol,
            schema,
            custom_schema.clone(),
            &strategy_constructor,
            starting_equity,
            exposure,
            &transaction_costs,
            Some((window_start, is_end)),
        );

        if is_results.is_empty() {
            println!("  No IS results — skipping window.");
            continue;
        }

        let (best_label, best_params, best_is_result, _) = &is_results[0];
        println!(
            "  Best IS: {} | Sharpe {:.2} | Return {:.2}%",
            best_label, best_is_result.sharpe_ratio, best_is_result.total_return_pct
        );

        // OOS: single run with best IS params, equity carried forward
        let oos_result = match strategy_constructor(best_params) {
            Ok(mut strategy) => run_backtest(
                symbol,
                backtest_manager.clone(),
                strategy.as_mut(),
                transaction_costs.clone(),
                current_equity,
                exposure,
                schema,
                custom_schema.clone(),
                Some((oos_start, oos_end)),
            )
            .await
            .ok(),
            Err(_) => None,
        };

        let oos_result = match oos_result {
            Some(r) => r,
            None => {
                println!("  OOS run failed — skipping window.");
                continue;
            }
        };

        println!(
            "  OOS: Return {:.2}% | Sharpe {:.2} | Sortino {:.2} | Trades {}",
            oos_result.total_return_pct,
            oos_result.sharpe_ratio,
            oos_result.sortino_ratio,
            oos_result.total_trades
        );

        // Chain equity and accumulate trades
        current_equity = oos_result.ending_equity;
        if oos_result.equity_curve.len() > 1 {
            combined_equity.extend_from_slice(&oos_result.equity_curve[1..]);
        }
        all_oos_trades.extend(oos_result.trades.clone());

        window_results.push(WalkForwardWindowResult {
            window: w + 1,
            is_start_ts: window_start,
            is_end_ts: is_end,
            oos_start_ts: oos_start,
            oos_end_ts: oos_end,
            best_params: best_label.clone(),
            is_sharpe: best_is_result.sharpe_ratio,
            is_return_pct: best_is_result.total_return_pct,
            oos_result,
        });
    }

    let combined_result = BacktestResult::calculate_metrics(
        starting_equity,
        current_equity,
        combined_equity.clone(),
        all_oos_trades,
    );

    WalkForwardSummary {
        windows: window_results,
        combined_oos_equity: combined_equity,
        combined_result,
    }
}

pub fn display_walk_forward_results(summary: &WalkForwardSummary) {
    println!("\n=== WALK-FORWARD RESULTS ===");
    println!(
        "{:<8} {:<12} {:<10} {:<10} {:<10} {:<10} {:<8}",
        "Window", "Best Params", "OOS Ret%", "OOS DD%", "Sharpe", "Sortino", "Trades"
    );
    println!("{}", "-".repeat(72));

    for w in &summary.windows {
        // Truncate param label to fit
        let label = if w.best_params.len() > 10 {
            format!("{}…", &w.best_params[..9])
        } else {
            w.best_params.clone()
        };
        println!(
            "{:<8} {:<12} {:<10.2} {:<10.2} {:<10.2} {:<10.2} {:<8}",
            w.window,
            label,
            w.oos_result.total_return_pct,
            w.oos_result.max_drawdown_pct,
            w.oos_result.sharpe_ratio,
            w.oos_result.sortino_ratio,
            w.oos_result.total_trades,
        );
    }

    println!("{}", "-".repeat(72));
    println!(
        "Combined OOS | Ret: {:.2}% | DD: {:.2}% | Sharpe: {:.2} | Sortino: {:.2} | Calmar: {:.2} | Trades: {}",
        summary.combined_result.total_return_pct,
        summary.combined_result.max_drawdown_pct,
        summary.combined_result.sharpe_ratio,
        summary.combined_result.sortino_ratio,
        summary.combined_result.calmar_ratio,
        summary.combined_result.total_trades,
    );
}

/// Plot the combined OOS equity curve from a walk-forward run.
pub fn plot_walk_forward(summary: &WalkForwardSummary) {
    // Per-window OOS curves
    let mut curves: Vec<(String, Vec<f64>)> = summary
        .windows
        .iter()
        .map(|w| {
            (
                format!("Window {} OOS", w.window),
                w.oos_result.equity_curve.clone(),
            )
        })
        .collect();

    // Add combined curve
    curves.push((
        "Combined OOS".to_string(),
        summary.combined_oos_equity.clone(),
    ));

    plot_equity_curves(curves, None);
}
