# InkBack

A historical back testing framework written in Rust with DataBento and Iced as the main dependencies.
Users can define their own strategies and then run them in parallel and view the results in an Iced window along with the benchmark, which is whatever symbol you are back testing on.

## Requirements
- Rust
    - Written with cargo 1.83.0 and rustc 1.83.0
- Databento API key (set as environment variable `DATABENTO_API_KEY`)
- Cargo
- Internet connection for fetching market data

## Usage

git clone the repo
```https://github.com/Joseph-Matteo-Scorsone/InkBack.git```
Then set your .env file and your DataBento API key in it as DATABENTO_API_KEY.

I designed this so that way users can make thier own struct and then impl Strategy for it. The default that it comes with is an order flow footprint imbalance detector.
That is what you should delete to make your own strategy.

There needs to be an on_candle method to be called by the back tester.
on_candle needs a candle and for convenience accepts the previous candle as well so you have both.

Risk is handled by the user, I didn't want to restrict the back tester to only fixed take profit and stop loss.
In on_candle update your indicators and what not, model your orders, and check for risk.

By design when you request a dataset from DataBento it will check if you already have it, if you don't it will get the data and save it as a csv.
I handle the 9th exponent compression in saving to the csv, it doesn't matter for the back tests.

Multiple DataBento Schemas and datasets are supported by this framework.

It comes as is to support back tests in parallel, users can define windows of parameters, gather every parameter combination and then run tests with each in parallel. Slippage and fees are also calculated per trade. Orders pend, they are not filled on the candle you calculated your edge on's close.

Every run also shows an Iced window with the equity curves for every back test as well as a back test for how just holding the benchmark would do.

# DISCLAIMER

PLEASE READ THIS DISCLAIMER CAREFULLY BEFORE USING THE SOFTWARE. BY ACCESSING OR USING THE SOFTWARE, YOU ACKNOWLEDGE AND AGREE TO BE BOUND BY THE TERMS HEREIN.

This software and related documentation ("Software") are provided solely for educational and research purposes. The Software is not intended, designed, tested, verified or certified for commercial deployment, live trading, or production use of any kind. The output of this software should not be used as financial, investment, legal, or tax advice.

ACKNOWLEDGMENT BY USING THE SOFTWARE, USERS ACKNOWLEDGE THAT THEY HAVE READ THIS DISCLAIMER, UNDERSTOOD IT, AND AGREE TO BE BOUND BY ITS TERMS AND CONDITIONS.
