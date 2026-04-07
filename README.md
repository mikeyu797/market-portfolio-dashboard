# Market & Portfolio Analytics Dashboard

This project analyzes stock and ETF performance alongside macroeconomic indicators using Python.

## Tools
- Python
- pandas
- yfinance
- FRED API
- Power BI

## Assets Analyzed
- SPY
- QQQ
- AAPL
- MSFT

## Macroeconomic Indicators
- CPIAUCSL (Inflation Index)
- DGS10 (10-Year Treasury Yield)
- FEDFUNDS (Fed Funds Rate)

## Outputs
### Raw Data
- data/raw/market_prices_raw.csv
- data/raw/macro_data_raw.csv

### Processed Data
- data/processed/market_returns_processed.csv
- data/processed/portfolio_summary_metrics.csv
- data/processed/macro_data_wide.csv
- data/processed/market_macro_merged.csv

## Metrics Calculated
- Daily Return
- Cumulative Return
- Annualized Return
- Annualized Volatility
- Sharpe Ratio

## Business Goal
To evaluate portfolio performance, compare risk-adjusted returns, and explore how macroeconomic conditions relate to market behavior.
