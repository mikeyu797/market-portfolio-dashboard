from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

MARKET_FILE = RAW_DIR / "market_prices_raw.csv"
MACRO_FILE = RAW_DIR / "macro_data_raw.csv"

RISK_FREE_RATE = 0.02

def load_data():
    market_df = pd.read_csv(MARKET_FILE, parse_dates=["Date"])
    macro_df = pd.read_csv(MACRO_FILE, parse_dates=["Date"])
    return market_df, macro_df

def prepare_market_data(market_df):
    market_df = market_df.copy()

    if "Close" not in market_df.columns:
        raise ValueError(f"'Close' column missing. Columns found: {list(market_df.columns)}")

    market_df["Date"] = pd.to_datetime(market_df["Date"], utc=True).dt.tz_localize(None).dt.normalize()
    market_df = market_df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    market_df["Daily_Return"] = market_df.groupby("Ticker")["Close"].pct_change(fill_method=None)
    market_df["Cumulative_Return"] = (
        1 + market_df["Daily_Return"]
    ).groupby(market_df["Ticker"]).cumprod() - 1

    return market_df

def create_summary_metrics(market_df):
    summary = (
        market_df.groupby("Ticker")
        .agg(
            Start_Date=("Date", "min"),
            End_Date=("Date", "max"),
            Start_Price=("Close", "first"),
            End_Price=("Close", "last"),
            Avg_Daily_Return=("Daily_Return", "mean"),
            Volatility_Daily=("Daily_Return", "std"),
        )
        .reset_index()
    )

    summary["Total_Return"] = (summary["End_Price"] / summary["Start_Price"]) - 1
    summary["Volatility_Annualized"] = summary["Volatility_Daily"] * np.sqrt(252)
    summary["Return_Annualized"] = (1 + summary["Avg_Daily_Return"]) ** 252 - 1
    summary["Sharpe_Ratio"] = (
        summary["Return_Annualized"] - RISK_FREE_RATE
    ) / summary["Volatility_Annualized"]

    summary = summary.round(4)
    return summary

def prepare_macro_wide(macro_df):
    macro_df = macro_df.copy()
    macro_df["Date"] = pd.to_datetime(macro_df["Date"]).dt.tz_localize(None).dt.normalize()

    macro_wide = (
        macro_df.pivot(index="Date", columns="Series_ID", values="Value")
        .reset_index()
        .sort_values("Date")
    )

    macro_wide = macro_wide.sort_values("Date").ffill()
    return macro_wide

def merge_market_macro(market_df, macro_wide):
    merged = pd.merge(
        market_df,
        macro_wide,
        on="Date",
        how="left"
    )

    merged = merged.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    macro_cols = [col for col in ["CPIAUCSL", "DGS10", "FEDFUNDS"] if col in merged.columns]
    if macro_cols:
        merged[macro_cols] = merged[macro_cols].ffill()

    if "CPIAUCSL" in merged.columns:
        merged["Inflation_YoY"] = merged["CPIAUCSL"].pct_change(252, fill_method=None)

    return merged

def main():
    market_df, macro_df = load_data()

    market_df = prepare_market_data(market_df)
    summary_df = create_summary_metrics(market_df)
    macro_wide = prepare_macro_wide(macro_df)
    merged_df = merge_market_macro(market_df, macro_wide)

    market_output = PROCESSED_DIR / "market_returns_processed.csv"
    summary_output = PROCESSED_DIR / "portfolio_summary_metrics.csv"
    macro_output = PROCESSED_DIR / "macro_data_wide.csv"
    merged_output = PROCESSED_DIR / "market_macro_merged.csv"

    market_df.to_csv(market_output, index=False)
    summary_df.to_csv(summary_output, index=False)
    macro_wide.to_csv(macro_output, index=False)
    merged_df.to_csv(merged_output, index=False)

    print("Saved processed market data to:", market_output)
    print("Saved summary metrics to:", summary_output)
    print("Saved macro wide data to:", macro_output)
    print("Saved merged dataset to:", merged_output)

    print("\nSummary metrics preview:")
    print(summary_df)

if __name__ == "__main__":
    main()
