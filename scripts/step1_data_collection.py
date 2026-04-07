import os
from pathlib import Path
import pandas as pd
import yfinance as yf
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

FRED_API_KEY = os.getenv("FRED_API_KEY")

START_DATE = "2019-01-01"
END_DATE = "2026-04-01"

TICKERS = ["SPY", "QQQ", "AAPL", "MSFT"]
FRED_SERIES = {
    "DGS10": "10Y_Treasury_Yield",
    "CPIAUCSL": "CPI_All_Urban_Consumers",
    "FEDFUNDS": "Fed_Funds_Rate"
}

def download_market_data(tickers, start_date, end_date):
    all_data = []

    for ticker in tickers:
        df = yf.Ticker(ticker).history(
            start=start_date,
            end=end_date,
            auto_adjust=True
        )

        if df.empty:
            print(f"Warning: no data returned for {ticker}")
            continue

        df = df.reset_index()

        # Keep only needed columns if they exist
        keep_cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
        df = df[[col for col in keep_cols if col in df.columns]].copy()

        df["Ticker"] = ticker

        # Reorder columns
        ordered_cols = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
        df = df[[col for col in ordered_cols if col in df.columns]]

        all_data.append(df)

    if not all_data:
        raise ValueError("No market data downloaded.")

    market_df = pd.concat(all_data, ignore_index=True)
    market_df["Date"] = pd.to_datetime(market_df["Date"])
    market_df = market_df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    return market_df

def download_fred_series(series_id, series_name, api_key, start_date, end_date):
    if not api_key:
        raise ValueError("Missing FRED_API_KEY in environment variables.")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()["observations"]
    df = pd.DataFrame(data)

    df = df[["date", "value"]].copy()
    df.columns = ["Date", "Value"]
    df["Date"] = pd.to_datetime(df["Date"])
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    df["Series_ID"] = series_id
    df["Series_Name"] = series_name

    return df

def build_macro_dataset(series_dict, api_key, start_date, end_date):
    frames = []

    for series_id, series_name in series_dict.items():
        df = download_fred_series(
            series_id=series_id,
            series_name=series_name,
            api_key=api_key,
            start_date=start_date,
            end_date=end_date
        )
        frames.append(df)

    macro_df = pd.concat(frames, ignore_index=True)
    macro_df = macro_df.sort_values(["Series_ID", "Date"]).reset_index(drop=True)
    return macro_df

def main():
    print("Downloading market data...")
    market_df = download_market_data(TICKERS, START_DATE, END_DATE)
    market_path = RAW_DIR / "market_prices_raw.csv"
    market_df.to_csv(market_path, index=False)
    print(f"Saved market data to: {market_path}")

    print("Downloading macro data from FRED...")
    macro_df = build_macro_dataset(FRED_SERIES, FRED_API_KEY, START_DATE, END_DATE)
    macro_path = RAW_DIR / "macro_data_raw.csv"
    macro_df.to_csv(macro_path, index=False)
    print(f"Saved macro data to: {macro_path}")

    print("\nDone.")

    print("\nMarket data columns:")
    print(market_df.columns.tolist())
    print("\nMarket data preview:")
    print(market_df.head())

    print("\nMacro data preview:")
    print(macro_df.head())

if __name__ == "__main__":
    main()
