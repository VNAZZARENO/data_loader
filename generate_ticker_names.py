#!/usr/bin/env python3
"""
One-off script to generate jp_names.csv mapping tickers to company names.

Usage:
    python3 data_loader/generate_ticker_names.py

Reads tickers from:
  - data_loader/tickers/jp.csv
  - CustomBacktest/JP/RiTM_x_ATLAS/portfolio_composition.csv (for any extras)

Saves to:
  - data_loader/tickers/jp_names.csv
"""

import os
import sys
import time

import pandas as pd
import yfinance as yf

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)

JP_CSV = os.path.join(SCRIPT_DIR, "tickers", "jp.csv")
COMP_CSV = os.path.join(ROOT_DIR, "CustomBacktest", "JP", "RiTM_x_ATLAS",
                         "portfolio_composition.csv")
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "tickers", "jp_names.csv")


def normalize_ticker(raw: str) -> str:
    """Normalize to 'XXXX JT' format (no ' Equity' suffix)."""
    return raw.replace(" Equity", "").strip()


def ticker_to_yf(ticker: str) -> str:
    """Convert 'XXXX JT' to yfinance format 'XXXX.T'."""
    code = ticker.split()[0]
    return f"{code}.T"


def fetch_name(ticker: str, retries: int = 2) -> str:
    """Fetch shortName from yfinance for a given 'XXXX JT' ticker."""
    yf_code = ticker_to_yf(ticker)
    for attempt in range(retries + 1):
        try:
            info = yf.Ticker(yf_code).info
            name = info.get("shortName") or info.get("longName") or ""
            return name.strip()
        except Exception as e:
            if attempt < retries:
                time.sleep(1)
            else:
                print(f"  WARN: failed for {ticker} ({yf_code}): {e}")
                return ""


def main():
    # Collect all tickers
    tickers = set()

    # From jp.csv
    jp_df = pd.read_csv(JP_CSV)
    for t in jp_df["Ticker"]:
        tickers.add(normalize_ticker(t))

    # From portfolio_composition.csv
    if os.path.exists(COMP_CSV):
        comp_df = pd.read_csv(COMP_CSV)
        for t in comp_df["ticker"].unique():
            tickers.add(normalize_ticker(t))

    tickers = sorted(tickers)
    print(f"Fetching names for {len(tickers)} tickers...")

    # Load existing file to avoid re-fetching known names
    existing = {}
    if os.path.exists(OUTPUT_CSV):
        existing_df = pd.read_csv(OUTPUT_CSV)
        for _, row in existing_df.iterrows():
            if pd.notna(row["Name"]) and row["Name"]:
                existing[row["Ticker"]] = row["Name"]
        print(f"  Loaded {len(existing)} existing names from cache")

    results = []
    for i, ticker in enumerate(tickers, 1):
        if ticker in existing:
            name = existing[ticker]
            print(f"  [{i}/{len(tickers)}] {ticker} -> {name} (cached)")
        else:
            name = fetch_name(ticker)
            print(f"  [{i}/{len(tickers)}] {ticker} -> {name or '???'}")
            time.sleep(0.3)  # rate limit courtesy
        results.append({"Ticker": ticker, "Name": name})

    out_df = pd.DataFrame(results)
    out_df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved {len(results)} entries to {OUTPUT_CSV}")

    # Summary
    found = sum(1 for r in results if r["Name"])
    print(f"Names found: {found}/{len(results)}")


if __name__ == "__main__":
    main()
