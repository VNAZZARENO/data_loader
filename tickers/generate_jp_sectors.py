#!/usr/bin/env python3
"""
One-time script to fetch GICS sectors for JP universe tickers via yfinance.

Reads jp_names.csv ("1605 JT", ...) and writes jp_sectors.csv (Ticker, Sector).
"""

import os
import time

import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
NAMES_PATH = os.path.join(HERE, "jp_names.csv")
OUT_PATH = os.path.join(HERE, "jp_sectors.csv")
SLEEP = 0.2  # seconds between requests


def jt_to_yf(ticker: str) -> str:
    """Convert '7203 JT' -> '7203.T' for yfinance."""
    code = ticker.strip().split()[0]
    return f"{code}.T"


def main():
    names = pd.read_csv(NAMES_PATH)
    print(f"Loaded {len(names)} tickers from jp_names.csv")

    rows = []
    for _, row in names.iterrows():
        ticker_jt = row["Ticker"]
        yf_ticker = jt_to_yf(ticker_jt)
        try:
            info = yf.Ticker(yf_ticker).info
            sector = info.get("sector", "")
            print(f"  [OK]  {ticker_jt:>10s}  ->  {yf_ticker:>8s}  |  {sector}")
        except Exception as e:
            sector = ""
            print(f"  [ERR] {ticker_jt:>10s}  ->  {yf_ticker:>8s}  |  {e}")
        rows.append({"Ticker": ticker_jt, "Sector": sector})
        time.sleep(SLEEP)

    out = pd.DataFrame(rows)
    out.to_csv(OUT_PATH, index=False)
    filled = out["Sector"].astype(bool).sum()
    print(f"\nWrote {OUT_PATH}  ({filled}/{len(out)} sectors filled)")


if __name__ == "__main__":
    main()
