#!/usr/bin/env python3
"""Probe which Bloomberg moneyness implied-vol mnemonics actually return data.

The option_europe surface only partially populated on the first fetch:
30DAY at 90%/95% moneyness worked, but the 80% pillar and the 90/180/360-day
tenors came back empty -> the tenor/moneyness tokens are wrong for those.

This script discovers the valid vocabulary cheaply: it probes ONE liquid
ticker over a short window, varying one axis at a time around the known-good
field `30DAY_IMPVOL_90.0%MNY_DF`. Run it on the Bloomberg terminal machine:

    source .venv/bin/activate && python3 probe_iv_fields.py
    python3 probe_iv_fields.py --ticker "OR FP Equity"   # cross-check a stock

Paste the OK/empty tables back and we lock the config to the survivors.
"""
import argparse
import datetime as dt

import pandas as pd
from xbbg import blp

# Axis vocabularies to probe. We hold one axis at a known-good value and sweep
# the other, so we learn the valid tokens without a full cross-product.
TENOR_TOKENS = [
    "10DAY", "20DAY", "30DAY", "40DAY", "50DAY", "60DAY", "90DAY",
    "120DAY", "150DAY", "180DAY", "270DAY", "360DAY",
    "1MTH", "2MTH", "3MTH", "6MTH", "9MTH", "12MTH", "18MTH", "24MTH",
    "1MO", "2MO", "3MO", "6MO", "12MO", "24MO",
]
MONEYNESS_TOKENS = [
    "60.0", "70.0", "75.0", "80.0", "85.0", "90.0", "95.0",
    "97.5", "100.0", "102.5", "105.0", "110.0", "120.0",
]
SUFFIXES = ["MNY_DF", "MNY_CF", "MNY"]  # DF=downside, CF=call/forward conv.


def probe(ticker: str, fields: list[str], start: str, end: str) -> pd.DataFrame:
    """Return one row per field: populated? + last value, querying individually."""
    rows = []
    for fld in fields:
        ok, last = False, None
        try:
            df = blp.bdh(ticker, fld, start, end)
            if df is not None and not df.empty:
                ser = df.iloc[:, 0].dropna()
                if len(ser):
                    ok, last = True, round(float(ser.iloc[-1]), 4)
        except Exception as e:  # invalid mnemonic / no data
            last = f"ERR:{type(e).__name__}"
        rows.append({"field": fld, "ok": ok, "last": last})
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default="SX5E Index")
    ap.add_argument("--days", type=int, default=30, help="lookback window")
    args = ap.parse_args()

    end = dt.date.today()
    start = (end - dt.timedelta(days=args.days)).isoformat()
    end = end.isoformat()
    print(f"Probing {args.ticker}  window {start} -> {end}\n")

    # Axis 1: tenor sweep at fixed 90.0% MNY_DF (known good pillar)
    tenor_fields = [f"{t}_IMPVOL_90.0%MNY_DF" for t in TENOR_TOKENS]
    t = probe(args.ticker, tenor_fields, start, end)
    print("=== TENOR sweep @ 90.0%MNY_DF ===")
    print(t.to_string(index=False))
    print("\nVALID tenor tokens:",
          [f.split("_")[0] for f in t[t.ok].field], "\n")

    # Axis 2: moneyness + suffix sweep at fixed 30DAY (known good tenor)
    mny_fields = [f"30DAY_IMPVOL_{m}%{s}" for s in SUFFIXES for m in MONEYNESS_TOKENS]
    m = probe(args.ticker, mny_fields, start, end)
    print("=== MONEYNESS/suffix sweep @ 30DAY ===")
    print(m[m.ok].to_string(index=False))
    print("\nVALID moneyness fields (30DAY):", list(m[m.ok].field))

    out = pd.concat([t, m], ignore_index=True)
    out.to_csv("probe_iv_fields_result.csv", index=False)
    print("\nWrote probe_iv_fields_result.csv")


if __name__ == "__main__":
    main()
