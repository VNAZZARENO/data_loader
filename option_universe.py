"""Dynamic ticker resolution for the option (IV surface) universes.

The `option_europe` universe used to be a frozen CSV: EU indices plus the PEQ
single stocks that were held for more than 15% of the position-panel window.
That threshold silently dropped names the backtests actually trade (BMW, BNP,
Novartis, Renault...), which shows up downstream as missing implied vol.

Two modes replace the frozen list:

- ``bt``        indices + every name the fund has EVER held, no duration
                threshold, so a backtest never meets a name without IV.
- ``screening`` indices + the fund's CURRENT holdings only, for the daily
                screener: a short, fast pull.

Both read from the GetFundPortfolios API over https, because the loader runs on
the Bloomberg terminal machine rather than the dev box (no access to the local
Flask instance or the mounted shares).

Endpoints used:
    GET /api/fund/<fund>/dates                  -> available position dates
    GET /api/fund/<fund>/holdings[?date=...]    -> holdings (latest if no date)
"""

from __future__ import annotations

import csv
import datetime as dt
import logging
import os
import urllib.request
import json

logger = logging.getLogger(__name__)

DEFAULT_API_BASE_URL = "https://pergam-tools/getfundportfolios-api"
DEFAULT_FUND = "PEQ"
EQUITY_SUFFIX = " Equity"

# Holdings rows we turn into option underlyings. Bonds, cash and funds have no
# single-stock option chain, so they are dropped rather than sent to Bloomberg.
EQUITY_ASSET_TYPES = {"equity", "action", "actions", "stock"}


def _get_json(url: str, timeout: int = 60) -> dict:
    """GET a JSON payload. The internal host uses a self-signed certificate."""
    import ssl

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(url, timeout=timeout, context=ctx) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _holdings_to_tickers(payload: dict) -> set[str]:
    """Extract Bloomberg equity tickers from a holdings payload."""
    out: set[str] = set()
    for row in payload.get("holdings", []) or []:
        if str(row.get("Asset_Type", "")).strip().lower() not in EQUITY_ASSET_TYPES:
            continue
        code = (row.get("Bloomberg Code Clean") or "").strip()
        if not code:
            continue
        out.add(code if code.endswith(EQUITY_SUFFIX) else code + EQUITY_SUFFIX)
    return out


def fetch_current_holdings(fund: str, base_url: str) -> set[str]:
    """Tickers held by `fund` at the latest available date (screening mode)."""
    payload = _get_json(f"{base_url}/api/fund/{fund}/holdings")
    if not payload.get("success"):
        raise RuntimeError(f"holdings request failed for {fund}: {payload.get('error')}")
    tickers = _holdings_to_tickers(payload)
    logger.info(f"Screening: {len(tickers)} equity holdings for {fund} on {payload.get('date')}")
    return tickers


def fetch_all_held_names(fund: str, base_url: str, since: str | None = None) -> set[str]:
    """Union of every equity ticker `fund` has held, across all position dates.

    Walks every available date. The API caches the fund dataframe server-side,
    so the first call costs several seconds and the rest run at ~0.2s each.
    Results are meant to be cached to CSV by the caller: this is a once-off.
    """
    payload = _get_json(f"{base_url}/api/fund/{fund}/dates")
    if not payload.get("success"):
        raise RuntimeError(f"dates request failed for {fund}: {payload.get('error')}")

    dates = sorted(payload.get("available_dates", []))
    if since:
        dates = [d for d in dates if d >= since]
    if not dates:
        raise RuntimeError(f"no position dates for {fund} (since={since})")

    logger.info(f"BT: scanning {len(dates)} position dates for {fund} ({dates[0]} -> {dates[-1]})")
    tickers: set[str] = set()
    failed = 0
    for i, date in enumerate(dates, 1):
        try:
            day = _get_json(f"{base_url}/api/fund/{fund}/holdings?date={date}")
        except Exception as exc:  # network hiccup on one date must not kill the scan
            failed += 1
            logger.warning(f"  {date}: {exc}")
            continue
        if day.get("success"):
            tickers |= _holdings_to_tickers(day)
        if i % 100 == 0 or i == len(dates):
            logger.info(f"  {i}/{len(dates)} dates, {len(tickers)} distinct names so far")

    if failed:
        logger.warning(f"BT: {failed}/{len(dates)} dates failed and were skipped")
    logger.info(f"BT: {len(tickers)} distinct equity names ever held by {fund}")
    return tickers


def read_ticker_csv(path: str) -> list[str]:
    """Read a `Ticker`-column CSV (same format as tickers/<universe>.csv)."""
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if "Ticker" not in (reader.fieldnames or []):
            raise ValueError(f"Ticker file missing 'Ticker' column: {path}")
        return [r["Ticker"].strip() for r in reader if r["Ticker"].strip()]


def write_ticker_csv(path: str, tickers: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Ticker"])
        for t in tickers:
            w.writerow([t])


def resolve_option_tickers(
    mode: str,
    base_csv_path: str,
    cache_csv_path: str,
    fund: str = DEFAULT_FUND,
    base_url: str = DEFAULT_API_BASE_URL,
    since: str | None = None,
    refresh: bool = False,
) -> list[str]:
    """Resolve the ticker list for an option universe in the given mode.

    mode='static'    : the frozen CSV, unchanged legacy behaviour.
    mode='bt'        : indices from the frozen CSV + every name ever held.
                       Cached to `cache_csv_path`; pass refresh=True to rebuild.
    mode='screening' : indices from the frozen CSV + current holdings only.
                       Never cached, the point is that it is current.
    """
    base = read_ticker_csv(base_csv_path)
    if mode == "static":
        return base

    # Indices carry the listed-option chains used as hedging references and are
    # not part of any fund position file, so they are always kept.
    indices = [t for t in base if not t.endswith(EQUITY_SUFFIX)]

    if mode == "screening":
        held = fetch_current_holdings(fund, base_url)
        tickers = indices + sorted(held)
        logger.info(f"Screening universe: {len(indices)} indices + {len(held)} holdings = {len(tickers)}")
        return tickers

    if mode == "bt":
        if os.path.isfile(cache_csv_path) and not refresh:
            tickers = read_ticker_csv(cache_csv_path)
            logger.info(f"BT universe from cache {os.path.basename(cache_csv_path)}: {len(tickers)} tickers")
            return tickers
        ever_held = fetch_all_held_names(fund, base_url, since=since)
        base_equities = {t for t in base if t.endswith(EQUITY_SUFFIX)}
        added = sorted(ever_held - base_equities)
        tickers = indices + sorted(base_equities | ever_held)
        write_ticker_csv(cache_csv_path, tickers)
        logger.info(
            f"BT universe: {len(indices)} indices + {len(base_equities | ever_held)} equities "
            f"= {len(tickers)} tickers ({len(added)} new vs the static list)"
        )
        if added:
            logger.info(f"  added: {', '.join(added[:20])}{' ...' if len(added) > 20 else ''}")
        logger.info(f"  cached to {cache_csv_path}")
        return tickers

    raise ValueError(f"Unknown option universe mode '{mode}' (static, bt, screening)")


def screening_start_date(months: int = 12) -> str:
    """Default start date for screening mode: a rolling window, not full history."""
    return (dt.date.today() - dt.timedelta(days=int(months * 30.44))).isoformat()
