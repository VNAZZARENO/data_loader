"""Devise de cotation et conversion EUR. Port de ATLAS ``atlas/data/fx.py`` sans yfinance :
les taux viennent de Bloomberg (``EUR{CCY} Curncy``), meme vendeur et meme close que les prix.
Fail-closed : un taux manquant donne NaN, jamais un prix local stocke comme de l'EUR.
"""

from __future__ import annotations

import pandas as pd

from ..tickers import split_ticker

# Repli quand CRNCY (BDP) est indisponible. SE = SIX Swiss dans la convention projet.
EXCHANGE_TO_CURRENCY = {
    "FP": "EUR", "GY": "EUR", "IM": "EUR", "NA": "EUR", "BB": "EUR", "AV": "EUR", "FH": "EUR",
    "SQ": "EUR", "ID": "EUR", "PL": "EUR", "GR": "EUR", "LN": "GBp", "SS": "SEK", "DC": "DKK",
    "NO": "NOK", "SE": "CHF", "PW": "PLN", "US": "USD", "UN": "USD", "UW": "USD", "UQ": "USD",
    "JT": "JPY", "JP": "JPY", "CN": "CAD", "AT": "AUD", "HK": "HKD",
}
# Cotations en centiemes : CRNCY renvoie le code avec une minuscule finale.
MINOR_UNITS = {"GBp": ("GBP", 100.0), "GBX": ("GBP", 100.0), "ZAr": ("ZAR", 100.0), "ILs": ("ILS", 100.0)}
BASE = "EUR"


def fx_ticker(ccy: str) -> str:
    return f"{BASE}{ccy} Curncy"


def quote_currency(ticker: str, refdata: pd.DataFrame | None = None) -> str | None:
    """Code de cotation brut (peut etre 'GBp')."""
    if refdata is not None and ticker in refdata.index:
        c = refdata.at[ticker, "currency"]
        if isinstance(c, str) and c:
            return c
    bare = ticker[: -len(" Equity")] if ticker.endswith(" Equity") else ticker
    return EXCHANGE_TO_CURRENCY.get(split_ticker(bare)[1])


def major(ccy: str) -> tuple[str, float]:
    """'GBp' -> ('GBP', 100.0) ; 'USD' -> ('USD', 1.0)."""
    return MINOR_UNITS.get(ccy, (ccy.upper(), 1.0))


def required_currencies(tickers, refdata=None) -> list[str]:
    out = {major(c)[0] for c in (quote_currency(t, refdata) for t in tickers) if c}
    return sorted(out - {BASE})


def convert_to_eur(local: pd.DataFrame, fx: pd.DataFrame, refdata=None) -> tuple[pd.DataFrame, list[str]]:
    """Retourne (prix EUR, tickers sans devise ou sans taux). Close du jour, sans decalage."""
    out, missing = {}, []
    for t in local.columns:
        ccy = quote_currency(t, refdata)
        if not ccy:
            missing.append(t)
            out[t] = pd.Series(float("nan"), index=local.index)
            continue
        code, div = major(ccy)
        s = local[t] / div
        if code != BASE:
            if code not in fx.columns:
                missing.append(t)
                out[t] = pd.Series(float("nan"), index=local.index)
                continue
            s = s / fx[code].reindex(local.index)   # pas de ffill : taux absent -> NaN
        out[t] = s
    return pd.DataFrame(out, index=local.index), missing
