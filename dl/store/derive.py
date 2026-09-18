"""Couches derivees, deterministes : raw -> fx_eur -> clean. Pur pandas + E/S du store."""

from __future__ import annotations

import logging

import pandas as pd

from . import fx as fxmod
from . import layout, reader, writer

logger = logging.getLogger(__name__)
CLEAN_VERSION = 1
HOLIDAY_ZERO_FRACTION = 0.9
STALE_TAIL_DAYS = 10
DEFAULT_CURRENCY_FIELDS = ("price", "open", "high", "low", "EPS", "best_eps")


def ffilled_holidays(prices: pd.DataFrame, zero_fraction: float = HOLIDAY_ZERO_FRACTION) -> pd.DatetimeIndex:
    """Dates ou >= ``zero_fraction`` des tickers cotes ont un rendement exactement nul
    (ferie recopie par BDH Calendar=5D + Fill=P). Meme regle que ATLAS ``drop_ffilled_holidays``."""
    if prices.empty:
        return pd.DatetimeIndex([])
    returns = prices.pct_change(fill_method=None)
    quoted = returns.notna().sum(axis=1)
    flat = (returns.abs() < 1e-12).sum(axis=1)
    is_holiday = (quoted > 0) & (flat / quoted.where(quoted > 0) >= zero_fraction)
    is_holiday.iloc[0] = False
    return prices.index[is_holiday]


def stale_tail_mask(prices: pd.DataFrame, min_days: int = STALE_TAIL_DAYS) -> pd.DataFrame:
    """True sur la queue figee d'un titre mort : prix constant jusqu'a la fin, >= ``min_days`` jours."""
    mask = pd.DataFrame(False, index=prices.index, columns=prices.columns)
    for t in prices.columns:
        s = prices[t].dropna()
        if len(s) <= min_days:
            continue
        moved = s.diff().abs() > 0
        last_move = moved[moved].index.max() if moved.any() else s.index[0]
        tail = s.index[s.index > last_move]
        if len(tail) >= min_days:
            mask.loc[tail, t] = True
    return mask


def clean_frame(df: pd.DataFrame, holidays: pd.DatetimeIndex, stale: pd.DataFrame | None) -> pd.DataFrame:
    out = df.loc[~df.index.isin(holidays)]
    if stale is not None:
        m = stale.reindex(index=out.index, columns=out.columns, fill_value=False)
        out = out.mask(m)
    return out


def derive(universe: str, config: dict | None = None, test: bool = False, since_year: int | None = None,
           registry_universe: str | None = None) -> dict:
    """Recalcule fx_eur et clean. ``since_year`` : limite la REECRITURE aux annees >= (daily).

    Le calcul lit tout l'historique des prix : la detection des feries et des queues figees
    depend du passe ; seule l'ecriture est bornee.
    """
    cfg = (config or {}).get("store", {})
    currency_fields = set(cfg.get("currency_fields", DEFAULT_CURRENCY_FIELDS))
    ov = (config or {}).get("universe_overrides", {}).get(registry_universe or universe, {})
    fx_layer = ov.get("fx_layer", True)

    names = layout.list_fields(universe, "raw", config, test)
    if not names:
        return {"fields": [], "fx_missing": []}
    refdata = reader.read_refdata(universe, config, test)
    raw_price = reader._read_dir(layout.field_dir(universe, "raw", "price", config, test)) if "price" in names else pd.DataFrame()
    holidays = ffilled_holidays(raw_price)
    stale = stale_tail_mask(raw_price.loc[~raw_price.index.isin(holidays)]) if not raw_price.empty else None
    fx = reader.read_fx(config=config, test=test) if fx_layer else pd.DataFrame()

    def keep(df):
        return df if since_year is None else df[df.index.year >= since_year]

    fx_missing: set[str] = set()
    for f in names:
        raw = raw_price if f == "price" else reader._read_dir(layout.field_dir(universe, "raw", f, config, test))
        if raw.empty:
            continue
        base = raw
        if fx_layer and f in currency_fields:
            base, missing = fxmod.convert_to_eur(raw, fx, refdata)
            fx_missing |= set(missing)
            writer.upsert_long(universe, "fx_eur", f, keep(base), config=config, test=test, replace_years=True)
        # un champ non monetaire n'a pas de queue "figee" au sens prix : seul le calendrier est nettoye
        cleaned = clean_frame(base, holidays, stale if f in currency_fields or f == "price" else None)
        writer.upsert_long(universe, "clean", f, keep(cleaned), config=config, test=test, replace_years=True)

    logger.info("derive %s: %d champs, %d feries retires, %d tickers sans FX",
                universe, len(names), len(holidays), len(fx_missing))
    return {"fields": names, "holidays": len(holidays), "fx_missing": sorted(fx_missing),
            "clean_version": CLEAN_VERSION}
