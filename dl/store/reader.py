"""API de lecture du store : ce que les consommateurs aval doivent utiliser a la place de read_excel."""

from __future__ import annotations

import pandas as pd
import pyarrow.parquet as pq

from .. import paths, registry
from ..registry import pit
from . import layout


def _read_dir(directory, start=None, end=None, tickers=None) -> pd.DataFrame:
    """Large (dates x tickers). Seules les annees demandees sont ouvertes."""
    lo = pd.Timestamp(start) if start is not None else None
    hi = pd.Timestamp(end) if end is not None else None
    frames = []
    for y in layout.list_years(directory):
        if (lo is not None and y < lo.year) or (hi is not None and y > hi.year):
            continue
        frames.append(pq.read_table(directory / f"{y}.parquet").to_pandas())
    if not frames:
        return pd.DataFrame(index=pd.DatetimeIndex([], name="date"))
    long = pd.concat(frames, ignore_index=True)
    long["date"] = pd.to_datetime(long["date"])
    if tickers is not None:
        long = long[long["ticker"].isin(list(tickers))]
    wide = long.pivot(index="date", columns="ticker", values="value").sort_index()
    wide.columns.name = None
    if lo is not None:
        wide = wide[wide.index >= lo]
    if hi is not None:
        wide = wide[wide.index <= hi]
    return wide


def fields(universe: str, layer: str = "raw", config=None, test=False) -> list[str]:
    return layout.list_fields(universe, layer, config, test)


def years(universe: str, layer: str = "raw", field: str = "price", config=None, test=False) -> list[int]:
    return layout.list_years(layout.field_dir(universe, layer, field, config, test))


def _splice(wide: pd.DataFrame, u) -> pd.DataFrame:
    """Recolle les renommages sous le ticker final (le successeur prime la ou il a une valeur)."""
    out = wide.copy()
    for final, chain in pit.spliced_columns(u).items():
        if len(chain) == 1:
            continue
        present = [t for t in chain if t in out.columns]
        if not present:
            continue
        s = out[present[0]]
        for t in present[1:]:
            s = s.combine_first(out[t])
        out = out.drop(columns=present)
        out[final] = s
    return out


def membership_mask(universe: str, index, config=None, test=False, splice_renames=True) -> pd.DataFrame:
    u = registry.load(universe, config)
    return pit.membership_mask(u, index, first_valid(universe, config=config, test=test), splice_renames)


def first_valid(universe: str, field: str = "price", config=None, test=False) -> pd.Series:
    wide = _read_dir(layout.field_dir(universe, "raw", field, config, test))
    return wide.apply(lambda c: c.first_valid_index())


def read(universe: str, fields=None, layer: str = "raw", start=None, end=None, tickers=None,
         pit_members: bool = False, splice_renames: bool = True, wide: bool = True,
         config=None, test: bool = False, registry_universe: str | None = None):
    """Lit le store.

    ``wide=True``  -> ``{champ: DataFrame dates x tickers}``
    ``wide=False`` -> DataFrame long (date, ticker, field, layer, value)
    ``pit_members`` -> NaN hors des periodes d'appartenance du registre.
    ``registry_universe`` : univers du registre si different du nom du store (ex. option_europe_bt).
    """
    single = isinstance(fields, str)
    names = [fields] if single else (list(fields) if fields else layout.list_fields(universe, layer, config, test))
    reg_name = registry_universe or universe
    u = registry.load(reg_name, config) if (pit_members or splice_renames) and registry.exists(reg_name, config) else None
    if pit_members and u is None:
        raise FileNotFoundError(f"pit_members=True mais '{reg_name}' est absent du registre")

    out: dict[str, pd.DataFrame] = {}
    fv = None
    for f in names:
        df = _read_dir(layout.field_dir(universe, layer, f, config, test), start, end, tickers)
        if u is not None and splice_renames:
            df = _splice(df, u)
        if pit_members and not df.empty:
            if fv is None:
                fv = first_valid(universe, config=config, test=test)
            mask = pit.membership_mask(u, df.index, fv, splice_renames)
            df = df.where(mask.reindex(columns=df.columns, fill_value=False))
        out[f] = df

    if wide:
        return out[names[0]] if single else out
    parts = []
    for f, df in out.items():
        long = df.rename_axis(index="date").reset_index().melt(
            id_vars="date", var_name="ticker", value_name="value").dropna(subset=["value"])
        long.insert(2, "field", f)
        long.insert(3, "layer", layer)
        parts.append(long)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(
        columns=["date", "ticker", "field", "layer", "value"])


def read_benchmark(universe: str, fields=None, start=None, end=None, config=None, test=False) -> pd.DataFrame:
    names = list(fields) if fields else layout.list_fields(universe, layout.BENCHMARK, config, test)
    cols = {}
    for f in names:
        df = _read_dir(layout.field_dir(universe, layout.BENCHMARK, f, config, test), start, end)
        if not df.empty:
            cols[f] = df.iloc[:, 0]
    return pd.DataFrame(cols)


def read_fx(ccys=None, start=None, end=None, config=None, test=False) -> pd.DataFrame:
    """Taux ``EUR{CCY}`` (unites de CCY pour 1 EUR), close du jour. Colonnes = codes devise."""
    return _read_dir(paths.fx_dir(config, test), start, end, ccys)


def read_refdata(universe: str, config=None, test=False) -> pd.DataFrame:
    p = layout.refdata_path(universe, config, test)
    if not p.is_file():
        return pd.DataFrame(columns=["currency", "name", "sector"])
    return pd.read_parquet(p)
