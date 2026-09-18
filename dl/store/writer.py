"""Ecriture incrementale : seules les partitions-annee touchees sont reecrites, atomiquement."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .. import smbio
from . import layout

_SCHEMA = pa.schema([("date", pa.date32()), ("ticker", pa.string()), ("value", pa.float64())])


def to_long(wide: pd.DataFrame) -> pd.DataFrame:
    if wide.empty:
        return pd.DataFrame(columns=["date", "ticker", "value"])
    w = wide.copy()
    w.index = pd.to_datetime(w.index)
    w.index.name = "date"
    w.columns = [str(c) for c in w.columns]
    long = w.apply(pd.to_numeric, errors="coerce").reset_index().melt(
        id_vars="date", var_name="ticker", value_name="value").dropna(subset=["value"])
    return long


def _read_year(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame(columns=["date", "ticker", "value"])
    df = pq.read_table(path).to_pandas()
    df["date"] = pd.to_datetime(df["date"])
    return df


def _write_year(path: Path, df: pd.DataFrame) -> None:
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)
    table = pa.Table.from_pandas(
        pd.DataFrame({"date": df["date"].dt.date, "ticker": df["ticker"].astype(str),
                      "value": df["value"].astype("float64")}),
        schema=_SCHEMA, preserve_index=False)
    smbio.atomic_write_via(path, lambda tmp: pq.write_table(table, tmp, compression="zstd"))


def upsert_dir(directory: Path, wide: pd.DataFrame, replace_years: bool = False) -> list[int]:
    """Fusionne ``wide`` (dates x tickers) dans ``directory``. Retourne les annees reecrites.

    ``replace_years`` : les annees touchees sont remplacees et non fusionnees (couches derivees).
    """
    long = to_long(wide)
    if long.empty:
        return []
    touched = []
    for year, chunk in long.groupby(long["date"].dt.year):
        path = directory / f"{year}.parquet"
        if replace_years:
            merged = chunk
        else:
            old = _read_year(path)
            merged = chunk if old.empty else pd.concat([old, chunk], ignore_index=True)
            merged = merged.drop_duplicates(["date", "ticker"], keep="last")
        _write_year(path, merged)
        touched.append(int(year))
    return touched


def upsert_long(universe: str, layer: str, field: str, wide: pd.DataFrame, *,
                config=None, test=False, replace_years=False) -> list[int]:
    return upsert_dir(layout.field_dir(universe, layer, field, config, test), wide, replace_years)


# -- watermarks ------------------------------------------------------------

def load_state(universe: str, config=None, test=False) -> dict:
    p = layout.state_path(universe, config, test)
    return smbio.read_json_retry(p) if p.is_file() else {"watermarks": {}}


def update_state(universe: str, field: str, wide: pd.DataFrame, config=None, test=False) -> dict:
    """``watermarks[field][ticker]`` = derniere date avec une valeur."""
    state = load_state(universe, config, test)
    wm = state.setdefault("watermarks", {}).setdefault(field, {})
    for t in wide.columns:
        last = wide[t].last_valid_index()
        if last is not None:
            iso = pd.Timestamp(last).date().isoformat()
            wm[str(t)] = max(wm.get(str(t), ""), iso)
    smbio.atomic_write_json(layout.state_path(universe, config, test), state)
    return state


def write_refdata(universe: str, df: pd.DataFrame, config=None, test=False) -> None:
    """``df`` indexe par ticker, colonnes currency / name / sector. Fusion avec l'existant."""
    p = layout.refdata_path(universe, config, test)
    if p.is_file():
        old = pd.read_parquet(p)
        df = pd.concat([old[~old.index.isin(df.index)], df])
    df.index.name = "ticker"
    smbio.atomic_write_via(p, lambda tmp: df.sort_index().to_parquet(tmp))
