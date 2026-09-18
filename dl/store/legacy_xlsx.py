"""Pont avec le xlsx historique : export a l'identique depuis ``raw`` et import one-shot.

Les lecteurs ATLAS (``load_price_xlsx`` & co) continuent de fonctionner tant qu'ils n'ont pas migre
vers ``dl.store.read``.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

from .. import paths, registry, smbio
from ..registry import ops
from . import derive as derive_mod
from . import layout, reader, writer

logger = logging.getLogger(__name__)
NON_FIELD_SHEETS = {"parameters", "benchmark"}


def build_sheets(universe: str, field_aliases: list[str], tickers: list[str] | None = None,
                 config=None, test=False, only_listed: bool = False) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """Reproduit la mise en forme du loader : feuilles alignees sur l'index ``price`` + ffill."""
    sheets = {a: reader._read_dir(layout.field_dir(universe, "raw", a, config, test)) for a in field_aliases}
    if tickers:  # ordre des colonnes = ordre de la liste, puis le reste (sortants historiques)
        for a, df in sheets.items():
            first = [t for t in tickers if t in df.columns]
            rest = [] if only_listed else [c for c in df.columns if c not in set(first)]
            sheets[a] = df[first + rest]   # only_listed : les deprecated restent dans le store, pas dans le xlsx
    price = sheets.get("price")
    if price is not None and not price.empty:
        for a, df in sheets.items():
            if a != "price" and not df.empty and len(df) < len(price.index):
                sheets[a] = df.reindex(price.index).ffill()
    return sheets, reader.read_benchmark(universe, field_aliases, config=config, test=test)


def write_workbook(path, parameters: dict, sheets: dict[str, pd.DataFrame], benchmark: pd.DataFrame | None) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame(list(parameters.items()), columns=["Parameter", "Value"]).to_excel(
            w, sheet_name="parameters", index=False)
        for name, df in sheets.items():
            if df.empty:
                continue
            df = df.copy()
            df.index.name = "Ticker"   # bizarrerie historique : la colonne s'appelle Ticker, contient des dates
            df.to_excel(w, sheet_name=name)
        if benchmark is not None and not benchmark.empty:
            benchmark = benchmark.copy()
            benchmark.index.name = "Date"
            benchmark.to_excel(w, sheet_name="benchmark")


def export(universe: str, path: str | Path, field_aliases: list[str], parameters: dict,
           tickers: list[str] | None = None, config=None, test=False, only_listed: bool = False) -> dict:
    sheets, bench = build_sheets(universe, field_aliases, tickers, config, test, only_listed)
    if all(df.empty for df in sheets.values()):
        raise ValueError(f"Store vide pour {universe} : export xlsx refuse")
    smbio.atomic_write_via(path, lambda tmp: write_workbook(tmp, parameters, sheets, bench))
    return {"path": str(path), "bytes": os.path.getsize(path),
            "sheets": {a: list(df.shape) for a, df in sheets.items()}}


# -- import one-shot -------------------------------------------------------

def _posix_output_path(universe: str, config: dict, profile: str | None) -> Path:
    name = Path(config["paths"]["output_xlsx"].replace("\\", "/")).name.format(universe=universe)
    if profile and profile != "default":
        root, ext = os.path.splitext(name)
        name = f"{root}_{profile}{ext}"
    return paths.share_root(config) / name


def import_workbook(universe: str, xlsx: Path, config=None, test=False, dry_run=False) -> dict:
    report: dict = {"file": str(xlsx), "sheets": {}}
    book = pd.ExcelFile(xlsx, engine="openpyxl")
    for sheet in book.sheet_names:
        if sheet == "parameters":
            continue
        logger.info("  feuille %s", sheet)
        df = book.parse(sheet, index_col=0)
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[df.index.notna()]
        report["sheets"][sheet] = list(df.shape)
        if dry_run or df.empty:
            continue
        if sheet == "benchmark":
            for col in df.columns:
                writer.upsert_long(universe, layout.BENCHMARK, str(col),
                                   df[[col]].rename(columns={col: "benchmark"}), config=config, test=test)
        else:
            writer.upsert_long(universe, "raw", sheet, df, config=config, test=test)
            writer.update_state(universe, sheet, df, config, test)
    return report


def set_entries_from_first_valid(universe: str, config=None, test=False) -> int:
    """Remplace les entrees inconnues par la premiere date de prix (``entry_source: first_valid``)."""
    if not registry.exists(universe, config):
        return 0
    u = registry.load(universe, config)
    fv = reader.first_valid(universe, config=config, test=test)
    all_ops = []
    for m in u.members:
        p = m.periods[0] if m.periods else None
        d = fv.get(m.ticker)
        if p is not None and p.entry is None and d is not None and not pd.isna(d):
            iso = pd.Timestamp(d).date().isoformat()
            if p.exit and p.exit < iso:
                continue
            p.entry, p.entry_source = iso, "first_valid"
            all_ops.append({"op": "set_entry", "ticker": m.ticker, "date": iso, "source": "first_valid"})
    if all_ops:
        registry.save(u, u.rev, all_ops, actor="migrate", source="migration", config=config)
    return len(all_ops)


def import_universe(universe: str, config: dict, profile: str | None = None, dry_run=False,
                    test=False, set_entries=True) -> dict:
    xlsx = _posix_output_path(universe, config, profile)
    if not xlsx.is_file():
        raise FileNotFoundError(xlsx)
    logger.info("Import %s -> store/%s", xlsx, universe)
    report = import_workbook(universe, xlsx, config, test, dry_run)
    if not dry_run:
        report["derive"] = derive_mod.derive(universe, config, test)
        if set_entries:
            report["entries_set"] = set_entries_from_first_valid(universe, config, test)
    return report
