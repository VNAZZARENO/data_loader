"""Indicateurs du dashboard : fraicheur, couverture, series figees, nb de composants, indice equal-weight."""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

from . import manifests, registry, store
from .registry import pit
from .store import layout, reader


def business_days_since(date_iso: str | None, today: dt.date | None = None) -> int | None:
    if not date_iso:
        return None
    today = today or dt.date.today()
    return max(int(np.busday_count(pd.Timestamp(date_iso[:10]).date(), today)), 0)


def composition_reviewed_at(u, config=None) -> str | None:
    """Derniere vraie revue de la composition : les changesets de migration ne comptent pas
    (seeder un CSV vieux de 7 mois ne le rend pas frais)."""
    from .registry import events

    review_ops = {"add", "deprecate", "reactivate", "remove", "create", "reviewed"}
    for cs in events.list_changesets(u.universe, config=config):
        # un changement de reglage ou un toggle de fetch n'est pas une revue de composition
        if cs.get("source") != "migration" and any(o.get("op") in review_ops for o in cs.get("ops", [])):
            return cs["ts"]
    return u.source.get("seed_date")


def summary(universe: str, config=None) -> dict:
    """Leger : registre + manifeste + watermarks. Ne lit aucun parquet (page d'accueil)."""
    u = registry.load(universe, config)
    last = manifests.latest(universe, config=config)
    from .store import writer

    # tous champs confondus : un univers sans alias "price" (euro_credit) a quand meme des donnees
    all_wm = writer.load_state(universe, config).get("watermarks", {})
    wm = {f"{f}:{t}": d for f, per in all_wm.items() for t, d in per.items()}
    data_date = max(wm.values()) if wm else None
    return {
        "universe": u.universe, "label": u.label, "kind": u.kind, "rev": u.rev,
        "n_active": len(u.active_tickers()), "n_deprecated": len(u.deprecated_tickers()),
        "n_fetch_off": sum(1 for m in u.members if not m.fetch), "fetch_enabled": u.fetch_enabled,
        "source": u.source, "benchmark": u.benchmark,
        "composition_updated_at": (reviewed := composition_reviewed_at(u, config)),
        "composition_age_bd": business_days_since(reviewed),
        "data_date": data_date, "data_age_bd": business_days_since(data_date),
        "last_run": None if last is None else {
            k: last.get(k) for k in ("run_id", "status", "profile", "started_at", "finished_at", "host", "error")},
        "in_store": bool(wm), "has_price": bool(all_wm.get("price")), "store_fields": sorted(all_wm),
    }


def _flat_tail(s: pd.Series) -> int:
    s = s.dropna()
    if len(s) < 2:
        return 0
    moved = (s.diff().abs() > 0).to_numpy()
    idx = np.flatnonzero(moved)
    return int(len(s) - 1 - idx[-1]) if len(idx) else len(s) - 1


def field_quality(universe: str, layer: str = "raw", window: int = 252, config=None) -> list[dict]:
    """Par champ : couverture sur les membres actifs, NaN% a la derniere date, bornes."""
    u = registry.load(universe, config)
    active = set(pit.spliced_columns(u)) & {t for t in u.active_tickers()} or set(u.active_tickers())
    out = []
    for f in reader.fields(universe, layer, config):
        df = store.read(universe, f, layer=layer, config=config)
        if df.empty:
            continue
        cols = [t for t in df.columns if t in active]
        recent = df[cols].tail(window)
        last_row = df[cols].iloc[-1] if cols else pd.Series(dtype=float)
        out.append({
            "field": f, "first_date": df.index.min().date().isoformat(),
            "last_date": df.index.max().date().isoformat(),
            "n_tickers": int(df.shape[1]), "n_active_with_data": int(df[cols].notna().any().sum()),
            "n_active": len(active),
            "coverage_pct": round(float(recent.notna().to_numpy().mean() * 100), 2) if cols else 0.0,
            "nan_last_pct": round(float(last_row.isna().mean() * 100), 2) if cols else 100.0,
            "missing_active": sorted(active - set(df.columns))[:50],
        })
    return out


def ticker_quality(universe: str, config=None) -> list[dict]:
    """Par ticker (prix brut) : premiere/derniere valeur, queue figee, echecs du dernier run."""
    u = registry.load(universe, config)
    px = store.read(universe, "price", splice_renames=False, config=config)
    last = manifests.latest(universe, config=config) or {}
    failed, missing = set(), set()
    for rep in (last.get("per_field") or {}).values():
        failed |= {t.replace(u.ticker_suffix, "") if u.ticker_suffix else t for t in rep.get("failed", [])}
        missing |= {t.replace(u.ticker_suffix, "") if u.ticker_suffix else t for t in rep.get("missing", [])}
    end = px.index.max() if not px.empty else None
    rows = []
    for m in u.members:
        s = px[m.ticker] if m.ticker in px.columns else pd.Series(dtype=float)
        fv, lv = s.first_valid_index(), s.last_valid_index()
        flat = _flat_tail(s)
        p = m.periods[-1] if m.periods else None
        flags = []
        if fv is None:
            flags.append("sans_donnees")
        elif m.active and end is not None and lv < end:
            flags.append("en_retard")
        if m.active and flat >= 10:
            flags.append("fige")
        if m.ticker in failed:
            flags.append("echec_fetch")
        if m.ticker in missing:
            flags.append("absent_reponse")
        rows.append({
            "ticker": m.ticker, "status": m.status, "fetch": m.fetch,
            "entry": p.entry if p else None, "entry_source": p.entry_source if p else None,
            "exit": p.exit if p else None, "exit_source": p.exit_source if p else None,
            "n_periods": len(m.periods), "successor": m.successor, "predecessor": m.predecessor,
            "note": m.note,
            "first_valid": fv.date().isoformat() if fv is not None else None,
            "last_valid": lv.date().isoformat() if lv is not None else None,
            "n_obs": int(s.notna().sum()), "flat_tail": flat, "flags": flags,
        })
    return rows


def count_series(universe: str, config=None) -> pd.DataFrame:
    """Par date : membres selon le registre (PIT) et membres avec un prix."""
    u = registry.load(universe, config)
    # prix bruts : "a un prix" ne doit pas dependre de la disponibilite des taux de change
    px = store.read(universe, "price", config=config)
    if px.empty:
        return pd.DataFrame(columns=["members", "with_price"])
    fv = px.apply(lambda c: c.first_valid_index())
    mask = pit.membership_mask(u, px.index, fv)   # inclut les membres sans aucune donnee
    priced = mask.reindex(columns=px.columns, fill_value=False) & px.notna()
    return pd.DataFrame({"members": mask.sum(axis=1), "with_price": priced.sum(axis=1)})


def ew_index(universe: str, layer: str = "clean", pit_members: bool = True, base: float = 100.0,
             config=None) -> pd.DataFrame:
    """Indice equal-weight rebalance chaque jour : moyenne des rendements des membres du jour.

    Methode : r_t = moyenne_i( P_i,t / P_i,t-1 - 1 ) sur les i membres a t (masque PIT du registre)
    ayant un prix a t-1 et a t ; indice = base * prod(1 + r_t). Pas de frais, pas de poids flottants.
    """
    ew_config = (config or {}).get("universe_overrides", {}).get(universe, {}).get("ew_index", {})
    if not ew_config.get("enabled", True):
        out = pd.DataFrame(columns=["ew", "n", "benchmark"])
        out.attrs["layer_used"] = layer
        out.attrs["unavailable_reason"] = ew_config.get("reason", "Indice equal-weight desactive pour cet univers.")
        return out
    u = registry.load(universe, config)
    px = store.read(universe, "price", layer=layer, config=config)
    if px.empty and layer != "raw":
        # couche derivee vide (taux FX pas encore extraits : fx_eur est fail-closed) -> prix locaux
        layer, px = "raw", store.read(universe, "price", layer="raw", config=config)
    if px.empty:
        return pd.DataFrame(columns=["ew", "n", "benchmark"])
    # A zero previous price has no defined percentage return.
    rets = px.pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan)
    if pit_members:
        fv = px.apply(lambda c: c.first_valid_index())
        mask = pit.membership_mask(u, px.index, fv).reindex(columns=px.columns, fill_value=False)
        rets = rets.where(mask)
    out = pd.DataFrame({"n": rets.notna().sum(axis=1)})
    out["ew"] = base * (1 + rets.mean(axis=1).fillna(0.0)).cumprod()
    bench = reader.read_benchmark(universe, ["price"], config=config)
    if not bench.empty:
        b = bench["price"].reindex(px.index).ffill()
        first = b.where(np.isfinite(b) & b.ne(0)).first_valid_index()
        if first is not None:
            out["benchmark"] = b / b.loc[first] * out["ew"].loc[first]
    out = out[[c for c in ("ew", "n", "benchmark") if c in out.columns]]
    out.attrs["layer_used"] = layer
    out.attrs["n_members_last"] = int(mask.iloc[-1].sum()) if pit_members else int(px.shape[1])
    out.attrs["n_priced_last"] = int(out["n"].iloc[-1])
    return out
