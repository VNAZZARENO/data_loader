"""Operations PURES sur un univers : chacune retourne (nouvel univers, liste d'ops journalisees)."""

from __future__ import annotations

import copy
import datetime as dt

from .. import tickers as tk
from .model import Member, Period, RegistryError, Universe, validate_name

GUARDRAIL = 0.5  # meme garde-fou que index_members.refresh_universe_csv


def _today() -> str:
    return dt.date.today().isoformat()


def _clone(u: Universe) -> Universe:
    return copy.deepcopy(u)


def add(u: Universe, tickers: list[str], date: str | None = None, source: str = "manual"):
    u, ops, date = _clone(u), [], date or _today()
    for t in tickers:
        m = u.member(t)
        if m is None:
            u.members.append(Member(ticker=t, periods=[Period(entry=date, entry_source=source)]))
            ops.append({"op": "add", "ticker": t, "date": date, "source": source})
        elif not m.active:
            m.periods.append(Period(entry=date, entry_source=source))
            ops.append({"op": "reactivate", "ticker": t, "date": date, "source": source})
    return u, ops


def deprecate(u: Universe, ticker: str, date: str | None = None, source: str = "manual"):
    u, date = _clone(u), date or _today()
    m = u.require(ticker)
    if not m.active:
        raise RegistryError(f"{ticker} est deja deprecated")
    m.periods[-1].exit, m.periods[-1].exit_source = date, source
    return u, [{"op": "deprecate", "ticker": ticker, "date": date, "source": source}]


def reactivate(u: Universe, ticker: str, date: str | None = None, source: str = "manual"):
    if u.require(ticker).active:
        raise RegistryError(f"{ticker} est deja actif")
    return add(u, [ticker], date, source)


def remove(u: Universe, ticker: str):
    """Suppression definitive (faute de frappe). Un vrai sortant se 'deprecate'."""
    u = _clone(u)
    m = u.require(ticker)
    for other in u.members:
        if other.successor == ticker:
            other.successor = None
        if other.predecessor == ticker:
            other.predecessor = None
    u.members.remove(m)
    return u, [{"op": "remove", "ticker": ticker}]


def toggle_fetch(u: Universe, enabled: bool, ticker: str | None = None):
    u = _clone(u)
    if ticker is None:
        u.fetch_enabled = enabled
    else:
        u.require(ticker).fetch = enabled
    return u, [{"op": "fetch", "ticker": ticker, "enabled": enabled}]


def link_rename(u: Universe, old: str, new: str):
    """ROG SE -> ROP SE : meme constituant, deux tickers. Lien toujours manuel."""
    u = _clone(u)
    if old == new:
        raise RegistryError("Un ticker ne peut pas se succeder a lui-meme")
    a, b = u.require(old), u.require(new)
    if a.successor or b.predecessor:
        raise RegistryError(f"Lien deja existant sur {old} ou {new}")
    a.successor, b.predecessor = new, old
    return u, [{"op": "link_rename", "from": old, "to": new}]


def unlink_rename(u: Universe, old: str):
    u = _clone(u)
    a = u.require(old)
    if not a.successor:
        raise RegistryError(f"{old} n'a pas de successeur")
    u.require(a.successor).predecessor = None
    new, a.successor = a.successor, None
    return u, [{"op": "unlink_rename", "from": old, "to": new}]


def update_member(u: Universe, ticker: str, periods: list[dict] | None = None, note: str | None = None):
    u = _clone(u)
    m = u.require(ticker)
    if periods is not None:
        if not periods:
            raise RegistryError("Au moins une periode est requise")
        m.periods = [Period(**p) for p in periods]
    if note is not None:
        m.note = note
    u.validate()
    return u, [{"op": "update", "ticker": ticker, "periods": periods, "note": note}]


def apply_index_diff(u: Universe, new_tickers: list[str], date: str | None = None,
                     source: str = "index", force: bool = False):
    """Applique une composition d'indice : entrants ajoutes, sortants deprecated (jamais supprimes)."""
    date = date or _today()
    active = set(u.active_tickers())
    new = set(new_tickers)
    if active and len(new) < GUARDRAIL * len(active) and not force:
        raise RegistryError(
            f"Garde-fou : {len(new)} membres recus contre {len(active)} actifs (< {GUARDRAIL:.0%})"
        )
    out, ops = add(u, [t for t in new_tickers if t not in active], date, source)
    for t in sorted(active - new):
        out, o = deprecate(out, t, date, source)
        ops += o
    return out, ops


def apply_history(u: Universe, snapshots: dict[str, list[str]]):
    """Reconstruit les periodes a partir de compositions historiques {date ISO: [tickers]}.

    Les periodes saisies a la main (``manual``) sont conservees. La derniere periode reste
    ouverte si le ticker est actif aujourd'hui, et garde sa sortie connue sinon.
    """
    u = _clone(u)
    dates = sorted(snapshots)
    if not dates:
        return u, []
    ops_log = []
    every = list(dict.fromkeys(t for d in dates for t in snapshots[d]))
    for t in every:
        periods, open_ = [], None
        for d in dates:
            inside = t in snapshots[d]
            if inside and open_ is None:
                open_ = Period(entry=d, entry_source="index_hist")
            elif not inside and open_ is not None:
                open_.exit, open_.exit_source = d, "index_hist"
                periods.append(open_)
                open_ = None
        m = u.member(t)
        if open_ is not None:  # present dans la derniere composition connue
            last = m.periods[-1] if m and m.periods else None
            if last is not None and last.exit and last.exit > open_.entry:
                open_.exit, open_.exit_source = last.exit, last.exit_source
            periods.append(open_)
        if m is None:
            if periods[-1].exit is None:  # jamais vu dans le registre mais "actif" : sortie inconnue
                periods[-1].exit, periods[-1].exit_source = dates[-1], "index_hist"
            u.members.append(Member(ticker=t, periods=periods))
        elif any(p.entry_source == "manual" for p in m.periods):
            continue
        else:
            m.periods = periods
        ops_log.append({"op": "history", "ticker": t,
                        "periods": [[p.entry, p.exit] for p in periods]})
    u.validate()
    return u, ops_log


def rename_candidates(u: Universe) -> list[dict]:
    """Paires sortant -> membre actif plausibles (ROG SE -> ROP SE). A confirmer a la main.

    Heuristique volontairement large : meme racine sur une autre place, ou meme place
    avec une racine a une lettre pres et une entree le jour de la sortie (ou de date inconnue).
    """
    out = []
    for a in u.members:
        if a.active or a.successor:
            continue
        (root, exch), exit_ = tk.split_ticker(a.ticker), a.periods[-1].exit
        for b in u.members:
            if b is a or b.predecessor or not b.active:
                continue
            broot, bexch = tk.split_ticker(b.ticker)
            entry = b.periods[-1].entry
            if broot == root:
                reason = "meme racine"
            elif (bexch == exch and entry in (None, exit_) and len(broot) == len(root)
                  and sum(x != y for x, y in zip(broot, root)) == 1):
                reason = "meme place, racine a une lettre pres"
            else:
                continue
            out.append({"from": a.ticker, "to": b.ticker, "date": exit_, "reason": reason})
    return out


# -- creation d'univers ---------------------------------------------------

def create(name: str, tickers: list[str], *, label: str = "", kind: str = "list",
           ticker_suffix: str = " Equity", source: dict | None = None, benchmark: str | None = None,
           date: str | None = None, entry_source: str = "manual", derived_from: dict | None = None,
           fields_profile: str | None = None) -> Universe:
    u = Universe(universe=validate_name(name), label=label or name, kind=kind,
                 ticker_suffix=ticker_suffix, source=source or {"type": "manual"},
                 benchmark=benchmark, derived_from=derived_from, fields_profile=fields_profile)
    u, _ = add(u, list(dict.fromkeys(tickers)), date, entry_source)
    return u


def combine(name: str, universes: list[Universe], how: str = "union", **kw) -> Universe:
    if not universes:
        raise RegistryError("Aucun univers source")
    sets = [u.active_tickers() for u in universes]
    if how == "union":
        tickers = list(dict.fromkeys(t for s in sets for t in s))
    elif how == "intersection":
        tickers = [t for t in sets[0] if all(t in set(s) for s in sets[1:])]
    elif how == "difference":
        rest = {t for s in sets[1:] for t in s}
        tickers = [t for t in sets[0] if t not in rest]
    else:
        raise RegistryError(f"Combinaison inconnue: {how}")
    suffixes = {u.ticker_suffix for u in universes}
    if len(suffixes) > 1:
        raise RegistryError("Univers a conventions de suffixe differentes : combinaison refusee")
    return create(name, tickers, kind="derived", ticker_suffix=suffixes.pop(),
                  derived_from={"how": how, "universes": [u.universe for u in universes]}, **kw)


def clone(name: str, src: Universe, **kw) -> Universe:
    """Clone complet : historique des periodes et liens de renommage conserves."""
    u = _clone(src)
    u.universe, u.rev, u.kind = validate_name(name), 0, "derived"
    u.label = kw.get("label") or f"{src.label} (copie)"
    u.derived_from = {"how": "clone", "universes": [src.universe]}
    u.source = {"type": "manual"}
    return u


def filter_(name: str, src: Universe, exchanges: list[str] | None = None,
            refdata: dict[str, dict] | None = None, sectors: list[str] | None = None,
            currencies: list[str] | None = None, **kw) -> Universe:
    keep = []
    for t in src.active_tickers():
        rd = (refdata or {}).get(t, {})
        if exchanges and tk.split_ticker(t)[1] not in exchanges:
            continue
        if sectors and rd.get("sector") not in sectors:
            continue
        if currencies and rd.get("currency") not in currencies:
            continue
        keep.append(t)
    return create(name, keep, kind="derived", ticker_suffix=src.ticker_suffix, benchmark=src.benchmark,
                  derived_from={"how": "filter", "universes": [src.universe], "exchanges": exchanges,
                                "sectors": sectors, "currencies": currencies}, **kw)
