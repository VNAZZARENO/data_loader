"""Masque d'appartenance point-in-time : 'membre a la date t'."""

from __future__ import annotations

import pandas as pd

from .model import Universe


def _chain(u: Universe, ticker: str) -> list[str]:
    """Ticker + tous ses predecesseurs (ROP SE -> [ROP SE, ROG SE])."""
    out, seen, cur = [], set(), ticker
    while cur and cur not in seen:
        seen.add(cur)
        out.append(cur)
        m = u.member(cur)
        cur = m.predecessor if m else None
    return out


def spliced_columns(u: Universe) -> dict[str, list[str]]:
    """{ticker final: [chaine de tickers a recoller]} ; les predecesseurs disparaissent."""
    return {m.ticker: _chain(u, m.ticker) for m in u.members if not m.successor}


def membership_mask(u: Universe, index: pd.DatetimeIndex, first_valid: pd.Series | dict | None = None,
                    splice_renames: bool = True) -> pd.DataFrame:
    """DataFrame bool (dates x tickers).

    ``entry`` inconnue -> membre depuis ``first_valid[ticker]`` (ou depuis le debut si absent) :
    c'est une hypothese, signalee par ``entry_source`` dans le registre.
    """
    index = pd.DatetimeIndex(index)
    fv = dict(first_valid) if first_valid is not None else {}

    def one(ticker: str) -> pd.Series:
        s = pd.Series(False, index=index)
        m = u.member(ticker)
        for p in (m.periods if m else []):
            start = pd.Timestamp(p.entry) if p.entry else fv.get(ticker)
            lo = index >= start if start is not None and not pd.isna(start) else True
            hi = index < pd.Timestamp(p.exit) if p.exit else True
            s |= pd.Series(lo & hi, index=index) if not (lo is True and hi is True) else True
        return s

    if splice_renames:
        cols = {final: pd.concat([one(t) for t in chain], axis=1).any(axis=1)
                for final, chain in spliced_columns(u).items()}
    else:
        cols = {m.ticker: one(m.ticker) for m in u.members}
    return pd.DataFrame(cols, index=index).astype(bool)


def count_series(u: Universe, index: pd.DatetimeIndex, first_valid=None) -> pd.Series:
    return membership_mask(u, index, first_valid).sum(axis=1)
