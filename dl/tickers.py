"""Normalisation des tickers a la convention du projet.

Univers actions : tickers SANS suffixe (" Equity" ajoute au fetch).
Univers mixtes (``ticker_suffix == ''``) : tickers avec leur cle jaune complete.
"""

from __future__ import annotations

import re

YELLOW_KEYS = ("Equity", "Index", "Curncy", "Comdty", "Corp", "Govt", "Mtge", "Muni", "Pfd")
_SPLIT = re.compile(r"[\n\r,;\t]+")


def split_ticker(t: str) -> tuple[str, str]:
    """('ROP SE') -> ('ROP', 'SE') ; tolere les racines avec '/' comme 'AV/ LN'."""
    root, _, exch = t.strip().rpartition(" ")
    if not root:
        return t.strip(), ""
    return root, exch


def to_bbg(ticker: str, suffix: str) -> str:
    return ticker + suffix


def parse_list(text: str) -> list[str]:
    """Decoupe un collage libre (lignes, virgules, points-virgules, tabulations)."""
    return [p.strip() for p in _SPLIT.split(text) if p.strip() and p.strip().lower() != "ticker"]


def normalize(
    raw: list[str],
    ticker_suffix: str = " Equity",
    exchange_map: dict[str, str] | None = None,
    known_exchanges: set[str] | None = None,
) -> dict:
    """Retourne {tickers, duplicates, unknown_exchanges, changed:{brut: normalise}}."""
    exchange_map = exchange_map or {}
    out: list[str] = []
    seen: set[str] = set()
    duplicates: list[str] = []
    unknown: set[str] = set()
    changed: dict[str, str] = {}

    for item in raw:
        t = re.sub(r"\s+", " ", item.strip())
        if not t:
            continue
        parts = t.split(" ")
        key = parts[-1].capitalize() if parts[-1].capitalize() in YELLOW_KEYS else None
        body = " ".join(parts[:-1]) if key else t
        body = body.upper()

        if key in (None, "Equity"):
            root, exch = split_ticker(body)
            if exch:
                mapped = exchange_map.get(exch, exch)
                if known_exchanges and mapped not in known_exchanges:
                    unknown.add(mapped)
                body = f"{root} {mapped}"

        if ticker_suffix:  # univers actions : on retire la cle jaune
            norm = body if key in (None, "Equity") else f"{body} {key}"
        else:  # univers mixte : cle jaune obligatoire
            norm = f"{body} {key or 'Equity'}"

        if norm != item:
            changed[item] = norm
        if norm in seen:
            duplicates.append(norm)
            continue
        seen.add(norm)
        out.append(norm)

    return {
        "tickers": out,
        "duplicates": duplicates,
        "unknown_exchanges": sorted(unknown),
        "changed": changed,
    }
