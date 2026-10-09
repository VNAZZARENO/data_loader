"""Catalogue de descriptions par ticker : ``<partage>/univers/descriptions.json``.

Une description appartient au ticker, pas a l'univers : la cle est le ticker Bloomberg
complet (cle jaune comprise), donc ``EUR003M Index`` porte le meme texte dans
``global_macro`` et ``euro_credit``. Fichier a part du registre : les clones du depot
pas encore a jour continuent de relire ``registry/<u>.json`` sans cle inconnue.

    python -m dl.descriptions seed [--dry-run] [--overwrite]   # config/ticker_descriptions.yaml -> partage
"""

from __future__ import annotations

import argparse
import datetime as dt
import socket
from pathlib import Path

from . import paths, smbio

SCHEMA = 1
SEED_FILE = Path(__file__).resolve().parent.parent / "config" / "ticker_descriptions.yaml"
MAX_LEN = 300


def _path(config: dict | None = None) -> Path:
    return paths.univers_root(config) / "descriptions.json"


def key(ticker: str, ticker_suffix: str = "") -> str:
    """Ticker du registre -> cle du catalogue ('MC FP' + ' Equity' ; 'SPX Index' tel quel)."""
    return ticker + (ticker_suffix or "")


def _read(config: dict | None = None) -> dict:
    p = _path(config)
    if not p.is_file():
        return {"schema": SCHEMA, "tickers": {}}
    return smbio.read_json_retry(p)


def load(config: dict | None = None) -> dict[str, str]:
    """{ticker Bloomberg complet: description}."""
    return {t: e["description"] for t, e in _read(config)["tickers"].items() if e.get("description")}


def for_universe(u, config: dict | None = None) -> dict[str, str]:
    """{ticker tel qu'au registre: description} pour les membres decrits d'un univers."""
    cat = load(config)
    out = {m.ticker: cat.get(key(m.ticker, u.ticker_suffix)) for m in u.members}
    return {t: d for t, d in out.items() if d}


def set_many(items: dict[str, str], *, source: str = "manual", actor: str = "cli",
             overwrite: bool = True, config: dict | None = None) -> list[str]:
    """Ecrit des descriptions (cle = ticker complet) ; une chaine vide efface. Retourne les
    tickers modifies. ``overwrite=False`` ne touche pas une entree deja renseignee."""
    p = _path(config)
    changed = []
    with smbio.DirLock(p.with_name("_descriptions.lock")):
        doc = _read(config)
        entries = doc["tickers"]
        for ticker, text in items.items():
            text = " ".join(str(text or "").split())[:MAX_LEN]
            current = entries.get(ticker, {}).get("description", "")
            if text == current or (current and not overwrite):
                continue
            if text:
                entries[ticker] = {"description": text, "source": source,
                                   "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                                   "updated_by": f"{actor}@{socket.gethostname()}"}
            else:
                entries.pop(ticker, None)
            changed.append(ticker)
        if changed:
            doc["tickers"] = dict(sorted(entries.items()))
            smbio.atomic_write_json(p, doc)
    return changed


def read_seed(path: Path = SEED_FILE) -> dict[str, str]:
    import yaml

    with open(path, encoding="utf-8") as f:
        return {str(k): str(v) for k, v in (yaml.safe_load(f) or {}).items()}


def seed(config: dict | None = None, dry_run: bool = False, overwrite: bool = False,
         path: Path = SEED_FILE) -> list[str]:
    """Pousse le fichier du depot vers le partage. Par defaut ne remplit que les tickers sans
    description ; ``overwrite`` remplace aussi les entrees d'origine ``seed``, jamais une saisie
    manuelle du dashboard."""
    items = read_seed(path)
    entries = _read(config)["tickers"]
    if overwrite:
        items = {t: d for t, d in items.items() if entries.get(t, {}).get("source", "seed") == "seed"}
    if dry_run:
        return [t for t, d in items.items()
                if d != entries.get(t, {}).get("description", "") and (overwrite or t not in entries)]
    return set_many(items, source="seed", overwrite=overwrite, config=config)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["seed"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--overwrite", action="store_true", help="remplace aussi les entrees d'origine seed")
    a = ap.parse_args()
    changed = seed(dry_run=a.dry_run, overwrite=a.overwrite)
    print(f"{'A ecrire' if a.dry_run else 'Ecrit'} : {len(changed)} description(s) -> {_path()}")
    for t in changed:
        print(f"  {t}")


if __name__ == "__main__":
    main()
