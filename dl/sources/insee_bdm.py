"""Séries INSEE BDM -> store du DataLoader (univers hors Bloomberg).

La BDM publie ses séries en SDMX 2.1, sans clé : une requête par paquet d'idbanks.
Chaque série devient une colonne (ticker = zone) d'un champ du store ; la
correspondance champ x zone -> idbank et l'unité attendue sont versionnées dans
config/insee_bdm.yaml. Le store ne garde que des flottants, sans unité : c'est
pourquoi un écart d'unité côté INSEE arrête la passe au lieu d'écrire une série
qu'un lecteur interpréterait de travers.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
import pandas as pd
import yaml

from .. import paths, registry, smbio
from ..registry import ops
from ..store import writer

REPO = Path(__file__).resolve().parents[2]
SPEC = REPO / "config" / "insee_bdm.yaml"
BASE = "https://bdm.insee.fr/series/sdmx/data/SERIES_BDM"
TIMEOUT = 60.0
HEADERS = {"User-Agent": "Pergam-DataLoader/insee_bdm (quant@pergam.net)"}


def load_spec(path: str | Path | None = None) -> dict:
    with open(path or SPEC, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_sdmx(xml_text: str) -> dict[str, dict]:
    """Réponse StructureSpecific de la BDM -> {idbank: attributs et points}.

    TIME_PERIOD « AAAA-MM » est placé au dernier jour du mois : Trame et Vigie
    lisent des fins de mois. Une observation sans valeur (série interrompue) est
    écartée plutôt que convertie en NaN puis perdue en silence par le store.
    """
    out: dict[str, dict] = {}
    root = ET.fromstring(xml_text)
    for el in root.iter():
        if _local(el.tag) != "Series":
            continue
        a = el.attrib
        dates, values = [], []
        for obs in el:
            if _local(obs.tag) != "Obs" or not obs.get("OBS_VALUE"):
                continue
            try:
                v = float(obs.get("OBS_VALUE"))
            except ValueError:
                continue
            dates.append(pd.Period(obs.get("TIME_PERIOD"), freq="M").to_timestamp(how="end").normalize())
            values.append(v)
        points = pd.Series(values, index=pd.DatetimeIndex(dates), dtype=float).dropna().sort_index()
        out[a["IDBANK"]] = {"title": a.get("TITLE_FR", ""), "unit_measure": a.get("UNIT_MEASURE", ""),
                            "unit_mult": a.get("UNIT_MULT", ""), "last_update": a.get("LAST_UPDATE", ""),
                            "points": points}
    return out


def fetch(idbanks: list[str], start: str, *, get=httpx.get, chunk: int = 20) -> dict[str, dict]:
    """Interroge la BDM par paquets : une URL trop longue est refusée par le serveur."""
    out: dict[str, dict] = {}
    for i in range(0, len(idbanks), chunk):
        part = "+".join(idbanks[i:i + chunk])
        r = get(f"{BASE}/{part}?startPeriod={start}", headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        out.update(parse_sdmx(r.text))
    return out


class UniteInattendue(ValueError):
    """L'INSEE ne sert plus la série comme le YAML l'attend (unité, multiplicateur, absence)."""


def to_frames(spec: dict, series: dict[str, dict]) -> tuple[dict[str, pd.DataFrame], list[dict]]:
    """Séries par idbank -> un tableau dates x zones par champ, et le relevé de la passe.

    Tout ou rien : une seule série non conforme arrête la passe, pour qu'un
    champ ne mélange jamais deux unités dans le store.
    """
    frames: dict[str, pd.DataFrame] = {}
    releve: list[dict] = []
    for champ, f in spec["fields"].items():
        cols = {}
        for ticker, idbank in f["idbanks"].items():
            s = series.get(idbank)
            if s is None:
                raise UniteInattendue(f"{champ}/{ticker}: série {idbank} absente de la réponse INSEE")
            if (s["unit_measure"], s["unit_mult"]) != (f["unit_measure"], str(f["unit_mult"])):
                raise UniteInattendue(
                    f"{champ}/{ticker}: série {idbank} en {s['unit_measure']} x10^{s['unit_mult']},"
                    f" attendu {f['unit_measure']} x10^{f['unit_mult']}")
            cols[ticker] = s["points"]
            last = s["points"].index.max()
            releve.append({"field": champ, "ticker": ticker, "idbank": idbank, "title": s["title"],
                           "unit": f["unit"], "last_update": s["last_update"], "n_obs": int(len(s["points"])),
                           "last": None if pd.isna(last) else last.date().isoformat()})
        frames[champ] = pd.DataFrame(cols).sort_index()
    return frames, releve


ENTREE = "2011-01-01"


def ensure_universe(spec: dict, config=None):
    """Univers du registre : créé une fois, collecte Bloomberg coupée.

    fetch_enabled = False est ce qui protège l'univers du chargeur Bloomberg
    (bloomberg_loader.py saute un univers désactivé) : ses tickers sont des
    zones INSEE, pas des instruments.
    """
    name, zones = spec["universe"], list(spec["zones"])
    if not registry.exists(name, config):
        u = ops.create(name, zones, label=spec["label"], kind="list", ticker_suffix="",
                       source={"type": "insee_bdm", "config": "config/insee_bdm.yaml"},
                       date=ENTREE, entry_source="manual")
        u, op_fetch = ops.toggle_fetch(u, False)
        return registry.save(u, None, [{"op": "create", "tickers": zones}, *op_fetch],
                             actor="insee_bdm", source="insee_bdm", config=config)
    u = registry.load(name, config)
    manquants = [z for z in zones if z not in u.active_tickers()]
    if not manquants:
        return u
    rev = u.rev
    u, op_add = ops.add(u, manquants, ENTREE, "manual")
    return registry.save(u, rev, op_add, actor="insee_bdm", source="insee_bdm", config=config)


def ingest(spec: dict | None = None, *, get=httpx.get, dry_run: bool = False, config=None) -> dict:
    """Une passe complète depuis spec["start"] : l'INSEE révise ses derniers mois
    (OBS_STATUS « P »), relire tout l'historique les remplace sans autre logique."""
    spec = spec or load_spec()
    idbanks = [i for f in spec["fields"].values() for i in f["idbanks"].values()]
    frames, releve = to_frames(spec, fetch(idbanks, spec["start"], get=get))
    man = {"universe": spec["universe"], "fields": list(frames), "series": releve, "written": {},
           "at": dt.datetime.now(dt.timezone.utc).isoformat(), "dry_run": dry_run}
    if dry_run:
        return man
    name = spec["universe"]
    ensure_universe(spec, config)
    for champ, wide in frames.items():
        man["written"][champ] = writer.upsert_long(name, "raw", champ, wide, config=config)
        writer.update_state(name, champ, wide, config=config)
    ref = pd.DataFrame({"name": spec["zones"], "currency": "", "sector": "Hôtellerie (INSEE)"})
    writer.write_refdata(name, ref, config=config)
    smbio.atomic_write_json(paths.runs_dir(name, config) / "latest_insee.json", man)
    return man


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="lit l'INSEE et contrôle les unités, n'écrit rien")
    args = ap.parse_args(argv)
    man = ingest(dry_run=args.dry_run)
    for r in man["series"]:
        print(f"{r['field']:<24} {r['ticker']:<9} {r['idbank']} {r['n_obs']:>4} obs, dernière {r['last']}")
    print("écrit:" if man["written"] else "rien écrit (--dry-run)", man["written"] or "")
    return 0


if __name__ == "__main__":
    sys.exit(main())
