"""Séries INSEE BDM -> store du DataLoader (univers hors Bloomberg).

La BDM publie ses séries en SDMX 2.1, sans clé : une requête par paquet d'idbanks.
Chaque série devient une colonne (ticker = zone) d'un champ du store ; la
correspondance champ x zone -> idbank et l'unité attendue sont versionnées dans
config/insee_bdm.yaml. Le store ne garde que des flottants, sans unité : c'est
pourquoi un écart d'unité côté INSEE arrête la passe au lieu d'écrire une série
qu'un lecteur interpréterait de travers.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
import pandas as pd
import yaml

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
