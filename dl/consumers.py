"""Rattachement univers -> consommateurs (config/consumers.yaml).

Un consommateur est un outil, une strategie ou un projet qui lit une sortie du
DataLoader (classeur, store, refdata, API du dashboard). Le fichier est la source de
verite du rattachement ; ce module le relit pour :
- journaliser, au debut d'une passe, qui depend de l'univers extrait ;
- avertir quand un champ attendu par un consommateur n'est pas dans la liste de
  champs de l'univers (il ne sera donc pas collecte) ;
- `python -m dl.consumers check` : matrice univers -> consommateurs et anomalies.
Aucune ecriture, aucun reseau : lecture du YAML et de la config seulement.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

ARTEFACTS = ("xlsx", "store", "refdata", "api", "registry")
STATUSES = ("actif", "donnees_figees", "en_echec", "dormant", "mort", "ponctuel")
KINDS = ("prod", "shadow", "service", "cron", "research", "procedure", "dormant")
# Champs servis par le store ou l'API sans figurer dans la liste de champs d'un univers.
IMPLICIT_FIELDS = {"benchmark", "parameters", "sector", "currency", "name"}


@dataclass
class Finding:
    level: str          # ERROR | WARN | INFO
    universe: str | None
    consumer: str | None
    message: str


@dataclass
class Doc:
    consumers: dict = field(default_factory=dict)
    path: Path | None = None

    def by_universe(self) -> dict[str, list[tuple[str, dict]]]:
        out: dict[str, list[tuple[str, dict]]] = {}
        for cid, c in self.consumers.items():
            for r in c.get("reads", []):
                out.setdefault(str(r.get("universe")), []).append((cid, r))
        return out


def default_path(config_path: str | os.PathLike | None = None) -> Path:
    base = Path(config_path).parent if config_path else Path(__file__).resolve().parent.parent / "config"
    return base / "consumers.yaml"


def load(path: str | os.PathLike | None = None, config_path: str | os.PathLike | None = None) -> Doc:
    p = Path(path) if path else default_path(config_path)
    if not p.is_file():
        return Doc({}, p)
    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    consumers = raw.get("consumers") or {}
    if not isinstance(consumers, dict):
        raise ValueError(f"{p}: 'consumers' doit etre un mapping id -> consommateur")
    return Doc(consumers, p)


def universe_fields(universe: str, config: dict) -> list[str]:
    """Liste de champs collectee par la passe sans --agents (meme regle que le loader)."""
    ov = (config.get("universe_overrides") or {}).get(universe) or {}
    return list((ov.get("fields") or config.get("fields") or {}).keys())


def known_universes(config: dict, registry_names: list[str] | None = None) -> set[str]:
    names = set((config.get("universes") or {}).get("available") or [])
    names |= set((config.get("universe_overrides") or {}).keys())
    names |= set(registry_names or [])
    return names


def check(doc: Doc, config: dict, registry_names: list[str] | None = None) -> list[Finding]:
    findings: list[Finding] = []
    known = known_universes(config, registry_names)
    seen_universes: set[str] = set()
    for cid, c in doc.consumers.items():
        if c.get("kind") not in KINDS:
            findings.append(Finding("ERROR", None, cid, f"kind inconnu {c.get('kind')!r} (attendu {', '.join(KINDS)})"))
        if c.get("status") not in STATUSES:
            findings.append(Finding("ERROR", None, cid, f"status inconnu {c.get('status')!r} (attendu {', '.join(STATUSES)})"))
        reads = c.get("reads") or []
        if not reads:
            findings.append(Finding("WARN", None, cid, "aucune lecture declaree"))
        for r in reads:
            u, art = str(r.get("universe")), r.get("artefact")
            seen_universes.add(u)
            if u not in known:
                findings.append(Finding("ERROR", u, cid, "univers inconnu de la config et du registre"))
            if art not in ARTEFACTS:
                findings.append(Finding("ERROR", u, cid, f"artefact inconnu {art!r} (attendu {', '.join(ARTEFACTS)})"))
            if art in ("xlsx", "store") and u in known:
                collected = set(universe_fields(u, config)) | IMPLICIT_FIELDS
                missing = [f for f in (r.get("fields") or []) if f not in collected]
                if missing:
                    findings.append(Finding("WARN", u, cid,
                                            f"champs attendus mais absents de la liste de champs de l'univers : {', '.join(missing)}"))
        if c.get("status") == "en_echec":
            findings.append(Finding("WARN", None, cid, f"consommateur en echec : {c.get('notes') or c.get('label')}"))
    for u in sorted(known - seen_universes):
        findings.append(Finding("INFO", u, None, "aucun consommateur declare"))
    return findings


def describe(universe: str, fields: list[str], config_path: str | os.PathLike | None = None,
             doc: Doc | None = None) -> tuple[str | None, list[str]]:
    """Ligne de journal et avertissements pour la passe d'un univers."""
    doc = doc or load(config_path=config_path)
    entries = doc.by_universe().get(universe, [])
    if not entries:
        return None, []
    labels = []
    warnings = []
    collected = set(fields) | IMPLICIT_FIELDS
    for cid, r in entries:
        c = doc.consumers[cid]
        labels.append(f"{c.get('label', cid)} [{c.get('status', '?')}]")
        if r.get("artefact") in ("xlsx", "store"):
            missing = [f for f in (r.get("fields") or []) if f not in collected]
            if missing:
                warnings.append(f"Universe '{universe}': {c.get('label', cid)} attend {', '.join(missing)}, "
                                "hors de la liste de champs de cette passe")
    return f"Universe '{universe}': {len(entries)} consommateur(s) declare(s) : " + " ; ".join(labels), warnings


def matrix(doc: Doc, config: dict, registry_names: list[str] | None = None) -> str:
    byu = doc.by_universe()
    lines = ["Univers | Consommateurs (statut) | Artefacts"]
    for u in sorted(known_universes(config, registry_names) | set(byu)):
        entries = byu.get(u, [])
        who = " ; ".join(f"{doc.consumers[cid].get('label', cid)} [{doc.consumers[cid].get('status', '?')}]" for cid, _ in entries) or "-"
        arts = ", ".join(sorted({str(r.get("artefact")) for _, r in entries})) or "-"
        lines.append(f"{u} | {who} | {arts}")
    return "\n".join(lines)


def _registry_names(config: dict) -> list[str]:
    try:
        from . import registry
        return list(registry.list_universes(config))
    except Exception:
        return []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rattachement univers -> consommateurs")
    parser.add_argument("command", choices=["check", "matrix"])
    parser.add_argument("--config", default=str(Path(__file__).resolve().parent.parent / "config" / "atlas_config.yaml"))
    parser.add_argument("--consumers", default=None, help="chemin de consumers.yaml (defaut : a cote de la config)")
    args = parser.parse_args(argv)
    config = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    doc = load(args.consumers, config_path=args.config)
    names = _registry_names(config)
    if args.command == "matrix":
        print(matrix(doc, config, names))
        return 0
    findings = check(doc, config, names)
    for f in findings:
        where = " / ".join(x for x in (f.universe, f.consumer) if x)
        print(f"{f.level:5s} {where}: {f.message}")
    n_err = sum(f.level == "ERROR" for f in findings)
    print(f"{len(doc.consumers)} consommateur(s), {len(findings)} constat(s), {n_err} erreur(s)")
    return 1 if n_err else 0


if __name__ == "__main__":
    sys.exit(main())
