"""Migration vers le registre et le store. Idempotent ; ``--dry-run`` partout.

    python -m dl.migrate seed --all
    python -m dl.migrate backfill-leavers --universe sxxr
    python -m dl.migrate import-xlsx --universe sxxr [--profile conviction]
"""

from __future__ import annotations

import argparse
import csv
import logging
import subprocess
from pathlib import Path

import pandas as pd
import yaml

from . import paths, registry
from .registry import ops

logger = logging.getLogger("dl.migrate")
REPO = Path(__file__).resolve().parent.parent  # seeds tickers/*.csv uniquement, jamais le partage
SIDECARS = {"jp_names", "jp_sectors", "option_europe_bt"}  # sidecars + cache du mode bt


def load_config(path: str | None = None) -> dict:
    with open(path or REPO / "config" / "atlas_config.yaml") as f:
        return yaml.safe_load(f)


def read_csv_tickers(path: Path) -> list[str]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return [r["Ticker"].strip() for r in csv.DictReader(f) if (r.get("Ticker") or "").strip()]


def seed_universe(universe: str, config: dict, tickers_dir: Path | None = None):
    """CSV -> registre. Dates d'entree inconnues (``entry: null, entry_source: seed``)."""
    tickers = read_csv_tickers((tickers_dir or REPO / "tickers") / f"{universe}.csv")
    ov = config.get("universe_overrides", {}).get(universe, {})
    index = (config.get("index_members", {}).get("index_override", {}) or {}).get(universe) \
        or config.get("benchmarks", {}).get(universe)
    u = ops.create(
        universe, tickers, label=universe,
        kind="index" if index else "list",
        ticker_suffix=ov.get("ticker_suffix", config["bloomberg"]["ticker_suffix"]),
        source={"type": "bbg_index", "index": index, "auto_apply": False} if index else {"type": "manual"},
        benchmark=config.get("benchmarks", {}).get(universe),
        entry_source="seed",
    )
    for m in u.members:
        m.periods[0].entry = None
    # la vraie date de derniere revue de la composition est celle du CSV, pas celle du seed
    u.source["seed_date"] = csv_last_commit_date(universe, (tickers_dir or REPO / "tickers").parent)
    return u


def csv_last_commit_date(universe: str, repo: Path = REPO) -> str | None:
    try:
        out = subprocess.run(["git", "-C", str(repo), "log", "-1", "--format=%ad", "--date=short", "--",
                              f"tickers/{universe}.csv"], capture_output=True, text=True, check=True).stdout.strip()
        return out or None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def seed(universes: list[str], config: dict, dry_run: bool = False, tickers_dir: Path | None = None) -> dict:
    out = {}
    for name in universes:
        if registry.exists(name, config):
            out[name] = "deja au registre"
            continue
        u = seed_universe(name, config, tickers_dir)
        out[name] = f"{len(u.members)} tickers"
        if not dry_run:
            registry.save(u, None, [{"op": "seed", "n": len(u.members)}],
                          actor="migrate", source="migration", config=config)
    return out


# -- sortants --------------------------------------------------------------

def leavers_from_git(universe: str, repo: Path = REPO) -> dict[str, str]:
    """{ticker: date du commit qui l'a retire de tickers/<u>.csv}."""
    rel = f"tickers/{universe}.csv"
    try:
        log = subprocess.run(
            ["git", "-C", str(repo), "log", "--follow", "-p", "--format=@@@%ad", "--date=short", "--", rel],
            capture_output=True, text=True, check=True).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return {}
    removed: dict[str, str] = {}
    date = None
    for line in log.splitlines():  # du plus recent au plus ancien
        if line.startswith("@@@"):
            date = line[3:]
        elif line.startswith("-") and not line.startswith("---"):
            t = line[1:].strip()
            if t and t != "Ticker":
                removed.setdefault(t, date)
    return removed


def rebalances_from_git(universe: str, repo: Path = REPO) -> dict[str, str]:
    """{entrant: date} pour les commits qui ressemblent a une revue d'indice : des sorties ET des
    entrees en nombre comparable (sxxr 2026-07-24 : 59/59). Un rattrapage de liste desequilibre
    (pbh 145 -> 208) n'est pas une date d'entree et est ignore."""
    rel = f"tickers/{universe}.csv"
    try:
        log = subprocess.run(
            ["git", "-C", str(repo), "log", "--follow", "-p", "--format=@@@%ad", "--date=short", "--", rel],
            capture_output=True, text=True, check=True).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return {}
    commits: list[tuple[str, set, set]] = []
    for line in log.splitlines():
        if line.startswith("@@@"):
            commits.append((line[3:], set(), set()))
        elif commits and line[:1] in "+-" and not line.startswith(("---", "+++")):
            t = line[1:].strip()
            if t and t != "Ticker":
                commits[-1][1 if line[0] == "+" else 2].add(t)
    out: dict[str, str] = {}
    for date, added, removed in commits[:-1]:            # le dernier = creation du fichier
        moved_in, moved_out = added - removed, removed - added
        if moved_out and 0.5 <= len(moved_in) / len(moved_out) <= 2:
            for t in moved_in:
                out.setdefault(t, date)
    return out


def leavers_from_bak(universe: str, repo: Path = REPO) -> dict[str, str]:
    out: dict[str, str] = {}
    csv_path = repo / "tickers" / f"{universe}.csv"
    try:
        date = subprocess.run(["git", "-C", str(repo), "log", "-1", "--format=%ad", "--date=short",
                               "--", f"tickers/{universe}.csv"],
                              capture_output=True, text=True, check=True).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        date = ""
    for bak in sorted(csv_path.parent.glob(f"{universe}.csv.bak*")):
        for t in read_csv_tickers(bak):
            out.setdefault(t, date or None)
    return out


def last_move_dates(price: pd.DataFrame) -> pd.Series:
    """Derniere date ou le prix brut bouge (Bloomberg forward-fill un titre mort)."""
    moved = price.diff().abs() > 0
    return moved.apply(lambda c: c[c].index.max() if c.any() else pd.NaT)


def legacy_xlsx_files(universe: str, config: dict) -> list[Path]:
    return sorted(paths.share_root(config).glob(f"ATLAS_data_{universe}_static*.xlsx"))


def leavers_from_xlsx(universe: str, config: dict) -> dict[str, str]:
    """Colonnes des xlsx absentes de la liste : le concat --daily a garde les sortants."""
    out: dict[str, str] = {}
    for f in legacy_xlsx_files(universe, config):
        if f.stem.endswith("_test"):
            continue
        logger.info("Lecture de %s (feuille price)", f)
        price = pd.read_excel(f, sheet_name="price", index_col=0)
        price.index = pd.to_datetime(price.index)
        for t, d in last_move_dates(price).items():
            if pd.notna(d):
                out[str(t)] = max(out.get(str(t), ""), d.date().isoformat())
    return out


def backfill_leavers(universe: str, config: dict, dry_run: bool = False, use_xlsx: bool = True,
                     repo: Path = REPO) -> dict:
    u = registry.load(universe, config)
    known = {m.ticker for m in u.members}
    found: dict[str, tuple[str | None, str]] = {}
    if use_xlsx:
        for t, d in leavers_from_xlsx(universe, config).items():
            found.setdefault(t, (d, "xlsx"))
    for src in (leavers_from_git(universe, repo), leavers_from_bak(universe, repo)):
        for t, d in src.items():
            found.setdefault(t, (d, "git"))

    # une correction de casse dans git ('albon fp' -> 'ALBON FP') n'est pas une sortie
    known_upper = {t.upper() for t in known}
    new = {t: v for t, v in found.items() if t.upper() not in known_upper}
    all_ops: list[dict] = []
    for t, (date, source) in sorted(new.items()):
        u, o = ops.add(u, [t], None, source)
        u.member(t).periods[0].entry = None
        u, o2 = ops.deprecate(u, t, date, source)
        all_ops += o + o2
    dated = []
    for t, date in rebalances_from_git(universe, repo).items():
        m = u.member(t)
        p0 = m.periods[0] if m and len(m.periods) == 1 else None
        if p0 is not None and m.active and p0.entry_source in ("seed", "first_valid"):
            p0.entry, p0.entry_source = date, "git"
            dated.append(t)
            all_ops.append({"op": "set_entry", "ticker": t, "date": date, "source": "git"})
    report = {"universe": universe, "added": sorted(new), "entries_dated": sorted(dated),
              "rename_candidates": ops.rename_candidates(u)}
    if (new or dated) and not dry_run:
        registry.save(u, u.rev, all_ops, actor="migrate", source="migration", config=config)
    return report


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=None)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--all", action="store_true")
    s.add_argument("--universe", action="append", default=[])
    b = sub.add_parser("backfill-leavers")
    b.add_argument("--universe", required=True)
    b.add_argument("--no-xlsx", action="store_true", help="git + .bak seulement (rapide)")
    i = sub.add_parser("import-xlsx")
    i.add_argument("--universe", required=True)
    i.add_argument("--profile", default=None)
    for p in (s, b, i):
        p.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    config = load_config(args.config)
    print(f"Partage : {paths.share_root(config)}")

    if args.cmd == "seed":
        names = args.universe or []
        if args.all:
            names = sorted(f.stem for f in (REPO / "tickers").glob("*.csv") if f.stem not in SIDECARS)
        for k, v in seed(names, config, args.dry_run).items():
            print(f"  {k:24s} {v}")
    elif args.cmd == "backfill-leavers":
        r = backfill_leavers(args.universe, config, args.dry_run, use_xlsx=not args.no_xlsx)
        print(f"  {len(r['added'])} sortant(s) ajoute(s) : {', '.join(r['added'][:30])}")
        print(f"  {len(r['entries_dated'])} entrant(s) date(s) depuis git")
        for c in r["rename_candidates"]:
            print(f"  candidat renommage : {c['from']} -> {c['to']} ({c['reason']})")
    elif args.cmd == "import-xlsx":
        from .store import legacy_xlsx
        r = legacy_xlsx.import_universe(args.universe, config, profile=args.profile, dry_run=args.dry_run)
        print(r)
    if args.dry_run:
        print("[DRY RUN] rien n'a ete ecrit")


if __name__ == "__main__":
    main()
