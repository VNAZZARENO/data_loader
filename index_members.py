#!/usr/bin/env python3
"""
Refresh a universe's ticker CSV from the live Bloomberg index membership.

`--update-universe` on the main loader calls in here. The authoritative source
for "who is in the index right now" is Bloomberg itself:

    blp.bds('SXXR Index', 'INDX_MEMBERS')

which returns the current constituents *with their Bloomberg tickers*, so there
is no name->ticker mapping to guess (unlike the STOXX components PDF, which
carries only company names and, on the public site, lags the real index by
years).

The one wrinkle: INDX_MEMBERS returns Bloomberg's standard exchange codes
(e.g. `ROP SW`, `SAN SM`), while this project's CSVs use a local convention
(`ROP SE`, `SAN SQ`). We do NOT hard-code that mapping: it is *learned* from the
existing CSV by matching on the ticker root, so continuing constituents keep
their exact current form and only genuine joiners go through the derived map
(with an optional manual override from config). This keeps the git diff to the
real membership changes instead of a convention-wide churn.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
from collections import Counter, defaultdict

logger = logging.getLogger(__name__)

# xbbg lower-cases and snake-cases the BDS field names; the INDX_MEMBERS bulk
# field comes back under one of these depending on the xbbg/blpapi version.
_MEMBER_COLS = (
    "member_ticker_and_exchange_code",
    "member_ticker_and_exchange_cod",  # some versions truncate the header
    "member_ticker",
)


def _split_ticker(t: str) -> tuple[str, str]:
    """('ROP SE') -> ('ROP', 'SE'); tolerates roots with '/' like 'AV/ LN'."""
    root, _, exch = t.strip().rpartition(" ")
    if not root:  # no space at all -> no exchange code
        return t.strip(), ""
    return root, exch


def fetch_index_members(index: str, blp_module) -> list[str]:
    """Return the current constituents of `index` as raw 'TICKER EXCH' strings.

    Uses BDS INDX_MEMBERS. Raises on an empty / unexpected response so the
    caller never silently overwrites a good CSV with nothing.
    """
    df = blp_module.bds(index, "INDX_MEMBERS")
    if df is None or df.empty:
        raise RuntimeError(f"INDX_MEMBERS returned no rows for {index!r}")

    col = next((c for c in _MEMBER_COLS if c in df.columns), None)
    if col is None:
        # fall back to the first column but make the guess visible
        col = list(df.columns)[0]
        logger.warning(
            "INDX_MEMBERS: expected member column not found, using %r "
            "(columns were: %s)",
            col,
            list(df.columns),
        )

    members = [str(v).strip() for v in df[col] if str(v).strip() and str(v) != "nan"]
    if not members:
        raise RuntimeError(f"INDX_MEMBERS for {index!r} yielded 0 usable tickers")
    logger.info("INDX_MEMBERS(%s): %d constituents", index, len(members))
    return members


def learn_exchange_map(
    raw_members: list[str], existing_tickers: list[str]
) -> dict[str, str]:
    """Derive {bloomberg_exch -> project_exch} from the current CSV.

    For every incoming member whose root maps to a single existing ticker, we
    record how that root is spelled in the CSV. The majority spelling per
    Bloomberg exchange code wins. This is what lets `ROP SW` become `ROP SE`
    without anyone writing that rule down.
    """
    existing_by_root: dict[str, set[str]] = defaultdict(set)
    for t in existing_tickers:
        root, exch = _split_ticker(t)
        existing_by_root[root].add(exch)

    votes: dict[str, Counter] = defaultdict(Counter)
    for m in raw_members:
        root, bexch = _split_ticker(m)
        exset = existing_by_root.get(root)
        if exset and len(exset) == 1:
            votes[bexch][next(iter(exset))] += 1

    learned = {bexch: c.most_common(1)[0][0] for bexch, c in votes.items()}
    changed = {k: v for k, v in learned.items() if k != v}
    if changed:
        logger.info("Learned exchange-code remaps from existing CSV: %s", changed)
    return learned


def reconcile_to_convention(
    raw_members: list[str],
    existing_tickers: list[str],
    override_map: dict[str, str] | None = None,
) -> tuple[list[str], dict]:
    """Map Bloomberg members to the project's ticker convention.

    Returns (new_tickers_sorted, info) where info carries the joiners/leavers
    diff and the exchange map actually applied.
    """
    learned = learn_exchange_map(raw_members, existing_tickers)
    exch_map = {**learned, **(override_map or {})}

    seen: set[str] = set()
    new_tickers: list[str] = []
    unmapped: set[str] = set()
    known_project_exch = {_split_ticker(t)[1] for t in existing_tickers}

    for m in raw_members:
        root, bexch = _split_ticker(m)
        pexch = exch_map.get(bexch, bexch)
        if pexch not in known_project_exch and bexch not in exch_map:
            unmapped.add(bexch)
        ticker = f"{root} {pexch}".strip()
        if ticker not in seen:
            seen.add(ticker)
            new_tickers.append(ticker)

    new_tickers.sort()
    old_set, new_set = set(existing_tickers), set(new_tickers)
    info = {
        "n_old": len(existing_tickers),
        "n_new": len(new_tickers),
        "joiners": sorted(new_set - old_set),
        "leavers": sorted(old_set - new_set),
        "exchange_map": exch_map,
        "unmapped_exchanges": sorted(unmapped),
    }
    return new_tickers, info


def _render_diff(universe: str, index: str, info: dict) -> str:
    """Human-readable add/remove summary shown before writing."""
    w = 60
    lines = [
        "=" * w,
        f"  Mise a jour de l'univers '{universe}'  <-  {index}",
        "=" * w,
    ]
    joiners, leavers = info["joiners"], info["leavers"]
    lines.append(f"  {len(joiners)} ajout(s) :")
    lines += [f"    + {t}" for t in joiners] or ["    (aucun)"]
    lines.append(f"  {len(leavers)} suppression(s) :")
    lines += [f"    - {t}" for t in leavers] or ["    (aucune)"]
    lines.append("-" * w)
    lines.append(f"  {info['n_old']} -> {info['n_new']} tickers")
    return "\n".join(lines)


def _confirm(prompt: str = "  Accepter ? [Y/n] ") -> bool:
    """Prompt on the terminal. Empty / y / yes / o / oui -> accept.

    Non-interactive stdin (cron, pipe) returns False so an unattended run never
    silently rewrites the CSV — pass assume_yes / --yes for that.
    """
    if not sys.stdin or not sys.stdin.isatty():
        logger.warning(
            "  Non-interactive stdin: not writing. Re-run with --yes to confirm."
        )
        return False
    try:
        answer = input(prompt).strip().lower()
    except EOFError:
        return False
    return answer in ("", "y", "yes", "o", "oui")


def _read_existing(csv_path: str) -> list[str]:
    if not os.path.isfile(csv_path):
        return []
    import csv as _csv

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = _csv.DictReader(f)
        if "Ticker" not in (reader.fieldnames or []):
            raise ValueError(f"Ticker file missing 'Ticker' column: {csv_path}")
        return [r["Ticker"].strip() for r in reader if r["Ticker"].strip()]


def _write_csv(csv_path: str, tickers: list[str]) -> None:
    """Write matching the existing convention: CRLF, 'Ticker' header, no trailing blank."""
    with open(csv_path, "w", newline="") as f:
        f.write("Ticker\r\n")
        f.write("\r\n".join(tickers))
        f.write("\r\n")


def refresh_universe_csv(
    *,
    universe: str,
    index: str,
    csv_path: str,
    blp_module,
    override_map: dict[str, str] | None = None,
    dry_run: bool = False,
    assume_yes: bool = False,
) -> dict:
    """Refresh tickers/<universe>.csv from INDX_MEMBERS(index).

    Shows an add/remove summary and asks for confirmation before writing (skip
    with assume_yes / --yes). Guardrails: aborts (raises) on an implausibly small
    member count so a bad Bloomberg response can never wipe the list. Backs up the
    previous CSV to <csv_path>.bak before writing. In dry_run, queries Bloomberg
    and prints the diff but writes nothing.
    """
    logger.info(
        "Refreshing universe %r from %s (%s)",
        universe,
        index,
        "DRY RUN — no write" if dry_run else "will write",
    )
    existing = _read_existing(csv_path)
    raw_members = fetch_index_members(index, blp_module)
    new_tickers, info = reconcile_to_convention(raw_members, existing, override_map)

    # Sanity guardrail: never overwrite with a suspiciously short list.
    if existing and info["n_new"] < 0.5 * info["n_old"]:
        raise RuntimeError(
            f"Refusing to write: {index} returned {info['n_new']} tickers vs "
            f"{info['n_old']} existing (>50% drop). Check the index/response."
        )

    if info["unmapped_exchanges"]:
        logger.warning(
            "  Exchange codes with no learned/override mapping (kept as-is, "
            "verify these tickers pull data): %s",
            ", ".join(info["unmapped_exchanges"]),
        )

    # Show the add/remove summary on stdout regardless of log level.
    print("\n" + _render_diff(universe, index, info), flush=True)
    info["written"] = False

    if dry_run:
        print("  [DRY RUN] aucune ecriture.", flush=True)
        return info

    if existing and not info["joiners"] and not info["leavers"]:
        print("  Aucun changement — rien a ecrire.", flush=True)
        return info

    if not assume_yes and not _confirm():
        print("  Annule — aucune ecriture.", flush=True)
        logger.info("Update cancelled by user; %s left unchanged", csv_path)
        return info

    if existing:
        backup = csv_path + ".bak"
        shutil.copy2(csv_path, backup)
        logger.info("  Backed up previous list to %s", backup)
    _write_csv(csv_path, new_tickers)
    info["written"] = True
    logger.info("  Wrote %d tickers to %s", len(new_tickers), csv_path)
    return info
