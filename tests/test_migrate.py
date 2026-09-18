import subprocess

import pandas as pd

from dl import migrate, registry

CFG = {"bloomberg": {"ticker_suffix": " Equity"}, "benchmarks": {"demo": "DEMO Index"},
       "universe_overrides": {"mixed": {"ticker_suffix": ""}}}


def _csv(path, tickers):
    path.write_bytes(("Ticker\r\n" + "".join(f"{t}\r\n" for t in tickers)).encode())


def test_seed_is_idempotent_and_entries_unknown(share, tmp_path):
    tdir = tmp_path / "tickers"
    tdir.mkdir()
    _csv(tdir / "demo.csv", ["AAA FP", "BBB GY"])
    _csv(tdir / "mixed.csv", ["SX5E Index"])
    assert migrate.seed(["demo", "mixed"], CFG, dry_run=True, tickers_dir=tdir)["demo"] == "2 tickers"
    assert registry.list_universes() == []
    migrate.seed(["demo", "mixed"], CFG, tickers_dir=tdir)
    u = registry.load("demo")
    assert u.kind == "index" and u.source["index"] == "DEMO Index"
    assert all(m.periods[0].entry is None and m.periods[0].entry_source == "seed" for m in u.members)
    assert registry.load("mixed").ticker_suffix == ""
    assert migrate.seed(["demo"], CFG, tickers_dir=tdir)["demo"] == "deja au registre"
    assert registry.load("demo").rev == 1


def test_backfill_leavers_from_git_and_bak(share, tmp_path):
    repo = tmp_path / "repo"
    (repo / "tickers").mkdir(parents=True)
    git = lambda *a: subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True)
    git("init", "-q")
    git("config", "user.email", "t@t"); git("config", "user.name", "t")
    _csv(repo / "tickers" / "demo.csv", ["AAA FP", "ROG SE"])
    git("add", "-A"); git("commit", "-qm", "v1", "--date=2025-01-10T00:00:00")
    _csv(repo / "tickers" / "demo.csv", ["AAA FP", "ROP SE"])
    _csv(repo / "tickers" / "demo.csv.bak_x", ["AAA FP", "BAK LN"])
    git("add", "-A"); git("commit", "-qm", "v2", "--date=2025-07-24T00:00:00")

    migrate.seed(["demo"], CFG, tickers_dir=repo / "tickers")
    r = migrate.backfill_leavers("demo", CFG, use_xlsx=False, repo=repo)
    assert r["added"] == ["BAK LN", "ROG SE"]
    u = registry.load("demo")
    old = u.member("ROG SE")
    assert not old.active and old.fetch and old.periods[0].exit == "2025-07-24"
    assert old.periods[0].exit_source == "git" and old.periods[0].entry is None
    assert any(c["from"] == "ROG SE" and c["to"] == "ROP SE" for c in r["rename_candidates"])
    assert u.member("ROP SE").predecessor is None            # jamais lie automatiquement
    assert r["entries_dated"] == ["ROP SE"]                   # revue equilibree : l'entrant est date
    assert u.member("ROP SE").periods[0].entry == "2025-07-24" and u.member("AAA FP").periods[0].entry is None
    assert migrate.backfill_leavers("demo", CFG, use_xlsx=False, repo=repo)["added"] == []


def test_last_move_dates_ignores_ffilled_tail():
    idx = pd.bdate_range("2025-01-01", periods=6)
    px = pd.DataFrame({"DEAD": [1, 2, 3, 3, 3, 3], "LIVE": [1, 2, 3, 4, 5, 6], "FLAT": [1.0] * 6}, index=idx)
    d = migrate.last_move_dates(px)
    assert d["DEAD"] == idx[2] and d["LIVE"] == idx[5] and pd.isna(d["FLAT"])


def test_backfill_ignores_case_fixes(share, tmp_path, monkeypatch):
    tdir = tmp_path / "tickers"
    tdir.mkdir()
    _csv(tdir / "demo.csv", ["ALBON FP"])
    migrate.seed(["demo"], CFG, tickers_dir=tdir)
    monkeypatch.setattr(migrate, "leavers_from_git", lambda u, repo: {"albon fp": "2025-08-07", "GONE FP": "2025-08-07"})
    monkeypatch.setattr(migrate, "leavers_from_bak", lambda u, repo: {})
    assert migrate.backfill_leavers("demo", CFG, use_xlsx=False)["added"] == ["GONE FP"]
