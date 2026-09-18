import pandas as pd
import pytest

from dl import registry
from dl.registry import events, ops, pit
from dl.registry.model import RegistryError, Universe


def _u(tickers=("AAA FP", "BBB GY", "ROG SE"), date="2025-01-02"):
    return ops.create("demo", list(tickers), date=date)


def test_create_and_roundtrip():
    u = _u()
    assert u.active_tickers() == ["AAA FP", "BBB GY", "ROG SE"]
    assert Universe.from_dict(u.to_dict()) == u
    with pytest.raises(RegistryError):
        ops.create("Bad Name", [])


def test_deprecate_keeps_member_and_fetch():
    u, o = ops.deprecate(_u(), "BBB GY", "2025-06-30", "index")
    assert u.deprecated_tickers() == ["BBB GY"]
    assert "BBB GY" in u.fetch_tickers()          # sortant toujours extrait
    assert o[0]["op"] == "deprecate"
    with pytest.raises(RegistryError):
        ops.deprecate(u, "BBB GY")


def test_fetch_switches():
    u, _ = ops.toggle_fetch(_u(), False, "AAA FP")
    assert u.fetch_tickers() == ["BBB GY", "ROG SE"]
    u, _ = ops.toggle_fetch(u, False)
    assert u.fetch_tickers() == []


def test_reentry_creates_second_period():
    u, _ = ops.deprecate(_u(), "AAA FP", "2025-03-01")
    u, o = ops.add(u, ["AAA FP"], "2025-09-01")
    assert o[0]["op"] == "reactivate"
    assert [(p.entry, p.exit) for p in u.member("AAA FP").periods] == [
        ("2025-01-02", "2025-03-01"), ("2025-09-01", None)]
    u.validate()


def test_apply_index_diff_and_guardrail():
    u, o = ops.apply_index_diff(_u(), ["AAA FP", "ROG SE", "CCC NA"], "2025-07-01")
    assert u.active_tickers() == ["AAA FP", "ROG SE", "CCC NA"]
    assert u.deprecated_tickers() == ["BBB GY"]
    assert {x["op"] for x in o} == {"add", "deprecate"}
    with pytest.raises(RegistryError, match="Garde-fou"):
        ops.apply_index_diff(_u(), ["AAA FP"])


def test_rename_link_and_candidates():
    u, _ = ops.apply_index_diff(_u(), ["AAA FP", "BBB GY", "ROP SE"], "2025-07-01")
    assert {"from": "ROG SE", "to": "ROP SE"}.items() <= ops.rename_candidates(u)[0].items()
    u, _ = ops.link_rename(u, "ROG SE", "ROP SE")
    assert u.member("ROP SE").predecessor == "ROG SE"
    assert ops.rename_candidates(u) == []
    with pytest.raises(RegistryError):
        ops.link_rename(u, "ROG SE", "AAA FP")
    u, _ = ops.remove(u, "ROG SE")
    assert u.member("ROP SE").predecessor is None


def test_ops_are_pure():
    u = _u()
    before = u.to_dict()
    ops.deprecate(u, "AAA FP")
    ops.add(u, ["ZZZ FP"])
    assert u.to_dict() == before


def test_combine_clone_filter():
    a, b = _u(), ops.create("other", ["AAA FP", "DDD FP"])
    assert ops.combine("c1", [a, b]).active_tickers() == ["AAA FP", "BBB GY", "ROG SE", "DDD FP"]
    assert ops.combine("c2", [a, b], "intersection").active_tickers() == ["AAA FP"]
    assert ops.combine("c3", [a, b], "difference").active_tickers() == ["BBB GY", "ROG SE"]
    assert ops.filter_("f1", a, exchanges=["FP"]).active_tickers() == ["AAA FP"]
    dep, _ = ops.deprecate(a, "BBB GY", "2025-05-01")
    c = ops.clone("cl", dep)
    assert c.rev == 0 and c.deprecated_tickers() == ["BBB GY"]
    mixed = ops.create("mixed", ["SX5E Index"], ticker_suffix="")
    with pytest.raises(RegistryError):
        ops.combine("c4", [a, mixed])


def test_repo_save_load_rev_conflict_and_events(share):
    u = registry.save(_u(), None, [{"op": "create"}], actor="test", source="migration")
    assert u.rev == 1 and registry.list_universes() == ["demo"]
    loaded = registry.load("demo")
    assert loaded == u
    u2, o = ops.deprecate(loaded, "AAA FP", "2025-02-01")
    registry.save(u2, 1, o)
    with pytest.raises(registry.RevConflict):      # deuxieme onglet, rev perimee
        registry.save(u2, 1, o)
    with pytest.raises(registry.RevConflict):      # creation d'un univers deja existant
        registry.save(_u(), None)
    cs = events.list_changesets("demo")
    assert [c["rev_to"] for c in cs] == [2, 1] and cs[0]["ops"][0]["ticker"] == "AAA FP"


def test_pit_mask_null_entry_reentry_and_splice():
    idx = pd.bdate_range("2025-01-01", "2025-12-31")
    u = ops.create("demo", ["AAA FP", "ROG SE"], date=None, entry_source="seed")
    for m in u.members:
        m.periods[0].entry = None
    u, _ = ops.deprecate(u, "AAA FP", "2025-03-03")
    u, _ = ops.add(u, ["AAA FP"], "2025-09-01")
    u, _ = ops.apply_index_diff(u, ["AAA FP", "ROP SE"], "2025-07-01")
    u, _ = ops.link_rename(u, "ROG SE", "ROP SE")

    mask = pit.membership_mask(u, idx, first_valid={"AAA FP": pd.Timestamp("2025-02-03")})
    assert list(mask.columns) == ["AAA FP", "ROP SE"]            # predecesseur recolle
    a = mask["AAA FP"]
    assert not a["2025-01-31"] and a["2025-02-03"] and a["2025-02-28"]
    assert not a["2025-03-03"] and not a["2025-08-29"] and a["2025-09-01"]
    assert mask["ROP SE"].all()                                   # membre continu malgre le renommage

    raw = pit.membership_mask(u, idx, splice_renames=False)
    assert raw["ROG SE"]["2025-06-30"] and not raw["ROG SE"]["2025-07-01"]
    assert not raw["ROP SE"]["2025-06-30"] and raw["ROP SE"]["2025-07-01"]
    assert pit.count_series(u, idx).loc["2025-07-01"] == 1       # AAA dehors, ROP dedans
