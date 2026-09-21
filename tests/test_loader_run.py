import json

import pandas as pd
import pytest
import yaml

import bloomberg_loader as bl


@pytest.fixture
def cfg_path(tmp_path, share):
    cfg = {
        "parameters": {"start_date": "2025-01-01", "period": "D"},
        "paths": {"output_xlsx": str(tmp_path / "out" / "ATLAS_data_{universe}_static.xlsx")},
        "bloomberg": {"batch_size": 2, "ticker_suffix": " Equity", "bdh_options": {}},
        "fields": {"price": "PX_LAST", "EPS": "IS_EPS"},
        "universes": {"default": "sx5e", "available": ["sx5e"]},
        "benchmarks": {"sx5e": "SX5E Index"},
    }
    p = tmp_path / "cfg.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return p


def _loader(cfg_path, fake_blp, **kw):
    return bl.ATLASBloombergLoader(
        str(cfg_path), universe="sx5e", end_date_override="2025-03-31", blp_module=fake_blp, **kw
    )


def _latest(share):
    return json.loads((share / "univers" / "runs" / "sx5e" / "latest_default.json").read_text())


def test_full_run_writes_xlsx_atomically_and_manifest(cfg_path, fake_blp, share, tmp_path):
    loader = _loader(cfg_path, fake_blp)
    loader.run()
    out = tmp_path / "out" / "ATLAS_data_sx5e_static.xlsx"
    assert [f.name for f in out.parent.iterdir()] == [out.name]
    price = pd.read_excel(out, sheet_name="price", index_col=0)
    assert price.shape[1] == len(loader.tickers)
    assert "benchmark" in pd.ExcelFile(out).sheet_names
    m = _latest(share)
    assert m["status"] == "ok" and m["n_requested"] == len(loader.tickers)
    assert m["per_field"]["price"]["n_returned"] == len(loader.tickers)
    assert m["xlsx"]["bytes"] > 0


def test_manifest_records_failed_and_silently_missing(cfg_path, fake_blp, share):
    loader = _loader(cfg_path, fake_blp)
    t = [x + " Equity" for x in loader.tickers]
    fake_blp.raise_for = {t[0]}     # batch en echec -> fallback par ticker -> failed
    fake_blp.silent_drop = {t[2]}   # absent de la reponse, sans erreur -> missing
    loader.run()
    rep = _latest(share)["per_field"]["price"]
    assert rep["failed"] == [t[0]]
    assert rep["missing"] == [t[2]]
    assert rep["fallback_batches"] == 1
    assert _latest(share)["status"] == "partial"


def test_dry_run_writes_nothing(cfg_path, fake_blp, share, tmp_path):
    _loader(cfg_path, fake_blp, dry_run=True).run()
    assert not (tmp_path / "out").exists()
    assert not (share / "univers").exists()
    assert fake_blp.calls == []


def test_loader_reads_registry_and_honours_fetch_switches(cfg_path, fake_blp, share):
    from dl import registry
    from dl.registry import ops

    u = ops.create("sx5e", ["AAA FP", "BBB GY", "CCC NA", "DDD IM"], date="2025-01-02")
    u, _ = ops.deprecate(u, "BBB GY", "2025-02-03", "index")     # sortant : toujours extrait
    u, _ = ops.deprecate(u, "CCC NA", "2025-02-03", "index")
    u, _ = ops.toggle_fetch(u, False, "CCC NA")                   # sortant coupe
    u, _ = ops.toggle_fetch(u, False, "DDD IM")                   # actif coupe
    registry.save(u, None)

    loader = _loader(cfg_path, fake_blp)
    assert loader.tickers == ["AAA FP", "BBB GY"]
    loader.run()
    asked = {t for t in fake_blp.requested_tickers() if t.endswith(" Equity")}
    assert asked == {"AAA FP Equity", "BBB GY Equity"}
    m = _latest(share)
    assert m["ticker_source"] == "registry" and m["registry_rev"] == 1
    # le xlsx reste l'univers tradable des strategies ATLAS : pas de deprecated dedans
    out = cfg_path.parent / "out" / "ATLAS_data_sx5e_static.xlsx"
    assert list(pd.read_excel(out, sheet_name="price", index_col=0).columns) == ["AAA FP"]


def test_universe_fetch_disabled_refuses_to_run(cfg_path, fake_blp, share):
    from dl import registry
    from dl.registry import ops

    u, _ = ops.toggle_fetch(ops.create("sx5e", ["AAA FP"]), False)
    registry.save(u, None)
    with pytest.raises(ValueError, match="disabled"):
        _loader(cfg_path, fake_blp)


def test_registry_only_universe_needs_no_yaml(cfg_path, fake_blp, share):
    from dl import registry
    from dl.registry import ops

    registry.save(ops.create("mon_panier", ["SX5E Index", "MC FP Equity"], ticker_suffix=""), None)
    loader = bl.ATLASBloombergLoader(str(cfg_path), universe="mon_panier",
                                     end_date_override="2025-03-31", blp_module=fake_blp)
    assert loader.ticker_suffix == "" and loader.tickers == ["SX5E Index", "MC FP Equity"]


@pytest.fixture
def store_cfg(cfg_path):
    cfg = yaml.safe_load(cfg_path.read_text())
    cfg["store"] = {"enabled": True, "xlsx_from_store": True, "currency_fields": ["price", "EPS"]}
    cfg_path.write_text(yaml.safe_dump(cfg))
    return cfg_path


def test_store_run_layers_fx_refdata_and_joiner_backfill(store_cfg, fake_blp, share, tmp_path, monkeypatch):
    from dl import registry, store
    from dl.registry import ops

    fake_blp.refdata = {"AAA FP Equity": {"CRNCY": "EUR"}, "BBB LN Equity": {"CRNCY": "GBp"},
                        "NEW US Equity": {"CRNCY": "USD"}}
    registry.save(ops.create("sx5e", ["AAA FP", "BBB LN"], date="2025-01-02"), None)
    _loader(store_cfg, fake_blp).run()

    raw = store.read("sx5e", "price")
    eur = store.read("sx5e", "price", layer="fx_eur")
    fxr = store.read_fx()
    assert list(fxr.columns) == ["GBP"] and store.read_refdata("sx5e").at["BBB LN", "currency"] == "GBp"
    d = raw.index[5]
    assert eur.at[d, "BBB LN"] == pytest.approx(raw.at[d, "BBB LN"] / 100 / fxr.at[d, "GBP"])
    assert store.read_benchmark("sx5e")["price"].notna().any()
    m = _latest(share)
    assert m["status"] == "ok" and m["xlsx"]["source"] == "store" and m["clean_version"] == 1
    out = tmp_path / "out" / "ATLAS_data_sx5e_static.xlsx"
    assert pd.read_excel(out, sheet_name="price", index_col=0).shape[1] == 2

    # J+n : un entrant arrive ; --daily ne relit pas le xlsx et backfille l'entrant
    u = registry.load("sx5e")
    u2, o = ops.add(u, ["NEW US"], "2025-04-01", "index")
    registry.save(u2, u.rev, o)
    monkeypatch.setattr(bl.ATLASBloombergLoader, "_load_existing_xlsx",
                        lambda self: (_ for _ in ()).throw(AssertionError("xlsx relu")))
    fake_blp.calls.clear()
    loader = bl.ATLASBloombergLoader(str(store_cfg), universe="sx5e", daily=True, blp_module=fake_blp)
    loader.end_date = "2025-04-30"
    loader.run()
    starts = {c[1]: c[3] for c in fake_blp.calls if c[0] == "bdh" and c[2] == ("PX_LAST",) and "Equity" in c[1][0]}
    assert starts[("AAA FP Equity", "BBB LN Equity")] == "2025-03-31"
    assert starts[("NEW US Equity",)] == "2025-01-01"
    px = store.read("sx5e", "price")
    assert px["NEW US"].first_valid_index() == px["AAA FP"].first_valid_index()
    assert sorted(store.read_fx().columns) == ["GBP", "USD"]
    assert store.read_fx()["USD"].first_valid_index() <= pd.Timestamp("2025-01-02")
    assert pd.read_excel(out, sheet_name="price", index_col=0).shape[1] == 3

    # un sortant reste dans le store mais quitte le xlsx exporte
    u = registry.load("sx5e")
    u3, o = ops.deprecate(u, "BBB LN", "2025-05-01", "index")
    registry.save(u3, u.rev, o)
    loader = bl.ATLASBloombergLoader(str(store_cfg), universe="sx5e", daily=True, blp_module=fake_blp)
    loader.end_date = "2025-05-30"
    loader.run()
    assert list(pd.read_excel(out, sheet_name="price", index_col=0).columns) == ["AAA FP", "NEW US"]
    assert store.read("sx5e", "price")["BBB LN"].last_valid_index() == pd.Timestamp("2025-05-30")


def test_all_nan_column_is_reported_missing(cfg_path, fake_blp, share):
    loader = _loader(cfg_path, fake_blp)
    target = loader.tickers[1] + " Equity"
    real = fake_blp._series
    fake_blp._series = lambda t, f, idx: real(t, f, idx) * (float("nan") if t == target else 1)
    loader.run()
    rep = _latest(share)["per_field"]["price"]
    assert rep["missing"] == [target] and rep["n_returned"] == len(loader.tickers) - 1


@pytest.mark.parametrize("from_store", [False, True])
def test_no_ffill_fields_are_aligned_without_forward_fill(cfg_path, fake_blp, share, tmp_path, from_store):
    cfg = yaml.safe_load(cfg_path.read_text())
    cfg["fields"] = {"price": "PX_LAST", "total_return": "TR", "EPS": "IS_EPS"}
    cfg["bloomberg"]["no_ffill_fields"] = ["total_return"]
    cfg["store"] = {"enabled": from_store, "xlsx_from_store": from_store}
    cfg_path.write_text(yaml.safe_dump(cfg))
    real = fake_blp.bdh

    def sparse(tickers, flds, start_date, end_date, **kw):   # champs creux : un jour sur deux
        df = real(tickers, flds, start_date, end_date, **kw)
        return df if flds == ["PX_LAST"] else df.iloc[::2]

    fake_blp.bdh = sparse
    _loader(cfg_path, fake_blp).run()
    out = tmp_path / "out" / "ATLAS_data_sx5e_static.xlsx"
    price, tr, eps = (pd.read_excel(out, sheet_name=s, index_col=0) for s in ("price", "total_return", "EPS"))
    assert len(tr) == len(eps) == len(price)
    assert tr.iloc[1].isna().all() and tr.iloc[0].notna().all()      # rendement : pas de ffill
    assert eps.iloc[1].notna().all()                                  # fondamental : ffill conserve


def test_field_added_later_is_backfilled_on_daily(store_cfg, fake_blp, share):
    from dl import store
    _loader(store_cfg, fake_blp).run()
    cfg = yaml.safe_load(store_cfg.read_text())
    cfg["fields"]["Pxtobook"] = "PX_TO_BOOK_RATIO"
    store_cfg.write_text(yaml.safe_dump(cfg))
    loader = bl.ATLASBloombergLoader(str(store_cfg), universe="sx5e", daily=True, blp_module=fake_blp)
    loader.end_date = "2025-04-30"
    loader.run()
    assert store.read("sx5e", "Pxtobook").index.min() == store.read("sx5e", "price").index.min()
