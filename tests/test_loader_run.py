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
