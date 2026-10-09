import logging
from pathlib import Path

import pytest
import yaml

import bloomberg_loader as bl
from dl import consumers

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def cfg_path(tmp_path, share):
    cfg = {
        "parameters": {"start_date": "2025-01-01", "period": "D"},
        "paths": {"output_xlsx": str(tmp_path / "out" / "ATLAS_data_{universe}_static.xlsx")},
        "bloomberg": {"batch_size": 2, "ticker_suffix": " Equity", "bdh_options": {}},
        "fields": {"price": "PX_LAST", "EPS": "IS_EPS"},
        "universes": {"default": "sx5e", "available": ["sx5e"]},
    }
    p = tmp_path / "cfg.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return p


def _doc(tmp_path, body):
    p = tmp_path / "consumers.yaml"
    p.write_text(yaml.safe_dump({"version": 1, "consumers": body}, allow_unicode=True))
    return p


def _cfg():
    return {"fields": {"price": "PX_LAST", "EPS": "IS_EPS"},
            "universes": {"available": ["sx5e", "jp"]},
            "universe_overrides": {"sx5e": {"fields": {"price": "PX_LAST", "EPS": "IS_EPS", "shares_out": "EQY_SH_OUT"}}}}


def test_check_flags_unknown_universe_and_uncollected_field(tmp_path):
    doc = consumers.load(_doc(tmp_path, {
        "a": {"label": "A", "kind": "shadow", "status": "actif",
              "reads": [{"universe": "sx5e", "artefact": "xlsx", "fields": ["price", "div_yield", "benchmark"]}]},
        "b": {"label": "B", "kind": "service", "status": "en_echec",
              "reads": [{"universe": "nope", "artefact": "api", "fields": ["price"]}]},
    }))
    findings = consumers.check(doc, _cfg())
    msgs = {(f.level, f.universe, f.consumer): f.message for f in findings}
    assert ("WARN", "sx5e", "a") in msgs and "div_yield" in msgs[("WARN", "sx5e", "a")] and "benchmark" not in msgs[("WARN", "sx5e", "a")]
    assert ("ERROR", "nope", "b") in msgs
    assert any(f.level == "WARN" and f.consumer == "b" and "echec" in f.message for f in findings)
    assert ("INFO", "jp", None) in msgs          # univers sans consommateur


def test_describe_and_loader_log(tmp_path, cfg_path, fake_blp, caplog):
    _doc(tmp_path, {
        "v3": {"label": "v3 pipeline", "kind": "prod", "status": "actif",
               "reads": [{"universe": "sx5e", "artefact": "xlsx", "fields": ["EPS", "Pxtobook"]}]},
    })
    summary, warnings = consumers.describe("sx5e", ["price", "EPS"], config_path=cfg_path)
    assert "1 consommateur(s)" in summary and "v3 pipeline [actif]" in summary
    assert warnings == ["Universe 'sx5e': v3 pipeline attend Pxtobook, hors de la liste de champs de cette passe"]
    assert consumers.describe("jp", ["price"], config_path=cfg_path) == (None, [])
    loader = bl.ATLASBloombergLoader(str(cfg_path), universe="sx5e", end_date_override="2025-01-31", blp_module=fake_blp, dry_run=True)
    with caplog.at_level(logging.INFO):
        loader.run()
    assert any("1 consommateur(s)" in r.message for r in caplog.records)
    assert any("attend Pxtobook" in r.message and r.levelno == logging.WARNING for r in caplog.records)


def test_real_consumers_file_is_consistent():
    config = yaml.safe_load((REPO / "config" / "atlas_config.yaml").read_text())
    doc = consumers.load(REPO / "config" / "consumers.yaml")
    assert len(doc.consumers) >= 20
    registry_names = ["macro", "macro_inflation"]           # univers du registre seul
    findings = consumers.check(doc, config, registry_names)
    assert [f for f in findings if f.level == "ERROR"] == []
    byu = doc.by_universe()
    assert {"sxxr", "igv", "pbh", "global_macro", "macro", "jp", "option_europe"} <= set(byu)
    # la passe sxxr couvre tout ce que ses consommateurs xlsx/store attendent
    sxxr_warn = [f for f in findings if f.universe == "sxxr" and f.level == "WARN" and "absents" in f.message]
    assert [f.consumer for f in sxxr_warn] == ["atlas_stoxx600_v2_shadow"]
    print(consumers.matrix(doc, config, registry_names))
