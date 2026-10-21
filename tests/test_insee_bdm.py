from pathlib import Path

import pandas as pd
import pytest
import yaml as _yaml

from dl import consumers, registry, store
from dl.sources import insee_bdm

FIXTURE = Path(__file__).parent / "fixtures" / "insee_bdm_sample.xml"
REPO = Path(__file__).resolve().parents[1]


def test_spec_couvre_des_zones_connues():
    spec = insee_bdm.load_spec()
    assert spec["universe"] == "hotellerie_fr"
    for champ, f in spec["fields"].items():
        assert set(f["idbanks"]) <= set(spec["zones"]), champ
        assert all(len(i) == 9 and i.isdigit() for i in f["idbanks"].values()), champ


def test_parse_sdmx_dates_fin_de_mois_et_attributs():
    got = insee_bdm.parse_sdmx(FIXTURE.read_text(encoding="utf-8"))
    to = got["010599343"]
    assert to["unit_measure"] == "POURCENT" and to["unit_mult"] == "0"
    assert to["title"].startswith("Taux d'occupation dans l'hôtellerie")
    assert to["points"].index.max() == pd.Timestamp("2026-08-31")
    assert to["points"].dtype == float
    assert all(d.is_month_end for d in to["points"].index)


def test_fetch_par_paquets():
    vus = []

    class R:
        status_code = 200
        text = FIXTURE.read_text(encoding="utf-8")

        def raise_for_status(self):
            pass

    def faux_get(url, **kw):
        vus.append(url)
        return R()

    insee_bdm.fetch([f"{i:09d}" for i in range(45)], "2026-01", get=faux_get, chunk=20)
    assert len(vus) == 3
    assert "startPeriod=2026-01" in vus[0] and vus[0].count("+") == 19


def _serie(unit_measure="POURCENT", unit_mult="0"):
    return {"title": "t", "unit_measure": unit_measure, "unit_mult": unit_mult, "last_update": "2026-10-13",
            "points": pd.Series([60.0, 61.5], index=pd.to_datetime(["2026-07-31", "2026-08-31"]))}


SPEC_MINI = {"universe": "hotellerie_fr", "zones": {"FR_METRO": "France", "D74": "Haute-Savoie"},
             "fields": {"taux_occupation": {"unit": "%", "unit_measure": "POURCENT", "unit_mult": "0",
                                            "idbanks": {"FR_METRO": "000000001", "D74": "000000002"}}}}


def test_to_frames_tickers_en_colonnes():
    frames, releve = insee_bdm.to_frames(SPEC_MINI, {"000000001": _serie(), "000000002": _serie()})
    df = frames["taux_occupation"]
    assert list(df.columns) == ["FR_METRO", "D74"]
    assert df.loc["2026-08-31", "D74"] == 61.5
    assert {r["ticker"] for r in releve} == {"FR_METRO", "D74"} and releve[0]["n_obs"] == 2


def test_unite_differente_arrete_la_passe():
    with pytest.raises(insee_bdm.UniteInattendue, match="000000002"):
        insee_bdm.to_frames(SPEC_MINI, {"000000001": _serie(), "000000002": _serie(unit_mult="3")})


def test_serie_absente_arrete_la_passe():
    with pytest.raises(insee_bdm.UniteInattendue, match="absente"):
        insee_bdm.to_frames(SPEC_MINI, {"000000001": _serie()})


def _faux_get_pour(spec):
    """Rend, pour chaque idbank demandé, deux mois de valeurs à l'unité attendue."""
    attendu = {i: (f["unit_measure"], str(f["unit_mult"])) for f in spec["fields"].values()
               for i in f["idbanks"].values()}

    class R:
        status_code = 200

        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    def get(url, **kw):
        ids = url.rsplit("/", 1)[1].split("?")[0].split("+")
        series = "".join(
            f'<Series IDBANK="{i}" FREQ="M" TITLE_FR="s{i}" LAST_UPDATE="2026-10-13" '
            f'UNIT_MEASURE="{attendu[i][0]}" UNIT_MULT="{attendu[i][1]}">'
            f'<Obs TIME_PERIOD="2026-08" OBS_VALUE="70.5"/><Obs TIME_PERIOD="2026-07" OBS_VALUE="68"/></Series>'
            for i in ids)
        return R(f'<m:StructureSpecificData xmlns:m="x"><m:DataSet>{series}</m:DataSet></m:StructureSpecificData>')
    return get


def test_ingest_ecrit_le_store_et_cree_l_univers_sans_collecte_bloomberg(share):
    spec = insee_bdm.load_spec()
    man = insee_bdm.ingest(spec, get=_faux_get_pour(spec))
    u = registry.load("hotellerie_fr")
    assert u.fetch_enabled is False and u.ticker_suffix == ""
    assert set(u.active_tickers()) == set(spec["zones"])
    df = store.read("hotellerie_fr", "taux_occupation")
    assert df.loc["2026-08-31", "D74"] == 70.5
    assert man["written"]["taux_occupation"] == [2026]
    assert (share / "univers" / "runs" / "hotellerie_fr" / "latest_insee.json").is_file()


def test_ingest_idempotent_et_revision_ecrasee(share):
    spec = insee_bdm.load_spec()
    insee_bdm.ingest(spec, get=_faux_get_pour(spec))
    insee_bdm.ingest(spec, get=_faux_get_pour(spec))
    assert registry.load("hotellerie_fr").rev == 1
    assert len(store.read("hotellerie_fr", "nuitees")) == 2


def test_dry_run_n_ecrit_rien(share):
    spec = insee_bdm.load_spec()
    man = insee_bdm.ingest(spec, get=_faux_get_pour(spec), dry_run=True)
    assert not registry.exists("hotellerie_fr")
    assert man["written"] == {}


def test_champs_declares_dans_la_config_egaux_au_yaml_insee():
    cfg = _yaml.safe_load((REPO / "config" / "atlas_config.yaml").read_text(encoding="utf-8"))
    spec = insee_bdm.load_spec()
    assert consumers.universe_fields("hotellerie_fr", cfg) == list(spec["fields"])


def test_consommateurs_hotellerie_sans_erreur():
    cfg = _yaml.safe_load((REPO / "config" / "atlas_config.yaml").read_text(encoding="utf-8"))
    doc = consumers.load(config_path=REPO / "config" / "consumers.yaml")
    found = [f for f in consumers.check(doc, cfg) if f.universe == "hotellerie_fr" and f.level != "INFO"]
    assert found == []
    assert {c for c, _ in doc.by_universe()["hotellerie_fr"]} == {"vigie_hotellerie", "trame"}
