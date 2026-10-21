from pathlib import Path

import pandas as pd
import pytest

from dl.sources import insee_bdm

FIXTURE = Path(__file__).parent / "fixtures" / "insee_bdm_sample.xml"


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
