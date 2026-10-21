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
