import numpy as np
import pandas as pd
import pytest

from dl import manifests, quality, registry
from dl.registry import ops
from dl.store import writer


@pytest.fixture
def demo(share):
    idx = pd.bdate_range("2025-01-01", periods=40)
    px = pd.DataFrame({"AAA FP": 100 * 1.01 ** np.arange(40), "BBB GY": 50 * 0.99 ** np.arange(40),
                       "DEAD FP": 10.0 + np.arange(40)}, index=idx)
    px.loc[idx[20]:, "DEAD FP"] = px.loc[idx[20], "DEAD FP"]
    for layer in ("raw", "clean"):
        writer.upsert_long("demo", layer, "price", px)
    writer.update_state("demo", "price", px)
    u = ops.create("demo", ["AAA FP", "BBB GY", "DEAD FP", "GHOST NA"], date="2025-01-01")
    u, _ = ops.deprecate(u, "BBB GY", idx[10].date().isoformat(), "index")
    registry.save(u, None)
    m = manifests.RunManifest(universe="demo", profile="default")
    m.per_field["price"] = manifests.FieldReport("PX_LAST", n_returned=3, missing=["GHOST NA Equity"])
    m.finish()
    manifests.write(m)
    return idx, px


def test_summary_reads_no_parquet(demo, monkeypatch):
    import pyarrow.parquet as pq
    monkeypatch.setattr(pq, "read_table", lambda *a, **k: (_ for _ in ()).throw(AssertionError("parquet lu")))
    s = quality.summary("demo")
    assert s["n_active"] == 3 and s["n_deprecated"] == 1 and s["in_store"]
    assert s["data_date"] == demo[0][-1].date().isoformat() and s["last_run"]["status"] == "partial"


def test_ew_index_is_pit_mean_of_returns(demo):
    idx, px = demo
    ew = quality.ew_index("demo")
    r = px.pct_change()
    d1, d2 = idx[5], idx[15]                          # BBB membre a d1, sorti a d2
    exp1 = r.loc[d1, ["AAA FP", "BBB GY", "DEAD FP"]].mean()
    exp2 = r.loc[d2, ["AAA FP", "DEAD FP"]].mean()
    assert ew["ew"].loc[d1] / ew["ew"].loc[idx[4]] - 1 == pytest.approx(exp1)
    assert ew["ew"].loc[d2] / ew["ew"].loc[idx[14]] - 1 == pytest.approx(exp2)
    assert ew["n"].loc[d1] == 3 and ew["n"].loc[d2] == 2
    no_pit = quality.ew_index("demo", pit_members=False)
    assert no_pit["n"].loc[d2] == 3


def test_count_series_and_quality_flags(demo):
    idx, _ = demo
    c = quality.count_series("demo")
    assert c.loc[idx[5], "members"] == 4 and c.loc[idx[5], "with_price"] == 3
    assert c.loc[idx[15], "members"] == 3
    rows = {r["ticker"]: r for r in quality.ticker_quality("demo")}
    assert rows["DEAD FP"]["flat_tail"] == 19 and "fige" in rows["DEAD FP"]["flags"]
    assert set(rows["GHOST NA"]["flags"]) == {"sans_donnees", "absent_reponse"}
    assert rows["AAA FP"]["flags"] == [] and rows["BBB GY"]["status"] == "deprecated"
    fq = quality.field_quality("demo")[0]
    assert fq["field"] == "price" and fq["n_active"] == 3 and fq["missing_active"] == ["GHOST NA"]
    assert fq["nan_last_pct"] == 0.0


def test_ew_index_ignores_undefined_returns_after_zero(share):
    idx = pd.bdate_range("2025-01-01", periods=3)
    registry.save(ops.create("zeros", ["ZERO", "OK"], date="2025-01-01"), None)
    px = pd.DataFrame({"ZERO": [0.0, 10.0, 11.0], "OK": [100.0, 110.0, 121.0]}, index=idx)
    writer.upsert_long("zeros", "clean", "price", px)
    writer.upsert_long("zeros", "_benchmark", "price",
                       pd.DataFrame({"benchmark": [0.0, 100.0, 110.0]}, index=idx))
    out = quality.ew_index("zeros")
    assert out["ew"].tolist() == pytest.approx([100, 110, 121])
    assert out["n"].tolist() == [0, 1, 2]
    assert np.isfinite(out["benchmark"]).all()
    assert out["benchmark"].iloc[1] == pytest.approx(110)
