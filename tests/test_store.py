import numpy as np
import pandas as pd
import pytest

from dl import registry, store
from dl.registry import ops
from dl.store import derive, fx, layout, legacy_xlsx, reader, writer


def _wide(start, end, tickers=("AAA FP", "BBB LN"), base=100.0):
    idx = pd.bdate_range(start, end)
    return pd.DataFrame({t: base + i * 10 + np.arange(len(idx)) for i, t in enumerate(tickers)}, index=idx)


def test_upsert_is_idempotent_and_roundtrips(share):
    w = _wide("2024-12-20", "2025-01-10")
    assert writer.upsert_long("u", "raw", "price", w) == [2024, 2025]
    writer.upsert_long("u", "raw", "price", w)
    got = store.read("u", "price")
    pd.testing.assert_frame_equal(got, w.astype(float), check_freq=False, check_names=False, check_index_type=False)


def test_daily_append_only_touches_current_year_and_overwrites_provisional(share):
    writer.upsert_long("u", "raw", "price", _wide("2024-12-20", "2025-01-10"))
    d = layout.field_dir("u", "raw", "price")
    before = {p.name: p.stat().st_mtime_ns for p in d.iterdir()}
    new = _wide("2025-01-10", "2025-01-13", base=500.0)       # le 10 est re-fetche (provisoire)
    assert writer.upsert_long("u", "raw", "price", new) == [2025]
    after = {p.name: p.stat().st_mtime_ns for p in d.iterdir()}
    assert after["2024.parquet"] == before["2024.parquet"]
    got = store.read("u", "price")
    assert got.loc["2025-01-10", "AAA FP"] == 500.0 and got.index.max() == pd.Timestamp("2025-01-13")


def test_joiner_backfill_and_watermarks(share):
    writer.upsert_long("u", "raw", "price", _wide("2025-01-01", "2025-01-31"))
    writer.update_state("u", "price", _wide("2025-01-01", "2025-01-31"))
    joiner = _wide("2024-06-03", "2025-01-31", tickers=("NEW GY",))
    writer.upsert_long("u", "raw", "price", joiner)
    st = writer.update_state("u", "price", joiner)
    assert st["watermarks"]["price"] == {"AAA FP": "2025-01-31", "BBB LN": "2025-01-31", "NEW GY": "2025-01-31"}
    got = store.read("u", "price")
    assert got["NEW GY"].first_valid_index() == pd.Timestamp("2024-06-03")
    assert pd.isna(got.loc["2024-06-03", "AAA FP"])


def test_reader_year_pruning_and_long_format(share, monkeypatch):
    writer.upsert_long("u", "raw", "price", _wide("2023-01-02", "2025-06-30"))
    opened = []
    real = reader.pq.read_table
    monkeypatch.setattr(reader.pq, "read_table", lambda p, *a, **k: (opened.append(p.name), real(p, *a, **k))[1])
    got = store.read("u", "price", start="2025-02-01", end="2025-03-31", tickers=["AAA FP"])
    assert opened == ["2025.parquet"] and list(got.columns) == ["AAA FP"]
    assert got.index.min() >= pd.Timestamp("2025-02-01")
    long = store.read("u", ["price"], start="2025-06-27", wide=False)
    assert list(long.columns) == ["date", "ticker", "field", "layer", "value"] and len(long) == 4


def test_pit_mask_and_rename_splice(share):
    idx = pd.bdate_range("2025-01-01", "2025-03-31")
    cut = pd.Timestamp("2025-02-17")
    px = pd.DataFrame({"AAA FP": 1.0, "ROG SE": 2.0, "ROP SE": 3.0}, index=idx)
    px.loc[idx >= cut, "ROG SE"] = np.nan
    px.loc[idx < cut, "ROP SE"] = np.nan
    writer.upsert_long("demo", "raw", "price", px)
    u = ops.create("demo", ["AAA FP", "ROG SE"], date="2025-01-01")
    u, _ = ops.apply_index_diff(u, ["ROP SE"], "2025-02-17", force=True)     # AAA sort, ROG -> ROP
    u, _ = ops.link_rename(u, "ROG SE", "ROP SE")
    registry.save(u, None)

    spliced = store.read("demo", "price")
    assert list(spliced.columns) == ["AAA FP", "ROP SE"] and spliced["ROP SE"].notna().all()
    assert spliced.loc["2025-02-14", "ROP SE"] == 2.0 and spliced.loc["2025-02-17", "ROP SE"] == 3.0
    pit = store.read("demo", "price", pit_members=True)
    assert pit["AAA FP"].last_valid_index() == pd.Timestamp("2025-02-14")   # NaN apres la sortie
    assert pit["ROP SE"].notna().all()
    assert list(store.read("demo", "price", splice_renames=False).columns) == ["AAA FP", "ROG SE", "ROP SE"]


def test_fx_pence_and_fail_closed():
    idx = pd.bdate_range("2025-01-01", periods=3)
    local = pd.DataFrame({"AAA FP": 10.0, "BBB LN": 850.0, "CCC US": 110.0, "DDD ZZ": 5.0, "EEE SS": 100.0}, index=idx)
    rates = pd.DataFrame({"GBP": 0.85, "USD": [1.1, np.nan, 1.1]}, index=idx)
    ref = pd.DataFrame({"currency": ["USD"]}, index=["CCC US"])
    eur, missing = fx.convert_to_eur(local, rates, ref)
    assert eur["AAA FP"].eq(10.0).all()
    assert np.allclose(eur["BBB LN"], 10.0)                     # 850 pence -> 8.5 GBP -> 10 EUR
    assert eur["CCC US"].iloc[0] == pytest.approx(100.0) and np.isnan(eur["CCC US"].iloc[1])  # pas de ffill du taux
    assert eur["DDD ZZ"].isna().all() and eur["EEE SS"].isna().all()
    assert missing == ["DDD ZZ", "EEE SS"]
    assert fx.required_currencies(local.columns, ref) == ["GBP", "SEK", "USD"]


def test_holiday_detection_matches_atlas_rule():
    idx = pd.bdate_range("2025-01-01", periods=8)
    rng = np.random.default_rng(0)
    px = pd.DataFrame(100 + rng.normal(size=(8, 10)).cumsum(axis=0), index=idx)
    px.iloc[4] = px.iloc[3]                                       # ferie recopie
    assert list(derive.ffilled_holidays(px)) == [idx[4]]
    # reference : implementation ATLAS
    r = px.pct_change()
    ref = px.index[((r.abs() < 1e-12).sum(axis=1) / r.notna().sum(axis=1)) >= 0.9]
    assert list(ref) == [idx[4]]


def test_derive_layers_end_to_end(share):
    idx = pd.bdate_range("2025-01-01", periods=30)
    rng = np.random.default_rng(1)
    px = pd.DataFrame({t: 100 + rng.normal(size=30).cumsum() for t in ["AAA FP", "BBB LN", "DEAD FP"]}, index=idx)
    px.iloc[10] = px.iloc[9]
    px.loc[idx[15]:, "DEAD FP"] = px.loc[idx[15], "DEAD FP"]    # titre mort, ffill Bloomberg
    writer.upsert_long("u", "raw", "price", px)
    writer.upsert_long("u", "raw", "short_int", px * 0 + 5)
    writer.upsert_dir(__import__("dl.paths", fromlist=["x"]).fx_dir(), pd.DataFrame({"GBP": 0.8}, index=idx))
    rep = derive.derive("u", {"store": {"currency_fields": ["price"]}})
    assert rep["holidays"] == 1 and rep["fx_missing"] == []
    eur = store.read("u", "price", layer="fx_eur")
    assert eur.loc[idx[0], "BBB LN"] == pytest.approx(px.loc[idx[0], "BBB LN"] / 100 / 0.8)
    clean = store.read("u", "price", layer="clean")
    assert idx[10] not in clean.index
    assert clean["DEAD FP"].last_valid_index() == idx[15]
    assert store.fields("u", "fx_eur") == ["price"]              # champ non monetaire : pas de couche fx
    si = store.read("u", "short_int", layer="clean")
    assert idx[10] not in si.index and si["DEAD FP"].notna().all()


def test_legacy_xlsx_roundtrip_matches_loader_layout(share, tmp_path):
    px = _wide("2025-01-01", "2025-01-31")
    eps = px.iloc[::5] * 0 + 3.0                                  # champ creux
    writer.upsert_long("u", "raw", "price", px)
    writer.upsert_long("u", "raw", "EPS", eps)
    writer.upsert_long("u", layout.BENCHMARK, "price", px[["AAA FP"]].rename(columns={"AAA FP": "benchmark"}))
    out = tmp_path / "ATLAS_data_u_static.xlsx"
    legacy_xlsx.export("u", out, ["price", "EPS"], {"start_date": "2025-01-01"}, tickers=["BBB LN", "AAA FP"])
    assert [f.name for f in tmp_path.iterdir() if f.suffix == ".xlsx"] == [out.name]
    book = pd.ExcelFile(out)
    assert book.sheet_names == ["parameters", "price", "EPS", "benchmark"]
    price = book.parse("price", index_col=0)
    assert price.index.name == "Ticker" and list(price.columns) == ["BBB LN", "AAA FP"]
    pd.testing.assert_frame_equal(price.astype(float), px[["BBB LN", "AAA FP"]].astype(float), check_freq=False, check_names=False, check_index_type=False)
    assert book.parse("EPS", index_col=0).shape == px.shape      # reindexe sur price + ffill

    rep = legacy_xlsx.import_workbook("u2", out)
    assert rep["sheets"]["price"] == [23, 2]
    pd.testing.assert_frame_equal(store.read("u2", "price"), store.read("u", "price"))
    assert store.read_benchmark("u2")["price"].notna().all()
