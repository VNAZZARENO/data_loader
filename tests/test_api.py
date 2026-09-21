import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from dl import registry, requests_queue as rq
from dl.registry import ops
from dl.store import writer


@pytest.fixture
def client(share, tmp_path, monkeypatch):
    from dashboard import settings
    monkeypatch.setattr(settings, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(settings, "config", lambda: {"index_members": {"exchange_code_map": {"SW": "SE"}},
                                                     "agents": {"default": {}, "conviction": {}}})
    from dashboard.app import create_app
    registry.save(ops.create("demo", ["AAA FP", "BBB GY", "ROG SE"], date="2025-01-02",
                             source={"type": "bbg_index", "index": "DEMO Index", "auto_apply": False}), None)
    idx = pd.bdate_range("2025-01-01", periods=30)
    px = pd.DataFrame({"AAA FP": 100 + np.arange(30.0), "BBB GY": 50 + np.arange(30.0)}, index=idx)
    for layer in ("raw", "clean"):
        writer.upsert_long("demo", layer, "price", px)
    writer.update_state("demo", "price", px)
    return TestClient(create_app())


def test_no_patch_route_and_no_auth(client):
    for route in client.app.routes:
        assert "PATCH" not in (getattr(route, "methods", None) or set()), route.path
        assert not getattr(route, "dependencies", []), route.path           # aucune dependance d'auth
    assert client.app.user_middleware == []
    assert client.get("/api/universes").status_code == 200                  # sans en-tete, sans cookie


def test_pages_render_with_and_without_proxy_prefix(client):
    for path in ("/", "/u/demo", "/requests", "/new"):
        direct = client.get(path)
        assert direct.status_code == 200 and 'const BASE = "";' in direct.text
        proxied = client.get(path, headers={"X-Proxy-Prefix": "/DataLoader"})
        assert 'const BASE = "/DataLoader";' in proxied.text and 'href="/DataLoader/static/app.css"' in proxied.text
    assert client.get("/u/nope").status_code == 404
    assert client.get("/static/app.js").status_code == 200


def test_overview_and_series(client):
    u = client.get("/api/universes").json()[0]
    assert u["universe"] == "demo" and u["n_active"] == 3 and u["in_store"] and u["data_date"] == "2025-02-11"
    ew = client.get("/api/universes/demo/ew_index").json()
    assert len(ew["dates"]) == 30 and ew["ew"][0] == 100.0 and ew["ew"][-1] > 100
    c = client.get("/api/universes/demo/count_series").json()
    assert c["members"][-1] == 3 and c["with_price"][-1] == 2
    q = client.get("/api/universes/demo/quality?layer=raw").json()[0]
    assert q["missing_active"] == ["ROG SE"]
    assert client.get("/api/universes/demo/quality?layer=bogus").status_code == 422
    flags = {m["ticker"]: m["flags"] for m in client.get("/api/universes/demo/members").json()}
    assert flags["ROG SE"] == ["sans_donnees"]


def test_composition_flow_and_rev_conflict(client):
    r = client.post("/api/universes/demo/members?dry_run=1", json={"text": "rop sw equity, AAA FP"}).json()
    assert r["tickers"] == ["ROP SE", "AAA FP"] and r["already_active"] == ["AAA FP"]
    assert registry.load("demo").rev == 1
    assert client.post("/api/universes/demo/members", json={"text": "ROP SW", "rev": 1, "date": "2025-07-01"}).json()["rev"] == 2
    stale = client.post("/api/universes/demo/members/ROG SE/deprecate", json={"rev": 1, "date": "2025-07-01"})
    assert stale.status_code == 409 and "rev" in stale.json()["detail"]
    assert client.post("/api/universes/demo/members/ROG SE/deprecate", json={"rev": 2, "date": "2025-07-01"}).status_code == 200
    assert client.get("/api/universes/demo").json()["rename_candidates"][0]["to"] == "ROP SE"
    assert client.post("/api/universes/demo/renames", json={"rev": 3, "from": "ROG SE", "to": "ROP SE"}).status_code == 200
    assert client.put("/api/universes/demo/members/ROG SE/fetch", json={"rev": 4, "enabled": False}).status_code == 200
    assert client.put("/api/universes/demo/fetch", json={"rev": 5, "enabled": False}).status_code == 200
    u = registry.load("demo")
    assert u.member("ROP SE").predecessor == "ROG SE" and not u.member("ROG SE").fetch and not u.fetch_enabled
    assert client.delete("/api/universes/demo/members/BBB GY?rev=6").status_code == 200
    assert client.post("/api/universes/demo/members/AAA FP/deprecate", json={"date": "2025-07-01"}).status_code == 422
    assert client.post("/api/universes/demo/members/ZZZ/deprecate", json={"rev": 7}).status_code == 422
    ev = client.get("/api/universes/demo/events").json()
    assert ev[0]["ops"][0]["op"] == "remove" and len(ev) == 7


def test_create_universes_all_sources(client):
    ok = client.post("/api/universes", json={"name": "panier", "source": "paste", "text": "MC FP\nrog sw"})
    assert ok.status_code == 200 and registry.load("panier").active_tickers() == ["MC FP", "ROG SE"]
    assert client.post("/api/universes", json={"name": "panier", "source": "paste", "text": "MC FP"}).status_code == 409
    assert client.post("/api/universes", json={"name": "Bad Name", "source": "paste", "text": "MC FP"}).status_code == 422
    assert client.post("/api/universes", json={"name": "vide", "source": "paste", "text": ""}).status_code == 422
    r = client.post("/api/universes", json={"name": "idx", "source": "index", "index": "SXXP Index"}).json()
    assert r["n_members"] == 0 and rq.get(r["request"])["params"] == {"index": "SXXP Index"}
    r = client.post("/api/universes", json={"name": "both", "source": "combine", "how": "intersection", "universes": ["demo", "panier"]})
    assert registry.load("both").active_tickers() == ["ROG SE"]
    assert client.post("/api/universes", json={"name": "copie", "source": "clone", "from": "demo"}).json()["n_members"] == 3
    client.post("/api/universes", json={"name": "fr", "source": "filter", "from": "demo", "exchanges": ["FP"]})
    assert registry.load("fr").active_tickers() == ["AAA FP"]


def test_request_flow_through_api(client, fake_blp):
    from dl import requests_worker
    req = client.post("/api/universes/demo/refresh_index", json={}).json()
    assert client.post("/api/universes/demo/refresh_index", json={}).status_code == 409     # deja en attente
    fake_blp.members["DEMO Index"] = ["AAA FP", "ROG SW", "NEW NA"]
    requests_worker.process_pending(fake_blp, {"index_members": {"exchange_code_map": {"SW": "SE"}}})
    listed = client.get("/api/requests?status=done").json()[0]
    assert listed["preview"]["joiners"] == ["NEW NA"] and listed["preview"]["leavers"] == ["BBB GY"]
    assert client.post(f"/api/requests/{req['id']}/apply", json={}).json()["rev"] == 2
    assert registry.load("demo").deprecated_tickers() == ["BBB GY"]
    p = client.post("/api/universes/demo/refresh_index", json={}).json()
    assert client.delete(f"/api/requests/{p['id']}").status_code == 200
    assert client.delete(f"/api/requests/{p['id']}").status_code == 404


def test_series_endpoint_transforms(client):
    assert client.get("/api/universes/demo/fields").json()["raw"] == ["price"]
    lvl = client.get("/api/universes/demo/series?field=price").json()
    assert lvl["available"] == ["AAA FP", "BBB GY"] and lvl["AAA FP"][0] == 100.0 and lvl["last"]["BBB GY"] == 79.0
    reb = client.get("/api/universes/demo/series?field=price&transform=rebase&tickers=BBB GY").json()
    assert reb["tickers"] == ["BBB GY"] and reb["BBB GY"][0] == 100.0 and reb["BBB GY"][-1] == 158.0
    idx = pd.bdate_range("2025-01-01", periods=3)
    writer.upsert_long("demo", "raw", "total_return", pd.DataFrame({"AAA FP": [1.0, -0.5, 2.0], "NUL GY": [np.nan] * 3}, index=idx))
    cum = client.get("/api/universes/demo/series?field=total_return&transform=cumret").json()
    assert cum["available"] == ["AAA FP"] and cum["AAA FP"][-1] == pytest.approx(100 * 1.01 * 0.995 * 1.02, abs=1e-3)
    assert client.get("/api/universes/demo/series?field=price&transform=nope").status_code == 422
