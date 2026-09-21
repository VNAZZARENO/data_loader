"""API JSON. GET/POST/PUT/DELETE uniquement : le proxy pergam-tools ne route pas PATCH.
Aucune authentification, par regle de la maison."""

from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from dl import manifests, quality, registry, requests_apply, requests_queue as rq, store, tickers as tk
from dl.registry import events, ops
from dl.registry.model import RegistryError

from .. import cache, settings

router = APIRouter(prefix="/api")


def cfg():
    return settings.config()


def _load(universe: str):
    try:
        return registry.load(universe, cfg())
    except (FileNotFoundError, RegistryError) as e:
        raise HTTPException(404, str(e))


def _mutate(universe: str, rev: int | None, fn, source: str = "manual"):
    """Charge, applique une op pure, sauve avec la rev du client -> 409 si quelqu'un est passe avant."""
    if rev is None:
        raise HTTPException(422, "Champ 'rev' requis")
    u = _load(universe)
    try:
        new, o = fn(u)
        saved = registry.save(new, rev, o, actor="dashboard", source=source, config=cfg())
    except registry.RevConflict as e:
        raise HTTPException(409, str(e))
    except RegistryError as e:
        raise HTTPException(422, str(e))
    return {"rev": saved.rev, "ops": o}


def _known_exchanges() -> set[str]:
    out = set()
    for name in registry.list_universes(cfg()):
        out |= {tk.split_ticker(t)[1] for t in registry.load(name, cfg()).active_tickers()}
    return out


def _exchange_map() -> dict:
    return cfg().get("index_members", {}).get("exchange_code_map", {})


# -- lecture ---------------------------------------------------------------

@router.get("/health")
def health():
    from dl import paths
    root = paths.share_root(cfg())
    return {"status": "ok", "service": "DataLoader", "share_root": str(root), "share_ok": root.is_dir()}


@router.get("/universes")
def universes():
    pending = rq.list_("pending", config=cfg()) + rq.list_("done", config=cfg())
    out = []
    for name in registry.list_universes(cfg()):
        s = quality.summary(name, cfg())
        s["open_requests"] = sum(1 for r in pending if r["universe"] == name)
        out.append(s)
    return out


@router.get("/universes/{universe}")
def universe(universe: str):
    u = _load(universe)
    return {**quality.summary(universe, cfg()), "ticker_suffix": u.ticker_suffix,
            "derived_from": u.derived_from, "fields_profile": u.fields_profile,
            "rename_candidates": ops.rename_candidates(u)}


@router.get("/universes/{universe}/members")
def members(universe: str):
    _load(universe)
    return cache.cached(universe, "members", lambda: quality.ticker_quality(universe, cfg()))


@router.get("/universes/{universe}/events")
def universe_events(universe: str, limit: int = 200):
    return events.list_changesets(universe, limit, cfg())


@router.get("/universes/{universe}/runs")
def runs(universe: str, limit: int = 50):
    return manifests.history(universe, limit, cfg())


@router.get("/universes/{universe}/quality")
def universe_quality(universe: str, layer: str = Query("raw", pattern="^(raw|fx_eur|clean)$")):
    _load(universe)
    return cache.cached(universe, f"quality_{layer}", lambda: quality.field_quality(universe, layer, config=cfg()))


def _series(df):
    return {"dates": [d.date().isoformat() for d in df.index],
            **{c: [None if v != v else round(float(v), 4) for v in df[c]] for c in df.columns}}


@router.get("/universes/{universe}/count_series")
def count_series(universe: str):
    _load(universe)
    return cache.cached(universe, "count", lambda: _series(quality.count_series(universe, cfg())))


@router.get("/universes/{universe}/ew_index")
def ew_index(universe: str, layer: str = Query("clean", pattern="^(raw|fx_eur|clean)$"), pit: bool = True):
    _load(universe)
    def compute():
        df = quality.ew_index(universe, layer, pit, config=cfg())
        return {**_series(df), "layer_used": df.attrs.get("layer_used", layer),
                "n_members_last": df.attrs.get("n_members_last"), "n_priced_last": df.attrs.get("n_priced_last")}
    return cache.cached(universe, f"ew_{layer}_{int(pit)}", compute)


MAX_SERIES = 8   # au-dela, un graphe en lignes n'est plus lisible (et la palette n'a que 8 teintes)


@router.get("/universes/{universe}/fields")
def universe_fields(universe: str):
    _load(universe)
    return {layer: store.fields(universe, layer, cfg()) for layer in store.LAYERS}


@router.get("/universes/{universe}/series")
def series(universe: str, field: str, layer: str = Query("raw", pattern="^(raw|fx_eur|clean)$"),
           tickers: str = "", transform: str = Query("level", pattern="^(level|rebase|cumret)$"),
           start: str | None = None):
    """Donnees brutes d'un champ. ``rebase`` : base 100 a la premiere valeur de chaque serie ;
    ``cumret`` : le champ est un rendement quotidien en % -> 100 * prod(1 + r/100)."""
    _load(universe)
    df = store.read(universe, field, layer=layer, start=start, splice_renames=False, config=cfg())
    df = df.dropna(how="all", axis=1)
    available = list(df.columns)
    wanted = [t for t in tickers.split(",") if t in df.columns][:MAX_SERIES] or available[:MAX_SERIES]
    df = df[wanted]
    if transform == "rebase":
        df = df / df.apply(lambda c: c.loc[c.first_valid_index()] if c.first_valid_index() is not None else float("nan")) * 100
    elif transform == "cumret":
        df = 100 * (1 + df.fillna(0) / 100).cumprod().where(df.notna().cummax())
    return {**_series(df), "tickers": wanted, "available": available,
            "last": {t: (None if df[t].dropna().empty else round(float(df[t].dropna().iloc[-1]), 4)) for t in wanted}}


# -- composition -----------------------------------------------------------

@router.post("/normalize")
def normalize(body: dict = Body(...)):
    raw = tk.parse_list(body.get("text", ""))
    return tk.normalize(raw, body.get("ticker_suffix", " Equity"), _exchange_map(), _known_exchanges())


@router.post("/universes/{universe}/members")
def add_members(universe: str, body: dict = Body(...), dry_run: bool = False):
    u = _load(universe)
    norm = tk.normalize(tk.parse_list(body.get("text", "")) + list(body.get("tickers", [])),
                        u.ticker_suffix, _exchange_map(), _known_exchanges())
    if dry_run:
        return {**norm, "already_active": [t for t in norm["tickers"] if t in set(u.active_tickers())]}
    return {**_mutate(universe, body.get("rev"), lambda x: ops.add(x, norm["tickers"], body.get("date"))), **norm}


@router.post("/universes/{universe}/members/{ticker}/deprecate")
def deprecate(universe: str, ticker: str, body: dict = Body(...)):
    return _mutate(universe, body.get("rev"), lambda u: ops.deprecate(u, ticker, body.get("date")))


@router.post("/universes/{universe}/members/{ticker}/reactivate")
def reactivate(universe: str, ticker: str, body: dict = Body(...)):
    return _mutate(universe, body.get("rev"), lambda u: ops.reactivate(u, ticker, body.get("date")))


@router.put("/universes/{universe}/members/{ticker}/fetch")
def member_fetch(universe: str, ticker: str, body: dict = Body(...)):
    return _mutate(universe, body.get("rev"), lambda u: ops.toggle_fetch(u, bool(body["enabled"]), ticker))


@router.put("/universes/{universe}/fetch")
def universe_fetch(universe: str, body: dict = Body(...)):
    return _mutate(universe, body.get("rev"), lambda u: ops.toggle_fetch(u, bool(body["enabled"])))


@router.put("/universes/{universe}/members/{ticker}")
def update_member(universe: str, ticker: str, body: dict = Body(...)):
    return _mutate(universe, body.get("rev"),
                   lambda u: ops.update_member(u, ticker, body.get("periods"), body.get("note")))


@router.delete("/universes/{universe}/members/{ticker}")
def remove_member(universe: str, ticker: str, rev: int = Query(...)):
    return _mutate(universe, rev, lambda u: ops.remove(u, ticker))


@router.post("/universes/{universe}/renames")
def link_rename(universe: str, body: dict = Body(...)):
    return _mutate(universe, body.get("rev"), lambda u: ops.link_rename(u, body["from"], body["to"]))


@router.delete("/universes/{universe}/renames/{ticker}")
def unlink_rename(universe: str, ticker: str, rev: int = Query(...)):
    return _mutate(universe, rev, lambda u: ops.unlink_rename(u, ticker))


@router.put("/universes/{universe}/settings")
def universe_settings(universe: str, body: dict = Body(...)):
    def fn(u):
        import copy
        u = copy.deepcopy(u)
        for k in ("label", "benchmark", "fields_profile"):
            if k in body:
                setattr(u, k, body[k] or None if k != "label" else body[k])
        if "auto_apply" in body and u.source.get("type") == "bbg_index":
            u.source["auto_apply"] = bool(body["auto_apply"])
        if "index" in body and body["index"]:
            u.source = {"type": "bbg_index", "index": body["index"], "auto_apply": u.source.get("auto_apply", False)}
            u.kind = "index"
        return u, [{"op": "settings", **{k: v for k, v in body.items() if k != "rev"}}]
    return _mutate(universe, body.get("rev"), fn)


@router.delete("/universes/{universe}")
def delete_universe(universe: str, rev: int = Query(...)):
    u = _load(universe)
    if u.rev != rev:
        raise HTTPException(409, f"rev attendue {rev}, rev courante {u.rev}")
    from dl.registry import repo
    repo.delete(universe, cfg())
    return {"deleted": universe}


# -- creation --------------------------------------------------------------

@router.post("/universes")
def create_universe(body: dict = Body(...)):
    name, source = body.get("name", ""), body.get("source", "paste")
    c = cfg()
    try:
        if registry.exists(name, c):
            raise HTTPException(409, f"L'univers '{name}' existe deja")
        common = {"label": body.get("label") or name}
        request = None
        if source == "paste":
            suffix = body.get("ticker_suffix", " Equity")
            norm = tk.normalize(tk.parse_list(body.get("text", "")), suffix, _exchange_map(), _known_exchanges())
            if not norm["tickers"]:
                raise HTTPException(422, "Aucun ticker dans la liste")
            u = ops.create(name, norm["tickers"], ticker_suffix=suffix, benchmark=body.get("benchmark") or None,
                           fields_profile=body.get("fields_profile") or None, **common)
        elif source == "index":
            index = (body.get("index") or "").strip()
            if not index:
                raise HTTPException(422, "Ticker d'indice requis (ex. SXXP Index)")
            u = ops.create(name, [], kind="index", benchmark=body.get("benchmark") or index,
                           source={"type": "bbg_index", "index": index, "auto_apply": False},
                           fields_profile=body.get("fields_profile") or None, **common)
        elif source == "combine":
            srcs = [_load(n) for n in body.get("universes", [])]
            u = ops.combine(name, srcs, body.get("how", "union"), **common)
        elif source == "clone":
            u = ops.clone(name, _load(body["from"]), **common)
        elif source == "filter":
            src = _load(body["from"])
            ref = store.read_refdata(src.universe, c)
            u = ops.filter_(name, src, exchanges=body.get("exchanges") or None,
                            sectors=body.get("sectors") or None, currencies=body.get("currencies") or None,
                            refdata=ref.to_dict("index") if not ref.empty else None, **common)
        else:
            raise HTTPException(422, f"Source inconnue: {source}")
        saved = registry.save(u, None, [{"op": "create", "source": source, "n": len(u.members)}],
                              actor="dashboard", source=f"create:{source}", config=c)
        if source == "index":
            request = rq.submit("index_members", name, {"index": u.source["index"]}, config=c)
    except registry.RevConflict as e:
        raise HTTPException(409, str(e))
    except RegistryError as e:
        raise HTTPException(422, str(e))
    return {"universe": saved.universe, "rev": saved.rev, "n_members": len(saved.members),
            "request": request and request["id"]}


# -- requetes --------------------------------------------------------------

@router.get("/requests")
def requests_list(status: str | None = None, universe: str | None = None):
    out = rq.list_(status, universe, config=cfg())
    for r in out:
        if r["status"] == "done":
            try:
                r["preview"] = requests_apply.preview(r, cfg())
            except (FileNotFoundError, RegistryError) as e:
                r["preview"] = {"error": str(e)}
        r.pop("result", None) if r["status"] != "failed" else None
    return out


@router.post("/universes/{universe}/refresh_index")
def refresh_index(universe: str, body: dict = Body(default={})):
    u = _load(universe)
    index = body.get("index") or u.source.get("index")
    if not index:
        raise HTTPException(422, "Cet univers n'a pas d'indice Bloomberg associe")
    if any(r["type"] == "index_members" for r in rq.list_("pending", universe, config=cfg())):
        raise HTTPException(409, "Une demande de rafraichissement est deja en attente")
    return rq.submit("index_members", universe, {"index": index}, config=cfg())


@router.post("/universes/{universe}/history_request")
def history_request(universe: str, body: dict = Body(...)):
    import pandas as pd
    u = _load(universe)
    index = body.get("index") or u.source.get("index")
    if not index:
        raise HTTPException(422, "Cet univers n'a pas d'indice Bloomberg associe")
    dates = [d.date().isoformat() for d in pd.date_range(body.get("start", "2013-01-01"),
                                                         pd.Timestamp.today(), freq=body.get("freq", "QE"))]
    return rq.submit("index_members_hist", universe, {"index": index, "dates": dates}, config=cfg())


@router.post("/requests/{req_id}/apply")
def request_apply(req_id: str, body: dict = Body(default={})):
    try:
        return requests_apply.apply(req_id, force=bool(body.get("force")), config=cfg())
    except registry.RevConflict as e:
        raise HTTPException(409, str(e))
    except RegistryError as e:
        raise HTTPException(422, str(e))


@router.post("/requests/{req_id}/reject")
def request_reject(req_id: str, body: dict = Body(default={})):
    try:
        return rq.close(req_id, "rejected", body.get("note", ""), cfg())
    except FileNotFoundError:
        raise HTTPException(404, "Requete introuvable ou non terminee")


@router.delete("/requests/{req_id}")
def request_cancel(req_id: str):
    if not rq.cancel(req_id, cfg()):
        raise HTTPException(404, "Seules les requetes en attente peuvent etre annulees")
    return {"cancelled": req_id}
