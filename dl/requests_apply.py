"""Cote dashboard : applique au registre le resultat d'une requete terminee."""

from __future__ import annotations

from . import registry, requests_queue as rq
from .registry import ops
from .registry.model import RegistryError


def preview(req: dict, config=None) -> dict:
    u = registry.load(req["universe"], config)
    res = req.get("result") or {}
    if req["type"] == "index_members":
        active, new = set(u.active_tickers()), res.get("new_tickers", [])
        return {"joiners": sorted(set(new) - active), "leavers": sorted(active - set(new)),
                "n_old": len(active), "n_new": len(new),
                "unmapped_exchanges": res.get("unmapped_exchanges", []),
                "guardrail": ops.guardrail_hit(active, set(new))}
    snaps = res.get("snapshots", {})
    return {"dates": sorted(snaps), "n_tickers": len({t for s in snaps.values() for t in s}),
            "errors": res.get("errors", {})}


def apply(req_id: str, force: bool = False, actor: str = "dashboard", config=None) -> dict:
    req = rq.get(req_id, config)
    if req is None or req["status"] != "done":
        raise RegistryError(f"Requete {req_id} introuvable ou non terminee")
    u = registry.load(req["universe"], config)
    if req["type"] == "index_members":
        if not u.members:  # premiere composition d'un univers cree depuis un indice
            new, o = ops.add(u, req["result"]["new_tickers"], None, "seed")
            for m in new.members:
                m.periods[0].entry = None
        else:
            new, o = ops.apply_index_diff(u, req["result"]["new_tickers"], source="index", force=force)
    else:
        new, o = ops.apply_history(u, req["result"]["snapshots"])
    if o:
        new = registry.save(new, u.rev, o, actor=actor, source=f"{req['type']}:{req_id}", config=config)
    rq.close(req_id, "applied", f"rev {new.rev}, {len(o)} operation(s)", config)
    return {"rev": new.rev, "ops": len(o)}


def auto_apply_pending(config=None) -> list[str]:
    """Univers avec ``source.auto_apply`` : application sans clic, garde-fou 50 % conserve."""
    out = []
    for req in rq.list_("done", config=config):
        try:
            u = registry.load(req["universe"], config)
            if req["type"] == "index_members" and u.source.get("auto_apply"):
                apply(req["id"], actor="auto_apply", config=config)
                out.append(req["id"])
        except (RegistryError, FileNotFoundError, OSError):
            continue  # reste visible dans l'UI pour decision manuelle
    return out
