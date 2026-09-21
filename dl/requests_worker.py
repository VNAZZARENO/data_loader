"""Cote poste Bloomberg : depile les requetes du dashboard. N'ecrit JAMAIS le registre."""

from __future__ import annotations

import logging

import index_members

from . import registry, requests_queue as rq

logger = logging.getLogger(__name__)


def _learning_sample(target, config) -> list[str]:
    """Tickers servant a apprendre la convention de place (ROP SW -> ROP SE).
    Univers vide (creation) : on apprend sur tous les univers actions du registre."""
    if target is not None and target.active_tickers():
        return target.active_tickers()
    sample: list[str] = []
    for name in registry.list_universes(config):
        u = registry.load(name, config)
        if u.ticker_suffix:
            sample += u.active_tickers()
    return sample


def run_index_members(universe: str, index: str, blp, config: dict | None = None) -> dict:
    target = registry.load(universe, config) if registry.exists(universe, config) else None
    existing = target.active_tickers() if target else []
    raw = index_members.fetch_index_members(index, blp)
    manual = (config or {}).get("index_members", {}).get("exchange_code_map", {})
    learned = index_members.learn_exchange_map(raw, _learning_sample(target, config))
    new_tickers, info = index_members.reconcile_to_convention(raw, existing, {**learned, **manual})
    return {"index": index, "new_tickers": new_tickers, **info}


def run_index_members_hist(index: str, dates: list[str], blp, config: dict | None = None,
                           universe: str | None = None) -> dict:
    """Compositions historiques (BDS INDX_MWEIGHT_HIST + END_DATE_OVERRIDE) : le seul vrai
    correctif du survivorship passe."""
    target = registry.load(universe, config) if universe and registry.exists(universe, config) else None
    manual = (config or {}).get("index_members", {}).get("exchange_code_map", {})
    sample = _learning_sample(target, config)
    snapshots, errors = {}, {}
    for d in dates:
        try:
            df = blp.bds(index, "INDX_MWEIGHT_HIST", END_DATE_OVERRIDE=d.replace("-", ""))
            col = index_members.member_column(df)
            raw = [str(x).strip() for x in df[col] if str(x).strip() and str(x) != "nan"]
            if not raw:
                raise ValueError("reponse vide")
            learned = index_members.learn_exchange_map(raw, sample)
            snapshots[d], _ = index_members.reconcile_to_convention(raw, [], {**learned, **manual})
        except Exception as e:
            errors[d] = str(e)
    if not snapshots:
        raise RuntimeError(f"Aucune composition historique obtenue pour {index}: {errors}")
    return {"index": index, "snapshots": snapshots, "errors": errors}


def process_pending(blp, config: dict | None = None) -> list[str]:
    try:
        rq.requeue_stale(config)
        pending = rq.list_("pending", config=config)
    except OSError as e:
        logger.warning(f"File de requetes inaccessible: {e}")
        return []
    done = []
    for req in reversed(pending):  # plus anciennes d'abord
        claimed = rq.claim(req["id"], config)
        if claimed is None:
            continue
        logger.info(f"Requete {claimed['id']} : {claimed['type']} {claimed['universe']}")
        try:
            p = claimed["params"]
            if claimed["type"] == "index_members":
                result = run_index_members(claimed["universe"], p["index"], blp, config)
            else:
                result = run_index_members_hist(p["index"], p["dates"], blp, config, claimed["universe"])
            rq.complete(claimed["id"], result, config)
        except Exception as e:
            logger.error(f"Requete {claimed['id']} en echec: {e}")
            rq.fail(claimed["id"], f"{type(e).__name__}: {e}", config)
        done.append(claimed["id"])
    return done
