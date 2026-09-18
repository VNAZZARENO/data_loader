"""File de requetes par fichiers : le dashboard (Linux) demande, le loader (poste Bloomberg) execute.

    pending/ -> claimed/ -> done/ | failed/        (loader)
    done/    -> applied/ | rejected/               (dashboard)

Le claim est un ``os.rename`` : un seul gagnant, meme entre deux machines.
"""

from __future__ import annotations

import datetime as dt
import os
import socket
import time
import uuid
from pathlib import Path

from . import paths, smbio
from .manifests import utc_ts

STATUSES = ("pending", "claimed", "done", "failed", "applied", "rejected")
TYPES = ("index_members", "index_members_hist")
STALE_CLAIM_SECONDS = 3600


def _dir(status: str, config=None) -> Path:
    return paths.requests_dir(config) / status


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def submit(type_: str, universe: str, params: dict, created_by: str = "dashboard", config=None) -> dict:
    if type_ not in TYPES:
        raise ValueError(f"Type de requete inconnu: {type_}")
    req = {"id": f"{utc_ts()}_{uuid.uuid4().hex[:8]}", "type": type_, "universe": universe,
           "params": params, "created_at": _now(), "created_by": created_by, "status": "pending",
           "claimed_by": None, "result": None, "error": None}
    smbio.atomic_write_json(_dir("pending", config) / f"{req['id']}.json", req)
    return req


def find(req_id: str, config=None) -> tuple[str, Path] | None:
    for status in STATUSES:
        p = _dir(status, config) / f"{req_id}.json"
        if p.is_file():
            return status, p
    return None


def get(req_id: str, config=None) -> dict | None:
    hit = find(req_id, config)
    return smbio.read_json_retry(hit[1]) if hit else None


def list_(status: str | None = None, universe: str | None = None, limit: int = 200, config=None) -> list[dict]:
    out = []
    for st in ([status] if status else STATUSES):
        d = _dir(st, config)
        if d.is_dir():
            out += [smbio.read_json_retry(f) for f in sorted(d.glob("*.json"), reverse=True)[:limit]]
    if universe:
        out = [r for r in out if r["universe"] == universe]
    return sorted(out, key=lambda r: r["id"], reverse=True)[:limit]


def _move(req: dict, src: Path, status: str, config=None) -> dict:
    req["status"] = status
    dst = _dir(status, config) / src.name
    smbio.atomic_write_json(dst, req)
    src.unlink(missing_ok=True)
    return req


def claim(req_id: str, config=None) -> dict | None:
    """None si un autre processus a gagne."""
    src = _dir("pending", config) / f"{req_id}.json"
    dst = _dir("claimed", config) / src.name
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.rename(src, dst)
    except OSError:
        return None
    req = smbio.read_json_retry(dst)
    req.update(status="claimed", claimed_by=f"{socket.gethostname()}:{os.getpid()}", claimed_at=_now())
    smbio.atomic_write_json(dst, req)
    return req


def _finish(req_id: str, status: str, config=None, **fields) -> dict:
    src = _dir("claimed", config) / f"{req_id}.json"
    req = smbio.read_json_retry(src)
    req.update(finished_at=_now(), **fields)
    return _move(req, src, status, config)


def complete(req_id: str, result: dict, config=None) -> dict:
    return _finish(req_id, "done", config, result=result)


def fail(req_id: str, error: str, config=None) -> dict:
    return _finish(req_id, "failed", config, error=error)


def close(req_id: str, status: str, note: str = "", config=None) -> dict:
    """done -> applied | rejected (cote dashboard)."""
    if status not in ("applied", "rejected"):
        raise ValueError(status)
    src = _dir("done", config) / f"{req_id}.json"
    req = smbio.read_json_retry(src)
    req.update(closed_at=_now(), note=note)
    return _move(req, src, status, config)


def cancel(req_id: str, config=None) -> bool:
    p = _dir("pending", config) / f"{req_id}.json"
    try:
        p.unlink()
        return True
    except FileNotFoundError:
        return False


def requeue_stale(config=None, max_age: float = STALE_CLAIM_SECONDS) -> list[str]:
    """Un loader mort en cours de route ne doit pas bloquer une requete pour toujours."""
    out = []
    d = _dir("claimed", config)
    for f in (d.glob("*.json") if d.is_dir() else []):
        if time.time() - f.stat().st_mtime > max_age:
            req = smbio.read_json_retry(f)
            req.update(claimed_by=None)
            _move(req, f, "pending", config)
            out.append(req["id"])
    return out
