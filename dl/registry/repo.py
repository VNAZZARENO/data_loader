"""Persistance du registre sur le partage, avec concurrence optimiste sur ``rev``."""

from __future__ import annotations

import datetime as dt
import socket

from .. import paths, smbio
from . import events
from .model import RegistryError, Universe, validate_name


class RevConflict(RegistryError):
    """Le registre a change depuis la lecture (deux onglets, deux outils)."""


def _path(universe: str, config: dict | None = None):
    return paths.registry_dir(config) / f"{validate_name(universe)}.json"


def exists(universe: str, config: dict | None = None) -> bool:
    try:
        return _path(universe, config).is_file()
    except RegistryError:
        return False


def list_universes(config: dict | None = None) -> list[str]:
    d = paths.registry_dir(config)
    if not d.is_dir():
        return []
    return sorted(f.stem for f in d.glob("*.json"))


def load(universe: str, config: dict | None = None) -> Universe:
    p = _path(universe, config)
    if not p.is_file():
        raise FileNotFoundError(f"Univers absent du registre: {universe}")
    return Universe.from_dict(smbio.read_json_retry(p))


def save(u: Universe, expected_rev: int | None, ops: list[dict] | None = None, *,
         actor: str = "cli", source: str = "manual", config: dict | None = None) -> Universe:
    """``expected_rev`` = rev lue par l'appelant ; ``None`` = creation (le fichier ne doit pas exister)."""
    u.validate()
    p = _path(u.universe, config)
    lock = paths.registry_dir(config) / "_locks" / f"{u.universe}.lock"
    with smbio.DirLock(lock):
        current = smbio.read_json_retry(p)["rev"] if p.is_file() else None
        if current != expected_rev:
            raise RevConflict(
                f"{u.universe}: rev attendue {expected_rev}, rev sur le partage {current}"
            )
        rev_from = current or 0
        u.rev = rev_from + 1
        u.updated_at = dt.datetime.now(dt.timezone.utc).isoformat()
        u.updated_by = f"{actor}@{socket.gethostname()}"
        smbio.atomic_write_json(p, u.to_dict())
        events.write_changeset(u.universe, rev_from, u.rev, ops or [], u.updated_by, source, config)
    return u


def delete(universe: str, config: dict | None = None) -> None:
    """Archive le fichier (jamais de suppression seche : l'historique reste relisible)."""
    p = _path(universe, config)
    if p.is_file():
        trash = paths.registry_dir(config) / "_deleted"
        trash.mkdir(parents=True, exist_ok=True)
        p.replace(trash / f"{universe}.{dt.datetime.now():%Y%m%dT%H%M%S}.json")
