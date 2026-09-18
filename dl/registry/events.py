"""Journal des changements : un fichier par changeset (jamais d'append sur SMB)."""

from __future__ import annotations

import datetime as dt
import uuid

from .. import paths, smbio
from ..manifests import utc_ts


def write_changeset(universe: str, rev_from: int, rev_to: int, ops: list[dict],
                    actor: str, source: str, config: dict | None = None) -> dict:
    cs = {
        # la rev dans le nom garantit l'ordre meme pour deux changesets dans la meme seconde
        "id": f"{utc_ts()}_r{rev_to:06d}_{uuid.uuid4().hex[:8]}",
        "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        "universe": universe, "rev_from": rev_from, "rev_to": rev_to,
        "actor": actor, "source": source, "ops": ops,
    }
    smbio.atomic_write_json(paths.events_dir(universe, config) / f"{cs['id']}.json", cs)
    return cs


def list_changesets(universe: str, limit: int = 200, config: dict | None = None) -> list[dict]:
    d = paths.events_dir(universe, config)
    if not d.is_dir():
        return []
    return [smbio.read_json_retry(f) for f in sorted(d.glob("*.json"), reverse=True)[:limit]]
