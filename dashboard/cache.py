"""Cache local (jamais sur le partage) des calculs lourds, cle = dernier run + rev du registre."""

from __future__ import annotations

import json

from dl import manifests, registry

from . import settings


def _key(universe: str, name: str) -> str:
    cfg = settings.config()
    last = manifests.latest(universe, config=cfg) or {}
    rev = registry.load(universe, cfg).rev
    from dl.store import writer
    wm = writer.load_state(universe, cfg).get("watermarks", {}).get("price", {})
    return f"{universe}__{name}__r{rev}__{last.get('run_id', 'norun')}__{max(wm.values()) if wm else 'nodata'}"


def cached(universe: str, name: str, compute):
    path = settings.CACHE_DIR / f"{_key(universe, name)}.json"
    if path.is_file():
        try:
            return json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            pass
    value = compute()
    try:
        settings.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        for old in settings.CACHE_DIR.glob(f"{universe}__{name}__*.json"):
            old.unlink(missing_ok=True)
        path.write_text(json.dumps(value, default=str))
    except OSError:
        pass
    return value
