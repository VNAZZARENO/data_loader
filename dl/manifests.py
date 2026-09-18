"""Manifestes de run d'extraction : ce que le dashboard lit pour la fraicheur et la qualite."""

from __future__ import annotations

import datetime as dt
import socket
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

from . import paths, smbio


def utc_ts() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


@dataclass
class FieldReport:
    bbg_field: str
    n_returned: int = 0
    failed: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)  # demandes - retournes
    seconds: float = 0.0
    batches: int = 0
    fallback_batches: int = 0


@dataclass
class RunManifest:
    universe: str
    profile: str
    mode: str = "static"
    run_id: str = field(default_factory=lambda: f"{utc_ts()}_{uuid.uuid4().hex[:8]}")
    host: str = field(default_factory=socket.gethostname)
    started_at: str = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc).isoformat())
    finished_at: str | None = None
    status: str = "running"  # ok | partial | failed
    daily: bool = False
    registry_rev: int | None = None
    ticker_source: str = "csv"  # registry | csv
    date_range: list[str] = field(default_factory=list)
    n_requested: int = 0
    per_field: dict[str, FieldReport] = field(default_factory=dict)
    fx_missing: list[str] = field(default_factory=list)
    provisional_date: str | None = None
    clean_version: int | None = None
    xlsx: dict = field(default_factory=dict)
    requests_processed: list[str] = field(default_factory=list)
    error: str | None = None

    def finish(self, error: str | None = None) -> None:
        self.finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
        self.error = error
        if error:
            self.status = "failed"
        elif any(r.failed or r.missing or r.n_returned == 0 for r in self.per_field.values()):
            self.status = "partial"
        else:
            self.status = "ok"

    def to_dict(self) -> dict:
        return asdict(self)


def write(manifest: RunManifest, config: dict | None = None) -> Path:
    d = paths.runs_dir(manifest.universe, config)
    path = d / f"{manifest.run_id}_{manifest.profile}.json"
    payload = manifest.to_dict()
    smbio.atomic_write_json(path, payload)
    smbio.atomic_write_json(d / f"latest_{manifest.profile}.json", payload)
    return path


def latest(universe: str, profile: str | None = None, config: dict | None = None) -> dict | None:
    d = paths.runs_dir(universe, config)
    if not d.is_dir():
        return None
    files = [d / f"latest_{profile}.json"] if profile else sorted(d.glob("latest_*.json"))
    found = [smbio.read_json_retry(f) for f in files if f.is_file()]
    if not found:
        return None
    return max(found, key=lambda m: m.get("started_at") or "")


def history(universe: str, limit: int = 50, config: dict | None = None) -> list[dict]:
    d = paths.runs_dir(universe, config)
    if not d.is_dir():
        return []
    files = sorted((f for f in d.glob("*.json") if not f.name.startswith("latest_")), reverse=True)
    return [smbio.read_json_retry(f) for f in files[:limit]]
