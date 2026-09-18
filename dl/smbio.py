"""Primitives d'E/S sures sur un partage SMB (ecrivain Windows + ecrivain Linux).

- ecriture atomique : tmp dans le meme dossier, fsync, ``os.replace``, avec retry
  (violation de partage cote Windows / erreurs transitoires cote cifs) ;
- lecture JSON tolerante a une lecture concurrente ;
- ``DirLock`` : verrou par ``mkdir`` (atomique sur SMB), avec casse des verrous perimes.
Ni append ni SQLite sur le partage.
"""

from __future__ import annotations

import json
import os
import socket
import time
import uuid
from pathlib import Path


def _tmp_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.{socket.gethostname()}.{os.getpid()}.{uuid.uuid4().hex[:6]}.tmp")


def atomic_replace(tmp: Path, path: Path, retries: int = 8, backoff: float = 0.25) -> None:
    last: Exception | None = None
    for attempt in range(retries):
        try:
            os.replace(tmp, path)
            return
        except (PermissionError, OSError) as e:  # lecteur qui tient la cible ouverte
            last = e
            time.sleep(backoff * (attempt + 1))
    try:
        os.unlink(tmp)
    except OSError:
        pass
    raise OSError(f"Ecriture atomique impossible vers {path}: {last}") from last


def atomic_write_bytes(path: str | Path, data: bytes, retries: int = 8, backoff: float = 0.25) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(path)
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    atomic_replace(tmp, path, retries=retries, backoff=backoff)


def atomic_write_json(path: str | Path, obj, **kw) -> None:
    data = json.dumps(obj, ensure_ascii=False, indent=1, default=str).encode("utf-8")
    atomic_write_bytes(path, data, **kw)


def atomic_write_via(path: str | Path, write_fn, retries: int = 8, backoff: float = 0.25) -> None:
    """``write_fn(tmp_path)`` ecrit le fichier ; il est ensuite renomme sur ``path``.

    Pour les ecrivains qui veulent un chemin (ExcelWriter, pyarrow). Le tmp garde
    l'extension de la cible car openpyxl/pandas choisissent le moteur dessus.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".tmp.{os.getpid()}.{uuid.uuid4().hex[:6]}.{path.name}")
    try:
        write_fn(tmp)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    atomic_replace(tmp, path, retries=retries, backoff=backoff)


def read_json_retry(path: str | Path, retries: int = 5, backoff: float = 0.2):
    last: Exception | None = None
    for attempt in range(retries):
        try:
            with open(path, "rb") as f:
                return json.loads(f.read().decode("utf-8"))
        except (json.JSONDecodeError, PermissionError) as e:
            last = e
            time.sleep(backoff * (attempt + 1))
    raise last  # type: ignore[misc]


class LockTimeout(TimeoutError):
    pass


class DirLock:
    """Verrou inter-machines par creation de dossier. Filet de securite pour les outils CLI."""

    def __init__(self, path: str | Path, ttl: float = 120.0, timeout: float = 30.0, poll: float = 0.2):
        self.path = Path(path)
        self.ttl, self.timeout, self.poll = ttl, timeout, poll

    def _is_stale(self) -> bool:
        try:
            return time.time() - self.path.stat().st_mtime > self.ttl
        except OSError:
            return False

    def _break(self) -> None:
        stale = self.path.with_name(f"{self.path.name}.stale.{uuid.uuid4().hex[:6]}")
        try:
            os.rename(self.path, stale)
        except OSError:
            return  # quelqu'un d'autre l'a casse
        for child in stale.iterdir():
            child.unlink(missing_ok=True)
        stale.rmdir()

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.time() + self.timeout
        while True:
            try:
                os.mkdir(self.path)
                break
            except FileExistsError:
                if self._is_stale():
                    self._break()
                    continue
                if time.time() > deadline:
                    raise LockTimeout(f"Verrou occupe: {self.path}")
                time.sleep(self.poll)
        owner = {"host": socket.gethostname(), "pid": os.getpid(), "ts": time.time()}
        (self.path / "owner.json").write_text(json.dumps(owner))

    def release(self) -> None:
        try:
            (self.path / "owner.json").unlink(missing_ok=True)
            self.path.rmdir()
        except OSError:
            pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()
