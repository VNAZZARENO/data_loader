import json
import multiprocessing as mp
import os

import pytest

from dl import smbio


def test_atomic_write_json_roundtrip(tmp_path):
    p = tmp_path / "a" / "x.json"
    smbio.atomic_write_json(p, {"a": 1, "é": "ü"})
    assert smbio.read_json_retry(p) == {"a": 1, "é": "ü"}
    assert [f.name for f in p.parent.iterdir()] == ["x.json"]  # pas de tmp residuel


def test_atomic_write_retries_on_permission_error(tmp_path, monkeypatch):
    real, n = os.replace, {"n": 0}

    def flaky(a, b):
        n["n"] += 1
        if n["n"] < 3:
            raise PermissionError("sharing violation")
        return real(a, b)

    monkeypatch.setattr(os, "replace", flaky)
    smbio.atomic_write_bytes(tmp_path / "f.bin", b"ok", backoff=0)
    assert (tmp_path / "f.bin").read_bytes() == b"ok" and n["n"] == 3


def test_atomic_write_gives_up_and_cleans_tmp(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "replace", lambda a, b: (_ for _ in ()).throw(PermissionError("x")))
    with pytest.raises(OSError):
        smbio.atomic_write_bytes(tmp_path / "f.bin", b"ok", retries=2, backoff=0)
    assert list(tmp_path.iterdir()) == []


def test_read_json_retry_tolerates_torn_read(tmp_path, monkeypatch):
    p = tmp_path / "x.json"
    p.write_text('{"a": 1}')
    real, n = json.loads, {"n": 0}

    def torn(s):
        n["n"] += 1
        return real(s[:3]) if n["n"] == 1 else real(s)

    monkeypatch.setattr(smbio.json, "loads", torn)
    assert smbio.read_json_retry(p, backoff=0) == {"a": 1}


def test_atomic_write_via_cleans_on_error(tmp_path):
    def boom(tmp):
        tmp.write_text("partial")
        raise ValueError("x")

    with pytest.raises(ValueError):
        smbio.atomic_write_via(tmp_path / "o.xlsx", boom)
    assert list(tmp_path.iterdir()) == []


def test_dirlock_contention_and_stale(tmp_path):
    lock = tmp_path / "u.lock"
    with smbio.DirLock(lock):
        with pytest.raises(smbio.LockTimeout):
            smbio.DirLock(lock, timeout=0.3, poll=0.05).acquire()
    assert not lock.exists()
    # verrou perime -> casse
    lock.mkdir()
    os.utime(lock, (0, 0))
    with smbio.DirLock(lock, ttl=1, timeout=1):
        assert (lock / "owner.json").exists()


def _claim(args):
    src, dst = args
    try:
        os.rename(src, dst)
        return 1
    except OSError:
        return 0


def test_rename_claim_has_single_winner(tmp_path):
    (tmp_path / "pending").mkdir()
    (tmp_path / "claimed").mkdir()
    src = tmp_path / "pending" / "r.json"
    src.write_text("{}")
    with mp.Pool(4) as pool:
        wins = pool.map(_claim, [(str(src), str(tmp_path / "claimed" / "r.json"))] * 8)
    assert sum(wins) == 1
