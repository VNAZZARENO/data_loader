import sys
import types

import numpy as np
import pandas as pd
import pytest


class FakeBlp:
    """Imite xbbg.blp : bdh en colonnes MultiIndex (ticker, champ), bds INDX_MEMBERS, bdp."""

    def __init__(self, start="2024-01-01", end="2026-12-31"):
        self.calendar = pd.bdate_range(start, end)
        self.fail_batches_over = None   # leve une exception si len(tickers) > n
        self.empty = set()              # tickers qui ne renvoient rien
        self.raise_for = set()          # tickers qui levent une exception
        self.silent_drop = set()        # tickers absents de la reponse batch, sans erreur
        self.members = {}               # index -> [membres]
        self.refdata = {}               # ticker -> {champ: valeur}
        self.calls = []

    def _series(self, ticker, field, idx):
        seed = abs(hash((ticker, field))) % 1000
        return 100 + seed / 10 + np.arange(len(idx)) * 0.01

    def bdh(self, tickers, flds, start_date, end_date, **kw):
        tickers = list(tickers)
        self.calls.append(("bdh", tuple(tickers), tuple(flds), str(start_date), str(end_date)))
        if self.fail_batches_over is not None and len(tickers) > self.fail_batches_over:
            raise RuntimeError("batch trop gros")
        if any(t in self.raise_for for t in tickers):
            raise RuntimeError(f"bad security {tickers}")
        idx = self.calendar[(self.calendar >= pd.Timestamp(start_date)) & (self.calendar <= pd.Timestamp(end_date))]
        cols = {}
        for t in tickers:
            if t in self.empty or t in self.silent_drop:
                continue
            for f in flds:
                cols[(t, f)] = self._series(t, f, idx)
        if not cols:
            return pd.DataFrame()
        df = pd.DataFrame(cols, index=idx)
        df.columns = pd.MultiIndex.from_tuples(df.columns)
        return df

    def bds(self, index, field, **kw):
        self.calls.append(("bds", index, field))
        return pd.DataFrame({"member_ticker_and_exchange_code": self.members.get(index, [])})

    def bdp(self, tickers, flds, **kw):
        self.calls.append(("bdp", tuple(tickers), tuple(flds)))
        rows = {t: {f.lower(): self.refdata.get(t, {}).get(f) for f in flds} for t in tickers}
        return pd.DataFrame.from_dict(rows, orient="index")

    def requested_tickers(self):
        return {t for c in self.calls if c[0] == "bdh" for t in c[1]}


@pytest.fixture
def fake_blp():
    return FakeBlp()


@pytest.fixture
def share(tmp_path, monkeypatch):
    root = tmp_path / "share"
    root.mkdir()
    monkeypatch.setenv("DL_SHARE_ROOT", str(root))
    return root


@pytest.fixture(autouse=True)
def _no_xbbg(monkeypatch):
    """xbbg n'existe pas hors du poste Bloomberg : module factice pour les imports."""
    if "xbbg" not in sys.modules:
        mod = types.ModuleType("xbbg")
        mod.blp = types.SimpleNamespace()
        monkeypatch.setitem(sys.modules, "xbbg", mod)
