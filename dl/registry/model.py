"""Modele du registre : un univers = des membres avec periodes d'appartenance datees."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

SCHEMA = 1
ENTRY_SOURCES = {"seed", "first_valid", "index", "index_hist", "manual", "git", "xlsx"}
KINDS = {"index", "list", "derived"}
_NAME = re.compile(r"^[a-z0-9][a-z0-9_]{0,39}$")


class RegistryError(ValueError):
    pass


def validate_name(name: str) -> str:
    if not _NAME.match(name or ""):
        raise RegistryError(
            f"Nom d'univers invalide '{name}' : minuscules, chiffres et '_' (40 car. max)"
        )
    return name


@dataclass
class Period:
    entry: str | None = None          # ISO ; None = inconnue (le masque PIT prend first_valid)
    entry_source: str = "seed"
    exit: str | None = None
    exit_source: str | None = None

    def validate(self) -> None:
        if self.entry_source not in ENTRY_SOURCES:
            raise RegistryError(f"entry_source inconnu: {self.entry_source}")
        if self.entry and self.exit and self.exit < self.entry:
            raise RegistryError(f"Sortie {self.exit} anterieure a l'entree {self.entry}")


@dataclass
class Member:
    ticker: str
    fetch: bool = True
    periods: list[Period] = field(default_factory=list)
    successor: str | None = None
    predecessor: str | None = None
    note: str = ""

    @property
    def active(self) -> bool:
        return bool(self.periods) and self.periods[-1].exit is None

    @property
    def status(self) -> str:
        return "active" if self.active else "deprecated"


@dataclass
class Universe:
    universe: str
    label: str = ""
    kind: str = "list"
    rev: int = 0
    fetch_enabled: bool = True
    source: dict = field(default_factory=lambda: {"type": "manual"})
    derived_from: dict | None = None
    ticker_suffix: str = " Equity"
    benchmark: str | None = None
    fields_profile: str | None = None
    members: list[Member] = field(default_factory=list)
    updated_at: str | None = None
    updated_by: str | None = None
    schema: int = SCHEMA

    # -- acces
    def member(self, ticker: str) -> Member | None:
        return next((m for m in self.members if m.ticker == ticker), None)

    def require(self, ticker: str) -> Member:
        m = self.member(ticker)
        if m is None:
            raise RegistryError(f"{ticker} absent de l'univers {self.universe}")
        return m

    def active_tickers(self) -> list[str]:
        return [m.ticker for m in self.members if m.active]

    def deprecated_tickers(self) -> list[str]:
        return [m.ticker for m in self.members if not m.active]

    def fetch_tickers(self) -> list[str]:
        """Ce que le loader extrait : actifs ET deprecated, tant que le fetch est actif."""
        if not self.fetch_enabled:
            return []
        return [m.ticker for m in self.members if m.fetch]

    def validate(self) -> None:
        validate_name(self.universe)
        if self.kind not in KINDS:
            raise RegistryError(f"kind inconnu: {self.kind}")
        seen: set[str] = set()
        for m in self.members:
            if m.ticker in seen:
                raise RegistryError(f"Ticker en double: {m.ticker}")
            seen.add(m.ticker)
            for p in m.periods:
                p.validate()
            for a, b in zip(m.periods, m.periods[1:]):
                if a.exit is None or (b.entry and b.entry < a.exit):
                    raise RegistryError(f"Periodes incoherentes pour {m.ticker}")
        for m in self.members:
            for link in (m.successor, m.predecessor):
                if link and link not in seen:
                    raise RegistryError(f"Lien de renommage vers un ticker absent: {link}")

    # -- (de)serialisation
    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Universe":
        d = dict(d)
        members = [
            Member(**{**m, "periods": [Period(**p) for p in m.get("periods", [])]})
            for m in d.pop("members", [])
        ]
        known = {f for f in cls.__dataclass_fields__}  # tolere des cles futures
        return cls(members=members, **{k: v for k, v in d.items() if k in known and k != "members"})
