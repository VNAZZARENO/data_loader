from __future__ import annotations

from pathlib import Path

from .. import paths

LAYERS = ("raw", "fx_eur", "clean")
BENCHMARK = "_benchmark"


def field_dir(universe: str, layer: str, field: str, config=None, test=False) -> Path:
    if layer not in LAYERS and layer != BENCHMARK:
        raise ValueError(f"Couche inconnue: {layer} (attendu: {', '.join(LAYERS)})")
    sub = BENCHMARK if layer == BENCHMARK else f"layer={layer}"
    return paths.store_dir(universe, config, test) / sub / f"field={field}"


def state_path(universe: str, config=None, test=False) -> Path:
    return paths.store_dir(universe, config, test) / "_state.json"


def refdata_path(universe: str, config=None, test=False) -> Path:
    return paths.store_dir(universe, config, test) / "_refdata.parquet"


def list_fields(universe: str, layer: str = "raw", config=None, test=False) -> list[str]:
    d = field_dir(universe, layer, "x", config, test).parent
    if not d.is_dir():
        return []
    return sorted(p.name.split("=", 1)[1] for p in d.glob("field=*") if p.is_dir())


def list_years(directory: Path) -> list[int]:
    if not directory.is_dir():
        return []
    return sorted(int(p.stem) for p in directory.glob("[0-9][0-9][0-9][0-9].parquet"))
