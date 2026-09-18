"""Resolution de la racine du partage (X:\\Quant\\Data == /mnt/srvPergam_docs/Quant/Data).

Jamais derivee de ``__file__`` : le depot existe en plusieurs clones.
Priorite : env ``DL_SHARE_ROOT`` > config ``paths.share_root[_posix]`` > defaut par OS.
"""

from __future__ import annotations

import os
from pathlib import Path

ENV_VAR = "DL_SHARE_ROOT"
DEFAULT_WINDOWS = r"X:\Quant\Data"
DEFAULT_POSIX = "/mnt/srvPergam_docs/Quant/Data"


def share_root(config: dict | None = None) -> Path:
    env = os.environ.get(ENV_VAR)
    if env:
        return Path(env)
    paths_cfg = (config or {}).get("paths", {})
    if os.name == "nt":
        return Path(paths_cfg.get("share_root", DEFAULT_WINDOWS))
    return Path(paths_cfg.get("share_root_posix", DEFAULT_POSIX))


def univers_root(config: dict | None = None) -> Path:
    return share_root(config) / "univers"


def registry_dir(config: dict | None = None) -> Path:
    return univers_root(config) / "registry"


def events_dir(universe: str, config: dict | None = None) -> Path:
    return registry_dir(config) / "_events" / universe


def requests_dir(config: dict | None = None) -> Path:
    return univers_root(config) / "requests"


def runs_dir(universe: str, config: dict | None = None) -> Path:
    return univers_root(config) / "runs" / universe


def store_dir(universe: str, config: dict | None = None, test: bool = False) -> Path:
    return univers_root(config) / ("store_test" if test else "store") / universe


def fx_dir(config: dict | None = None, test: bool = False) -> Path:
    return univers_root(config) / ("store_test" if test else "store") / "_fx"
