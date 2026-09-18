import os
from functools import lru_cache
from pathlib import Path

from dl import migrate

PORT = int(os.environ.get("DL_DASHBOARD_PORT", "7016"))
CACHE_DIR = Path(os.environ.get("DL_DASHBOARD_CACHE", Path.home() / ".cache" / "dl_dashboard"))
START_SCHEDULER = os.environ.get("DL_DASHBOARD_SCHEDULER", "false").lower() == "true"


@lru_cache(maxsize=1)
def config() -> dict:
    return migrate.load_config(os.environ.get("DL_CONFIG"))
