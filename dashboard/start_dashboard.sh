#!/usr/bin/env bash
# Lance par launch_pergam_tools.sh (service "DataLoader"). Ecoute en local : le proxy intranet expose.
set -e
cd "$(dirname "$0")/.."
source .venv/bin/activate
export DL_DASHBOARD_SCHEDULER=true
export PYTHONPATH="$(pwd):${PYTHONPATH:-}"
exec python3 -m uvicorn dashboard.app:app --host 127.0.0.1 --port "${DL_DASHBOARD_PORT:-7016}"
