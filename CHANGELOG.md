# Changelog

## 2026-02-04

### Added
- Multi-universe support with `--universe` CLI flag (`sxxr`, `nky`, `spx`, `pbh`, `sx5e`, `splpeqty`)
- Per-universe ticker CSV files in `tickers/` directory
- `universes` section in config with default and available list
- `{universe}` placeholder in output path for per-universe xlsx files
- `--today` flag to set end date to current date
- `--start-date` flag to override start date from CLI

### Changed
- Tickers are now loaded from `tickers/<universe>.csv` instead of inline YAML config
- Output path includes universe name: `ATLAS_data_{universe}_static.xlsx`
- `paths` section in config (previously hardcoded in `extract_tickers.py`)

### Deprecated
- `extract_tickers.py` -- tickers now come from pre-built CSV files

### Initial release
- Bloomberg data loader using xbbg with BDH extraction
- YAML-based configuration for fields, parameters, and paths
- 3-tier error handling (batch -> per-ticker fallback -> skip & log)
- Multi-sheet xlsx output (parameters + one sheet per field)
- `--dry-run` mode for validation without API calls
