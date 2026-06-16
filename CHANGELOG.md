# Changelog

## 2026-06-16

### Added
- `option_europe` universe: EU indices + Euro Stoxx 50 large caps for OTM-put /
  volatility-tail research on PEQ
- Implied-volatility field set for `option_europe` via
  `universe_overrides.option_europe`: ATM call vol + options flow (continuity
  with the `sxxr` vol CSVs), named put implied vol (`PUT_IMP_VOL_30D/60D/90D`),
  and a downside moneyness surface (`{30,90,180,360}DAY_IMPVOL_{80,90,95}.0%MNY_DF`)
- `tickers/option_europe.csv` (50 full-suffix tickers; `ticker_suffix: ''`)

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
