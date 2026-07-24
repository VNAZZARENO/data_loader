# Changelog

## 2026-07-24

### Added
- `--mode {static,bt,screening}` for the `option_*` universes, resolving tickers
  dynamically instead of from a frozen CSV (`option_universe.py`).
  - **bt**: indices + every name the fund has EVER held, no duration threshold.
    Fixes the coverage hole left by the >15% rule: names the backtests trade
    (BMW, BNP, Novartis, Renault) had no implied vol, which showed up downstream
    as 65/98 basket names priceable in the PEQ overlay study.
  - **screening**: indices + current holdings only, on a rolling
    `screening_months` window (12), for the daily screener.
- `--fund`, `--api-base-url`, `--refresh-universe` flags and an `option_modes:`
  config block.

### Changed
- Each option mode writes its **own** output file (`option_europe_bt`,
  `option_screening`). A screening run covers a handful of names over one year,
  so writing it to the shared path would have destroyed the backtest history.

### Notes
- Positions come from the GetFundPortfolios API over **https**
  (`https://pergam-tools/getfundportfolios-api`), not the local Flask instance,
  because the loader runs on the Bloomberg terminal machine. The internal host
  uses a self-signed certificate, so verification is disabled for these calls.
- `bt` caches its list to `tickers/option_europe_bt.csv`: the date scan is
  ~1400 API calls (a few minutes), done once, refreshed with
  `--refresh-universe`.
- `--mode static` is the default, so existing invocations are unchanged.

## 2026-06-22

### Changed
- `option_europe` single-stock list re-pointed from Euro Stoxx 50 constituents to the
  **PEQ-held universe** (current holdings ∪ names held >15% of the position-panel
  window) — 124 single names + 13 indices (137 total). Rationale: the tail-put overlay
  can only be sized on names PEQ actually holds; the SX5E list overlapped PEQ by only 5
  names. Non-optionable mid-caps return empty IV and are dropped by the loader.

## 2026-06-17

### Changed
- `option_europe` IV field set locked to terminal-validated mnemonics
  (`probe_iv_fields.py`): moneyness surface = tenors
  `{30DAY,60DAY,3MTH,6MTH,12MTH,18MTH,24MTH}` x pillars `{90,95,97.5,100,105,110}`
  (`_DF`). Adds back-month + LEAP (1Y/1.5Y/2Y) tenors that the first fetch missed.
- Dropped `PUT_IMP_VOL_30D/60D/90D` fields (returned ATM vol, ~91% identical
  to `CALL_IMP_VOL_*`; the put wing lives in the moneyness surface instead).
- Note: Bloomberg publishes no <90% moneyness pillar for these names.

### Fixed
- `tickers/option_europe.csv`: `BYAN GY` -> `BAYN GY` (Bayer; bad ticker
  returned no data on first fetch).

### Added
- `probe_iv_fields.py`: discovers valid moneyness IV mnemonics by sweeping
  tenor/moneyness/suffix vocab on a single ticker over a short window.

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
