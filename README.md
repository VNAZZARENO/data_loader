# ATLAS Bloomberg Data Loader

Pulls historical Bloomberg data (via [xbbg](https://github.com/alpha-xone/xbbg)) for multiple ticker universes and writes multi-sheet xlsx files for use by the ATLAS system.

## Prerequisites

- Python 3.12+
- Bloomberg Terminal running locally (xbbg connects via `blpapi`)

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
# Default universe (sxxr, 600 tickers)
source .venv/bin/activate && python3 bloomberg_loader.py

# Specific universe
source .venv/bin/activate && python3 bloomberg_loader.py --universe nky

# Dry run (validate config, no API calls)
source .venv/bin/activate && python3 bloomberg_loader.py --universe spx --dry-run

# Override date range
source .venv/bin/activate && python3 bloomberg_loader.py --start-date 2020-01-01 --today

# Refresh the ticker list from the live index membership, then exit
source .venv/bin/activate && python3 bloomberg_loader.py --universe sxxr --update-universe
# Preview the joiners/leavers diff without writing
source .venv/bin/activate && python3 bloomberg_loader.py --universe sxxr --update-universe --dry-run
```

### CLI flags

| Flag | Description |
|------|-------------|
| `--universe` | Ticker universe to load (default: `sxxr`) |
| `--dry-run` | Validate config and print plan without API calls |
| `--start-date` | Override start date (e.g. `2013-01-01`) |
| `--end-date` | Override end date (e.g. `2026-02-04`) |
| `--today` | Set end date to today |
| `--config` | Path to YAML config (default: `config/atlas_config.yaml`) |
| `--mode` | Option universe mode: `static` (default), `bt`, `screening` |
| `--fund` | Fund driving `--mode bt`/`screening` (default: `PEQ`) |
| `--api-base-url` | GetFundPortfolios base URL |
| `--refresh-universe` | `--mode bt`: rescan every position date instead of reusing the cache |
| `--update-universe` | Refresh `tickers/<universe>.csv` from live `INDX_MEMBERS`, then exit (preview with `--dry-run`) |
| `--yes` / `-y` | Skip the `--update-universe` confirmation prompt (cron / unattended) |
| `--log-level` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |

### Refreshing a universe (`--update-universe`)

Rebuilds `tickers/<universe>.csv` from the **live Bloomberg index membership** —
`BDS INDX_MEMBERS` on the universe's benchmark index (`benchmarks:` in the
config, e.g. `sxxr` → `SXXR Index`) — then exits without extracting. Bloomberg
returns the current constituents *with their tickers*, so there is no
name→ticker guessing (the public STOXX components PDF carries only company names
and, on the free site, lags the real index by years).

Bloomberg's standard exchange codes are converted to this project's convention
(`ROP SW`→`ROP SE`, `SAN SM`→`SAN SQ`) via a map **learned at runtime from the
existing CSV** (matched on the ticker root): the ~590 continuing names keep their
exact current form, only genuine joiners run through the map. Manual fallbacks
live in `index_members.exchange_code_map`. Safety: aborts on a >50% drop in
member count, backs the old list up to `<csv>.bak`, and `--dry-run` prints the
joiners/leavers diff without writing. Requires a running Bloomberg terminal.

Before writing, it shows the changes and asks to confirm:

```
============================================================
  Mise a jour de l'univers 'sxxr'  <-  SXXR Index
============================================================
  10 ajout(s) :
    + RENK GY
    ...
  8 suppression(s) :
    - SXS LN
    ...
------------------------------------------------------------
  600 -> 600 tickers
  Accepter ? [Y/n]
```

`--yes` skips the prompt for unattended runs; a non-interactive stdin refuses to
write rather than hang.

```bash
python3 bloomberg_loader.py --universe sxxr --update-universe --dry-run  # preview
python3 bloomberg_loader.py --universe sxxr --update-universe            # prompt, then write
python3 bloomberg_loader.py --universe sxxr --update-universe --yes      # cron: no prompt
```

### Option universe modes

`option_europe` was a frozen 137-ticker CSV: EU indices plus the PEQ single
stocks held for more than 15% of the position-panel window. That threshold
silently dropped names the backtests actually trade (BMW, BNP, Novartis,
Renault), which surfaces downstream as missing implied vol. `--mode` makes the
list dynamic, sourcing positions from the GetFundPortfolios API over https
(the loader runs on the Bloomberg terminal machine, not the dev box).

```bash
# Backtest universe: indices + every name PEQ has EVER held (no duration threshold)
python3 bloomberg_loader.py --universe option_europe --mode bt

# Rebuild that list from scratch (rescans all position dates, a few minutes)
python3 bloomberg_loader.py --universe option_europe --mode bt --refresh-universe

# Screening universe: indices + current holdings only, rolling 12-month window
python3 bloomberg_loader.py --universe option_europe --mode screening
```

| Mode | Tickers | Window | Output file |
|------|---------|--------|-------------|
| `static` | frozen `tickers/option_europe.csv` (137) | full history | `ATLAS_data_option_europe_static.xlsx` |
| `bt` | indices + all names ever held | full history | `ATLAS_data_option_europe_bt_static.xlsx` |
| `screening` | indices + current holdings | rolling `screening_months` (12) | `ATLAS_data_option_screening_static.xlsx` |

**Each mode writes its own file on purpose.** A screening run covers a handful
of names over one year: pointing it at the bt or static path would destroy the
long history the backtests depend on.

`bt` caches its resolved list to `tickers/option_europe_bt.csv` so the date scan
(~1400 API calls, a few minutes) happens once. Pass `--refresh-universe` to
rebuild it after the fund has traded new names. Indices are always kept: they
carry the listed-option chains used as hedging references and never appear in a
position file.

Settings live under `option_modes:` in `config/atlas_config.yaml` (`fund`,
`api_base_url`, `bt_since`, `screening_months`, `output_universe`).

## Universes

| Universe | Tickers | Index |
|----------|--------:|-------|
| `sxxr` | 600 | STOXX Europe 600 |
| `spx` | 503 | S&P 500 |
| `nky` | 225 | Nikkei 225 |
| `pbh` | 148 | Custom |
| `splpeqty` | 38 | S&P Listed Private Equity |
| `sx5e` | 37 | Euro Stoxx 50 |
| `option_europe` | 137 | EU indices + PEQ-held single stocks (IV surface) |

Ticker lists live in `tickers/<universe>.csv` (single `Ticker` column).

### `option_europe` (implied-volatility surface)

Dedicated universe for OTM-put / volatility-tail research on PEQ. Mixes
European indices (with their listed-option chains) and the **PEQ-held single
stocks** (current holdings + names held >15% of the position-panel window), so
tail puts are only ever sized on underlyings the fund actually holds. Tickers
carry their full Bloomberg suffix and `ticker_suffix` is set to `''`.
Non-optionable mid-caps simply return empty IV (dropped by the loader). Instead of the default price fields it pulls
an implied-vol field set (defined in `universe_overrides.option_europe`):
ATM call vol + options flow (continuity with the existing `sxxr` vol CSVs)
plus a moneyness IV surface across 7 tenors x 6 pillars.

Surface mnemonics were validated on the terminal with `probe_iv_fields.py`:

- **Tenors:** `30DAY`, `60DAY`, `3MTH`, `6MTH`, `12MTH`, `18MTH`, `24MTH`
  (front month, back month, and a 1Y/1.5Y/2Y LEAP ladder)
- **Pillars (`_DF` suffix only):** `90 / 95 / 97.5` = OTM put wing, `100` = ATM,
  `105 / 110` = call side (for skew / risk-reversal)

> Bloomberg does **not** publish moneyness below 90% for these names, so the
> deepest OTM put on the surface is ~10% OTM; deeper strikes need the option
> chain. Skew is computed downstream from the pillars, not in the loader.

## Bloomberg fields

Configured in `config/atlas_config.yaml` under `fields:`:

| Sheet name | Bloomberg field |
|------------|----------------|
| `price` | `PX_LAST` |
| `Pxtobook` | `PX_TO_BOOK_RATIO` |
| `EPS` | `IS_EPS` |

## Output

One xlsx per universe at the path configured in `paths.output_xlsx`:

```
X:\Quant\Data\ATLAS_data_{universe}_static.xlsx
```

Each file contains a `parameters` sheet plus one data sheet per field.

## Project structure

```
data_loader/
  bloomberg_loader.py      # Main loader script
  extract_tickers.py       # Legacy bootstrap utility (deprecated)
  requirements.txt
  config/
    atlas_config.yaml      # Date range, fields, universe list, paths
  tickers/
    sxxr.csv               # 600 tickers
    nky.csv                # 225 tickers
    spx.csv                # 503 tickers
    pbh.csv                # 148 tickers
    sx5e.csv               #  37 tickers
    splpeqty.csv           #  38 tickers
```

## Error handling

The loader uses 3-tier error handling for Bloomberg extraction:

1. **Batch request** -- tickers are sent in batches of 250
2. **Per-ticker fallback** -- if a batch fails, each ticker in that batch is retried individually
3. **Skip & log** -- tickers that still fail are logged and skipped

A summary of failed tickers is printed at the end of each field extraction.
