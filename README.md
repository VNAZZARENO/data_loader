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
| `--log-level` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |

## Universes

| Universe | Tickers | Index |
|----------|--------:|-------|
| `sxxr` | 600 | STOXX Europe 600 |
| `spx` | 503 | S&P 500 |
| `nky` | 225 | Nikkei 225 |
| `pbh` | 148 | Custom |
| `splpeqty` | 38 | S&P Listed Private Equity |
| `sx5e` | 37 | Euro Stoxx 50 |

Ticker lists live in `tickers/<universe>.csv` (single `Ticker` column).

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
